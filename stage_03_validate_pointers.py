# =============================================================================
# stage_03_validate_pointers.py  ·  Flood Data Pipeline — Validate & Filter
# =============================================================================
# Checklist Stage 3 (pilot phase)
#
# Three operations in strict order:
#   1. Validation    — drop malformed pointers (bad offset/length/filename)
#   2. Size filter   — flag TOO_SMALL (<500 bytes) and TOO_LARGE (>5MB)
#   3. Deduplication — within flood_id first, then cross-event flag
#
# Reads:
#   output/cc_index_hits.parquet       (from stage_02)
#
# Outputs:
#   output/validated_pointers.parquet
#     Columns: pointer_id, flood_id, query_id, crawl_id, url, filename,
#              offset, length, digest, timestamp, retrieval_strategy,
#              retrieval_rank, is_pointer_duplicate, cross_event_shared,
#              size_filter_status, status, reject_reason
#   output/rejects.parquet
#     All rows with status=REJECTED or size_filter_status != VALID
#
# Run:
#   python stage_03_validate_pointers.py           # pilot events only
#   python stage_03_validate_pointers.py --all     # Phase 2 full run
# =============================================================================

import argparse
import importlib.util
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

# ---------------------------------------------------------------------------
# Force-load local config.py
# ---------------------------------------------------------------------------
_config_path = Path(__file__).parent / "config.py"
_spec = importlib.util.spec_from_file_location("config", _config_path)
_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)
sys.modules["config"] = _config

from config import (
    LOGS_DIR,
    OUTPUT_DIR,
    PILOT_FLOOD_IDS,
    POINTER_MAX_BYTES,
    POINTER_MIN_BYTES,
    SCHEMA_VALIDATED_POINTERS,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_03_validate_pointers.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# WARC filename pattern — e.g. crawl-data/CC-MAIN-2025-47/segments/.../warc/*.warc.gz
# ---------------------------------------------------------------------------
WARC_FILENAME_PATTERN = re.compile(
    r"^crawl-data/CC-MAIN-\d{4}-\d{2}/segments/.+\.warc\.gz$"
)


# =============================================================================
# STEP 1 — Validation
# =============================================================================

def validate_pointers(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate each pointer row. Returns (valid_df, rejected_df).
    Rejection criteria (any one is enough to reject):
      - offset is null, non-numeric, or < 0
      - length is null, non-numeric, or <= 0
      - filename is null or doesn't match WARC path pattern
    """
    log.info("--- Step 1: Validation ---")

    df = df.copy()
    df["offset"] = pd.to_numeric(df["offset"], errors="coerce")
    df["length"] = pd.to_numeric(df["length"], errors="coerce")

    reject_reasons = []

    for idx, row in df.iterrows():
        reasons = []

        if pd.isna(row["offset"]) or row["offset"] < 0:
            reasons.append(f"invalid offset ({row['offset']})")

        if pd.isna(row["length"]) or row["length"] <= 0:
            reasons.append(f"invalid length ({row['length']})")

        filename = str(row.get("filename", "") or "")
        if not filename or not WARC_FILENAME_PATTERN.match(filename):
            reasons.append(f"malformed filename ({filename[:60]})")

        reject_reasons.append(", ".join(reasons) if reasons else "")

    df["_reject_reason"] = reject_reasons
    df["status"]         = df["_reject_reason"].apply(lambda r: "REJECTED" if r else "VALID")
    df["reject_reason"]  = df["_reject_reason"]

    valid_df    = df[df["status"] == "VALID"].copy()
    rejected_df = df[df["status"] == "REJECTED"].copy()

    log.info(f"  Input rows     : {len(df)}")
    log.info(f"  Valid          : {len(valid_df)}")
    log.info(f"  Rejected       : {len(rejected_df)}")

    if not rejected_df.empty:
        reason_counts = (
            rejected_df["reject_reason"]
            .str.split(", ")
            .explode()
            .str.replace(r"\(.*\)", "", regex=True)
            .str.strip()
            .value_counts()
        )
        for reason, count in reason_counts.items():
            log.info(f"    {reason:<35} : {count}")

    return valid_df.drop(columns=["_reject_reason"]), rejected_df.drop(columns=["_reject_reason"])


# =============================================================================
# STEP 2 — Size filter
# =============================================================================

def apply_size_filter(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Flag pointers by byte length:
      TOO_SMALL (<500 bytes)   -> add to rejects table
      TOO_LARGE (>5MB)         -> set aside for separate review (not fully rejected)
      VALID                    -> proceed to dedup
    Returns (all_df_with_flag, too_small_df)
    """
    log.info(f"--- Step 2: Size filter  (min={POINTER_MIN_BYTES}B, max={POINTER_MAX_BYTES}B) ---")

    df = df.copy()

    def size_status(length):
        if pd.isna(length):
            return "TOO_SMALL"  # already caught by validation but belt-and-suspenders
        if length < POINTER_MIN_BYTES:
            return "TOO_SMALL"
        if length > POINTER_MAX_BYTES:
            return "TOO_LARGE"
        return "VALID"

    df["size_filter_status"] = df["length"].apply(size_status)

    counts = df["size_filter_status"].value_counts().to_dict()
    log.info(f"  VALID     : {counts.get('VALID', 0)}")
    log.info(f"  TOO_SMALL : {counts.get('TOO_SMALL', 0)}  -> moved to rejects")
    log.info(f"  TOO_LARGE : {counts.get('TOO_LARGE', 0)}  -> flagged for separate review")

    # TOO_SMALL goes straight to rejects; TOO_LARGE stays in but is flagged
    too_small_df = df[df["size_filter_status"] == "TOO_SMALL"].copy()
    too_small_df["status"]        = "REJECTED"
    too_small_df["reject_reason"] = "TOO_SMALL"

    # Everything else (VALID + TOO_LARGE) continues to dedup
    proceed_df = df[df["size_filter_status"] != "TOO_SMALL"].copy()

    return proceed_df, too_small_df


# =============================================================================
# STEP 3 — Deduplication
# =============================================================================

def deduplicate_pointers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Three-step deduplication per checklist:
      Step 1 — Exact pointer dedup within flood_id
               key = (flood_id, crawl_id, filename, offset, length)
      Step 2 — Digest dedup within flood_id
               key = (flood_id, crawl_id, digest) where digest is not null
      Step 3 — Cross-event flag
               URLs in multiple flood_id partitions -> cross_event_shared = TRUE
               Never drop — a URL can be valid for multiple events.
    """
    log.info("--- Step 3: Deduplication ---")

    df = df.copy()
    df["is_pointer_duplicate"] = False
    df["cross_event_shared"]   = False

    # --- Step 3.1: Exact pointer dedup within flood_id ---
    exact_key = ["flood_id", "crawl_id", "filename", "offset", "length"]
    # Sort so we keep the row with the lowest retrieval_rank as canonical
    df = df.sort_values(["flood_id", "retrieval_rank"], na_position="last")

    dup_mask = df.duplicated(subset=exact_key, keep="first")
    df.loc[dup_mask, "is_pointer_duplicate"] = True

    exact_dup_count = dup_mask.sum()
    log.info(f"  Step 1 (exact pointer dedup)  : {exact_dup_count} duplicates flagged")

    # --- Step 3.2: Digest dedup within flood_id ---
    # Only apply to rows not already flagged and where digest is not null/empty
    digest_key = ["flood_id", "crawl_id", "digest"]
    has_digest = (
        df["digest"].notna() &
        (df["digest"].astype(str).str.strip() != "") &
        (~df["is_pointer_duplicate"])
    )

    digest_dup_mask = (
        df[has_digest]
        .duplicated(subset=digest_key, keep="first")
        .reindex(df.index, fill_value=False)
    )
    df.loc[digest_dup_mask, "is_pointer_duplicate"] = True

    digest_dup_count = digest_dup_mask.sum()
    log.info(f"  Step 2 (digest dedup)         : {digest_dup_count} duplicates flagged")

    # --- Step 3.3: Cross-event flag ---
    # Find URLs that appear in more than one flood_id partition
    url_flood_counts = (
        df.groupby("url")["flood_id"]
        .nunique()
    )
    shared_urls = set(url_flood_counts[url_flood_counts > 1].index)
    df.loc[df["url"].isin(shared_urls), "cross_event_shared"] = True

    shared_count = df["cross_event_shared"].sum()
    log.info(f"  Step 3 (cross-event shared)   : {shared_count} pointers flagged (NOT dropped)")

    # Summary
    total      = len(df)
    dup_total  = df["is_pointer_duplicate"].sum()
    valid_post = total - dup_total
    log.info(f"  Post-dedup: {valid_post} unique pointers  ({dup_total} marked duplicate)")

    return df


# =============================================================================
# STEP 4 — URL deduplication within flood_id
# =============================================================================

def deduplicate_urls(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Within each flood_id, deduplicate by URL.

    The same URL (e.g. detik.com/, a tag page) can appear many times in
    validated_pointers because it was captured across multiple CC crawls or
    matched multiple query variants (A, B, C, D).  Every copy gets
    downloaded and processed separately — wasting bandwidth and inflating
    counts without adding any new content.

    Strategy:
      - Key = (flood_id, url)
      - Among duplicates, keep the pointer with the most recent timestamp
        (most likely to have the freshest byte range in the WARC).
      - Mark all other copies as is_url_duplicate = TRUE.
      - Move them to the rejects table with reason = url_duplicate_within_event.

    IMPORTANT: Never deduplicate across flood_id boundaries.
    The same URL can legitimately cover two different flood events (e.g. a
    news site's flood tag page that kept being updated).

    Returns (deduped_df, url_dup_rejects_df).
    """
    log.info("--- Step 4: URL deduplication within flood_id ---")

    df = df.copy()
    df["is_url_duplicate"] = False

    # Only dedup rows that have a URL — null URLs can't be key-matched
    has_url    = df["url"].notna() & (df["url"].astype(str).str.strip() != "")
    no_url_df  = df[~has_url].copy()
    has_url_df = df[has_url].copy()

    # Sort so that within each (flood_id, url) group the most recent
    # timestamp comes first — that row will be kept as canonical.
    has_url_df["_ts_sort"] = pd.to_datetime(
        has_url_df["timestamp"], errors="coerce", utc=True
    )
    has_url_df = has_url_df.sort_values(
        ["flood_id", "url", "_ts_sort"],
        ascending=[True, True, False],
        na_position="last",
    )

    # Mark all but the first (most recent) occurrence as URL duplicates
    dup_mask = has_url_df.duplicated(subset=["flood_id", "url"], keep="first")
    has_url_df.loc[dup_mask, "is_url_duplicate"] = True

    dup_count    = dup_mask.sum()
    unique_count = (~dup_mask).sum()
    total_has_url = len(has_url_df)

    log.info(f"  Pointers with URL   : {total_has_url}")
    log.info(f"  Unique (flood,url)  : {unique_count}")
    log.info(f"  URL duplicates      : {dup_count}  -> moved to rejects")

    if dup_count > 0:
        dup_rate = dup_count / total_has_url
        if dup_rate > 0.20:
            log.warning(
                f"  ⚠ URL dup rate {dup_rate:.1%} > 20% — likely indicates "
                "over-broad domain queries returning index/tag pages"
            )

    # Per-event breakdown
    for flood_id, grp in has_url_df.groupby("flood_id"):
        grp_dups = grp["is_url_duplicate"].sum()
        log.info(
            f"    Flood #{int(flood_id):>3}  total={len(grp):>6}  "
            f"url_dups={grp_dups:>5}  ({grp_dups/len(grp):.1%})"
        )

    # Recombine with no-URL rows (they pass through unchanged)
    has_url_df = has_url_df.drop(columns=["_ts_sort"])
    combined   = pd.concat([has_url_df, no_url_df], ignore_index=True)

    # Split into keepers and rejects
    url_dup_rejects = combined[combined["is_url_duplicate"]].copy()
    url_dup_rejects["status"]        = "REJECTED"
    url_dup_rejects["reject_reason"] = "url_duplicate_within_event"

    keepers = combined[~combined["is_url_duplicate"]].copy()

    log.info(f"  Keepers after URL dedup : {len(keepers)}")
    return keepers, url_dup_rejects


# =============================================================================
# Assign retrieval_rank within each flood_id + query_id group
# =============================================================================

def assign_retrieval_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Rank pointers within each (flood_id, query_id) group by timestamp desc."""
    df = df.copy()
    df["retrieval_rank"] = (
        df.sort_values("timestamp", ascending=False)
        .groupby(["flood_id", "query_id"])
        .cumcount() + 1
    )
    return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 03 — Validate & filter pointers")
    parser.add_argument("--all",      action="store_true", help="Process all events (Phase 2)")
    parser.add_argument("--flood-id", type=int,            help="Process a single flood_id only (incremental domain add)")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("STAGE 03 — VALIDATE & FILTER POINTERS")
    mode = f"flood_id={args.flood_id}" if args.flood_id else ("ALL events" if args.all else "PILOT events only")
    log.info(f"Mode : {mode}")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Load cc_index_hits from stage_02
    # ------------------------------------------------------------------
    hits_path = OUTPUT_DIR / "cc_index_hits.parquet"
    if not hits_path.exists():
        log.error("cc_index_hits.parquet not found — run stage_02 first")
        sys.exit(1)

    hits_df = pd.read_parquet(hits_path)
    log.info(f"Loaded {len(hits_df)} index hits from {hits_path}")

    if PILOT_FLOOD_IDS and not args.all:
        hits_df = hits_df[hits_df["flood_id"].isin(PILOT_FLOOD_IDS)]
        log.info(f"Filtered to {len(hits_df)} hits for events: {PILOT_FLOOD_IDS}")

    if args.flood_id:
        hits_df = hits_df[hits_df["flood_id"] == args.flood_id]
        log.info(f"Filtered to {len(hits_df)} hits for flood_id={args.flood_id}")

    # ------------------------------------------------------------------
    # Add join keys (flood_id, query_id, crawl_id already present from stage_02)
    # Rename hit_id -> pointer_id for this stage onward
    # ------------------------------------------------------------------
    hits_df = hits_df.rename(columns={"hit_id": "pointer_id"})

    # Assign retrieval rank before dedup (used as tiebreaker)
    hits_df = assign_retrieval_rank(hits_df)

    # Initialise columns that get filled in below
    hits_df["size_filter_status"]  = "VALID"
    hits_df["is_pointer_duplicate"] = False
    hits_df["cross_event_shared"]   = False
    hits_df["status"]               = "VALID"
    hits_df["reject_reason"]        = ""

    all_rejects = []

    # ------------------------------------------------------------------
    # Step 1 — Validation
    # ------------------------------------------------------------------
    valid_df, rejected_df = validate_pointers(hits_df)
    rejected_df["size_filter_status"] = "N/A"
    all_rejects.append(rejected_df)

    # ------------------------------------------------------------------
    # Step 2 — Size filter
    # ------------------------------------------------------------------
    proceed_df, too_small_df = apply_size_filter(valid_df)
    all_rejects.append(too_small_df)

    # ------------------------------------------------------------------
    # Step 3 — Pointer-level deduplication
    # ------------------------------------------------------------------
    deduped_df = deduplicate_pointers(proceed_df)

    # ------------------------------------------------------------------
    # Spot-check: pointer duplicate rate per event
    # ------------------------------------------------------------------
    log.info("--- Pointer duplicate rate per pilot event ---")
    for flood_id, group in deduped_df.groupby("flood_id"):
        total     = len(group)
        dup_count = group["is_pointer_duplicate"].sum()
        dup_rate  = dup_count / total if total > 0 else 0
        flag      = " ⚠ INVESTIGATE" if dup_rate > 0.70 else ""
        log.info(f"  Flood #{int(flood_id):>3}  total={total:>6}  dupes={dup_count:>5}  rate={dup_rate:.1%}{flag}")

    # ------------------------------------------------------------------
    # Step 4 — URL deduplication within flood_id
    # ------------------------------------------------------------------
    deduped_df, url_dup_rejects = deduplicate_urls(deduped_df)
    all_rejects.append(url_dup_rejects)

    # ------------------------------------------------------------------
    # Build final validated_pointers table
    # ------------------------------------------------------------------
    # Add is_url_duplicate to schema if not already there
    schema_cols = list(SCHEMA_VALIDATED_POINTERS)
    if "is_url_duplicate" not in schema_cols:
        schema_cols.append("is_url_duplicate")

    for col in schema_cols:
        if col not in deduped_df.columns:
            deduped_df[col] = None

    validated_df = deduped_df[schema_cols].copy()
    validated_df["flood_id"] = validated_df["flood_id"].astype(int)

    out_path = OUTPUT_DIR / "validated_pointers.parquet"
    processed_flood_ids = set(validated_df["flood_id"].unique())

    if out_path.exists():
        existing_vp = pd.read_parquet(out_path)
        existing_vp["flood_id"] = existing_vp["flood_id"].astype(int)
        kept_vp = existing_vp[~existing_vp["flood_id"].isin(processed_flood_ids)]
        dropped = len(existing_vp) - len(kept_vp)
        if dropped > 0:
            log.info(
                f"Upsert: dropped {dropped} existing validated_pointers rows for "
                f"flood_id(s) {sorted(processed_flood_ids)}"
            )
        validated_df = pd.concat([kept_vp, validated_df], ignore_index=True)
    else:
        log.info("No existing validated_pointers.parquet — creating fresh file")

    validated_df.to_parquet(out_path, index=False)
    log.info(f"Saved validated_pointers -> {out_path}  ({len(validated_df)} rows total)")

    # ------------------------------------------------------------------
    # Build rejects table
    # ------------------------------------------------------------------
    if all_rejects:
        rejects_df = pd.concat(all_rejects, ignore_index=True)

        for col in schema_cols:
            if col not in rejects_df.columns:
                rejects_df[col] = None

        rejects_df = rejects_df[schema_cols].copy()
        rejects_df["flood_id"] = rejects_df["flood_id"].astype(int)

        rejects_path = OUTPUT_DIR / "rejects.parquet"

        if rejects_path.exists():
            existing_rej = pd.read_parquet(rejects_path)
            existing_rej["flood_id"] = existing_rej["flood_id"].astype(int)
            kept_rej = existing_rej[~existing_rej["flood_id"].isin(processed_flood_ids)]
            dropped_rej = len(existing_rej) - len(kept_rej)
            if dropped_rej > 0:
                log.info(
                    f"Upsert: dropped {dropped_rej} existing rejects rows for "
                    f"flood_id(s) {sorted(processed_flood_ids)}"
                )
            rejects_df = pd.concat([kept_rej, rejects_df], ignore_index=True)

        rejects_df.to_parquet(rejects_path, index=False)
        log.info(f"Saved rejects -> {rejects_path}  ({len(rejects_df)} rows total)")
    else:
        log.info("No rejects — rejects.parquet not written (check this is expected)")

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    valid_count      = len(validated_df[validated_df["status"] == "VALID"])
    reject_total     = sum(len(r) for r in all_rejects)
    ptr_dup_total    = validated_df["is_pointer_duplicate"].sum()
    url_dup_total    = len(url_dup_rejects)
    shared_total     = validated_df["cross_event_shared"].sum()

    log.info("=" * 70)
    log.info(f"Input hits              : {len(hits_df)}")
    log.info(f"Rejected (validation)   : {len(rejected_df)}")
    log.info(f"Rejected (TOO_SMALL)    : {len(too_small_df)}")
    log.info(f"Rejected (URL dup)      : {url_dup_total}")
    log.info(f"TOO_LARGE flagged       : {(validated_df['size_filter_status'] == 'TOO_LARGE').sum()}")
    log.info(f"Pointer dups flagged    : {ptr_dup_total}")
    log.info(f"Cross-event shared      : {shared_total}")
    log.info(f"VALID pointers          : {valid_count}")
    log.info("Next: run stage_04_download_warc.py")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
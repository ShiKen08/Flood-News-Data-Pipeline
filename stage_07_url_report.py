# =============================================================================
# stage_07_url_report.py  ·  Flood Data Pipeline — URL Report
# =============================================================================
# Reads clean_text.parquet and produces a clean CSV for manual review.
#
# Key formatting rules:
#   - doc_num column: sequential 1-based number per report (easy row reference)
#   - clean_text_relevant: newlines stripped -> single cell per document
#     (embedded newlines are what cause CSV row-splitting in Excel/Sheets)
#   - Paragraphs preserved as " || " separator so text is still readable
#   - Text only included for docs with flood_hits >= 2 AND loc_hits >= 1
#
# Reads:
#   output/clean_text.parquet
#   output/rejects.parquet          (optional — include with --rejects)
#   flood_crawl.csv                 (for country metadata)
#
# Outputs:
#   output/url_report_{scope}.csv
#
# Run:
#   python stage_07_url_report.py                              # pilot events
#   python stage_07_url_report.py --pilot                      # same
#   python stage_07_url_report.py --full                       # all events
#   python stage_07_url_report.py --flood-id 126               # single event
#   python stage_07_url_report.py --flood-id 126 --rejects     # include rejects
#   python stage_07_url_report.py --flood-id 126 --relevant-only
#   python stage_07_url_report.py --flood-id 126 --relevant-only --include-text
#   python stage_07_url_report.py --full --rejects --relevant-only --include-text
#   python stage_07_url_report.py --flood-id 126 --no-csv      # summary only
# =============================================================================

import argparse
import importlib.util
import re
import sys
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

from config import OUTPUT_DIR, PILOT_FLOOD_IDS

FLOOD_CSV = Path(__file__).parent / "flood_crawl.csv"

# ---------------------------------------------------------------------------
# Column order for the CSV (trimmed to what matters for manual review)
# ---------------------------------------------------------------------------
REPORT_COLS = [
    "doc_num",                    # 1-based sequential number — easy reference
    "flood_id",
    "country",
    "url",
    "domain",
    "page_title",
    "pub_date",
    "pub_in_window",
    "timestamp",
    "language_detected",
    "language_match",
    "is_relevant",
    "flood_term_hits",
    "location_term_hits",
    "subnational_hits",
    "location_specificity_score",
    "word_count",
    "char_count",
    "is_content_duplicate",
    "signal_many_short_lines",
    "signal_no_long_sentence",
    "signal_large_low_flood",
]

# Added last when --include-text is passed
TEXT_COL = "clean_text_relevant"

# Separator used to replace paragraph breaks so text stays in one cell
PARA_SEP = " || "


def extract_domain(url: str) -> str:
    if not url or not isinstance(url, str):
        return ""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.lstrip("www.").lower()
    except Exception:
        return ""


def load_event_meta() -> dict:
    if not FLOOD_CSV.exists():
        return {}
    df = pd.read_csv(FLOOD_CSV)
    return {
        int(row["Flood_ID"]): str(row.get("Country", "") or "")
        for _, row in df.iterrows()
    }


def flatten_text(text: str) -> str:
    """
    Replace newlines with PARA_SEP so the entire article fits in one CSV cell.
    Multiple blank lines (paragraph breaks) become a single separator.
    Single newlines (line wraps) become a space.
    """
    if not text or not isinstance(text, str):
        return ""
    # Collapse paragraph breaks (2+ newlines) into separator
    text = re.sub(r"\n{2,}", PARA_SEP, text)
    # Collapse remaining single newlines into space
    text = text.replace("\n", " ")
    # Clean up any double spaces left behind
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def build_report(
    clean_df:        pd.DataFrame,
    rejects_df:      pd.DataFrame,
    event_meta:      dict,
    include_rejects: bool,
    relevant_only:   bool,
    include_text:    bool,
) -> pd.DataFrame:

    clean_df = clean_df.copy()
    clean_df["_source"] = "kept"

    frames = [clean_df]
    if include_rejects and not rejects_df.empty:
        rej = rejects_df.copy()
        rej["_source"] = "rejected"
        frames.append(rej)

    df = pd.concat(frames, ignore_index=True)

    # Domain
    if "domain" not in df.columns or df["domain"].isna().all():
        df["domain"] = df["url"].fillna("").apply(extract_domain)

    # Country from event metadata
    df["country"] = df["flood_id"].apply(
        lambda fid: event_meta.get(int(fid), "") if pd.notna(fid) else ""
    )

    # Relevant-only filter on kept docs
    if relevant_only:
        kept_mask = df["_source"] == "kept"
        rel_mask  = df["is_relevant"].fillna(False).astype(bool)
        df        = df[~(kept_mask & ~rel_mask)].copy()

    # Sort: flood_id -> relevant first -> flood_term_hits desc
    sort_cols = ["flood_id"]
    ascending = [True]
    if "is_relevant" in df.columns:
        df["_rel_sort"] = (~df["is_relevant"].fillna(False)).astype(int)
        sort_cols.append("_rel_sort")
        ascending.append(True)
    if "flood_term_hits" in df.columns:
        sort_cols.append("flood_term_hits")
        ascending.append(False)
    df = df.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)

    # Sequential doc number
    df.insert(0, "doc_num", range(1, len(df) + 1))

    # Build clean_text_relevant column — single cell, no newlines
    if include_text and "clean_text" in df.columns:
        text_eligible = (
            df["flood_term_hits"].fillna(0).astype(int) >= 2
        ) & (
            df["location_term_hits"].fillna(0).astype(int) >= 1
        )
        df[TEXT_COL] = df["clean_text"].where(text_eligible, other="").apply(flatten_text)

    # Assemble output columns
    out_cols = []
    if include_rejects:
        out_cols += ["_source"]
        if "reject_reason" in df.columns:
            out_cols += ["reject_reason"]
    out_cols += [c for c in REPORT_COLS if c in df.columns]
    if include_text and TEXT_COL in df.columns:
        out_cols.append(TEXT_COL)

    return df[out_cols].reset_index(drop=True)


def print_summary(df: pd.DataFrame, scope: str) -> None:
    total    = len(df)
    has_src  = "_source" in df.columns
    kept     = (df["_source"] == "kept").sum() if has_src else total
    rejected = total - kept

    print(f"\n{'='*65}")
    print(f"  URL REPORT — {scope.upper()}")
    print(f"{'='*65}")
    print(f"  Total rows      : {total}")
    print(f"  Kept docs       : {kept}")
    if rejected:
        print(f"  Rejected docs   : {rejected}")

    if "flood_id" in df.columns:
        print()
        hdr = f"  {'#':>3}  {'flood_id':<8} {'country':<20} {'kept':>5} {'relevant':>8} {'lang_ok':>7} {'dupes':>6}"
        print(hdr)
        print(f"  {'-'*3}  {'-'*8} {'-'*20} {'-'*5} {'-'*8} {'-'*7} {'-'*6}")
        for i, (fid, grp) in enumerate(df.groupby("flood_id"), 1):
            kept_g = (grp["_source"] == "kept").sum() if has_src else len(grp)
            rel_g  = grp["is_relevant"].fillna(False).sum()          if "is_relevant"         in grp else "-"
            lm_g   = grp["language_match"].fillna(False).sum()       if "language_match"       in grp else "-"
            dup_g  = grp["is_content_duplicate"].fillna(False).sum() if "is_content_duplicate" in grp else "-"
            ctry   = (grp["country"].iloc[0] if "country" in grp else "")[:20]
            print(f"  {i:>3}  {int(fid):<8} {ctry:<20} {kept_g:>5} {str(rel_g):>8} {str(lm_g):>7} {str(dup_g):>6}")

    if "domain" in df.columns:
        kept_df = df[df["_source"] == "kept"] if has_src else df
        print(f"\n  Top 15 domains:")
        for domain, count in kept_df["domain"].value_counts().head(15).items():
            print(f"    {count:>5}  {domain}")

    if "reject_reason" in df.columns and rejected:
        print(f"\n  Reject reasons:")
        for reason, count in df["reject_reason"].value_counts().items():
            print(f"    {count:>5}  {reason}")

    print(f"{'='*65}\n")


def main():
    parser = argparse.ArgumentParser(description="Stage 07 — URL report CSV")

    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--full",     action="store_true", help="All events")
    scope.add_argument("--pilot",    action="store_true", help="Pilot events only (default)")
    scope.add_argument("--flood-id", type=int,            help="Single flood_id")

    parser.add_argument("--rejects",        action="store_true", help="Include rejected docs")
    parser.add_argument("--relevant-only",  action="store_true", help="Only is_relevant=True docs")
    parser.add_argument("--include-text",   action="store_true",
                        help="Add clean_text_relevant column (flood_hits>=2, loc_hits>=1 only). "
                             "Newlines replaced with ' || ' so each doc stays in one CSV row.")
    parser.add_argument("--no-csv",         action="store_true", help="Print summary only, skip CSV write")
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    clean_path = OUTPUT_DIR / "clean_text.parquet"
    if not clean_path.exists():
        print(f"ERROR: {clean_path} not found — run stage_06 first")
        sys.exit(1)

    clean_df   = pd.read_parquet(clean_path)
    rejects_df = pd.DataFrame()
    if args.rejects:
        rp = OUTPUT_DIR / "rejects.parquet"
        if rp.exists():
            rejects_df = pd.read_parquet(rp)
        else:
            print("WARNING: rejects.parquet not found")

    event_meta = load_event_meta()

    # ------------------------------------------------------------------
    # Scope
    # ------------------------------------------------------------------
    if args.flood_id:
        scope_label = f"flood_{args.flood_id}"
        clean_df    = clean_df[clean_df["flood_id"] == args.flood_id]
        if not rejects_df.empty:
            rejects_df = rejects_df[rejects_df["flood_id"] == args.flood_id]
    elif args.full:
        scope_label = "full"
    else:
        scope_label = "pilot"
        clean_df    = clean_df[clean_df["flood_id"].isin(PILOT_FLOOD_IDS)]
        if not rejects_df.empty:
            rejects_df = rejects_df[rejects_df["flood_id"].isin(PILOT_FLOOD_IDS)]

    if args.relevant_only: scope_label += "_relevant"
    if args.rejects:       scope_label += "_with_rejects"
    if args.include_text:  scope_label += "_with_text"

    if clean_df.empty:
        print(f"No docs found for scope '{scope_label}'. Check flood_id or run stage_06 first.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Build + write
    # ------------------------------------------------------------------
    report_df = build_report(
        clean_df        = clean_df,
        rejects_df      = rejects_df,
        event_meta      = event_meta,
        include_rejects = args.rejects,
        relevant_only   = args.relevant_only,
        include_text    = args.include_text,
    )

    print_summary(report_df, scope_label)

    if not args.no_csv:
        out_path = OUTPUT_DIR / f"url_report_{scope_label}.csv"
        # quoting=csv.QUOTE_ALL ensures cells with commas/newline remnants stay intact
        import csv
        report_df.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL)
        print(f"Saved -> {out_path}  ({len(report_df)} rows, {len(report_df.columns)} columns)")
        if args.include_text:
            text_filled = (report_df[TEXT_COL].str.len() > 0).sum() if TEXT_COL in report_df else 0
            print(f"  Text column populated for {text_filled} / {len(report_df)} rows")


if __name__ == "__main__":
    main()
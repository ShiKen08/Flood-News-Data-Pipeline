# =============================================================================
# stage_01_query_specs.py  ·  Flood Data Pipeline — Build Event Query Specs
# =============================================================================
# Checklist Stage 1 (pilot phase — 7 events only by default)
#
# Reads:
#   output/crawl_coverage.parquet       (from stage_00 — used to skip NO_CRAWL)
#   output/language_assignments.parquet (from stage_00 — query_language_codes)
#   output/location_dictionary.parquet  (from stage_00 — place names + aliases)
#   config/keyword_lexicon.json         (flood terms per language)
#   config/source_domain_list.json      (optional — domain-restricted filter)
#   flood_crawl.csv                     (raw event data)
#
# Outputs:
#   output/event_query_specs.parquet
#     Columns: query_id, flood_id, crawl_id, query_text, query_language_codes,
#              query_language_skipped, domain_filter, window_start, window_end,
#              window_rule_version, retrieval_strategy, created_at
#
# Run:
#   python stage_01_query_specs.py           # pilot events only
#   python stage_01_query_specs.py --all     # all covered events (Phase 2)
# =============================================================================

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    COL_DURATION,
    COL_FLOOD_ID,
    COL_ISO,
    COL_LANGUAGE,
    COL_LOCATION,
    COL_START_DATE,
    FLOOD_CSV,
    KEYWORD_LEXICON,
    LOGS_DIR,
    OUTPUT_DIR,
    PILOT_FLOOD_IDS,
    PILOT_PRIMARY_ONLY,
    SOURCE_DOMAIN_LIST,
    WINDOW_RULE_VERSION,
    SCHEMA_EVENT_QUERY_SPECS,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_01_query_specs.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# Helpers
# =============================================================================

def load_lexicon() -> dict:
    if not KEYWORD_LEXICON.exists():
        log.error(f"keyword_lexicon.json not found at {KEYWORD_LEXICON}")
        sys.exit(1)
    with open(KEYWORD_LEXICON, encoding='utf-8') as f:
        return json.load(f)


def load_domain_list() -> dict:
    """Load source_domain_list.json if it exists. Returns empty dict if not found."""
    if not SOURCE_DOMAIN_LIST.exists():
        log.warning(f"source_domain_list.json not found at {SOURCE_DOMAIN_LIST} — domain filter will be empty.")
        return {}
    with open(SOURCE_DOMAIN_LIST) as f:
        return json.load(f)


def load_stage00_outputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the three parquet files produced by stage_00."""
    paths = {
        "crawl_coverage":       OUTPUT_DIR / "crawl_coverage.parquet",
        "language_assignments": OUTPUT_DIR / "language_assignments.parquet",
        "location_dictionary":  OUTPUT_DIR / "location_dictionary.parquet",
    }
    missing = [name for name, p in paths.items() if not p.exists()]
    if missing:
        log.error(f"Missing stage_00 outputs: {missing}. Run stage_00_preflight.py first.")
        sys.exit(1)

    coverage_df = pd.read_parquet(paths["crawl_coverage"])
    lang_df     = pd.read_parquet(paths["language_assignments"])
    loc_df      = pd.read_parquet(paths["location_dictionary"])
    return coverage_df, lang_df, loc_df


# =============================================================================
# Query text builders — one function per variant
# =============================================================================

def build_variant_a(lang_codes: list[str], lexicon: dict) -> str:
    """Variant A — broad flood keywords only (all flood terms for each query language)."""
    terms = []
    for lang in lang_codes:
        entry = lexicon.get(lang, {})
        terms.extend(entry.get("flood", []))
    return " OR ".join(sorted(set(terms))) if terms else ""


def build_variant_b(location_terms: list[str]) -> str:
    """Variant B — location names only (place names + aliases)."""
    if not location_terms:
        return ""
    # Wrap multi-word terms in quotes for phrase matching
    quoted = [f'"{t}"' if " " in t else t for t in location_terms]
    return " OR ".join(sorted(set(quoted)))


def build_variant_c(lang_codes: list[str], location_terms: list[str], lexicon: dict) -> str:
    """Variant C — flood keywords + location combined [PRIMARY query]."""
    flood_part    = build_variant_a(lang_codes, lexicon)
    location_part = build_variant_b(location_terms)
    if flood_part and location_part:
        return f"({flood_part}) AND ({location_part})"
    return flood_part or location_part


def build_variant_d(lang_codes: list[str], location_terms: list[str], lexicon: dict) -> str:
    """Variant D — impact/emergency terms + location [optional]."""
    terms = []
    for lang in lang_codes:
        entry = lexicon.get(lang, {})
        terms.extend(entry.get("impact", []))
    if not terms:
        return ""
    impact_part   = " OR ".join(sorted(set(terms)))
    location_part = build_variant_b(location_terms)
    if impact_part and location_part:
        return f"({impact_part}) AND ({location_part})"
    return impact_part or location_part


# =============================================================================
# Domain filter builder
# =============================================================================

def get_domain_filter(iso_code: str, domain_list: dict) -> list[str]:
    """
    Return list of domains for the given ISO country code.
    Falls back to empty list (open-web) if not in domain list.
    """
    return domain_list.get(iso_code.upper(), [])


# =============================================================================
# Core — build all query spec rows for one event
# =============================================================================

def build_specs_for_event(
    event_row:    pd.Series,
    coverage_row: pd.Series,
    lang_row:     pd.Series,
    loc_rows:     pd.DataFrame,
    lexicon:      dict,
    domain_list:  dict,
    crawl_ids:    list[str],
    primary_only: bool = True,
) -> list[dict]:
    """
    Build all query spec rows for a single event.
    Returns a list of dicts (one per variant × crawl_id × language).
    """
    flood_id    = int(event_row[COL_FLOOD_ID])
    iso_code    = str(event_row.get(COL_ISO, "")).strip()
    window_start = coverage_row["window_start"]
    window_end   = coverage_row["window_end"]

    # Language codes and skipped (from stage_00)
    query_lang_codes  = json.loads(lang_row["query_language_codes"])
    query_lang_skipped = json.loads(lang_row["query_language_skipped"])

    # Always include English/Spanish/Portuguese as universal fallbacks — pipeline scope
    # is restricted to these three languages. French removed: it was producing noise
    # for non-Francophone events and is outside the intended language scope.
    for universal_lang in ("eng", "spa", "por"):
        if universal_lang not in query_lang_codes:
            query_lang_codes.append(universal_lang)

    # Location terms: normalised names + aliases
    location_terms = []
    for _, loc_row in loc_rows.iterrows():
        location_terms.append(loc_row["location_normalised"])
        aliases = json.loads(loc_row.get("aliases", "[]") or "[]")
        location_terms.extend(aliases)
    location_terms = list(set(t for t in location_terms if t))  # deduplicate

    # Domain filter
    domains       = get_domain_filter(iso_code, domain_list)
    domain_filter = "restricted" if domains else "open"

    # Build variant query texts
    variant_queries = {
        "A": build_variant_a(query_lang_codes, lexicon),
        "B": build_variant_b(location_terms),
        "C": build_variant_c(query_lang_codes, location_terms, lexicon),  # PRIMARY
        "D": build_variant_d(query_lang_codes, location_terms, lexicon),  # OPTIONAL
    }

    # Filter to variant C only if primary_only is set
    if primary_only:
        variant_queries = {"C": variant_queries["C"]}

    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for crawl_id in crawl_ids:
        for variant, query_text in variant_queries.items():
            if not query_text:
                log.debug(f"  Flood #{flood_id} variant {variant} — empty query text, skipping")
                continue

            # Retrieval strategy: C is primary domain-restricted, others secondary
            if variant == "C" and domain_filter == "restricted":
                strategy = "primary_restricted"
            elif variant == "C":
                strategy = "primary_open"
            else:
                strategy = "secondary_open"

            rows.append({
                "query_id":               f"{flood_id}_{variant}",   # NEVER ISO-based
                "flood_id":               flood_id,                   # integer join key
                "crawl_id":               crawl_id,
                "query_text":             query_text,
                "query_language_codes":   json.dumps(query_lang_codes),
                "query_language_skipped": json.dumps(query_lang_skipped),
                "domain_filter":          domain_filter,
                "domain_list":            json.dumps(domains),
                "window_start":           window_start,
                "window_end":             window_end,
                "window_rule_version":    WINDOW_RULE_VERSION,
                "retrieval_strategy":     strategy,
                "created_at":             now,
            })

    return rows


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 01 — Build event query specs")
    parser.add_argument(
        "--all", action="store_true",
        help="Process all covered events (default: pilot events only)"
    )
    parser.add_argument(
        "--primary-only", action="store_true", default=PILOT_PRIMARY_ONLY,
        help="Only generate variant C (primary) queries — skips A, B, D (default: True)"
    )
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("STAGE 01 — BUILD EVENT QUERY SPECS")
    log.info(f"Mode         : {'ALL covered events' if args.all else 'PILOT events only'}")
    log.info(f"Variants     : {'C only (primary)' if args.primary_only else 'A, B, C, D'}")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------
    coverage_df, lang_df, loc_df = load_stage00_outputs()
    lexicon     = load_lexicon()
    domain_list = load_domain_list()

    raw_df = pd.read_csv(FLOOD_CSV)
    log.info(f"Loaded {len(raw_df)} events from {FLOOD_CSV}")

    # ------------------------------------------------------------------
    # Filter events
    # ------------------------------------------------------------------
    if not args.all:
        raw_df = raw_df[raw_df[COL_FLOOD_ID].isin(PILOT_FLOOD_IDS)].copy()
        log.info(f"Filtered to {len(raw_df)} pilot events")

    # Only process events that have COVERED or PARTIAL crawl status
    covered = coverage_df[coverage_df["coverage_status"].isin(["COVERED", "PARTIAL"])]
    covered_ids = set(covered["flood_id"].tolist())

    skipped_no_crawl = []
    events_to_process = []
    for _, row in raw_df.iterrows():
        fid = int(row[COL_FLOOD_ID])
        if fid not in covered_ids:
            skipped_no_crawl.append(fid)
        else:
            events_to_process.append(row)

    if skipped_no_crawl:
        log.warning(f"Skipping {len(skipped_no_crawl)} NO_CRAWL events: {skipped_no_crawl}")

    log.info(f"Building query specs for {len(events_to_process)} events")

    # ------------------------------------------------------------------
    # Build specs
    # ------------------------------------------------------------------
    all_rows = []

    for event_row in events_to_process:
        flood_id = int(event_row[COL_FLOOD_ID])

        # Get matching crawl IDs for this event
        event_coverage = covered[covered["flood_id"] == flood_id]
        crawl_ids = []
        for _, cov_row in event_coverage.iterrows():
            matching = json.loads(cov_row.get("matching_crawls", "[]") or "[]")
            crawl_ids.extend(matching)
        crawl_ids = list(set(crawl_ids))

        if not crawl_ids:
            log.warning(f"  Flood #{flood_id} — no crawl IDs found despite COVERED status, skipping")
            continue

        # Pull corresponding rows from stage_00 outputs
        coverage_row = event_coverage.iloc[0]
        lang_rows    = lang_df[lang_df["flood_id"] == flood_id]
        loc_rows     = loc_df[loc_df["flood_id"] == flood_id]

        if lang_rows.empty:
            log.warning(f"  Flood #{flood_id} — no language assignment found, skipping")
            continue

        lang_row = lang_rows.iloc[0]
        lang_codes = json.loads(lang_row["query_language_codes"])

        rows = build_specs_for_event(
            event_row=event_row,
            coverage_row=coverage_row,
            lang_row=lang_row,
            loc_rows=loc_rows,
            lexicon=lexicon,
            domain_list=domain_list,
            crawl_ids=crawl_ids,
            primary_only=args.primary_only,
        )

        log.info(
            f"  Flood #{flood_id:>3}  ({str(event_row.get(COL_LOCATION, '') or '')[:40]:<40})  "
            f"langs={lang_codes}  crawls={crawl_ids}  rows={len(rows)}"
        )
        all_rows.extend(rows)

    if not all_rows:
        log.error("No query spec rows generated. Check stage_00 outputs and CSV.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Validate schema and write output
    # ------------------------------------------------------------------
    specs_df = pd.DataFrame(all_rows)

    # Confirm join key is integer flood_id
    specs_df["flood_id"] = specs_df["flood_id"].astype(int)

    # Sanity check — no ISO-based query_ids
    iso_pattern = specs_df["query_id"].str.match(r"^[A-Z]{3}_")
    if iso_pattern.any():
        bad = specs_df[iso_pattern]["query_id"].tolist()
        log.error(f"ISO-based query_ids detected — this must never happen: {bad}")
        sys.exit(1)

    # Check all expected schema columns are present
    missing_cols = [c for c in SCHEMA_EVENT_QUERY_SPECS if c not in specs_df.columns]
    if missing_cols:
        log.warning(f"Output is missing schema columns: {missing_cols}")

    out_path = OUTPUT_DIR / "event_query_specs.parquet"
    specs_df.to_parquet(out_path, index=False)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    log.info("=" * 70)
    log.info(f"Total query spec rows : {len(specs_df)}")
    log.info(f"Events covered        : {specs_df['flood_id'].nunique()}")
    log.info(f"Variants breakdown    :")
    for variant, count in specs_df["query_id"].str.extract(r"_([A-D])$")[0].value_counts().items():
        log.info(f"    Variant {variant} : {count} rows")
    log.info(f"Saved -> {out_path}")
    log.info("Next: run stage_02_query_cc_index.py")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
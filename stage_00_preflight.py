# =============================================================================
# stage_00_preflight.py  ·  Flood Data Pipeline — Pre-Flight Setup
# =============================================================================
# Covers checklist Stages 0a, 0b, 0c, 0d (pilot phase)
#
# What this script does:
#   0a — Fetches + caches collinfo.json, checks crawl lag for pilot events
#   0b — Validates windowing rules are applied correctly (v1)
#   0c — Resolves query_language_codes + query_language_skipped per event
#   0d — Builds location_dictionary from flood_crawl.csv
#
# Outputs (all written to output/):
#   - collinfo.json             (cached to BASE_DIR)
#   - crawl_coverage.parquet    (flood_id | window_start | window_end | coverage_status | matching_crawls | note)
#   - language_assignments.parquet  (flood_id | query_language_codes | query_language_skipped)
#   - location_dictionary.parquet   (flood_id | location_raw | location_normalised | aliases)
#
# Run:
#   python stage_00_preflight.py [--all]
#   Default: pilot events only (7 IDs). Pass --all to process all 150 events.
# =============================================================================

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import sys
sys.stdout.reconfigure(encoding='utf-8')

# ---------------------------------------------------------------------------
# Bootstrap — make sure config.py is importable when running from any cwd
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent))
from config import (
    BASE_DIR,
    CACHE_DIR,
    CC_COLLINFO_URL,
    COLLINFO_PATH,
    COL_DURATION,
    COL_FLOOD_ID,
    COL_LANGUAGE,
    COL_LOCATION,
    COL_START_DATE,
    EXPECTED_NO_CRAWL_IDS,
    FLOOD_CSV,
    LOGS_DIR,
    OUTPUT_DIR,
    PILOT_FLOOD_IDS,
    TIER_1_LANGUAGES,
    TIER_2_LANGUAGES,
    TIER_3_FALLBACK_LANGUAGES,
    TIER_3_LANGUAGES,
    WINDOW_LONG_DURATION_THRESHOLD,
    WINDOW_POST_DAYS,
    WINDOW_POST_LONG_DAYS,
    WINDOW_PRE_DAYS,
    WINDOW_RULE_VERSION,
    ZERO_DURATION_TREAT_AS_DAYS,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_00_preflight.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# STAGE 0a — Crawl lag check
# =============================================================================

def fetch_collinfo(force_refresh: bool = False) -> list[dict]:
    """
    Fetch the CC crawl listing and cache it locally.
    Returns the parsed list of crawl metadata dicts.
    """
    if COLLINFO_PATH.exists() and not force_refresh:
        log.info(f"Loading cached collinfo from {COLLINFO_PATH}")
        with open(COLLINFO_PATH) as f:
            return json.load(f)

    log.info(f"Fetching collinfo from {CC_COLLINFO_URL}")
    resp = requests.get(CC_COLLINFO_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    with open(COLLINFO_PATH, "w") as f:
        json.dump(data, f, indent=2)
    log.info(f"Saved collinfo to {COLLINFO_PATH}  ({len(data)} crawls)")
    return data


def parse_crawl_windows(collinfo: list[dict]) -> list[dict]:
    """
    Extract crawl_id, from_dt, to_dt from collinfo entries.
    CC uses 'from' and 'to' keys with format 'YYYY-MM-DDThh:mm:ssZ'.
    Falls back gracefully if keys are missing.
    """
    crawls = []
    for entry in collinfo:
        crawl_id = entry.get("id", entry.get("name", "unknown"))
        try:
            from_dt = datetime.fromisoformat(entry["from"].replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
            to_dt   = datetime.fromisoformat(entry["to"].replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
        except (KeyError, ValueError):
            log.warning(f"Could not parse date range for crawl {crawl_id} — skipping")
            continue
        crawls.append({"crawl_id": crawl_id, "from_dt": from_dt, "to_dt": to_dt})
    return crawls


def check_crawl_coverage(event_row: pd.Series, crawls: list[dict]) -> dict:
    """
    For a single event row, compute its window and check CC crawl coverage.
    Returns a dict with coverage_status, matching_crawls, window_start, window_end.
    """
    flood_id    = int(event_row[COL_FLOOD_ID])
    event_start = pd.to_datetime(event_row[COL_START_DATE]).to_pydatetime().replace(tzinfo=timezone.utc)
    duration    = int(event_row.get(COL_DURATION, 0) or 0)

    # --- Stage 0b windowing rules (v1) ---
    if duration == 0:
        effective_duration = ZERO_DURATION_TREAT_AS_DAYS
    else:
        effective_duration = duration

    event_end = event_start + timedelta(days=effective_duration - 1)

    window_start = event_start - timedelta(days=WINDOW_PRE_DAYS)

    if duration > WINDOW_LONG_DURATION_THRESHOLD:
        window_end = event_end + timedelta(days=WINDOW_POST_LONG_DAYS)
        window_note = f"Long event ({duration}d) — post window capped at +{WINDOW_POST_LONG_DAYS}d"
    else:
        window_end = event_end + timedelta(days=WINDOW_POST_DAYS)
        window_note = ""

    # --- Overlap check ---
    matching = []
    for crawl in crawls:
        # Overlap: crawl starts before window ends AND crawl ends after window starts
        if crawl["from_dt"] <= window_end and crawl["to_dt"] >= window_start:
            matching.append(crawl["crawl_id"])

    if not matching:
        status = "NO_CRAWL"
        if flood_id in EXPECTED_NO_CRAWL_IDS:
            window_note = (window_note + " | Expected NO_CRAWL (crawl lag)").strip(" | ")
    elif len(matching) == 1:
        # Partial if the single crawl only clips the window
        crawl_data = next(c for c in crawls if c["crawl_id"] == matching[0])
        if crawl_data["from_dt"] > window_start or crawl_data["to_dt"] < window_end:
            status = "PARTIAL"
        else:
            status = "COVERED"
    else:
        status = "COVERED"

    return {
        "flood_id":          flood_id,
        "window_start":      window_start.isoformat(),
        "window_end":        window_end.isoformat(),
        "window_rule_version": WINDOW_RULE_VERSION,
        "coverage_status":   status,
        "matching_crawls":   json.dumps(matching),
        "note":              window_note,
    }


def run_crawl_lag_check(df: pd.DataFrame, crawls: list[dict]) -> pd.DataFrame:
    log.info("--- Stage 0a: Crawl lag check ---")
    results = []
    for _, row in df.iterrows():
        result = check_crawl_coverage(row, crawls)
        status_label = result["coverage_status"]
        log.info(f"  Flood #{result['flood_id']:>3}  {status_label:<10}  {result['note']}")
        results.append(result)

    coverage_df = pd.DataFrame(results)

    no_crawl = coverage_df[coverage_df["coverage_status"] == "NO_CRAWL"]["flood_id"].tolist()
    if no_crawl:
        log.warning(f"NO_CRAWL events (will be excluded from query specs): {no_crawl}")

    out_path = OUTPUT_DIR / "crawl_coverage.parquet"
    coverage_df.to_parquet(out_path, index=False)
    log.info(f"Saved crawl_coverage -> {out_path}")
    return coverage_df


# =============================================================================
# STAGE 0c — Language resolution
# =============================================================================

def resolve_languages(event_row: pd.Series) -> dict:
    """
    For a single event, parse its language codes and assign to
    query_language_codes (will query) and query_language_skipped (dropped).
    """
    flood_id = int(event_row[COL_FLOOD_ID])

    # Language column may be comma or space separated, e.g. "eng,spa,lin"
    raw_langs = str(event_row.get(COL_LANGUAGE, "") or "")
    codes = [c.strip().lower() for c in re.split(r"[,\s]+", raw_langs) if c.strip()]

    query_codes   = []
    skipped_codes = {}

    for code in codes:
        if code in TIER_1_LANGUAGES:
            query_codes.append(code)
        elif code in TIER_2_LANGUAGES:
            query_codes.append(code)
        elif code in TIER_3_LANGUAGES:
            # Use fallback languages instead
            for fb in TIER_3_FALLBACK_LANGUAGES:
                if fb not in query_codes:
                    query_codes.append(fb)
            skipped_codes[code] = f"Tier 3 — low CC coverage; fallback to {sorted(TIER_3_FALLBACK_LANGUAGES)}"
        else:
            skipped_codes[code] = "Unknown language code — not in any tier"

    # Universal fallback: always include major international reporting languages.
    # Reuters, AP, AFP, Al Jazeera cover every major flood in these languages
    # regardless of event country — we always want this international coverage.
    for universal_lang in ("eng", "fra", "spa", "por"):
        if universal_lang not in query_codes:
            query_codes.append(universal_lang)

    return {
        "flood_id":               flood_id,
        "query_language_codes":   json.dumps(sorted(set(query_codes))),
        "query_language_skipped": json.dumps(skipped_codes),
    }


def run_language_assignments(df: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Stage 0c: Language assignments ---")
    results = [resolve_languages(row) for _, row in df.iterrows()]
    lang_df = pd.DataFrame(results)

    out_path = OUTPUT_DIR / "language_assignments.parquet"
    lang_df.to_parquet(out_path, index=False)
    log.info(f"Saved language_assignments -> {out_path}")
    return lang_df


# =============================================================================
# STAGE 0d — Location dictionary
# =============================================================================

# ── Country name set — used to tag level = "country" ─────────────────────────
COUNTRY_NAMES = {
    "syria", "syrian arab republic", "indonesia", "colombia", "iran",
    "iran (islamic republic of)", "democratic republic of the congo", "drc",
    "dr congo", "gambia", "the gambia", "united states", "united states of america",
    "usa", "india", "malaysia", "bolivia", "iraq", "thailand", "morocco",
    "afghanistan", "zimbabwe", "tunisia", "algeria", "costa rica", "honduras",
    "mexico", "nepal", "bulgaria", "cambodia", "ukraine", "georgia", "sierra leone",
    "sudan", "uganda", "china", "pakistan", "south korea", "republic of korea",
    "central african republic", "cabo verde", "japan", "myanmar", "equatorial guinea",
    "taiwan", "romania", "nigeria", "laos", "lao people's democratic republic",
    "bangladesh", "vietnam", "viet nam", "philippines", "south africa", "brazil",
    "croatia", "bosnia and herzegovina", "italy", "argentina", "peru", "namibia",
    "botswana", "madagascar", "spain", "dominican republic", "haiti", "france",
    "kenya", "malawi", "gabon", "tanzania", "united republic of tanzania", "somalia",
    "libya", "yemen", "cameroon", "chad", "guinea", "hong kong",
}

# ── Ambiguous names — Stage 06 requires flood keyword in same sentence ────────
AMBIGUOUS_LOCATION_NAMES = {
    "georgia",    # US state AND country
    "columbia",   # DC AND country variant
    "virginia",   # US state
    "victoria",   # city in multiple countries + African lake
    "santa cruz",  # Bolivia AND other countries
    "niger",      # country AND Nigerian state
    "congo",      # DRC AND Republic of Congo
}

# ── Full alias map — canonical lowercase -> known alternate spellings ──────────
# Covers all pilot events + common variants. Extend after pilot review.
KNOWN_ALIASES: dict[str, list[str]] = {
    # Countries
    "democratic republic of the congo": [
        "drc", "dr congo", "congo-kinshasa", "zaire",
        "republique democratique du congo", "rdc", "rd congo",
    ],
    "syrian arab republic":   ["syria", "syrian republic"],
    "iran (islamic republic of)": ["iran", "persia", "islamic republic of iran"],
    "united states of america": ["usa", "united states", "us", "america", "u.s.", "u.s.a."],
    "united states":          ["usa", "us", "america", "u.s."],
    "bolivia (plurinational state of)": ["bolivia"],
    "lao people's democratic republic": ["laos", "lao pdr"],
    "united republic of tanzania": ["tanzania"],
    "viet nam":               ["vietnam", "viet-nam"],
    "republic of korea":      ["south korea", "korea"],
    "gambia":                 ["the gambia"],
    "colombia":               ["republic of colombia"],
    "indonesia":              ["republic of indonesia"],
    "iran":                   ["islamic republic of iran", "persia"],
    "syria":                  ["syrian arab republic"],
    # High-value subnational — pilot events
    "greater jakarta":        ["greater jakarta area", "jakarta", "jabodetabek"],
    "indramayu":              ["indramayu regency"],
    "java":                   ["java island", "java isl."],
    "khyber pakhtunkhwa":     ["kpk", "nwfp"],
    "west bengal":            ["west bengal state"],
    "kinshasa":               ["kinshasa capital city", "kinshasa city"],
    "south kivu":             ["sud-kivu"],
    "esfahan":                ["isfahan"],
    "khorasan razavi":        ["razavi khorasan"],
    "santa barbara":          ["santa barbara county"],
    "los angeles":            ["los angeles county", "la county"],
    "ventura":                ["ventura county"],
    "phra nakhon si ayutthaya": ["ayutthaya"],
    "emilia-romagna":         ["emilia romagna"],
    "odesa":                  ["odessa", "odesa oblast"],
    "ivory coast":            ["côte d'ivoire", "cote d'ivoire"],
    "cordoba":                ["córdoba"],
    "la guajira":             ["guajira"],
    "chocó":                  ["choco"],
    "antioquia":              ["antioquia department"],
    "idleb":                  ["idlib"],
    "lattaquié":              ["latakia", "lattakia"],
    "nord-kivu":              ["north kivu"],
    # Spain validation event — Valencia flood Oct 2024
    "valencia":               ["valència", "comunitat valenciana", "comunidad valenciana", "valenciana"],
    "paiporta":               ["paiporta municipality"],
    "catarroja":              ["catarroja municipality"],
    "aldaya":                 ["aldaia"],
}


def normalise_location(raw: str) -> str:
    """Lowercase, strip, collapse whitespace, remove trailing punctuation."""
    text = re.sub(r"\s+", " ", raw.strip().lower())
    text = text.strip(".,;:()")
    return text


def get_aliases(normalised: str) -> list[str]:
    """Return known aliases for a normalised place name."""
    return KNOWN_ALIASES.get(normalised, [])


_ADMIN_SUFFIX_RE = re.compile(
    r"\s+(province|state|region|district|department|governorate|"
    r"prefecture|county|municipality|regency|oblast|commune|"
    r"capital city|city area|isl\.|island)s?$",
    re.IGNORECASE,
)
_DIRECTIONAL_PREFIX_RE = re.compile(
    r"^(eastern|western|northern|southern|central|northeast|northwest|"
    r"southeast|southwest)\s+",
    re.IGNORECASE,
)
_FILLER_TOKENS = {"and", "or", "the", "of", "in", "with", "isl", "island"}


def _clean_location_token(sp: str) -> str:
    """Strip admin suffixes, directional prefixes, leading conjunctions, normalise."""
    sp = sp.strip()
    sp = re.sub(r"^(and|or|the)\s+", "", sp, flags=re.IGNORECASE).strip()
    sp = _ADMIN_SUFFIX_RE.sub("", sp).strip()
    sp = _DIRECTIONAL_PREFIX_RE.sub("", sp).strip()
    return normalise_location(sp)


def parse_location_field(raw: str) -> list[str]:
    """
    Split a Location field into individual clean place names.

    Parenthetical content is now EXTRACTED as additional terms (not stripped),
    so e.g. "Kerr county (Texas)" yields both "kerr" and "texas", and
    "Roswell (Chaves county, Eastern New Mexico)" yields "roswell", "chaves",
    and "new mexico".
    """
    if not raw or not raw.strip():
        return []

    # Extract content inside parentheses as additional location terms
    paren_parts: list[str] = re.findall(r"\(([^)]+)\)", raw)

    # Remove parentheses from main text so we don't double-count the outer token
    main_text = re.sub(r"\([^)]*\)", "", raw)

    # Process main text + each parenthetical block
    all_raw_parts: list[str] = []
    for chunk in [main_text] + paren_parts:
        parts = re.split(r"[,;]", chunk)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            # Fallback split on " and " for very long unsplit strings
            if len(part) > 40 and " and " in part.lower():
                all_raw_parts.extend(re.split(r"\s+and\s+", part, flags=re.IGNORECASE))
            else:
                all_raw_parts.append(part)

    seen: set[str] = set()
    result: list[str] = []
    for raw_tok in all_raw_parts:
        sp = _clean_location_token(raw_tok)
        if len(sp) < 3:
            continue
        if sp in _FILLER_TOKENS:
            continue
        if sp not in seen:
            seen.add(sp)
            result.append(sp)

    return result


def build_location_dictionary(df: pd.DataFrame) -> pd.DataFrame:
    log.info("--- Stage 0d: Building location dictionary ---")
    records = []

    for _, row in df.iterrows():
        flood_id     = int(row[COL_FLOOD_ID])
        raw_location = str(row.get(COL_LOCATION, "") or "")
        country_name = normalise_location(str(row.get("Country", "") or ""))

        place_names = parse_location_field(raw_location)

        # River Basin column → additional subnational terms (e.g. "Guadalupe")
        river_basin_raw = str(row.get("River Basin", "") or "").strip()
        if river_basin_raw and river_basin_raw.lower() not in {"nan", "n/a", "none", ""}:
            for basin_name in parse_location_field(river_basin_raw):
                if basin_name not in place_names:
                    place_names.append(basin_name)

        # Always include the country itself
        all_names = []
        if country_name:
            all_names.append(country_name)
        # Add country aliases as separate entries so both forms match in Stage 06
        for alias in KNOWN_ALIASES.get(country_name, []):
            alias_norm = normalise_location(alias)
            if alias_norm not in all_names:
                all_names.append(alias_norm)
        # Add subnational place names
        for name in place_names:
            if name not in all_names:
                all_names.append(name)

        for name in all_names:
            # Level: country if in country set or is a country alias
            country_aliases_norm = [
                normalise_location(a) for a in KNOWN_ALIASES.get(country_name, [])
            ]
            is_country = (
                name == country_name or
                name in COUNTRY_NAMES or
                name in country_aliases_norm
            )
            level = "country" if is_country else "subnational"

            records.append({
                "flood_id":            flood_id,
                "location_raw":        name,
                "location_normalised": name,
                "aliases":             json.dumps(KNOWN_ALIASES.get(name, [])),
                "level":               level,
                "ambiguous":           name in AMBIGUOUS_LOCATION_NAMES,
            })

    loc_df   = pd.DataFrame(records)
    out_path = OUTPUT_DIR / "location_dictionary.parquet"
    loc_df.to_parquet(out_path, index=False)
    log.info(f"Saved location_dictionary -> {out_path}  ({len(loc_df)} location rows)")
    return loc_df


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 00 — Pre-flight setup")
    parser.add_argument(
        "--all", action="store_true",
        help="Process all events in flood_crawl.csv (default: pilot events only)"
    )
    parser.add_argument(
        "--refresh-collinfo", action="store_true",
        help="Force re-download of collinfo.json even if cached copy exists"
    )
    parser.add_argument(
        "--rebuild-locations", action="store_true",
        help="Rebuild location_dictionary.parquet only (no network calls). Use after "
             "fixing parse_location_field without re-running the full pipeline."
    )
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("STAGE 00 — PRE-FLIGHT SETUP")
    log.info(f"Window rule version : {WINDOW_RULE_VERSION}")
    log.info(f"Mode                : {'ALL events' if args.all else 'PILOT events only'}")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Load flood_crawl.csv
    # ------------------------------------------------------------------
    if not FLOOD_CSV.exists():
        log.error(f"flood_crawl.csv not found at {FLOOD_CSV}. Place it in {BASE_DIR} and retry.")
        sys.exit(1)

    full_df = pd.read_csv(FLOOD_CSV)
    log.info(f"Loaded {len(full_df)} events from {FLOOD_CSV}")

    # Filter to pilot set unless --all flag passed
    if not args.all:
        df = full_df[full_df[COL_FLOOD_ID].isin(PILOT_FLOOD_IDS)].copy()
        log.info(f"Filtered to {len(df)} pilot events: {PILOT_FLOOD_IDS}")
    else:
        df = full_df.copy()

    if df.empty:
        log.error("No matching events found. Check Flood_ID column name in your CSV.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # --rebuild-locations: skip network calls, just rebuild location dict
    # ------------------------------------------------------------------
    if args.rebuild_locations:
        loc_df = build_location_dictionary(df)
        log.info("Location dictionary rebuilt. Re-run stage_06v with --fresh.")
        return

    # ------------------------------------------------------------------
    # Stage 0a — Crawl lag check
    # ------------------------------------------------------------------
    collinfo = fetch_collinfo(force_refresh=args.refresh_collinfo)
    crawls   = parse_crawl_windows(collinfo)
    log.info(f"Parsed {len(crawls)} crawl windows from collinfo")

    coverage_df = run_crawl_lag_check(df, crawls)

    # Summary
    summary = coverage_df["coverage_status"].value_counts().to_dict()
    log.info(f"Coverage summary: {summary}")

    # ------------------------------------------------------------------
    # Stage 0c — Language assignments
    # ------------------------------------------------------------------
    lang_df = run_language_assignments(df)

    # ------------------------------------------------------------------
    # Stage 0d — Location dictionary
    # ------------------------------------------------------------------
    loc_df = build_location_dictionary(df)

    # ------------------------------------------------------------------
    # Done
    # ------------------------------------------------------------------
    log.info("=" * 70)
    log.info("Stage 00 complete. Outputs written to output/")
    log.info("  crawl_coverage.parquet")
    log.info("  language_assignments.parquet")
    log.info("  location_dictionary.parquet")
    log.info("Next: run stage_01_query_specs.py (pilot events only, skip NO_CRAWL)")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
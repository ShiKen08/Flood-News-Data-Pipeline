# =============================================================================
# stage_02_query_cc_index.py  ·  Flood Data Pipeline — Query CC Index
# =============================================================================
# Checklist Stage 2 (pilot phase)
#
# How the CC CDX index API works:
#   - Queries by URL pattern + timestamp window, NOT by keyword
#   - Endpoint: https://index.commoncrawl.org/{crawl_id}-index
#   - domain-restricted strategy: query each domain in source_domain_list
#   - open-web strategy: query broad wildcard (*.tld/*) for event's country TLD
#   - Keyword matching happens downstream in stage_06 (content filtering)
#
# Reads:
#   output/event_query_specs.parquet    (from stage_01)
#   config/source_domain_list.json
#
# Outputs:
#   raw_index_responses/{flood_id}_{crawl_id}_{domain}.jsonl  (raw saves)
#   output/cc_index_hits.parquet
#     Columns: hit_id, flood_id, query_id, crawl_id, url, timestamp,
#              status, mime, filename, offset, length, digest, domain,
#              retrieval_strategy, fetched_at
#   output/hit_count_summary.parquet
#     Columns: flood_id, query_id, variant, crawl_id, domain, hit_count
#
# Run:
#   python stage_02_query_cc_index.py           # pilot events only
#   python stage_02_query_cc_index.py --all     # Phase 2 full run
#   python stage_02_query_cc_index.py --flood-id 3   # single event debug
# =============================================================================

import argparse
import importlib.util
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
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

import pandas as pd
from config import (
    CC_INDEX_URL,
    CC_INDEX_WORKERS,
    LOGS_DIR,
    OUTPUT_DIR,
    PILOT_FLOOD_IDS,
    RAW_INDEX_DIR,
    SOURCE_DOMAIN_LIST,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_02_query_cc_index.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CC_INDEX_PAGE_SIZE   = 1000     # max results per API page
CC_REQUEST_TIMEOUT   = 30       # seconds
CC_RETRY_MAX         = 3
CC_RETRY_BACKOFF     = 2        # seconds; wait = backoff ^ attempt
CC_RATE_LIMIT_SLEEP  = 0.5      # seconds between requests — respect CC servers

# Per-domain CC hit cap — prevents high-volume national/international domains
# (foxnews.com, aljazeera.com, cbsnews.com) from consuming the pointer budget.
# National outlets contribute 8k+ rejects per flood with near-zero relevance on
# specific county-level events. Local/regional domains are exempt (see domain_hints).
MAX_DOMAIN_HITS         = 3000   # default cap per domain per flood
MAX_DOMAIN_HITS_NATIONAL = 500   # tighter cap for national/international scope domains


# =============================================================================
# Helpers
# =============================================================================

def load_domain_list() -> dict:
    if not SOURCE_DOMAIN_LIST.exists():
        log.warning("source_domain_list.json not found — all queries will use open-web strategy")
        return {}
    with open(SOURCE_DOMAIN_LIST) as f:
        return json.load(f)


def load_national_scope_domains() -> set:
    """Load the set of national/international scope domains from domain_hints.json."""
    hints_path = Path(__file__).parent / "config" / "domain_hints.json"
    if not hints_path.exists():
        log.debug("domain_hints.json not found — all domains will use default hit cap")
        return set()
    with open(hints_path) as f:
        hints = json.load(f)
    return set(hints.get("national_scope", []))


def ts_to_cc_format(iso_ts: str) -> str:
    """
    Convert ISO timestamp string to CC CDX 14-digit format: YYYYMMDDHHmmss
    e.g. '2026-02-03T00:00:00+00:00' -> '20260203000000'
    """
    dt = datetime.fromisoformat(iso_ts)
    return dt.strftime("%Y%m%d%H%M%S")


def extract_domain(url: str) -> str:
    """Extract bare domain from a URL string."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.lstrip("www.")
    except Exception:
        return ""


def raw_response_path(flood_id: int, crawl_id: str, domain_slug: str) -> Path:
    """Build path for saving raw index response JSONL."""
    safe_crawl = crawl_id.replace("-", "_")
    safe_domain = domain_slug.replace(".", "_").replace("/", "_")[:60]
    return RAW_INDEX_DIR / f"{flood_id}_{safe_crawl}_{safe_domain}.jsonl"


# =============================================================================
# Core CC index query — one domain, one crawl, one time window
# =============================================================================

def _log_failed_query(url_pattern: str, crawl_id: str, page: int, reason: str, http_code: int = 0) -> None:
    """Append a failed query record to logs/failed_queries.jsonl."""
    record = {
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "crawl_id":    crawl_id,
        "url_pattern": url_pattern,
        "page":        page,
        "http_code":   http_code,
        "reason":      reason,
    }
    failed_log = LOGS_DIR / "failed_queries.jsonl"
    with open(failed_log, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def query_cc_index(
    crawl_id:      str,
    url_pattern:   str,
    from_ts:       str,
    to_ts:         str,
    raw_save_path: Path,
    hit_cap:       int = MAX_DOMAIN_HITS,
) -> list[dict]:
    """
    Query the CC CDX index for a single URL pattern + time window.
    Pages through all results. Saves raw JSONL to disk immediately.
    Failed queries are logged to logs/failed_queries.jsonl — never crash.
    Returns list of parsed hit dicts (may be partial if errors occurred).
    hit_cap: stop paginating once this many hits are collected (prevents high-volume
    domains from consuming excessive pointer budget).
    """
    import http.client

    _hit_cap = hit_cap
    base_url = CC_INDEX_URL.format(crawl_id=crawl_id)
    hits = []
    page = 0

    raw_save_path.parent.mkdir(parents=True, exist_ok=True)

    with open(raw_save_path, "a", encoding="utf-8") as raw_f:
        while True:
            params = {
                "url":    url_pattern,
                "output": "json",
                "from":   from_ts,
                "to":     to_ts,
                "limit":  CC_INDEX_PAGE_SIZE,
                "fl":     "url,timestamp,status,mime,filename,offset,length,digest",
                "filter": ["status:200", "mime:text/html"],
                "page":   page,
            }

            query_parts = []
            for k, v in params.items():
                if isinstance(v, list):
                    for item in v:
                        query_parts.append((k, item))
                else:
                    query_parts.append((k, v))
            full_url = base_url + "?" + urlencode(query_parts)

            response_lines = []
            success = False

            for attempt in range(CC_RETRY_MAX):
                try:
                    with urlopen(full_url, timeout=CC_REQUEST_TIMEOUT) as resp:
                        response_lines = resp.read().decode("utf-8").strip().splitlines()
                    success = True
                    break

                except HTTPError as e:
                    if e.code == 404:
                        # Domain simply not in this crawl — not an error, stop quietly
                        return hits
                    if e.code in (400, 504):
                        # 400 = bad request (pattern CC can't handle)
                        # 504 = gateway timeout (query too broad — skip immediately, no retry)
                        reason = f"HTTP {e.code} — {'bad request pattern' if e.code == 400 else 'gateway timeout (query too broad)'}"
                        log.warning(f"  Skipping {url_pattern[:60]} — {reason}")
                        _log_failed_query(url_pattern, crawl_id, page, reason, e.code)
                        return hits  # skip this pattern entirely
                    log.warning(f"  HTTP {e.code} attempt {attempt+1}: {url_pattern[:60]}")
                    _log_failed_query(url_pattern, crawl_id, page, f"HTTP {e.code}", e.code)
                    if attempt < CC_RETRY_MAX - 1:
                        time.sleep(CC_RETRY_BACKOFF ** attempt)

                except http.client.RemoteDisconnected:
                    # CC drops connection for domains not in this crawl — treat as 404
                    log.debug(f"  Not in crawl (RemoteDisconnected): {url_pattern[:60]}")
                    return hits

                except http.client.IncompleteRead as e:
                    # Server cut the connection mid-stream — save what we got
                    partial = e.partial.decode("utf-8", errors="replace")
                    response_lines = partial.strip().splitlines()
                    reason = f"IncompleteRead after {len(e.partial)} bytes on page {page}"
                    log.warning(f"  {reason} — saving partial results for {url_pattern[:60]}")
                    _log_failed_query(url_pattern, crawl_id, page, reason)
                    success = True   # treat as partial success — use what we have
                    break

                except URLError as e:
                    reason = f"URLError: {e.reason}"
                    log.warning(f"  {reason} attempt {attempt+1}: {url_pattern[:60]}")
                    _log_failed_query(url_pattern, crawl_id, page, reason)
                    if attempt < CC_RETRY_MAX - 1:
                        time.sleep(CC_RETRY_BACKOFF ** attempt)

                except Exception as e:
                    reason = f"Unexpected error: {type(e).__name__}: {e}"
                    log.warning(f"  {reason} — skipping {url_pattern[:60]}")
                    _log_failed_query(url_pattern, crawl_id, page, reason)
                    return hits  # don't retry unknown errors

            if not success:
                log.error(f"  All retries exhausted for {url_pattern[:60]} page {page} — moving on")
                break

            if not response_lines:
                break  # No more results

            page_hits = []
            for line in response_lines:
                line = line.strip()
                if not line:
                    continue
                raw_f.write(line + "\n")
                try:
                    obj = json.loads(line)
                    page_hits.append(obj)
                except json.JSONDecodeError:
                    log.debug(f"  Could not parse line: {line[:80]}")

            hits.extend(page_hits)

            if len(page_hits) < CC_INDEX_PAGE_SIZE:
                break  # Last page

            if len(hits) >= _hit_cap:
                log.debug(f"  Hit cap reached ({_hit_cap}) for {url_pattern[:60]} — stopping pagination")
                break

            page += 1
            time.sleep(CC_RATE_LIMIT_SLEEP)

    return hits


# =============================================================================
# Per query spec row — resolve domains and run queries
# =============================================================================

def load_raw_jsonl(raw_path: Path) -> list[dict]:
    """Replay hits from an existing raw JSONL file. Returns [] if absent/empty."""
    if not raw_path.exists() or raw_path.stat().st_size == 0:
        return []
    hits = []
    with open(raw_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    hits.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return hits


def process_query_spec_row(
    spec_row:              pd.Series,
    domain_list:           dict,
    national_scope_domains: set | None = None,
) -> list[dict]:
    """
    For one row from event_query_specs, determine what URL patterns to query
    and run the CC index queries. Returns list of enriched hit dicts.
    national_scope_domains: set of domains that get MAX_DOMAIN_HITS_NATIONAL cap.
    """
    if national_scope_domains is None:
        national_scope_domains = set()
    flood_id   = int(spec_row["flood_id"])
    query_id   = spec_row["query_id"]
    crawl_id   = spec_row["crawl_id"]
    from_ts    = ts_to_cc_format(spec_row["window_start"])
    to_ts      = ts_to_cc_format(spec_row["window_end"])
    strategy   = spec_row["retrieval_strategy"]
    iso        = _get_iso_for_flood(flood_id)

    # Determine URL patterns to query
    url_patterns = []

    if "restricted" in strategy:
        # Domain-restricted: query each domain from source_domain_list
        country_entry = domain_list.get(iso, {})
        all_domains = (
            country_entry.get("news_domains", []) +
            country_entry.get("gov_domains",  []) +
            country_entry.get("org_domains",  [])
        )
        for domain in all_domains:
            # Use domain/* — more universally accepted by CC CDX API than *.domain
            # *.domain returns HTTP 400 for many org/international domains (unicef, ifrc etc.)
            url_patterns.append((f"{domain}/*", domain, "restricted"))

        if not url_patterns:
            log.warning(f"  No domains found for ISO={iso}, falling back to open-web")
            strategy = "open"

    if "open" in strategy or not url_patterns:
        # Open-web: use specific international flood/news/humanitarian domains only
        # Never use ccTLD wildcards (e.g. *.id/*) — too broad, causes 504 timeouts
        for fallback_domain in [
            "reliefweb.int",
            "floodlist.com",
            "ifrc.org",
            "unocha.org",
            "preventionweb.net",
            "disasteralert.pfh.org",
            "gdacs.org",
        ]:
            url_patterns.append((f"{fallback_domain}/*", fallback_domain, "open"))

    # Run queries and collect hits
    all_hits = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for url_pattern, domain_slug, filter_type in url_patterns:
        raw_path = raw_response_path(flood_id, crawl_id, domain_slug)
        # Apply tighter cap for national/international scope domains — they
        # generate thousands of hits but near-zero relevance for county events.
        # Open-web fallback domains are exempt: they're the only source for
        # events with no restricted domain list, so cap them at the default.
        domain_hit_cap = (
            MAX_DOMAIN_HITS_NATIONAL
            if filter_type == "restricted" and domain_slug in national_scope_domains
            else MAX_DOMAIN_HITS
        )

        cached = load_raw_jsonl(raw_path)
        if cached:
            log.debug(f"  Resume: {len(cached)} hits from cache — {raw_path.name}")
            raw_hits = cached
        else:
            raw_hits = query_cc_index(
                crawl_id=crawl_id,
                url_pattern=url_pattern,
                from_ts=from_ts,
                to_ts=to_ts,
                raw_save_path=raw_path,
                hit_cap=domain_hit_cap,
            )
            time.sleep(CC_RATE_LIMIT_SLEEP)  # only sleep on actual network requests

        for hit in raw_hits:
            all_hits.append({
                "hit_id":             str(uuid.uuid4()),
                "flood_id":           flood_id,
                "query_id":           query_id,
                "crawl_id":           crawl_id,
                "url":                hit.get("url", ""),
                "timestamp":          hit.get("timestamp", ""),
                "status":             hit.get("status", ""),
                "mime":               hit.get("mime", ""),
                "filename":           hit.get("filename", ""),
                "offset":             hit.get("offset", ""),
                "length":             hit.get("length", ""),
                "digest":             hit.get("digest", ""),
                "domain":             extract_domain(hit.get("url", "")),
                "retrieval_strategy": filter_type,
                "fetched_at":         fetched_at,
            })

    return all_hits


# =============================================================================
# ISO / TLD lookup helpers (built from query specs + flood CSV at runtime)
# =============================================================================

_flood_iso_map: dict[int, str] = {}
_iso_tld_map: dict[str, str] = {
    # Common ccTLDs for flood countries — extend as needed
    "SYR": "sy",  "IDN": "id",  "COL": "co",  "USA": "us",
    "IRN": "ir",  "COD": "cd",  "GMB": "gm",  "IND": "in",
    "BGD": "bd",  "PAK": "pk",  "PHL": "ph",  "NGA": "ng",
    "BRA": "br",  "CHN": "cn",  "JPN": "jp",  "KOR": "kr",
    "THA": "th",  "VNM": "vn",  "MYS": "my",  "KHM": "kh",
    "MMR": "mm",  "LAO": "la",  "AFG": "af",  "IRQ": "iq",
    "TUN": "tn",  "MAR": "ma",  "DZA": "dz",  "EGY": "eg",
    "ETH": "et",  "KEN": "ke",  "TZA": "tz",  "UGA": "ug",
    "ZMB": "zm",  "ZWE": "zw",  "MOZ": "mz",  "MWI": "mw",
    "SDN": "sd",  "SOM": "so",  "CMR": "cm",  "GHA": "gh",
    "SLE": "sl",  "LBR": "lr",  "MLI": "ml",  "NER": "ne",
    "TCD": "td",  "CAF": "cf",  "NAM": "na",  "BWA": "bw",
    "MDG": "mg",  "HTI": "ht",  "BOL": "bo",  "PER": "pe",
    "ECU": "ec",  "HND": "hn",  "GTM": "gt",  "NIC": "ni",
    "BGR": "bg",  "UKR": "ua",  "GEO": "ge",  "NPL": "np",
}


def _get_iso_for_flood(flood_id: int) -> str:
    return _flood_iso_map.get(flood_id, "")


def _get_tld_for_iso(iso: str) -> str:
    return _iso_tld_map.get(iso.upper(), "")


def _build_flood_iso_map(specs_df: pd.DataFrame, flood_csv_path: Path) -> None:
    """Populate _flood_iso_map from the flood CSV."""
    global _flood_iso_map
    try:
        pass
    except Exception:
        pass
    flood_df = pd.read_csv(flood_csv_path)
    _flood_iso_map = dict(zip(
        flood_df["Flood_ID"].astype(int),
        flood_df["ISO"].astype(str)
    ))


# =============================================================================
# Spot-check summary
# =============================================================================

def print_hit_summary(hits_df: pd.DataFrame) -> None:
    log.info("--- Pilot spot-check: hit counts per event / variant ---")
    if hits_df.empty:
        log.warning("No hits at all — investigate before proceeding to Stage 3")
        return

    # Extract variant from query_id  e.g. "3_C" -> "C"
    hits_df = hits_df.copy()
    hits_df["variant"] = hits_df["query_id"].str.extract(r"_([A-D])$")

    summary = (
        hits_df.groupby(["flood_id", "variant", "crawl_id"])
        .size()
        .reset_index(name="hit_count")
        .sort_values(["flood_id", "variant"])
    )

    for _, row in summary.iterrows():
        log.info(
            f"  Flood #{int(row['flood_id']):>3}  variant={row['variant']}  "
            f"crawl={row['crawl_id']}  hits={row['hit_count']}"
        )

    # Flag zero-hit events
    total_per_event = hits_df.groupby("flood_id").size()
    zero_hit = [fid for fid, cnt in total_per_event.items() if cnt == 0]
    if zero_hit:
        log.warning(f"Zero-hit events — investigate before Stage 3: {zero_hit}")
    else:
        log.info("All events have at least 1 hit [OK]")

    return summary


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 02 — Query CC index")
    parser.add_argument("--all",      action="store_true", help="Process all events (Phase 2)")
    parser.add_argument("--flood-id", type=int,            help="Process a single flood_id only (debug)")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("STAGE 02 — QUERY CC INDEX")
    mode = f"flood_id={args.flood_id}" if args.flood_id else ("ALL" if args.all else "PILOT")
    log.info(f"Mode : {mode}")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------
    specs_path = OUTPUT_DIR / "event_query_specs.parquet"
    if not specs_path.exists():
        log.error("event_query_specs.parquet not found — run stage_01 first")
        sys.exit(1)

    specs_df = pd.read_parquet(specs_path)
    domain_list = load_domain_list()
    national_scope_domains = load_national_scope_domains()
    log.info(f"National-scope domains loaded: {len(national_scope_domains)} (cap={MAX_DOMAIN_HITS_NATIONAL})")

    # Build flood -> ISO map
    from config import FLOOD_CSV
    _build_flood_iso_map(specs_df, FLOOD_CSV)

    # ------------------------------------------------------------------
    # Filter specs to process
    # ------------------------------------------------------------------
    if args.flood_id:
        specs_df = specs_df[specs_df["flood_id"] == args.flood_id]
    elif PILOT_FLOOD_IDS and not args.all:
        specs_df = specs_df[specs_df["flood_id"].isin(PILOT_FLOOD_IDS)]

    # For pilot: only use primary queries (variant C) for the spot-check
    # to keep request volume low. Run A/B/D after confirming C has hits.
    primary_specs = specs_df[specs_df["query_id"].str.endswith("_C")]
    secondary_specs = specs_df[~specs_df["query_id"].str.endswith("_C")]

    log.info(f"Primary (C) specs  : {len(primary_specs)}")
    log.info(f"Secondary specs    : {len(secondary_specs)}")
    log.info(f"Total specs        : {len(specs_df)}")

    # ------------------------------------------------------------------
    # Run queries — primary first, then secondary
    # ------------------------------------------------------------------
    all_hits = []

    for phase_label, phase_df in [("PRIMARY (C)", primary_specs), ("SECONDARY (A/B/D)", secondary_specs)]:
        if phase_df.empty:
            continue
        log.info(f"--- Running {phase_label} queries ({CC_INDEX_WORKERS} workers) ---")

        with ThreadPoolExecutor(max_workers=CC_INDEX_WORKERS) as executor:
            futures = {
                executor.submit(process_query_spec_row, row, domain_list, national_scope_domains): row
                for _, row in phase_df.iterrows()
            }
            for future in as_completed(futures):
                row = futures[future]
                try:
                    hits = future.result()
                    all_hits.extend(hits)
                    log.info(
                        f"  flood #{int(row['flood_id']):>3}  {row['query_id']}"
                        f"  crawl={row['crawl_id']}  -> {len(hits)} hits"
                    )
                except Exception as exc:
                    log.error(f"  flood #{int(row['flood_id'])} {row['query_id']} failed: {exc}")

        # After primary phase: spot-check before running secondary
        if phase_label.startswith("PRIMARY") and all_hits:
            interim_df = pd.DataFrame(all_hits)
            print_hit_summary(interim_df)

            zero_events = set(specs_df["flood_id"].unique()) - set(interim_df["flood_id"].unique())
            if zero_events:
                log.warning(
                    f"Events with 0 hits after primary queries: {zero_events}\n"
                    f"  -> Check time windows and CC coverage before continuing.\n"
                    f"  -> Consider switching to open-web strategy for these events."
                )

    # ------------------------------------------------------------------
    # Assemble and save outputs
    # ------------------------------------------------------------------
    if not all_hits:
        log.error("No hits collected. Check query specs and CC coverage.")
        sys.exit(1)

    hits_df = pd.DataFrame(all_hits)

    # Type coercions
    hits_df["flood_id"] = hits_df["flood_id"].astype(int)
    hits_df["offset"]   = pd.to_numeric(hits_df["offset"],  errors="coerce")
    hits_df["length"]   = pd.to_numeric(hits_df["length"],  errors="coerce")

    # ------------------------------------------------------------------
    # Additive merge on natural key — preserves existing hit_ids
    #
    # WHY NOT partition-replacement:
    #   hit_id is a fresh uuid4() every run. Replacing a flood's partition
    #   would invalidate all of stage_04's on-disk WARC cache (keyed by
    #   pointer_id, which derives from hit_id), forcing a full re-download
    #   of files already on disk.
    #
    # WHY natural key works:
    #   (flood_id, crawl_id, filename, offset, length) is stable — it
    #   identifies the same WARC byte range across runs regardless of
    #   when the query ran. New domains add genuinely new rows.
    #   Re-queried old domains return the same offsets, which dedup out,
    #   preserving their original hit_ids and stage_04 cache hits.
    # ------------------------------------------------------------------
    hits_path = OUTPUT_DIR / "cc_index_hits.parquet"
    NATURAL_KEY = ["flood_id", "crawl_id", "filename", "offset", "length"]

    if hits_path.exists():
        existing_df = pd.read_parquet(hits_path)
        existing_df["flood_id"] = existing_df["flood_id"].astype(int)

        before   = len(existing_df)
        combined = pd.concat([existing_df, hits_df], ignore_index=True)
        # Keep the FIRST occurrence on the natural key — that's the existing
        # row with its original hit_id. New-domain rows that aren't already
        # present are appended and kept.
        combined  = combined.drop_duplicates(subset=NATURAL_KEY, keep="first")
        new_count = len(combined) - before

        log.info(
            f"Additive merge: {before} existing + {len(hits_df)} fetched "
            f"-> {new_count} genuinely new rows added  ({len(combined)} total)"
        )
        hits_df = combined
    else:
        log.info("No existing cc_index_hits.parquet — creating fresh file")

    hits_df.to_parquet(hits_path, index=False)
    log.info(f"Saved cc_index_hits -> {hits_path}  ({len(hits_df)} rows total)")

    # Save hit count summary
    hits_df["variant"] = hits_df["query_id"].str.extract(r"_([A-D])$")
    summary_df = (
        hits_df.groupby(["flood_id", "query_id", "variant", "crawl_id"])
        .size()
        .reset_index(name="hit_count")
    )
    summary_path = OUTPUT_DIR / "hit_count_summary.parquet"
    summary_df.to_parquet(summary_path, index=False)
    log.info(f"Saved hit_count_summary -> {summary_path}")

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    log.info("=" * 70)
    log.info(f"Total hits collected  : {len(hits_df)}")
    log.info(f"Unique events         : {hits_df['flood_id'].nunique()}")
    log.info(f"Unique URLs           : {hits_df['url'].nunique()}")
    log.info(f"Raw responses saved to: {RAW_INDEX_DIR}/")
    log.info("Next: run stage_03_validate_pointers.py")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
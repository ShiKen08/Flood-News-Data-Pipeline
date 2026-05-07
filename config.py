# =============================================================================
# config.py  ·  Flood Data Pipeline — Central Configuration
# =============================================================================
# All pipeline scripts import from here.
# Never hard-code paths, thresholds, or rule versions in pipeline scripts.
# =============================================================================

from pathlib import Path

# -----------------------------------------------------------------------------
# VERSIONING
# -----------------------------------------------------------------------------

WINDOW_RULE_VERSION = "v2"          # v2: WINDOW_POST_DAYS 14→45, WINDOW_POST_LONG_DAYS 5→21


# -----------------------------------------------------------------------------
# PATHS
# -----------------------------------------------------------------------------

BASE_DIR          = Path(__file__).parent                  # commoncrawl/
INPUT_DIR         = BASE_DIR                               # flood_crawl.csv lives here
COLLINFO_PATH     = BASE_DIR / "collinfo.json"             # cached CC crawl listing
CONFIG_DIR        = BASE_DIR / "config"                    # keyword lexicon, domain list
CACHE_DIR         = BASE_DIR / "cache"                     # local WARC slice cache
RAW_INDEX_DIR     = BASE_DIR / "raw_index_responses"       # Stage 2 raw CC index saves
OUTPUT_DIR        = BASE_DIR / "output"                    # all output tables (Parquet)
LOGS_DIR          = BASE_DIR / "logs"                      # pipeline run logs
DATA_DIR          = BASE_DIR / "data"                      # for any other data files (e.g. flood_crawl.csv copy)
FLOOD_CRAWL_CSV   = DATA_DIR / "flood_crawl.csv"
FLOOD_CSV         = FLOOD_CRAWL_CSV              # alias used by stage scripts


# -----------------------------------------------------------------------------
# CSV COLUMN NAMES  (actual names in flood_crawl.csv)
# -----------------------------------------------------------------------------

COL_FLOOD_ID   = "Flood_ID"
COL_ISO        = "ISO"
COL_COUNTRY    = "Country"
COL_LOCATION   = "Location"
COL_START_DATE = "Start Date"       # note: space not underscore
COL_END_DATE   = "End Date"         # note: space not underscore
COL_DURATION   = "Duration"         # note: not Duration_Days
COL_LANGUAGE   = "Language_ISO_639_3"
COL_LOCAL_LANG = "Local_Languages"  # human-readable language names

KEYWORD_LEXICON    = CONFIG_DIR / "keyword_lexicon.json"
SOURCE_DOMAIN_LIST = CONFIG_DIR / "source_domain_list.json"

# Create dirs if they don't exist yet (safe to call on every import)
for _dir in [CONFIG_DIR, CACHE_DIR, RAW_INDEX_DIR, OUTPUT_DIR, LOGS_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# COMMON CRAWL
# -----------------------------------------------------------------------------

CC_COLLINFO_URL   = "https://index.commoncrawl.org/collinfo.json"
CC_INDEX_URL      = "https://index.commoncrawl.org/{crawl_id}-index"   # format with crawl_id
CC_DATA_URL       = "https://data.commoncrawl.org/{filename}"           # format with filename

CC_INDEX_PARAMS = {
    "output": "json",
    "status": "200",
    "mime":   "text/html",
}


# -----------------------------------------------------------------------------
# WINDOWING RULES  (Stage 0b — version v2)
# -----------------------------------------------------------------------------

WINDOW_PRE_DAYS          = 5    # event_start − 5 days
WINDOW_POST_DAYS         = 45   # event_end   + 45 days (covers ~2 CC monthly crawl cycles of post-event reporting)
WINDOW_POST_LONG_DAYS    = 21   # cap for events with duration > 20 days (was 5 — too tight for follow-up coverage)
WINDOW_LONG_DURATION_THRESHOLD = 20
ZERO_DURATION_TREAT_AS_DAYS    = 1    # treat 0-duration events as 1-day event
NEAREST_CRAWL_MAX_GAP_DAYS = 120  # fallback: assign nearest post-window crawl if within this many days

# Events with duration = 0 are treated as 1-day events (do NOT collapse window)
# Pilot runs primary variant (C) only — set to False to include A/B/D
PILOT_PRIMARY_ONLY   = True


# -----------------------------------------------------------------------------
# LANGUAGE TIERS  (Stage 0c)
# -----------------------------------------------------------------------------

# Tier 1 — high CC coverage; query in full for every event that lists these codes
TIER_1_LANGUAGES = {
    "arb",   # Arabic (Modern Standard)
    "cmn",   # Mandarin Chinese (Simplified)
    "yue",   # Cantonese Chinese
    "eng",   # English
    "fra",   # French
    "spa",   # Spanish
    "por",   # Portuguese
    "deu",   # German
    "ita",   # Italian
    "ind",   # Indonesian
    "zlm",   # Malay
    "hin",   # Hindi
    "ben",   # Bengali
    "urd",   # Urdu
    "tha",   # Thai
    "kor",   # Korean
    "jpn",   # Japanese
    "vie",   # Vietnamese
    "fas",   # Persian (Farsi)
    "prs",   # Dari (Persian dialect — Afghanistan)
}

# Tier 2 — moderate CC coverage; include if explicitly listed for the event
TIER_2_LANGUAGES = {
    "npi",   # Nepali
    "bul",   # Bulgarian
    "ukr",   # Ukrainian
    "kat",   # Georgian
    "khm",   # Khmer
    "lao",   # Lao
    "mya",   # Burmese (Myanmar)
    "tgl",   # Filipino/Tagalog
    "pus",   # Pashto (Afghanistan)
    "ckb",   # Sorani Kurdish (Iraq)
    "sna",   # Shona (Zimbabwe)
    "ber",   # Berber/Tamazight (Morocco)
    "afr",   # Afrikaans
    "ron",   # Romanian
    "srp",   # Serbian
    "bos",   # Bosnian
    "hrv",   # Croatian
    "hat",   # Haitian Creole (moderate CC; fra fallback also used)
}

# Tier 3 — very low CC coverage; use French or English fallback instead
TIER_3_LANGUAGES = {
    "lin",   # Lingala       (DRC)
    "swa",   # Swahili       (East/Central Africa)
    "kon",   # Kongo         (DRC)
    "lua",   # Luba-Kasai    (DRC)
    "sag",   # Sango         (CAR)
    "men",   # Mende         (Sierra Leone)
    "tem",   # Temne         (Sierra Leone)
    "mnk",   # Mandinka      (Gambia)
    "wol",   # Wolof         (Gambia/Senegal)
    "ayu",   # Aymara        (Bolivia)
    "que",   # Quechua       (Bolivia/Peru)
    "hau",   # Hausa         (Nigeria/Niger)
    "ibo",   # Igbo          (Nigeria)
    "yor",   # Yoruba        (Nigeria)
    "lug",   # Luganda       (Uganda)
    "mlg",   # Malagasy      (Madagascar)
    "nya",   # Chichewa/Nyanja (Malawi/Zambia)
    "som",   # Somali        (Somalia)
    "tsn",   # Tswana        (Botswana/South Africa)
    "xho",   # Xhosa         (South Africa)
    "zul",   # Zulu          (South Africa)
}

# Fallback languages to use in place of Tier 3
TIER_3_FALLBACK_LANGUAGES = {"fra", "eng"}


# -----------------------------------------------------------------------------
# PILOT EVENTS  (Phase 1 — run ONLY these 7 flood IDs)
# -----------------------------------------------------------------------------

PILOT_FLOOD_IDS = list(range(7, 21))   # floods 1–20; set to None to process all events

# Crawls returning 403 — not yet fully public, exclude from downloads
BLOCKED_CRAWLS = []

# -----------------------------------------------------------------------------
# POINTER VALIDATION THRESHOLDS  (Stage 3)
# -----------------------------------------------------------------------------

POINTER_MIN_BYTES     = 500         # below → TOO_SMALL → rejects table
POINTER_MAX_BYTES     = 5_000_000   # above → TOO_LARGE → separate review


# -----------------------------------------------------------------------------
# TEXT QUALITY THRESHOLDS  (Stage 6)
# -----------------------------------------------------------------------------

MIN_CHAR_COUNT        = 200         # shorter docs → is_usable = FALSE
MAX_NON_ASCII_RATIO   = 0.4         # above → likely encoding failure → is_usable = FALSE

ERROR_PAGE_PATTERNS   = [           # page_title matches → is_usable = FALSE
    "404",
    "403",
    "Access Denied",
    "Error",
]


# -----------------------------------------------------------------------------
# DOWNLOAD / RETRY SETTINGS  (Stage 4)
# -----------------------------------------------------------------------------

MAX_RETRIES           = 3
RETRY_BACKOFF_BASE           = 5    # seconds; wait = 5^attempt → 5s, 25s, 125s
DOWNLOAD_INTER_REQUEST_SLEEP = 1.0  # seconds between every WARC request — prevents rate limiting
BATCH_SIZE            = 200000      # max downloads per event per run
DOWNLOAD_SUCCESS_RATE_FLOOR = 0.80  # pause and investigate if rate drops below this

EXTRACT_WORKERS     = 8    # parallel threads for Stage 5 text extraction (bumped from 4)
LANG_DETECT_WORKERS = 16   # parallel threads for Stage 6 language detection (bumped from 8)
CC_INDEX_WORKERS    = 3    # parallel spec-row workers for Stage 2 CDX queries


# -----------------------------------------------------------------------------
# FUNNEL METRIC THRESHOLDS  (Pilot Quality Review)
# -----------------------------------------------------------------------------

FUNNEL_THRESHOLDS = {
    "pointer_hit_rate":         0.0,    # expect > 0 for all pilot events
    "download_success_rate":    0.85,
    "extraction_success_rate":  0.80,
    "usable_doc_rate":          0.50,
    "language_match_rate":      0.60,
    "post_dedup_retention":     0.30,   # investigate if below this
}


# -----------------------------------------------------------------------------
# OUTPUT TABLE COLUMN SCHEMAS  (Stage 0d — collinfo schema)
# -----------------------------------------------------------------------------

# Documenting expected columns per output table so every stage is aligned.

SCHEMA_EVENT_QUERY_SPECS = [
    "query_id",               # {Flood_ID}_{variant}  — NEVER {ISO}_{variant}
    "flood_id",               # integer join key
    "crawl_id",
    "query_text",
    "query_language_codes",   # list of codes actually queried
    "query_language_skipped", # dict of {code: reason} for dropped languages
    "domain_filter",          # 'restricted' | 'open'
    "window_start",
    "window_end",
    "window_rule_version",    # always WINDOW_RULE_VERSION
    "retrieval_strategy",
    "created_at",
]

SCHEMA_VALIDATED_POINTERS = [
    "pointer_id",
    "flood_id",
    "query_id",
    "crawl_id",
    "url",
    "filename",
    "offset",
    "length",
    "digest",
    "timestamp",
    "retrieval_strategy",
    "retrieval_rank",
    "is_pointer_duplicate",
    "cross_event_shared",
    "size_filter_status",     # VALID | TOO_SMALL | TOO_LARGE
    "status",                 # VALID | REJECTED
    "reject_reason",
]

SCHEMA_WARC_FETCH_LOG = [
    "pointer_id",
    "flood_id",
    "download_success",
    "http_status",
    "bytes_received",
    "bytes_expected",
    "bytes_match",
    "error_type",
    "error_message",
    "retry_count",
    "local_cache_path",
    "fetched_at",
]

SCHEMA_EXTRACTED_TEXT = [
    "doc_id",                 # surrogate key — generated at Stage 5
    "pointer_id",
    "flood_id",
    "page_title",
    "meta_description",
    "raw_text",
    "extraction_success",
    "extraction_error",
    "encoding_detected",
]

SCHEMA_MAIN_DATA = [
    # Identifiers & provenance
    "doc_id", "flood_id", "query_id", "crawl_id",
    "url", "domain", "timestamp",
    "warc_filename", "warc_offset", "warc_length",
    "retrieval_strategy",
    # Content
    "page_title", "clean_text", "text_hash", "char_count", "word_count",
    # Quality flags
    "extraction_success", "extraction_error",
    "language_detected", "language_confidence", "language_match",
    "is_usable", "is_content_duplicate", "duplicate_group_id", "cross_event_shared",
    # Relevance signals
    "is_relevant", "is_soft_relevant", "is_event_article",
    "flood_term_hits", "impact_term_hits", "location_term_hits", "subnational_hits",
    # Reproducibility
    "window_rule_version",
]
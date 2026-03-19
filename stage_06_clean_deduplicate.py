# =============================================================================
# stage_06_clean_deduplicate.py  ·  Flood Data Pipeline — Clean, Detect, Dedup
# =============================================================================
# Covers checklist Stages 6 and 7
#
# Stage 6 — Text cleaning + language detection:
#   - Normalise whitespace, preserve paragraph breaks
#   - Strip boilerplate remnants
#   - Compute char_count, word_count, non_ascii_ratio
#   - Set is_usable = FALSE for short/empty/garbled/error docs
#   - Detect language on clean_text, check against event's query_language_codes
#
# Stage 7 — Content deduplication:
#   - SHA256 hash of clean_text within each flood_id
#   - Flag exact duplicates, keep canonical (earliest timestamp)
#   - Check duplicate rate per event — warn if > 70%
#
# Stage 6b — Relevance filter (keyword match on actual text):
#   - Score clean_text against event's flood keywords + location names
#   - Set is_relevant = TRUE/FALSE
#   - is_usable docs that fail relevance check are kept but flagged
#
# Reads:
#   output/extracted_text.parquet
#   output/validated_pointers.parquet      (for timestamps)
#   output/event_query_specs.parquet       (for query_language_codes + query_text)
#   output/language_assignments.parquet    (for per-event language codes)
#   output/location_dictionary.parquet     (for location terms)
#   config/keyword_lexicon.json
#
# Outputs:
#   output/clean_text.parquet
#   output/rejects.parquet                 (appended with new rejects)
#
# Run:
#   python stage_06_clean_deduplicate.py
#   python stage_06_clean_deduplicate.py --flood-id 3
# =============================================================================

import argparse
import hashlib
import importlib.util
import json
import logging
import re
import sys
import uuid
from pathlib import Path

import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

try:
    import langid
    langid.set_languages(None)  # full model, no restriction
    LANGID_AVAILABLE = True
except ImportError:
    LANGID_AVAILABLE = False
    print("WARNING: langid not installed. Run: pip install langid")
    print("Language detection will be skipped.")

# ---------------------------------------------------------------------------
# Force-load local config.py
# ---------------------------------------------------------------------------
_config_path = Path(__file__).parent / "config.py"
_spec = importlib.util.spec_from_file_location("config", _config_path)
_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)
sys.modules["config"] = _config

from config import (
    ERROR_PAGE_PATTERNS,
    KEYWORD_LEXICON,
    LOGS_DIR,
    MAX_NON_ASCII_RATIO,
    MIN_CHAR_COUNT,
    OUTPUT_DIR,
    PILOT_FLOOD_IDS,
    TIER_3_FALLBACK_LANGUAGES,
    TIER_3_LANGUAGES,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_06_clean_deduplicate.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Langdetect ISO 639-1 -> ISO 639-3 mapping (for languages in our dataset)
# ---------------------------------------------------------------------------
LANG_1_TO_3 = {
    "ar": "arb", "zh-cn": "cmn", "zh-tw": "cmn", "zh": "cmn",
    "en": "eng", "fr": "fra", "es": "spa", "pt": "por",
    "id": "ind", "ms": "zlm", "hi": "hin", "bn": "ben",
    "ur": "urd", "th": "tha", "ko": "kor", "ja": "jpn",
    "vi": "vie", "fa": "fas", "de": "deu", "it": "ita",
    "ne": "npi", "bg": "bul", "uk": "ukr", "ka": "kat",
    "km": "khm", "lo": "lao", "my": "mya", "tl": "tgl",
    "ps": "pus", "ro": "ron", "sr": "srp", "hr": "hrv",
    "bs": "bos", "af": "afr", "sw": "swa", "ha": "hau",
    "ig": "ibo", "yo": "yor", "so": "som",
}


def lang1_to_lang3(code: str) -> str:
    return LANG_1_TO_3.get(code.lower(), code.lower())


# =============================================================================
# STAGE 6 — Text cleaning
# =============================================================================

BOILERPLATE_REMNANTS = [
    re.compile(p, re.I) for p in [
        r"^subscribe\b",
        r"^sign up\b",
        r"^newsletter\b",
        r"^follow us\b",
        r"^share this\b",
        r"^click here\b",
        r"^read more\b",
        r"^advertisement\b",
        r"^loading\.\.\.",
        r"^cookie",
        r"accept cookies",
    ]
]


# =============================================================================
# TAG / INDEX / ARCHIVE PAGE FILTER
# =============================================================================
# These are hard gates applied BEFORE usability checks.
# Tag pages, archive pages, homepages and search result pages contain flood
# keywords in aggregated headlines but are not articles — they must never
# appear in clean_text.parquet.

# URL path patterns that indicate non-article pages
_TAG_URL_PATTERNS = re.compile(
    r"""
    /tag/           |   # detik.com/tag/banjir
    /tags/          |   # various
    /tag$           |   # bare /tag
    /tags$          |
    /kategori/      |   # Indonesian category pages
    /category/      |   # English category pages
    /categories/    |
    /topic/         |   # topic aggregation
    /topics/        |
    /etiqueta/      |   # Spanish tag
    /rubrique/      |   # French category
    /rubric/        |
    /section/       |   # section index
    /sections/      |
    /page/\d+       |   # pagination: /page/3
    [_-]page[_-]\d+ |   # pagination: -page-3
    [?&]page=\d+    |   # query param: ?page=2
    [?&]p=\d+       |   # WordPress pagination
    /\d{4}/\d{2}/$  |   # year/month archive: /2024/10/
    /\d{4}/$        |   # year archive: /2024/
    /archive/       |   # archive index
    /archives/      |
    /search/        |   # search results
    /buscar/        |   # Spanish search
    /recherche/     |   # French search
    /suche/         |   # German search
    /pencarian/     |   # Indonesian search
    [?&]q=          |   # search query param
    [?&]s=          |   # WordPress search
    [?&]query=      |   # generic search param
    [?&]keyword=    |   # keyword search param
    ;jsessionid=    |   # Java session ID URLs (library/portal systems, not articles)
    /opac/          |   # OPAC library catalogue pages
    /BrowseThesaurus    # thesaurus browser pages
    """,
    re.VERBOSE | re.IGNORECASE,
)

# URL paths that are just the bare homepage (path is / or empty)
_HOMEPAGE_PATTERN = re.compile(r"^https?://[^/]+/?$")

# Title patterns that indicate non-article pages
_TAG_TITLE_PATTERNS = [
    # Pagination suffixes
    re.compile(r"\bpage\s+\d+\b", re.I),
    re.compile(r"halaman\s+\d+", re.I),           # Indonesian "page N"
    re.compile(r"—\s*page\s+\d+", re.I),
    re.compile(r"-\s*page\s+\d+$", re.I),
    # Date archive titles: "October 14, 2025 - SiteName" or "January 2026 Archives"
    re.compile(r"^[A-Za-z]+ \d{1,2},?\s+\d{4}\s*[-—]", re.I),
    re.compile(r"^\d{4}\s+archives?\b", re.I),
    re.compile(r"\barchives?\s+\d{4}", re.I),
    # Tag/topic page titles
    re.compile(r"\ball articles tagged\b", re.I),
    re.compile(r"\btag:\s*", re.I),
    re.compile(r"berita terbaru .+ hari ini$", re.I),     # Indonesian tag page formula
    re.compile(r"berita dan informasi .+ terkini$", re.I), # Indonesian tag page formula
    re.compile(r"berita tentang .+ terkini", re.I),
    re.compile(r"informasi .+ terkini dan terbaru", re.I),
    # Site name only (homepage titles)
    re.compile(r"^[\w\s\.\-]+\s*[-—]\s*(breaking news|home|homepage|accueil|inicio|beranda)$", re.I),
]


def is_index_or_tag_page(url: str, page_title: str) -> tuple[bool, str]:
    """
    Returns (True, reason) if the page is a tag/index/archive/homepage.
    Returns (False, '') if it looks like a genuine article.

    Applies URL-based checks first (fast, no text needed), then title checks.
    """
    url_str   = str(url or "")
    title_str = str(page_title or "")

    # 1. Bare homepage
    if url_str and _HOMEPAGE_PATTERN.match(url_str):
        return True, "homepage"

    # 2. URL path patterns
    if url_str and _TAG_URL_PATTERNS.search(url_str):
        return True, f"tag/index URL pattern: {url_str[:80]}"

    # 3. Title patterns
    for pat in _TAG_TITLE_PATTERNS:
        if pat.search(title_str):
            return True, f"tag/index title pattern: {title_str[:80]}"

    return False, ""


def _content_index_signals(clean_text_val: str, word_count: int, flood_hits: int) -> dict:
    """
    Soft signals (not hard gates) that suggest index/aggregation pages.
    Returns a dict of flags for metadata — does not reject documents.
    """
    lines = clean_text_val.splitlines() if clean_text_val else []
    short_lines = sum(1 for l in lines if 0 < len(l.split()) < 15)
    has_long_sentence = any(
        len(s.split()) >= 30
        for s in re.split(r'[.!?]\s+', clean_text_val or "")
    )
    return {
        "signal_many_short_lines":  short_lines > 20,
        "signal_no_long_sentence":  not has_long_sentence,
        "signal_large_low_flood":   word_count > 5000 and flood_hits < 3,
    }


def clean_text(raw_text: str) -> str:
    """
    Clean raw_text:
    - Collapse repeated spaces within lines
    - Collapse 3+ consecutive newlines to 2 (preserve paragraph breaks)
    - Strip boilerplate remnant lines
    """
    if not raw_text:
        return ""

    lines = raw_text.splitlines()
    clean_lines = []
    for line in lines:
        # Collapse repeated spaces
        line = re.sub(r" {2,}", " ", line).strip()
        if not line:
            clean_lines.append("")
            continue
        # Drop boilerplate remnant lines
        if any(p.match(line) for p in BOILERPLATE_REMNANTS):
            continue
        clean_lines.append(line)

    # Collapse 3+ consecutive blank lines to 2
    text = "\n".join(clean_lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def compute_metrics(text: str) -> tuple[int, int, float]:
    """Returns (char_count, word_count, non_ascii_ratio)."""
    if not text:
        return 0, 0, 0.0
    char_count = len(text)
    word_count = len(text.split())
    non_ascii  = sum(1 for c in text if ord(c) > 127)
    non_ascii_ratio = non_ascii / char_count if char_count > 0 else 0.0
    return char_count, word_count, non_ascii_ratio


def check_usability(
    clean_text_val: str,
    char_count:     int,
    non_ascii_ratio: float,
    page_title:     str,
) -> tuple[bool, str]:
    """
    Returns (is_usable, reason).
    Checks all four disqualifying conditions from the checklist.
    """
    if not clean_text_val or char_count == 0:
        return False, "empty text"
    if char_count < MIN_CHAR_COUNT:
        return False, f"char_count={char_count} < {MIN_CHAR_COUNT}"
    if non_ascii_ratio > MAX_NON_ASCII_RATIO:
        return False, f"non_ascii_ratio={non_ascii_ratio:.2f} > {MAX_NON_ASCII_RATIO}"
    title_lower = (page_title or "").lower()
    for pattern in ERROR_PAGE_PATTERNS:
        if pattern.lower() in title_lower:
            return False, f"error page title: {pattern}"
    return True, ""


# =============================================================================
# STAGE 6 — Language detection
# =============================================================================

def detect_language(text: str) -> tuple[str, float]:
    """
    Detect language using langid.
    Returns (iso_639_3_code, confidence_0_to_1).

    langid is stateless and thread-safe — no deadlock risk, no randomness,
    ~10x faster than langdetect. The confidence score is a log-probability
    normalised to 0–1 range.
    """
    if not LANGID_AVAILABLE or not text or len(text) < 50:
        return "unknown", 0.0
    try:
        lang, score = langid.classify(text[:3000])
        # langid returns a log-probability (negative). Normalise to 0–1
        # using a sigmoid-style squash so downstream code gets a usable number.
        import math
        confidence = round(1.0 / (1.0 + math.exp(-score / 50)), 3)
        code3 = lang1_to_lang3(lang)
        return code3, confidence
    except Exception:
        return "unknown", 0.0


def check_language_match(
    detected_lang: str,
    flood_id:      int,
    lang_df:       pd.DataFrame,
) -> bool:
    """
    Returns True if detected_lang is in the event's query_language_codes,
    or if it's a Tier 3 fallback (fra/eng) for events that used fallback.
    """
    rows = lang_df[lang_df["flood_id"] == flood_id]
    if rows.empty:
        return False

    query_codes = json.loads(rows.iloc[0]["query_language_codes"])
    skipped     = json.loads(rows.iloc[0]["query_language_skipped"])

    # Direct match
    if detected_lang in query_codes:
        return True

    # Accept fra/eng fallback for Tier 3 events
    had_tier3 = any(
        code in TIER_3_LANGUAGES
        for code in (skipped if isinstance(skipped, list) else skipped.keys())
    )
    if had_tier3 and detected_lang in TIER_3_FALLBACK_LANGUAGES:
        return True

    return False


# =============================================================================
# STAGE 6b — Relevance filter
# =============================================================================

def build_relevance_terms(
    flood_id:  int,
    lang_df:   pd.DataFrame,
    loc_df:    pd.DataFrame,
    lexicon:   dict,
) -> tuple[list[str], list[str]]:
    """
    Build flood keyword list and location term list for a given flood_id.
    Returns (flood_terms, location_terms) — all lowercased.
    """
    # Language codes for this event
    lang_rows = lang_df[lang_df["flood_id"] == flood_id]
    query_codes = json.loads(lang_rows.iloc[0]["query_language_codes"]) if not lang_rows.empty else []

    # Flood terms from lexicon
    flood_terms = []
    for lang in query_codes:
        entry = lexicon.get(lang, {})
        flood_terms.extend(entry.get("flood", []))
        flood_terms.extend(entry.get("river", []))

    # Location terms
    loc_rows = loc_df[loc_df["flood_id"] == flood_id]
    location_terms = []
    for _, row in loc_rows.iterrows():
        location_terms.append(row["location_normalised"])
        aliases = json.loads(row.get("aliases", "[]") or "[]")
        location_terms.extend([a.lower() for a in aliases])

    flood_terms_deduped    = list(dict.fromkeys(t.lower() for t in flood_terms))
    location_terms_deduped = list(dict.fromkeys(t.lower() for t in location_terms))
    return flood_terms_deduped, location_terms_deduped


def _make_word_pattern(term: str) -> re.Pattern:
    """
    Compile a word-boundary regex for a term.
    Uses \b for Latin scripts, falls back to lookaround for non-Latin scripts
    (Arabic, Chinese, Thai etc.) where \b does not work reliably.
    """
    escaped = re.escape(term)
    try:
        if term.isascii():
            return re.compile(r'\b' + escaped + r'\b', re.I | re.UNICODE)
        else:
            return re.compile(
                r'(?<![^\s\.,;:!?\-\(\)\[\]])' + escaped +
                r'(?![^\s\.,;:!?\-\(\)\[\]])',
                re.UNICODE
            )
    except re.error:
        return re.compile(escaped, re.I | re.UNICODE)


_pattern_cache: dict[str, re.Pattern] = {}


def _term_matches(term: str, text: str) -> bool:
    """Check if term appears as a whole word in text."""
    if term not in _pattern_cache:
        _pattern_cache[term] = _make_word_pattern(term)
    return bool(_pattern_cache[term].search(text))


def score_relevance(
    text:              str,
    flood_terms:       list[str],
    location_terms:    list[str],
    loc_df_rows:       pd.DataFrame = None,
) -> dict:
    """
    Score a document for relevance using word-boundary matching.

    Returns a dict with:
      is_relevant          — True if flood_hits >= 2 AND loc_hits >= 1
      flood_mentioned      — True if flood_hits >= 1 (softer signal, kept but labelled)
      flood_term_hits      — count of distinct flood terms matched
      location_term_hits   — count of distinct location terms matched (subnational + country)
      subnational_hits     — location hits that are NOT country-level only
      location_specificity_score — subnational_hits / location_term_hits (0.0 if no loc hits)
      low_specificity      — True if only country name matched (no subnational hit)

    Threshold design:
      - flood_hits >= 2 required for is_relevant (single mention too weak)
      - loc_hits >= 1 required — article must mention somewhere in the event region
      - Falls back to flood_hits >= 2 only if event has no location terms at all
    """
    if not text:
        return {
            "is_relevant": False, "flood_mentioned": False,
            "flood_term_hits": 0, "location_term_hits": 0,
            "subnational_hits": 0, "location_specificity_score": 0.0,
            "low_specificity": False,
        }

    text_lower = text.lower()

    flood_hits = sum(1 for term in flood_terms if _term_matches(term, text_lower))
    loc_hits   = sum(1 for term in location_terms if _term_matches(term, text_lower))

    # Distinguish country-level vs subnational location hits
    # loc_df_rows has a 'level' column: 'country' or 'subnational'
    subnational_hits = 0
    if loc_df_rows is not None and not loc_df_rows.empty:
        for _, loc_row in loc_df_rows.iterrows():
            level   = str(loc_row.get("level", "subnational")).lower()
            term    = str(loc_row.get("location_normalised", "")).lower()
            aliases = json.loads(loc_row.get("aliases", "[]") or "[]")
            all_loc_terms = [term] + [a.lower() for a in aliases]
            matched = any(_term_matches(t, text_lower) for t in all_loc_terms if t)
            if matched and level != "country":
                subnational_hits += 1

    specificity = (subnational_hits / loc_hits) if loc_hits > 0 else 0.0
    low_spec    = loc_hits > 0 and subnational_hits == 0

    # Relevance gate
    has_loc_terms = len(location_terms) > 0
    if has_loc_terms:
        is_relevant = flood_hits >= 2 and loc_hits >= 1
    else:
        # No location terms available for this event — fall back to flood-only
        is_relevant = flood_hits >= 2

    flood_mentioned = flood_hits >= 1

    return {
        "is_relevant":                is_relevant,
        "flood_mentioned":            flood_mentioned,
        "flood_term_hits":            flood_hits,
        "location_term_hits":         loc_hits,
        "subnational_hits":           subnational_hits,
        "location_specificity_score": round(specificity, 3),
        "low_specificity":            low_spec,
    }


# =============================================================================
# STAGE 7 — Content deduplication
# =============================================================================

def deduplicate_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    Within each flood_id:
    - Compute SHA256 of clean_text
    - Flag duplicates (keep earliest timestamp as canonical)
    - Assign duplicate_group_id
    """
    log.info("--- Stage 7: Content deduplication ---")

    df = df.copy()
    df["text_hash"]           = df["clean_text"].apply(
        lambda t: hashlib.sha256(t.encode("utf-8", errors="replace")).hexdigest() if t else ""
    )
    df["is_content_duplicate"] = False
    df["duplicate_group_id"]   = ""

    total_dupes = 0

    for flood_id, group in df.groupby("flood_id"):
        # Sort by timestamp so earliest is canonical
        group_sorted = group.sort_values("timestamp", na_position="last")

        # Find duplicate text_hashes (exclude empty hashes)
        valid_hashes = group_sorted[group_sorted["text_hash"] != ""]
        dup_mask = valid_hashes.duplicated(subset=["text_hash"], keep="first")
        dup_indices = valid_hashes[dup_mask].index

        # Assign duplicate_group_id to all members of each duplicate group
        hash_to_group = {}
        for idx, row in valid_hashes.iterrows():
            h = row["text_hash"]
            if h not in hash_to_group:
                hash_to_group[h] = str(uuid.uuid4())
            df.at[idx, "duplicate_group_id"] = hash_to_group[h]

        df.loc[dup_indices, "is_content_duplicate"] = True
        total_dupes += len(dup_indices)

        dup_rate = len(dup_indices) / len(group) if len(group) > 0 else 0
        flag = " ⚠ INVESTIGATE" if dup_rate > 0.70 else ""
        log.info(
            f"  Flood #{int(flood_id):>3}  total={len(group):>5}  "
            f"dupes={len(dup_indices):>4}  rate={dup_rate:.1%}{flag}"
        )

    log.info(f"  Total content duplicates flagged: {total_dupes}")
    return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 06 — Clean, detect language, deduplicate")
    parser.add_argument("--all",              action="store_true", help="All events (Phase 2)")
    parser.add_argument("--flood-id",         type=int,            help="Single flood_id (debug)")
    parser.add_argument("--compare-variants", action="store_true", help="Log A/B/C/D variant breakdown per event")
    parser.add_argument("--fresh",            action="store_true", help="Ignore any existing checkpoint and start from scratch")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("STAGE 06 — CLEAN TEXT + LANGUAGE DETECTION + DEDUP + RELEVANCE")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Checkpoint paths
    # ------------------------------------------------------------------
    CHECKPOINT_EVERY        = 5000   # flush to disk every N processed rows
    ckpt_clean_path         = OUTPUT_DIR / "stage06_checkpoint_clean.parquet"
    ckpt_reject_path        = OUTPUT_DIR / "stage06_checkpoint_rejects.parquet"
    ckpt_progress_path      = OUTPUT_DIR / "stage06_checkpoint_progress.json"

    def save_checkpoint(clean_rows, reject_rows, last_doc_id):
        """Flush in-progress results to checkpoint files."""
        if clean_rows:
            pd.DataFrame(clean_rows).to_parquet(ckpt_clean_path, index=False)
        if reject_rows:
            pd.DataFrame(reject_rows).to_parquet(ckpt_reject_path, index=False)
        import json as _json
        ckpt_progress_path.write_text(_json.dumps({"last_doc_id": str(last_doc_id), "clean": len(clean_rows), "rejected": len(reject_rows)}))
        log.info(f"  [OK] Checkpoint saved — clean={len(clean_rows)}  rejected={len(reject_rows)}  last_doc_id={last_doc_id}")

    def clear_checkpoint():
        for p in (ckpt_clean_path, ckpt_reject_path, ckpt_progress_path):
            if p.exists():
                p.unlink()

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------
    extracted_df = pd.read_parquet(OUTPUT_DIR / "extracted_text.parquet")
    pointers_df  = pd.read_parquet(OUTPUT_DIR / "validated_pointers.parquet")
    lang_df      = pd.read_parquet(OUTPUT_DIR / "language_assignments.parquet")
    loc_df       = pd.read_parquet(OUTPUT_DIR / "location_dictionary.parquet")

    with open(KEYWORD_LEXICON) as f:
        lexicon = json.load(f)

    # Load event windows for pub_date filtering
    specs_path = OUTPUT_DIR / "event_query_specs.parquet"
    if specs_path.exists():
        query_specs   = pd.read_parquet(specs_path)
        event_windows = (
            query_specs[["flood_id", "window_start", "window_end"]]
            .drop_duplicates("flood_id")
            .set_index("flood_id")
        )
    else:
        log.warning("event_query_specs.parquet not found — pub_date window filter disabled")
        event_windows = pd.DataFrame()

    # Join timestamp AND url from validated_pointers
    pointer_meta = pointers_df[["pointer_id", "timestamp", "url"]].drop_duplicates("pointer_id")
    extracted_df = extracted_df.merge(pointer_meta, on="pointer_id", how="left")

    if args.flood_id:
        extracted_df = extracted_df[extracted_df["flood_id"] == args.flood_id]
    elif not args.all:
        extracted_df = extracted_df[extracted_df["flood_id"].isin(PILOT_FLOOD_IDS)]

    log.info(f"Docs to process : {len(extracted_df)}")

    # Pre-build relevance term lists once per flood_id — identical for every
    # doc in the same event, no need to rebuild 218k times inside the loop.
    log.info("  Pre-building relevance term cache...")
    relevance_cache: dict[int, tuple[list, list]] = {}
    for fid in extracted_df["flood_id"].unique():
        relevance_cache[int(fid)] = build_relevance_terms(int(fid), lang_df, loc_df, lexicon)
    log.info(f"  Cached terms for {len(relevance_cache)} event(s)")

    # ------------------------------------------------------------------
    # Resume from checkpoint if one exists
    # ------------------------------------------------------------------
    clean_rows:  list = []
    reject_rows: list = []
    already_processed: set = set()

    if not args.fresh and ckpt_progress_path.exists():
        try:
            import json as _json
            progress = _json.loads(ckpt_progress_path.read_text())
            if ckpt_clean_path.exists():
                ckpt_clean  = pd.read_parquet(ckpt_clean_path)
                clean_rows  = ckpt_clean.to_dict("records")
                already_processed.update(str(r["doc_id"]) for r in clean_rows)
            if ckpt_reject_path.exists():
                ckpt_reject  = pd.read_parquet(ckpt_reject_path)
                reject_rows  = ckpt_reject.to_dict("records")
                already_processed.update(str(r["doc_id"]) for r in reject_rows if "doc_id" in r)
            log.info(
                f"  Resuming from checkpoint — "
                f"clean={len(clean_rows)}  rejected={len(reject_rows)}  "
                f"already_processed={len(already_processed)}"
            )
        except Exception as e:
            log.warning(f"  Checkpoint load failed ({e}) — starting from scratch")
            clean_rows, reject_rows, already_processed = [], [], set()
    elif args.fresh:
        log.info("  --fresh flag set — ignoring any existing checkpoint")
        clear_checkpoint()

    # ------------------------------------------------------------------
    # Signal handler — save checkpoint then exit cleanly on Ctrl+C / kill
    # ------------------------------------------------------------------
    import signal

    def _handle_shutdown(signum, frame):
        log.info("Shutdown signal received — saving checkpoint and exiting...")
        save_checkpoint(clean_rows, reject_rows, "interrupted")
        log.info("Checkpoint saved. Re-run without --fresh to resume.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    # ------------------------------------------------------------------
    # Stage 6 — Clean text + usability + language + relevance
    # ------------------------------------------------------------------
    log.info("--- Stage 6: Clean + usability + tag filter + language + relevance ---")

    for i, (_, row) in enumerate(extracted_df.iterrows(), 1):
        doc_id     = str(row.get("doc_id", ""))
        flood_id   = int(row["flood_id"])
        raw_text   = str(row.get("raw_text", "") or "")
        page_title = str(row.get("page_title", "") or "")
        url        = str(row.get("url", "") or "")
        pub_date   = str(row.get("pub_date", "") or "")

        # ── Skip already processed (resume after checkpoint) ─────────
        if doc_id and doc_id in already_processed:
            continue

        # ── Skip if extraction already failed ────────────────────────
        if not row.get("extraction_success", False):
            reject_rows.append({**row.to_dict(), "reject_reason": "extraction_failed"})
            continue

        # ── Hard gate 1: tag/index/archive/homepage filter ───────────
        is_index, index_reason = is_index_or_tag_page(url, page_title)
        if is_index:
            reject_rows.append({
                **row.to_dict(),
                "reject_reason": f"tag_or_index_page: {index_reason[:120]}"
            })
            continue

        # ── Clean text ───────────────────────────────────────────────
        cleaned = clean_text(raw_text)
        char_count, word_count, non_ascii_ratio = compute_metrics(cleaned)

        # ── Hard gate 2: usability check ─────────────────────────────
        is_usable, usable_reason = check_usability(
            cleaned, char_count, non_ascii_ratio, page_title
        )
        if not is_usable:
            reject_rows.append({**row.to_dict(), "reject_reason": usable_reason})
            continue

        # ── Language detection ────────────────────────────────────────
        lang_detected, lang_confidence = detect_language(cleaned)
        lang_match = check_language_match(lang_detected, flood_id, lang_df)

        # ── Relevance scoring ─────────────────────────────────────────
        flood_terms, loc_terms = relevance_cache.get(flood_id, ([], []))
        loc_df_rows = loc_df[loc_df["flood_id"] == flood_id]
        rel_scores  = score_relevance(cleaned, flood_terms, loc_terms, loc_df_rows)

        flood_hits = rel_scores["flood_term_hits"]
        loc_hits   = rel_scores["location_term_hits"]

        # ── Hard gate 3: location hit required ───────────────────────
        # Must mention at least one location term from the event.
        # Skip gate if event has no location terms defined.
        has_loc_terms = len(loc_terms) > 0
        if has_loc_terms and loc_hits == 0:
            reject_rows.append({
                **row.to_dict(),
                "reject_reason": "no_location_match",
                "clean_text":    cleaned,
                "char_count":    char_count,
                "word_count":    word_count,
            })
            continue

        # ── Hard gate 4: at least 1 flood term required ──────────────
        # Docs with 0 flood hits are noise (directories, election results,
        # patent pages etc. that mention a location name incidentally).
        if flood_hits == 0:
            reject_rows.append({
                **row.to_dict(),
                "reject_reason": "no_flood_term_match",
                "clean_text":    cleaned,
                "char_count":    char_count,
                "word_count":    word_count,
            })
            continue

        # ── Pub date window check ─────────────────────────────────────
        pub_in_window = None  # None = unknown (no pub_date extracted)
        if pub_date:
            try:
                from datetime import date as _date
                pub_d = _date.fromisoformat(pub_date)
                if flood_id in event_windows.index:
                    win       = event_windows.loc[flood_id]
                    win_start = pd.Timestamp(win["window_start"]).date()
                    win_end   = pd.Timestamp(win["window_end"]).date()
                    pub_in_window = (win_start <= pub_d <= win_end)
            except Exception:
                pub_in_window = None

        # Hard gate: confirmed out-of-window publication date
        if pub_in_window is False:
            reject_rows.append({
                **row.to_dict(),
                "reject_reason":   "pub_date_out_of_window",
                "clean_text":      cleaned,
                "char_count":      char_count,
                "word_count":      word_count,
                "non_ascii_ratio": round(non_ascii_ratio, 4),
                "pub_date":        pub_date,
                "pub_in_window":   pub_in_window,
            })
            continue

        # ── Content index signals (soft flags — not gates) ────────────
        content_signals = _content_index_signals(cleaned, word_count, flood_hits)

        # ── Assemble document ─────────────────────────────────────────
        doc = {
            **row.to_dict(),
            "clean_text":                  cleaned,
            "char_count":                  char_count,
            "word_count":                  word_count,
            "non_ascii_ratio":             round(non_ascii_ratio, 4),
            "is_usable":                   True,
            "usable_reason":               "",
            "language_detected":           lang_detected,
            "language_confidence":         lang_confidence,
            "language_match":              lang_match,
            "pub_date":                    pub_date,
            "pub_in_window":               pub_in_window,
            **rel_scores,
            **content_signals,
        }
        clean_rows.append(doc)

        # ── Periodic checkpoint ───────────────────────────────────────
        total_processed = len(clean_rows) + len(reject_rows)
        if total_processed % CHECKPOINT_EVERY == 0:
            save_checkpoint(clean_rows, reject_rows, doc_id)

        if i % 500 == 0 or i == len(extracted_df):
            log.info(
                f"  Progress: {i}/{len(extracted_df)}"
                f"  kept={len(clean_rows)}"
                f"  rejected={len(reject_rows)}"
            )

    # ------------------------------------------------------------------
    # Stage 7 — Content deduplication
    # ------------------------------------------------------------------
    clean_df = pd.DataFrame(clean_rows)
    clean_df = deduplicate_content(clean_df)

    # ------------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------------
    out_path = OUTPUT_DIR / "clean_text.parquet"
    clean_df.to_parquet(out_path, index=False)
    log.info(f"Saved clean_text -> {out_path}  ({len(clean_df)} rows)")

    # Clear checkpoint — run completed successfully
    clear_checkpoint()
    log.info("  Checkpoint files cleared (run completed successfully)")

    # Append to rejects
    if reject_rows:
        reject_df = pd.DataFrame(reject_rows)
        existing_rejects_path = OUTPUT_DIR / "rejects.parquet"
        if existing_rejects_path.exists():
            existing = pd.read_parquet(existing_rejects_path)
            current_flood_ids = reject_df["flood_id"].unique()
            existing = existing[~existing["flood_id"].isin(current_flood_ids)]
            reject_df = pd.concat([existing, reject_df], ignore_index=True)
        reject_df.to_parquet(existing_rejects_path, index=False)
        log.info(f"Saved rejects -> {existing_rejects_path}  ({len(reject_df)} total rows)")

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    total       = len(extracted_df)
    kept        = len(clean_df)
    rejected    = len(reject_rows)
    kept_rate   = kept / total if total > 0 else 0

    # Reject breakdown
    rej_df = pd.DataFrame(reject_rows) if reject_rows else pd.DataFrame()
    def _rej_count(reason):
        if rej_df.empty: return 0
        return (rej_df.get("reject_reason", pd.Series()).str.startswith(reason, na=False)).sum()

    n_extraction_failed  = _rej_count("extraction_failed")
    n_tag_index          = _rej_count("tag_or_index_page")
    n_no_loc             = _rej_count("no_location_match")
    n_no_flood           = _rej_count("no_flood_term_match")
    n_oot                = _rej_count("pub_date_out_of_window")
    n_usability          = rejected - n_extraction_failed - n_tag_index - n_no_loc - n_no_flood - n_oot

    log.info("=" * 70)
    log.info(f"Total docs processed       : {total}")
    log.info(f"Kept in clean_text         : {kept}  ({kept_rate:.1%})")
    log.info(f"Rejected total             : {rejected}")
    log.info(f"  extraction_failed        : {n_extraction_failed}")
    log.info(f"  tag_or_index_page        : {n_tag_index}")
    log.info(f"  usability (short/garbled): {max(n_usability, 0)}")
    log.info(f"  no_location_match        : {n_no_loc}")
    log.info(f"  no_flood_term_match      : {n_no_flood}")
    log.info(f"  pub_date_out_of_window   : {n_oot}")
    log.info(f"Content duplicates flagged : {clean_df['is_content_duplicate'].sum() if not clean_df.empty else 0}")
    log.info("")

    if not clean_df.empty:
        log.info("Per-event breakdown:")
        for fid, grp in clean_df.groupby("flood_id"):
            n           = len(grp)
            rel         = grp["is_relevant"].sum()
            mentioned   = grp["flood_mentioned"].sum() if "flood_mentioned" in grp else rel
            lm          = grp["language_match"].sum()
            pub_known   = grp["pub_in_window"].notna().sum() if "pub_in_window" in grp else 0
            pub_ok      = (grp["pub_in_window"] == True).sum() if "pub_in_window" in grp else 0
            low_spec    = grp["low_specificity"].sum() if "low_specificity" in grp else 0
            avg_spec    = grp["location_specificity_score"].mean() if "location_specificity_score" in grp else 0
            log.info(
                f"  Flood #{int(fid):>3}"
                f"  docs={n:>5}"
                f"  relevant={rel:>4} ({rel/n:.0%})"
                f"  mentioned={mentioned:>4}"
                f"  lang_match={lm:>4}"
                f"  pub_dated={pub_known}/{n}"
                f"  pub_ok={pub_ok}"
                f"  low_spec={low_spec}"
                f"  avg_specificity={avg_spec:.2f}"
            )

        if args.compare_variants:
            log.info("")
            log.info("=== VARIANT COMPARISON ===")
            log.info(f"  Variant A (flood_mentioned=True)   : {clean_df['flood_mentioned'].sum()} / {len(clean_df)}")
            log.info(f"  Variant C (is_relevant=True)       : {clean_df['is_relevant'].sum()} / {len(clean_df)}")
            log.info(f"  low_specificity docs               : {clean_df['low_specificity'].sum()} / {len(clean_df)}")
            if "signal_many_short_lines" in clean_df:
                log.info(f"  signal_many_short_lines            : {clean_df['signal_many_short_lines'].sum()}")
                log.info(f"  signal_no_long_sentence            : {clean_df['signal_no_long_sentence'].sum()}")
                log.info(f"  signal_large_low_flood             : {clean_df['signal_large_low_flood'].sum()}")

    log.info("")
    log.info("Next: review clean_text.parquet for quality, then proceed to Phase 2")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
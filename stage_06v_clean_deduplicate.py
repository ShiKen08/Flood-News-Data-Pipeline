# =============================================================================
# stage_06_clean_deduplicate.py  ·  Flood Data Pipeline — Clean, Detect, Dedup
# =============================================================================
# Rewritten with vectorised architecture for speed.
#
# Architecture:
#   Step 1  — Pre-filter (vectorised pandas):
#               tag/index URL + title patterns, extraction failures
#   Step 2  — Text cleaning (apply on survivors only):
#               whitespace normalise, boilerplate strip
#   Step 3  — Usability filter (vectorised):
#               char_count, non_ascii_ratio, error page titles
#   Step 4  — Language detection (batch, ThreadPoolExecutor with langid):
#               all surviving clean texts in parallel
#   Step 5  — Relevance scoring (per flood_id group, pre-compiled regex):
#               flood term hits, location hits, specificity score
#   Step 6  — Pub date window filter (vectorised)
#   Step 7  — Content deduplication (SHA256 per flood_id group)
#
# Checkpoint/resume:
#   Saves checkpoint before dedup step.
#   Re-run same command after a crash to resume automatically.
#   Use --fresh to ignore checkpoint and start over.
#
# Reads:
#   output/extracted_text.parquet
#   output/validated_pointers.parquet      (for timestamps + url)
#   output/event_query_specs.parquet       (for event windows)
#   output/language_assignments.parquet    (for per-event language codes)
#   output/location_dictionary.parquet     (for location terms)
#   config/keyword_lexicon.json
#
# Outputs:
#   output/clean_text.parquet
#   output/rejects.parquet                 (appended, with reject_reason)
#
# Run:
#   python stage_06_clean_deduplicate.py                     # pilot events
#   python stage_06_clean_deduplicate.py --flood-id 126      # single event
#   python stage_06_clean_deduplicate.py --all               # Phase 2
#   python stage_06_clean_deduplicate.py --fresh             # ignore checkpoint
#   python stage_06_clean_deduplicate.py --compare-variants  # extra stats
# =============================================================================

import argparse
import hashlib
import importlib.util
import json
import logging
import math
import os
import re
import signal
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date
from pathlib import Path

import numpy as np
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

# ---------------------------------------------------------------------------
# Language detection — langid (stateless, no deadlock, ~1k docs/s per thread)
# ---------------------------------------------------------------------------
try:
    import langid
    langid.set_languages(None)
    LANGID_AVAILABLE = True
except ImportError:
    LANGID_AVAILABLE = False
    print("WARNING: langid not installed. Run: pip install langid")

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
# Constants
# ---------------------------------------------------------------------------
LANG_DETECT_WORKERS = 8      # parallel threads for language detection
LANG_DETECT_CHARS   = 3000   # chars fed to langid per document

# ---------------------------------------------------------------------------
# ISO 639-1 to ISO 639-3 mapping
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
# STEP 1b — Tag / index / archive / homepage filter
# =============================================================================

_TAG_URL_RE = re.compile(
    r"/tag/|/tags/|/tag$|/tags$"
    r"|/kategori/|/category/|/categories/"
    r"|/topic/|/topics/"
    r"|/etiqueta/|/rubrique/|/rubric/"
    r"|/section/|/sections/"
    r"|/page/\d+|[_-]page[_-]\d+|[?&]page=\d+|[?&]p=\d+"
    r"|/\d{4}/\d{2}/$|/\d{4}/$"
    r"|/archive/|/archives/"
    r"|/search/|/buscar/|/recherche/|/suche/|/pencarian/"
    r"|[?&]q=|[?&]s=|[?&]query=|[?&]keyword="
    r"|;jsessionid=|/opac/|/BrowseThesaurus",
    re.IGNORECASE,
)

_HOMEPAGE_RE = re.compile(r"^https?://[^/]+/?$")

_TAG_TITLE_RES = [
    re.compile(r"\bpage\s+\d+\b",                              re.I),
    re.compile(r"halaman\s+\d+",                               re.I),
    re.compile(r"[=\-]\s*page\s+\d+",                         re.I),
    re.compile(r"^[A-Za-z]+ \d{1,2},?\s+\d{4}\s*[-=]",       re.I),
    re.compile(r"^\d{4}\s+archives?\b",                        re.I),
    re.compile(r"\barchives?\s+\d{4}",                         re.I),
    re.compile(r"\ball articles tagged\b",                     re.I),
    re.compile(r"\btag:\s*",                                   re.I),
    re.compile(r"berita terbaru .+ hari ini$",                 re.I),
    re.compile(r"berita dan informasi .+ terkini$",            re.I),
    re.compile(r"berita tentang .+ terkini",                   re.I),
    re.compile(r"informasi .+ terkini dan terbaru",            re.I),
    re.compile(
        r"^[\w\s\.\-]+\s*[-=]\s*(breaking news|home|homepage|accueil|inicio|beranda)$",
        re.I,
    ),
]


def _is_tag_url(url: str) -> bool:
    if not url:
        return False
    if _HOMEPAGE_RE.match(url):
        return True
    return bool(_TAG_URL_RE.search(url))


def _is_tag_title(title: str) -> bool:
    if not title:
        return False
    return any(p.search(title) for p in _TAG_TITLE_RES)


def apply_tag_filter(df: pd.DataFrame) -> tuple:
    url_mask   = df["url"].fillna("").apply(_is_tag_url)
    title_mask = (~url_mask) & df["page_title"].fillna("").apply(_is_tag_title)
    tag_mask   = url_mask | title_mask
    rejects    = df[tag_mask].copy()
    rejects["reject_reason"] = "tag_or_index_page"
    return df[~tag_mask].copy(), rejects


# =============================================================================
# STEP 2 — Text cleaning
# =============================================================================

_BOILERPLATE_RES = [
    re.compile(p, re.I) for p in [
        r"^subscribe\b", r"^sign up\b", r"^newsletter\b", r"^follow us\b",
        r"^share this\b", r"^click here\b", r"^read more\b",
        r"^advertisement\b", r"^loading\.\.\.", r"^cookie", r"accept cookies",
    ]
]
_MULTI_SPACE_RE   = re.compile(r" {2,}")
_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")


def _clean_one(raw: str) -> str:
    if not raw:
        return ""
    lines = raw.splitlines()
    out = []
    for line in lines:
        line = _MULTI_SPACE_RE.sub(" ", line).strip()
        if not line:
            out.append("")
            continue
        if any(p.match(line) for p in _BOILERPLATE_RES):
            continue
        out.append(line)
    return _MULTI_NEWLINE_RE.sub("\n\n", "\n".join(out)).strip()


def clean_texts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["clean_text"]      = df["raw_text"].fillna("").apply(_clean_one)
    df["char_count"]      = df["clean_text"].str.len()
    df["word_count"]      = df["clean_text"].str.split().str.len().fillna(0).astype(int)
    df["non_ascii_ratio"] = df["clean_text"].apply(
        lambda t: round(sum(1 for c in t if ord(c) > 127) / max(len(t), 1), 4) if t else 0.0
    )
    return df


# =============================================================================
# STEP 3 — Usability filter (vectorised)
# =============================================================================

def apply_usability_filter(df: pd.DataFrame) -> tuple:
    empty_mask   = (df["char_count"] == 0) | df["clean_text"].isna()
    short_mask   = (~empty_mask) & (df["char_count"] < MIN_CHAR_COUNT)
    garble_mask  = (~empty_mask) & (df["non_ascii_ratio"] > MAX_NON_ASCII_RATIO)

    if ERROR_PAGE_PATTERNS:
        err_re   = re.compile("|".join(re.escape(p) for p in ERROR_PAGE_PATTERNS), re.I)
        err_mask = df["page_title"].fillna("").str.contains(err_re, regex=True)
    else:
        err_mask = pd.Series(False, index=df.index)

    fail_mask = empty_mask | short_mask | garble_mask | err_mask

    reasons = pd.Series("", index=df.index)
    reasons = reasons.where(~empty_mask,  "empty text")
    reasons = reasons.where(~short_mask,  "char_count_too_short")
    reasons = reasons.where(~garble_mask, "non_ascii_ratio_too_high")
    reasons = reasons.where(~err_mask,    "error_page_title")

    rejects = df[fail_mask].copy()
    rejects["reject_reason"] = reasons[fail_mask]
    return df[~fail_mask].copy(), rejects


# =============================================================================
# STEP 4 — Language detection (batch, parallel threads)
# =============================================================================

def _detect_one(text: str) -> tuple:
    if not LANGID_AVAILABLE or not text or len(text) < 50:
        return "unknown", 0.0
    try:
        lang, score = langid.classify(text[:LANG_DETECT_CHARS])
        confidence  = round(1.0 / (1.0 + math.exp(-score / 50)), 3)
        return lang1_to_lang3(lang), confidence
    except Exception:
        return "unknown", 0.0


def detect_languages_batch(texts: pd.Series) -> pd.DataFrame:
    log.info(f"  Language detection: {len(texts)} texts  workers={LANG_DETECT_WORKERS}")
    results: dict = {}
    with ThreadPoolExecutor(max_workers=LANG_DETECT_WORKERS) as pool:
        futures = {pool.submit(_detect_one, text): idx for idx, text in texts.items()}
        done = 0
        for future in as_completed(futures):
            idx          = futures[future]
            results[idx] = future.result()
            done += 1
            if done % 10_000 == 0:
                log.info(f"    lang detection: {done}/{len(texts)}")
    return pd.DataFrame({
        "language_detected":   pd.Series({i: v[0] for i, v in results.items()}),
        "language_confidence": pd.Series({i: v[1] for i, v in results.items()}),
    })


def build_lang_match(df: pd.DataFrame, lang_df: pd.DataFrame) -> pd.Series:
    accepted: dict = {}
    for _, row in lang_df.iterrows():
        fid     = int(row["flood_id"])
        codes   = set(json.loads(row["query_language_codes"]))
        skipped = json.loads(row["query_language_skipped"])
        skipped_keys = skipped if isinstance(skipped, list) else list(skipped.keys())
        if any(c in TIER_3_LANGUAGES for c in skipped_keys):
            codes |= set(TIER_3_FALLBACK_LANGUAGES)
        accepted[fid] = codes

    return df.apply(
        lambda r: r["language_detected"] in accepted.get(int(r["flood_id"]), set()),
        axis=1,
    )


# =============================================================================
# STEP 5 — Relevance scoring
# =============================================================================

_PATTERN_CACHE: dict = {}

def _get_pattern(term: str) -> re.Pattern:
    if term not in _PATTERN_CACHE:
        escaped = re.escape(term)
        if term.isascii():
            _PATTERN_CACHE[term] = re.compile(r"\b" + escaped + r"\b", re.I | re.UNICODE)
        else:
            _PATTERN_CACHE[term] = re.compile(
                r"(?<![^\s\.,;:!?\-\(\)\[\]])" + escaped +
                r"(?![^\s\.,;:!?\-\(\)\[\]])",
                re.UNICODE,
            )
    return _PATTERN_CACHE[term]


def build_relevance_terms(flood_id: int, lang_df: pd.DataFrame,
                          loc_df: pd.DataFrame, lexicon: dict) -> tuple:
    lang_rows   = lang_df[lang_df["flood_id"] == flood_id]
    query_codes = json.loads(lang_rows.iloc[0]["query_language_codes"]) if not lang_rows.empty else []

    flood_terms = []
    for lang in query_codes:
        entry = lexicon.get(lang, {})
        for cat in ("flood", "river", "disaster"):
            flood_terms.extend(entry.get(cat, []))
    flood_terms = list(dict.fromkeys(t.lower() for t in flood_terms))

    loc_entries = []
    for _, r in loc_df[loc_df["flood_id"] == flood_id].iterrows():
        norm    = str(r["location_normalised"]).lower()
        level   = str(r.get("level", "subnational")).lower()
        aliases = [a.lower() for a in json.loads(r.get("aliases", "[]") or "[]")]
        loc_entries.append((norm, level, aliases))

    return flood_terms, loc_entries


def _score_one(text_lower: str, flood_terms: list, loc_entries: list) -> dict:
    flood_hits       = sum(1 for t in flood_terms if _get_pattern(t).search(text_lower))
    loc_hits         = 0
    subnational_hits = 0
    for norm, level, aliases in loc_entries:
        all_terms = [norm] + aliases
        if any(_get_pattern(t).search(text_lower) for t in all_terms if t):
            loc_hits += 1
            if level != "country":
                subnational_hits += 1
    specificity = (subnational_hits / loc_hits) if loc_hits > 0 else 0.0
    has_locs    = bool(loc_entries)
    is_relevant = (flood_hits >= 2 and loc_hits >= 1) if has_locs else (flood_hits >= 2)
    return {
        "is_relevant":                is_relevant,
        "flood_mentioned":            flood_hits >= 1,
        "flood_term_hits":            flood_hits,
        "location_term_hits":         loc_hits,
        "subnational_hits":           subnational_hits,
        "location_specificity_score": round(specificity, 3),
        "low_specificity":            loc_hits > 0 and subnational_hits == 0,
    }


def score_relevance_all(df: pd.DataFrame, lang_df: pd.DataFrame,
                        loc_df: pd.DataFrame, lexicon: dict) -> pd.DataFrame:
    log.info(f"  Relevance scoring: {len(df)} docs")
    parts = []
    for flood_id, group in df.groupby("flood_id"):
        flood_terms, loc_entries = build_relevance_terms(int(flood_id), lang_df, loc_df, lexicon)
        scores = group["clean_text"].str.lower().apply(
            lambda t: _score_one(t, flood_terms, loc_entries)
        )
        scores_df = pd.DataFrame(scores.tolist(), index=group.index)
        parts.append(pd.concat([group, scores_df], axis=1))
        rel = scores_df["is_relevant"].sum()
        log.info(f"    flood #{int(flood_id):>3}  docs={len(group):>5}  relevant={rel:>4}")
    return pd.concat(parts, ignore_index=True) if parts else df.copy()


def add_content_signals(df: pd.DataFrame) -> pd.DataFrame:
    def _signals(row) -> pd.Series:
        text  = row["clean_text"] or ""
        lines = text.splitlines()
        short_lines = sum(1 for l in lines if 0 < len(l.split()) < 15)
        has_long    = any(len(s.split()) >= 30 for s in re.split(r"[.!?]\s+", text))
        return pd.Series({
            "signal_many_short_lines": short_lines > 20,
            "signal_no_long_sentence": not has_long,
            "signal_large_low_flood":  row["word_count"] > 5000 and row["flood_term_hits"] < 3,
        })
    return pd.concat([df, df.apply(_signals, axis=1)], axis=1)


# =============================================================================
# STEP 6 — Pub date window filter
# =============================================================================

def apply_pubdate_filter(df: pd.DataFrame, event_windows: pd.DataFrame) -> tuple:
    df = df.copy()
    if event_windows.empty:
        df["pub_in_window"] = None
        return df, pd.DataFrame()

    def _check(row):
        pub = str(row.get("pub_date", "") or "").strip()
        if not pub:
            return None
        fid = int(row["flood_id"])
        if fid not in event_windows.index:
            return None
        try:
            pub_d     = _date.fromisoformat(pub)
            win       = event_windows.loc[fid]
            win_start = pd.Timestamp(win["window_start"]).date()
            win_end   = pd.Timestamp(win["window_end"]).date()
            return win_start <= pub_d <= win_end
        except Exception:
            return None

    df["pub_in_window"] = df.apply(_check, axis=1)
    oot_mask  = df["pub_in_window"] == False   # noqa: E712 intentional
    rejects   = df[oot_mask].copy()
    rejects["reject_reason"] = "pub_date_out_of_window"
    return df[~oot_mask].copy(), rejects


# =============================================================================
# STEP 7 — Content deduplication
# =============================================================================

def deduplicate_content(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"--- Step 7: Content deduplication ({len(df)} docs) ---")
    df = df.copy()
    df["text_hash"]            = df["clean_text"].apply(
        lambda t: hashlib.sha256(t.encode("utf-8", errors="replace")).hexdigest() if t else ""
    )
    df["is_content_duplicate"] = False
    df["duplicate_group_id"]   = ""
    total_dupes = 0
    for flood_id, group in df.groupby("flood_id"):
        group_sorted = group.sort_values("timestamp", na_position="last")
        valid        = group_sorted[group_sorted["text_hash"] != ""]
        dup_mask     = valid.duplicated(subset=["text_hash"], keep="first")
        dup_indices  = valid[dup_mask].index
        hash_to_gid: dict = {}
        for idx, row in valid.iterrows():
            h = row["text_hash"]
            if h not in hash_to_gid:
                hash_to_gid[h] = str(uuid.uuid4())
            df.at[idx, "duplicate_group_id"] = hash_to_gid[h]
        df.loc[dup_indices, "is_content_duplicate"] = True
        total_dupes += len(dup_indices)
        dup_rate = len(dup_indices) / len(group) if len(group) > 0 else 0
        flag     = " ⚠ INVESTIGATE" if dup_rate > 0.70 else ""
        log.info(f"  flood #{int(flood_id):>3}  total={len(group):>5}  dupes={len(dup_indices):>4}  rate={dup_rate:.1%}{flag}")
    log.info(f"  Total duplicates flagged: {total_dupes}")
    return df


# =============================================================================
# Checkpoint helpers
# =============================================================================

CKPT_CLEAN    = OUTPUT_DIR / "stage06_ckpt_clean.parquet"
CKPT_REJECTS  = OUTPUT_DIR / "stage06_ckpt_rejects.parquet"
CKPT_PROGRESS = OUTPUT_DIR / "stage06_ckpt_progress.json"


def save_checkpoint(clean_df: pd.DataFrame, rejects_df: pd.DataFrame, note: str = "") -> None:
    if not clean_df.empty:
        clean_df.to_parquet(CKPT_CLEAN, index=False)
    if not rejects_df.empty:
        rejects_df.to_parquet(CKPT_REJECTS, index=False)
    CKPT_PROGRESS.write_text(json.dumps({"clean": len(clean_df), "rejected": len(rejects_df), "note": note}))
    log.info(f"  Checkpoint saved — clean={len(clean_df)}  rejected={len(rejects_df)}  {note}")


def load_checkpoint() -> tuple:
    clean_df   = pd.read_parquet(CKPT_CLEAN)   if CKPT_CLEAN.exists()   else pd.DataFrame()
    rejects_df = pd.read_parquet(CKPT_REJECTS) if CKPT_REJECTS.exists() else pd.DataFrame()
    processed  = set()
    if "doc_id" in clean_df.columns:
        processed.update(clean_df["doc_id"].astype(str))
    if "doc_id" in rejects_df.columns:
        processed.update(rejects_df["doc_id"].astype(str))
    log.info(f"  Resumed from checkpoint — clean={len(clean_df)}  rejected={len(rejects_df)}  already_done={len(processed)}")
    return clean_df, rejects_df, processed


def clear_checkpoint() -> None:
    for p in (CKPT_CLEAN, CKPT_REJECTS, CKPT_PROGRESS):
        if p.exists():
            p.unlink()


def save_rejects(new_rejects: pd.DataFrame) -> None:
    if new_rejects.empty:
        return
    path = OUTPUT_DIR / "rejects.parquet"
    if path.exists():
        existing        = pd.read_parquet(path)
        current_ids     = new_rejects["flood_id"].unique()
        existing        = existing[~existing["flood_id"].isin(current_ids)]
        new_rejects     = pd.concat([existing, new_rejects], ignore_index=True)
    new_rejects.to_parquet(path, index=False)
    log.info(f"Saved rejects -> {path}  ({len(new_rejects)} total rows)")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 06 — vectorised clean / detect / dedup")
    parser.add_argument("--all",              action="store_true")
    parser.add_argument("--flood-id",         type=int)
    parser.add_argument("--compare-variants", action="store_true")
    parser.add_argument("--fresh",            action="store_true", help="Ignore checkpoint, start from scratch")
    args = parser.parse_args()

    log.info("=" * 70)
    log.info("STAGE 06 — CLEAN / DETECT / DEDUPLICATE  (vectorised)")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Load inputs
    # ------------------------------------------------------------------
    log.info("Loading inputs...")
    extracted_df = pd.read_parquet(OUTPUT_DIR / "extracted_text.parquet")
    pointers_df  = pd.read_parquet(OUTPUT_DIR / "validated_pointers.parquet")
    lang_df      = pd.read_parquet(OUTPUT_DIR / "language_assignments.parquet")
    loc_df       = pd.read_parquet(OUTPUT_DIR / "location_dictionary.parquet")

    with open(KEYWORD_LEXICON) as f:
        lexicon = json.load(f)

    specs_path = OUTPUT_DIR / "event_query_specs.parquet"
    if specs_path.exists():
        query_specs   = pd.read_parquet(specs_path)
        event_windows = (
            query_specs[["flood_id", "window_start", "window_end"]]
            .drop_duplicates("flood_id").set_index("flood_id")
        )
    else:
        log.warning("event_query_specs.parquet not found — pub_date filter disabled")
        event_windows = pd.DataFrame()

    pointer_meta = pointers_df[["pointer_id", "timestamp", "url"]].drop_duplicates("pointer_id")
    extracted_df = extracted_df.merge(pointer_meta, on="pointer_id", how="left")

    if args.flood_id:
        extracted_df = extracted_df[extracted_df["flood_id"] == args.flood_id]
    elif not args.all:
        extracted_df = extracted_df[extracted_df["flood_id"].isin(PILOT_FLOOD_IDS)]

    log.info(f"Docs in scope: {len(extracted_df)}")

    # ------------------------------------------------------------------
    # Checkpoint / resume
    # ------------------------------------------------------------------
    prior_clean   = pd.DataFrame()
    prior_rejects = pd.DataFrame()

    if args.fresh:
        log.info("--fresh: ignoring any existing checkpoint")
        clear_checkpoint()
    elif CKPT_PROGRESS.exists():
        try:
            prior_clean, prior_rejects, processed_ids = load_checkpoint()
            if processed_ids:
                extracted_df = extracted_df[~extracted_df["doc_id"].astype(str).isin(processed_ids)]
                log.info(f"Remaining after resume: {len(extracted_df)}")
        except Exception as e:
            log.warning(f"Checkpoint load failed ({e}) — starting fresh")
            prior_clean, prior_rejects = pd.DataFrame(), pd.DataFrame()

    # ------------------------------------------------------------------
    # Signal handler — save on Ctrl+C / kill
    # ------------------------------------------------------------------
    _clean_ref   = [prior_clean]
    _rejects_ref = [prior_rejects]

    def _shutdown(signum, frame):
        log.info("Shutdown signal — saving checkpoint...")
        save_checkpoint(_clean_ref[0], _rejects_ref[0], note="interrupted")
        log.info("Re-run same command to resume.")
        os._exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------
    if extracted_df.empty:
        log.info("Nothing to process.")
    else:
        # Step 1a — extraction failures
        log.info("--- Step 1a: Drop extraction failures ---")
        fail_mask    = ~extracted_df["extraction_success"].fillna(False).astype(bool)
        extr_rejects = extracted_df[fail_mask].copy()
        extr_rejects["reject_reason"] = "extraction_failed"
        df           = extracted_df[~fail_mask].copy()
        log.info(f"  extraction_failed={len(extr_rejects)}  survivors={len(df)}")

        # Step 1b — tag/index filter
        log.info("--- Step 1b: Tag/index/archive filter ---")
        df, tag_rejects = apply_tag_filter(df)
        log.info(f"  tag_or_index_page={len(tag_rejects)}  survivors={len(df)}")

        # Step 2 — clean text
        log.info("--- Step 2: Text cleaning ---")
        df = clean_texts(df)

        # Step 3 — usability filter
        log.info("--- Step 3: Usability filter ---")
        df, usability_rejects = apply_usability_filter(df)
        log.info(f"  usability_failures={len(usability_rejects)}  survivors={len(df)}")

        # Step 4 — language detection
        log.info("--- Step 4: Language detection ---")
        if LANGID_AVAILABLE and not df.empty:
            lang_results = detect_languages_batch(df["clean_text"])
            df           = df.join(lang_results)
            df["language_match"] = build_lang_match(df, lang_df)
        else:
            df["language_detected"]   = "unknown"
            df["language_confidence"] = 0.0
            df["language_match"]      = False

        # Step 5 — relevance scoring
        log.info("--- Step 5: Relevance scoring ---")
        if not df.empty:
            df = score_relevance_all(df, lang_df, loc_df, lexicon)
            df = add_content_signals(df)

            # Reject: no location match
            has_loc = df["flood_id"].apply(
                lambda fid: not loc_df[loc_df["flood_id"] == fid].empty
            )
            no_loc_mask      = has_loc & (df["location_term_hits"] == 0)
            no_loc_rejects   = df[no_loc_mask].copy()
            no_loc_rejects["reject_reason"] = "no_location_match"
            df               = df[~no_loc_mask]

            # Reject: no flood term hit at all
            no_flood_mask    = df["flood_term_hits"] == 0
            no_flood_rejects = df[no_flood_mask].copy()
            no_flood_rejects["reject_reason"] = "no_flood_term_match"
            df               = df[~no_flood_mask]

            log.info(f"  no_location_match={len(no_loc_rejects)}  no_flood_term={len(no_flood_rejects)}  survivors={len(df)}")
        else:
            no_loc_rejects   = pd.DataFrame()
            no_flood_rejects = pd.DataFrame()

        # Step 6 — pub date window filter
        log.info("--- Step 6: Pub date window filter ---")
        if not df.empty:
            df, oot_rejects = apply_pubdate_filter(df, event_windows)
            log.info(f"  pub_date_out_of_window={len(oot_rejects)}  survivors={len(df)}")
        else:
            oot_rejects = pd.DataFrame()

        # Collect all rejects from this run
        all_rejects = pd.concat(
            [r for r in [extr_rejects, tag_rejects, usability_rejects,
                         no_loc_rejects, no_flood_rejects, oot_rejects] if not r.empty],
            ignore_index=True,
        )

        # Merge with any prior checkpoint data
        if not prior_clean.empty:
            df = pd.concat([prior_clean, df], ignore_index=True)
        if not prior_rejects.empty:
            all_rejects = pd.concat([prior_rejects, all_rejects], ignore_index=True)

        _clean_ref[0]   = df
        _rejects_ref[0] = all_rejects

        # Checkpoint before dedup
        save_checkpoint(df, all_rejects, note="pre-dedup")

        # Step 7 — dedup
        clean_df = deduplicate_content(df) if not df.empty else df.copy()

        # ------------------------------------------------------------------
        # Save final outputs
        # ------------------------------------------------------------------
        out_path = OUTPUT_DIR / "clean_text.parquet"
        clean_df.to_parquet(out_path, index=False)
        log.info(f"Saved clean_text -> {out_path}  ({len(clean_df)} rows)")

        save_rejects(all_rejects)
        clear_checkpoint()
        log.info("Checkpoint cleared (run completed successfully)")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    clean_path = OUTPUT_DIR / "clean_text.parquet"
    rej_path   = OUTPUT_DIR / "rejects.parquet"
    clean_df   = pd.read_parquet(clean_path) if clean_path.exists() else pd.DataFrame()
    rej_df     = pd.read_parquet(rej_path)   if rej_path.exists()   else pd.DataFrame()

    if args.flood_id:
        clean_df = clean_df[clean_df["flood_id"] == args.flood_id] if not clean_df.empty else clean_df
        rej_df   = rej_df[rej_df["flood_id"]     == args.flood_id] if not rej_df.empty   else rej_df
    elif not args.all:
        clean_df = clean_df[clean_df["flood_id"].isin(PILOT_FLOOD_IDS)] if not clean_df.empty else clean_df
        rej_df   = rej_df[rej_df["flood_id"].isin(PILOT_FLOOD_IDS)]     if not rej_df.empty   else rej_df

    total = len(clean_df) + len(rej_df)

    def _rc(reason):
        if rej_df.empty: return 0
        return rej_df["reject_reason"].str.startswith(reason, na=False).sum()

    log.info("=" * 70)
    log.info(f"Total docs processed       : {total}")
    if total:
        log.info(f"Kept in clean_text         : {len(clean_df)}  ({len(clean_df)/total:.1%})")
    log.info(f"Rejected total             : {len(rej_df)}")
    log.info(f"  extraction_failed        : {_rc('extraction_failed')}")
    log.info(f"  tag_or_index_page        : {_rc('tag_or_index_page')}")
    log.info(f"  usability                : {_rc('char_count') + _rc('empty') + _rc('non_ascii') + _rc('error_page')}")
    log.info(f"  no_location_match        : {_rc('no_location_match')}")
    log.info(f"  no_flood_term_match      : {_rc('no_flood_term_match')}")
    log.info(f"  pub_date_out_of_window   : {_rc('pub_date_out_of_window')}")
    if not clean_df.empty and "is_content_duplicate" in clean_df:
        log.info(f"Content duplicates flagged : {clean_df['is_content_duplicate'].sum()}")
    log.info("")

    if not clean_df.empty:
        log.info("Per-event breakdown:")
        for fid, grp in clean_df.groupby("flood_id"):
            n        = len(grp)
            rel      = grp["is_relevant"].sum()                    if "is_relevant"               in grp else 0
            ment     = grp["flood_mentioned"].sum()                if "flood_mentioned"            in grp else rel
            lm       = grp["language_match"].sum()                 if "language_match"             in grp else 0
            pub_k    = grp["pub_in_window"].notna().sum()          if "pub_in_window"              in grp else 0
            pub_ok   = (grp["pub_in_window"] == True).sum()        if "pub_in_window"              in grp else 0
            low_spec = grp["low_specificity"].sum()                if "low_specificity"            in grp else 0
            avg_spec = grp["location_specificity_score"].mean()    if "location_specificity_score" in grp else 0.0
            log.info(
                f"  Flood #{int(fid):>3}"
                f"  docs={n:>5}"
                f"  relevant={rel:>4} ({rel/n:.0%})"
                f"  mentioned={ment:>4}"
                f"  lang_match={lm:>4}"
                f"  pub_dated={pub_k}/{n}"
                f"  pub_ok={pub_ok}"
                f"  low_spec={low_spec}"
                f"  avg_spec={avg_spec:.2f}"
            )

        if args.compare_variants:
            log.info("")
            log.info("=== VARIANT COMPARISON ===")
            for col, label in [
                ("flood_mentioned",            "flood_mentioned=True"),
                ("is_relevant",                "is_relevant=True"),
                ("low_specificity",            "low_specificity"),
                ("signal_many_short_lines",    "signal_many_short_lines"),
                ("signal_no_long_sentence",    "signal_no_long_sentence"),
                ("signal_large_low_flood",     "signal_large_low_flood"),
            ]:
                if col in clean_df:
                    log.info(f"  {label:35}: {clean_df[col].sum()} / {len(clean_df)}")

    log.info("")
    log.info("Next: review clean_text.parquet, then proceed to Phase 2")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
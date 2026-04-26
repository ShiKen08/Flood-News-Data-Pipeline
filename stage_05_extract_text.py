# =============================================================================
# stage_05_extract_text.py  ·  Flood Data Pipeline — Extract HTML -> Plaintext
# =============================================================================
# Checklist Stage 5 (pilot phase)
#
# Uses trafilatura for main article body extraction — it identifies and discards
# sidebars, news tickers, nav, footers, and cookie banners using ML, returning
# only the article body. This prevents ticker keyword contamination in stage 06.
#
# Per cached WARC slice:
#   - Parse WARC record headers, skip non-response records
#   - Skip non-HTML responses (PDF, JSON, etc.)
#   - Use trafilatura to extract main article body (raw_text)
#   - Fall back to BeautifulSoup if trafilatura returns nothing
#   - Extract page_title and meta_description via BeautifulSoup
#   - Store extraction_success, extraction_error, encoding_detected
#
# Reads:
#   output/warc_fetch_log.parquet      (to find cached file paths)
#
# Outputs:
#   output/extracted_text.parquet
#     Columns: doc_id, pointer_id, flood_id, page_title, meta_description,
#              raw_text, extraction_method, extraction_success, extraction_error,
#              encoding_detected
#
# Run:
#   python stage_05_extract_text.py             # pilot events (cached files only)
#   python stage_05_extract_text.py --all       # Phase 2, all events
#   python stage_05_extract_text.py --flood-id 3
# =============================================================================

import argparse
import gzip
import importlib.util
import logging
import os
import re
import signal
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import chardet
import pandas as pd
from bs4 import BeautifulSoup
import sys
sys.stdout.reconfigure(encoding='utf-8')

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False
    print("WARNING: trafilatura not installed. Run: pip install trafilatura")
    print("Falling back to BeautifulSoup extraction (lower quality).")

# ---------------------------------------------------------------------------
# Force-load local config.py
# ---------------------------------------------------------------------------
_config_path = Path(__file__).parent / "config.py"
_spec = importlib.util.spec_from_file_location("config", _config_path)
_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)
sys.modules["config"] = _config

from config import (
    EXTRACT_WORKERS,
    LOGS_DIR,
    OUTPUT_DIR,
    PILOT_FLOOD_IDS,
    SCHEMA_EXTRACTED_TEXT,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_05_extract_text.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# WARC parser — extract HTTP response body
# =============================================================================

def parse_warc_response(raw_bytes: bytes) -> tuple[bytes, str, str]:
    """
    Parse a WARC response record.
    Returns (html_bytes, content_type, encoding_hint).
    Returns (b'', '', '') if not an HTML response record.
    """
    try:
        # Decompress if gzip
        try:
            data = gzip.decompress(raw_bytes)
        except (gzip.BadGzipFile, OSError):
            data = raw_bytes

        # Find end of WARC headers (first blank line)
        warc_header_end = data.find(b"\r\n\r\n")
        if warc_header_end == -1:
            return b"", "", ""

        # Parse WARC headers — confirm this is a response record
        warc_headers_raw = data[:warc_header_end].decode("utf-8", errors="replace")
        warc_type = ""
        for line in warc_headers_raw.splitlines():
            if line.lower().startswith("warc-type:"):
                warc_type = line.partition(":")[2].strip().lower()
                break

        if warc_type != "response":
            return b"", "", ""

        # Everything after WARC headers is the HTTP response
        http_response = data[warc_header_end + 4:]

        # Find end of HTTP headers
        http_header_end = http_response.find(b"\r\n\r\n")
        if http_header_end == -1:
            return b"", "", ""

        http_headers_raw = http_response[:http_header_end].decode("utf-8", errors="replace")
        html_body        = http_response[http_header_end + 4:]

        # Parse content-type and charset from HTTP headers
        content_type  = ""
        encoding_hint = ""
        for line in http_headers_raw.splitlines():
            if line.lower().startswith("content-type:"):
                content_type = line.partition(":")[2].strip()
                if "charset=" in content_type.lower():
                    encoding_hint = (
                        content_type.lower()
                        .split("charset=")[-1]
                        .split(";")[0]
                        .strip()
                    )
                break

        if "text/html" not in content_type.lower():
            return b"", content_type, encoding_hint

        return html_body, content_type, encoding_hint

    except Exception:
        return b"", "", ""


# =============================================================================
# Encoding detection
# =============================================================================

def detect_encoding(html_bytes: bytes, encoding_hint: str = "") -> str:
    """
    Detect encoding in priority order:
    1. HTTP header hint
    2. <meta charset> tag
    3. chardet
    4. utf-8 fallback
    """
    if encoding_hint:
        return encoding_hint.lower()

    meta_match = re.search(
        rb'<meta[^>]+charset=["\']?([a-zA-Z0-9\-]+)',
        html_bytes[:2048], re.I
    )
    if meta_match:
        return meta_match.group(1).decode("ascii", errors="replace").lower()

    detected = chardet.detect(html_bytes[:10000])
    if detected and detected.get("confidence", 0) > 0.7:
        return (detected.get("encoding") or "utf-8").lower()

    return "utf-8"


# =============================================================================
# Metadata extraction (title + meta description) via BeautifulSoup
# =============================================================================

def extract_metadata(html_bytes: bytes, encoding: str) -> tuple[str, str]:
    """Extract page_title and meta_description from raw HTML."""
    try:
        html_str = html_bytes.decode(encoding, errors="replace")
    except (LookupError, UnicodeDecodeError):
        html_str = html_bytes.decode("utf-8", errors="replace")

    try:
        soup = BeautifulSoup(html_str, "html.parser")

        title_tag  = soup.find("title")
        page_title = title_tag.get_text(strip=True) if title_tag else ""

        meta_desc = ""
        for meta in soup.find_all("meta"):
            name = meta.get("name", "").lower()
            prop = meta.get("property", "").lower()
            if name in ("description",) or prop in ("og:description",):
                meta_desc = meta.get("content", "")
                break

        return page_title, meta_desc
    except Exception:
        return "", ""


def extract_pub_date(html_bytes: bytes, encoding: str, url: str = "") -> str:
    """
    Extract article publication date. Returns ISO date string (YYYY-MM-DD) or "".

    Priority order:
    1. <meta> tags: article:published_time, og:article:published_time,
                    datePublished, pubdate, date, DC.date
    2. JSON-LD structured data: datePublished
    3. URL date pattern: /YYYY/MM/DD/ or /YYYY-MM-DD or ?date=YYYY-MM-DD
    4. Returns "" if nothing found — never raises
    """
    # ── 1. Meta tags ─────────────────────────────────────────────────────────
    META_PROPS = {
        "article:published_time", "og:article:published_time",
        "datepublished", "pubdate", "date", "dc.date",
        "article:modified_time",  # fallback if published not present
    }
    try:
        try:
            html_str = html_bytes.decode(encoding, errors="replace")
        except (LookupError, UnicodeDecodeError):
            html_str = html_bytes.decode("utf-8", errors="replace")

        soup = BeautifulSoup(html_str, "html.parser")

        for meta in soup.find_all("meta"):
            prop = (meta.get("property", "") or "").lower()
            name = (meta.get("name", "") or "").lower()
            itemprop = (meta.get("itemprop", "") or "").lower()
            if prop in META_PROPS or name in META_PROPS or itemprop in META_PROPS:
                content = meta.get("content", "") or ""
                date = _parse_date_str(content)
                if date:
                    return date

        # ── 2. JSON-LD ───────────────────────────────────────────────────────
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json as _json
                data = _json.loads(script.string or "")
                # data can be a list or a dict
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict):
                        for key in ("datePublished", "dateCreated", "dateModified"):
                            val = item.get(key, "")
                            if val:
                                date = _parse_date_str(str(val))
                                if date:
                                    return date
            except Exception:
                continue

    except Exception:
        pass

    # ── 3. URL pattern ───────────────────────────────────────────────────────
    if url:
        date = _extract_date_from_url(url)
        if date:
            return date

    return ""


def _parse_date_str(s: str) -> str:
    """Parse a date string into YYYY-MM-DD. Returns '' on failure."""
    if not s:
        return ""
    # Try common patterns: ISO datetime, ISO date, slash-separated
    import re as _re
    # ISO: 2025-04-08T... or 2025-04-08
    m = _re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2000 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return ""


def _extract_date_from_url(url: str) -> str:
    """Extract YYYY-MM-DD from URL path patterns like /2025/04/08/ or /2025-04-08."""
    import re as _re
    # /YYYY/MM/DD/
    m = _re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2000 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    # /YYYY-MM-DD
    m = _re.search(r"/(\d{4})-(\d{2})-(\d{2})", url)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2000 <= y <= 2030 and 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return ""


# =============================================================================
# Article body extraction — trafilatura primary, BeautifulSoup fallback
# =============================================================================

def extract_article_body_trafilatura(html_bytes: bytes, encoding: str) -> str:
    """
    Use trafilatura to extract main article body only.
    Discards sidebars, tickers, nav, footers automatically.
    Returns empty string if trafilatura finds nothing useful.
    """
    try:
        html_str = html_bytes.decode(encoding, errors="replace")
    except (LookupError, UnicodeDecodeError):
        html_str = html_bytes.decode("utf-8", errors="replace")

    try:
        text = trafilatura.extract(
            html_str,
            include_comments=False,
            include_tables=True,
            no_fallback=False,
            favor_precision=True,   # fewer false positives over recall
        )
        return text or ""
    except Exception:
        return ""


def extract_article_body_bs4_fallback(html_bytes: bytes, encoding: str) -> str:
    """
    BeautifulSoup fallback — strips known boilerplate tags.
    Used only when trafilatura returns empty.
    """
    STRIP_TAGS = [
        "nav", "header", "footer", "aside", "script", "style",
        "noscript", "iframe", "form", "button",
    ]

    try:
        html_str = html_bytes.decode(encoding, errors="replace")
    except (LookupError, UnicodeDecodeError):
        html_str = html_bytes.decode("utf-8", errors="replace")

    try:
        soup = BeautifulSoup(html_str, "html.parser")
        # Collect first, then decompose — never modify tree while iterating
        to_remove = [t for t in soup.find_all(STRIP_TAGS)]
        for tag in to_remove:
            tag.decompose()
        return soup.get_text(separator="\n").strip()
    except Exception:
        return ""


# =============================================================================
# Truncation detection
# =============================================================================

# Characters that indicate a sentence has properly ended
_SENTENCE_ENDERS = frozenset(".!?。！？…\"'»›）)")

def detect_truncation(text: str) -> bool:
    """
    Returns True if raw_text appears to have been cut off mid-sentence.
    Heuristic: text is >= 200 chars AND the last non-whitespace character
    is not a recognised sentence-ending character.
    Trafilatura occasionally cuts off at paywall notices or comment sections.
    """
    if not text or len(text.strip()) < 200:
        return False
    last_char = text.rstrip()[-1]
    return last_char not in _SENTENCE_ENDERS


# =============================================================================
# Checkpoint helpers
# =============================================================================

CHECKPOINT_PATH  = None   # set in main() once OUTPUT_DIR is known
CHECKPOINT_EVERY = 5_000  # flush to disk every N processed files


def load_checkpoint(ckpt_path: Path) -> tuple[list[dict], set]:
    """
    Load existing checkpoint. Returns (results_so_far, processed_pointer_ids).
    Returns ([], set()) if no checkpoint exists.
    """
    if not ckpt_path.exists():
        return [], set()
    try:
        df = pd.read_parquet(ckpt_path)
        results = df.to_dict("records")
        done_ids = set(str(r["pointer_id"]) for r in results)
        log.info(f"Checkpoint loaded: {len(done_ids)} already processed -> resuming")
        return results, done_ids
    except Exception as e:
        log.warning(f"Could not load checkpoint ({e}) — starting fresh")
        return [], set()


def save_checkpoint(results: list[dict], ckpt_path: Path):
    """Flush current results to checkpoint parquet."""
    if not results:
        return
    try:
        df = pd.DataFrame(results)
        df["flood_id"] = df["flood_id"].astype(int)
        df.to_parquet(ckpt_path, index=False)
    except Exception as e:
        log.warning(f"Checkpoint save failed: {e}")


# =============================================================================
# Process one WARC file
# =============================================================================

def process_warc_file(pointer_id: str, flood_id: int, cache_path: str, url: str = "", capture_timestamp: str = "") -> dict:
    """
    Process a single cached WARC file.
    Returns an extracted_text row dict — always, even on failure.
    """
    doc_id = str(uuid.uuid4())

    base = {
        "doc_id":             doc_id,
        "pointer_id":         pointer_id,
        "flood_id":           flood_id,
        "page_title":         "",
        "meta_description":   "",
        "pub_date":           "",   # YYYY-MM-DD article publication date, "" if not found
        "pub_date_source":    "",   # "meta", "capture_ts", or ""
        "raw_text":           "",
        "is_truncated":       False,  # True if text appears cut off mid-sentence
        "extraction_method":  "",
        "extraction_success": False,
        "extraction_error":   "",
        "encoding_detected":  "",
    }

    try:
        raw_bytes = Path(cache_path).read_bytes()
    except Exception as e:
        base["extraction_error"] = f"cache read failed: {e}"
        return base

    html_bytes, content_type, encoding_hint = parse_warc_response(raw_bytes)

    if not html_bytes:
        base["extraction_error"] = (
            f"non-HTML or non-response record "
            f"(content-type={content_type or 'unknown'})"
        )
        return base

    encoding = detect_encoding(html_bytes, encoding_hint)
    base["encoding_detected"] = encoding

    # Metadata (title + description) — always from BeautifulSoup
    page_title, meta_desc    = extract_metadata(html_bytes, encoding)
    base["page_title"]       = page_title
    base["meta_description"] = meta_desc

    # Publication date — meta tags, JSON-LD, then URL fallback
    base["pub_date"] = extract_pub_date(html_bytes, encoding, url=url)

    # If no pub_date found, fall back to CC capture timestamp as approximate date
    # (labelled separately so downstream can distinguish real vs approximate dates)
    if not base["pub_date"] and capture_timestamp:
        try:
            # capture_timestamp may be ISO datetime string or pandas Timestamp str
            ts_str = str(capture_timestamp)[:10]  # take YYYY-MM-DD prefix
            import datetime
            datetime.date.fromisoformat(ts_str)   # validate it parses
            base["pub_date"] = ts_str
            base["pub_date_source"] = "capture_ts"
        except Exception:
            pass
    else:
        base["pub_date_source"] = "meta" if base["pub_date"] else ""

    # Article body — trafilatura first, bs4 fallback
    raw_text = ""
    method   = ""

    if TRAFILATURA_AVAILABLE:
        raw_text = extract_article_body_trafilatura(html_bytes, encoding)
        method   = "trafilatura"

    if not raw_text:
        raw_text = extract_article_body_bs4_fallback(html_bytes, encoding)
        method   = "bs4_fallback"

    if raw_text:
        base["raw_text"]          = raw_text
        base["extraction_method"] = method
        base["extraction_success"] = True
        base["is_truncated"]      = detect_truncation(raw_text)
    else:
        base["extraction_error"]  = "no text extracted by trafilatura or bs4"
        base["extraction_method"] = method

    return base


# =============================================================================
# MAIN
# =============================================================================

# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 05 — Extract HTML to plaintext")
    parser.add_argument("--all",      action="store_true", help="All events (Phase 2)")
    parser.add_argument("--flood-id", type=int,            help="Single flood_id (debug)")
    parser.add_argument("--fresh",    action="store_true", help="Ignore checkpoint, reprocess everything")
    parser.add_argument("--workers",  type=int, default=EXTRACT_WORKERS, help=f"Parallel worker threads (default: {EXTRACT_WORKERS})")
    args = parser.parse_args()

    if not TRAFILATURA_AVAILABLE:
        log.warning("trafilatura not available — using BeautifulSoup fallback only")
        log.warning("Install with: pip install trafilatura")

    ckpt_path = OUTPUT_DIR / "stage05_ckpt.parquet"

    log.info("=" * 70)
    log.info("STAGE 05 — EXTRACT HTML -> PLAINTEXT")
    log.info(f"Extractor : {'trafilatura (article body only)' if TRAFILATURA_AVAILABLE else 'bs4 fallback'}")
    log.info(f"Workers   : {args.workers}")
    log.info(f"Checkpoint: {ckpt_path}")
    log.info("=" * 70)

    fetch_log_path = OUTPUT_DIR / "warc_fetch_log.parquet"
    if not fetch_log_path.exists():
        log.error("warc_fetch_log.parquet not found — run stage_04 first")
        sys.exit(1)

    fetch_log = pd.read_parquet(fetch_log_path)
    fetch_log = fetch_log[
        fetch_log["download_success"] &
        fetch_log["local_cache_path"].notna() &
        (fetch_log["local_cache_path"] != "")
    ]

    # Join URL from validated_pointers so extract_pub_date can use it as fallback
    pointers_path = OUTPUT_DIR / "validated_pointers.parquet"
    if pointers_path.exists():
        ptr = pd.read_parquet(pointers_path)[["pointer_id", "url"]].drop_duplicates("pointer_id")
        fetch_log = fetch_log.merge(ptr, on="pointer_id", how="left")
    else:
        fetch_log["url"] = ""

    if args.flood_id:
        fetch_log = fetch_log[fetch_log["flood_id"] == args.flood_id]
    elif not args.all:
        fetch_log = fetch_log[fetch_log["flood_id"].isin(PILOT_FLOOD_IDS)]

    log.info(f"WARC files to process : {len(fetch_log)}")

    # ------------------------------------------------------------------
    # Load checkpoint (skip already-processed pointer_ids)
    # ------------------------------------------------------------------
    if args.fresh and ckpt_path.exists():
        ckpt_path.unlink()
        log.info("--fresh: checkpoint cleared")

    results, done_ids = load_checkpoint(ckpt_path)

    to_process = fetch_log[~fetch_log["pointer_id"].astype(str).isin(done_ids)]
    if done_ids:
        log.info(f"Skipping {len(done_ids)} already-processed  |  {len(to_process)} remaining")

    if to_process.empty:
        log.info("Nothing left to process — all files already in checkpoint.")
    else:
        # ------------------------------------------------------------------
        # Shared state for parallel workers
        # ------------------------------------------------------------------
        results_lock   = threading.Lock()
        shutdown_event = threading.Event()
        success_count  = sum(1 for r in results if r.get("extraction_success"))
        fallback_count = sum(1 for r in results if r.get("extraction_method") == "bs4_fallback")
        skip_count     = sum(1 for r in results if "non-HTML" in (r.get("extraction_error") or ""))
        error_count    = sum(1 for r in results if not r.get("extraction_success") and
                             "non-HTML" not in (r.get("extraction_error") or ""))

        def _handle_shutdown(signum, frame):
            log.info("\nShutdown signal — saving checkpoint and exiting cleanly...")
            shutdown_event.set()

        signal.signal(signal.SIGINT,  _handle_shutdown)
        signal.signal(signal.SIGTERM, _handle_shutdown)

        rows = list(to_process.itertuples(index=False))

        # Progress bar — tqdm if available, else plain counter
        if TQDM_AVAILABLE:
            pbar = tqdm(
                total=len(rows),
                desc="Stage 05",
                unit="file",
                dynamic_ncols=True,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
            )
        else:
            pbar = None
            log.info("(tqdm not installed — install with: pip install tqdm)")

        def process_row(row):
            return process_warc_file(
                pointer_id=str(row.pointer_id),
                flood_id=int(row.flood_id),
                cache_path=str(row.local_cache_path),
                url=str(getattr(row, "url", "") or ""),
                capture_timestamp=str(getattr(row, "fetched_at", "") or ""),
            )

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_row, row): row for row in rows}
            processed_since_ckpt = 0

            for future in as_completed(futures):
                if shutdown_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                result = future.result()

                with results_lock:
                    results.append(result)
                    processed_since_ckpt += 1

                    if result["extraction_success"]:
                        success_count += 1
                        if result["extraction_method"] == "bs4_fallback":
                            fallback_count += 1
                    elif "non-HTML" in result.get("extraction_error", ""):
                        skip_count += 1
                    else:
                        error_count += 1

                    if pbar:
                        pbar.set_postfix(
                            ok=success_count,
                            skip=skip_count,
                            err=error_count,
                            trunc=sum(1 for r in results if r.get("is_truncated")),
                            refresh=False,
                        )
                        pbar.update(1)

                    # Periodic checkpoint
                    if processed_since_ckpt >= CHECKPOINT_EVERY:
                        save_checkpoint(results, ckpt_path)
                        processed_since_ckpt = 0
                        if not pbar:
                            log.info(
                                f"  Checkpoint: {len(results)} total  "
                                f"success={success_count}  skip={skip_count}  err={error_count}"
                            )

        if pbar:
            pbar.close()

        # Final checkpoint save
        save_checkpoint(results, ckpt_path)

        if shutdown_event.is_set():
            log.info(f"Interrupted — checkpoint saved ({len(results)} results). Re-run to resume.")
            sys.exit(0)

    # ------------------------------------------------------------------
    # Build final output from all results (checkpoint + this run)
    # ------------------------------------------------------------------
    extracted_df = pd.DataFrame(results)
    extracted_df["flood_id"] = extracted_df["flood_id"].astype(int)

    for col in SCHEMA_EXTRACTED_TEXT:
        if col not in extracted_df.columns:
            extracted_df[col] = None

    out_path = OUTPUT_DIR / "extracted_text.parquet"
    extracted_df.to_parquet(out_path, index=False)

    # Clean up checkpoint now that the full output is saved
    if ckpt_path.exists():
        ckpt_path.unlink()
        log.info("Checkpoint cleared (full output saved)")

    total        = len(extracted_df)
    success_count  = extracted_df["extraction_success"].sum()
    fallback_count = (extracted_df["extraction_method"] == "bs4_fallback").sum()
    skip_count     = extracted_df["extraction_error"].str.contains("non-HTML", na=False).sum()
    trunc_count    = extracted_df.get("is_truncated", pd.Series(dtype=bool)).sum()
    success_rate   = success_count / total if total > 0 else 0

    log.info("=" * 70)
    log.info(f"Total processed       : {total}")
    log.info(f"Extraction success    : {success_count}  ({success_rate:.1%})")
    log.info(f"  via trafilatura     : {success_count - fallback_count}")
    log.info(f"  via bs4 fallback    : {fallback_count}")
    log.info(f"Truncated (is_truncated=True) : {trunc_count}  ({trunc_count/total:.1%})" if total else "")
    log.info(f"Skipped (non-HTML)    : {skip_count}")
    log.info(f"Errors                : {total - success_count - skip_count}")
    log.info(f"Saved -> {out_path}")
    log.info("Next: run stage_06_clean_deduplicate.py")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
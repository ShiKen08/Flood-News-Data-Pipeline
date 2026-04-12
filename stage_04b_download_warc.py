# =============================================================================
# stage_04_download_warc.py  ·  Flood Data Pipeline — Download WARC Slices
# =============================================================================
# Checklist Stage 4 (pilot phase)
#
# Per valid pointer:
#   - Build WARC URL: https://data.commoncrawl.org/{filename}
#   - Issue HTTP Range GET: bytes={offset}-{offset+length-1}
#   - Verify bytes_received == length -> flag PARTIAL if mismatch
#   - Decompress if gzip-encoded
#   - Parse WARC record headers to confirm record type
#   - Store raw WARC slice to local cache (avoids re-downloading)
#   - Log every attempt regardless of success or failure
#
# Reads:
#   output/validated_pointers.parquet    (from stage_03)
#
# Outputs:
#   cache/{flood_id}/{pointer_id}.warc.gz     (raw WARC slices)
#   output/warc_fetch_log.parquet
#     Columns: pointer_id, flood_id, download_success, http_status,
#              bytes_received, bytes_expected, bytes_match, error_type,
#              error_message, retry_count, local_cache_path, fetched_at
#
# Run:
#   python stage_04_download_warc.py                    # pilot, test batch (100/event)
#   python stage_04_download_warc.py --full             # pilot, all pointers
#   python stage_04_download_warc.py --all              # Phase 2, all events
#   python stage_04_download_warc.py --flood-id 3       # single event debug
#   python stage_04_download_warc.py --flood-id 3 --full
# =============================================================================

import argparse
import os
import gzip
import importlib.util
import io
import logging
import re
import signal
import sys
import queue as queue_module
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    CACHE_DIR,
    CC_DATA_URL,
    DOWNLOAD_SUCCESS_RATE_FLOOR,
    LOGS_DIR,
    MAX_RETRIES,
    OUTPUT_DIR,
    PILOT_BATCH_SIZE,
    BATCH_SIZE,
    PILOT_FLOOD_IDS,
    RETRY_BACKOFF_BASE,
    DOWNLOAD_INTER_REQUEST_SLEEP,
    SCHEMA_WARC_FETCH_LOG,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_04_download_warc.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DOWNLOAD_TIMEOUT        = (10, 30)   # (connect, read) — primary pass, fail fast on stalls
DEFERRED_TIMEOUT        = (10, 300)  # (connect, read) — deferred pass, run to completion
MIN_THROUGHPUT_BPS      = 10_000     # 10 KB/s — abort primary pass if slower than this
MIN_THROUGHPUT_GRACE    = 5          # seconds before throughput check kicks in (connection ramp-up)
WORKER_THREADS          = 8        # starting worker count — adaptive throttle adjusts this
WORKER_THREADS_MIN      = 8          # floor — never go below this
WORKER_THREADS_MAX      = 18         # ceiling — never go above this
WARC_RESPONSE_TYPE      = b"WARC/1.0"
CHECKPOINT_EVERY        = 3000       # flush fetch log to disk every N completed downloads
RATE_LIMIT_PAUSE        = 300        # seconds to pause when 403s detected (5 min)
RATE_LIMIT_THRESHOLD    = 3          # consecutive 403s before triggering throttle-down
RAMP_UP_AFTER           = 2000        # downloads with no failures before adding a worker

# =============================================================================
# URL pre-filter — skip known junk URLs before downloading any WARC bytes
# =============================================================================
# Mirrors the tag/index filter in stage_06v but applied at the pointer level
# to avoid wasting bandwidth on pages that will never be relevant articles.

_PREFILTER_JUNK_DOMAIN_RE = re.compile(
    r"^https?://(?:www\.)?weather\.gov(?:/|$)",
    re.IGNORECASE,
)

_PREFILTER_URL_PATH_RE = re.compile(
    r"/tag/|/tags/|/tag$|/tags$"
    r"|/category/|/categories/"
    r"|/topic/|/topics/"
    r"|/archive/|/archives/"
    r"|/search/|/buscar/|/recherche/"
    r"|[?&]q=|[?&]s=|[?&]query=|[?&]keyword="
    r"|/page/\d+|[?&]page=\d+",
    re.IGNORECASE,
)

_PREFILTER_HOMEPAGE_RE = re.compile(r"^https?://[^/]+/?(?:\?.*)?$")


def _is_prefilter_junk(url: str) -> bool:
    if not url:
        return False
    if _PREFILTER_JUNK_DOMAIN_RE.match(url):
        return True
    if _PREFILTER_HOMEPAGE_RE.match(url):
        return True
    return bool(_PREFILTER_URL_PATH_RE.search(url))


# =============================================================================
# Thread-local requests.Session — one persistent connection pool per thread
# =============================================================================

_thread_local = threading.local()

def get_session() -> requests.Session:
    """
    Returns a requests.Session local to the current thread.
    Each thread gets its own session with a persistent connection pool to
    data.commoncrawl.org — avoids TCP+SSL handshake overhead on every request.
    """
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        # Mount adapter with connection pooling — pool_connections=1 because
        # we only talk to one host; pool_maxsize=4 allows a few kept-alive
        # connections per thread for burst requests
        adapter = HTTPAdapter(
            pool_connections=1,
            pool_maxsize=4,
            max_retries=Retry(total=0),  # we handle retries ourselves
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _thread_local.session = session
    return _thread_local.session


# =============================================================================
# Adaptive throttle — dynamically adjusts concurrency based on 403/503 rate
# =============================================================================

class AdaptiveThrottle:
    """
    Controls concurrency via a semaphore that can be scaled up/down at runtime.

    - Starts at WORKER_THREADS_MAX
    - On rate limiting: halves concurrency, pauses RATE_LIMIT_PAUSE seconds
    - After RAMP_UP_AFTER consecutive clean downloads: adds 1 worker (up to max)
    - Never goes below WORKER_THREADS_MIN
    """

    def __init__(self):
        self._workers       = WORKER_THREADS_MAX
        self._semaphore     = threading.Semaphore(self._workers)
        self._lock          = threading.Lock()
        self._clean_streak  = 0   # consecutive successful downloads

    @property
    def workers(self):
        return self._workers

    def acquire(self):
        self._semaphore.acquire()

    def release(self):
        self._semaphore.release()

    def report_success(self):
        with self._lock:
            self._clean_streak += 1
            # Ramp up: add 1 worker after a clean streak, up to max
            if self._clean_streak >= RAMP_UP_AFTER and self._workers < WORKER_THREADS_MAX:
                self._workers      += 1
                self._clean_streak  = 0
                self._semaphore.release()  # add one slot to the semaphore
                log.info(f"  ↑ Ramping up — workers now {self._workers}")

    def report_rate_limit(self) -> bool:
        """
        Called on consecutive 403 threshold breach.
        Halves concurrency and pauses. Returns True if throttled down.
        """
        with self._lock:
            self._clean_streak = 0
            new_workers = max(self._workers - 4, WORKER_THREADS_MIN)
            if new_workers < self._workers:
                reduction = self._workers - new_workers
                self._workers = new_workers
                # Drain semaphore slots to reduce concurrency
                for _ in range(reduction):
                    self._semaphore.acquire()
                log.warning(
                    f"  ↓ Throttling down — workers {self._workers + reduction} -> {self._workers}. "
                    f"Pausing {RATE_LIMIT_PAUSE}s..."
                )
                time.sleep(RATE_LIMIT_PAUSE)
                log.info(f"  Resuming at {self._workers} workers")
                return True
            else:
                # Already at floor — just pause
                log.warning(f"  Already at minimum workers ({self._workers}). Pausing {RATE_LIMIT_PAUSE}s...")
                time.sleep(RATE_LIMIT_PAUSE)
                return False


# =============================================================================
# Cache helpers
# =============================================================================

# Directories created this session — avoids a blocking mkdir syscall on every
# cache_path_for() call. is_cached() is called once per row at startup across
# potentially tens of thousands of rows; without this, mkdir fires for every
# single one even though the directory already exists after the first call.
_CREATED_DIRS: set = set()


def cache_path_for(flood_id: int, pointer_id: str) -> Path:
    """Returns the local cache path for a WARC slice."""
    if flood_id not in _CREATED_DIRS:
        flood_dir = CACHE_DIR / str(flood_id)
        flood_dir.mkdir(parents=True, exist_ok=True)
        _CREATED_DIRS.add(flood_id)
    return CACHE_DIR / str(flood_id) / f"{pointer_id}.warc.gz"


def is_cached(flood_id: int, pointer_id: str, expected_length: int = 0) -> bool:
    """
    Returns True if a complete cache file already exists.
    If expected_length is provided, also checks file size matches —
    catches partial downloads from interrupted runs.
    """
    path = cache_path_for(flood_id, pointer_id)
    if not path.exists() or path.stat().st_size == 0:
        return False
    if expected_length > 0 and path.stat().st_size < expected_length * 0.95:
        # File is suspiciously small — likely a partial download, re-fetch
        log.debug(f"  Partial cache detected for {pointer_id} ({path.stat().st_size} < {expected_length}) — will re-download")
        path.unlink()
        return False
    return True


# =============================================================================
# WARC record header parser
# =============================================================================

def parse_warc_headers(data: bytes) -> dict:
    """
    Parse the first WARC record header from raw bytes.
    Returns dict of header fields, or empty dict if parsing fails.
    """
    try:
        # WARC headers end at the first blank line (\r\n\r\n)
        header_end = data.find(b"\r\n\r\n")
        if header_end == -1:
            return {}
        header_bytes = data[:header_end].decode("utf-8", errors="replace")
        headers = {}
        for line in header_bytes.splitlines():
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        return headers
    except Exception:
        return {}


# =============================================================================
# Core download — one pointer
# =============================================================================

def _build_result(pointer_id, flood_id, cache_file, length, status, bytes_received,
                  bytes_match, attempt, error_type="", error_message="", success=True):
    """Build a warc_fetch_log dict."""
    return {
        "pointer_id":       pointer_id,
        "flood_id":         flood_id,
        "download_success": success,
        "http_status":      status,
        "bytes_received":   bytes_received,
        "bytes_expected":   length,
        "bytes_match":      bytes_match,
        "error_type":       error_type,
        "error_message":    error_message,
        "retry_count":      attempt,
        "local_cache_path": str(cache_file) if success else "",
        "fetched_at":       datetime.now(timezone.utc).isoformat(),
    }


def _download_attempt(row: pd.Series, attempt: int, timeout: tuple = DOWNLOAD_TIMEOUT) -> dict:
    """
    Single download attempt using the thread-local requests.Session.
    Returns a result dict. Does NOT retry — caller handles retry logic.
    timeout: (connect_timeout, read_timeout) — pass DEFERRED_TIMEOUT for the deferred pass.
    """
    pointer_id = str(row["pointer_id"])
    flood_id   = int(row["flood_id"])
    filename   = str(row["filename"])
    offset     = int(row["offset"])
    length     = int(row["length"])
    cache_file = cache_path_for(flood_id, pointer_id)
    warc_url   = CC_DATA_URL.format(filename=filename)
    byte_range = f"bytes={offset}-{offset + length - 1}"

    session = get_session()
    try:
        resp = session.get(
            warc_url,
            headers={"Range": byte_range},
            timeout=timeout,
            stream=True,
        )
        http_status = resp.status_code

        # Read in chunks with wall-clock + throughput abort.
        #
        # Why both checks are needed:
        # - Wall-clock fires if iter_content blocks for > timeout[1]s with NO data at all
        # - Throughput check fires if CC drip-throttles (sends tiny chunks continuously
        #   but at < 10KB/s) — iter_content keeps yielding so wall-clock never triggers,
        #   but the download would take many minutes to complete
        # - On the deferred pass (timeout[1]==300) we skip the throughput check entirely
        #   and run to completion — these items already failed primary pass once
        is_deferred_pass = (timeout == DEFERRED_TIMEOUT)
        CHUNK_READ_TIMEOUT = timeout[1]
        t_read_start = time.time()
        bytes_so_far = 0
        try:
            chunks = []
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    chunks.append(chunk)
                    bytes_so_far += len(chunk)
                elapsed = time.time() - t_read_start

                # Wall-clock cap — fires if no data OR data so slow elapsed exceeds limit
                if elapsed > CHUNK_READ_TIMEOUT:
                    raise requests.exceptions.Timeout("wall-clock read limit exceeded")

                # Throughput floor — fires on primary pass only
                if not is_deferred_pass and elapsed > MIN_THROUGHPUT_GRACE:
                    rate = bytes_so_far / elapsed
                    if rate < MIN_THROUGHPUT_BPS:
                        raise requests.exceptions.Timeout(
                            f"throughput too low: {rate/1024:.1f}KB/s < {MIN_THROUGHPUT_BPS/1024:.0f}KB/s"
                        )
        finally:
            resp.close()  # always return connection to pool — required with stream=True
        raw_data       = b"".join(chunks)
        bytes_received = len(raw_data)

        if http_status == 404:
            return _build_result(pointer_id, flood_id, cache_file, length,
                                  http_status, 0, False, attempt,
                                  "HTTPError", f"HTTP 404 Not Found", success=False)

        if http_status in (403, 503):
            return _build_result(pointer_id, flood_id, cache_file, length,
                                  http_status, 0, False, attempt,
                                  "RateLimit", f"HTTP {http_status}", success=False)

        if http_status not in (200, 206):
            return _build_result(pointer_id, flood_id, cache_file, length,
                                  http_status, bytes_received, False, attempt,
                                  "HTTPError", f"HTTP {http_status}", success=False)

        bytes_match = (bytes_received == length)

        # Decompress if gzip to parse WARC headers; keep compressed for cache
        try:
            decompressed = gzip.decompress(raw_data)
            warc_data    = raw_data
        except (gzip.BadGzipFile, OSError):
            decompressed = raw_data
            warc_data    = raw_data

        warc_headers = parse_warc_headers(decompressed)
        warc_type    = warc_headers.get("warc-type", "unknown")
        if warc_type not in ("response", "unknown"):
            log.debug(f"  Non-response WARC type={warc_type} for {pointer_id}")

        cache_file.write_bytes(warc_data)
        # NOTE: the polite inter-request sleep was here previously, but that held
        # the throttle slot during the sleep — see download_warc_slice for the
        # corrected placement (sleep runs after throttle.release()).

        return _build_result(pointer_id, flood_id, cache_file, length,
                              http_status, bytes_received, bytes_match, attempt,
                              "" if bytes_match else "PARTIAL",
                              "" if bytes_match else f"got {bytes_received}, expected {length}",
                              success=True)

    except requests.exceptions.Timeout:
        return _build_result(pointer_id, flood_id, cache_file, length,
                              0, 0, False, attempt, "Timeout", "Request timed out", success=False)
    except requests.exceptions.ConnectionError as e:
        return _build_result(pointer_id, flood_id, cache_file, length,
                              0, 0, False, attempt, "ConnectionError", str(e), success=False)
    except Exception as e:
        return _build_result(pointer_id, flood_id, cache_file, length,
                              0, 0, False, attempt, type(e).__name__, str(e), success=False)


def download_warc_slice(row: pd.Series, throttle: "AdaptiveThrottle", timeout: tuple = DOWNLOAD_TIMEOUT) -> dict:
    """
    Download a single WARC slice using a persistent requests.Session.
    On transient failure: releases throttle slot, sleeps backoff, re-acquires.
    Workers never hold their slot while sleeping — slot is free for others.
    timeout: pass DEFERRED_TIMEOUT for the deferred pass (run to completion).
    Always returns a result dict.
    """
    pointer_id = str(row["pointer_id"])
    flood_id   = int(row["flood_id"])
    length     = int(row["length"])

    # Cache hit — skip entirely
    if is_cached(flood_id, pointer_id, expected_length=length):
        log.debug(f"  Cache hit: {pointer_id}")
        cache_file = cache_path_for(flood_id, pointer_id)
        return _build_result(pointer_id, flood_id, cache_file, length,
                              200, length, True, 0)

    last_result = None
    # BUG FIX: was range(MAX_RETRIES) which gives MAX_RETRIES total attempts
    # (e.g. 0,1,2 for MAX_RETRIES=3). Should be range(MAX_RETRIES + 1) to match
    # stage_04 and give MAX_RETRIES+1 total attempts (0,1,2,3).
    for attempt in range(MAX_RETRIES + 1):
        throttle.acquire()
        try:
            result = _download_attempt(row, attempt, timeout=timeout)
        finally:
            throttle.release()  # always release before any sleep

        last_result = result

        if result["download_success"]:
            # BUG FIX: polite inter-request sleep moved here — AFTER throttle.release().
            # Previously this sleep was inside _download_attempt, which meant the
            # throttle slot was held during the sleep. With 8 workers and 2s sleep
            # that was a hard ceiling of 8/2.0 = 4 req/s regardless of network speed.
            # Now the slot is free immediately after the HTTP work completes.
            time.sleep(DOWNLOAD_INTER_REQUEST_SLEEP)
            return result

        http_status = result.get("http_status", 0)
        error_type  = result.get("error_type", "")

        # Permanent failure — no retry
        if error_type == "HTTPError" and http_status == 404:
            return result

        # Timeout — return immediately so the worker can defer to Pass 2.
        # Retrying inline just blocks the worker slot for another 30s — don't.
        if error_type == "Timeout":
            return result

        # Throttle slot already released — sleep without blocking other workers
        if http_status in (403, 503):
            wait = RETRY_BACKOFF_BASE ** (attempt + 3)
        else:
            wait = RETRY_BACKOFF_BASE ** attempt
        log.debug(f"  Retry {attempt+1}/{MAX_RETRIES + 1} for {pointer_id} in {wait:.0f}s")
        time.sleep(wait)

    return last_result


# =============================================================================
# Batch runner with progress logging and success rate monitoring
# =============================================================================

def save_fetch_log(all_results: list[dict], existing_logs: list, fetch_log_path: Path, schema: list):
    """Flush current results to disk — called periodically and on shutdown."""
    if not all_results:
        return
    new_log_df = pd.DataFrame(all_results)
    new_log_df["flood_id"] = new_log_df["flood_id"].astype(int)
    for col in schema:
        if col not in new_log_df.columns:
            new_log_df[col] = None
    combined = pd.concat(existing_logs + [new_log_df[schema]], ignore_index=True)
    combined.to_parquet(fetch_log_path, index=False)
    return combined


def run_batch(
    batch_df:        pd.DataFrame,
    label:           str,
    all_results:     list,
    existing_logs:   list,
    fetch_log_path:  Path,
    schema:          list,
    shutdown_event:  threading.Event,
) -> list[dict]:
    """
    Two-pass parallel downloader with adaptive throttling.

    Pass 1 — Primary:
      Workers pull from primary_queue with DOWNLOAD_TIMEOUT (10s connect, 30s read).
      Any item that times out is pushed to deferred_queue instead of recording a failure.
      Fast items keep flowing; stalled items step aside.

    Pass 2 — Deferred:
      After primary_queue fully drains, workers switch to deferred_queue.
      DEFERRED_TIMEOUT (10s connect, 300s read) — run to completion, no more deferral.
      Whatever happens here is the final result.
    """
    log.info(f"  Downloading {len(batch_df)} pointers [{label}]  (workers={WORKER_THREADS_MAX} -> adaptive)")
    n_pointers      = len(batch_df)
    results         = []
    results_lock    = threading.Lock()
    completed       = 0      # unique pointers resolved (success or final failure)
    failed          = 0
    consecutive_403 = 0
    n_deferred      = 0
    total_bytes     = 0      # cumulative bytes received across all successful downloads
    start_time      = time.time()
    throttle        = AdaptiveThrottle()
    stop_event      = threading.Event()
    primary_done    = threading.Event()   # set after primary_queue.join()

    primary_queue  = queue_module.Queue()
    deferred_queue = queue_module.Queue()
    for _, row in batch_df.iterrows():
        primary_queue.put(row)

    def _record(result):
        """
        Record a completed result and update counters. Call with results_lock held.
        Returns True if a checkpoint save is needed — caller does the actual write
        AFTER releasing the lock so workers aren't blocked during disk I/O.
        """
        nonlocal completed, failed, consecutive_403, total_bytes

        results.append(result)
        all_results.append(result)
        completed += 1
        total_bytes += result.get("bytes_received", 0) or 0

        needs_checkpoint = False
        if not result["download_success"]:
            failed += 1
            if result.get("http_status") in (403, 503):
                consecutive_403 += 1
                if consecutive_403 >= RATE_LIMIT_THRESHOLD:
                    throttle.report_rate_limit()
                    consecutive_403 = 0
                    needs_checkpoint = True   # PERF FIX: save happens outside the lock
            else:
                consecutive_403 = 0
        else:
            consecutive_403 = 0
            throttle.report_success()

        if completed % CHECKPOINT_EVERY == 0:
            needs_checkpoint = True           # PERF FIX: save happens outside the lock

        if completed % 100 == 0 or completed == n_pointers:
            success_rate  = (completed - failed) / completed
            elapsed_total = time.time() - start_time
            rate          = completed / elapsed_total if elapsed_total > 0 else 0
            mbps          = (total_bytes / elapsed_total / 1_048_576) if elapsed_total > 0 else 0
            remaining     = n_pointers - completed
            eta_secs      = remaining / rate if rate > 0 else 0
            eta_str       = (
                f"{int(eta_secs // 3600)}h {int((eta_secs % 3600) // 60)}m"
                if eta_secs > 60 else f"{int(eta_secs)}s"
            )
            log.info(
                f"    Progress: {completed}/{n_pointers}  "
                f"success={success_rate:.1%}  failed={failed}  "
                f"deferred={n_deferred}  workers={throttle.workers}  "
                f"rate={rate:.1f}/s  speed={mbps:.2f}MB/s  ETA={eta_str}"
            )
            if completed >= 20 and success_rate < DOWNLOAD_SUCCESS_RATE_FLOOR:
                log.warning(
                    f"    ⚠ Success rate {success_rate:.1%} below floor "
                    f"{DOWNLOAD_SUCCESS_RATE_FLOOR:.0%} — investigate before continuing"
                )

        return needs_checkpoint

    def worker():
        nonlocal n_deferred

        # ---- Pass 1: primary queue ----
        while not stop_event.is_set() and not shutdown_event.is_set():
            try:
                row = primary_queue.get(timeout=0.5)
            except queue_module.Empty:
                if primary_done.is_set():
                    break   # primary done and queue empty — move on
                continue

            t0     = time.time()
            result = download_warc_slice(row, throttle, timeout=DOWNLOAD_TIMEOUT)
            elapsed = time.time() - t0

            # Timed out on primary pass -> defer, don't record failure yet
            if not result["download_success"] and result["error_type"] == "Timeout":
                with results_lock:
                    n_deferred += 1
                log.debug(
                    f"  -> Deferred {row['pointer_id']} (timed out after {elapsed:.0f}s, "
                    f"total deferred={n_deferred})"
                )
                deferred_queue.put(row)
                primary_queue.task_done()
                continue

            with results_lock:
                needs_ckpt = _record(result)
            # PERF FIX: checkpoint save runs outside the lock — workers aren't
            # blocked during the parquet concat+write (~1-2s at 3000 rows).
            if needs_ckpt:
                save_fetch_log(all_results, existing_logs, fetch_log_path, schema)
                log.info(f"  [OK] Checkpoint at {completed}/{n_pointers}  workers={throttle.workers}  deferred={n_deferred}")
            primary_queue.task_done()

        # ---- Pass 2: deferred queue ----
        # Only entered after primary_queue.join() has completed (primary_done is set).
        # Run to completion — whatever happens here is the final result, no more deferral.
        while not stop_event.is_set() and not shutdown_event.is_set():
            try:
                row = deferred_queue.get(timeout=0.5)
            except queue_module.Empty:
                break   # deferred queue empty — this worker is done

            result = download_warc_slice(row, throttle, timeout=DEFERRED_TIMEOUT)
            with results_lock:
                needs_ckpt = _record(result)
            if needs_ckpt:
                save_fetch_log(all_results, existing_logs, fetch_log_path, schema)
                log.info(f"  [OK] Checkpoint at {completed}/{n_pointers}  workers={throttle.workers}  deferred={n_deferred}")
            deferred_queue.task_done()

    # Start workers
    threads = [
        threading.Thread(target=worker, daemon=True, name=f"dl-worker-{i}")
        for i in range(WORKER_THREADS_MAX)
    ]
    for t in threads:
        t.start()

    # Coordinator: wait for primary to drain, then signal workers to switch to deferred pass
    def _primary_coordinator():
        primary_queue.join()
        if n_deferred > 0:
            log.info(
                f"  ↳ Primary pass complete. {n_deferred} slow items deferred "
                f"-> starting deferred pass (timeout={DEFERRED_TIMEOUT[1]}s)..."
            )
        else:
            log.info("  ↳ Primary pass complete. No items deferred.")
        primary_done.set()

    threading.Thread(target=_primary_coordinator, daemon=True, name="primary-coord").start()

    # Main thread: wait for all workers to finish, polling shutdown_event
    while any(t.is_alive() for t in threads):
        if shutdown_event.is_set():
            log.info("  Shutdown signal received — stopping workers")
            stop_event.set()
            break
        time.sleep(0.5)

    stop_event.set()
    for t in threads:
        t.join(timeout=5)

    return results


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stage 04 — Download WARC slices")
    parser.add_argument(
        "--full", action="store_true",
        help="(no-op — kept for backwards compat; all pointers are downloaded by default)"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Process all events (Phase 2)"
    )
    parser.add_argument(
        "--flood-id", type=int,
        help="Process a single flood_id only (debug)"
    )
    parser.add_argument(
        "--random", action="store_true",
        help="(no-op — kept for backwards compat)"
    )
    args = parser.parse_args()

    is_full  = True  # pilot batch limit removed — always download all eligible pointers
    mode_str = "FULL"

    log.info("=" * 70)
    log.info("STAGE 04 — DOWNLOAD WARC SLICES")
    log.info(f"Mode     : {mode_str}")
    log.info(f"Workers  : {WORKER_THREADS}")
    log.info(f"Retries  : {MAX_RETRIES}")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Load validated pointers
    # ------------------------------------------------------------------
    pointers_path = OUTPUT_DIR / "validated_pointers.parquet"
    if not pointers_path.exists():
        log.error("validated_pointers.parquet not found — run stage_03 first")
        sys.exit(1)

    pointers_df = pd.read_parquet(pointers_path)

    # Only process VALID, non-duplicate, non-TOO_LARGE pointers
    eligible = pointers_df[
        (pointers_df["status"] == "VALID") &
        (~pointers_df["is_pointer_duplicate"]) &
        (pointers_df["size_filter_status"] == "VALID")
    ].copy()

    log.info(f"Total valid pointers    : {len(pointers_df)}")
    log.info(f"Eligible for download   : {len(eligible)}")

    # Filter by flood_id if specified
    if args.flood_id:
        eligible = eligible[eligible["flood_id"] == args.flood_id]
        log.info(f"Filtered to flood #{args.flood_id}: {len(eligible)} pointers")
    elif not args.all:
        eligible = eligible[eligible["flood_id"].isin(PILOT_FLOOD_IDS)]

    # ------------------------------------------------------------------
    # Check for already-cached pointers — skip them
    # ------------------------------------------------------------------
    already_cached = eligible.apply(
        lambda r: is_cached(int(r["flood_id"]), str(r["pointer_id"])), axis=1
    )
    to_download = eligible[~already_cached]
    log.info(f"Already cached          : {already_cached.sum()}")
    log.info(f"To download             : {len(to_download)}")

    # ------------------------------------------------------------------
    # URL pre-filter — drop known junk URLs before downloading any bytes
    # ------------------------------------------------------------------
    junk_mask   = to_download["url"].fillna("").apply(_is_prefilter_junk)
    n_junk      = junk_mask.sum()
    to_download = to_download[~junk_mask].copy()
    log.info(f"URL pre-filter dropped  : {n_junk} junk URLs  |  remaining={len(to_download)}")

    if to_download.empty:
        log.info("Nothing to download — all pointers already cached.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Load existing fetch log if present (accumulate across runs)
    # ------------------------------------------------------------------
    fetch_log_path = OUTPUT_DIR / "warc_fetch_log.parquet"
    existing_logs  = []
    if fetch_log_path.exists():
        existing_logs = [pd.read_parquet(fetch_log_path)]
        log.info(f"Loaded {len(existing_logs[0])} existing fetch log rows")

    # ------------------------------------------------------------------
    # Shutdown handler — save fetch log on Ctrl+C or kill
    # ------------------------------------------------------------------
    shutdown_event = threading.Event()
    all_results    = []

    def _handle_shutdown(signum, frame):
        log.info("Shutdown signal received — saving checkpoint and exiting cleanly...")
        shutdown_event.set()
        if all_results:
            save_fetch_log(all_results, existing_logs, fetch_log_path, SCHEMA_WARC_FETCH_LOG)
            log.info(f"Checkpoint saved — {len(all_results)} results written.")
        else:
            log.info("No results to save yet.")
        os._exit(0)  # Force-kill — sys.exit() gets swallowed by ThreadPoolExecutor

    signal.signal(signal.SIGINT,  _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGQUIT, _handle_shutdown)

    # ------------------------------------------------------------------
    # Run downloads — grouped by flood_id for clean progress reporting
    # ------------------------------------------------------------------
    for flood_id, group in to_download.groupby("flood_id"):
        if shutdown_event.is_set():
            break
        log.info(f"--- Flood #{int(flood_id)} ({len(group)} pointers) ---")
        results = run_batch(
            batch_df=group,
            label=f"flood_{flood_id}",
            all_results=all_results,
            existing_logs=existing_logs,
            fetch_log_path=fetch_log_path,
            schema=SCHEMA_WARC_FETCH_LOG,
            shutdown_event=shutdown_event,
        )

        successes = sum(1 for r in results if r["download_success"])
        partials  = sum(1 for r in results if r["error_type"] == "PARTIAL")
        failures  = len(results) - successes
        log.info(
            f"  flood #{int(flood_id)} done: "
            f"success={successes}  partial={partials}  failed={failures}"
        )

    # ------------------------------------------------------------------
    # Final save
    # ------------------------------------------------------------------
    combined_log = save_fetch_log(all_results, existing_logs, fetch_log_path, SCHEMA_WARC_FETCH_LOG)
    n_saved = len(combined_log) if combined_log is not None else 0
    log.info(f"Saved warc_fetch_log -> {fetch_log_path}  ({n_saved} total rows)")

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    total      = len(all_results)
    successes  = sum(1 for r in all_results if r["download_success"])
    failures   = total - successes
    partials   = sum(1 for r in all_results if r["error_type"] == "PARTIAL")
    success_rate = successes / total if total > 0 else 0

    log.info("=" * 70)
    log.info(f"Downloaded this run  : {total}")
    log.info(f"Successful           : {successes}  ({success_rate:.1%})")
    log.info(f"Partial (byte mismatch): {partials}")
    log.info(f"Failed               : {failures}")
    log.info(f"Cache location       : {CACHE_DIR}/")
    if success_rate < DOWNLOAD_SUCCESS_RATE_FLOOR:
        log.warning(f"⚠ Overall success rate {success_rate:.1%} below {DOWNLOAD_SUCCESS_RATE_FLOOR:.0%} floor")
    else:
        log.info(f"Success rate {success_rate:.1%} [OK]")
    log.info("Next: run stage_05_extract_text.py")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
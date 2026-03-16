# =============================================================================
# stage_04_download_warc.py  ·  Flood Data Pipeline — Download WARC Slices
# =============================================================================
# REWRITE — replaces the requests + threading implementation.
#
# ROOT CAUSE OF THE OLD SLOWNESS:
#   Common Crawl throttles by drip-feeding bytes at ~1 KB/s continuously.
#   With requests + stream=True, Python never got a chance to fire a
#   wall-clock check between chunks because recv() never fully blocked —
#   data kept trickling in. Every worker was silently stuck for 2-5 minutes
#   per download. The "deferral" system couldn't trigger because downloads
#   technically never timed out; they just crawled.
#
# WHY aiohttp + asyncio FIXES THIS:
#   asyncio.wait_for(coro, timeout=N) cancels the coroutine at the event-loop
#   level using asyncio.CancelledError. The cancellation fires exactly at N
#   seconds regardless of whether bytes are trickling in. No workaround needed.
#   Additionally, asyncio runs 50 coroutines concurrently in a single thread
#   with zero GIL contention — all time is spent waiting on I/O.
#
# ARCHITECTURE:
#   ┌─────────────────────────────────────────────────────┐
#   │  Phase 1 — PRIMARY PASS (PRIMARY_TIMEOUT = 30s)     │
#   │  50 coroutines pull from primary_queue in parallel  │
#   │  ├─ Success / permanent failure -> record result     │
#   │  ├─ Timeout -> push to deferred_queue (not a failure)│
#   │  └─ 403/503 burst -> pause all workers briefly       │
#   ├─────────────────────────────────────────────────────┤
#   │  Phase 2 — DEFERRED PASS (DEFERRED_TIMEOUT = 300s)  │
#   │  Runs only after Phase 1 fully drains               │
#   │  No throughput floor — items run to completion      │
#   │  Whatever result comes back is final                │
#   └─────────────────────────────────────────────────────┘
#
# SEAMLESS WITH OTHER STAGES:
#   - Reads:  output/validated_pointers.parquet   (from stage_03)
#   - Writes: output/warc_fetch_log.parquet        (read by stage_05)
#             cache/{flood_id}/{pointer_id}.warc.gz (read by stage_05)
#   - Schema: SCHEMA_WARC_FETCH_LOG from config.py — unchanged
#   - CLI flags unchanged: --full, --all, --flood-id, --random
#
# INSTALL:
#   pip install aiohttp
#   (remove: pip uninstall requests urllib3  — not needed by this script)
#
# RUN:
#   python stage_04_download_warc.py                  # test batch (BATCH_SIZE/event)
#   python stage_04_download_warc.py --full           # all pointers, pilot events
#   python stage_04_download_warc.py --all            # Phase 2 — all 150 events
#   python stage_04_download_warc.py --flood-id 126   # single event debug
#   python stage_04_download_warc.py --flood-id 126 --full
#   python stage_04_download_warc.py --random         # fresh random sample
# =============================================================================

import argparse
import asyncio
import gzip
import importlib.util
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import pandas as pd
import sys
sys.stdout.reconfigure(encoding='utf-8')

# ---------------------------------------------------------------------------
# Force-load local config.py (works regardless of cwd)
# ---------------------------------------------------------------------------
_config_path = Path(__file__).parent / "config.py"
_spec        = importlib.util.spec_from_file_location("config", _config_path)
_config      = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)
sys.modules["config"] = _config

from config import (
    CACHE_DIR,
    CC_DATA_URL,
    DOWNLOAD_SUCCESS_RATE_FLOOR,
    LOGS_DIR,
    MAX_RETRIES,
    OUTPUT_DIR,
    PILOT_FLOOD_IDS,
    RETRY_BACKOFF_BASE,
    SCHEMA_WARC_FETCH_LOG,
)

# ---------------------------------------------------------------------------
# Constants that may not yet exist in config.py.
# If you add them to config.py later, import them above and remove these.
# ---------------------------------------------------------------------------
try:
    from config import BATCH_SIZE
except ImportError:
    BATCH_SIZE = 100                    # max pointers per event in test-batch mode

try:
    from config import PILOT_BATCH_SIZE
except ImportError:
    PILOT_BATCH_SIZE = BATCH_SIZE       # legacy alias — same value

try:
    from config import DOWNLOAD_INTER_REQUEST_SLEEP
except ImportError:
    DOWNLOAD_INTER_REQUEST_SLEEP = 1.0  # polite pause between requests per coroutine

# ---------------------------------------------------------------------------
# Tuning constants — adjust here, not in individual functions
# ---------------------------------------------------------------------------
CONCURRENT_DOWNLOADS    = 50      # coroutines in flight simultaneously
PRIMARY_TIMEOUT         = 30      # seconds — asyncio.wait_for hard cancel, primary pass
DEFERRED_TIMEOUT        = 300     # seconds — deferred pass, run to completion
MIN_THROUGHPUT_BPS      = 10_000  # 10 KB/s — abort primary pass if slower than this
MIN_THROUGHPUT_GRACE    = 5       # seconds before throughput check activates
CHECKPOINT_EVERY        = 500     # flush fetch log every N completed downloads
RATE_LIMIT_PAUSE        = 120     # seconds to pause when 403/503 burst detected
RATE_LIMIT_THRESHOLD    = 5       # consecutive 403/503 results before pausing
TIMEOUT_BURST_PAUSE     = 60      # seconds to pause on silent CC throttle (hung connections)
TIMEOUT_BURST_THRESHOLD = 20      # consecutive primary-pass timeouts before pausing
INTER_REQUEST_SLEEP     = 0.3     # polite delay per coroutine — keeps rate ~25/s sustained
WATCHDOG_INTERVAL       = 5       # watchdog polls every N seconds
WATCHDOG_STALL_SECS     = 40      # if completed hasn't moved in this long, all slots are hung
CHUNK_SIZE              = 65_536  # 64 KB read chunks from aiohttp response

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


# =============================================================================
# Cache helpers
# =============================================================================

# Directories created this session — avoids a blocking mkdir syscall (0.15ms)
# on every cache_path_for() call. Called up to 3x per download; at 200k
# downloads this saves ~90s of unnecessary blocking I/O.
_CREATED_DIRS: set = set()


def ensure_cache_dir(flood_id: int) -> Path:
    """Create cache dir for flood_id once per session, then return it."""
    if flood_id not in _CREATED_DIRS:
        d = CACHE_DIR / str(flood_id)
        d.mkdir(parents=True, exist_ok=True)
        _CREATED_DIRS.add(flood_id)
    return CACHE_DIR / str(flood_id)


def cache_path_for(flood_id: int, pointer_id: str) -> Path:
    """Return the local filesystem path where a WARC slice is cached."""
    return ensure_cache_dir(flood_id) / f"{pointer_id}.warc.gz"


def is_cached(flood_id: int, pointer_id: str, expected_length: int = 0) -> bool:
    """
    True if a complete cache file exists.
    If expected_length is provided, also validates file is not suspiciously
    small (< 95% of expected) — catches incomplete downloads from prior runs.
    """
    path = cache_path_for(flood_id, pointer_id)
    if not path.exists() or path.stat().st_size == 0:
        return False
    if expected_length > 0 and path.stat().st_size < expected_length * 0.95:
        log.debug(f"  Partial cache for {pointer_id} ({path.stat().st_size}B < {expected_length}B) — will re-download")
        path.unlink(missing_ok=True)
        return False
    return True


# =============================================================================
# WARC header parser
# =============================================================================

def parse_warc_type(data: bytes) -> str:
    """
    Extract the WARC-Type field from raw WARC bytes.
    Returns 'response', 'request', 'warcinfo', etc., or 'unknown' on failure.
    We want 'response' records — that's where the HTML content lives.
    """
    try:
        end = data.find(b"\r\n\r\n")
        if end == -1:
            return "unknown"
        header = data[:end].decode("utf-8", errors="replace")
        for line in header.splitlines():
            if line.lower().startswith("warc-type:"):
                return line.split(":", 1)[1].strip().lower()
    except Exception:
        pass
    return "unknown"


# =============================================================================
# Result builder — identical schema to previous version
# =============================================================================

def _build_result(
    pointer_id:     str,
    flood_id:       int,
    cache_file:     Path,
    length:         int,
    http_status:    int,
    bytes_received: int,
    bytes_match:    bool,
    attempt:        int,
    error_type:     str = "",
    error_message:  str = "",
    success:        bool = True,
) -> dict:
    """
    Build a warc_fetch_log row dict.
    Schema matches SCHEMA_WARC_FETCH_LOG in config.py exactly.
    stage_05 reads: download_success, local_cache_path, flood_id, pointer_id, fetched_at.
    """
    return {
        "pointer_id":       pointer_id,
        "flood_id":         flood_id,
        "download_success": success,
        "http_status":      http_status,
        "bytes_received":   bytes_received,
        "bytes_expected":   length,
        "bytes_match":      bytes_match,
        "error_type":       error_type,
        "error_message":    error_message,
        "retry_count":      attempt,
        "local_cache_path": str(cache_file) if success else "",
        "fetched_at":       datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# Single download attempt — one pointer, one try
# =============================================================================

async def _attempt(
    session:     aiohttp.ClientSession,
    row:         pd.Series,
    attempt:     int,
    is_deferred: bool,
) -> dict:
    """
    One HTTP Range GET for a single WARC pointer.

    is_deferred=False (primary pass):
        - Throughput floor: abort if < MIN_THROUGHPUT_BPS after grace period
        - asyncio.wait_for outside this call enforces PRIMARY_TIMEOUT

    is_deferred=True (deferred pass):
        - No throughput floor — let it run to completion
        - asyncio.wait_for outside enforces DEFERRED_TIMEOUT

    Returns a result dict. Does NOT retry. Does NOT sleep. Caller decides next step.
    """
    pointer_id = str(row["pointer_id"])
    flood_id   = int(row["flood_id"])
    filename   = str(row["filename"])
    offset     = int(row["offset"])
    length     = int(row["length"])
    cache_file = cache_path_for(flood_id, pointer_id)
    warc_url   = CC_DATA_URL.format(filename=filename)
    byte_range = f"bytes={offset}-{offset + length - 1}"

    def _fail(http_status, bytes_rx, error_type, msg):
        return _build_result(
            pointer_id, flood_id, cache_file, length,
            http_status, bytes_rx, False, attempt,
            error_type, msg, success=False,
        )

    try:
        async with session.get(warc_url, headers={"Range": byte_range}) as resp:
            http_status = resp.status

            # ── Permanent failure — no value in retrying ─────────────────
            if http_status == 404:
                return _fail(404, 0, "HTTPError", "HTTP 404 Not Found")

            # ── Rate limit — caller will back off ────────────────────────
            if http_status in (403, 503):
                return _fail(http_status, 0, "RateLimit", f"HTTP {http_status}")

            # ── Other unexpected status ───────────────────────────────────
            if http_status not in (200, 206):
                return _fail(http_status, 0, "HTTPError", f"HTTP {http_status}")

            # ── Stream response with throughput floor (primary pass only) ─
            chunks       = []
            bytes_so_far = 0
            t_start      = time.monotonic()

            async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                if chunk:
                    chunks.append(chunk)
                    bytes_so_far += len(chunk)

                # Primary-pass only: abort if drip-throttled
                if not is_deferred and bytes_so_far > 0:
                    elapsed = time.monotonic() - t_start
                    if elapsed > MIN_THROUGHPUT_GRACE:
                        rate_bps = bytes_so_far / elapsed
                        if rate_bps < MIN_THROUGHPUT_BPS:
                            # Raise asyncio.TimeoutError so caller treats this
                            # identically to a wall-clock timeout and defers it
                            raise asyncio.TimeoutError(
                                f"throughput {rate_bps / 1024:.1f} KB/s "
                                f"< floor {MIN_THROUGHPUT_BPS // 1024} KB/s"
                            )

            # ── Assemble, validate, and cache ────────────────────────────
            raw_data       = b"".join(chunks)
            bytes_received = len(raw_data)
            bytes_match    = (bytes_received == length)

            # Decompress + WARC type check + disk write all run in a thread
            # so they don't block the event loop. gzip.decompress is CPU-bound
            # (0.9ms/file) and write_bytes is blocking I/O (0.12ms/file) —
            # both freeze ALL coroutines if run on the event loop thread.
            def _process_and_cache():
                try:
                    decomp = gzip.decompress(raw_data)
                except (gzip.BadGzipFile, OSError):
                    decomp = raw_data
                warc_t = parse_warc_type(decomp)
                cache_file.write_bytes(raw_data)
                return warc_t

            warc_type = await asyncio.to_thread(_process_and_cache)
            if warc_type not in ("response", "unknown"):
                log.debug(f"  WARC type={warc_type} for {pointer_id} (non-response)")

            return _build_result(
                pointer_id, flood_id, cache_file, length,
                http_status, bytes_received, bytes_match, attempt,
                "" if bytes_match else "PARTIAL",
                "" if bytes_match else f"got {bytes_received}, expected {length}",
                success=True,
            )

    except asyncio.TimeoutError as exc:
        return _fail(0, 0, "Timeout", str(exc) or "asyncio timeout")

    # NOTE: Do NOT catch asyncio.CancelledError here.
    # asyncio.wait_for() cancels this coroutine by injecting CancelledError.
    # If we catch and swallow it, wait_for hangs indefinitely waiting for
    # acknowledgement — the semaphore slot is held forever, and once all
    # CONCURRENT_DOWNLOADS slots fill up with stuck wait_for calls, the
    # entire pipeline freezes. Let CancelledError propagate; wait_for
    # converts it to TimeoutError for the caller in download_one().

    except aiohttp.ServerDisconnectedError as exc:
        return _fail(0, 0, "ConnectionError", f"server disconnected: {exc}")

    except aiohttp.ClientConnectionError as exc:
        return _fail(0, 0, "ConnectionError", str(exc))

    except Exception as exc:
        return _fail(0, 0, type(exc).__name__, str(exc))


# =============================================================================
# Single-pointer downloader with retry and defer logic
# =============================================================================

async def download_one(
    session:        aiohttp.ClientSession,
    row:            pd.Series,
    sem:            asyncio.Semaphore,
    is_deferred:    bool,
    rate_limit_evt: asyncio.Event,
) -> dict:
    """
    Download one WARC pointer with retry on transient failures.

    Decision tree after each attempt:
        success          -> return result immediately
        404              -> permanent failure, no retry
        Timeout          -> return immediately (no inline retry; caller defers)
        403/503          -> exponential backoff + retry up to MAX_RETRIES
        other error      -> exponential backoff + retry up to MAX_RETRIES

    The semaphore slot is held ONLY during the actual HTTP request, not during
    sleeps — other coroutines can use the slot while we wait.

    rate_limit_evt is cleared while a rate-limit pause is in progress;
    all coroutines await it before acquiring the semaphore.
    """
    pointer_id = str(row["pointer_id"])
    flood_id   = int(row["flood_id"])
    length     = int(row["length"])
    timeout_s  = DEFERRED_TIMEOUT if is_deferred else PRIMARY_TIMEOUT

    # Cache hit — skip entirely, treat as immediate success
    if is_cached(flood_id, pointer_id, expected_length=length):
        log.debug(f"  Cache hit: {pointer_id}")
        cache_file = cache_path_for(flood_id, pointer_id)
        return _build_result(
            pointer_id, flood_id, cache_file, length,
            200, length, True, 0,
        )

    last_result = None
    for attempt in range(MAX_RETRIES + 1):
        # Wait if a rate-limit pause is in progress
        await rate_limit_evt.wait()

        async with sem:
            try:
                result = await asyncio.wait_for(
                    _attempt(session, row, attempt, is_deferred),
                    timeout=timeout_s,
                )
            except asyncio.TimeoutError:
                cache_file = cache_path_for(flood_id, pointer_id)
                result = _build_result(
                    pointer_id, flood_id, cache_file, length,
                    0, 0, False, attempt,
                    "Timeout", f"wait_for cancelled after {timeout_s}s",
                    success=False,
                )

        last_result = result
        if result["download_success"]:
            # Small polite delay after each success. asyncio.sleep(0) was triggering
            # CC's silent throttle (~2100 req burst -> hung connections, not 403s).
            # 0.2s with 50 coroutines = ~25 req/s sustained, which CC tolerates.
            await asyncio.sleep(INTER_REQUEST_SLEEP)
            return result

        error_type  = result.get("error_type", "")
        http_status = result.get("http_status", 0)

        # Permanent — stop immediately
        if http_status == 404:
            return result

        # Timeout on primary pass — return now; worker will defer to Phase 2.
        # Retrying inline would just burn another PRIMARY_TIMEOUT seconds.
        # On deferred pass, return the final timeout result as-is.
        if error_type == "Timeout":
            return result

        # Transient (403/503/connection error) — backoff and retry
        if http_status in (403, 503):
            wait = RETRY_BACKOFF_BASE ** (attempt + 3)   # longer wait for rate limits
        else:
            wait = RETRY_BACKOFF_BASE ** attempt

        if attempt < MAX_RETRIES:
            log.debug(f"  Retry {attempt + 1}/{MAX_RETRIES} for {pointer_id} in {wait:.0f}s")
            await asyncio.sleep(wait)

    return last_result


# =============================================================================
# Fetch log persistence
# =============================================================================

def save_fetch_log(
    new_results:    list[dict],
    existing_logs:  list[pd.DataFrame],
    fetch_log_path: Path,
) -> pd.DataFrame | None:
    """
    Merge new_results with existing_logs and write to parquet.
    Enforces SCHEMA_WARC_FETCH_LOG column order.
    Returns the combined DataFrame, or None if nothing to save.
    """
    if not new_results:
        return None
    new_df           = pd.DataFrame(new_results)
    new_df["flood_id"] = new_df["flood_id"].astype(int)
    for col in SCHEMA_WARC_FETCH_LOG:
        if col not in new_df.columns:
            new_df[col] = None
    combined = pd.concat(
        existing_logs + [new_df[SCHEMA_WARC_FETCH_LOG]],
        ignore_index=True,
    )
    combined.to_parquet(fetch_log_path, index=False)
    return combined


# =============================================================================
# Two-phase batch downloader
# =============================================================================

async def run_batch(
    rows:           list[pd.Series],
    label:          str,
    all_results:    list[dict],
    existing_logs:  list[pd.DataFrame],
    fetch_log_path: Path,
    shutdown_flag:  list[bool],           # mutable singleton so signal handler can set it
) -> list[dict]:
    """
    Download a batch of pointers in two explicit phases.

    Phase 1 — Primary (PRIMARY_TIMEOUT):
        N coroutines run concurrently from primary list.
        Timed-out items are pushed to deferred list.
        All other outcomes are recorded immediately.

    Phase 2 — Deferred (DEFERRED_TIMEOUT):
        Runs only after Phase 1 fully completes.
        Slow/large items given DEFERRED_TIMEOUT to finish.
        No further deferral — result is final.

    Shared state is mutated only inside the event loop, so no locks needed.
    """
    results        = []
    n_total        = len(rows)
    completed      = 0
    failed         = 0
    n_deferred     = 0
    total_bytes    = 0
    consec_rl      = 0           # consecutive rate-limit (403/503) results
    consec_timeout = 0           # consecutive primary-pass timeouts (CC silent throttle)
    start_time     = time.monotonic()

    # Semaphore: max CONCURRENT_DOWNLOADS requests in flight at once
    sem            = asyncio.Semaphore(CONCURRENT_DOWNLOADS)

    # rate_limit_evt: set=OK to proceed, clear=pausing due to rate limit
    rate_limit_evt = asyncio.Event()
    rate_limit_evt.set()   # start in "OK to proceed" state

    def _record(result: dict):
        """Update all shared counters and record the result. Called in the event loop."""
        nonlocal completed, failed, total_bytes, consec_rl

        results.append(result)
        all_results.append(result)
        completed  += 1
        total_bytes += result.get("bytes_received", 0) or 0

        if result["download_success"]:
            consec_rl = 0
        else:
            http_status = result.get("http_status", 0)
            failed     += 1
            if http_status in (403, 503):
                consec_rl += 1
            else:
                consec_rl = 0

        # Checkpoint flush
        if completed % CHECKPOINT_EVERY == 0:
            save_fetch_log(all_results, existing_logs, fetch_log_path)
            log.info(f"  [OK] Checkpoint: {completed}/{n_total} complete, {n_deferred} deferred")

        # Progress line every 100 completions
        if completed % 100 == 0 or completed == n_total:
            elapsed      = time.monotonic() - start_time
            rate         = completed / elapsed if elapsed > 0 else 0
            mbps         = total_bytes / elapsed / 1_048_576 if elapsed > 0 else 0
            success_rate = (completed - failed) / completed if completed > 0 else 0
            remaining    = n_total - completed
            eta_s        = remaining / rate if rate > 0 else 0
            eta_str      = (
                f"{int(eta_s // 3600)}h {int((eta_s % 3600) // 60)}m"
                if eta_s > 60 else f"{int(eta_s)}s"
            )
            log.info(
                f"    [{label}] {completed}/{n_total}  "
                f"success={success_rate:.1%}  failed={failed}  "
                f"deferred={n_deferred}  "
                f"rate={rate:.1f}/s  speed={mbps:.2f} MB/s  ETA={eta_str}"
            )
            if completed >= 20 and success_rate < DOWNLOAD_SUCCESS_RATE_FLOOR:
                log.warning(
                    f"    ⚠ Success rate {success_rate:.1%} below "
                    f"floor {DOWNLOAD_SUCCESS_RATE_FLOOR:.0%} — check for pipeline issues"
                )

    async def _maybe_pause_rate_limit():
        """
        If consecutive rate-limit responses hit the threshold, pause all
        coroutines by clearing rate_limit_evt, sleeping, then setting it again.
        Only one coroutine should do this at a time; others will just wait on
        rate_limit_evt.wait() in download_one().
        """
        if consec_rl >= RATE_LIMIT_THRESHOLD and rate_limit_evt.is_set():
            rate_limit_evt.clear()
            log.warning(
                f"  ↓ {consec_rl} consecutive rate-limits — "
                f"pausing all coroutines for {RATE_LIMIT_PAUSE}s..."
            )
            save_fetch_log(all_results, existing_logs, fetch_log_path)
            await asyncio.sleep(RATE_LIMIT_PAUSE)
            log.info("  ↑ Resuming after rate-limit pause")

    async def _maybe_pause_timeout_burst():
        """
        CC also throttles by silently hanging connections — these return
        error_type=Timeout, not 403, so consec_rl never increments.
        If too many primary-pass timeouts pile up, pause the same way.
        """
        nonlocal consec_timeout
        if consec_timeout >= TIMEOUT_BURST_THRESHOLD and rate_limit_evt.is_set():
            rate_limit_evt.clear()
            log.warning(
                f"  ↓ {consec_timeout} consecutive timeouts — CC is silently throttling. "
                f"Pausing all coroutines for {TIMEOUT_BURST_PAUSE}s..."
            )
            save_fetch_log(all_results, existing_logs, fetch_log_path)
            await asyncio.sleep(TIMEOUT_BURST_PAUSE)
            consec_timeout = 0
            log.info("  ↑ Resuming after timeout-burst pause")
            rate_limit_evt.set()

    # ------------------------------------------------------------------
    # aiohttp session — shared across all coroutines in this batch
    # ------------------------------------------------------------------
    connector = aiohttp.TCPConnector(
        limit=CONCURRENT_DOWNLOADS,        # one connection per coroutine — no pool surplus
        ttl_dns_cache=300,                 # cache DNS for 5 minutes
        enable_cleanup_closed=True,
        force_close=True,                  # never reuse connections — prevents hung CC
                                           # connections from staying in pool and blocking
                                           # the next request before wait_for even starts
    )
    session_timeout = aiohttp.ClientTimeout(
        total=None,         # we control overall timeout via asyncio.wait_for
        connect=10,         # TCP connect: 10s hard limit
        sock_read=35,       # per-chunk read: 35s — catches hangs aiohttp-side too,
                            # as a belt-and-suspenders backup to asyncio.wait_for
    )
    headers = {
        "User-Agent": "flood-pipeline/2.0 (academic research)",
    }

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=session_timeout,
        headers=headers,
    ) as session:

        # ----------------------------------------------------------------
        # Phase 1 — Primary pass
        # ----------------------------------------------------------------
        log.info(
            f"  [{label}] Phase 1 — {n_total} pointers  "
            f"concurrency={CONCURRENT_DOWNLOADS}  timeout={PRIMARY_TIMEOUT}s  "
            f"throughput floor={MIN_THROUGHPUT_BPS // 1024}KB/s"
        )

        deferred_rows = []

        # Queue-based bounded pool: only CONCURRENT_DOWNLOADS coroutines are
        # alive at any time. The old asyncio.gather(*[worker(r) for r in rows])
        # created ALL coroutines at once — at 200k rows that's ~586MB of
        # suspended coroutine frames in memory with GC tracking all of them.
        primary_q = asyncio.Queue()
        for row in rows:
            primary_q.put_nowait(row)

        async def primary_pool_worker():
            nonlocal n_deferred, consec_timeout
            while True:
                try:
                    row = primary_q.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if shutdown_flag[0]:
                    primary_q.task_done()
                    break
                result = await download_one(
                    session, row, sem,
                    is_deferred=False,
                    rate_limit_evt=rate_limit_evt,
                )
                if result["error_type"] == "Timeout":
                    deferred_rows.append(row)
                    n_deferred += 1
                    consec_timeout += 1
                    log.debug(f"  -> Deferred {row['pointer_id']} (timeout streak={consec_timeout})")
                    await _maybe_pause_timeout_burst()
                else:
                    consec_timeout = 0
                    _record(result)
                    await _maybe_pause_rate_limit()
                primary_q.task_done()

        # Watchdog runs alongside workers. Every WATCHDOG_INTERVAL seconds it
        # checks whether `completed` has advanced. If not — all slots are hung
        # (CC silently holding connections) — it forces a pause immediately
        # without waiting for individual sock_read timeouts to fire one by one.
        _last_completed = [0]  # mutable so coroutine can see updates

        async def _watchdog():
            nonlocal consec_timeout
            await asyncio.sleep(WATCHDOG_INTERVAL)
            while not primary_q.empty() or primary_q._unfinished_tasks > 0:
                await asyncio.sleep(WATCHDOG_INTERVAL)
                if completed == _last_completed[0] and completed < n_total:
                    stall_s = WATCHDOG_INTERVAL
                    if stall_s >= WATCHDOG_STALL_SECS and rate_limit_evt.is_set():
                        rate_limit_evt.clear()
                        log.warning(
                            f"  ⏱ Watchdog: no progress for {stall_s}s — "
                            f"all {CONCURRENT_DOWNLOADS} slots appear hung. "
                            f"Pausing {TIMEOUT_BURST_PAUSE}s to let CC recover..."
                        )
                        save_fetch_log(all_results, existing_logs, fetch_log_path)
                        await asyncio.sleep(TIMEOUT_BURST_PAUSE)
                        consec_timeout = 0
                        log.info("  ↑ Watchdog: resuming after stall pause")
                        rate_limit_evt.set()
                _last_completed[0] = completed

        await asyncio.gather(
            *[primary_pool_worker() for _ in range(CONCURRENT_DOWNLOADS)],
            _watchdog(),
            return_exceptions=True,
        )

        # ----------------------------------------------------------------
        # Phase 2 — Deferred pass
        # ----------------------------------------------------------------
        if deferred_rows and not shutdown_flag[0]:
            log.info(
                f"  [{label}] Phase 2 — {len(deferred_rows)} deferred items  "
                f"timeout={DEFERRED_TIMEOUT}s  (no throughput floor)"
            )

            deferred_q = asyncio.Queue()
            for row in deferred_rows:
                deferred_q.put_nowait(row)

            async def deferred_pool_worker():
                while True:
                    try:
                        row = deferred_q.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    if shutdown_flag[0]:
                        deferred_q.task_done()
                        break
                    result = await download_one(
                        session, row, sem,
                        is_deferred=True,
                        rate_limit_evt=rate_limit_evt,
                    )
                    _record(result)
                    await _maybe_pause_rate_limit()
                    deferred_q.task_done()

            await asyncio.gather(
                *[deferred_pool_worker() for _ in range(min(CONCURRENT_DOWNLOADS, len(deferred_rows)))],
                return_exceptions=True,
            )
        elif not deferred_rows:
            log.info(f"  [{label}] Phase 2 — no deferred items [OK]")

    return results


# =============================================================================
# Main entry point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Stage 04 — Download WARC slices (aiohttp + asyncio)"
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Download all valid pointers (default: BATCH_SIZE/event test batch)",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Process all 150 events — Phase 2 full run (implies --full)",
    )
    parser.add_argument(
        "--flood-id", type=int,
        help="Process a single flood_id only (debug)",
    )
    parser.add_argument(
        "--random", action="store_true",
        help="Draw a fresh random sample (default: resume saved sample if one exists)",
    )
    args    = parser.parse_args()
    is_full = args.full or args.all

    log.info("=" * 70)
    log.info("STAGE 04 — DOWNLOAD WARC SLICES  [aiohttp + asyncio rewrite]")
    log.info(f"Mode          : {'FULL' if is_full else f'TEST BATCH ({BATCH_SIZE}/event)'}")
    log.info(f"Concurrency   : {CONCURRENT_DOWNLOADS} coroutines")
    log.info(f"Primary TO    : {PRIMARY_TIMEOUT}s  |  Deferred TO: {DEFERRED_TIMEOUT}s")
    log.info(f"Throughput fl : {MIN_THROUGHPUT_BPS // 1024} KB/s (primary pass)")
    log.info(f"Max retries   : {MAX_RETRIES}")
    log.info(f"Sleep/request : {DOWNLOAD_INTER_REQUEST_SLEEP}s")
    log.info("=" * 70)

    # ------------------------------------------------------------------
    # Load validated pointers from stage_03
    # ------------------------------------------------------------------
    pointers_path = OUTPUT_DIR / "validated_pointers.parquet"
    if not pointers_path.exists():
        log.error("validated_pointers.parquet not found — run stage_03 first")
        sys.exit(1)

    pointers_df = pd.read_parquet(pointers_path)
    log.info(f"Total valid pointers    : {len(pointers_df)}")

    # Only eligible pointers: VALID status, not duplicate, not TOO_LARGE
    eligible = pointers_df[
        (pointers_df["status"] == "VALID") &
        (~pointers_df["is_pointer_duplicate"].astype(bool)) &
        (pointers_df["size_filter_status"] == "VALID")
    ].copy()
    log.info(f"Eligible for download   : {len(eligible)}")

    # Filter by flood_id or pilot set
    if args.flood_id:
        eligible = eligible[eligible["flood_id"] == args.flood_id].copy()
        log.info(f"Filtered to flood #{args.flood_id}: {len(eligible)} pointers")
    elif not args.all:
        eligible = eligible[eligible["flood_id"].isin(PILOT_FLOOD_IDS)].copy()
        log.info(f"Filtered to pilot events {PILOT_FLOOD_IDS}: {len(eligible)} pointers")

    # ------------------------------------------------------------------
    # Skip already-cached pointers
    # ------------------------------------------------------------------
    log.info("Checking cache...")
    already_cached = eligible.apply(
        lambda r: is_cached(int(r["flood_id"]), str(r["pointer_id"])), axis=1
    )
    to_download = eligible[~already_cached].copy()
    log.info(f"Already cached          : {already_cached.sum()}")
    log.info(f"Remaining to download   : {len(to_download)}")

    # ------------------------------------------------------------------
    # Apply batch limit or resume saved sample
    # ------------------------------------------------------------------
    sample_path = OUTPUT_DIR / "batch_sample_pointers.parquet"

    if not is_full:
        if not args.random and sample_path.exists():
            # Resume from the same sample chosen in a previous run
            saved_sample    = pd.read_parquet(sample_path)
            total_in_sample = len(saved_sample)
            in_sample       = to_download["pointer_id"].isin(saved_sample["pointer_id"])
            already_done    = total_in_sample - in_sample.sum()
            to_download     = to_download[in_sample].copy()
            log.info(
                f"Resumed sample          : {total_in_sample} total  |  "
                f"{already_done} already cached  |  {len(to_download)} remaining"
            )
        else:
            # Draw a new sample — BATCH_SIZE per event
            batches = []
            for flood_id, group in to_download.groupby("flood_id"):
                n = min(BATCH_SIZE, len(group))
                batches.append(group.sample(n, random_state=42))
            if batches:
                to_download = pd.concat(batches, ignore_index=True)
            mode = "random" if args.random else "new"
            to_download[["pointer_id", "flood_id"]].to_parquet(sample_path, index=False)
            log.info(
                f"Batch sample            : {len(to_download)} pointers ({mode} — saved)"
            )

    if to_download.empty:
        log.info("Nothing to download — all pointers already cached.")
        sys.exit(0)

    # ------------------------------------------------------------------
    # Load existing fetch log (accumulate across runs)
    # ------------------------------------------------------------------
    fetch_log_path  = OUTPUT_DIR / "warc_fetch_log.parquet"
    existing_logs: list[pd.DataFrame] = []
    if fetch_log_path.exists():
        existing_logs = [pd.read_parquet(fetch_log_path)]
        log.info(f"Loaded {len(existing_logs[0])} existing fetch log rows")

    # ------------------------------------------------------------------
    # Shutdown handler — fires on Ctrl+C or SIGTERM
    # Using a mutable list as a flag so the async coroutines can see it
    # ------------------------------------------------------------------
    all_results:  list[dict] = []
    shutdown_flag: list[bool] = [False]

    def _handle_shutdown(signum, frame):
        log.info(f"Signal {signum} received — saving checkpoint and exiting...")
        shutdown_flag[0] = True
        if all_results:
            save_fetch_log(all_results, existing_logs, fetch_log_path)
            log.info(f"Checkpoint saved — {len(all_results)} results written.")
        else:
            log.info("No results to save yet.")
        os._exit(0)

    signal.signal(signal.SIGINT,  _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    try:
        signal.signal(signal.SIGQUIT, _handle_shutdown)
    except AttributeError:
        pass   # SIGQUIT not available on Windows

    # ------------------------------------------------------------------
    # Run — one batch per flood_id for clean progress reporting
    # ------------------------------------------------------------------
    async def _run_all():
        for flood_id, group in to_download.groupby("flood_id"):
            if shutdown_flag[0]:
                break

            rows  = [row for _, row in group.iterrows()]
            label = f"flood_{int(flood_id)}"
            log.info(f"\n--- Flood #{int(flood_id)} ({len(rows)} pointers) ---")

            batch_results = await run_batch(
                rows           = rows,
                label          = label,
                all_results    = all_results,
                existing_logs  = existing_logs,
                fetch_log_path = fetch_log_path,
                shutdown_flag  = shutdown_flag,
            )

            # Per-event summary
            successes  = sum(1 for r in batch_results if r["download_success"])
            timeouts   = sum(1 for r in batch_results if r.get("error_type") == "Timeout")
            failures   = len(batch_results) - successes
            rate       = successes / len(batch_results) if batch_results else 0
            log.info(
                f"  Flood #{int(flood_id)} complete — "
                f"success={successes}  failed={failures}  "
                f"final_timeouts={timeouts}  rate={rate:.1%}"
            )

    asyncio.run(_run_all())

    # ------------------------------------------------------------------
    # Final save and summary
    # ------------------------------------------------------------------
    combined = save_fetch_log(all_results, existing_logs, fetch_log_path)
    n_saved  = len(combined) if combined is not None else 0
    log.info(f"\nSaved warc_fetch_log -> {fetch_log_path}  ({n_saved} total rows)")

    # Overall stats
    total        = len(all_results)
    successes    = sum(1 for r in all_results if r["download_success"])
    failures     = total - successes
    partials     = sum(1 for r in all_results if r.get("error_type") == "PARTIAL")
    timeouts     = sum(1 for r in all_results if r.get("error_type") == "Timeout")
    success_rate = successes / total if total > 0 else 0

    log.info("=" * 70)
    log.info("STAGE 04 COMPLETE")
    log.info(f"Downloaded this run      : {total}")
    log.info(f"Successful               : {successes}  ({success_rate:.1%})")
    log.info(f"  Byte-count mismatches  : {partials}")
    log.info(f"Failed                   : {failures}")
    log.info(f"  Final timeouts         : {timeouts}")
    log.info(f"Cache location           : {CACHE_DIR}/")
    if success_rate < DOWNLOAD_SUCCESS_RATE_FLOOR:
        log.warning(
            f"⚠ Success rate {success_rate:.1%} is below the "
            f"{DOWNLOAD_SUCCESS_RATE_FLOOR:.0%} floor — investigate before stage_05"
        )
    else:
        log.info(f"[OK] Success rate {success_rate:.1%} is above the floor")
    log.info("Next: python stage_05_extract_text.py")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
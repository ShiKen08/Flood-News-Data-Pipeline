#!/usr/bin/env python3
"""
logs.py  ·  Flood Pipeline — Log viewer

Shows the tail of every stage log in one place.

Usage:
    python3 logs.py              # last 20 lines of each log
    python3 logs.py -n 50        # last 50 lines
    python3 logs.py --errors     # only ERROR / WARNING lines
    python3 logs.py --stage 04   # only stage_04 log
"""

import argparse
import re
import sys
from pathlib import Path

# Load LOGS_DIR from config without triggering full pipeline imports
try:
    import importlib.util
    _spec = importlib.util.spec_from_file_location("config", Path(__file__).parent / "config.py")
    _cfg  = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_cfg)
    LOGS_DIR = Path(_cfg.LOGS_DIR)
except Exception:
    LOGS_DIR = Path(__file__).parent / "logs"

STAGE_ORDER = [
    "stage_00_preflight",
    "stage_01_query_specs",
    "stage_02_query_cc_index",
    "stage_03_validate_pointers",
    "stage_04_download_warc",
    "stage_05_extract_text",
    "stage_06_clean_deduplicate",
    "stage_07_url_report",
    "stage_08_nlp_analysis",
]

ERROR_RE = re.compile(r"\b(ERROR|WARNING|CRITICAL|Traceback|Exception|Error:)\b")


def tail(path: Path, n: int) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-n:] if len(lines) > n else lines
    except Exception as e:
        return [f"[could not read: {e}]"]


def last_modified(path: Path) -> str:
    try:
        import datetime
        ts = path.stat().st_mtime
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "?"


def main():
    parser = argparse.ArgumentParser(description="Pipeline log viewer")
    parser.add_argument("-n", type=int, default=20, help="Lines per log (default 20)")
    parser.add_argument("--errors", action="store_true", help="Only show ERROR/WARNING lines")
    parser.add_argument("--stage", type=str, default=None,
                        help="Filter to a specific stage number or name (e.g. '04' or 'stage_04')")
    args = parser.parse_args()

    if not LOGS_DIR.exists():
        print(f"logs/ directory not found at {LOGS_DIR}")
        sys.exit(1)

    # Collect log files — prefer STAGE_ORDER, then any remaining .log files
    ordered = []
    seen    = set()
    for stem in STAGE_ORDER:
        p = LOGS_DIR / f"{stem}.log"
        if p.exists():
            ordered.append(p)
            seen.add(p.name)
    for p in sorted(LOGS_DIR.glob("*.log")):
        if p.name not in seen:
            ordered.append(p)

    # Filter to specific stage if requested
    if args.stage:
        term = args.stage.lstrip("stage_").lstrip("0") or "0"
        ordered = [p for p in ordered if term in p.name]
        if not ordered:
            print(f"No log found matching '{args.stage}'")
            sys.exit(1)

    for log_path in ordered:
        lines = tail(log_path, args.n)
        if args.errors:
            lines = [l for l in lines if ERROR_RE.search(l)]
            if not lines:
                continue

        print(f"\n{'='*70}")
        print(f"  {log_path.name}   (modified {last_modified(log_path)})")
        print(f"{'='*70}")
        for line in lines:
            print(line)

    # Quick error summary across all logs
    if not args.errors and not args.stage:
        print(f"\n{'='*70}")
        print("  ERROR SUMMARY (all logs)")
        print(f"{'='*70}")
        any_errors = False
        for log_path in ordered:
            try:
                text = log_path.read_text(encoding="utf-8", errors="replace")
                errors = [l for l in text.splitlines() if ERROR_RE.search(l)]
                if errors:
                    any_errors = True
                    print(f"\n  [{log_path.name}]")
                    for e in errors[-5:]:   # last 5 errors per log
                        print(f"    {e.strip()}")
            except Exception:
                pass
        if not any_errors:
            print("  No errors found.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
generate_progress.py  ·  Flood Data Pipeline — Progress Manifest Generator

Run this after completing any pipeline stage to update the shared progress/
folder. Commit and push the results so teammates can see what's been done
and avoid re-downloading files that already exist.

Usage:
    python generate_progress.py                  # update all flood events
    python generate_progress.py --flood-id 12   # update one event

Output:
    progress/flood_{id:03d}_{iso}.json           # one file per event
    progress/summary.json                        # aggregate across all events
"""

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Force-load local config.py
_config_path = Path(__file__).parent / "config.py"
_spec = importlib.util.spec_from_file_location("config", _config_path)
_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)
sys.modules["config"] = _config

from config import (
    CACHE_DIR,
    OUTPUT_DIR,
    FLOOD_CRAWL_CSV,
)

PROGRESS_DIR = Path(__file__).parent / "progress"
PROGRESS_DIR.mkdir(exist_ok=True)

STAGES = {
    0: "preflight",
    1: "query_specs",
    2: "cc_index",
    3: "validated_pointers",
    4: "warc_download",
    5: "html_extraction",
    6: "clean_dedup_filter",
}

# Files that indicate a stage is complete for a given flood_id
STAGE_OUTPUT_FILES = {
    1: OUTPUT_DIR / "event_query_specs.parquet",
    2: OUTPUT_DIR / "raw_index_responses",        # directory
    3: OUTPUT_DIR / "validated_pointers.parquet",
    4: OUTPUT_DIR / "warc_fetch_log.parquet",
    5: OUTPUT_DIR / "extracted_text.parquet",
    6: OUTPUT_DIR / "clean_text.parquet",
}


def load_flood_events(flood_ids=None):
    df = pd.read_csv(FLOOD_CRAWL_CSV)
    if flood_ids:
        df = df[df["Flood_ID"].isin(flood_ids)]
    return df


def stages_complete_for(flood_id: int) -> list[int]:
    """Infer which stages are complete by checking output files for this flood_id."""
    complete = []

    # Stage 3: check validated_pointers for this flood_id
    vp_path = STAGE_OUTPUT_FILES[3]
    if vp_path.exists():
        try:
            vp = pd.read_parquet(vp_path)
            if "flood_id" in vp.columns and flood_id in vp["flood_id"].values:
                complete.extend([1, 2, 3])
        except Exception:
            pass

    # Stage 4: check warc_fetch_log for this flood_id
    wfl_path = STAGE_OUTPUT_FILES[4]
    if wfl_path.exists():
        try:
            wfl = pd.read_parquet(wfl_path)
            if "flood_id" in wfl.columns and flood_id in wfl["flood_id"].values:
                complete.append(4)
        except Exception:
            pass

    # Stage 5: check extracted_text for this flood_id
    et_path = STAGE_OUTPUT_FILES[5]
    if et_path.exists():
        try:
            et = pd.read_parquet(et_path)
            if "flood_id" in et.columns and flood_id in et["flood_id"].values:
                complete.append(5)
        except Exception:
            pass

    # Stage 6: check clean_text for this flood_id
    ud_path = STAGE_OUTPUT_FILES[6]
    if ud_path.exists():
        try:
            ud = pd.read_parquet(ud_path)
            if "flood_id" in ud.columns and flood_id in ud["flood_id"].values:
                complete.append(6)
        except Exception:
            pass

    return sorted(set(complete))


def download_stats_for(flood_id: int) -> dict:
    """Pull download stats from warc_fetch_log for this flood_id."""
    wfl_path = STAGE_OUTPUT_FILES[4]
    if not wfl_path.exists():
        return {}
    try:
        wfl = pd.read_parquet(wfl_path)
        rows = wfl[wfl["flood_id"] == flood_id]
        if rows.empty:
            return {}
        total       = len(rows)
        successful  = rows["download_success"].sum()
        return {
            "pointers_total":        int(total),
            "pointers_downloaded":   int(successful),
            "warc_fetch_success_rate": round(float(successful / total), 4) if total else 0.0,
        }
    except Exception:
        return {}


def cache_size_for(flood_id: int) -> str:
    """Return human-readable size of cached WARC files for this flood_id."""
    flood_cache = CACHE_DIR / str(flood_id)
    if not flood_cache.exists():
        return "0 MB"
    total_bytes = sum(f.stat().st_size for f in flood_cache.rglob("*.warc.gz"))
    if total_bytes < 1024 ** 2:
        return f"{total_bytes / 1024:.1f} KB"
    elif total_bytes < 1024 ** 3:
        return f"{total_bytes / 1024 ** 2:.1f} MB"
    else:
        return f"{total_bytes / 1024 ** 3:.2f} GB"


def generate_manifest(row: pd.Series) -> dict:
    flood_id    = int(row["Flood_ID"])
    iso         = str(row["ISO"])
    country     = str(row["Country"])
    stages_done = stages_complete_for(flood_id)
    dl_stats    = download_stats_for(flood_id)
    cache_size  = cache_size_for(flood_id)

    manifest = {
        "flood_id":        flood_id,
        "iso":             iso,
        "country":         country,
        "last_updated":    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stages_complete": stages_done,
        "cache_size":      cache_size,
        **dl_stats,
    }
    return manifest


def write_manifest(manifest: dict, iso: str, flood_id: int):
    filename = PROGRESS_DIR / f"flood_{flood_id:03d}_{iso.lower()}.json"
    with open(filename, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  ✓ flood_{flood_id:03d}_{iso.lower()}.json  "
          f"stages={manifest['stages_complete']}  "
          f"cache={manifest.get('cache_size', '—')}")


def write_summary(manifests: list[dict]):
    total_events        = len(manifests)
    fully_complete      = sum(1 for m in manifests if 6 in m["stages_complete"])
    download_complete   = sum(1 for m in manifests if 4 in m["stages_complete"])
    in_progress         = sum(1 for m in manifests if m["stages_complete"] and 6 not in m["stages_complete"])
    not_started         = sum(1 for m in manifests if not m["stages_complete"])

    summary = {
        "generated_at":      datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_events":      total_events,
        "fully_complete":    fully_complete,
        "download_complete": download_complete,
        "in_progress":       in_progress,
        "not_started":       not_started,
        "events":            [
            {
                "flood_id":        m["flood_id"],
                "iso":             m["iso"],
                "country":         m["country"],
                "stages_complete": m["stages_complete"],
                "cache_size":      m.get("cache_size", "0 MB"),
            }
            for m in sorted(manifests, key=lambda x: x["flood_id"])
        ],
    }

    out = PROGRESS_DIR / "summary.json"
    with open(out, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n  📊 summary.json — {fully_complete}/{total_events} events fully complete, "
          f"{not_started} not started")


def main():
    parser = argparse.ArgumentParser(description="Generate pipeline progress manifests")
    parser.add_argument("--flood-id", type=int, default=None,
                        help="Update a single flood event by ID")
    args = parser.parse_args()

    flood_ids = [args.flood_id] if args.flood_id else None
    events    = load_flood_events(flood_ids)

    print(f"\n🌊 Generating progress manifests for {len(events)} event(s)...\n")

    manifests = []
    for _, row in events.iterrows():
        manifest = generate_manifest(row)
        write_manifest(manifest, row["ISO"], int(row["Flood_ID"]))
        manifests.append(manifest)

    # Always regenerate summary over ALL events (not just the filtered set)
    if not flood_ids:
        write_summary(manifests)

    print("\n  Done. Commit and push progress/ to share with teammates.\n")
    print("  git add progress/")
    print("  git commit -m 'chore: update pipeline progress'")
    print("  git push\n")


if __name__ == "__main__":
    main()

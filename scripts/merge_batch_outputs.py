#!/usr/bin/env python3
"""
Merge per-batch output directories into the main output/ directory.

Run after all batch jobs complete:
    python3 scripts/merge_batch_outputs.py

Each batch wrote to output/batch_START_END/ — this script combines them
all into output/ using flood_id as the dedup key.
"""
from __future__ import annotations
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"


MERGE_KEY = "flood_id"  # dedup key for all pipeline parquets

# Parquets that should NOT be merged (stage_00 static lookups)
SKIP_FILES = {"crawl_coverage.parquet", "language_assignments.parquet", "location_dictionary.parquet"}


def merge_parquet(files: list[Path], out_path: Path) -> int:
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            print(f"  WARNING: could not read {f.name}: {e} — skipping")
    if not dfs:
        return 0
    combined = pd.concat(dfs, ignore_index=True)
    if MERGE_KEY in combined.columns:
        combined[MERGE_KEY] = combined[MERGE_KEY].astype(int)
        combined = combined.drop_duplicates(subset=[MERGE_KEY] + [
            c for c in ["url", "pointer_id", "doc_id", "hit_id"] if c in combined.columns
        ], keep="last")
    combined.to_parquet(out_path, index=False)
    return len(combined)


def main() -> None:
    batch_dirs = sorted(OUTPUT_DIR.glob("batch_*_*"))
    if not batch_dirs:
        print("No batch directories found in output/. Nothing to merge.")
        sys.exit(0)

    print(f"Found {len(batch_dirs)} batch directories:")
    for d in batch_dirs:
        print(f"  {d.name}")

    # Collect all unique parquet filenames across batch dirs
    all_files: set[str] = set()
    for d in batch_dirs:
        all_files.update(f.name for f in d.glob("*.parquet"))
    all_files -= SKIP_FILES

    print(f"\nMerging {len(all_files)} parquet files...")
    for fname in sorted(all_files):
        sources = [d / fname for d in batch_dirs if (d / fname).exists()]
        if not sources:
            continue
        out_path = OUTPUT_DIR / fname
        n = merge_parquet(sources, out_path)
        print(f"  {fname}: {len(sources)} batches → {n} rows → {out_path}")

    print("\nDone. You can now run:")
    print("  python3 scripts/build_annotation_batch_auto.py")
    print("  sbatch run_finetune.sh")


if __name__ == "__main__":
    main()

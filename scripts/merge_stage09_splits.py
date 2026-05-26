#!/usr/bin/env python3
"""
Merge the 4 split CSVs from run_stage09_split.sh into combined output files.

Run after all stage_09 split jobs complete:
    python3 scripts/merge_stage09_splits.py
"""

import csv
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"

SPLIT_PATTERN = "model_event_articles_multi_*_*_verified.csv"
FULL_PATTERN  = "model_event_articles_multi_*_*.csv"


def merge_csvs(pattern: str, out_name: str) -> int:
    parts = sorted(OUTPUT_DIR.glob(pattern))
    if not parts:
        print(f"No files matching {pattern}")
        return 0

    print(f"Merging {len(parts)} files for {out_name}:")
    for p in parts:
        print(f"  {p.name}")

    frames = [pd.read_csv(p) for p in parts]
    combined = pd.concat(frames, ignore_index=True)

    # Re-sort by flood_id, model_flood_prob
    if "flood_id" in combined.columns and "model_flood_prob" in combined.columns:
        combined = combined.sort_values(
            ["flood_id", "model_flood_prob"], ascending=[True, False]
        )

    # Reassign doc_num
    combined.insert(0, "doc_num", range(1, len(combined) + 1))
    if "doc_num" in combined.columns[1:]:
        cols = list(combined.columns)
        cols = [c for i, c in enumerate(cols) if c != "doc_num" or i == 0]
        combined = combined[cols]

    out_path = OUTPUT_DIR / out_name
    combined.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL)
    print(f"  -> {out_path}  ({len(combined)} rows)")
    return len(combined)


def main():
    print("=" * 60)
    print("Merging stage_09 split outputs")
    print("=" * 60)

    n_verified = merge_csvs(
        "model_event_articles_multi_*_*_verified.csv",
        "model_event_articles_multi_verified.csv",
    )
    n_full = merge_csvs(
        # full files don't have _verified suffix and don't end with a number
        # pattern: model_event_articles_multi_1_65.csv etc.
        "model_event_articles_multi_[0-9]*_[0-9]*.csv",
        "model_event_articles_multi.csv",
    )

    print()
    print(f"Done — {n_verified} verified rows, {n_full} full rows")
    print("Next: run cleaning pipeline on model_event_articles_multi_verified.csv")


if __name__ == "__main__":
    main()

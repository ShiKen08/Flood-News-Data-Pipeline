#!/usr/bin/env python3
"""
scripts/build_annotation_set.py  ·  Flood Pipeline — Annotation Set Builder

Samples a balanced set of articles from the pipeline output for manual labeling.
Run this on the cluster after stage_06 completes.

Strata (why three groups):
  positive      — is_event_article=True  → pipeline says "yes"; you verify
  hard_negative — is_relevant=True but is_event_article=False → boundary cases,
                  most valuable for training because the model must learn the
                  distinction the keyword rules miss
  easy_negative — is_relevant=False, usable text → clearly off-topic

Output: data/annotation_batch_<N>.csv
  Fill the 'label' column: 1 = genuine flood event article, 0 = not relevant
  Leave 'notes' blank or add a short reason for tricky cases.

Usage:
    python3 scripts/build_annotation_set.py               # batch 1, n=300
    python3 scripts/build_annotation_set.py --n 500       # larger batch
    python3 scripts/build_annotation_set.py --batch 2     # second batch (excludes batch 1)
    python3 scripts/build_annotation_set.py --floods 1 2  # specific flood IDs only
    python3 scripts/build_annotation_set.py --seed 99     # different random draw
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from config import (
    FLOOD_CSV,
    OUTPUT_DIR,
    LOGS_DIR,
    COL_FLOOD_ID,
    COL_COUNTRY,
    COL_LOCATION,
    COL_START_DATE,
    COL_END_DATE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sampling ratios per stratum
# ---------------------------------------------------------------------------

STRATA = {
    "positive":      0.40,   # is_event_article=True
    "hard_negative": 0.40,   # is_relevant=True, is_event_article=False
    "easy_negative": 0.20,   # is_relevant=False, usable
}

# Columns written to the annotation CSV (annotator sees these)
ANNOTATION_COLS = [
    "id",
    "flood_id",
    "country",
    "event_location",
    "event_dates",
    "language_detected",
    "url",
    "domain",
    "page_title",
    "text_snippet",          # first 600 chars of clean_text
    "pipeline_prediction",   # pipeline_yes / pipeline_boundary / pipeline_no
    "flood_term_hits",
    "impact_term_hits",
    "location_term_hits",
    "word_count",
    "label",                 # ← annotator fills: 1 = yes, 0 = no
    "notes",                 # ← annotator fills: optional free text
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_clean_text() -> pd.DataFrame:
    path = OUTPUT_DIR / "clean_text.parquet"
    if not path.exists():
        log.error("clean_text.parquet not found at %s — run stage_06 first", path)
        sys.exit(1)
    df = pd.read_parquet(path)
    log.info("Loaded clean_text.parquet: %d rows, %d cols", len(df), len(df.columns))
    return df


def load_flood_meta() -> pd.DataFrame:
    df = pd.read_csv(FLOOD_CSV)
    df = df.rename(columns={
        COL_FLOOD_ID:   "flood_id",
        COL_COUNTRY:    "country",
        COL_LOCATION:   "event_location",
        COL_START_DATE: "start_date",
        COL_END_DATE:   "end_date",
    })
    df["event_dates"] = df["start_date"].astype(str) + " → " + df["end_date"].astype(str)
    return df[["flood_id", "event_location", "event_dates"]].copy()


def load_existing_batches(data_dir: Path) -> set[str]:
    """Return URL set of already-annotated articles so we never duplicate."""
    seen: set[str] = set()
    for csv in sorted(data_dir.glob("annotation_batch_*.csv")):
        try:
            df = pd.read_csv(csv, usecols=["url"])
            seen.update(df["url"].dropna().tolist())
            log.info("Excluding %d URLs from %s", len(df), csv.name)
        except Exception as e:
            log.warning("Could not read %s: %s", csv.name, e)
    return seen


def make_snippet(text: str | None, max_chars: int = 600) -> str:
    if not text or not isinstance(text, str):
        return ""
    text = " ".join(text.split())          # collapse whitespace
    return text[:max_chars] + ("…" if len(text) > max_chars else "")


def stratum_label(row: pd.Series) -> str:
    if row.get("is_event_article", False):
        return "pipeline_yes"
    if row.get("is_relevant", False):
        return "pipeline_boundary"
    return "pipeline_no"


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def sample_stratum(
    pool: pd.DataFrame,
    n: int,
    seed: int,
    label: str,
) -> pd.DataFrame:
    if len(pool) == 0:
        log.warning("Stratum '%s' pool is empty — skipping", label)
        return pd.DataFrame()
    take = min(n, len(pool))
    if take < n:
        log.warning("Stratum '%s': asked %d, only %d available", label, n, take)
    return pool.sample(n=take, random_state=seed).copy()


def build_sample(df: pd.DataFrame, n_total: int, seed: int) -> pd.DataFrame:
    positives      = df[df["is_event_article"].fillna(False)]
    hard_negatives = df[df["is_relevant"].fillna(False) & ~df["is_event_article"].fillna(False)]
    easy_negatives = df[~df["is_relevant"].fillna(False)]

    log.info(
        "Pool sizes — positives: %d  hard_negatives: %d  easy_negatives: %d",
        len(positives), len(hard_negatives), len(easy_negatives),
    )

    if len(positives) < 20:
        log.error(
            "Only %d positive examples found — need at least 20 to build a useful annotation set. "
            "Run stage_06 on more floods first.", len(positives)
        )
        sys.exit(1)

    n_pos  = round(n_total * STRATA["positive"])
    n_hard = round(n_total * STRATA["hard_negative"])
    n_easy = n_total - n_pos - n_hard

    parts = [
        sample_stratum(positives,      n_pos,  seed,      "positive"),
        sample_stratum(hard_negatives, n_hard, seed + 1,  "hard_negative"),
        sample_stratum(easy_negatives, n_easy, seed + 2,  "easy_negative"),
    ]
    return pd.concat([p for p in parts if not p.empty], ignore_index=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Build annotation batch CSV")
    parser.add_argument("--n",      type=int, default=300, help="Total rows to sample (default 300)")
    parser.add_argument("--batch",  type=int, default=None, help="Batch number (auto-detected if omitted)")
    parser.add_argument("--floods", type=int, nargs="+",   help="Restrict to specific flood IDs")
    parser.add_argument("--seed",   type=int, default=42,  help="Random seed (default 42)")
    args = parser.parse_args()

    # --- Load data ---
    df   = load_clean_text()
    meta = load_flood_meta()

    # --- Flood filter ---
    if args.floods:
        df = df[df["flood_id"].isin(args.floods)]
        log.info("Filtered to floods %s: %d rows", args.floods, len(df))

    # --- Usability filter: need actual text ---
    has_text = df["clean_text"].notna() & (df["clean_text"].str.len() > 50)
    df = df[has_text].copy()
    log.info("After text filter: %d rows", len(df))

    # --- Exclude already-annotated ---
    data_dir = ROOT / "data"
    data_dir.mkdir(exist_ok=True)
    seen_urls = load_existing_batches(data_dir)
    if seen_urls:
        df = df[~df["url"].isin(seen_urls)]
        log.info("After excluding prior batches: %d rows", len(df))

    # --- Sample ---
    sampled = build_sample(df, args.n, args.seed)
    sampled = sampled.sample(frac=1, random_state=args.seed).reset_index(drop=True)  # shuffle

    # --- Enrich ---
    sampled = sampled.merge(meta, on="flood_id", how="left")

    # country may already exist from clean_text; use meta as fallback
    if "country" not in sampled.columns:
        flood_country = (
            pd.read_csv(FLOOD_CSV)
            .rename(columns={COL_FLOOD_ID: "flood_id", COL_COUNTRY: "country"})
            [["flood_id", "country"]]
        )
        sampled = sampled.merge(flood_country, on="flood_id", how="left")

    sampled["pipeline_prediction"] = sampled.apply(stratum_label, axis=1)
    sampled["text_snippet"]        = sampled["clean_text"].apply(make_snippet)
    sampled["id"]                  = range(1, len(sampled) + 1)
    sampled["label"]               = ""   # annotator fills
    sampled["notes"]               = ""   # annotator fills (optional)

    # --- Select output columns ---
    out_cols = [c for c in ANNOTATION_COLS if c in sampled.columns]
    missing  = [c for c in ANNOTATION_COLS if c not in sampled.columns]
    if missing:
        log.warning("Missing columns (will skip): %s", missing)

    result = sampled[out_cols].copy()

    # --- Determine batch number ---
    if args.batch is None:
        existing = sorted(data_dir.glob("annotation_batch_*.csv"))
        args.batch = len(existing) + 1

    out_path = data_dir / f"annotation_batch_{args.batch}.csv"
    result.to_csv(out_path, index=False)

    # --- Summary ---
    counts = sampled["pipeline_prediction"].value_counts()
    log.info("")
    log.info("=== Annotation Batch %d Written ===", args.batch)
    log.info("  File    : %s", out_path)
    log.info("  Rows    : %d", len(result))
    log.info("  Strata  :")
    for cat, count in counts.items():
        log.info("    %-22s %d", cat, count)
    log.info("")
    log.info("Next steps:")
    log.info("  1. Open %s in Excel or Google Sheets", out_path.name)
    log.info("  2. Fill 'label' column: 1 = genuine flood event article, 0 = not")
    log.info("  3. Save as CSV (keep same filename) and copy back to data/")
    log.info("  4. Run: python3 scripts/finetune_flood_classifier.py")


if __name__ == "__main__":
    main()

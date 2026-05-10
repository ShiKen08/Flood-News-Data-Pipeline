#!/usr/bin/env python3
"""
classifier/predict.py  ·  Flood Pipeline — Batch Inference

Runs the fine-tuned classifier on output/clean_text.parquet and writes:
  model_flood_prob        float  [0.0 – 1.0]  raw probability
  model_is_event_article  bool   True if prob ≥ threshold

Called by stage_09_classify.py — not usually run directly.

Usage (direct):
    python3 classifier/predict.py
    python3 classifier/predict.py --threshold 0.6
    python3 classifier/predict.py --floods 1 2 3
    python3 classifier/predict.py --batch-size 128  # faster on GPU
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

from config import OUTPUT_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MODEL_DIR       = ROOT / "classifier" / "model"
DEFAULT_THRESHOLD = 0.50
MAX_WORDS         = 300   # words from clean_text to use (keeps inference fast)


def load_model():
    try:
        from setfit import SetFitModel
    except ImportError:
        log.error("setfit not installed — run:  pip install setfit datasets")
        sys.exit(1)

    if not MODEL_DIR.exists():
        log.error("No model found at %s — run classifier/finetune.py first", MODEL_DIR)
        sys.exit(1)

    log.info("Loading model from %s", MODEL_DIR)
    return SetFitModel.from_pretrained(str(MODEL_DIR))


def prepare_texts(df: pd.DataFrame) -> list[str]:
    """Build input strings: title [SEP] first MAX_WORDS words of clean_text."""
    def _row(row):
        title = str(row.get("page_title",  "") or "").strip()
        text  = str(row.get("clean_text",  "") or "").strip()
        words = text.split()[:MAX_WORDS]
        body  = " ".join(words)
        return f"{title} [SEP] {body}"[:1000]

    return df.apply(_row, axis=1).tolist()


def run_inference(
    model,
    df: pd.DataFrame,
    threshold: float,
    batch_size: int,
) -> pd.DataFrame:
    texts = prepare_texts(df)
    n     = len(texts)
    log.info("Running inference on %d articles (batch_size=%d)...", n, batch_size)

    all_probs: list[float] = []
    for start in range(0, n, batch_size):
        batch = texts[start : start + batch_size]
        probs = model.predict_proba(batch)[:, 1]
        all_probs.extend(probs.tolist())
        done = min(start + batch_size, n)
        if done % (batch_size * 10) == 0 or done == n:
            log.info("  %d / %d", done, n)

    df = df.copy()
    df["model_flood_prob"]        = np.array(all_probs, dtype=np.float32)
    df["model_is_event_article"]  = df["model_flood_prob"] >= threshold
    return df


def print_summary(df: pd.DataFrame, threshold: float) -> None:
    n_pos = df["model_is_event_article"].sum()
    n_tot = len(df)
    log.info("")
    log.info("=== Inference Summary (threshold=%.2f) ===", threshold)
    log.info("  Total articles     : %d", n_tot)
    log.info("  model_is_event_article=True : %d  (%.1f%%)", n_pos, 100 * n_pos / n_tot)
    log.info("")

    if "flood_id" in df.columns:
        log.info("  Per-flood breakdown:")
        per_flood = (
            df.groupby("flood_id")["model_is_event_article"]
            .agg(["sum", "count"])
            .rename(columns={"sum": "model_yes", "count": "total"})
        )
        for fid, row in per_flood.iterrows():
            log.info("    Flood %3d : %3d / %3d", fid, int(row["model_yes"]), int(row["total"]))

    # Compare with keyword rule if available
    if "is_event_article" in df.columns:
        rule_pos  = df["is_event_article"].sum()
        model_pos = df["model_is_event_article"].sum()
        agree     = (df["is_event_article"] == df["model_is_event_article"]).sum()
        log.info("")
        log.info("  Comparison with keyword rule:")
        log.info("    Keyword rule   : %d positives", rule_pos)
        log.info("    Model          : %d positives", model_pos)
        log.info("    Agreement      : %d / %d  (%.1f%%)", agree, n_tot, 100 * agree / n_tot)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run flood classifier on clean_text.parquet")
    parser.add_argument("--threshold",  type=float, default=DEFAULT_THRESHOLD, help="Probability threshold (default 0.50)")
    parser.add_argument("--floods",     type=int,   nargs="+",                 help="Restrict to specific flood IDs")
    parser.add_argument("--batch-size", type=int,   default=64,                help="Inference batch size (default 64)")
    parser.add_argument("--no-save",    action="store_true",                   help="Print summary only, do not write parquet")
    args = parser.parse_args()

    parquet_path = OUTPUT_DIR / "clean_text.parquet"
    if not parquet_path.exists():
        log.error("clean_text.parquet not found — run stage_06 first")
        sys.exit(1)

    df = pd.read_parquet(parquet_path)
    log.info("Loaded clean_text.parquet: %d rows", len(df))

    if args.floods:
        df = df[df["flood_id"].isin(args.floods)]
        log.info("Filtered to floods %s: %d rows", args.floods, len(df))

    model = load_model()
    df    = run_inference(model, df, args.threshold, args.batch_size)

    print_summary(df, args.threshold)

    if not args.no_save:
        df.to_parquet(parquet_path, index=False)
        log.info("Updated clean_text.parquet written (added model_flood_prob, model_is_event_article)")


if __name__ == "__main__":
    main()

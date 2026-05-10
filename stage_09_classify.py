#!/usr/bin/env python3
"""
stage_09_classify.py  ·  Flood Data Pipeline — ML Article Classification

Runs the fine-tuned SetFit classifier on output/clean_text.parquet and adds:
  model_flood_prob        float  [0.0 – 1.0]  probability of being a flood event article
  model_is_event_article  bool   True if prob ≥ threshold

Prerequisites:
  1. stage_06v_clean_deduplicate.py must have run (clean_text.parquet exists)
  2. classifier/finetune.py must have run (classifier/model/ directory exists)

Run:
    python3 stage_09_classify.py
    python3 stage_09_classify.py --threshold 0.6   # stricter
    python3 stage_09_classify.py --pilot            # pilot floods only
    python3 stage_09_classify.py --batch-size 128   # faster on GPU
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

# Load config without triggering pipeline imports
_spec = importlib.util.spec_from_file_location("config", ROOT / "config.py")
_cfg  = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cfg)

import numpy as np
import pandas as pd

LOGS_DIR   = Path(_cfg.LOGS_DIR)
OUTPUT_DIR = Path(_cfg.OUTPUT_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_DIR / "stage_09_classify.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

MODEL_DIR = ROOT / "classifier" / "model"
MAX_WORDS = 300


def load_model():
    try:
        from setfit import SetFitModel
    except ImportError:
        log.error("setfit not installed — run:  pip install setfit datasets")
        sys.exit(1)

    if not MODEL_DIR.exists():
        log.error("No classifier model found at %s", MODEL_DIR)
        log.error("Run:  python3 classifier/finetune.py")
        sys.exit(1)

    log.info("Loading classifier model from %s", MODEL_DIR)
    return SetFitModel.from_pretrained(str(MODEL_DIR))


def prepare_texts(df: pd.DataFrame) -> list[str]:
    def _row(r):
        title = str(r.get("page_title",  "") or "").strip()
        text  = str(r.get("clean_text",  "") or "").strip()
        body  = " ".join(text.split()[:MAX_WORDS])
        return f"{title} [SEP] {body}"[:1000]
    return df.apply(_row, axis=1).tolist()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ML flood article classifier")
    parser.add_argument("--threshold",  type=float, default=0.50,    help="Probability threshold (default 0.50)")
    parser.add_argument("--pilot",      action="store_true",          help="Restrict to PILOT_FLOOD_IDS")
    parser.add_argument("--batch-size", type=int,   default=64,       help="Inference batch size (default 64)")
    parser.add_argument("--no-save",    action="store_true",          help="Dry-run: print stats only")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("Stage 09 — ML Article Classification")
    log.info("=" * 60)

    # --- Load data ---
    parquet_path = OUTPUT_DIR / "clean_text.parquet"
    if not parquet_path.exists():
        log.error("clean_text.parquet not found — run stage_06v first")
        sys.exit(1)

    df = pd.read_parquet(parquet_path)
    log.info("Loaded clean_text.parquet: %d rows", len(df))

    # --- Pilot filter ---
    pilot_ids = getattr(_cfg, "PILOT_FLOOD_IDS", None)
    if args.pilot and pilot_ids:
        df = df[df["flood_id"].isin(pilot_ids)]
        log.info("Pilot scope (%s): %d rows", pilot_ids, len(df))

    # --- Need text to classify ---
    has_text = df["clean_text"].notna() & (df["clean_text"].str.len() > 50)
    no_text  = (~has_text).sum()
    if no_text:
        log.warning("%d rows have no usable text — will get prob=NaN", no_text)

    df_with_text = df[has_text].copy()

    # --- Load model and run inference ---
    model  = load_model()
    texts  = prepare_texts(df_with_text)
    n      = len(texts)
    t0     = time.time()

    log.info("Classifying %d articles (batch_size=%d, threshold=%.2f)...",
             n, args.batch_size, args.threshold)

    all_probs: list[float] = []
    for start in range(0, n, args.batch_size):
        batch = texts[start : start + args.batch_size]
        probs = model.predict_proba(batch)[:, 1]
        all_probs.extend(probs.tolist())
        done = min(start + args.batch_size, n)
        if done % (args.batch_size * 5) == 0 or done == n:
            elapsed = time.time() - t0
            rate    = done / elapsed if elapsed > 0 else 0
            log.info("  %d / %d  (%.0f art/s)", done, n, rate)

    df_with_text["model_flood_prob"]       = np.array(all_probs, dtype=np.float32)
    df_with_text["model_is_event_article"] = df_with_text["model_flood_prob"] >= args.threshold

    # Merge back into full dataframe (rows without text get NaN / False)
    df = df.merge(
        df_with_text[["doc_id", "model_flood_prob", "model_is_event_article"]],
        on="doc_id",
        how="left",
    )
    df["model_is_event_article"] = df["model_is_event_article"].fillna(False)

    # --- Summary ---
    n_model  = df["model_is_event_article"].sum()
    n_kw     = df.get("is_event_article", pd.Series(dtype=bool)).sum()
    elapsed  = time.time() - t0

    log.info("")
    log.info("=== Stage 09 Results ===")
    log.info("  Articles classified       : %d", len(df_with_text))
    log.info("  model_is_event_article=True : %d  (%.1f%%)",
             n_model, 100 * n_model / max(len(df), 1))
    if n_kw:
        log.info("  keyword is_event_article=True : %d  (for comparison)", n_kw)
    log.info("  Elapsed                   : %.1f s", elapsed)

    if "flood_id" in df.columns:
        log.info("")
        log.info("  Per-flood model_is_event_article=True counts:")
        per = df.groupby("flood_id")["model_is_event_article"].sum().astype(int)
        for fid, cnt in per.items():
            log.info("    Flood %3d : %d", fid, cnt)

    # --- Save ---
    if not args.no_save:
        df.to_parquet(parquet_path, index=False)
        log.info("")
        log.info("clean_text.parquet updated with model columns.")
    else:
        log.info("--no-save: parquet not written")


if __name__ == "__main__":
    main()

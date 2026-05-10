#!/usr/bin/env python3
"""
classifier/evaluate.py  ·  Flood Pipeline — Classifier Evaluation

Detailed evaluation on labeled annotation data:
  - Overall precision, recall, F1, ROC-AUC
  - Per-flood breakdown
  - Confusion analysis: worst false positives and false negatives
  - Comparison with keyword rule (is_event_article)

Usage:
    python3 classifier/evaluate.py
    python3 classifier/evaluate.py --threshold 0.6
    python3 classifier/evaluate.py --batch 1
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MODEL_DIR = ROOT / "classifier" / "model"
DATA_DIR  = ROOT / "data"


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


def load_annotations(batch: int | None) -> pd.DataFrame:
    if batch is not None:
        paths = [DATA_DIR / f"annotation_batch_{batch}.csv"]
    else:
        paths = sorted(DATA_DIR.glob("annotation_batch_*.csv"))

    dfs = [pd.read_csv(p) for p in paths if p.exists()]
    if not dfs:
        log.error("No annotation CSVs found in %s", DATA_DIR)
        sys.exit(1)

    df = pd.concat(dfs, ignore_index=True)
    df = df[df["label"].isin([0, 1, "0", "1"])].copy()
    df["label"] = df["label"].astype(int)
    return df


def prepare_text(row: pd.Series) -> str:
    title   = str(row.get("page_title",   "") or "").strip()
    snippet = str(row.get("text_snippet", "") or "").strip()
    return f"{title} [SEP] {snippet}"[:1000]


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate flood classifier")
    parser.add_argument("--batch",      type=int,   default=None, help="Annotation batch (default: all)")
    parser.add_argument("--threshold",  type=float, default=0.50, help="Probability threshold (default 0.50)")
    args = parser.parse_args()

    df    = load_annotations(args.batch)
    model = load_model()

    texts  = df.apply(prepare_text, axis=1).tolist()
    y_true = df["label"].tolist()

    log.info("Running inference on %d labeled examples...", len(texts))
    y_prob = model.predict_proba(texts)[:, 1]
    y_pred = (y_prob >= args.threshold).astype(int)

    df["y_prob"] = y_prob
    df["y_pred"] = y_pred

    # --- Overall metrics ---
    print("\n" + "=" * 70)
    print("  FLOOD ARTICLE CLASSIFIER — EVALUATION REPORT")
    print("=" * 70)
    print(f"  Examples : {len(df)}  (pos={sum(y_true)}  neg={len(y_true) - sum(y_true)})")
    print(f"  Threshold: {args.threshold}")
    print()
    print(classification_report(
        y_true, y_pred,
        target_names=["not_event_article", "event_article"],
        digits=3,
    ))

    try:
        auc = roc_auc_score(y_true, y_prob)
        print(f"  ROC-AUC  : {auc:.3f}")
    except Exception:
        pass

    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()
    print(f"\n  Confusion matrix:")
    print(f"    True Pos : {tp:4d}  |  False Pos : {fp:4d}")
    print(f"    False Neg: {fn:4d}  |  True Neg  : {tn:4d}")

    # --- Keyword rule comparison ---
    if "pipeline_prediction" in df.columns:
        kw_pos  = (df["pipeline_prediction"] == "pipeline_yes").astype(int)
        kw_f1   = f1_score(y_true, kw_pos, average="binary", zero_division=0)
        mod_f1  = f1_score(y_true, y_pred, average="binary")
        kw_prec = precision_score(y_true, kw_pos, average="binary", zero_division=0)
        kw_rec  = recall_score(y_true, kw_pos, average="binary", zero_division=0)
        mod_prec = precision_score(y_true, y_pred, average="binary")
        mod_rec  = recall_score(y_true, y_pred, average="binary")

        print(f"\n  {'':20s}  {'Precision':>10}  {'Recall':>8}  {'F1':>6}")
        print(f"  {'Keyword rule':20s}  {kw_prec:>10.3f}  {kw_rec:>8.3f}  {kw_f1:>6.3f}")
        print(f"  {'Fine-tuned model':20s}  {mod_prec:>10.3f}  {mod_rec:>8.3f}  {mod_f1:>6.3f}")

    # --- Per-flood breakdown ---
    if "flood_id" in df.columns:
        print(f"\n  Per-flood F1:")
        for fid, grp in df.groupby("flood_id"):
            if len(grp) < 3:
                continue
            f1 = f1_score(grp["label"], grp["y_pred"], average="binary", zero_division=0)
            pos = grp["label"].sum()
            n   = len(grp)
            print(f"    Flood {fid:3d} : F1={f1:.3f}  (pos={pos}/{n})")

    # --- Worst errors ---
    fp_rows = df[(df["label"] == 0) & (df["y_pred"] == 1)].nlargest(5, "y_prob")
    fn_rows = df[(df["label"] == 1) & (df["y_pred"] == 0)].nsmallest(5, "y_prob")

    if len(fp_rows):
        print(f"\n  Top false positives (model said flood, actually not):")
        for _, r in fp_rows.iterrows():
            print(f"    prob={r['y_prob']:.3f}  [{r.get('flood_id','?')}]  {str(r.get('page_title',''))[:80]!r}")

    if len(fn_rows):
        print(f"\n  Top false negatives (missed genuine flood articles):")
        for _, r in fn_rows.iterrows():
            print(f"    prob={r['y_prob']:.3f}  [{r.get('flood_id','?')}]  {str(r.get('page_title',''))[:80]!r}")

    print()


if __name__ == "__main__":
    main()

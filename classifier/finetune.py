#!/usr/bin/env python3
"""
classifier/finetune.py  ·  Flood Pipeline — Fine-tune Flood Article Classifier

Trains a SetFit binary classifier on manually labeled annotation CSVs.
SetFit uses contrastive sentence-pair learning — works well with 50–300 labels.

Model: sentence-transformers/LaBSE (EN/ES/PT, 471M params)
  LaBSE is shared with the NLP-model submodule — already cached on cluster.
  Alternatively set --model sentence-transformers/paraphrase-multilingual-mpnet-base-v2

Usage:
    python3 classifier/finetune.py                            # all annotation batches
    python3 classifier/finetune.py --batch 1                  # single batch only
    python3 classifier/finetune.py --model sentence-transformers/LaBSE
    python3 classifier/finetune.py --epochs 2 --iters 30      # more training
    python3 classifier/finetune.py --eval-only                 # evaluate saved model
"""

from __future__ import annotations

import os
os.environ["ACCELERATE_USE_MPS_DEVICE"] = "false"
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "0"

import torch
# Force CPU on Apple Silicon — MPS causes OOM on constrained systems
torch.backends.mps.is_available = lambda: False  # type: ignore[method-assign]
torch.backends.mps.is_built     = lambda: False  # type: ignore[method-assign]

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import classification_report, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_MODEL = "sentence-transformers/LaBSE"
MODEL_DIR     = ROOT / "classifier" / "model"
DATA_DIR      = ROOT / "data"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_annotations(batch: int | None) -> pd.DataFrame:
    """Load annotation CSVs and return only rows with filled labels."""
    if batch is not None:
        paths = [DATA_DIR / f"annotation_batch_{batch}.csv"]
    else:
        paths = sorted(DATA_DIR.glob("annotation_batch_*.csv"))

    if not paths or not paths[0].exists():
        log.error("No annotation batch files found in %s", DATA_DIR)
        log.error("Run:  python3 scripts/build_annotation_set.py  then label the CSV")
        sys.exit(1)

    dfs = []
    for path in paths:
        if not path.exists():
            log.warning("Not found: %s", path)
            continue
        df = pd.read_csv(path)
        log.info("  %s: %d rows", path.name, len(df))
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    # Keep rows where label is 0 or 1
    labeled = combined[combined["label"].isin([0, 1, "0", "1"])].copy()
    labeled["label"] = labeled["label"].astype(int)

    skipped = len(combined) - len(labeled)
    if skipped:
        log.warning("Skipped %d unlabeled rows", skipped)

    pos = (labeled["label"] == 1).sum()
    neg = (labeled["label"] == 0).sum()
    log.info("Labeled: %d total  |  pos=%d  neg=%d", len(labeled), pos, neg)

    if pos < 10 or neg < 10:
        log.error("Need ≥10 examples per class (got pos=%d, neg=%d)", pos, neg)
        sys.exit(1)

    return labeled


def prepare_text(row: pd.Series) -> str:
    """Combine title + text snippet into a single input string."""
    title   = str(row.get("page_title",   "") or "").strip()
    snippet = str(row.get("text_snippet", "") or "").strip()
    # Cap at ~1000 chars; model tokeniser handles truncation to 512 tokens
    return f"{title} [SEP] {snippet}"[:1000]


def build_splits(df: pd.DataFrame, val_frac: float, seed: int):
    texts  = df.apply(prepare_text, axis=1).tolist()
    labels = df["label"].tolist()
    return train_test_split(
        texts, labels,
        test_size=val_frac,
        stratify=labels,
        random_state=seed,
    )


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_model(x_train, y_train, x_val, y_val, args) -> object:
    try:
        from datasets import Dataset
        from setfit import SetFitModel, Trainer, TrainingArguments
    except ImportError:
        log.error("setfit not installed — run:  pip install setfit datasets")
        sys.exit(1)

    log.info("Loading base model: %s  (device=%s)", args.model, args.device)
    model = SetFitModel.from_pretrained(args.model, device=args.device)

    train_ds = Dataset.from_dict({"text": x_train, "label": y_train})
    val_ds   = Dataset.from_dict({"text": x_val,   "label": y_val})

    def compute_f1(y_pred, y_true):
        return {"f1": f1_score(y_true, y_pred, average="binary")}

    training_args = TrainingArguments(
        output_dir=str(MODEL_DIR),
        batch_size=args.batch_size,
        num_epochs=args.epochs,
        num_iterations=args.iters,    # sentence pairs per class for contrastive step
        # Note: evaluation_strategy / metric_for_best_model only apply to the
        # head training phase; omit them here to avoid KeyError in embedding phase
        report_to="none",
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        metric=compute_f1,
        column_mapping={"text": "text", "label": "label"},
    )

    log.info("Training — %d train / %d val", len(x_train), len(x_val))
    trainer.train()

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    trainer.model.save_pretrained(str(MODEL_DIR))
    # Save model name so predict.py knows which tokeniser to load
    (MODEL_DIR / "base_model_name.txt").write_text(args.model)
    log.info("Model saved → %s", MODEL_DIR)
    return trainer.model


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def print_metrics(model, x_val, y_val, x_train=None, y_train=None) -> None:
    log.info("Running inference on val set (%d examples)...", len(x_val))
    y_pred = model.predict(x_val)
    y_prob = model.predict_proba(x_val)[:, 1]

    print("\n" + "=" * 60)
    print("  Validation Results")
    print("=" * 60)
    print(classification_report(
        y_val, y_pred,
        target_names=["not_event_article", "event_article"],
        digits=3,
    ))

    try:
        auc = roc_auc_score(y_val, y_prob)
        print(f"  ROC-AUC : {auc:.3f}")
    except Exception:
        pass

    if x_train is not None:
        train_f1 = f1_score(y_train, model.predict(x_train), average="binary")
        val_f1   = f1_score(y_val,   y_pred,                 average="binary")
        print(f"  Train F1: {train_f1:.3f}  |  Val F1: {val_f1:.3f}  "
              f"|  Gap: {train_f1 - val_f1:+.3f}")

    # Show worst errors
    val_df = pd.DataFrame({"text": x_val, "true": y_val, "prob": y_prob})
    y_pred_arr = np.array(y_pred)
    fp = val_df[(val_df["true"] == 0) & (y_pred_arr == 1)].nlargest(3, "prob")
    fn = val_df[(val_df["true"] == 1) & (y_pred_arr == 0)].nsmallest(3, "prob")

    if len(fp):
        print("\n  False positives (predicted flood, actually not):")
        for _, r in fp.iterrows():
            print(f"    prob={r['prob']:.2f}  {r['text'][:100]!r}")
    if len(fn):
        print("\n  False negatives (missed flood articles):")
        for _, r in fn.iterrows():
            print(f"    prob={r['prob']:.2f}  {r['text'][:100]!r}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Fine-tune flood article classifier")
    parser.add_argument("--batch",      type=int,   default=None,          help="Annotation batch number (default: all)")
    parser.add_argument("--model",      type=str,   default=DEFAULT_MODEL, help="HuggingFace model ID")
    parser.add_argument("--epochs",     type=int,   default=1,             help="Contrastive learning epochs (default 1)")
    parser.add_argument("--iters",      type=int,   default=20,            help="Sentence pairs per class (default 20)")
    parser.add_argument("--batch-size", type=int,   default=16,            help="Batch size (default 16)")
    parser.add_argument("--val-frac",   type=float, default=0.20,          help="Validation fraction (default 0.20)")
    parser.add_argument("--seed",       type=int,   default=42,            help="Random seed")
    parser.add_argument("--device",     type=str,   default="cpu",         help="Device: cpu, cuda, mps (default cpu)")
    parser.add_argument("--eval-only",  action="store_true",               help="Skip training, evaluate saved model")
    args = parser.parse_args()

    df = load_annotations(args.batch)

    if len(df) < 30:
        log.error("Need ≥30 labeled examples (got %d)", len(df))
        sys.exit(1)

    x_train, x_val, y_train, y_val = build_splits(df, args.val_frac, args.seed)

    if args.eval_only:
        try:
            from setfit import SetFitModel
        except ImportError:
            log.error("setfit not installed — run:  pip install setfit")
            sys.exit(1)
        log.info("Loading saved model from %s", MODEL_DIR)
        model = SetFitModel.from_pretrained(str(MODEL_DIR))
    else:
        model = train_model(x_train, y_train, x_val, y_val, args)

    print_metrics(model, x_val, y_val, x_train, y_train)


if __name__ == "__main__":
    main()

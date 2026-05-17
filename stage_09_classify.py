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

MODEL_DIR   = ROOT / "classifier" / "model"
MAX_WORDS   = 300
NLP_OUT_DIR = ROOT / "NLP-model" / "output"

# Frames that indicate genuine event reporting (used by --nlp-gate)
_EVENT_FRAMES = {"impact", "response"}


def load_nlp_enrichment() -> "pd.DataFrame | None":
    """
    Load NLP-model enriched CSV(s) and return a url-indexed DataFrame with
    srl_complete and dominant_frame columns.
    Looks for all_floods_enriched.csv first, then individual flood files.
    Returns None if no enriched data is found.
    """
    combined = NLP_OUT_DIR / "all_floods_enriched.csv"
    if combined.exists():
        df = pd.read_csv(combined, usecols=["url", "srl_complete", "dominant_frame"])
        log.info("NLP gate: loaded %d enriched rows from %s", len(df), combined.name)
        return df.drop_duplicates("url").set_index("url")

    # Fallback: merge individual per-flood enriched files
    parts = sorted(NLP_OUT_DIR.glob("flood_*_enriched.csv"))
    if not parts:
        log.warning("NLP gate: no enriched CSVs found in %s — gate disabled", NLP_OUT_DIR)
        return None

    frames = []
    for p in parts:
        try:
            frames.append(pd.read_csv(p, usecols=["url", "srl_complete", "dominant_frame"]))
        except Exception as e:
            log.warning("NLP gate: skipping %s (%s)", p.name, e)
    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True).drop_duplicates("url")
    log.info("NLP gate: loaded %d enriched rows from %d files", len(df), len(parts))
    return df.set_index("url")


def apply_nlp_gate(df: pd.DataFrame, nlp: "pd.DataFrame") -> pd.DataFrame:
    """
    Add combined_is_event_article column:
      - Rows with NLP enrichment: model_is_event_article AND (srl_complete=1 OR frame in impact/response)
      - Rows without NLP enrichment: same as model_is_event_article (no penalty)
    """
    nlp_srl   = nlp["srl_complete"].reindex(df["url"]).values
    nlp_frame = nlp["dominant_frame"].reindex(df["url"]).values

    has_nlp = pd.notna(nlp_srl)
    nlp_pass = (nlp_srl == 1) | pd.Series(nlp_frame, dtype=object).isin(_EVENT_FRAMES).values

    # Combined = model AND nlp where enrichment exists; model-only elsewhere
    combined = df["model_is_event_article"].copy()
    combined[has_nlp] = df["model_is_event_article"][has_nlp] & nlp_pass[has_nlp]

    df = df.copy()
    df["nlp_srl_complete"]     = nlp_srl
    df["nlp_dominant_frame"]   = nlp_frame
    df["combined_is_event_article"] = combined

    n_model    = df["model_is_event_article"].sum()
    n_combined = combined.sum()
    n_gated    = has_nlp.sum()
    log.info("NLP gate applied to %d / %d rows", n_gated, len(df))
    log.info("  model_is_event_article=True   : %d", n_model)
    log.info("  combined_is_event_article=True: %d  (-%d filtered by NLP gate)",
             n_combined, n_model - n_combined)
    return df


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
    parser.add_argument("--nlp-gate",   action="store_true",
                        help="Combine classifier with NLP-model signals (srl_complete + dominant_frame)")
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

    # Drop old model columns to avoid _x/_y suffix conflicts on re-run
    for col in ["model_flood_prob", "model_is_event_article"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Merge back into full dataframe (rows without text get NaN / False)
    df = df.merge(
        df_with_text[["doc_id", "model_flood_prob", "model_is_event_article"]],
        on="doc_id",
        how="left",
    )
    df["model_is_event_article"] = df["model_is_event_article"].fillna(False)

    # --- Optional NLP gate ---
    if args.nlp_gate:
        nlp = load_nlp_enrichment()
        if nlp is not None:
            df = apply_nlp_gate(df, nlp)
        else:
            log.warning("--nlp-gate requested but no enriched CSVs found — skipping gate")

    # --- Summary ---
    n_model  = df["model_is_event_article"].sum()
    n_kw     = df.get("is_event_article", pd.Series(dtype=bool)).sum()
    elapsed  = time.time() - t0

    log.info("")
    log.info("=== Stage 09 Results ===")
    log.info("  Articles classified           : %d", len(df_with_text))
    log.info("  model_is_event_article=True   : %d  (%.1f%%)",
             n_model, 100 * n_model / max(len(df), 1))
    if "combined_is_event_article" in df.columns:
        n_combined = df["combined_is_event_article"].sum()
        log.info("  combined_is_event_article=True: %d  (%.1f%%)  [after NLP gate]",
                 n_combined, 100 * n_combined / max(len(df), 1))
    if n_kw:
        log.info("  keyword is_event_article=True : %d  (for comparison)", n_kw)
    log.info("  Elapsed                   : %.1f s", elapsed)

    if "flood_id" in df.columns:
        log.info("")
        log.info("  Per-flood model_is_event_article=True counts:")
        per = df.groupby("flood_id")["model_is_event_article"].sum().astype(int)
        for fid, cnt in per.items():
            log.info("    Flood %3d : %d", fid, cnt)

    # --- Save parquet ---
    if not args.no_save:
        df.to_parquet(parquet_path, index=False)
        log.info("")
        log.info("clean_text.parquet updated with model columns.")
    else:
        log.info("--no-save: parquet not written")

    # --- Write CSV of model event articles ---
    if not args.no_save:
        filter_col = "combined_is_event_article" if "combined_is_event_article" in df.columns \
                     else "model_is_event_article"
        event_df = df[df[filter_col].fillna(False)].copy()

        # Load country metadata
        flood_csv = ROOT / "data" / "flood_crawl.csv"
        if flood_csv.exists():
            meta = pd.read_csv(flood_csv)[["Flood_ID", "Country"]].rename(
                columns={"Flood_ID": "flood_id", "Country": "country"}
            )
            event_df = event_df.merge(meta, on="flood_id", how="left")

        # Domain from URL if not already present
        if "domain" not in event_df.columns or event_df["domain"].isna().all():
            from urllib.parse import urlparse
            event_df["domain"] = event_df["url"].fillna("").apply(
                lambda u: urlparse(u).netloc.lstrip("www.").lower()
            )

        # Select output columns (include what exists)
        csv_cols = [
            "flood_id", "country", "url", "domain", "page_title",
            "pub_date", "pub_in_window", "language_detected",
            "model_flood_prob", "model_is_event_article",
            "is_event_article",
            "flood_term_hits", "impact_term_hits", "location_term_hits",
            "word_count",
        ]
        if "combined_is_event_article" in df.columns:
            csv_cols += ["combined_is_event_article", "nlp_srl_complete", "nlp_dominant_frame"]

        out_cols  = [c for c in csv_cols if c in event_df.columns]
        event_df  = event_df[out_cols].sort_values("model_flood_prob", ascending=False)
        event_df.insert(0, "doc_num", range(1, len(event_df) + 1))

        import csv as _csv
        scope = f"flood_{args.pilot and 'pilot' or 'all'}" if not args.pilot else "pilot"
        flood_ids = df["flood_id"].unique()
        scope = f"flood_{flood_ids[0]}" if len(flood_ids) == 1 else "multi"
        if args.nlp_gate:
            scope += "_nlp"
        csv_path = OUTPUT_DIR / f"model_event_articles_{scope}.csv"
        event_df.to_csv(csv_path, index=False, quoting=_csv.QUOTE_ALL)
        log.info("CSV saved -> %s  (%d rows)", csv_path, len(event_df))


if __name__ == "__main__":
    main()

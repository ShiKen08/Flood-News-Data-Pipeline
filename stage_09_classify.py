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
import re as _re
import sys
import time
from collections import Counter
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


# ---------------------------------------------------------------------------
# Post-classification filter — rule-based removal of systematic false positives
# ---------------------------------------------------------------------------
_COVID_TERMS = _re.compile(
    r"\b(covid[-\s]?19|coronavirus|pandemia|cuarentena|contagios|vacuna(?:ci[oó]n)?)\b",
    _re.IGNORECASE,
)
_FLOOD_TERMS = _re.compile(
    r"\b(inundaci[oó]n|inundaç[aã]o|lluvia|crecida|desborde|huaico|enchente|"
    r"alagamento|flood|riada|temporal|tormenta|deslizamiento|desbordamiento|"
    r"precipitaci[oó]n|encharcamiento|emergencia\s+h[íi]drica|inundaciones|"
    r"lluvias|chuvas|enchentes|alagou|transbordou|desbordó)\b",
    _re.IGNORECASE,
)
_WEATHER_PAGE      = _re.compile(r"Weather\s*\|\s*Forecast Conditions|Traffic\s*\|\s*Road Conditions", _re.I)
_STAFF_PROFILE     = _re.compile(r"Staff Personalities", _re.I)
_CEMADEN_ADMIN     = _re.compile(
    r"licitaç[aã]o|mobiliário|água mineral|baterias|manutenção predial|"
    r"Escolas participam|promove palestra|recebe estagiár|lança edital",
    _re.I,
)
_CEMADEN_FORECAST  = _re.compile(r"Previsão de Risco Geo-Hidrológico", _re.I)
_PORTAL_PAGE       = _re.compile(
    r"^Noticias\s*::\s*|Consejo de Representantes|Notas de Prensa COVID|"
    r"\|\s*OCHA\s*$|Rumo aos \d+ anos",
    _re.I,
)
_NON_ARTICLE       = _re.compile(
    r"Watch .+ (News Program|Videos) Online|LIVE:\s*.+ News|Traffic\s*\|",
    _re.I,
)
_FORECAST_ES_PT    = _re.compile(
    r"\b(pronóstico|previsão do tempo|forecast\s+today|previsão.*semana|"
    r"tempo.*próximos|previsiones?\s+del\s+tiempo|clima\s+de\s+hoy)\b",
    _re.I,
)
_CLIMATE_POLICY    = _re.compile(
    r"\b(cambio\s+climático|climate\s+change|mudança\s+climática|"
    r"desarrollo\s+sostenible|planejamento\s+urbano|política\s+ambiental|"
    r"greenhouse\s+gas|emisiones?\s+de\s+CO2)\b",
    _re.I,
)
_FIRE_NOT_FLOOD    = _re.compile(
    r"\b(incendio|wildfire|bushfire|forest\s+fire|incêndio)\b",
    _re.I,
)

# ---------------------------------------------------------------------------
# Text sanitisation for CSV output
# ---------------------------------------------------------------------------
_CTRL_CHARS    = _re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u200b\u00ad\ufeff]")
_MULTI_SPACE   = _re.compile(r" {2,}")
# Keep ASCII + Latin Extended-A/B + Greek + Cyrillic — strips CJK noise / box-drawing chars
_NON_TEXT_CHARS = _re.compile(
    r"[^\x09\x20-\x7e\u00a0-\u024f\u0370-\u03ff\u0400-\u04ff]"
)
_SANITIZE_MAX  = 1500  # max chars written to CSV clean_text column


def _sanitize_text(t) -> str:
    """
    Clean extracted article text for safe CSV output:
    - Fix mojibake (ftfy)
    - Strip control characters and characters outside Latin/Greek/Cyrillic
    - Replace double-quotes with apostrophes (prevents CSV cell boundary confusion)
    - Flatten newlines/tabs to single space
    - Truncate to _SANITIZE_MAX characters
    """
    if not isinstance(t, str) or not t:
        return ""
    try:
        import ftfy as _ftfy
        t = _ftfy.fix_text(t)
    except ImportError:
        pass
    t = _CTRL_CHARS.sub("", t)           # strip control chars / zero-width
    t = _NON_TEXT_CHARS.sub("", t)       # strip characters outside Latin+Greek+Cyrillic
    t = t.replace('"', "'")              # double-quote → apostrophe (CSV safety)
    t = _re.sub(r"[\r\n\t]+", " ", t)   # flatten newlines
    t = _MULTI_SPACE.sub(" ", t)         # collapse spaces
    return t.strip()[:_SANITIZE_MAX]


def _post_filter_row(title: str, url: str, domain: str,
                     flood_hits: int = 0, loc_hits: int = 0) -> tuple:
    """Return (keep: bool, reason: str). reason is empty string when kept."""
    title  = title  or ""
    url    = url    or ""
    domain = domain or ""

    if _WEATHER_PAGE.search(title):
        return False, "weather_forecast_page"
    if _STAFF_PROFILE.search(title):
        return False, "journalist_profile"
    if "/staff-personalities" in url.lower():
        return False, "journalist_profile"
    if _COVID_TERMS.search(title) and not _FLOOD_TERMS.search(title):
        return False, "covid_content"
    if "cemaden" in domain.lower() and _CEMADEN_ADMIN.search(title):
        return False, "cemaden_institutional"
    if _CEMADEN_FORECAST.search(title):
        return False, "cemaden_forecast_bulletin"
    if _PORTAL_PAGE.search(title):
        return False, "institutional_portal"
    if _NON_ARTICLE.search(title):
        return False, "non_article_content"
    if _FORECAST_ES_PT.search(title) and not _FLOOD_TERMS.search(title):
        return False, "weather_forecast_es_pt"
    if _CLIMATE_POLICY.search(title) and not _FLOOD_TERMS.search(title):
        return False, "climate_policy_not_event"
    if _FIRE_NOT_FLOOD.search(title) and not _FLOOD_TERMS.search(title):
        return False, "fire_not_flood"
    # Reject model-only positives with zero keyword support
    if flood_hits == 0 and loc_hits == 0:
        return False, "zero_keyword_hits"
    return True, ""


def apply_post_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add post_filter_pass (bool) and post_filter_reason (str) columns.
    Rows with post_filter_pass=True are considered verified flood event articles.
    """
    results = [
        _post_filter_row(
            str(r.get("page_title")      or ""),
            str(r.get("url")             or ""),
            str(r.get("domain")          or ""),
            int(r.get("flood_term_hits") or 0),
            int(r.get("location_term_hits") or 0),
        )
        for _, r in df.iterrows()
    ]
    passes, reasons = zip(*results) if results else ([], [])
    df = df.copy()
    df["post_filter_pass"]   = list(passes)
    df["post_filter_reason"] = list(reasons)

    n_pass = sum(passes)
    log.info("Post-filter: %d / %d articles pass (%d rejected)", n_pass, len(df), len(df) - n_pass)
    counts = Counter(r for r in reasons if r)
    for reason, cnt in counts.most_common():
        log.info("  %-30s : %d", reason, cnt)
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
        import csv as _csv
        from urllib.parse import urlparse

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
            event_df["domain"] = event_df["url"].fillna("").apply(
                lambda u: urlparse(u).netloc.lstrip("www.").lower()
            )

        # Round probability for readability
        event_df["model_flood_prob"] = event_df["model_flood_prob"].round(4)

        # Scope tag for file names
        flood_ids = df["flood_id"].unique()
        scope = f"flood_{flood_ids[0]}" if len(flood_ids) == 1 else "multi"
        if args.nlp_gate:
            scope += "_nlp"

        # Ordered column list (only include columns that exist)
        base_cols = [
            "flood_id", "country", "url", "domain", "page_title",
            "pub_date", "pub_in_window", "language_detected",
            "model_flood_prob", "model_is_event_article",
            "is_event_article",
            "flood_term_hits", "impact_term_hits", "location_term_hits",
            "word_count", "clean_text",
        ]
        nlp_cols  = ["combined_is_event_article", "nlp_srl_complete", "nlp_dominant_frame"] \
                    if "combined_is_event_article" in df.columns else []
        all_cols  = base_cols + nlp_cols

        # --- 1. Full model-positive CSV (unfiltered, adds post_filter columns) ---
        event_df  = apply_post_filter(event_df)
        full_cols = [c for c in all_cols + ["post_filter_pass", "post_filter_reason"]
                     if c in event_df.columns]
        full_out  = event_df[full_cols].sort_values(
            ["flood_id", "model_flood_prob"], ascending=[True, False]
        ).copy()
        full_out.insert(0, "doc_num", range(1, len(full_out) + 1))
        full_path = OUTPUT_DIR / f"model_event_articles_{scope}.csv"
        full_out.to_csv(full_path, index=False, quoting=_csv.QUOTE_ALL)
        log.info("Full CSV  -> %s  (%d rows)", full_path, len(full_out))

        # --- 2. Verified CSV (post-filter pass only, clean columns for analysis) ---
        verified  = event_df[event_df["post_filter_pass"]].copy()
        # Deduplicate by URL within each flood (keep highest probability row)
        verified  = (verified
                     .sort_values("model_flood_prob", ascending=False)
                     .drop_duplicates(subset=["flood_id", "url"], keep="first"))
        if "clean_text" in verified.columns:
            verified["clean_text"] = verified["clean_text"].apply(_sanitize_text)
        ver_cols  = [c for c in all_cols if c in verified.columns]
        verified  = verified[ver_cols].sort_values(
            ["flood_id", "model_flood_prob"], ascending=[True, False]
        ).copy()
        verified.insert(0, "doc_num", range(1, len(verified) + 1))
        ver_path  = OUTPUT_DIR / f"model_event_articles_{scope}_verified.csv"
        verified.to_csv(ver_path, index=False, quoting=_csv.QUOTE_ALL)
        log.info("Verified CSV -> %s  (%d rows, post-filter applied)", ver_path, len(verified))

        log.info("")
        log.info("Post-filter removed %d / %d model positives (%.0f%%)",
                 len(full_out) - len(verified), len(full_out),
                 100 * (len(full_out) - len(verified)) / max(len(full_out), 1))


if __name__ == "__main__":
    main()

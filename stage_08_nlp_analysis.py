# =============================================================================
# stage_08_nlp_analysis.py  ·  Flood Data Pipeline — NLP Bridge
# =============================================================================
# Reads clean_text.parquet (output of stage_06v), selects relevant articles,
# and writes per-flood NLP input CSVs matched to the NLP-model CSV schema.
#
# This stage does NOT run the NLP analysis — it prepares the input files so
# the analysis can be run independently:
#   cd NLP-model && python run_nlp_pipeline.py --input ../output/nlp/flood_65_input.csv
#
# Reads:
#   output/clean_text.parquet         (from stage_06v)
#   data/flood_crawl.csv              (for country name + flood_date)
#
# Outputs:
#   output/nlp/flood_{id}_input.csv   (one file per flood with relevant articles)
#   output/nlp/all_floods_input.csv   (combined file for multi-event analysis)
#   output/nlp/nlp_bridge_summary.txt (per-flood counts and schema notes)
#
# CSV schema matches NLP-model/data/*.csv — see NLP-model/CLAUDE.md for details.
#
# Run:
#   python stage_08_nlp_analysis.py                    # relevant only
#   python stage_08_nlp_analysis.py --include-soft     # include soft-relevant
#   python stage_08_nlp_analysis.py --flood-id 65      # single flood
# =============================================================================

import argparse
import importlib.util
import logging
import sys
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Force-load local config.py
# ---------------------------------------------------------------------------
_config_path = Path(__file__).parent / "config.py"
_spec = importlib.util.spec_from_file_location("config", _config_path)
_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)
sys.modules["config"] = _config

from config import LOGS_DIR, OUTPUT_DIR

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "stage_08_nlp_analysis.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CLEAN_TEXT_PATH  = OUTPUT_DIR / "clean_text.parquet"
FLOOD_CSV_PATH   = Path(__file__).parent / "data" / "flood_crawl.csv"
NLP_OUTPUT_DIR   = OUTPUT_DIR / "nlp"

# ---------------------------------------------------------------------------
# NLP input schema — columns written to each output CSV
# Matches CLAUDE.md schema for NLP-model/data/*.csv
# ---------------------------------------------------------------------------
NLP_COLUMNS = [
    "doc_num",
    "flood_id",
    "country",
    "url",
    "domain",
    "page_title",
    "pub_date",
    "pub_in_window",
    "timestamp",
    "language_detected",
    "language_match",
    "is_relevant",
    "flood_term_hits",
    "location_term_hits",
    "subnational_hits",
    "location_specificity_score",
    "word_count",
    "char_count",
    "is_content_duplicate",
    "signal_many_short_lines",
    "signal_no_long_sentence",
    "signal_large_low_flood",
    "flood_date",          # added: flood onset date for temporal phase computation
    "clean_text_relevant", # renamed from clean_text
]


def extract_domain(url: str) -> str:
    """Extract bare domain (no www.) from a URL string."""
    try:
        return urlparse(url).netloc.lstrip("www.")
    except Exception:
        return ""


def load_flood_metadata() -> dict[int, dict]:
    """
    Load flood_crawl.csv and return a dict of flood_id -> {country, flood_date, iso}.
    flood_date is the Start Date (onset date) used for temporal phase calculation.
    """
    df = pd.read_csv(FLOOD_CSV_PATH)
    result = {}
    for _, row in df.iterrows():
        fid = int(row["Flood_ID"])
        result[fid] = {
            "country":    str(row["Country"]),
            "iso":        str(row["ISO"]),
            "flood_date": str(row["Start Date"]),
        }
    return result


def build_nlp_input(
    clean_df: pd.DataFrame,
    flood_meta: dict[int, dict],
    include_soft: bool = False,
) -> pd.DataFrame:
    """
    Transform clean_text.parquet rows into NLP input schema.
    Filters to is_relevant=True (or also is_soft_relevant=True if include_soft).
    """
    # Relevance filter
    if include_soft and "is_soft_relevant" in clean_df.columns:
        mask = clean_df["is_relevant"] | clean_df["is_soft_relevant"]
        log.info(
            f"Including soft-relevant articles. "
            f"is_relevant={clean_df['is_relevant'].sum()}, "
            f"is_soft_relevant={clean_df['is_soft_relevant'].sum()}, "
            f"union={mask.sum()}"
        )
    else:
        mask = clean_df["is_relevant"]
        if include_soft:
            log.warning(
                "is_soft_relevant column not found in clean_text.parquet — "
                "run stage_06v after the is_soft_relevant fix to generate it. "
                "Falling back to is_relevant only."
            )

    df = clean_df[mask].copy()
    log.info(f"Articles passing relevance filter: {len(df)}")

    # Derive columns not in clean_text.parquet
    df["domain"] = df["url"].fillna("").apply(extract_domain)
    df["country"] = df["flood_id"].map(
        lambda fid: flood_meta.get(fid, {}).get("country", "")
    )
    df["flood_date"] = df["flood_id"].map(
        lambda fid: flood_meta.get(fid, {}).get("flood_date", "")
    )

    # Rename clean_text -> clean_text_relevant (NLP schema name)
    df = df.rename(columns={"clean_text": "clean_text_relevant"})

    # doc_num: integer row counter within each flood (1-based)
    df = df.sort_values(["flood_id", "pub_date"]).reset_index(drop=True)
    df.insert(0, "doc_num", range(1, len(df) + 1))

    # Ensure all required columns are present; fill missing with empty string
    for col in NLP_COLUMNS:
        if col not in df.columns:
            log.warning(f"Column '{col}' not in dataframe — filling with empty string")
            df[col] = ""

    return df[NLP_COLUMNS]


def write_per_flood_csvs(nlp_df: pd.DataFrame, output_dir: Path) -> dict[int, int]:
    """
    Write one CSV per flood_id to output_dir. Returns {flood_id: n_articles}.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    counts = {}

    for flood_id, group in nlp_df.groupby("flood_id"):
        # Re-number doc_num within each flood (1-based)
        group = group.copy().reset_index(drop=True)
        group["doc_num"] = range(1, len(group) + 1)

        out_path = output_dir / f"flood_{flood_id}_input.csv"
        group.to_csv(out_path, index=False, encoding="utf-8")
        counts[flood_id] = len(group)
        log.info(f"  flood_{flood_id}: {len(group)} articles -> {out_path.name}")

    return counts


def write_summary(
    counts: dict[int, int],
    flood_meta: dict[int, dict],
    output_dir: Path,
    include_soft: bool,
) -> None:
    """Write a plain-text summary of what was output."""
    summary_path = output_dir / "nlp_bridge_summary.txt"
    lines = [
        "Stage 08 — NLP Bridge Output Summary",
        "=" * 60,
        f"Relevance filter : {'is_relevant OR is_soft_relevant' if include_soft else 'is_relevant only'}",
        f"Total articles   : {sum(counts.values())}",
        f"Floods with data : {len(counts)}",
        "",
        "Per-flood breakdown:",
        f"  {'flood_id':>8}  {'country':30}  {'articles':>8}",
        "  " + "-" * 52,
    ]
    for fid in sorted(counts):
        country = flood_meta.get(fid, {}).get("country", "")
        lines.append(f"  {fid:>8}  {country[:30]:30}  {counts[fid]:>8}")

    lines += [
        "",
        "NLP schema columns (in order):",
    ]
    for i, col in enumerate(NLP_COLUMNS, 1):
        lines.append(f"  {i:>2}. {col}")

    lines += [
        "",
        "To run NLP analysis:",
        "  cd NLP-model",
        "  # Single flood:",
        "  python run_nlp_pipeline.py --input ../output/nlp/flood_65_input.csv",
        "  # All floods combined:",
        "  python run_nlp_pipeline.py --input ../output/nlp/all_floods_input.csv",
    ]

    summary_path.write_text("\n".join(lines), encoding="utf-8")
    log.info(f"Summary written -> {summary_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 08 — NLP bridge: prep input CSVs")
    parser.add_argument(
        "--include-soft", action="store_true",
        help="include is_soft_relevant articles (relaxed subnational requirement)",
    )
    parser.add_argument(
        "--flood-id", type=int, default=None,
        help="only process a single flood event (debug)",
    )
    args = parser.parse_args()

    log.info("=== STAGE 08: NLP ANALYSIS BRIDGE ===")

    # Load inputs
    log.info(f"Loading clean_text.parquet from {CLEAN_TEXT_PATH}")
    clean_df = pd.read_parquet(CLEAN_TEXT_PATH)
    log.info(f"Loaded {len(clean_df)} rows")

    flood_meta = load_flood_metadata()
    log.info(f"Loaded flood metadata for {len(flood_meta)} events")

    # Optional single-flood filter
    if args.flood_id:
        clean_df = clean_df[clean_df["flood_id"] == args.flood_id]
        log.info(f"Filtered to flood_id={args.flood_id}: {len(clean_df)} rows")

    # Build NLP input dataframe
    nlp_df = build_nlp_input(clean_df, flood_meta, include_soft=args.include_soft)

    if nlp_df.empty:
        log.warning("No articles passed the relevance filter — nothing to write.")
        return

    # Write per-flood CSVs
    log.info(f"Writing per-flood CSVs to {NLP_OUTPUT_DIR}")
    counts = write_per_flood_csvs(nlp_df, NLP_OUTPUT_DIR)

    # Write combined CSV for multi-event analysis
    combined_path = NLP_OUTPUT_DIR / "all_floods_input.csv"
    nlp_df.to_csv(combined_path, index=False, encoding="utf-8")
    log.info(f"Combined CSV ({len(nlp_df)} rows) -> {combined_path}")

    # Write summary
    write_summary(counts, flood_meta, NLP_OUTPUT_DIR, include_soft=args.include_soft)

    log.info("=== STAGE 08 COMPLETE ===")
    log.info(f"Output directory: {NLP_OUTPUT_DIR}")
    log.info(f"Total articles  : {sum(counts.values())} across {len(counts)} floods")


if __name__ == "__main__":
    main()

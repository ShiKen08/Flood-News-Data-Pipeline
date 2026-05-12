#!/usr/bin/env python3
"""
Auto-build annotation batches for unlabeled floods using keyword-based silver labeling.

Positive (label=1): is_event_article=True AND strong flood + location keywords in text
Negative (label=0): sampled from is_event_article=False rows with minimal flood content

Run after stage_06v completes:
    python3 scripts/build_annotation_batch_auto.py
    python3 scripts/build_annotation_batch_auto.py --flood-ids 7 8 9 10
"""

from __future__ import annotations
import argparse, csv, json, re, sys
from pathlib import Path

import pandas as pd
import numpy as np

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

PARQUET    = ROOT / "output" / "clean_text.parquet"
DATA_DIR   = ROOT / "data"
META_CSV   = DATA_DIR / "flood_crawl.csv"

# Already labeled flood IDs — skip these
ALREADY_LABELED = {2, 3, 4, 5, 6, 22}

FLOOD_KW = re.compile(
    r'flood|inundac|enchente|chuva|lluvias|desbord|tormenta|temporal|'
    r'precipit|overfl|surge|rescue|evacuac|damage|victim|afectad|'
    r'morti|dead|death|disaster|emergenc|calamid|alerta|alagam|'
    r'transbord|desliz|landslide|hurricane|hurac|alud|avalancha|'
    r'ciclone|cyclone|typhoon|torrente|anegad|inond|submers|'
    r'riada|desbordamiento|encharcamiento|anegamiento',
    re.I
)


def has_flood_kw(text: str, title: str = "") -> bool:
    return bool(FLOOD_KW.search(str(text) + " " + str(title)))


def build_batch_for_flood(flood_id: int, group: pd.DataFrame,
                          meta_row: pd.Series, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed + flood_id)

    text_col   = group["clean_text"].fillna("")
    title_col  = group["page_title"].fillna("")
    combined   = text_col + " " + title_col

    has_kw = combined.apply(has_flood_kw)

    # --- Positives: is_event_article=True AND has flood keyword ---
    pos_mask = group["is_event_article"].fillna(False) & has_kw
    pos = group[pos_mask].copy()
    # Sort by relevance signals, cap at 50
    pos = pos.sort_values(
        ["flood_term_hits", "impact_term_hits", "location_term_hits"],
        ascending=False
    ).head(50)
    pos["label"] = 1

    # --- Negatives: keyword-absent articles (clearly off-topic) ---
    neg_mask = ~has_kw
    neg_pool = group[neg_mask]
    n_neg    = min(len(neg_pool), max(len(pos) * 2, 30))
    neg      = neg_pool.sample(n=n_neg, random_state=int(rng.integers(1000)))
    neg["label"] = 0

    combined_df = pd.concat([pos, neg], ignore_index=True)
    combined_df["flood_id"]       = flood_id
    combined_df["country"]        = meta_row.get("Country", "")
    combined_df["event_location"] = meta_row.get("Location", "")
    combined_df["event_date"]     = meta_row.get("Start Date", "")
    combined_df["text_snippet"]   = combined_df["clean_text"].str[:500]

    cols = ["flood_id", "country", "event_location", "event_date",
            "url", "page_title", "language_detected", "text_snippet",
            "flood_term_hits", "impact_term_hits", "location_term_hits",
            "word_count", "label"]
    return combined_df[[c for c in cols if c in combined_df.columns]]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flood-ids", nargs="+", type=int, default=None,
                        help="Specific flood IDs to label (default: all unlabeled)")
    parser.add_argument("--batch-num", type=int, default=3,
                        help="Starting annotation batch number (default: 3)")
    args = parser.parse_args()

    if not PARQUET.exists():
        print(f"ERROR: {PARQUET} not found — run stage_06v first")
        sys.exit(1)

    print("Loading parquet...")
    df = pd.read_parquet(PARQUET)
    meta = pd.read_csv(META_CSV).set_index("Flood_ID")

    available = sorted(df["flood_id"].unique())
    print(f"Floods in parquet: {available}")

    target_ids = args.flood_ids or [
        fid for fid in available if fid not in ALREADY_LABELED
    ]
    print(f"Floods to label: {target_ids}")
    print()

    all_parts = []
    for flood_id in target_ids:
        group = df[df["flood_id"] == flood_id]
        if len(group) == 0:
            print(f"  Flood {flood_id}: no rows — skipping")
            continue
        meta_row = meta.loc[flood_id] if flood_id in meta.index else pd.Series()
        batch    = build_batch_for_flood(flood_id, group, meta_row)
        pos      = (batch["label"] == 1).sum()
        neg      = (batch["label"] == 0).sum()
        print(f"  Flood {flood_id:>3} ({meta_row.get('Country','?')[:20]:<20}): "
              f"{len(group):>5} total  →  {pos} pos / {neg} neg labeled")
        all_parts.append(batch)

    if not all_parts:
        print("Nothing to label.")
        sys.exit(0)

    combined = pd.concat(all_parts, ignore_index=True)
    out_path = DATA_DIR / f"annotation_batch_{args.batch_num}.csv"
    combined.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL)

    total_pos = (combined["label"] == 1).sum()
    total_neg = (combined["label"] == 0).sum()
    print()
    print(f"Saved {len(combined)} rows → {out_path}")
    print(f"  pos={total_pos}  neg={total_neg}")


if __name__ == "__main__":
    main()

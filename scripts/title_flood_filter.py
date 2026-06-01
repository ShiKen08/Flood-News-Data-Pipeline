#!/usr/bin/env python3
"""
title_flood_filter.py — Keep only articles whose title contains explicit flood vocabulary.

Run:
    python3 scripts/title_flood_filter.py --input output/model_event_articles_multi_verified_late_batch.csv
    python3 scripts/title_flood_filter.py --input output/model_event_articles_multi_verified_late_batch.csv --out output/flood_titled_late_batch.csv
"""

import argparse
import csv
import re
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Flood vocabulary — terms that must appear in the title to keep the article.
# Organised by language; any single match is enough.
# ---------------------------------------------------------------------------

_FLOOD_TITLE = re.compile(
    r"\b("
    # --- English ---
    r"flood|flooding|flooded|floodwater|inundation|flash.flood|storm.surge|levee|"
    r"overflowed|overflows|high.water|floodplain|"
    # --- Portuguese ---
    r"inundaç[aã]o|inundações|enchente|enchentes|alagamento|alagamentos|"
    r"cheia|cheias|enxurrada|enxurradas|"
    r"chuvas?|temporal|temporais|"
    r"deslizamento|deslizamentos|desabamento|desabamentos|desabou|deslizou|"
    r"barragem.rompe|rompimento.de.barragem|ruptura.de.barragem|"
    r"cota.de.alerta|n[íi]vel.do.rio|rio.transbordou|rio.desbordou|"
    r"situa[cç][aã]o.de.emerg[eê]ncia|calamidade.p[úu]blica|"
    r"chuvas.afetam|chuvas.causam|chuvas.deixam|chuvas.matam|"
    r"desalojados|desabrigados|"
    # --- Spanish ---
    r"inundaci[oó]n|inundaciones|encharcamiento|desbordamiento|"
    r"crecida|crecidas|desborde|huaico|huaicos|"
    r"lluvias?|"
    r"deslizamiento|deslizamientos|alud|avalancha|"
    r"alerta.roja|alerta.naranja|"
    r"emergencia.invernal|emergencia.por.lluvia|temporada.de.lluvias|"
    r"evacuaci[oó]n.por|afectados.por.lluvia|"
    r"desbord[oó].el.r[íi]o|r[íi]o.desbord|r[íi]o.creci[oó]|"
    r"declaratoria.de.desastre|estado.de.emergencia|"
    r"sitios.inundados|zonas.inundadas|familias.afectadas"
    r")\b",
    re.IGNORECASE | re.UNICODE,
)

# Terms that look flood-related but are NOT (override the above)
_NOT_FLOOD_TITLE = re.compile(
    r"\b("
    r"hurac[aá]n|hurricane|ciclone|tif[oó]n|typhoon|tornado|"       # wind storms ≠ floods
    r"barragem.da.vale|brumadinho|"                                  # mining dam collapse
    r"seca|drought|escasez.de.agua|falta.de.agua|"                  # drought
    r"inc[eê]ndio|wildfire|queimada|inc[eê]ndios.florestais|"        # fire
    r"derrame.de.petr[oó]leo|vazamento.de.[oó]leo|mancha.de.[oó]leo|"  # oil spill
    r"terremoto|sismo|temblor|earthquake|tsunami|"                   # seismic
    r"not[íi]cias.do.dia|noticias.del.d[íi]a|"                      # news digests
    r"virtual.library|shelter.listing|"                              # portal pages
    r"carnaval|eleic[õo]es|elecci[oó]n|pol[íi]tica|futebol|copa.do.mundo|"  # off-topic
    r"concurso.nacional|bolsa.de.estudos|mestrado|doutorado"         # institutional
    r")\b",
    re.IGNORECASE | re.UNICODE,
)


def is_flood_title(title: str) -> bool:
    t = title or ""
    if _NOT_FLOOD_TITLE.search(t):
        return False
    return bool(_FLOOD_TITLE.search(t))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True, help="Input verified CSV")
    parser.add_argument("--out",    default=None,  help="Output CSV (default: adds _titled suffix)")
    args = parser.parse_args()

    in_path = Path(args.input)
    if args.out:
        out_path = Path(args.out)
    else:
        out_path = in_path.with_name(in_path.stem + "_titled.csv")

    df = pd.read_csv(in_path)
    print(f"Loaded {len(df)} rows from {in_path.name}")

    df["flood_title"] = df["page_title"].fillna("").apply(is_flood_title)

    keep    = df[df["flood_title"]].drop(columns=["flood_title"]).copy()
    dropped = df[~df["flood_title"]].drop(columns=["flood_title"]).copy()

    # Reassign doc_num
    if "doc_num" in keep.columns:
        keep = keep.drop(columns=["doc_num"])
    keep.insert(0, "doc_num", range(1, len(keep) + 1))

    keep.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL)

    print(f"\nResults:")
    print(f"  Flood-titled (KEEP)  : {len(keep):>5}  → {out_path.name}")
    print(f"  No flood in title    : {len(dropped):>5}  (rejected)")
    print(f"\nKept by flood ID:")
    per = keep.groupby(["flood_id", "country"]).size().reset_index(name="n")
    for _, row in per.iterrows():
        print(f"  Flood {int(row.flood_id):>3}  {row.country:<40} {int(row.n):>4} articles")

    print(f"\nRejected sample (first 40 titles):")
    for title in dropped["page_title"].head(40):
        print(f"  - {title}")


if __name__ == "__main__":
    main()

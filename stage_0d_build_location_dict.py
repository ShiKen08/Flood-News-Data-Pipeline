"""
Stage 0d — Location Dictionary Builder
Reads flood_crawl.csv and produces location_dictionary.json
partitioned by flood_id, with level tags, aliases, and ambiguity flags.

Usage:
    python stage_0d_build_location_dict.py \
        --csv flood_crawl.csv \
        --out location_dictionary.json
"""

import csv
import json
import re
import argparse
from pathlib import Path

# ── Country-level names ────────────────────────────────────────────────────────
# Built from the Country column of all 150 rows.
# Anything NOT in this set will be tagged subnational.
COUNTRY_NAMES = {
    # From ISO / Country column — normalised to lowercase
    "syrian arab republic", "syria",
    "indonesia",
    "colombia",
    "tunisia",
    "algeria",
    "afghanistan",
    "zimbabwe",
    "united states of america", "usa", "united states",
    "iran", "iran (islamic republic of)",
    "morocco",
    "malaysia",
    "bolivia", "bolivia (plurinational state of)",
    "iraq",
    "thailand",
    "democratic republic of the congo", "drc", "dr congo", "congo", "congo-kinshasa",
    "costa rica",
    "honduras",
    "mexico",
    "nepal",
    "india",
    "bulgaria",
    "cambodia",
    "ukraine",
    "georgia",           # NOTE: also a US state — flagged as ambiguous below
    "gambia",
    "sierra leone",
    "sudan",
    "uganda",
    "china",
    "pakistan",
    "republic of korea", "south korea", "korea",
    "central african republic",
    "cabo verde",
    "japan",
    "myanmar",
    "equatorial guinea",
    "taiwan", "taiwan (province of china)",
    "romania",
    "nigeria",
    "laos", "lao people's democratic republic",
    "bangladesh",
    "viet nam", "vietnam",
    "philippines",
    "south africa",
    "brazil",
    "croatia",
    "bosnia and herzegovina",
    "italy",
    "argentina",
    "peru",
    "namibia",
    "botswana",
    "madagascar",
    "spain",
    "dominican republic",
    "haiti",
    "france",
    "kenya",
    "malawi",
    "gabon",
    "united republic of tanzania", "tanzania",
    "somalia",
    "libya",
    "yemen",
    "cameroon",
    "chad",
    "guinea",
    "china, hong kong special administrative region", "hong kong",
    "myanmar",
    "lao people's democratic republic",
}

# ── Ambiguous names ────────────────────────────────────────────────────────────
# Names that could match unrelated events if not checked against flood context.
AMBIGUOUS_NAMES = {
    "georgia",      # US state AND country (Flood #29)
    "columbia",     # District of Columbia AND country variant
    "virginia",     # US state but very common word
    "nueva",        # common Spanish prefix
    "victoria",     # city name in many countries
    "wellington",   # city in multiple countries
    "queensland",   # Australia — not in dataset but guard anyway
    "santa cruz",   # appears in Bolivia AND other countries
    "san jose",     # multiple countries
}

# ── Alias map ──────────────────────────────────────────────────────────────────
# Maps canonical lowercase name → list of known alternate spellings.
# Seeded for all countries + high-priority subnational names in the dataset.
# EXTEND THIS as pilot documents surface new spellings.
ALIAS_MAP = {
    # Countries
    "democratic republic of the congo": [
        "drc", "dr congo", "congo-kinshasa", "zaire", "rd congo",
        "republique democratique du congo", "rdc"
    ],
    "syrian arab republic": ["syria", "syrian republic"],
    "iran (islamic republic of)": ["iran", "persia", "islamic republic of iran"],
    "united states of america": ["usa", "united states", "us", "america"],
    "bolivia (plurinational state of)": ["bolivia"],
    "lao people's democratic republic": ["laos", "lao pdr", "lao"],
    "united republic of tanzania": ["tanzania"],
    "viet nam": ["vietnam", "viet-nam"],
    "republic of korea": ["south korea", "korea"],
    "taiwan (province of china)": ["taiwan"],
    "china, hong kong special administrative region": ["hong kong", "hksar"],
    "bosnia and herzegovina": ["bosnia", "bih"],

    # High-value subnational aliases from the CSV
    "kinshasa": ["kinshasa capital city", "kinshasa city"],
    "khyber pakhtunkhwa": ["kpk", "nwfp"],
    "azad jammu and kashmir": ["ajk", "azad kashmir"],
    "gilgit-baltistan": ["gb", "gilgit baltistan"],
    "west bengal": ["west bengal state"],
    "greater jakarta": ["greater jakarta area", "jakarta"],
    "java": ["java island", "java isl."],
    "sulawesi": ["celebes"],
    "north sulawesi": ["north sulawesi province"],
    "kalimantan": ["borneo"],
    "north maluku": ["north maluku province", "maluku utara"],
    "west papua": ["west papua province"],
    "santa barbara": ["santa barbara county"],
    "los angeles": ["los angeles county", "la county"],
    "ventura": ["ventura county"],
    "san bernardino": ["san bernardino county"],
    "esfahan": ["isfahan"],
    "khorasan razavi": ["razavi khorasan"],
    "sistan-o baluchestan": ["sistan and baluchestan", "sistan-baluchestan"],
    "azarbayejan sharghi": ["east azerbaijan", "east azarbaijan"],
    "kohgiluyeh va boyerahma": ["kohgiluyeh and boyer-ahmad"],
    "uttarakhand": ["uttaranchal"],
    "himachal pradesh": ["hp"],
    "jammu and kashmir": ["j&k", "jk"],
    "phra nakhon si ayutthaya": ["ayutthaya"],
    "nakhon si thammarat": ["nakhon si thammarat province"],
    "nueva jersey": ["new jersey"],  # for Spanish-language docs
    "nueva york": ["new york"],
    "grand'anse": ["grande anse", "grand anse"],
    "borno": ["borno state"],
    "adamawa": ["adamawa state"],
    "niger state": ["niger"],   # careful — country Niger also exists
    "emilia-romagna": ["emilia romagna"],
    "toscane": ["tuscany", "toscana"],
    "comunitat valenciana": ["valencia", "valencian community"],
    "castilla-la mancha": ["castile-la mancha"],
    "kakheti": ["kakheti municipality", "kakheti region"],
    "odesa": ["odessa", "odesa oblast"],
    "negeri sembilan": ["negri sembilan"],
    "sarawak": ["sarawak state"],
    "sabah": ["sabah state"],
    "terengganu": ["trengganu"],
    "pahang": ["pahang state"],
    "kelantan": ["kelantan state"],
    "puno": ["puno region"],
    "analamanga": ["analamanga region"],
    "antananarivo": ["antananarivo city"],
    "mbeya": ["mbeya region"],
    "kyela": ["kyela district"],
    "south kivu": ["sud-kivu", "south kivu province"],
    "kasaba": ["kasaba territory"],
    "tanganyika province": ["tanganyika"],
    "ngaliema": ["ngaliema commune"],
    "kalehe": ["kalehe territory"],
    "ngounié": ["ngounie", "ngounié province"],
    "estuaire": ["estuaire province"],
    "batha": ["batha region"],
    "chari-baguirmi": ["chari baguirmi"],
    "n'djamena": ["ndjamena", "n djamena"],
    "logone oriental": ["logone oriental region"],
    "logone et chari": ["logone-et-chari"],
    "far north region": ["extreme nord", "far north"],
    "maqbanah": ["maqbanah district"],
    "taez": ["taiz", "ta'iz"],
    "al-mahwit": ["mahwit", "al mahwit"],
    "al hudaydah": ["hudaydah", "hodeidah"],
    "siguiri": ["siguiri prefecture"],
    "garzê": ["garze", "garzê tibetan autonomous prefecture"],
    "sichuan": ["szechuan", "szechwan"],
    "busan": ["pusan"],
    "gyeonggi": ["gyeonggi province", "gyeonggi-do"],
    "gangwon": ["gangwon province"],
    "chungcheong": ["south chungcheong"],
    "gwangju": ["kwangju"],
    "ordos": ["ordos city"],
    "inner mongolia": ["nei mongol", "inner mongolia autonomous region"],
    "hebei": ["hopei"],
    "shanxi": ["shansi"],
    "shandong": ["shantung"],
    "gansu": ["kansu"],
    "yuzhong": ["yuzhong county"],
    "kagoshima": ["kagoshima prefecture"],
    "ishikawa": ["ishikawa prefecture"],
    "kumamoto": ["kumamoto prefecture"],
    "mondulkiri": ["mondulkiri province"],
    "ratanakiri": ["ratanakiri province"],
    "laiza": ["laiza city"],
    "malabo": ["malabo city"],
    "bioko norte": ["bioko norte province"],
    "suceava": ["suceava county"],
    "neamt": ["neamț", "neamt county"],
    "yola": ["yola city"],
    "maiduguri": ["maiduguri city"],
    "mokwa": ["mokwa city"],
    "puntland": ["puntland state"],
    "banaadir": ["banadir", "banaadir region"],
    "hirshabelle": ["hirshabelle state"],
    "gaalkacyo": ["galcaio", "gaalkacyo city"],
    "mogadiscio": ["mogadishu", "mogadiscio"],
    "balcad": ["balcad district"],
    "dubrovnik": ["dubrovnik city"],
    "prijedor": ["prijedor city"],
    "donja jablanica": ["jablanica"],
    "bahia blanca": ["bahía blanca"],
    "buenos aires": ["buenos aires province"],
    "petrópolis": ["petropolis"],
    "rio de janeiro": ["rio"],
    "são paulo": ["sao paulo"],
    "carapicuíba": ["carapicuiba"],
    "florence": ["firenze"],
    "prato": ["prato city"],
    "rhône": ["rhone"],
    "haute-loire": ["haute loire"],
    "ardèche": ["ardeche"],
    "alpes-maritimes": ["alpes maritimes"],
    "álora": ["alora"],
    "málaga": ["malaga"],
    "campaillas": ["campanillas"],
}


def normalise(text: str) -> str:
    """Lowercase, strip, collapse internal whitespace."""
    return re.sub(r'\s+', ' ', text.strip().lower())


def parse_location_field(raw: str) -> list[str]:
    """
    Split the Location field into individual place names.
    Handles commas as primary delimiter, strips parenthetical notes,
    and drops empty/very short tokens.
    """
    if not raw or not raw.strip():
        return []

    # Remove parenthetical clarifications e.g. "(Java Isl.)" but keep the rest
    raw = re.sub(r'\([^)]*\)', '', raw)

    # Split on comma or semicolon
    parts = re.split(r'[,;]', raw)

    cleaned = []
    for p in parts:
        p = normalise(p)
        # Drop tokens that are just filler words or too short
        if len(p) < 3:
            continue
        if p in {'and', 'the', 'of', 'in', 'or', 'with', 'isl.', 'isl', 'island'}:
            continue
        cleaned.append(p)

    return cleaned


def get_level(name: str) -> str:
    return "country" if name in COUNTRY_NAMES else "subnational"


def build_location_dictionary(csv_path: str) -> dict:
    location_dict = {}

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            flood_id = int(row["Flood_ID"])
            country  = normalise(row["Country"])
            raw_loc  = row.get("Location", "")

            entries = []
            seen    = set()   # avoid duplicate entries per flood_id

            # ── Always include the country itself ──────────────────────────
            if country and country not in seen:
                seen.add(country)
                entries.append({
                    "name":      country,
                    "level":     "country",
                    "ambiguous": country in AMBIGUOUS_NAMES,
                    "aliases":   ALIAS_MAP.get(country, [])
                })

            # Also add short-form country alias as a separate entry so both match
            for alias in ALIAS_MAP.get(country, []):
                if alias not in seen:
                    seen.add(alias)
                    entries.append({
                        "name":      alias,
                        "level":     "country",
                        "ambiguous": alias in AMBIGUOUS_NAMES,
                        "aliases":   []   # aliases of aliases not needed
                    })

            # ── Parse subnational place names from Location field ──────────
            place_names = parse_location_field(raw_loc)
            for name in place_names:
                if name in seen:
                    continue
                seen.add(name)

                # Strip trailing "province", "state", "region" etc. to get
                # a clean canonical name, then keep both forms
                canonical = re.sub(
                    r'\s+(province|state|region|district|department|'
                    r'governorate|prefecture|county|municipality|'
                    r'regency|oblast|commune|city area|capital city)s?$',
                    '', name
                ).strip()

                if canonical != name and canonical not in seen and len(canonical) >= 3:
                    seen.add(canonical)
                    entries.append({
                        "name":      canonical,
                        "level":     get_level(canonical),
                        "ambiguous": canonical in AMBIGUOUS_NAMES,
                        "aliases":   ALIAS_MAP.get(canonical, [])
                    })

                entries.append({
                    "name":      name,
                    "level":     get_level(name),
                    "ambiguous": name in AMBIGUOUS_NAMES,
                    "aliases":   ALIAS_MAP.get(name, [])
                })

            location_dict[flood_id] = entries

    return location_dict


def print_summary(location_dict: dict):
    print(f"\n{'─'*55}")
    print(f"  Location dictionary built for {len(location_dict)} events")
    total_entries   = sum(len(v) for v in location_dict.values())
    subnational     = sum(
        1 for v in location_dict.values()
        for e in v if e['level'] == 'subnational'
    )
    ambiguous_count = sum(
        1 for v in location_dict.values()
        for e in v if e['ambiguous']
    )
    print(f"  Total place entries : {total_entries}")
    print(f"  Subnational entries : {subnational}")
    print(f"  Ambiguous flagged   : {ambiguous_count}")
    print(f"{'─'*55}\n")

    # Show pilot events for quick sanity check
    pilot_ids = [1, 2, 3, 9, 12, 19, 34]
    print("  Pilot event spot-check:")
    for fid in pilot_ids:
        entries = location_dict.get(fid, [])
        names   = [e['name'] for e in entries]
        print(f"  #{fid:>3}  {len(entries):>3} entries  →  {names[:5]}{'...' if len(names) > 5 else ''}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 0d — Build location dictionary")
    parser.add_argument("--csv", default="flood_crawl.csv",  help="Path to flood_crawl.csv")
    parser.add_argument("--out", default="location_dictionary.json", help="Output JSON path")
    args = parser.parse_args()

    if not Path(args.csv).exists():
        raise FileNotFoundError(f"CSV not found: {args.csv}")

    print(f"Reading {args.csv}...")
    location_dict = build_location_dictionary(args.csv)

    print_summary(location_dict)

    # Save — keys must be strings in JSON
    out_data = {str(k): v for k, v in location_dict.items()}
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(out_data, f, ensure_ascii=False, indent=2)

    print(f"Saved → {args.out}")
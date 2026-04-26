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
import sys
sys.stdout.reconfigure(encoding='utf-8')

# ── Country-level names ────────────────────────────────────────────────────────
# Built from the Country column of the Americas 2020-2025 dataset (227 events).
# Anything NOT in this set will be tagged subnational.
COUNTRY_NAMES = {
    # Americas — primary dataset
    "united states of america", "usa", "united states",
    "brazil",
    "colombia",
    "bolivia", "bolivia (plurinational state of)",
    "peru",
    "ecuador",
    "mexico",
    "venezuela", "venezuela (bolivarian republic of)",
    "guatemala",
    "canada",
    "argentina",
    "uruguay",
    "dominican republic",
    "paraguay",
    "honduras",
    "costa rica",
    "panama",
    "trinidad and tobago",
    "saint lucia",
    "el salvador",
    "cuba",
    "guyana",
    "french guiana",
    "puerto rico",
    "chile",
    "haiti",
}

# ── Ambiguous names ────────────────────────────────────────────────────────────
# Names that could match unrelated events if not checked against flood context.
AMBIGUOUS_NAMES = {
    "georgia",      # US state AND former Soviet country
    "columbia",     # District of Columbia AND country variant
    "virginia",     # US state — common word
    "nueva",        # common Spanish prefix
    "victoria",     # city name in many countries
    "santa cruz",   # Bolivia dept AND city in multiple countries
    "san jose",     # Costa Rica capital AND city in USA/multiple countries
    "florida",      # US state AND Uruguay department
    "sucre",        # Bolivia capital AND Colombia department
    "miranda",      # Venezuela state AND common surname
    "merida",       # Venezuela state AND Mexico city AND Spain city
    "cordoba",      # Argentina city AND Spain city
    "santiago",     # Chile capital AND multiple other cities
    "amazonas",     # Brazil state AND Colombia/Venezuela dept
    "parana",       # Brazil state AND river name
    "san marcos",   # Guatemala dept AND Texas city
    "loreto",       # Peru region AND Italy city
}

# ── Alias map ──────────────────────────────────────────────────────────────────
# Maps canonical lowercase name -> list of known alternate spellings.
# Covers all countries + high-priority subnational names in the Americas dataset.
# EXTEND THIS as pipeline runs surface new spellings.
ALIAS_MAP = {
    # ── Country name variants ──────────────────────────────────────────────────
    "united states of america": ["usa", "united states", "us", "america", "estados unidos"],
    "bolivia (plurinational state of)": [
        "bolivia", "estado plurinacional de bolivia"
    ],
    "venezuela (bolivarian republic of)": [
        "venezuela", "vzla", "república bolivariana de venezuela",
        "republica bolivariana de venezuela"
    ],
    "trinidad and tobago": ["t&t", "trinidad", "tobago", "trinidad & tobago"],
    "saint lucia": ["st. lucia", "st lucia", "sainte-lucie", "sta. lucia"],
    "french guiana": [
        "guyane", "french guyana", "guyane française", "guyane francaise",
        "guiana francesa"
    ],
    "guyana": ["cooperative republic of guyana", "co-operative republic of guyana"],
    "dominican republic": ["república dominicana", "republica dominicana", "rd"],
    "el salvador": ["república de el salvador", "republica de el salvador"],
    "puerto rico": ["pr", "borinquen", "estado libre asociado de puerto rico"],

    # ── Brazil — states and major cities ──────────────────────────────────────
    "rio grande do sul": ["rs", "rio grande do sul state", "estado do rio grande do sul", "rgs"],
    "bahia": ["ba", "bahia state", "estado da bahia"],
    "são paulo": ["sp", "sao paulo", "sao paulo state", "estado de são paulo",
                  "estado de sao paulo"],
    "santa catarina": ["sc", "santa catarina state", "estado de santa catarina"],
    "paraná": ["parana", "parana state", "estado do parana", "estado do paraná"],
    "minas gerais": ["mg", "minas gerais state", "estado de minas gerais"],
    "pará": ["pa", "para", "para state", "estado do para", "estado do pará"],
    "maranhão": ["ma", "maranhao", "maranhao state", "estado do maranhão"],
    "pernambuco": ["pe", "pernambuco state"],
    "ceará": ["ce", "ceara", "ceara state", "estado do ceará"],
    "rio de janeiro": ["rj", "rio", "rio de janeiro state", "estado do rio de janeiro"],
    "rio grande do norte": ["rn", "rio grande do norte state"],
    "espírito santo": ["es", "espirito santo", "espirito santo state"],
    "goiás": ["go", "goias", "goias state"],
    "mato grosso do sul": ["ms", "mato grosso do sul state"],
    "mato grosso": ["mt", "mato grosso state"],
    "roraima": ["rr", "roraima state"],
    "rondônia": ["ro", "rondonia", "rondonia state"],
    "tocantins": ["to", "tocantins state"],
    "amapá": ["ap", "amapa", "amapa state"],
    "piauí": ["pi", "piaui", "piaui state"],
    "alagoas": ["al", "alagoas state"],
    "sergipe": ["se", "sergipe state"],
    "paraíba": ["pb", "paraiba", "paraiba state"],
    "petrópolis": ["petropolis"],
    "oriximiná": ["oriximina"],
    "carapicuíba": ["carapicuiba"],

    # ── Colombia — departments ─────────────────────────────────────────────────
    "cundinamarca": ["cundinamarca department", "departamento de cundinamarca"],
    "nariño": ["narino", "narino department", "departamento de nariño"],
    "norte de santander": ["north santander", "norte santander", "north santander department"],
    "la mojana": ["la mojana sub-region", "sub-región de la mojana"],
    "bolívar": ["bolivar", "bolivar department"],
    "sucre": ["sucre department"],   # Colombia dept — see AMBIGUOUS for Bolivia capital
    "antioquia": ["antioquia department"],
    "chocó": ["choco", "choco department"],
    "cauca": ["cauca department"],
    "valle del cauca": ["valle del cauca department", "valle"],
    "magdalena": ["magdalena department"],
    "córdoba": ["cordoba department"],   # Colombia dept
    "atlántico": ["atlantico", "atlantico department"],
    "meta": ["meta department"],
    "huila": ["huila department"],
    "tolima": ["tolima department"],
    "caldas": ["caldas department"],
    "santander": ["santander department"],
    "boyacá": ["boyaca", "boyaca department"],
    "casanare": ["casanare department"],
    "guainía": ["guainia", "guainia department"],
    "vichada": ["vichada department"],

    # ── Bolivia — departments ──────────────────────────────────────────────────
    "chuquisaca": ["chuquisaca department", "departamento de chuquisaca"],
    "la paz": ["la paz department", "la paz dept", "departamento de la paz"],
    "cochabamba": ["cochabamba department", "departamento de cochabamba"],
    "beni": ["beni department", "departamento del beni", "el beni"],
    "oruro": ["oruro department"],
    "potosí": ["potosi", "potosi department"],
    "santa cruz": ["santa cruz department", "santa cruz de la sierra"],
    "tarija": ["tarija department"],
    "pando": ["pando department"],
    "tipuani": ["tipuani municipality"],

    # ── Peru — regions ─────────────────────────────────────────────────────────
    "cusco": ["cuzco", "cusco region", "departamento del cusco", "departamento de cusco"],
    "tumbes": ["tumbes region", "tumbes province"],
    "junín": ["junin", "junin region", "departamento de junin"],
    "ancash": ["ancash region", "departamento de ancash", "áncash"],
    "piura": ["piura region", "departamento de piura"],
    "loreto": ["loreto region", "departamento de loreto"],
    "madre de dios": ["madre de dios region"],
    "puno": ["puno region", "departamento de puno"],
    "ayacucho": ["ayacucho region"],
    "apurímac": ["apurimac", "apurimac region"],
    "arequipa": ["arequipa region", "departamento de arequipa"],
    "ica": ["ica region"],
    "la libertad": ["la libertad region"],
    "lambayeque": ["lambayeque region"],
    "lima": ["lima region", "departamento de lima"],
    "ucayali": ["ucayali region"],
    "huánuco": ["huanuco", "huanuco region"],
    "cajamarca": ["cajamarca region"],
    "san martín": ["san martin", "san martin region"],
    "tahuamanu": ["tahuamanu province"],
    "contralmirante villar": ["contralmirante villar province"],

    # ── Ecuador — provinces ────────────────────────────────────────────────────
    "esmeraldas": ["esmeraldas province"],
    "manabí": ["manabi", "manabi province"],
    "el oro": ["el oro province"],
    "morona santiago": ["morona-santiago", "morona santiago province"],
    "cotopaxi": ["cotopaxi province"],
    "azuay": ["azuay province"],
    "chimborazo": ["chimborazo province"],
    "guayas": ["guayas province"],
    "los ríos": ["los rios", "los rios province"],
    "pichincha": ["pichincha province"],
    "loja": ["loja province"],
    "imbabura": ["imbabura province"],
    "tungurahua": ["tungurahua province"],
    "bolívar province": ["bolivar province"],
    "carchi": ["carchi province"],
    "napo": ["napo province"],
    "sucumbíos": ["sucumbios", "sucumbios province"],
    "pastaza": ["pastaza province"],
    "zamora chinchipe": ["zamora-chinchipe", "zamora chinchipe province"],
    "alfredo baquerizo moreno": ["jujan"],

    # ── Mexico — states ────────────────────────────────────────────────────────
    "veracruz": ["veracruz state", "veracruz-llave", "estado de veracruz"],
    "morelos": ["morelos state", "estado de morelos"],
    "querétaro": ["queretaro", "queretaro state", "estado de querétaro"],
    "guerrero": ["guerrero state", "estado de guerrero"],
    "oaxaca": ["oaxaca state", "estado de oaxaca"],
    "chiapas": ["chiapas state"],
    "tabasco": ["tabasco state"],
    "hidalgo": ["hidalgo state"],
    "jalisco": ["jalisco state"],
    "puebla": ["puebla state"],
    "michoacán": ["michoacan", "michoacan state"],
    "sinaloa": ["sinaloa state"],
    "sonora": ["sonora state"],
    "colima": ["colima state"],
    "nuevo león": ["nuevo leon", "nuevo leon state"],
    "tamaulipas": ["tamaulipas state"],
    "durango": ["durango state"],
    "zacatecas": ["zacatecas state"],
    "san luis potosí": ["san luis potosi", "san luis potosi state"],
    "guanajuato": ["guanajuato state"],
    "baja california": ["baja california state"],
    "campeche": ["campeche state"],
    "yucatán": ["yucatan", "yucatan state"],
    "quintana roo": ["quintana roo state"],
    "nayarit": ["nayarit state"],
    "aguascalientes": ["aguascalientes state"],
    "coahuila": ["coahuila state"],
    "chihuahua": ["chihuahua state"],
    "tlaxcala": ["tlaxcala state"],
    "filomeno mata": ["filomeno mata municipality"],
    "tlayacapan": ["tlayacapan municipality"],

    # ── Venezuela — states ─────────────────────────────────────────────────────
    "táchira": ["tachira", "tachira state", "estado táchira"],
    "barinas": ["barinas state", "estado barinas"],
    "miranda": ["miranda state", "estado miranda"],
    "la guaira": ["vargas", "vargas state", "la guaira state", "estado vargas"],
    "mérida": ["merida state", "estado mérida"],
    "zulia": ["zulia state"],
    "carabobo": ["carabobo state"],
    "aragua": ["aragua state"],
    "bolívar state": ["bolivar state", "estado bolivar"],
    "anzoátegui": ["anzoategui", "anzoategui state"],
    "sucre state": ["sucre estado"],
    "monagas": ["monagas state"],
    "falcón": ["falcon", "falcon state"],
    "lara": ["lara state"],
    "portuguesa": ["portuguesa state"],
    "yaracuy": ["yaracuy state"],
    "cojedes": ["cojedes state"],
    "guárico": ["guarico", "guarico state"],
    "apure": ["apure state"],
    "nueva esparta": ["nueva esparta state"],
    "macuto": ["macuto parish"],
    "caraballeda": ["caraballeda parish"],
    "febres cordero": ["febres cordero municipality"],

    # ── Argentina — provinces ──────────────────────────────────────────────────
    "buenos aires": ["buenos aires province", "pba", "provincia de buenos aires"],
    "bahia blanca": ["bahía blanca"],
    "corrientes": ["corrientes province"],
    "entre ríos": ["entre rios", "entre rios province"],
    "mendoza": ["mendoza province"],
    "córdoba province": ["cordoba province", "provincia de córdoba"],
    "tucumán": ["tucuman", "tucuman province"],
    "salta": ["salta province"],
    "jujuy": ["jujuy province"],
    "chaco": ["chaco province"],
    "formosa": ["formosa province"],
    "misiones": ["misiones province"],
    "santa fe": ["santa fe province"],
    "san juan": ["san juan province"],
    "la rioja": ["la rioja province"],
    "catamarca": ["catamarca province"],
    "río negro": ["rio negro", "rio negro province"],
    "neuquén": ["neuquen", "neuquen province"],
    "chubut": ["chubut province"],
    "santa cruz province": ["santa cruz argentina"],
    "tierra del fuego": ["tierra del fuego province"],
    "la pampa": ["la pampa province"],
    "san luis": ["san luis province"],
    "santiago del estero": ["santiago del estero province"],

    # ── Chile — regions ────────────────────────────────────────────────────────
    "ñuble": ["nuble", "nuble region", "región de ñuble"],
    "biobío": ["biobio", "biobio region", "bío-bío"],
    "araucanía": ["araucania", "la araucania", "araucania region", "región de la araucanía"],
    "maule": ["maule region"],
    "los ríos": ["los rios region", "región de los ríos"],
    "o'higgins": [
        "o'higgins region", "libertador o'higgins",
        "libertador general bernardo o'higgins", "ohiggins"
    ],
    "metropolitana": [
        "santiago metropolitan", "metropolitan region", "rm",
        "region metropolitana", "región metropolitana de santiago"
    ],
    "valparaíso": ["valparaiso", "valparaiso region"],
    "los lagos": ["los lagos region"],
    "aysén": ["aysen", "aysen region"],
    "magallanes": ["magallanes region"],
    "coquimbo": ["coquimbo region"],
    "atacama": ["atacama region"],
    "antofagasta": ["antofagasta region"],
    "tarapacá": ["tarapaca", "tarapaca region"],
    "arica y parinacota": ["arica and parinacota"],
    "o'higgins": ["ohiggins", "libertador region"],

    # ── Guatemala — departments ────────────────────────────────────────────────
    "zacapa": ["zacapa department"],
    "quiché": ["quiche", "quiche department", "el quiche"],
    "sacatepéquez": ["sacatepequez", "sacatepequez department"],
    "suchitepéquez": ["suchitepequez", "suchitepequez department"],
    "jutiapa": ["jutiapa department"],
    "san marcos": ["san marcos department"],
    "huehuetenango": ["huehuetenango department"],
    "alta verapaz": ["alta verapaz department"],
    "baja verapaz": ["baja verapaz department"],
    "chiquimula": ["chiquimula department"],
    "retalhuleu": ["retalhuleu department"],
    "escuintla": ["escuintla department"],
    "izabal": ["izabal department"],
    "petén": ["peten", "peten department"],
    "chimaltenango": ["chimaltenango department"],
    "guatemal": ["guatemala department"],

    # ── Honduras — departments ─────────────────────────────────────────────────
    "francisco morazán": ["francisco morazan", "francisco morazan department"],
    "cortés": ["cortes", "cortes department"],
    "atlántida": ["atlantida", "atlantida department"],
    "lempira": ["lempira department"],
    "intibucá": ["intibuca", "intibuca department"],
    "islas de la bahía": ["islas de la bahia", "bay islands"],
    "colón": ["colon department"],
    "gracias a dios": ["gracias a dios department"],
    "yoro": ["yoro department"],
    "comayagua": ["comayagua department"],
    "copán": ["copan", "copan department"],
    "santa bárbara": ["santa barbara department"],
    "la masica": ["la masica municipality"],

    # ── Paraguay — departments ─────────────────────────────────────────────────
    "alto paraná": ["alto parana", "alto parana department"],
    "itapúa": ["itapua", "itapua department"],
    "concepción": ["concepcion", "concepcion department"],
    "canindeyú": ["canindeyu", "canindeyu department"],
    "misiones": ["misiones department"],
    "neembucú": ["neembucu", "neembucu department"],
    "central": ["central department"],
    "paraguarí": ["paraguari", "paraguari department"],
    "caaguazú": ["caaguazu", "caaguazu department"],
    "alto paraguay": ["alto paraguay department"],
    "amambay": ["amambay department"],
    "boquerón": ["boqueron", "boqueron department"],
    "presidente hayes": ["presidente hayes department"],

    # ── Uruguay — departments ──────────────────────────────────────────────────
    "montevideo": ["montevideo department", "montevideo capital city", "montevideo capital"],
    "canelones": ["canelones department"],
    "paysandú": ["paysandu", "paysandu department"],
    "cerro largo": ["cerro largo department"],
    "rocha": ["rocha department"],
    "salto": ["salto department"],
    "soriano": ["soriano department"],
    "tacuarembó": ["tacuarembo", "tacuarembo department"],
    "treinta y tres": ["treinta y tres department"],
    "durazno": ["durazno department"],
    "flores": ["flores department"],
    "san josé": ["san jose department", "san jose"],
    "florida department": ["florida uruguay"],
    "lavalleja": ["lavalleja department"],
    "maldonado": ["maldonado department"],
    "artigas": ["artigas department"],
    "colonia": ["colonia department", "nueva helvecia"],
    "rivera": ["rivera department"],

    # ── USA — states and counties ──────────────────────────────────────────────
    "kerr county": ["kerr county texas"],
    "chaves county": ["chaves county new mexico"],
    "new mexico": ["nm", "nuevo mexico"],
    "texas": ["tx", "tejas"],
    "san antonio": ["san antonio texas"],
    "ruidoso": ["ruidoso new mexico", "village of ruidoso"],
    "roswell": ["roswell new mexico"],
    "nueva jersey": ["new jersey"],   # Spanish-language docs
    "nueva york": ["new york"],

    # ── Costa Rica — provinces ─────────────────────────────────────────────────
    "alajuela": ["alajuela province"],
    "cartago": ["cartago province"],
    "guanacaste": ["guanacaste province"],
    "puntarenas": ["puntarenas province"],
    "heredia": ["heredia province"],
    "limón": ["limon", "limon province"],
    "san josé province": ["san jose province"],
    "sarapiquí": ["sarapiqui", "sarapiqui canton"],

    # ── Dominican Republic ─────────────────────────────────────────────────────
    "santo domingo": ["santo domingo province", "distrito nacional"],
    "azua": ["azua province"],
    "barahona": ["barahona province"],
    "duarte": ["duarte province"],
    "monte plata": ["monte plata province"],
    "san cristóbal": ["san cristobal", "san cristobal province"],
    "san pedro de macorís": ["san pedro de macoris", "san pedro de macoris province"],
    "la altagracia": ["la altagracia province"],
    "espaillat": ["espaillat province"],
    "hato mayor": ["hato mayor province"],
    "samana": ["samaná", "samana province"],
    "puerto plata": ["puerto plata province"],

    # ── Panama ─────────────────────────────────────────────────────────────────
    "bocas del toro": ["bocas del toro province"],
    "chiriquí": ["chiriqui", "chiriqui province"],
    "los santos": ["los santos province"],
    "colón province": ["colon province"],
    "veraguas": ["veraguas province"],
    "coclé": ["cocle", "cocle province"],
    "herrera": ["herrera province"],
    "darién": ["darien", "darien province"],
    "nabe-buglé": ["nabe bugle"],

    # ── Cuba ───────────────────────────────────────────────────────────────────
    "matanzas": ["matanzas province"],
    "pinar del río": ["pinar del rio", "pinar del rio province"],
    "granma": ["granma province"],
    "las tunas": ["las tunas province"],
    "santiago de cuba": ["santiago de cuba province"],
    "camagüey": ["camaguey", "camaguey province"],
    "san juan y martínez": ["san juan y martinez"],

    # ── Canada ─────────────────────────────────────────────────────────────────
    "alberta": ["alberta province"],
    "nova scotia": ["nova scotia province"],
    "québec": ["quebec", "quebec province"],
    "ontario": ["ontario province"],
    "british columbia": ["bc", "british columbia province"],
    "charlevoix": ["charlevoix region"],
    "lanaudière": ["lanaudiere", "lanaudiere region"],
    "baie-saint-paul": ["baie saint paul"],

    # ── Haiti ──────────────────────────────────────────────────────────────────
    "grand'anse": ["grande anse", "grand anse", "grand'anse department"],
    "artibonite": ["artibonite department"],
    "nord": ["nord department", "north haiti"],
    "sud": ["sud department", "south haiti"],
    "ouest": ["ouest department"],
    "nippes": ["nippes department"],

    # ── Guyana ─────────────────────────────────────────────────────────────────
    "demerara-mahaica": ["region 4", "region four"],
    "mahaica-berbice": ["region 5"],
    "upper demerara-upper berbice": ["region 10"],
    "cuyuni-mazaruni": ["region 7"],
    "upper takutu-upper essequibo": ["region 9"],
    "essequibo islands-west demerara": ["region 3"],

    # ── French Guiana ──────────────────────────────────────────────────────────
    "saint-laurent du maroni": ["saint-laurent-du-maroni", "saint laurent du maroni"],
    "camopi": ["camopi commune", "camopi-trois sauts"],
    "kourou": ["kourou district"],

    # ── El Salvador ────────────────────────────────────────────────────────────
    "sonsonate": ["sonsonate department"],
    "chalatenango": ["chalatenango department"],
    "ahuachapán": ["ahuachapan", "ahuachapan department"],
    "usulután": ["usulutan", "usulutan department"],
    "tecoluca": ["tecoluca municipality"],
    "jiquilisco": ["jiquilisco municipality"],

    # ── Puerto Rico ────────────────────────────────────────────────────────────
    "san juan": ["san juan pr", "san juan puerto rico"],
    "bayamón": ["bayamon"],
    "carolina": ["carolina municipality"],
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

    # Show first events for quick sanity check
    pilot_ids = [1, 2, 3, 4, 5]
    print("  Spot-check (first 5 events):")
    for fid in pilot_ids:
        entries = location_dict.get(fid, [])
        names   = [e['name'] for e in entries]
        print(f"  #{fid:>3}  {len(entries):>3} entries  ->  {names[:5]}{'...' if len(names) > 5 else ''}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 0d — Build location dictionary")
    parser.add_argument("--csv", default="data/flood_crawl.csv",  help="Path to flood_crawl.csv")
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

    print(f"Saved -> {args.out}")
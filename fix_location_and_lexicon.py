# =============================================================================
# fix_location_and_lexicon.py  ·  One-time fix script
# =============================================================================
# Rebuilds location_dictionary.parquet with proper location string parsing,
# and writes an expanded keyword_lexicon.json covering all 59 language codes
# in the dataset with everyday flood vocabulary (not just formal/technical terms).
#
# Run once before re-running stage_06:
#   python fix_location_and_lexicon.py
# =============================================================================

import csv
import importlib.util
import json
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Force-load local config.py
# ---------------------------------------------------------------------------
_config_path = Path(__file__).parent / "config.py"
_spec = importlib.util.spec_from_file_location("config", _config_path)
_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)
sys.modules["config"] = _config

from config import KEYWORD_LEXICON, OUTPUT_DIR

FLOOD_CSV    = Path(__file__).parent / "flood_crawl.csv"
CONFIG_DIR   = Path(__file__).parent / "config"

# =============================================================================
# PART 1 — Location dictionary rebuild
# =============================================================================

# Admin suffixes to strip from the end of a location token
ADMIN_SUFFIXES = re.compile(
    r'\b(autonomous region|special administrative region|'
    r'capital city area|city area|'
    r'provinces?|departments?|districts?|'
    r'regenc(y|ies)|counties?|states?|'
    r'governorates?|municipalit(y|ies)|'
    r'islands?|regions?|areas?|subdistricts?|'
    r'territories?|prefectures?|oblasts?|'
    r'wilayahs?|woredas?|upazilas?|'
    r'divisions?|sectors?|zones?)\s*$',
    re.I
)

# Tokens to drop entirely — pure noise
DROP_TOKENS = {
    "departments", "provinces", "province", "department", "region",
    "regions", "district", "districts", "area", "and", "the",
    "islands", "island", "states", "state", "county", "counties",
}


def normalize_text(s: str) -> str:
    """Lowercase and strip accents for consistent matching."""
    s = s.lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.strip()


def parse_location_string(raw: str) -> list[str]:
    """
    Parse a raw location string into clean individual place names.
    Handles: comma/semicolon splits, parenthetical content, admin suffixes,
    leading 'and', trailing noise tokens.
    """
    if not raw or not raw.strip():
        return []

    # Remove parenthetical content: (Java Isl.), (West Java province), etc.
    cleaned = re.sub(r'\([^)]*\)', ' ', raw)

    # Split on commas and semicolons
    parts = re.split(r'[,;]', cleaned)

    results = []
    for part in parts:
        part = part.strip()
        if not part:
            continue

        # Strip leading "and " (case insensitive)
        part = re.sub(r'^and\s+', '', part, flags=re.I).strip()

        # Strip trailing admin suffixes repeatedly until stable
        prev = None
        while prev != part:
            prev = part
            part = ADMIN_SUFFIXES.sub('', part).strip()

        # Strip trailing punctuation
        part = part.rstrip('.,;').strip()

        # Skip if empty, too short, or pure noise
        if not part or len(part) < 2:
            continue
        if part.lower() in DROP_TOKENS:
            continue
        # Skip if it's only numbers
        if re.match(r'^\d+$', part):
            continue

        results.append(part)

    return results


def rebuild_location_dictionary():
    """Rebuild location_dictionary.parquet from flood_crawl.csv."""
    print("=== Rebuilding location_dictionary ===")

    with open(FLOOD_CSV) as f:
        rows = list(csv.DictReader(f))

    records = []
    for row in rows:
        flood_id = int(row["Flood_ID"])
        raw_loc  = row.get("Location", "")

        places = parse_location_string(raw_loc)

        if not places:
            # Always add at least the country name as a fallback
            records.append({
                "flood_id":            flood_id,
                "location_raw":        raw_loc,
                "location_normalised": normalize_text(row.get("Country", "")),
                "aliases":             "[]",
            })
            continue

        for place in places:
            normalised = normalize_text(place)
            # Build aliases: original casing + normalized
            aliases = list({place, normalised} - {normalised})
            records.append({
                "flood_id":            flood_id,
                "location_raw":        raw_loc,
                "location_normalised": normalised,
                "aliases":             json.dumps(aliases),
            })

    loc_df = pd.DataFrame(records)
    out_path = OUTPUT_DIR / "location_dictionary.parquet"
    loc_df.to_parquet(out_path, index=False)

    print(f"  Saved {len(loc_df)} location records → {out_path}")

    # Show sample for pilot events
    print("\n  Sample — pilot events:")
    for fid in [1, 2, 3, 12, 19, 34]:
        terms = loc_df[loc_df["flood_id"] == fid]["location_normalised"].tolist()
        print(f"    Flood #{fid}: {terms}")


# =============================================================================
# PART 2 — Expanded keyword lexicon
# =============================================================================

# Flood vocabulary per language — everyday/journalistic terms, not just formal
# Key: ISO 639-3 code
# flood = flood/inundation words
# rain  = rain/storm words (often appear in flood articles)
# impact = emergency/disaster/affected words

EXPANDED_LEXICON = {

    # -------------------------------------------------------------------------
    # Tier 1 — High coverage languages
    # -------------------------------------------------------------------------
    "eng": {
        "flood": ["flood", "floods", "flooding", "flooded", "flash flood",
                  "inundation", "inundated", "deluge", "submerged", "overflow",
                  "overflowed", "floodwater", "floodwaters", "waterlogged",
                  "storm surge", "high water", "rising water"],
        "rain":  ["heavy rain", "heavy rainfall", "torrential rain", "downpour",
                  "storm", "cyclone", "hurricane", "typhoon", "monsoon"],
        "impact": ["emergency", "disaster", "evacuated", "displaced", "victims",
                   "casualties", "deaths", "relief", "rescue", "damage"],
    },

    "fra": {
        "flood": ["inondation", "inondations", "inondé", "inondée", "inondés",
                  "crue", "crues", "débordement", "débordements", "débordé",
                  "déluge", "submersion", "submergé", "montée des eaux",
                  "eaux montantes", "innondation", "innondations"],  # common misspelling
        "rain":  ["pluies", "pluies torrentielles", "fortes pluies", "averse",
                  "orage", "tempête", "cyclone", "intempéries"],
        "impact": ["urgence", "catastrophe", "évacués", "déplacés", "victimes",
                   "bilan", "secours", "dégâts", "sinistres"],
    },

    "spa": {
        "flood": ["inundación", "inundaciones", "inundado", "inundada",
                  "inundados", "desbordamiento", "desbordó", "desborde",
                  "crecida", "creciente", "riada", "anegado", "anegamiento",
                  "aluvión", "alud", "diluvio", "aguacero"],
        "rain":  ["lluvias", "lluvia", "lluvias intensas", "lluvias torrenciales",
                  "tormenta", "temporal", "aguacero", "precipitaciones",
                  "ciclón", "huracán", "tifón"],
        "impact": ["emergencia", "desastre", "evacuados", "desplazados",
                   "víctimas", "afectados", "muertos", "rescate", "daños",
                   "damnificados"],
    },

    "por": {
        "flood": ["inundação", "inundações", "inundado", "inundada",
                  "enchente", "enchentes", "cheia", "cheias", "alagamento",
                  "alagamentos", "transbordamento", "transbordou",
                  "submersão", "dilúvio"],
        "rain":  ["chuvas", "chuva intensa", "chuva torrencial", "tempestade",
                  "temporal", "ciclone", "furacão", "precipitação"],
        "impact": ["emergência", "desastre", "evacuados", "deslocados",
                   "vítimas", "afetados", "mortos", "resgate", "danos"],
    },

    "arb": {
        "flood": ["فيضان", "فيضانات", "سيول", "سيل", "فياضانات",
                  "إغراق", "غرق", "طوفان", "أمطار جارفة", "فيضة"],
        "rain":  ["أمطار غزيرة", "أمطار", "عاصفة", "إعصار", "موسم الأمطار",
                  "هطول الأمطار", "منخفض جوي"],
        "impact": ["كارثة", "إخلاء", "نازحون", "ضحايا", "قتلى", "إنقاذ",
                   "طوارئ", "أضرار", "مناطق منكوبة"],
    },

    "ind": {
        "flood": ["banjir", "banjir bandang", "banjir rob", "banjir besar",
                  "kebanjiran", "terendam", "tergenang", "genangan",
                  "meluap", "luapan", "bencana banjir", "air bah",
                  "banjir parah", "banjir terjang"],
        "rain":  ["hujan lebat", "hujan deras", "hujan", "badai", "topan",
                  "siklon", "curah hujan", "banjir kiriman"],
        "impact": ["darurat", "bencana", "pengungsian", "mengungsi", "korban",
                   "evakuasi", "kerugian", "terdampak", "meninggal", "selamatkan"],
    },

    "zlm": {  # Malay
        "flood": ["banjir", "banjir kilat", "banjir besar", "air bah",
                  "bencana banjir", "limpahan", "melimpah", "dilanda banjir",
                  "kawasan banjir", "banjir teruk"],
        "rain":  ["hujan lebat", "hujan", "ribut", "taufan", "siklon",
                  "hujan deras", "hujan berterusan"],
        "impact": ["darurat", "bencana", "mangsa", "dipindahkan", "korban",
                   "kerosakan", "bantuan", "operasi menyelamat"],
    },

    "hin": {
        "flood": ["बाढ़", "बाढ़ आई", "बाढ़ का", "बाढ़ में", "सैलाब",
                  "जलप्रलय", "जलभराव", "जलमग्न", "बाढ़ग्रस्त"],
        "rain":  ["भारी बारिश", "बारिश", "मूसलाधार बारिश", "तूफान",
                  "चक्रवात", "मानसून", "भारी वर्षा"],
        "impact": ["आपदा", "राहत", "बचाव", "मृत्यु", "मौतें", "विस्थापित",
                   "प्रभावित", "नुकसान", "आपातकाल"],
    },

    "urd": {
        "flood": ["سیلاب", "سیل", "طوفان", "بارشیں", "ڈوبنا",
                  "سیلابی صورتحال", "طغیانی"],
        "rain":  ["شدید بارش", "بارش", "طوفان", "سمندری طوفان", "مون سون"],
        "impact": ["آفت", "ریلیف", "بچاؤ", "ہلاکتیں", "متاثرین",
                   "نقصان", "ایمرجنسی"],
    },

    "fas": {  # Persian (Farsi)
        "flood": ["سیل", "سیلاب", "طغیان", "آبگرفتگی", "غرق شدن",
                  "جاری شدن سیل", "بارندگی سیل‌آسا"],
        "rain":  ["باران شدید", "باران", "طوفان", "سیکلون", "بارش"],
        "impact": ["فاجعه", "تخلیه", "آواره", "قربانیان", "کشته",
                   "امداد", "اورژانس", "خسارت"],
    },

    "prs": {  # Dari (Afghanistan)
        "flood": ["سیل", "سیلاب", "طغیان", "آبخیزی", "جاری شدن سیل"],
        "rain":  ["باران شدید", "باران", "طوفان", "بارش"],
        "impact": ["فاجعه", "آواره", "قربانیان", "کشته", "امداد", "اورژانس"],
    },

    "cmn": {  # Mandarin Chinese
        "flood": ["洪水", "洪灾", "水灾", "洪涝", "内涝", "暴洪",
                  "洪峰", "汛情", "决堤", "漫堤", "淹水", "被淹"],
        "rain":  ["暴雨", "大雨", "强降雨", "台风", "龙卷风", "强降水"],
        "impact": ["灾害", "灾情", "撤离", "转移", "伤亡", "遇难",
                   "救援", "紧急", "损失", "受灾"],
    },

    "yue": {  # Cantonese
        "flood": ["洪水", "水浸", "洪災", "水災", "氾濫", "泥石流"],
        "rain":  ["暴雨", "大雨", "颱風", "熱帶氣旋"],
        "impact": ["災害", "撤離", "傷亡", "救援", "緊急"],
    },

    "tha": {
        "flood": ["น้ำท่วม", "อุทกภัย", "น้ำหลาก", "น้ำท่วมขัง",
                  "น้ำท่วมฉับพลัน", "น้ำล้นตลิ่ง", "น้ำท่วมใหญ่"],
        "rain":  ["ฝนตกหนัก", "ฝน", "พายุ", "พายุไต้ฝุ่น", "มรสุม"],
        "impact": ["ภัยพิบัติ", "อพยพ", "ผู้ประสบภัย", "เสียชีวิต",
                   "ช่วยเหลือ", "ฉุกเฉิน", "ความเสียหาย"],
    },

    "kor": {
        "flood": ["홍수", "수해", "침수", "범람", "홍수 피해",
                  "침수 피해", "폭우 피해"],
        "rain":  ["폭우", "집중호우", "호우", "태풍", "장마"],
        "impact": ["재난", "재해", "대피", "이재민", "사망", "구조", "피해"],
    },

    "jpn": {
        "flood": ["洪水", "水害", "浸水", "氾濫", "鉄砲水",
                  "冠水", "洪水被害"],
        "rain":  ["大雨", "豪雨", "台風", "記録的大雨"],
        "impact": ["災害", "避難", "被災", "死者", "救助", "緊急"],
    },

    "vie": {
        "flood": ["lũ lụt", "lũ", "ngập lụt", "ngập", "lụt",
                  "lũ quét", "nước dâng", "vỡ đê"],
        "rain":  ["mưa lớn", "mưa", "bão", "lốc xoáy", "áp thấp nhiệt đới"],
        "impact": ["thảm họa", "sơ tán", "thiệt hại", "nạn nhân",
                   "tử vong", "cứu hộ", "khẩn cấp"],
    },

    "ben": {  # Bengali
        "flood": ["বন্যা", "বন্যা পরিস্থিতি", "প্লাবন", "জলাবদ্ধতা",
                  "ডুবে যাওয়া", "বন্যাকবলিত"],
        "rain":  ["ভারী বৃষ্টি", "বৃষ্টি", "ঝড়", "ঘূর্ণিঝড়", "বর্ষা"],
        "impact": ["দুর্যোগ", "উদ্ধার", "মৃত্যু", "ক্ষতিগ্রস্ত",
                   "ত্রাণ", "জরুরি", "বাস্তুচ্যুত"],
    },

    "npi": {  # Nepali
        "flood": ["बाढी", "बाढी आयो", "बाढी पहिरो", "जलमग्न",
                  "डुबान", "बाढीले"],
        "rain":  ["भारी वर्षा", "वर्षा", "मनसुन", "तुफान"],
        "impact": ["विपद", "उद्धार", "मृत्यु", "प्रभावित",
                   "राहत", "आपतकाल"],
    },

    "pus": {  # Pashto
        "flood": ["سیلاب", "سیل", "بهیر", "طوفاني بارانونه"],
        "rain":  ["درنه باران", "باران", "طوفان"],
        "impact": ["ناورین", "وژل شوي", "زیانونه", "مرسته"],
    },

    # -------------------------------------------------------------------------
    # African languages
    # -------------------------------------------------------------------------
    "swa": {  # Swahili
        "flood": ["mafuriko", "gharika", "mafuriko makubwa",
                  "mto kufurika", "maji kufurika", "eneo kuzama"],
        "rain":  ["mvua kubwa", "mvua", "dhoruba", "kipindi cha mvua"],
        "impact": ["maafa", "msaada", "waathirika", "vifo",
                   "uokoaji", "dharura", "uharibifu"],
    },

    "fra_afr": {  # French used as fallback for African Tier 3 languages
        "flood": ["inondation", "inondations", "inondé", "crue", "crues",
                  "débordement", "innondation"],
        "rain":  ["pluies", "fortes pluies", "averse", "tempête"],
        "impact": ["catastrophe", "victimes", "évacués", "secours", "dégâts"],
    },

    "hau": {  # Hausa
        "flood": ["ambaliyar ruwa", "ambaliya", "ruwan sama mai yawa",
                  "kogin yaduwa"],
        "rain":  ["ruwan sama", "ruwa mai yawa", "hadari"],
        "impact": ["bala'i", "taimako", "mutuwa", "lalacewa"],
    },

    "yor": {  # Yoruba
        "flood": ["ikun omi", "omi nṣan", "ìkún omi", "ìgbàkúnlé"],
        "rain":  ["òjò", "òjò líle", "iji"],
        "impact": ["àjálù", "ìrànlọ́wọ́", "ikú", "ìpalára"],
    },

    "ibo": {  # Igbo
        "flood": ["mmiri ọnụ", "mmiri ukwu", "ozuzo dara ọda"],
        "rain":  ["ozuzo", "ozuzo ukwu", "ifufe"],
        "impact": ["ọghọm", "enyemaka", "ọnwụ", "mweda"],
    },

    "som": {  # Somali
        "flood": ["daad", "daadgureeysi", "biyo badan", "roob baahsan"],
        "rain":  ["roob", "roob xoog leh", "duufaan"],
        "impact": ["masiibo", "gargaar", "dhimasho", "barakac"],
    },

    "afr": {  # Afrikaans
        "flood": ["vloed", "vloede", "oorstroming", "oorstromings",
                  "watersnood", "onderwater"],
        "rain":  ["swaar reën", "reën", "storm", "sikloon"],
        "impact": ["ramp", "noodsituasie", "slagoffers", "skade", "redding"],
    },

    "zul": {  # Zulu
        "flood": ["izikhukhula", "ukuphuphuma kwamanzi", "izikhukhula"],
        "rain":  ["izimvula", "izimvula ezinamandla", "isiphepho"],
        "impact": ["inhlekelele", "usizo", "ukufa", "ukulimala"],
    },

    "xho": {  # Xhosa
        "flood": ["izikhukhula", "ukuphuphuma kwamanzi"],
        "rain":  ["imvula", "imvula enamandla", "isiphango"],
        "impact": ["intlekele", "uncedo", "ukufa"],
    },

    "mlg": {  # Malagasy
        "flood": ["tondra-drano", "tondra", "fanidiana"],
        "rain":  ["orana", "orana be", "rivotra"],
        "impact": ["loza", "famonjena", "maty"],
    },

    "sna": {  # Shona
        "flood": ["mafashamo", "mvura inoyerera"],
        "rain":  ["mvura", "mvura inokora", "dutu"],
        "impact": ["dambudziko", "rubatsiro", "kufa"],
    },

    "nya": {  # Chichewa
        "flood": ["chigumula", "madzi osefukira"],
        "rain":  ["mvula", "mvula yambiri", "mphepo"],
        "impact": ["ngozi", "thandizo", "imfa"],
    },

    "tsn": {  # Tswana
        "flood": ["morwalela", "metsi a tlhaela"],
        "rain":  ["pula", "pula e e ntsintsi", "sefefo"],
        "impact": ["kotsi", "thuso", "loso"],
    },

    "lug": {  # Luganda
        "flood": ["amazzi amayirika", "nfuzi"],
        "rain":  ["enkuba", "enkuba ennene", "kibuyaga"],
        "impact": ["akabi", "obuyambi", "okufa"],
    },

    # Tier 3 — used with fra/eng fallback, but add native terms anyway
    "lin": {"flood": ["mai etikali", "nzela ya mai"], "rain": ["mbula", "mbula makasi"], "impact": ["likambo", "lisalisi"]},
    "kon": {"flood": ["maza manene", "nzadi ya mfula"], "rain": ["mvula", "mvula ya nene"], "impact": ["mbevo", "lisalisi"]},
    "lua": {"flood": ["maji makubwa", "mafuriko"], "rain": ["mvula", "mvula mukubwa"], "impact": ["bubi", "lusungu"]},
    "sag": {"flood": ["ndö", "lo ndo"], "rain": ["tö", "tö nzöni"], "impact": ["gïgï", "mbï"]},
    "men": {"flood": ["ji big", "kawuli"], "rain": ["ji", "moli"], "impact": ["ngorgo", "kambei"]},
    "tem": {"flood": ["kpam", "lowulo"], "rain": ["saŋ", "saŋ puŋ"], "impact": ["kafoo", "ballii"]},
    "mnk": {"flood": ["jii kooro", "baa kooro"], "rain": ["sito", "sito baa"], "impact": ["janto", "diyaando"]},
    "wol": {"flood": ["ndiaye", "ndox bu xoore"], "rain": ["ndox", "todd"], "impact": ["bon yëgël", "ndimbël"]},
    "hat": {  # Haitian Creole
        "flood": ["inondasyon", "dlo ap monte", "ravin", "inondasyon bèf"],
        "rain":  ["lapli fò", "lapli", "siklòn", "loraj"],
        "impact": ["katastwòf", "sekou", "mouri", "viktim", "ijans"],
    },
    "que": {  # Quechua
        "flood": ["para yakun", "yacu huayco", "wayqo"],
        "rain":  ["para", "hatun para", "wayra"],
        "impact": ["llaki", "yanapay", "wañuy"],
    },
    "ayu": {  # Aymara
        "flood": ["uma phujru", "uma tantachawi"],
        "rain":  ["jallu", "hatun jallu"],
        "impact": ["jach'a panthaña", "yanapiri"],
    },

    # -------------------------------------------------------------------------
    # European languages
    # -------------------------------------------------------------------------
    "bul": {
        "flood": ["наводнение", "наводнения", "потоп", "разлив"],
        "rain":  ["проливен дъжд", "дъжд", "буря"],
        "impact": ["бедствие", "жертви", "евакуация", "щети"],
    },
    "ukr": {
        "flood": ["повінь", "повені", "підтоплення", "затоплення", "паводок"],
        "rain":  ["сильні дощі", "злива", "шторм"],
        "impact": ["катастрофа", "жертви", "евакуація", "збитки"],
    },
    "hrv": {
        "flood": ["poplava", "poplave", "poplavljeno", "bujica"],
        "rain":  ["jake kiše", "kiša", "oluja"],
        "impact": ["katastrofa", "žrtve", "evakuacija", "šteta"],
    },
    "bos": {
        "flood": ["poplava", "poplave", "poplavljeno", "bujica"],
        "rain":  ["jake kiše", "kiša", "oluja"],
        "impact": ["katastrofa", "žrtve", "evakuacija", "šteta"],
    },
    "srp": {
        "flood": ["поплава", "поплаве", "поплављено", "бујица"],
        "rain":  ["јаке кише", "киша", "олуја"],
        "impact": ["катастрофа", "жртве", "евакуација", "штета"],
    },
    "ron": {
        "flood": ["inundație", "inundații", "inundat", "revărsare"],
        "rain":  ["ploi torențiale", "ploaie", "furtună"],
        "impact": ["catastrofă", "victime", "evacuare", "pagube"],
    },
    "kat": {  # Georgian
        "flood": ["წყალდიდობა", "დატბორვა", "კატასტროფული წყალდიდობა"],
        "rain":  ["ძლიერი წვიმა", "წვიმა", "ქარიშხალი"],
        "impact": ["კატასტროფა", "მსხვერპლი", "ევაკუაცია"],
    },
    "ita": {
        "flood": ["alluvione", "alluvioni", "inondazione", "esondazione",
                  "allagamento", "esondato"],
        "rain":  ["piogge intense", "pioggia", "temporale", "nubifragio"],
        "impact": ["catastrofe", "vittime", "evacuati", "danni"],
    },
    "deu": {
        "flood": ["überschwemmung", "überschwemmungen", "hochwasser",
                  "flut", "überflutung", "überflutungen"],
        "rain":  ["starkregen", "regen", "sturm", "unwetter"],
        "impact": ["katastrophe", "opfer", "evakuierung", "schäden"],
    },
    "ckb": {  # Central Kurdish (Sorani)
        "flood": ["لافاو", "ئاو گەورە", "ئاو داهاتن"],
        "rain":  ["بارانی زۆر", "باران", "توفان"],
        "impact": ["فاجیعە", "قوربانی", "خەسارەت"],
    },
    "khm": {  # Khmer
        "flood": ["ទឹកជំនន់", "ជំនន់ទឹក", "ទឹកលើក"],
        "rain":  ["ភ្លៀងធ្លាក់", "ព្យុះ", "ខ្យល់"],
        "impact": ["គ្រោះមហន្ត", "ជំនួយ", "ស្លាប់"],
    },
    "lao": {
        "flood": ["ນ້ຳຖ້ວມ", "ນ້ຳທ່ວມ", "ອຸທົກກະໄພ"],
        "rain":  ["ຝົນຕົກໜັກ", "ຝົນ", "ພະຍຸ"],
        "impact": ["ໄພພິບັດ", "ຜູ້ຖືກກະທົບ", "ຕາຍ"],
    },
    "mya": {  # Burmese
        "flood": ["ရေကြီးရေလျှံ", "ရေကြီး", "ရေလျှံ"],
        "rain":  ["မိုးသည်းထန်", "မိုး", "မုန်တိုင်း"],
        "impact": ["ဘေးအန္တရာယ်", "သေဆုံး", "ထိခိုက်"],
    },
    "tgl": {  # Filipino/Tagalog
        "flood": ["baha", "pagbaha", "bumaha", "bumabaha", "naanod"],
        "rain":  ["malakas na ulan", "ulan", "bagyo", "typhoon"],
        "impact": ["kalamidad", "likas", "namatay", "nasalanta", "tulong"],
    },
    "ber": {  # Berber/Tamazight
        "flood": ["asif", "ammas n waman", "tamurt tessun"],
        "rain":  ["anzar", "tafat", "aḍu"],
        "impact": ["tamsultant", "tallelt", "imtti"],
    },
}


def rebuild_keyword_lexicon():
    """Write expanded keyword_lexicon.json."""
    print("\n=== Rebuilding keyword_lexicon ===")

    # Merge with any existing lexicon to preserve entries we might have missed
    existing = {}
    if KEYWORD_LEXICON.exists():
        with open(KEYWORD_LEXICON) as f:
            existing = json.load(f)

    # Expanded lexicon takes priority; merge river terms from existing
    merged = {}
    for lang, terms in EXPANDED_LEXICON.items():
        merged[lang] = terms.copy()
        if lang in existing and "river" in existing[lang]:
            merged[lang]["river"] = existing[lang]["river"]

    # Keep any languages in existing that aren't in expanded
    for lang, terms in existing.items():
        if lang not in merged:
            merged[lang] = terms

    CONFIG_DIR.mkdir(exist_ok=True)
    with open(KEYWORD_LEXICON, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"  Saved {len(merged)} language entries → {KEYWORD_LEXICON}")
    print("  Languages covered:", sorted(merged.keys()))


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    rebuild_location_dictionary()
    rebuild_keyword_lexicon()
    print("\nDone. Now re-run: python3 stage_06_clean_deduplicate.py")
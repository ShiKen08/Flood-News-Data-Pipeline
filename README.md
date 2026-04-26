# Flood Data Pipeline

A Python pipeline for extracting news and government articles about flood events
from [Common Crawl](https://commoncrawl.org/)'s web archive. Covers **227 flood
events across the Americas (2020–2025)**, spanning 25 countries.

Primary languages: Spanish (62%), Portuguese (20%), English (15%), French (<3%).

## Overview

Sequential stage architecture. Each stage reads the previous stage's Parquet output
and writes its own to `output/`.

```
Stage 00  Preflight              Crawl lag check, window assignment, language tiers, location dict
Stage 01  Query Specs            Build keyword + domain queries per event
Stage 02  CC Index               Query Common Crawl CDX index → raw pointer list
Stage 03  Validate Pointers      Filter, size-check, deduplicate pointers
Stage 04  WARC Download          Download WARC slices from CC S3
Stage 05  Text Extraction        Extract article body text (trafilatura ML)
Stage 06  Clean & Deduplicate    Clean text, detect language, score relevance, deduplicate
Stage 07  URL Report             Generate human-readable CSV for review
Stage 08  NLP Bridge             Export relevant articles to NLP-model submodule
```

---

## Setup

### 1. Clone with submodule

```bash
git clone --recurse-submodules https://github.com/ShiKen08/Flood-News-Data-Pipeline
cd flood-pipeline
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure event scope

Open `config.py` and set `PILOT_FLOOD_IDS`:

```python
# Run all 227 events (full dataset)
PILOT_FLOOD_IDS = None

# Run a specific subset (e.g. first 20 events)
PILOT_FLOOD_IDS = list(range(1, 21))

# Run a hand-picked set
PILOT_FLOOD_IDS = [65, 128]
```

When `PILOT_FLOOD_IDS` is set, all stages default to that subset without any extra flags.
Pass `--all` to any stage to override and force the full dataset regardless.

---

## Running the pipeline

Run stages in order. Each stage reads `output/` from the previous stage.

```bash
python stage_00_preflight.py
python stage_01_query_specs.py
python stage_02_query_cc_index.py
python stage_03_validate_pointers.py
python stage_04_download_warc.py
python stage_05_extract_text.py
python stage_06v_clean_deduplicate.py
python stage_07_url_report.py
python stage_08_nlp_analysis.py
```

### Common flags (available on most stages)

```bash
# Force all 227 events regardless of PILOT_FLOOD_IDS
python stage_02_query_cc_index.py --all

# Single event (useful for debugging)
python stage_04_download_warc.py --flood-id 65

# Download stage: download all pointers (not just BATCH_SIZE sample)
python stage_04_download_warc.py --full
```

### Performance tuning

Worker counts are centralised in `config.py`:

```python
EXTRACT_WORKERS     = 8    # Stage 05 parallel extraction threads
LANG_DETECT_WORKERS = 16   # Stage 06 language detection threads
```

---

## Data

### Reference data (in this repo)
- `data/flood_crawl.csv` — 227 flood events (country, dates, languages, location)
- `config/keyword_lexicon.json` — flood keyword lexicon (English, Spanish, Portuguese, French)
- `config/source_domain_list.json` — curated news/gov/org domains per country (25 countries)
- `config/domain_hints.json` — national/international scope flags for hit cap tuning

### Pipeline data (NOT in this repo — too large for git)
- `cache/{flood_id}/*.warc.gz` — raw WARC slices (GBs per event)
- `output/*.parquet` — stage outputs
- `raw_index_responses/` — raw CC CDX API responses

### Progress manifests (in this repo)
- `progress/flood_*.json` — per-event stage completion status

After completing stages, update and push progress:

```bash
python generate_progress.py
git add progress/
git commit -m "chore: update pipeline progress"
git push
```

---

## Blocked crawls

Some CC crawls return HTTP 403 (not yet public on S3). Add their IDs to `config.py`:

```python
BLOCKED_CRAWLS = []   # e.g. ["CC-MAIN-2026-04"] — remove once accessible
```

---

## Key design decisions

- **Trafilatura** over BeautifulSoup — ML-based extraction prevents sidebar/ticker contamination
- **Variant C queries** (flood keywords AND location) are the primary retrieval strategy — highest precision
- **Relevance threshold** `flood_hits >= 2` required to pass Stage 06
- **Word-boundary regex** prevents substring false positives (e.g. "alud" inside "resultado")
- **Download-then-filter**: non-Latin URLs use numeric slugs that can't be screened without fetching
- **Domain hit caps**: 500 for national/international outlets, 3000 for local — prevents high-volume
  sources from exhausting the pointer budget for county-level events

---

## Project structure

```
flood-pipeline/
├── data/
│   └── flood_crawl.csv                # 227 Americas flood events (2020-2025)
├── config/
│   ├── keyword_lexicon.json           # Flood keywords (4 languages)
│   ├── source_domain_list.json        # News/gov/org domains (25 countries)
│   └── domain_hints.json             # Scope flags for hit cap tuning
├── progress/                          # Stage completion manifests (git-tracked)
│   └── flood_*.json
├── NLP-model/                         # NLP submodule (git submodule)
├── cache/                             # WARC slices (gitignored)
├── output/                            # Parquet outputs (gitignored)
├── raw_index_responses/               # CC CDX responses (gitignored)
├── logs/                              # Run logs (gitignored)
├── stage_00_preflight.py
├── stage_01_query_specs.py
├── stage_02_query_cc_index.py
├── stage_03_validate_pointers.py
├── stage_04_download_warc.py          # asyncio downloader (preferred)
├── stage_04b_download_warc.py         # threading downloader (alternative)
├── stage_05_extract_text.py
├── stage_06v_clean_deduplicate.py
├── stage_07_url_report.py
├── stage_08_nlp_analysis.py
├── config.py                          # Central configuration
├── collinfo.json                      # Cached CC crawl listing
└── requirements.txt
```

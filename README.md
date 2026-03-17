# Flood Data Pipeline

A Python pipeline for extracting news and government articles about flood events
from [Common Crawl](https://commoncrawl.org/)'s web archive. Covers 150 flood
events across 6 regions.

## Overview

The pipeline follows a sequential stage architecture. Each stage reads from the
previous stage's Parquet output and writes its own.

```
Stage 00  Preflight         Crawl lag check, crawl window assignment
Stage 01  Query Specs       Build keyword + domain queries per event
Stage 02  CC Index          Query Common Crawl index → raw pointer list
Stage 03  Validate          Filter, size-check, deduplicate pointers
Stage 04  WARC Download     Download WARC slices from CC S3
Stage 05  HTML Extraction   Extract article body text (trafilatura)
Stage 06  Clean & Filter    Clean text, detect language, deduplicate, score relevance
```

## Setup

### 1. Clone and run setup

```bash
git clone https://github.com/ShiKen08/Flood-News-Data-Pipeline
cd flood-pipeline
python setup.py
```

This creates the required local directories and copies `config.py.template` → `config.py`.

### 2. Configure paths

Open `config.py` and set your local paths:

```python
# Where WARC slices are cached locally (needs several GB of free space)
CACHE_DIR = Path("/your/local/path/cache")

# Where pipeline outputs (Parquet files) are written
OUTPUT_DIR = Path("/your/local/path/output")
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Check what's already done

Before running anything, check the shared progress manifests:

```bash
cat progress/summary.json
```

Or browse `progress/flood_*.json` for individual events. This tells you
which stages are complete so you don't re-download files unnecessarily.

---

## Running the pipeline

Each stage has its own script. Run them in order:

```bash
# Pilot mode — 7 hand-picked events, 200000 pointers each 
python stage_00_preflight.py
python stage_01_query_specs.py
python stage_02_cc_index.py
python stage_03_validate_pointers.py
python stage_04_download_warc.py
python stage_05_extract_html.py
python stage_06_clean_filter.py

# Full pilot — all pointers for the 7 pilot events
python stage_04_download_warc.py --full

# Single event debug
python stage_04_download_warc.py --flood-id 12

# Phase 2 — all 150 events
python stage_04_download_warc.py --all
```

The pilot flood IDs are defined in `config.py`:
```python
PILOT_FLOOD_IDS = [1, 2, 3, 9, 12, 19, 34]
```

---

## Sharing progress with teammates

After completing any stage, regenerate the progress manifests and push:

```bash
python generate_progress.py
git add progress/
git commit -m "chore: update pipeline progress"
git push
```

Teammates pull before starting work:

```bash
git pull
cat progress/summary.json
```

---

## Data

### Reference data (in this repo)
- `data/flood_crawl.csv` — 150 flood events with metadata (country, dates, languages, location)

### Pipeline data (NOT in this repo — too large for git)
- `cache/{flood_id}/*.warc.gz` — raw WARC slices (can be GBs per event)
- `output/*.parquet` — stage outputs (validated pointers, fetch logs, extracted text, etc.)

Large files are stored separately. Ask the team for access to shared storage.

---

## Blocked crawls

Some Common Crawl crawls are indexed but not yet publicly accessible on S3
(return HTTP 403). These are listed in `config.py`:

```python
BLOCKED_CRAWLS = [
    "CC-MAIN-2026-04",
    "CC-MAIN-2026-08",
]
```

Events that fall only within these crawl windows (Syria #1, Indonesia #2, Colombia #3)
cannot be processed until CC makes them public. Remove from `BLOCKED_CRAWLS` once accessible.

---

## Key design decisions

- **Trafilatura** is used over BeautifulSoup for article body extraction — its ML-based
  isolation prevents news ticker / sidebar contamination that causes false positive keyword matches
- **Relevance threshold**: `flood_hits >= 2` keyword matches required to pass Stage 06
- **Word boundary regex** prevents substring false positives (e.g. "alud" inside "resultado")
- **Download-then-filter** architecture: URL pre-filtering is skipped because non-Latin URLs
  use numeric slugs that can't be screened without fetching

---

## Project structure

```
flood-pipeline/
├── data/
│   └── flood_crawl.csv          # 150 flood events reference data
├── progress/                    # Shared progress manifests (committed to git)
│   ├── summary.json
│   └── flood_*.json
├── cache/                       # WARC slices (gitignored — large)
├── output/                      # Parquet outputs (gitignored — large)
├── logs/                        # Log files (gitignored)
├── raw_index_responses/         # CC index responses (gitignored)
├── stage_00_preflight.py
├── stage_01_query_specs.py
├── stage_02_cc_index.py
├── stage_03_validate_pointers.py
├── stage_04_download_warc.py
├── stage_05_extract_html.py
├── stage_06_clean_filter.py
├── generate_progress.py         # Regenerate progress/ manifests
├── setup.py                     # One-time setup for new teammates
├── config.py.template           # Config template (copy → config.py)
├── config.py                    # Your local config (gitignored)
└── requirements.txt
```

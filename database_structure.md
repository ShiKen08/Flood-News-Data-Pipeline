# Database Structure — Flood Communication Pipeline

**Project:** The CSSci Natural Disasters — Flood Communication Actionability Analysis  
**Research Question:** To what extent does flood-related web content emphasize hazard description over actionable behavioral advice across English and Spanish/Portuguese, located in North and South America?

---

## Overview

The pipeline transforms two raw inputs — a curated flood event registry and a web archive — into a structured analytical dataset of verified flood news articles. Data flows through nine sequential stages, each producing an intermediate file. The primary key linking all datasets is `flood_id`.

```
EM-DAT Flood Registry (269 events)
        │
        ▼
Stage 01 → event_query_specs      (search parameters per flood)
        │
        ▼
Stage 02 → cc_index_hits          (Common Crawl URL candidates)
        │
        ▼
Stage 03 → validated_pointers     (deduplicated, valid WARC pointers)
        │
        ▼
Stage 04 → WARC downloads         (raw HTML archives)
        │
        ▼
Stage 05 → extracted_text         (plain text from HTML)
        │
        ▼
Stage 06 → clean_text             (deduplicated, keyword-filtered articles)
        │
        ▼
Stage 09 → model_event_articles   (ML-classified articles)
        │
        ▼
     verified_articles_clean.csv  (final analytical dataset — 388 articles)
```

---

## 1. Primary Input: EM-DAT Flood Registry

**Source:** EM-DAT Public Natural Disaster Database (`public.emdat.be`)  
**Scope for this project:** 269 flood events in the Americas (North, Central, South America + Caribbean), 2020–2025  

| Field | Type | Description |
|-------|------|-------------|
| `Flood_ID` | Integer | Internal project ID (1–269), primary key |
| `ISO` | String (3) | ISO 3166-1 alpha-3 country code (e.g. `BRA`, `USA`) |
| `Country` | String | Full country name |
| `Subregion` | String | UN subregion (e.g. `Latin America and the Caribbean`, `Northern America`) |
| `Region` | String | UN region |
| `Location` | String | Affected provinces, departments, or states |
| `River Basin` | String | River basin name (where applicable) |
| `Start Date` | Date | Flood onset date |
| `End Date` | Date | Flood end date |
| `Duration` | Integer | Event duration in days |
| `Language_ISO_639_3` | String | Primary language(s) of the country |
| `Local_Languages` | String | Additional local languages |

**Distribution:** 38 of 269 floods (14%) yielded verified articles; 231 floods had no coverage in the pipeline (see Missingness section).

---

## 2. Common Crawl (CC) — Web Archive Source

**Source:** Common Crawl (`commoncrawl.org`) — monthly snapshots of the public web in WARC format  
**Crawls used:** CC-MAIN-2020 through CC-MAIN-2026, aligned to each flood event's date window  

Data types extracted per page:
- Unstructured web text (WARC/WET format → parsed with `trafilatura`)
- Publication date (extracted from HTML metadata or Open Graph tags)
- URL and domain
- HTTP status and MIME type
- WARC file pointer (filename, byte offset, length) for deterministic retrieval

---

## 3. Intermediate Pipeline Files

### Stage 01 — `event_query_specs.parquet`

One row per (flood × crawl) query. Defines what keywords and domains to search in the CC index for each event.

| Field | Type | Description |
|-------|------|-------------|
| `query_id` | String | Composite key: `{flood_id}_{suffix}` |
| `flood_id` | Integer | FK → EM-DAT flood registry |
| `crawl_id` | String | CC crawl identifier (e.g. `CC-MAIN-2021-49`) |
| `query_text` | String | Boolean keyword query (flood terms in EN/ES/PT/FR) |
| `query_language_codes` | JSON list | Languages to accept (e.g. `["eng","spa","por"]`) |
| `domain_filter` | String | `restricted` (curated domains) or `open` (all domains) |
| `domain_list` | JSON | Country-specific news and government domains |
| `window_start` | Datetime | Start of publication window (flood start − 7 days) |
| `window_end` | Datetime | End of publication window (flood end + 30 days) |
| `retrieval_strategy` | String | `primary_restricted` or `open_web_fallback` |
| `created_at` | Datetime | Timestamp when spec was generated |

---

### Stage 02 — `cc_index_hits.parquet`

All URL candidates returned by querying the CC CDX (index) API. One row per URL hit.

| Field | Type | Description |
|-------|------|-------------|
| `hit_id` | UUID | Unique row identifier |
| `flood_id` | Integer | FK → EM-DAT flood registry |
| `query_id` | String | FK → `event_query_specs` |
| `crawl_id` | String | CC crawl identifier |
| `url` | String | Full URL of the candidate page |
| `timestamp` | String | CC capture timestamp (`YYYYMMDDHHmmss`) |
| `status` | String | HTTP status at crawl time (e.g. `200`) |
| `mime` | String | MIME type (e.g. `text/html`) |
| `filename` | String | WARC file path within CC S3 bucket |
| `offset` | Integer | Byte offset of this page within the WARC file |
| `length` | Integer | Byte length of the WARC record |
| `digest` | String | SHA-1 content digest (for deduplication) |
| `domain` | String | Registered domain (e.g. `bbc.com`) |
| `retrieval_strategy` | String | Strategy that found this hit |
| `fetched_at` | Datetime | When the CDX query was made |

**Volume:** 455,431 candidate URLs across 11 pilot floods.

---

### Stage 03 — `validated_pointers.parquet`

Filtered and deduplicated CC hits that are valid for WARC download. Removes duplicates, oversized files, and cross-event contamination.

| Field | Type | Description |
|-------|------|-------------|
| `pointer_id` | UUID | Unique row identifier |
| `flood_id` | Integer | FK → EM-DAT flood registry |
| `query_id` | String | FK → `event_query_specs` |
| `crawl_id` | String | CC crawl identifier |
| `url` | String | Canonical URL |
| `filename` | String | WARC file path |
| `offset` | Integer | Byte offset |
| `length` | Integer | Byte length |
| `digest` | String | SHA-1 content digest |
| `timestamp` | String | CC capture timestamp |
| `retrieval_strategy` | String | How this URL was found |
| `retrieval_rank` | Integer | Rank within the query result set |
| `is_pointer_duplicate` | Boolean | True if this WARC record appears under multiple flood queries |
| `cross_event_shared` | Boolean | True if URL appears in >1 flood event |
| `size_filter_status` | String | `VALID`, `TOO_LARGE`, `TOO_SMALL` |
| `status` | String | Overall validity status |
| `reject_reason` | String | Reason for rejection (empty if valid) |
| `is_url_duplicate` | Boolean | True if URL already seen for this flood |

**Volume:** 447,325 validated pointers (of 455,431 hits — 1.8% dropped).

---

### Stage 05–06 — `clean_text_cluster.parquet`

Extracted, cleaned, and keyword-filtered article text. One row per unique article that survived deduplication and soft-relevance filtering. This is the main working corpus.

| Field | Type | Description |
|-------|------|-------------|
| `doc_id` | UUID | Unique document identifier |
| `pointer_id` | UUID | FK → `validated_pointers` |
| `flood_id` | Integer | FK → EM-DAT flood registry |
| `url` | String | Source URL |
| `page_title` | String | HTML `<title>` or `<h1>` |
| `meta_description` | String | HTML meta description tag |
| `pub_date` | Date | Article publication date (see `pub_date_source`) |
| `pub_date_source` | String | `meta` (from HTML metadata) or `capture_ts` (CC crawl date fallback) |
| `pub_in_window` | Boolean | True if pub_date falls within the flood event window |
| `raw_text` | String | Full extracted text (before cleaning) |
| `clean_text` | String | Cleaned text (encoding fixed, noise stripped) |
| `extraction_method` | String | `trafilatura` or `bs4_fallback` |
| `extraction_success` | Boolean | Whether text extraction succeeded |
| `char_count` | Integer | Character count of clean_text |
| `word_count` | Integer | Word count of clean_text |
| `non_ascii_ratio` | Float | Proportion of non-ASCII characters (noise indicator) |
| `language_detected` | String | ISO 639-3 language code (e.g. `por`, `spa`, `eng`) |
| `language_confidence` | Float | Language detection confidence score |
| `language_match` | Boolean | True if detected language matches expected flood country language |
| `flood_term_hits` | Integer | Count of flood keyword matches in text |
| `impact_term_hits` | Integer | Count of impact/severity keyword matches |
| `location_term_hits` | Integer | Count of location keyword matches |
| `subnational_hits` | Integer | Count of subnational location matches |
| `location_specificity_score` | Float | Composite score of geographic specificity |
| `is_relevant` | Boolean | Passes hard keyword threshold (flood + location terms) |
| `is_soft_relevant` | Boolean | Passes relaxed keyword threshold |
| `is_event_article` | Boolean | Keyword-level judgment: genuine event article |
| `flood_mentioned` | Boolean | Any flood term present in text |
| `low_specificity` | Boolean | Too geographically vague for event-level analysis |
| `is_content_duplicate` | Boolean | Near-duplicate of another article in corpus |
| `duplicate_group_id` | UUID | Groups near-duplicate articles together |
| `cross_event_shared` | Boolean | Article appears under multiple flood IDs |
| `text_hash` | String | MD5 hash of clean_text (for exact dedup) |
| `model_flood_prob` | Float32 | SetFit ML model probability this is a flood event article |
| `model_is_event_article` | Boolean | ML model binary prediction (threshold 0.5) |

**Volume:** 12,175 articles across 51 flood events.

**Rejection reasons** for articles that did not reach this file (tracked in `rejects.parquet`):

| Reason | Count | % |
|--------|-------|---|
| `no_location_match` | 182,113 | 55.9% |
| `no_flood_term_match` | 76,308 | 23.4% |
| `tag_or_index_page` | 32,238 | 9.9% |
| `char_count_too_short` | 22,121 | 6.8% |
| `language_mismatch` | 6,193 | 1.9% |
| `non_ascii_ratio_too_high` | 5,658 | 1.7% |
| `error_page_title` | 596 | 0.2% |
| `extraction_failed` | 322 | 0.1% |

---

### Stage 09 — `model_event_articles_multi.csv`

All articles scored by the ML classifier (SetFit fine-tuned on `paraphrase-multilingual-mpnet-base-v2`), including those that did not pass the post-filter. One row per article.

| Field | Type | Description |
|-------|------|-------------|
| `doc_num` | Integer | Sequential row number |
| `flood_id` | Integer | FK → EM-DAT flood registry |
| `country` | String | Country of the flood event |
| `url` | String | Source URL |
| `domain` | String | Registered domain |
| `page_title` | String | Article title |
| `pub_date` | Date | Publication date |
| `pub_in_window` | Boolean | Published within the flood event window |
| `language_detected` | String | ISO 639-3 language code |
| `model_flood_prob` | Float | ML model probability (0–1) of being a flood event article |
| `model_is_event_article` | Boolean | ML binary prediction |
| `is_event_article` | Boolean | Keyword-rule binary judgment |
| `flood_term_hits` | Integer | Flood keyword count |
| `impact_term_hits` | Integer | Impact keyword count |
| `location_term_hits` | Integer | Location keyword count |
| `word_count` | Integer | Article word count |
| `clean_text` | String | Cleaned article text |
| `post_filter_pass` | Boolean | Passed all 14 post-filter rules |
| `post_filter_reason` | String | Rejection reason if `post_filter_pass = False` |

**Post-filter rules** (14 categories) include: `strict_domain_no_flood_title`, `fire_not_flood`, `cemaden_forecast`, `weather_forecast`, `zero_keyword_hits`, `general_climate_policy`, and others.

---

## 4. Final Output — `verified_articles_clean.csv`

The analytical dataset shared with the research group. Contains only verified flood event articles that passed both the ML classifier and all post-filter rules. Manually double-checked by the team.

**388 articles | 25 flood events | Languages: English, Spanish, Portuguese**

| Field | Type | Description |
|-------|------|-------------|
| `article_id` | Integer | Sequential article identifier (1–388) |
| `flood_id` | Integer | FK → EM-DAT flood registry |
| `iso` | String (3) | ISO country code of the flood event |
| `country` | String | Country of the flood event |
| `location` | String | Affected regions/departments (from EM-DAT) |
| `river_basin` | String | River basin (null where not applicable) |
| `start_date` | Date | Flood event start date |
| `end_date` | Date | Flood event end date |
| `language_detected` | String | ISO 639-3 language of the article (`eng`, `spa`, `por`) |
| `url` | String | Source URL |
| `page_title` | String | Article headline / page title |
| `pub_date` | Date | Publication date (from HTML metadata; null for 59 articles where only crawl date was available) |
| `clean_text` | String | Cleaned article body text |

**Coverage breakdown:**

| Language | Articles | Share |
|----------|----------|-------|
| Spanish (spa) | ~220 | ~57% |
| Portuguese (por) | ~110 | ~28% |
| English (eng) | ~58 | ~15% |

---

## 5. Missingness: Why 231 Floods Have No Articles

| Dropout stage | Floods | Explanation |
|---------------|--------|-------------|
| No CC extraction (never crawled) | 218 | Flood not included in a batch run, or CC index returned 0 hits for those event/date/keyword combinations |
| Extracted but model rejected all | 13 | Articles scored high by ML (mean prob 0.955) but rejected by post-filter `strict_domain_no_flood_title` (mainly texastribune.org, eltiempo.com archive pages) |
| **Passed — in final dataset** | **38** | **14% of all 269 floods** |

The `output/missingness_analysis.csv` file records the dropout stage for all 269 floods.

---

## 6. Key Relationships Between Files

```
verified_floods.csv (269 rows)
    │ flood_id (PK)
    ├──► event_query_specs.parquet       (flood_id FK)
    ├──► cc_index_hits.parquet           (flood_id FK)
    │         │ hit_id
    │         ▼
    │    validated_pointers.parquet      (flood_id FK, pointer_id PK)
    │         │ pointer_id
    │         ▼
    │    clean_text_cluster.parquet      (flood_id FK, doc_id PK)
    │         │ url
    │         ▼
    │    model_event_articles_multi.csv  (flood_id FK)
    │         │ url
    │         ▼
    └──► verified_articles_clean.csv     (flood_id FK, article_id PK)
```

The `url` column serves as a secondary join key between `clean_text_cluster.parquet`, `model_event_articles_multi.csv`, and `verified_articles_clean.csv`.

---

## 7. ML Classifier

**Model:** SetFit fine-tuned on `paraphrase-multilingual-mpnet-base-v2` (HuggingFace)  
**Training data:** 1,904 manually and Claude-labeled examples across 4 annotation batches  
- Batch 1–3: hand-labeled by team members  
- Batch 4: 430 examples (Claude-labeled), covering flood IDs 228–268  

**Label distribution:** 822 positive (flood event articles) / 1,082 negative  
**Architecture:** Sentence-transformer backbone + logistic regression classification head  
**Languages:** Multilingual (EN, ES, PT, FR) — single model, no language-specific variants  
**Output:** `model_flood_prob` (0–1 continuous score) + `model_is_event_article` (boolean at threshold 0.5)

---

*Generated: 2026-05-22 | Pipeline repository: flood-pipeline (main branch)*

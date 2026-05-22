# Database Structure — Flood Communication Pipeline

**Project:** The CSSci Natural Disasters — Flood Communication Actionability Analysis  
**Research Question:** To what extent does flood-related web content emphasize hazard description over actionable behavioral advice across English and Spanish/Portuguese, located in North and South America?

---

## Overview

The full pipeline has two phases. **Phase 1 (flood-pipeline)** extracts and verifies flood news articles from Common Crawl through nine sequential stages. **Phase 2 (NLP-model)** takes the verified articles and applies NLP analysis to score each article for actionability, source authority, news framing, and semantic clustering. The primary key linking all datasets across both phases is `flood_id` / `article_id`.

```
EM-DAT Flood Registry (269 events)
        │
        ▼
Stage 00 → crawl_coverage, language_assignments, location_dictionary
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
Stage 04 → WARC downloads         (raw HTML archives, cached locally)
        │
        ▼
Stage 05 → extracted_text         (plain text from HTML via trafilatura)
        │
        ▼
Stage 06 → clean_text             (deduplicated, keyword-filtered articles)
        │
        ▼
Stage 09 → model_event_articles   (SetFit ML classifier + post-filter)
        │
        ▼
  verified_articles_clean.csv     (388 verified articles — pipeline output)
        │
        ▼  ◄─── NLP-model submodule begins here
        │
  NLP: preprocessing              (sentence splitting, language normalisation)
        │
        ▼
  NLP: actionability              (imperative scoring, short/long-term, SRL, tense)
        │
        ▼
  NLP: authority                  (source scope, credibility tier, Global N/S)
        │
        ▼
  NLP: framing                    (Entman 1993 — impact/response/accountability/recovery)
        │
        ▼
  NLP: clustering                 (LaBSE embeddings, UMAP, BERTopic topic modelling)
        │
        ▼
  enriched.csv                    (final analytical dataset with all NLP scores)
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

**Distribution:** 25 of 269 floods (9%) yielded verified articles in the latest model run; 244 floods have no articles (see Missingness section).

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

## 5. Missingness: Why 244 Floods Have No Articles

| Dropout stage | Floods | Explanation |
|---------------|--------|-------------|
| 0 CC hits (queried, nothing returned) | 192 | All 269 floods were queried. CC index returned no matching URLs for these events despite CC having crawled the time window |
| Got hits but failed extraction | 26 | URLs found but WARC download / text extraction produced nothing usable |
| Extracted but model/filter rejected all | 26 | Clean text produced but ML classifier or post-filter rejected every article |
| **Passed — in final dataset** | **25** | **9% of all 269 floods** |

The `output/missingness_analysis.csv` file records the dropout stage for all 269 floods. See `missingness_report.md` for the full statistical analysis (MCAR/MAR/MNAR tests).

---

## 6. Key Relationships Between Files

```
flood_crawl.csv / verified_floods.csv   (269 flood events — master registry)
    │ flood_id (PK)
    ├──► event_query_specs.parquet       (flood_id FK — one row per flood×crawl query)
    ├──► cc_index_hits.parquet           (flood_id FK — CC URL candidates)
    │         │
    │         ▼
    │    validated_pointers.parquet      (flood_id FK, pointer_id PK)
    │         │
    │         ▼
    │    clean_text_cluster.parquet      (flood_id FK, doc_id PK — 12,175 articles)
    │         │ url
    │         ▼
    │    model_event_articles_multi.csv  (flood_id FK — all ML-scored articles)
    │         │ url / article_id
    │         ▼
    └──► verified_articles_clean.csv     (flood_id FK, article_id PK — 388 articles)
                │
                │  ◄── NLP-model takes this as input
                │ article_id / flood_id
                ▼
         NLP-model/output/enriched.csv   (article_id PK — full NLP scores)
```

Join keys:
- `flood_id` links EM-DAT events across all pipeline files
- `url` links `clean_text_cluster` → `model_event_articles_multi` → `verified_articles_clean`
- `article_id` links `verified_articles_clean` → `enriched.csv`

---

## 7. ML Classifier (Stage 09)

**Model:** SetFit fine-tuned on `paraphrase-multilingual-mpnet-base-v2` (HuggingFace)  
**Training data:** 1,904 manually and Claude-labeled examples across 4 annotation batches
- Batch 1–3: hand-labeled by team members
- Batch 4: 430 examples (Claude-labeled), covering flood IDs 228–268

**Label distribution:** 822 positive (flood event articles) / 1,082 negative  
**Architecture:** Sentence-transformer backbone + logistic regression classification head  
**Languages:** Multilingual (EN, ES, PT, FR) — single model, no language-specific variants  
**Output:** `model_flood_prob` (0–1 continuous score) + `model_is_event_article` (boolean at threshold 0.5)

---

## 8. NLP Analysis Phase (NLP-model submodule)

**Repository:** `github.com/Lenap0911/NLP-model`  
**Input:** `verified_articles_clean.csv` (388 articles, 25 floods)  
**Output:** `NLP-model/output/enriched.csv` (same 388 rows + all NLP score columns)  
**Entry point:** `python run_nlp_pipeline.py --input data/verified_articles_clean.csv`

The NLP pipeline runs four analytical modules in sequence:

### 8a. Preprocessing (`nlp/preprocessing.py`)
Prepares text for downstream analysis.
- Sentence splitting using spaCy sentencizer
- Language normalisation (ISO 639-3 → spaCy model mapping)
- Produces article-level and sentence-level dataframes

### 8b. Actionability (`nlp/actionability.py`)
Scores each article for actionability across multiple dimensions, grounded in Mostafiz et al. (2022), Zade et al. (2018), and Jurafsky (2014) semantic role labelling.

| Output column | Type | Description |
|--------------|------|-------------|
| `imperative_score` | Float | Count of imperative sentences (direct instructions) |
| `imperative_count` | Integer | Raw imperative sentence count |
| `short_term_score` | Float | Short-term actionability — immediate survival actions (evacuate, prepare) |
| `short_term_count` | Integer | Raw short-term keyword count |
| `long_term_score` | Float | Long-term actionability — recovery/resilience actions (rebuild, insure) |
| `long_term_count` | Integer | Raw long-term keyword count |
| `spatial_score` | Float | Geographic specificity of actionable content (location mentions in action sentences) |
| `spatial_count` | Integer | Raw spatial reference count |
| `actionability_score` | Float | Composite actionability score (weighted combination of above) |
| `has_agent` | Boolean | SRL: article contains an identifiable agent (WHO) |
| `has_action` | Boolean | SRL: article contains a concrete action (WHAT) |
| `has_location` | Boolean | SRL: article contains a location reference (WHERE) |
| `srl_complete` | Boolean | All three SRL dimensions present (agent + action + location) |
| `top_locations` | String | Named location entities found in actionable sentences |
| `top_orgs` | String | Named organisation entities |
| `past_tense` | Integer | Count of past-tense sentences |
| `present_tense` | Integer | Count of present-tense sentences |
| `future_tense` | Integer | Count of future-tense sentences |
| `past_tense_ratio` | Float | Proportion of past-tense sentences (high = descriptive/reporting bias) |

### 8c. Authority (`nlp/authority.py`)
Classifies the source authority of each article based on its domain, grounded in Gordon (2000) and Khawaja et al. (2025) Global North/South media framing.

| Output column | Type | Description |
|--------------|------|-------------|
| `scope` | String | `government` \| `national` \| `regional` \| `local` \| `ngo` |
| `scope_score` | Float | Numeric authority weight per scope tier |
| `credibility_tier` | String | Credibility classification of the source domain |
| `authority_score` | Float | Composite source authority score |
| `global_region` | String | `Global North` \| `Global South` (based on country, from config) |

### 8d. Framing (`nlp/framing.py`)
Detects dominant news frame per article using Entman's (1993) four-frame model applied to multilingual keyword lexicons (EN/ES/PT).

| Output column | Type | Description |
|--------------|------|-------------|
| `frame_impact_score` | Float | Emphasis on casualties, damage, displacement |
| `frame_response_score` | Float | Emphasis on rescue, evacuation, aid delivery |
| `frame_accountability_score` | Float | Emphasis on government failure, policy, warnings missed |
| `frame_recovery_score` | Float | Emphasis on reconstruction, resilience, long-term planning |
| `dominant_frame` | String | Highest-scoring frame for the article |
| `frame_diversity` | Float | Entropy across all four frame scores (high = multi-frame article) |

### 8e. Clustering (`nlp/clustering.py`)
Generates multilingual semantic embeddings and clusters articles by topic using BERTopic.

| Output column | Type | Description |
|--------------|------|-------------|
| `embed_text` | String | Combined text used for embedding (title + first sentences) |
| `umap_cluster` | Integer | UMAP 2D cluster assignment |
| `topic_id` | Integer | BERTopic topic label (−1 = outlier) |

**Embedding model:** LaBSE (Language-Agnostic BERT Sentence Embeddings) — produces language-neutral 768-dim vectors, enabling cross-lingual comparison of EN/ES/PT articles  
**Cached at:** `NLP-model/output/labse_embeddings_cache.npz`

### Final output — `NLP-model/output/enriched.csv`

One row per article (388 rows). All columns from `verified_articles_clean.csv` plus every NLP score column listed above. This is the dataset used for the final quantitative analysis comparing actionability across languages, source types, and Global North/South contexts.

---

## 9. Complementary Data Collection (`NLP-model/complementary/`)

A secondary, targeted web scraping pipeline that supplements Common Crawl with direct RSS polling and Wayback Machine archive scraping. Designed to fill coverage gaps for floods with 0 CC hits.

**Components:**
- `scraper/rss_poller.py` — polls RSS feeds from approved regional outlets (e.g. G1 Brazil, Agencia EFE, El Tiempo Colombia)
- `scraper/archive_scraper.py` — retrieves articles via Wayback Machine CDX API
- `scraper/news_scraper.py` — direct HTTP scraping with trafilatura extraction
- `config/outlets.json` — curated list of approved news domains per country

**Output:** `complementary/output/` — supplementary articles in the same schema as `verified_articles_clean.csv`, ready to be merged and run through the NLP pipeline.

---

*Generated: 2026-05-22 | Pipeline: flood-pipeline (main) + NLP-model submodule (Lenap0911/NLP-model)*

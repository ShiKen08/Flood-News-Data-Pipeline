# Missingness Analysis Report
## Flood Communication Pipeline — Americas 2020–2025

**Dataset:** `verified_articles_clean.csv` (latest, post-retrained model)  
**Date:** 2026-05-22  

---

## 1. Overview

The pipeline was designed to retrieve and classify flood-related web articles from Common Crawl for 269 flood events across the Americas between 2020 and 2025. The final verified dataset contains **388 articles across 25 floods** — meaning 244 of 269 floods (91%) have no articles in the final output.

This report diagnoses *where* and *why* those floods are absent, and tests whether the missingness is random (MCAR), systematically predictable (MAR), or tied to unobserved severity characteristics of the floods themselves (MNAR).

---

## 2. Pipeline Funnel — Where Floods Drop Out

Using intermediate parquet files pulled from the cluster (`event_query_specs.parquet`, `hit_count_summary.parquet`, `clean_text_cluster.parquet`), we traced every flood through each pipeline stage:

| Stage | Floods | % of total | Explanation |
|-------|--------|-----------|-------------|
| Query specs generated | 269 | 100% | All floods were sent to Common Crawl CDX API |
| **Got 0 CC hits** | **192** | **71%** | CC index returned no matching URLs for these events |
| Got CC hits but failed extraction | 26 | 10% | URLs found but WARC download/text extraction failed or all text filtered |
| Extracted but model rejected all | 26 | 10% | Clean text produced but ML classifier + post-filter rejected all articles |
| **Passed — in final dataset** | **25** | **9%** | Verified flood event articles |

### Key finding: all 269 floods were queried

A critical clarification: the `event_query_specs.parquet` confirms that **all 269 floods had query specs generated and were sent to Common Crawl**. The 218 floods previously labelled "no CC extraction" were not pipeline failures — they were genuinely searched. The dominant dropout is at the CC index stage (192 floods, 71%), not a coverage gap in the pipeline itself.

### CC crawl coverage for zero-hit floods

For the 192 floods that returned 0 CC hits, the `crawl_coverage.parquet` shows that Common Crawl *did* crawl those time windows:

| Coverage status | Floods |
|----------------|--------|
| COVERED (full crawl overlap) | 91 |
| PARTIAL (partial overlap) | 86 |
| NEAREST (closest available crawl used) | 15 |

This means the CC index was searched for these events, but no pages matching the keyword + domain filter combination were archived. This is a genuine **web coverage gap** — these flood events were either not reported on domains that CC crawled, or the reports were not captured in the relevant crawl snapshot.

---

## 3. Test 1 — Chi-Square Tests (MCAR)

**Question:** Is missingness independent of observable flood characteristics?  
**Framework:** If all p-values > 0.05, we cannot reject MCAR on those dimensions.

| Variable | χ² | df | p-value | Result |
|----------|----|----|---------|--------|
| Subregion | 1.25 | 1 | 0.2634 | Not significant |
| Language group | 0.84 | 2 | 0.6574 | Not significant |
| Country (top 8 + Other) | 7.78 | 8 | 0.4548 | Not significant |

**Interpretation:** There is no statistically significant association between missingness and subregion, language, or country. Missing floods are distributed across Latin America and North America, and across English, Spanish, and Portuguese contexts, in proportions not significantly different from covered floods.

---

## 4. Test 2 — Logistic Regression (MAR)

**Question:** Can missingness be predicted from a combination of observable flood characteristics?  
**Predictors:** subregion, language group, year (centred on 2022), event duration

| Feature | Coefficient | Odds Ratio | Direction |
|---------|------------|------------|-----------|
| is_latin_america | −0.514 | 0.598 | Latin American floods slightly less likely to be covered |
| is_portuguese | +0.227 | 1.255 | Portuguese-language floods slightly more likely |
| is_spanish | +0.454 | 1.574 | Spanish-language floods slightly more likely |
| year_centered | −0.194 | 0.823 | More recent floods less likely (newer = less crawl history) |
| duration | −0.236 | 0.789 | Longer floods slightly less likely to be covered |

**Likelihood ratio test vs. null model:** χ² = 4.87, p = 0.4323 — **not significant**

**Interpretation:** The logistic model cannot predict missingness better than chance. The model accuracy (90.71%) equals the naive baseline of always predicting "missing." No combination of subregion, language, year, or duration explains which floods are covered. This rules out MAR driven by these observable variables.

---

## 5. Test 3 — Mann-Whitney U Tests (MNAR)

**Question:** Are missing floods systematically less severe than covered floods?  
**Rationale:** If missing floods are smaller/less newsworthy, the dataset would be biased toward high-impact events (MNAR). If the opposite is true, the missingness is structural rather than editorial.

| Severity variable | n covered | n missing | Median covered | Median missing | p-value | Result |
|------------------|-----------|-----------|---------------|----------------|---------|--------|
| Total Deaths | 13 | 141 | 3 | 8 | 0.0804 | Not significant |
| Total Affected | 16 | 201 | 3,000 | 3,704 | 0.6637 | Not significant |
| Total Damage ($000 USD) | 10 | 71 | 30,500 | 120,000 | 0.0969 | Not significant |

**Interpretation:** No severity variable reaches statistical significance. Notably, the direction of all three comparisons is *against* MNAR — covered floods have *lower* median deaths, affected people, and economic damage than missing floods. The dataset does not appear to over-represent large, high-impact events at the expense of smaller ones. If anything, some of the largest-damage floods are absent, which is consistent with structural CC coverage gaps rather than editorial selection bias.

---

## 6. Distribution of Covered Floods

### By subregion
| Subregion | Covered | Total | Coverage rate |
|-----------|---------|-------|--------------|
| Latin America & Caribbean | 18 | 237 | 7.6% |
| Northern America | 7 | 32 | 21.9% |

### By language
| Language | Covered floods | 
|----------|---------------|
| Spanish | 15 |
| English / Other | 7 |
| Portuguese | 3 |

### By country (covered floods)
| Country | Covered floods |
|---------|---------------|
| United States | 6 |
| Colombia | 3 |
| Brazil | 3 |
| Mexico | 3 |
| Peru | 2 |
| Canada | 1 |
| Ecuador | 1 |
| Panama | 1 |

### Zero-hit floods by country (top 10)
| Country | Floods with 0 CC hits |
|---------|-----------------------|
| Brazil | 35 |
| Bolivia | 20 |
| Colombia | 18 |
| Peru | 17 |
| United States | 16 |
| Ecuador | 11 |
| Venezuela | 10 |
| Guatemala | 8 |
| Canada | 8 |
| Mexico | 7 |

---

## 7. Summary and Classification

| Test | Result | Implication |
|------|--------|-------------|
| Chi-square (subregion, language, country) | All p > 0.25 | Cannot reject MCAR on geographic/linguistic dimensions |
| Logistic regression (full model) | p = 0.43, not significant | Missingness not predictable from observed covariates |
| Mann-Whitney (severity) | All p > 0.08, not significant | No evidence of MNAR — missing floods not systematically smaller |
| Stage funnel | 192/269 (71%) return 0 CC hits | Dominant cause is web coverage gap, not pipeline failure |

### Classification: Structurally Driven, Consistent with MCAR

The missingness in this dataset cannot be classified as MAR or MNAR based on the available evidence. All statistical tests are non-significant. The dominant mechanism (71% of missing floods) is that Common Crawl returned zero matching results despite being queried — meaning news about those events was either not archived by CC, or was archived on domains outside the keyword + domain filter scope.

This is best described as **structurally driven missingness**: the probability of a flood being covered depends primarily on whether its reporting happened to be captured by Common Crawl at the right crawl window, with the keyword and domain filters in place — a quasi-random process not systematically tied to the flood's geography, language, or severity as measured by EM-DAT.

---

## 8. Limitations and Caveats

1. **Severity data coverage is low.** Only 13 covered floods and 141 missing floods have `Total Deaths` data in EM-DAT, limiting the power of the MNAR test. The Mann-Whitney tests are underpowered and near-significant results (deaths p=0.08, damage p=0.10) should not be dismissed entirely.

2. **Domain filter scope.** The pipeline uses a curated list of news and government domains per country. Floods that were only covered by local/community outlets outside this list would show 0 CC hits even if CC archived them. This is a keyword + domain filter gap, not a web availability gap.

3. **CC crawl timing.** Common Crawl produces monthly snapshots. If a flood occurred between crawl cycles, the reporting window may not overlap with any archived snapshot. The `NEAREST` coverage status (15 zero-hit floods) captures this case.

4. **25 covered floods is a small sample.** Logistic regression and Mann-Whitney tests are underpowered at this scale. Null results should be interpreted as "no evidence of bias" rather than "proof of MCAR."

5. **Model rejection at Stage 09.** 26 floods were extracted but rejected by the ML classifier or post-filter. These are not CC coverage failures — they represent floods where text was found but did not meet the verification threshold. This is a separate, model-level selection process.

---

*Generated from: `verified_articles_clean.csv`, `event_query_specs.parquet`, `hit_count_summary.parquet`, `crawl_coverage.parquet`, `clean_text_cluster.parquet`, `flood_crawl_og.csv`*  
*Pipeline repository: flood-pipeline (main branch)*

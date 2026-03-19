"""
Tests for stage_06 text cleaning functions:
  - clean_text
  - is_index_or_tag_page
  - score_relevance / _make_word_pattern / _term_matches
  - compute_metrics
  - check_usability
"""
import re
import sys
import types
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Import the module under test — config is already mocked via conftest.py
# Patch langid so tests run without the optional dependency
# ---------------------------------------------------------------------------
langid_mock = MagicMock()
langid_mock.classify.return_value = ("en", -50.0)
sys.modules["langid"] = langid_mock

import importlib
import stage_06_clean_deduplicate as s6


# =============================================================================
# clean_text
# =============================================================================

class TestCleanText:
    def test_empty_string_returns_empty(self):
        assert s6.clean_text("") == ""

    def test_none_returns_empty(self):
        assert s6.clean_text(None) == ""

    def test_collapses_multiple_spaces(self):
        result = s6.clean_text("hello   world")
        assert result == "hello world"

    def test_preserves_single_blank_line_between_paragraphs(self):
        text = "Para one.\n\nPara two."
        result = s6.clean_text(text)
        assert "\n\n" in result

    def test_collapses_three_or_more_blank_lines_to_two(self):
        text = "Para one.\n\n\n\nPara two."
        result = s6.clean_text(text)
        assert "\n\n\n" not in result
        assert "Para one." in result
        assert "Para two." in result

    def test_strips_boilerplate_subscribe_line(self):
        text = "Good article content.\nSubscribe to our newsletter\nMore content."
        result = s6.clean_text(text)
        assert "Subscribe" not in result
        assert "Good article content." in result

    def test_strips_boilerplate_sign_up(self):
        text = "Content here.\nSign up for updates\nMore."
        result = s6.clean_text(text)
        assert "Sign up" not in result

    def test_strips_boilerplate_advertisement(self):
        text = "Article text.\nAdvertisement\nMore text."
        result = s6.clean_text(text)
        assert "Advertisement" not in result

    def test_strips_boilerplate_accept_cookies(self):
        text = "Article body.\nAccept cookies to continue\nArticle continues."
        result = s6.clean_text(text)
        assert "Accept cookies" not in result

    def test_non_boilerplate_line_is_kept(self):
        text = "The river flooded the town.\nResidents were evacuated."
        result = s6.clean_text(text)
        assert "The river flooded the town." in result
        assert "Residents were evacuated." in result

    def test_strips_leading_and_trailing_whitespace(self):
        text = "   Clean article.   "
        assert s6.clean_text(text) == "Clean article."

    def test_boilerplate_case_insensitive(self):
        text = "Content.\nSUBSCRIBE NOW\nMore content."
        result = s6.clean_text(text)
        assert "SUBSCRIBE" not in result


# =============================================================================
# is_index_or_tag_page
# =============================================================================

class TestIsIndexOrTagPage:
    def test_article_url_returns_false(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/news/flood-hits-city-2024",
            "Flood hits city center"
        )
        assert is_tag is False
        assert reason == ""

    def test_homepage_url_returns_true(self):
        is_tag, reason = s6.is_index_or_tag_page("https://example.com/", "")
        assert is_tag is True
        assert reason == "homepage"

    def test_homepage_without_trailing_slash(self):
        is_tag, reason = s6.is_index_or_tag_page("https://example.com", "")
        assert is_tag is True

    def test_tag_url_pattern(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://detik.com/tag/banjir", "Banjir"
        )
        assert is_tag is True
        assert "tag/index" in reason

    def test_category_url_pattern(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/category/floods", "Floods"
        )
        assert is_tag is True

    def test_pagination_url_pattern(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/news/floods/page/3", "Floods Page 3"
        )
        assert is_tag is True

    def test_search_url_pattern(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/search?q=flood", "Search results"
        )
        assert is_tag is True

    def test_archive_url_pattern(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/2024/10/", "October 2024"
        )
        assert is_tag is True

    def test_pagination_title_pattern(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/news/floods",
            "Flood news — Page 2"
        )
        assert is_tag is True

    def test_tag_title_pattern(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/news",
            "Tag: banjir"
        )
        assert is_tag is True

    def test_none_url_does_not_crash(self):
        is_tag, reason = s6.is_index_or_tag_page(None, "Normal article title")
        assert isinstance(is_tag, bool)

    def test_none_title_does_not_crash(self):
        is_tag, reason = s6.is_index_or_tag_page(
            "https://example.com/article/flood-2024", None
        )
        assert is_tag is False


# =============================================================================
# _make_word_pattern / _term_matches
# =============================================================================

class TestWordBoundaryMatching:
    def test_ascii_term_matches_whole_word(self):
        assert s6._term_matches("flood", "the flood was severe") is True

    def test_ascii_term_does_not_match_substring(self):
        # "flood" should not match inside "flooding" when using word boundaries
        assert s6._term_matches("flood", "flooding continued") is False

    def test_ascii_term_case_insensitive(self):
        assert s6._term_matches("flood", "The FLOOD hit the city") is True

    def test_ascii_term_not_in_text(self):
        assert s6._term_matches("flood", "it rained heavily") is False

    def test_ascii_term_at_start_of_text(self):
        assert s6._term_matches("flood", "flood warning issued") is True

    def test_ascii_term_at_end_of_text(self):
        assert s6._term_matches("flood", "the town was hit by a flood") is True

    def test_term_with_punctuation(self):
        assert s6._term_matches("flood", "major flood, 10 dead") is True

    def test_non_ascii_term_matches(self):
        # Arabic word for flood: فيضان — should match when surrounded by spaces
        term = "فيضان"
        text = f"حدث {term} كبير"
        assert s6._term_matches(term, text) is True

    def test_empty_text_returns_false(self):
        assert s6._term_matches("flood", "") is False

    def test_pattern_cache_is_populated(self):
        # Clear cache, check it's populated after call
        s6._pattern_cache.clear()
        s6._term_matches("testterm123", "testterm123 appears here")
        assert "testterm123" in s6._pattern_cache


# =============================================================================
# score_relevance
# =============================================================================

class TestScoreRelevance:
    FLOOD_TERMS = ["flood", "inundation", "river burst"]
    LOC_TERMS = ["jakarta", "java"]

    def test_empty_text_returns_not_relevant(self):
        result = s6.score_relevance("", self.FLOOD_TERMS, self.LOC_TERMS)
        assert result["is_relevant"] is False
        assert result["flood_term_hits"] == 0
        assert result["location_term_hits"] == 0

    def test_none_text_returns_not_relevant(self):
        result = s6.score_relevance(None, self.FLOOD_TERMS, self.LOC_TERMS)
        assert result["is_relevant"] is False

    def test_relevant_document(self):
        text = "The flood and inundation in Jakarta caused widespread damage to Java island."
        result = s6.score_relevance(text, self.FLOOD_TERMS, self.LOC_TERMS)
        assert result["is_relevant"] is True
        assert result["flood_term_hits"] >= 2
        assert result["location_term_hits"] >= 1
        assert result["flood_mentioned"] is True

    def test_single_flood_hit_is_not_relevant(self):
        text = "The flood in Jakarta caused some damage."
        result = s6.score_relevance(text, self.FLOOD_TERMS, self.LOC_TERMS)
        assert result["is_relevant"] is False  # only 1 flood term hit
        assert result["flood_mentioned"] is True

    def test_no_location_hit_is_not_relevant(self):
        text = "The flood and inundation caused widespread damage across the region."
        result = s6.score_relevance(text, self.FLOOD_TERMS, self.LOC_TERMS)
        assert result["is_relevant"] is False  # no location hit
        assert result["flood_term_hits"] >= 2

    def test_no_location_terms_falls_back_to_flood_only(self):
        text = "The flood and inundation caused widespread damage."
        result = s6.score_relevance(text, self.FLOOD_TERMS, [])
        # With no location terms, only flood_hits >= 2 is required
        assert result["is_relevant"] is True

    def test_returns_all_expected_keys(self):
        result = s6.score_relevance("some text", self.FLOOD_TERMS, self.LOC_TERMS)
        expected_keys = {
            "is_relevant", "flood_mentioned", "flood_term_hits",
            "location_term_hits", "subnational_hits",
            "location_specificity_score", "low_specificity",
        }
        assert expected_keys == set(result.keys())

    def test_specificity_score_zero_when_no_loc_hits(self):
        text = "The flood and inundation in an unknown place."
        result = s6.score_relevance(text, self.FLOOD_TERMS, self.LOC_TERMS)
        assert result["location_specificity_score"] == 0.0

    def test_low_specificity_flag(self):
        # Only country-level match, no subnational
        flood_terms = ["flood", "inundation"]
        loc_terms = ["indonesia"]  # country only
        text = "The flood and inundation in Indonesia was devastating."
        loc_rows = pd.DataFrame([{
            "location_normalised": "indonesia",
            "level": "country",
            "aliases": "[]",
        }])
        result = s6.score_relevance(text, flood_terms, loc_terms, loc_df_rows=loc_rows)
        if result["location_term_hits"] > 0 and result["subnational_hits"] == 0:
            assert result["low_specificity"] is True


# =============================================================================
# compute_metrics
# =============================================================================

class TestComputeMetrics:
    def test_empty_string(self):
        char_count, word_count, ratio = s6.compute_metrics("")
        assert char_count == 0
        assert word_count == 0
        assert ratio == 0.0

    def test_none(self):
        char_count, word_count, ratio = s6.compute_metrics(None)
        assert char_count == 0

    def test_ascii_text(self):
        text = "hello world"
        char_count, word_count, ratio = s6.compute_metrics(text)
        assert char_count == 11
        assert word_count == 2
        assert ratio == 0.0

    def test_non_ascii_ratio(self):
        text = "héllo"  # 2 out of 5 chars are non-ascii (é is > 127)
        char_count, word_count, ratio = s6.compute_metrics(text)
        assert char_count == 5
        assert ratio > 0.0

    def test_fully_non_ascii_text(self):
        text = "فيضان"  # all Arabic chars
        char_count, word_count, ratio = s6.compute_metrics(text)
        assert ratio == 1.0

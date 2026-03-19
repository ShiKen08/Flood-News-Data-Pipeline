"""
Tests for stage_00 crawl coverage logic:
  - parse_crawl_windows
  - check_crawl_coverage
"""
import json
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import stage_00_preflight as s0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_event(
    flood_id: int,
    start_date: str,
    duration: int,
) -> pd.Series:
    return pd.Series({
        "Flood_ID":   flood_id,
        "Start_Date": start_date,
        "Duration":   duration,
    })


def make_crawl(crawl_id: str, from_date: str, to_date: str) -> dict:
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return {
        "crawl_id": crawl_id,
        "from_dt":  datetime.strptime(from_date, fmt).replace(tzinfo=timezone.utc),
        "to_dt":    datetime.strptime(to_date,   fmt).replace(tzinfo=timezone.utc),
    }


# Patch the column name constants to match our Series keys
@pytest.fixture(autouse=True)
def patch_col_names(monkeypatch):
    monkeypatch.setattr(s0, "COL_FLOOD_ID",   "Flood_ID")
    monkeypatch.setattr(s0, "COL_START_DATE", "Start_Date")
    monkeypatch.setattr(s0, "COL_DURATION",   "Duration")


# =============================================================================
# parse_crawl_windows
# =============================================================================

class TestParseCrawlWindows:
    def test_valid_entries_parsed(self):
        collinfo = [
            {"id": "CC-MAIN-2024-10", "from": "2024-02-26T00:00:00Z", "to": "2024-03-04T00:00:00Z"},
            {"id": "CC-MAIN-2024-18", "from": "2024-04-22T00:00:00Z", "to": "2024-04-28T00:00:00Z"},
        ]
        crawls = s0.parse_crawl_windows(collinfo)
        assert len(crawls) == 2
        assert crawls[0]["crawl_id"] == "CC-MAIN-2024-10"
        assert isinstance(crawls[0]["from_dt"], datetime)
        assert isinstance(crawls[0]["to_dt"], datetime)

    def test_entry_with_missing_dates_is_skipped(self):
        collinfo = [
            {"id": "CC-MAIN-2024-10", "from": "2024-02-26T00:00:00Z", "to": "2024-03-04T00:00:00Z"},
            {"id": "CC-MAIN-BAD"},  # no from/to
        ]
        crawls = s0.parse_crawl_windows(collinfo)
        assert len(crawls) == 1

    def test_empty_collinfo_returns_empty_list(self):
        assert s0.parse_crawl_windows([]) == []

    def test_uses_name_as_fallback_id(self):
        collinfo = [{"name": "CC-MAIN-2024-10", "from": "2024-02-26T00:00:00Z", "to": "2024-03-04T00:00:00Z"}]
        crawls = s0.parse_crawl_windows(collinfo)
        assert crawls[0]["crawl_id"] == "CC-MAIN-2024-10"


# =============================================================================
# check_crawl_coverage
# =============================================================================

class TestCheckCrawlCoverage:
    def _covered_crawl(self, event_start: str) -> list[dict]:
        """Return a crawl that fully covers 7 days before and 30 days after event_start."""
        start = datetime.strptime(event_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return [make_crawl(
            "CC-MAIN-2024-10",
            (start - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            (start + timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )]

    def test_fully_covered_event(self):
        event = make_event(flood_id=1, start_date="2024-03-01", duration=10)
        crawls = self._covered_crawl("2024-03-01")
        result = s0.check_crawl_coverage(event, crawls)
        assert result["coverage_status"] == "COVERED"
        assert result["flood_id"] == 1

    def test_no_crawl_available(self):
        event = make_event(flood_id=2, start_date="2024-03-01", duration=10)
        result = s0.check_crawl_coverage(event, [])
        assert result["coverage_status"] == "NO_CRAWL"

    def test_partial_coverage_single_crawl(self):
        event = make_event(flood_id=3, start_date="2024-03-01", duration=10)
        # Crawl starts AFTER the window start — partial coverage
        crawl = make_crawl(
            "CC-MAIN-2024-10",
            "2024-03-05T00:00:00Z",   # starts after window_start (2024-02-23)
            "2024-04-01T00:00:00Z",
        )
        result = s0.check_crawl_coverage(event, [crawl])
        assert result["coverage_status"] == "PARTIAL"

    def test_zero_duration_treated_as_default(self):
        event = make_event(flood_id=4, start_date="2024-03-01", duration=0)
        crawls = self._covered_crawl("2024-03-01")
        result = s0.check_crawl_coverage(event, crawls)
        # Should not crash and should produce a valid status
        assert result["coverage_status"] in ("COVERED", "PARTIAL", "NO_CRAWL")

    def test_long_duration_event_uses_capped_post_window(self):
        # duration > WINDOW_LONG_DURATION_THRESHOLD (30) should use WINDOW_POST_LONG_DAYS
        event = make_event(flood_id=5, start_date="2024-01-01", duration=60)
        crawls = self._covered_crawl("2024-01-01")
        result = s0.check_crawl_coverage(event, crawls)
        assert "Long event" in result["note"]

    def test_short_duration_event_uses_standard_post_window(self):
        event = make_event(flood_id=6, start_date="2024-03-01", duration=7)
        crawls = self._covered_crawl("2024-03-01")
        result = s0.check_crawl_coverage(event, crawls)
        assert "Long event" not in result["note"]

    def test_multiple_crawls_gives_covered(self):
        event = make_event(flood_id=7, start_date="2024-03-01", duration=10)
        crawl1 = make_crawl("CC-MAIN-2024-10", "2024-02-01T00:00:00Z", "2024-03-10T00:00:00Z")
        crawl2 = make_crawl("CC-MAIN-2024-14", "2024-03-10T00:00:00Z", "2024-04-15T00:00:00Z")
        result = s0.check_crawl_coverage(event, [crawl1, crawl2])
        assert result["coverage_status"] == "COVERED"
        matching = json.loads(result["matching_crawls"])
        assert len(matching) == 2

    def test_result_contains_expected_keys(self):
        event = make_event(flood_id=8, start_date="2024-03-01", duration=5)
        result = s0.check_crawl_coverage(event, [])
        expected_keys = {
            "flood_id", "window_start", "window_end",
            "window_rule_version", "coverage_status", "matching_crawls", "note"
        }
        assert expected_keys == set(result.keys())

    def test_matching_crawls_is_json_serialisable(self):
        event = make_event(flood_id=9, start_date="2024-03-01", duration=5)
        crawls = self._covered_crawl("2024-03-01")
        result = s0.check_crawl_coverage(event, crawls)
        # Should not raise
        parsed = json.loads(result["matching_crawls"])
        assert isinstance(parsed, list)

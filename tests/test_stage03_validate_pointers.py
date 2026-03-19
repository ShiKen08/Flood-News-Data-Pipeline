"""
Tests for stage_03 pointer validation:
  - validate_pointers
  - apply_size_filter
"""
import sys

import pandas as pd
import pytest

import stage_03_validate_pointers as s3

VALID_FILENAME = "crawl-data/CC-MAIN-2024-10/segments/1234567890/warc/CC-MAIN-20241001.warc.gz"


def make_df(**overrides):
    """Build a minimal single-row pointer DataFrame with valid defaults."""
    row = {
        "offset":    "12345",
        "length":    "98765",
        "filename":  VALID_FILENAME,
        "flood_id":  1,
        "url":       "https://example.com/article",
    }
    row.update(overrides)
    return pd.DataFrame([row])


# =============================================================================
# validate_pointers
# =============================================================================

class TestValidatePointers:
    def test_valid_row_passes(self):
        df = make_df()
        valid, rejected = s3.validate_pointers(df)
        assert len(valid) == 1
        assert len(rejected) == 0

    def test_null_offset_is_rejected(self):
        df = make_df(offset=None)
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1
        assert "offset" in rejected.iloc[0]["reject_reason"]

    def test_negative_offset_is_rejected(self):
        df = make_df(offset="-1")
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1

    def test_zero_offset_is_valid(self):
        df = make_df(offset="0")
        valid, rejected = s3.validate_pointers(df)
        assert len(valid) == 1

    def test_null_length_is_rejected(self):
        df = make_df(length=None)
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1
        assert "length" in rejected.iloc[0]["reject_reason"]

    def test_zero_length_is_rejected(self):
        df = make_df(length="0")
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1

    def test_negative_length_is_rejected(self):
        df = make_df(length="-500")
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1

    def test_non_numeric_offset_is_rejected(self):
        df = make_df(offset="abc")
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1

    def test_non_numeric_length_is_rejected(self):
        df = make_df(length="abc")
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1

    def test_null_filename_is_rejected(self):
        df = make_df(filename=None)
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1
        assert "filename" in rejected.iloc[0]["reject_reason"]

    def test_empty_filename_is_rejected(self):
        df = make_df(filename="")
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1

    def test_malformed_filename_is_rejected(self):
        df = make_df(filename="not/a/valid/warc/path.html")
        valid, rejected = s3.validate_pointers(df)
        assert len(rejected) == 1

    def test_valid_warc_filename_pattern_passes(self):
        fn = "crawl-data/CC-MAIN-2023-06/segments/9876543210/warc/CC-MAIN-20230215.warc.gz"
        df = make_df(filename=fn)
        valid, rejected = s3.validate_pointers(df)
        assert len(valid) == 1

    def test_multiple_reasons_combined(self):
        df = make_df(offset=None, length=None, filename="bad")
        valid, rejected = s3.validate_pointers(df)
        reason = rejected.iloc[0]["reject_reason"]
        assert "offset" in reason
        assert "length" in reason
        assert "filename" in reason

    def test_mixed_valid_and_invalid_rows(self):
        valid_row = {
            "offset": "100", "length": "500",
            "filename": VALID_FILENAME, "flood_id": 1, "url": "https://a.com"
        }
        bad_row = {
            "offset": None, "length": "500",
            "filename": VALID_FILENAME, "flood_id": 1, "url": "https://b.com"
        }
        df = pd.DataFrame([valid_row, bad_row])
        valid, rejected = s3.validate_pointers(df)
        assert len(valid) == 1
        assert len(rejected) == 1

    def test_output_does_not_contain_internal_reject_column(self):
        df = make_df()
        valid, rejected = s3.validate_pointers(df)
        assert "_reject_reason" not in valid.columns
        assert "_reject_reason" not in rejected.columns

    def test_status_column_set_correctly(self):
        df = make_df()
        valid, _ = s3.validate_pointers(df)
        assert valid.iloc[0]["status"] == "VALID"

    def test_rejected_status_column(self):
        df = make_df(offset=None)
        _, rejected = s3.validate_pointers(df)
        assert rejected.iloc[0]["status"] == "REJECTED"


# =============================================================================
# apply_size_filter
# =============================================================================

class TestApplySizeFilter:
    def _make_valid_df(self, length_bytes):
        df = make_df(length=str(length_bytes))
        # validate_pointers adds 'status' column needed by apply_size_filter
        valid, _ = s3.validate_pointers(df)
        return valid

    def test_valid_size_passes(self):
        df = self._make_valid_df(1_000)
        result, too_small = s3.apply_size_filter(df)
        assert result.iloc[0]["size_filter_status"] == "VALID"
        assert len(too_small) == 0

    def test_too_small_flagged(self):
        df = self._make_valid_df(100)  # < POINTER_MIN_BYTES=500
        result, too_small = s3.apply_size_filter(df)
        # TOO_SMALL rows are moved to the rejects table, not kept in proceed_df
        assert len(result) == 0
        assert len(too_small) == 1
        assert too_small.iloc[0]["size_filter_status"] == "TOO_SMALL"

    def test_too_large_flagged(self):
        df = self._make_valid_df(6_000_000)  # > POINTER_MAX_BYTES=5MB
        result, too_small = s3.apply_size_filter(df)
        assert result.iloc[0]["size_filter_status"] == "TOO_LARGE"
        # TOO_LARGE rows are set aside but not put in too_small
        assert len(too_small) == 0

    def test_boundary_min_is_valid(self):
        df = self._make_valid_df(500)  # exact min
        result, _ = s3.apply_size_filter(df)
        assert result.iloc[0]["size_filter_status"] == "VALID"

    def test_boundary_max_is_valid(self):
        df = self._make_valid_df(5_000_000)  # exact max
        result, _ = s3.apply_size_filter(df)
        assert result.iloc[0]["size_filter_status"] == "VALID"

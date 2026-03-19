"""
Shared fixtures and path setup for the flood pipeline test suite.

Adds the project root to sys.path so stage modules can be imported
without triggering their config.py side effects (directory creation).
We patch config constants where needed to avoid touching the filesystem.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Minimal config mock — prevents config.py from creating directories on import
# ---------------------------------------------------------------------------
_mock_config = MagicMock()
_mock_config.MIN_CHAR_COUNT = 200
_mock_config.MAX_NON_ASCII_RATIO = 0.6
_mock_config.ERROR_PAGE_PATTERNS = ["404", "403", "page not found", "access denied"]
_mock_config.WINDOW_PRE_DAYS = 7
_mock_config.WINDOW_POST_DAYS = 14
_mock_config.WINDOW_POST_LONG_DAYS = 30
_mock_config.WINDOW_LONG_DURATION_THRESHOLD = 30
_mock_config.ZERO_DURATION_TREAT_AS_DAYS = 7
_mock_config.WINDOW_RULE_VERSION = "v1"
_mock_config.POINTER_MIN_BYTES = 500
_mock_config.POINTER_MAX_BYTES = 5_000_000
_mock_config.EXPECTED_NO_CRAWL_IDS = []
_mock_config.TIER_3_LANGUAGES = ["tha", "khm", "lao", "mya"]
_mock_config.TIER_3_FALLBACK_LANGUAGES = ["fra", "eng"]
_mock_config.LOGS_DIR = Path("/tmp")
_mock_config.OUTPUT_DIR = Path("/tmp")
_mock_config.PILOT_FLOOD_IDS = [1, 2, 3]

sys.modules["config"] = _mock_config

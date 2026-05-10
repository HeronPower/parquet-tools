"""Tests for timeutils.py timestamp parsing and formatting."""

import sys
from pathlib import Path
import pytest
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from timeutils import parse_timestamp, format_timestamp, series_to_unix


def test_parse_unix_float():
    assert parse_timestamp(1710403267.123) == pytest.approx(1710403267.123)


def test_parse_unix_int():
    assert parse_timestamp(1710403267) == pytest.approx(1710403267.0)


def test_parse_unix_string():
    assert parse_timestamp("1710403267.123") == pytest.approx(1710403267.123)


def test_parse_iso_with_z():
    ts = parse_timestamp("2024-03-14T08:01:07Z")
    assert ts == pytest.approx(1710403267.0, abs=1.0)


def test_parse_iso_with_offset():
    ts = parse_timestamp("2024-03-14T08:01:07+00:00")
    assert ts == pytest.approx(1710403267.0, abs=1.0)


def test_parse_iso_with_ms():
    ts = parse_timestamp("2024-03-14T08:01:07.123Z")
    assert ts == pytest.approx(1710403267.123, abs=0.01)


def test_parse_invalid_raises():
    with pytest.raises(ValueError):
        parse_timestamp("not-a-timestamp")


def test_format_unix():
    result = format_timestamp(1710403267.123456, iso=False)
    assert result == "1710403267.123456"


def test_format_iso():
    result = format_timestamp(1710403267.0, iso=True)
    assert "2024-03-14" in result
    assert result.endswith("Z")


def test_series_to_unix_numeric():
    s = pd.Series([1710403267.0, 1710403268.0])
    result = series_to_unix(s)
    assert list(result) == pytest.approx([1710403267.0, 1710403268.0])


def test_series_to_unix_datetime():
    s = pd.to_datetime(["2024-03-14T08:01:07Z", "2024-03-14T08:01:08Z"], utc=True)
    result = series_to_unix(pd.Series(s))
    assert result.iloc[0] == pytest.approx(1710403267.0, abs=1.0)

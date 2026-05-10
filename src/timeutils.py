"""
timeutils.py — Timestamp conversion utilities.

Internally all timestamps are float Unix seconds (UTC).
Display format is controlled by the --iso flag on each tool.

Accepts either format as input:
  Unix:  1710403267.123
  ISO:   2024-03-14T08:01:07.123Z  or  2024-03-14T08:01:07+00:00
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Union

import pandas as pd


# ─── Input Parsing ─────────────────────────────────────────────────────────────

_ISO_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"          # date part
    r"[T ]\d{2}:\d{2}(:\d{2})?"    # time part (seconds optional)
    r"(\.\d+)?"                     # fractional seconds
    r"(Z|[+-]\d{2}:?\d{2})?$"      # timezone
)


def parse_timestamp(value: Union[str, float, int]) -> float:
    """
    Parse a timestamp in any supported format and return Unix seconds (float).

    Accepts:
      - float/int: treated as Unix seconds directly
      - string matching ISO 8601: parsed as UTC datetime
      - string of digits (possibly with decimal): treated as Unix seconds
    """
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()

    # Pure numeric string
    if re.match(r"^\d+(\.\d+)?$", s):
        return float(s)

    # ISO 8601 variants
    if _ISO_RE.match(s):
        # Normalise timezone marker
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass

    raise ValueError(
        f"Cannot parse timestamp: {value!r}\n"
        "Expected Unix seconds (e.g. 1710403267.123) or "
        "ISO 8601 UTC (e.g. 2024-03-14T08:01:07Z)"
    )


# ─── Output Formatting ─────────────────────────────────────────────────────────

def format_timestamp(unix_seconds: float, iso: bool = False) -> str:
    """
    Format a Unix timestamp for display.

    unix_seconds: float seconds since epoch
    iso:          if True, return ISO 8601 UTC string; otherwise return
                  Unix decimal seconds as string
    """
    if iso:
        dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
        # Include microseconds only if non-zero
        if dt.microsecond:
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"  # ms precision
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return f"{unix_seconds:.6f}"


# ─── Pandas Series Conversion ──────────────────────────────────────────────────

def series_to_unix(series: pd.Series) -> pd.Series:
    """
    Convert a pandas Series to float64 Unix seconds, regardless of input dtype.

    Handles:
      - datetime64[ns] and datetime64[ns, UTC]
      - object dtype strings (ISO 8601)
      - numeric (already Unix seconds — passthrough)
    """
    if pd.api.types.is_datetime64_any_dtype(series):
        # Convert to UTC if timezone-aware, then to Unix nanoseconds → seconds
        ts = series
        if hasattr(series.dt, "tz") and series.dt.tz is not None:
            ts = series.dt.tz_convert("UTC")
        else:
            ts = series.dt.tz_localize("UTC")
        # pandas datetime64 resolution varies: ns (older) or us/ms (newer pandas)
        # Convert via float seconds to be resolution-agnostic
        if hasattr(ts, 'dt'):
            return (ts.dt.floor('us').astype('int64') / 1_000_000).astype('float64')
        return ts.astype('int64').astype('float64') / 1_000_000_000

    if pd.api.types.is_numeric_dtype(series):
        return series.astype("float64")

    # Object dtype — try parsing each value
    return series.apply(lambda v: parse_timestamp(v) if pd.notna(v) else float("nan"))


def format_series(unix_series: pd.Series, iso: bool = False) -> pd.Series:
    """Format a Series of Unix floats for display."""
    return unix_series.apply(lambda v: format_timestamp(v, iso=iso) if pd.notna(v) else "")

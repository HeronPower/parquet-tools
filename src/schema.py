"""
schema.py — Column classification and signal taxonomy.

Classifies each column in a parquet file into one of five categories:
  alert       Boolean 0/1 fault or alert signal
  measurement Numeric sensor readback
  config      Fixed FW threshold (rarely changes)
  command     User/operator command
  control     Internal FW control signal

Detection is heuristic and based on:
  1. Column name / parent frame name containing keywords
  2. Data type
  3. Value range (booleans that only contain {0, 1})

Run `mise schema-interactive` with a .dbc or Modbus file to populate
KNOWN_ALERT_SIGNALS and KNOWN_SIGNAL_CATEGORIES with real signal names,
then update CLAUDE.md with the results.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import pandas as pd
import numpy as np


# ─── Signal Taxonomy ──────────────────────────────────────────────────────────

class SignalCategory(Enum):
    ALERT       = "alert"        # boolean 0/1 fault/alert active
    MEASUREMENT = "measurement"  # numeric sensor readback
    CONFIG      = "config"       # fixed FW threshold (ends in _cfg / _config)
    COMMAND     = "command"      # user/operator command (ends in _cmd / _command)
    CONTROL     = "control"      # internal FW control signal
    TIMESTAMP   = "timestamp"    # the time axis — not a signal
    UNKNOWN     = "unknown"


# ─── Keyword Heuristics ───────────────────────────────────────────────────────
# These drive automatic classification. Update as your schema is confirmed.

ALERT_KEYWORDS = ["fault", "alert", "alarm", "error", "trip", "oc", "uvlo", "ovlo",
                  "overtemp", "uvp", "ovp", "mia", "lost", "tripped"]

CONFIG_SUFFIXES = ["_cfg", "_config", "_threshold", "_limit", "_setpoint", "_param"]

COMMAND_SUFFIXES = ["_cmd", "_command", "_req", "_request", "_setpt"]

CONTROL_SUFFIXES = ["_ctrl", "_control", "_enable", "_disable", "_mode"]

TIMESTAMP_KEYWORDS = ["timestamp", "time", "datetime", "logged_at", "created_at", "epoch"]
# Note: short keywords like "ts", "date", "when" are matched with word-boundary logic
# in detect_timestamp_col to avoid false matches (e.g. "ts" inside "alerts")

# ─── TODO: Populate with real signal names from your .dbc / Modbus files ──────
# Run `mise schema-interactive` to generate these lists automatically.
#
# Format: set of exact column names (lowercased)
KNOWN_ALERT_SIGNALS: set[str] = {
    # TODO: populated by schema-interactive from .dbc / Modbus
    # Examples from the signal tree in the screenshot:
    # "cell_mia", "l_phasea_oc", "l_phaseb_oc", "l_phasec_oc",
    # "mv_aux_uvlo", "mv_to_lv_comms_lost", "mv_tripped",
    # "dcac_bot_overtemp", "v_flyback_out_uvlo", "mia",
    # "overtemp_dcdc_bot", "overtemp_dcdc_top",
}

KNOWN_MEASUREMENT_SIGNALS: set[str] = {
    # TODO: populated by schema-interactive
}

KNOWN_CONFIG_SIGNALS: set[str] = {
    # TODO: populated by schema-interactive
}

KNOWN_COMMAND_SIGNALS: set[str] = {
    # TODO: populated by schema-interactive
}

KNOWN_CONTROL_SIGNALS: set[str] = {
    # TODO: populated by schema-interactive
}

# Parent CAN frame names that indicate all child signals are alerts
# e.g. "dcacCascadedControls_ALERTS" → any column from this frame is an alert
ALERT_FRAME_KEYWORDS = ["_ALERTS", "_alerts", "_FAULTS", "_faults"]


# ─── Schema Detection Result ──────────────────────────────────────────────────

@dataclass
class ColumnInfo:
    name: str
    category: SignalCategory
    dtype: str
    is_boolean: bool          # True if dtype is bool or only contains {0, 1}
    parent_frame: Optional[str]  # CAN frame name if encoded in column prefix
    nunique: int
    sample_values: list

    def is_alert(self) -> bool:
        return self.category == SignalCategory.ALERT

    def is_numeric(self) -> bool:
        return self.category in (SignalCategory.MEASUREMENT, SignalCategory.CONFIG,
                                 SignalCategory.COMMAND, SignalCategory.CONTROL)


@dataclass
class SchemaInfo:
    timestamp_col: Optional[str]
    columns: dict[str, ColumnInfo] = field(default_factory=dict)

    @property
    def alert_cols(self) -> list[str]:
        return [n for n, c in self.columns.items() if c.is_alert()]

    @property
    def numeric_cols(self) -> list[str]:
        return [n for n, c in self.columns.items() if c.is_numeric()]

    @property
    def signal_cols(self) -> list[str]:
        """All non-timestamp columns."""
        return [n for n in self.columns if n != self.timestamp_col]

    def summary(self) -> str:
        cats = {}
        for c in self.columns.values():
            cats[c.category.value] = cats.get(c.category.value, 0) + 1
        parts = [f"{k}={v}" for k, v in sorted(cats.items())]
        return f"Schema: {len(self.columns)} cols — {', '.join(parts)}"


# ─── Detection Logic ──────────────────────────────────────────────────────────

def _is_boolean_column(series: pd.Series) -> bool:
    """True if the column only ever contains 0 and 1 (or NaN)."""
    if series.dtype == bool:
        return True
    if not pd.api.types.is_numeric_dtype(series):
        return False
    unique = set(series.dropna().unique())
    return unique.issubset({0, 1, 0.0, 1.0})


def _extract_parent_frame(col_name: str) -> Optional[str]:
    """
    If column names encode CAN frame hierarchy as a prefix (e.g.
    "cabinet__dcacCascadedControls_ALERTS__cell_mia"), extract the
    parent frame portion.

    TODO: adjust the separator to match your actual parquet encoding.
    Common patterns: "__", ".", "/", ":"
    """
    # TODO: update separator to match your parquet column naming convention
    separator = "__"
    if separator in col_name:
        parts = col_name.split(separator)
        if len(parts) >= 2:
            return parts[-2]
    return None


def _classify_column(col_name: str, series: pd.Series) -> SignalCategory:
    """Heuristic classification of a single column."""
    lower = col_name.lower()
    parent = _extract_parent_frame(col_name)
    parent_lower = parent.lower() if parent else ""

    # Check known lists first (populated by schema-interactive)
    # Strip potential prefix to get bare signal name for lookup
    bare = lower.split("__")[-1] if "__" in lower else lower
    if bare in KNOWN_ALERT_SIGNALS:
        return SignalCategory.ALERT
    if bare in KNOWN_MEASUREMENT_SIGNALS:
        return SignalCategory.MEASUREMENT
    if bare in KNOWN_CONFIG_SIGNALS:
        return SignalCategory.CONFIG
    if bare in KNOWN_COMMAND_SIGNALS:
        return SignalCategory.COMMAND
    if bare in KNOWN_CONTROL_SIGNALS:
        return SignalCategory.CONTROL

    # Timestamp
    if any(kw in lower for kw in TIMESTAMP_KEYWORDS):
        return SignalCategory.TIMESTAMP

    # Alert: parent frame name contains alert keyword
    if parent_lower and any(kw.lower() in parent_lower for kw in ["alerts", "faults", "_alert", "_fault"]):
        return SignalCategory.ALERT

    # Alert: signal name itself contains alert/fault keyword
    if any(kw in lower for kw in ALERT_KEYWORDS):
        if _is_boolean_column(series):
            return SignalCategory.ALERT

    # Config / Command / Control by suffix
    if any(lower.endswith(s) for s in CONFIG_SUFFIXES):
        return SignalCategory.CONFIG
    if any(lower.endswith(s) for s in COMMAND_SUFFIXES):
        return SignalCategory.COMMAND
    if any(lower.endswith(s) for s in CONTROL_SUFFIXES):
        return SignalCategory.CONTROL

    # Fallback: boolean → alert candidate, numeric → measurement
    if _is_boolean_column(series):
        # Boolean but no alert keyword found — flag as unknown for review
        return SignalCategory.UNKNOWN
    if pd.api.types.is_numeric_dtype(series):
        return SignalCategory.MEASUREMENT

    return SignalCategory.UNKNOWN


def detect_timestamp_col(df: pd.DataFrame) -> Optional[str]:
    """Find the timestamp column. Prefers datetime dtype, then keyword match."""
    # 1. datetime dtype
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            return col
    # 2. keyword match on numeric (likely Unix seconds) — use word boundaries for short keywords
    SHORT_TS_KEYWORDS = ["ts", "date", "when"]
    for col in df.columns:
        lower = col.lower()
        if any(kw in lower for kw in TIMESTAMP_KEYWORDS):
            if pd.api.types.is_numeric_dtype(df[col]):
                return col
        # Short keywords: only match if the whole column name IS the keyword
        # or it appears as a word segment (bounded by _ or start/end)
        if any(re.fullmatch(rf'.*\b{kw}\b.*', lower) or lower == kw
               for kw in SHORT_TS_KEYWORDS):
            if pd.api.types.is_numeric_dtype(df[col]):
                return col
    # 3. first numeric column that looks like Unix timestamps (>1e9)
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            sample = df[col].dropna().head(10)
            if len(sample) and sample.mean() > 1e9:
                return col
    return None


def analyse_schema(df: pd.DataFrame) -> SchemaInfo:
    """
    Classify every column in the dataframe.
    Returns a SchemaInfo with timestamp_col and per-column ColumnInfo.
    """
    timestamp_col = detect_timestamp_col(df)
    schema = SchemaInfo(timestamp_col=timestamp_col)

    for col in df.columns:
        series = df[col]
        if col == timestamp_col:
            cat = SignalCategory.TIMESTAMP
        else:
            cat = _classify_column(col, series)

        parent = _extract_parent_frame(col)
        sample = series.dropna().head(5).tolist()

        schema.columns[col] = ColumnInfo(
            name=col,
            category=cat,
            dtype=str(series.dtype),
            is_boolean=_is_boolean_column(series),
            parent_frame=parent,
            nunique=series.nunique(),
            sample_values=sample,
        )

    return schema


def filter_alert_cols_by_pattern(schema: SchemaInfo, pattern: str) -> list[str]:
    """
    Return alert column names matching a regex pattern.
    Matches against the full column name and the bare signal name.
    """
    regex = re.compile(pattern, re.IGNORECASE)
    result = []
    for col in schema.alert_cols:
        bare = col.split("__")[-1] if "__" in col else col
        if regex.search(col) or regex.search(bare):
            result.append(col)
    return result


def print_schema_report(schema: SchemaInfo, console=None) -> None:
    """Print a rich table showing schema classification results."""
    from rich.console import Console
    from rich.table import Table
    from rich import box

    if console is None:
        from rich.console import Console
        console = Console()

    table = Table(title="Schema Classification", box=box.ROUNDED,
                  header_style="bold white on grey23")
    table.add_column("Column", style="cyan", max_width=50)
    table.add_column("Category", min_width=12)
    table.add_column("Dtype", style="dim", min_width=10)
    table.add_column("Boolean", justify="center")
    table.add_column("Parent Frame", style="dim", max_width=30)
    table.add_column("Sample", style="dim", max_width=30)

    cat_styles = {
        SignalCategory.ALERT:       "bold red",
        SignalCategory.MEASUREMENT: "green",
        SignalCategory.CONFIG:      "blue",
        SignalCategory.COMMAND:     "magenta",
        SignalCategory.CONTROL:     "yellow",
        SignalCategory.TIMESTAMP:   "dim cyan",
        SignalCategory.UNKNOWN:     "bold yellow",
    }

    for col, info in schema.columns.items():
        style = cat_styles.get(info.category, "")
        table.add_row(
            col,
            f"[{style}]{info.category.value}[/{style}]",
            info.dtype,
            "✓" if info.is_boolean else "",
            info.parent_frame or "",
            str(info.sample_values[:3]),
        )

    console.print(table)
    console.print(f"\n[dim]{schema.summary()}[/dim]")

    unknowns = [c for c, i in schema.columns.items() if i.category == SignalCategory.UNKNOWN]
    if unknowns:
        console.print(f"\n[bold yellow]⚠  {len(unknowns)} UNKNOWN columns — review and add to KNOWN_* sets:[/bold yellow]")
        for u in unknowns:
            console.print(f"  [yellow]{u}[/yellow]")

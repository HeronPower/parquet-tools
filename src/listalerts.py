"""
listalerts — List fault/alert boolean transitions in chronological order.

Alert signals are boolean columns (0/1) where:
  0→1 transition = fault SET   (displayed in red)
  1→0 transition = fault CLEAR (displayed in green)

Alert columns are detected automatically via schema.py — the parent CAN
frame name OR the signal name must contain 'fault' or 'alert'.
"""

import sys
import click
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

from loader import resolve_parquet_path, parse_window_args
from schema import (
    analyse_schema, filter_alert_cols_by_pattern,
    ALERT_KEYWORDS, ALERT_FRAME_KEYWORDS,
    detect_timestamp_col,
)
from timeutils import parse_timestamp, series_to_unix
from display import render_alert_table, render_summary, paged_print, console


def _preselect_columns(path: Path) -> list[str]:
    """
    Read only column names from parquet metadata (no data loaded) and return
    candidates that are likely alert/fault columns plus any timestamp column.

    This avoids loading all 300+ non-alert columns into memory.
    """
    arrow_schema = pq.read_schema(path)
    names = arrow_schema.names

    selected = []
    for name in names:
        lower = name.lower()
        parts = lower.split("/")
        signal_name = parts[-1]
        frame_name = parts[-2] if len(parts) >= 2 else ""

        # Timestamp candidates
        if any(kw in lower for kw in ["timestamp", "time", "datetime", "epoch"]):
            selected.append(name)
            continue

        # Frame name contains alert/fault keyword
        if any(kw.lower() in frame_name for kw in ["alert", "fault"]):
            selected.append(name)
            continue

        # Signal name contains alert/fault keyword
        if any(kw in signal_name for kw in ALERT_KEYWORDS):
            selected.append(name)

    return selected


def _find_transitions(df, alert_cols, ts_col, set_only=False, clear_only=False):
    """
    Vectorised transition detection on explicit logged values only.

    NaN rows are skipped entirely — only consecutive non-NaN values are
    compared. This prevents three classes of false event:
      - NaN → 1  (logging gap before an active fault)
      - 1  → NaN (logging gap after an active fault)
      - NaN → 0  (logging gap before a clear)

    A SET is fired when an explicit logged value changes 0 → 1, or when
    the very first logged value is 1 (implicit prior state = inactive).
    A CLR is fired only when an explicit logged value changes 1 → 0.
    """
    events = []

    for col in alert_cols:
        series = df[col]
        valid = series.notna()
        explicit_vals = series[valid]
        explicit_times = df.loc[valid, ts_col]

        if explicit_vals.empty:
            continue

        prev = explicit_vals.shift(1)   # NaN only for the very first explicit row

        if not clear_only:
            # 0 → 1, or first logged value is 1 (no prior explicit value = was inactive)
            set_mask = (explicit_vals == 1) & ((prev == 0) | prev.isna())
            for t in explicit_times[set_mask].to_numpy(dtype=float):
                events.append({"timestamp": float(t), "signal": col,
                               "transition": "set", "value_before": 0, "value_after": 1})

        if not set_only:
            # 1 → 0 only (explicit zero must be logged)
            clr_mask = (explicit_vals == 0) & (prev == 1)
            for t in explicit_times[clr_mask].to_numpy(dtype=float):
                events.append({"timestamp": float(t), "signal": col,
                               "transition": "clear", "value_before": 1, "value_after": 0})

    return sorted(events, key=lambda e: e["timestamp"])


def _load_alert_columns(path: Path, start, end):
    """
    Two-pass load:
      1. Read column names only (metadata) → pre-select alert + timestamp candidates.
      2. Load only those columns from parquet → run full schema analysis.

    This reduces I/O and memory for wide files (e.g. 383 columns → ~92 loaded).
    """
    candidate_cols = _preselect_columns(path)

    pf = pq.ParquetFile(path)
    table = pf.read(columns=candidate_cols)
    df = table.to_pandas()

    schema = analyse_schema(df)

    if schema.timestamp_col:
        df[schema.timestamp_col] = series_to_unix(df[schema.timestamp_col])
        df = df.sort_values(schema.timestamp_col).reset_index(drop=True)

    if start is not None or end is not None:
        ts = df[schema.timestamp_col]
        mask = (ts >= (start or ts.min())) & (ts <= (end or ts.max()))
        df = df[mask].reset_index(drop=True)

    return df, schema


@click.command(
    name="listalerts",
    help="""List fault/alert boolean transitions in chronological order.

\b
Colour coding:
  RED   = alert SET   (signal transitions 0→1, fault becomes active)
  GREEN = alert CLEAR (signal transitions 1→0, fault clears)

Alert columns are auto-detected: any boolean column whose name or parent
CAN frame name contains 'fault' or 'alert'.

\b
Examples:
  # All alert transitions in the file
  mise listalerts -- --file data.parquet

  # Only SET events matching a regex
  mise listalerts -- --filter 'overtemp.*' --set

  # Only CLEAR events, ISO timestamp output
  mise listalerts -- --filter 'mv_.*' --clear --iso

  # Time-windowed, set events only
  mise listalerts -- --windowstart 1710403200 --windowend 1710406800 --set

  # Combine filter with time window
  mise listalerts -- --filter 'dcdc' --windowstart 2024-03-14T08:00:00Z
""",
)
@click.option("--file", "file_path", envvar="PARQUET_FILE",
              help="Path to the .parquet file [env: PARQUET_FILE]")
@click.option("--filter", "filter_regex", default=None,
              help="Regex to match signal names (case-insensitive). Applied to full column name and bare signal name.")
@click.option("--set", "only_set", is_flag=True, default=False,
              help="Show only SET transitions (0→1). Default: show both.")
@click.option("--clear", "only_clear", is_flag=True, default=False,
              help="Show only CLEAR transitions (1→0). Default: show both.")
@click.option("--windowstart", default=None,
              help="Start of time window. Unix seconds or ISO 8601 UTC.")
@click.option("--windowend", default=None,
              help="End of time window. Unix seconds or ISO 8601 UTC.")
@click.option("--iso", is_flag=True, default=False,
              help="Output timestamps as ISO 8601 UTC instead of Unix decimal seconds.")
@click.option("--page-size", default=40, show_default=True,
              help="Rows per page in interactive output.")
@click.option("--no-pager", is_flag=True, default=False,
              help="Print all results without interactive paging.")
def listalerts(file_path, filter_regex, only_set, only_clear,
               windowstart, windowend, iso, page_size, no_pager):

    if only_set and only_clear:
        console.print("[red]ERROR:[/red] --set and --clear are mutually exclusive.")
        sys.exit(1)

    path = resolve_parquet_path(file_path)
    start, end = parse_window_args(windowstart, windowend)

    console.print(f"[dim]Loading[/dim] [cyan]{path}[/cyan] …")
    df, schema = _load_alert_columns(path, start, end)

    if schema.timestamp_col is None:
        console.print("[red]ERROR:[/red] No timestamp column detected. Check schema.py.")
        sys.exit(2)

    alert_cols = schema.alert_cols
    if not alert_cols:
        console.print("[yellow]No alert/fault columns detected in this file.[/yellow]")
        console.print("[dim]Check that column names or parent frame names contain 'fault' or 'alert'.[/dim]")
        sys.exit(2)

    if filter_regex:
        alert_cols = filter_alert_cols_by_pattern(schema, filter_regex)
        if not alert_cols:
            console.print(f"[yellow]No alert columns match filter:[/yellow] [cyan]{filter_regex}[/cyan]")
            sys.exit(2)

    console.print(f"[dim]Analysing {len(alert_cols)} alert column(s)…[/dim]")

    events = _find_transitions(df, alert_cols, schema.timestamp_col,
                               set_only=only_set, clear_only=only_clear)

    if not events:
        console.print("[yellow]No transitions found in the specified window / filter.[/yellow]")
        sys.exit(0)

    set_count = sum(1 for e in events if e["transition"] == "set")
    clear_count = sum(1 for e in events if e["transition"] == "clear")

    if no_pager:
        table = render_alert_table(events, iso=iso)
        console.print(table)
    else:
        paged_print(events, render_alert_table, page_size=page_size, iso=iso)

    render_summary({"Total": len(events), "SET": set_count, "CLEAR": clear_count})


if __name__ == "__main__":
    listalerts()

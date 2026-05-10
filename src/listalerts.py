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
from rich.console import Console

# Allow running from src/ directory directly
sys.path.insert(0, str(Path(__file__).parent))

from loader import resolve_parquet_path, load_window, parse_window_args
from schema import filter_alert_cols_by_pattern
from timeutils import parse_timestamp, format_timestamp
from display import render_alert_table, render_summary, paged_print, console


def _find_transitions(df, alert_cols, ts_col, set_only=False, clear_only=False):
    """Extract all 0/1 transitions from alert columns, return sorted event list."""
    events = []
    for col in alert_cols:
        series = df[col].fillna(0).astype(int)
        times = df[ts_col]

        prev_val = None
        for i in range(len(series)):
            val = series.iloc[i]
            t = float(times.iloc[i])

            if prev_val is not None and val != prev_val:
                if val == 1:   # 0→1 SET
                    if not clear_only:
                        events.append({
                            "timestamp": t,
                            "signal": col,
                            "transition": "set",
                            "value_before": prev_val,
                            "value_after": val,
                        })
                elif val == 0:  # 1→0 CLEAR
                    if not set_only:
                        events.append({
                            "timestamp": t,
                            "signal": col,
                            "transition": "clear",
                            "value_before": prev_val,
                            "value_after": val,
                        })
            prev_val = val

    return sorted(events, key=lambda e: e["timestamp"])


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
    df, schema = load_window(path, start=start, end=end)

    if schema.timestamp_col is None:
        console.print("[red]ERROR:[/red] No timestamp column detected. Check schema.py.")
        sys.exit(2)

    alert_cols = schema.alert_cols
    if not alert_cols:
        console.print("[yellow]No alert/fault columns detected in this file.[/yellow]")
        console.print("[dim]Check that column names or parent frame names contain 'fault' or 'alert'.[/dim]")
        sys.exit(2)

    # Apply regex filter
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

"""
whatchanged — Find signals with sharp d(signal)/dt spikes around a timestamp.

A spike is detected when |d(signal)/dt| > threshold × mean(|d(signal)/dt|)
for that signal over the analysis window.

Use --before to look backwards from a timestamp (reverse-chronological output).
Use --after  to look forwards from a timestamp (chronological output).
"""

import sys
import click
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from loader import resolve_parquet_path, load_window, parse_window_args
from derivatives import find_all_sharp_changes
from timeutils import parse_timestamp
from display import render_change_table, render_summary, paged_print, console


@click.command(
    name="whatchanged",
    help="""Find signals with sharp d(signal)/dt spikes around a timestamp.

\b
A spike is flagged when:
  |d(signal)/dt| > threshold × mean(|d(signal)/dt|)

where the baseline mean is computed over the full loaded window.

\b
Direction:
  --before TIMESTAMP  traverse backwards (reverse-chronological output)
  --after  TIMESTAMP  traverse forwards  (chronological output)

\b
Examples:
  # All sharp changes before a Unix timestamp
  mise whatchanged -- --before 1710403267.123

  # All sharp changes after an ISO timestamp
  mise whatchanged -- --after 2024-03-14T08:00:00Z

  # More sensitive detection (lower multiplier = more results)
  mise whatchanged -- --before 1710403267 --threshold 1.5

  # Less sensitive (fewer, more dramatic changes only)
  mise whatchanged -- --after 1710403267 --threshold 4.0 --iso

  # Restrict to a time window around a point
  mise whatchanged -- --after 1710400000 --windowend 1710410000
""",
)
@click.option("--file", "file_path", envvar="PARQUET_FILE",
              help="Path to the .parquet file [env: PARQUET_FILE]")
@click.option("--before", "before_ts", default=None,
              help="Anchor timestamp. Report changes BEFORE this point, reverse-chronological. Unix seconds or ISO 8601 UTC.")
@click.option("--after", "after_ts", default=None,
              help="Anchor timestamp. Report changes AFTER this point, chronological. Unix seconds or ISO 8601 UTC.")
@click.option("--windowstart", default=None,
              help="Start of analysis window (additional constraint). Unix seconds or ISO 8601 UTC.")
@click.option("--windowend", default=None,
              help="End of analysis window (additional constraint). Unix seconds or ISO 8601 UTC.")
@click.option("--threshold", default=2.0, show_default=True,
              help="Spike multiplier. A change is reported when |d/dt| > threshold × mean(|d/dt|).")
@click.option("--iso", is_flag=True, default=False,
              help="Output timestamps as ISO 8601 UTC instead of Unix decimal seconds.")
@click.option("--page-size", default=40, show_default=True,
              help="Rows per page in interactive output.")
@click.option("--no-pager", is_flag=True, default=False,
              help="Print all results without interactive paging.")
def whatchanged(file_path, before_ts, after_ts, windowstart, windowend,
                threshold, iso, page_size, no_pager):

    if before_ts and after_ts:
        console.print("[red]ERROR:[/red] --before and --after are mutually exclusive.")
        sys.exit(1)
    if not before_ts and not after_ts:
        console.print("[red]ERROR:[/red] Specify either --before or --after with a timestamp.")
        sys.exit(1)

    path = resolve_parquet_path(file_path)
    start, end = parse_window_args(windowstart, windowend)

    # Anchor timestamp becomes additional window bound
    anchor: float
    if before_ts:
        anchor = parse_timestamp(before_ts)
        end = min(end, anchor) if end is not None else anchor
    else:
        anchor = parse_timestamp(after_ts)
        start = max(start, anchor) if start is not None else anchor

    console.print(f"[dim]Loading[/dim] [cyan]{path.name}[/cyan] …")
    df, schema = load_window(path, start=start, end=end)

    if schema.timestamp_col is None:
        console.print("[red]ERROR:[/red] No timestamp column detected.")
        sys.exit(2)

    numeric_cols = schema.numeric_cols
    if not numeric_cols:
        console.print("[yellow]No numeric signal columns detected.[/yellow]")
        sys.exit(2)

    console.print(f"[dim]Analysing {len(numeric_cols)} signal(s) with threshold={threshold}×…[/dim]")

    events = find_all_sharp_changes(
        df, numeric_cols, schema.timestamp_col,
        threshold=threshold,
    )

    if before_ts:
        events = sorted(events, key=lambda e: e.timestamp_start, reverse=True)
        direction_label = f"before {format_ts(anchor, iso)}"
    else:
        events = sorted(events, key=lambda e: e.timestamp_start)
        direction_label = f"after {format_ts(anchor, iso)}"

    if not events:
        console.print(f"[yellow]No sharp changes detected ({direction_label}, threshold={threshold}×).[/yellow]")
        console.print("[dim]Try lowering --threshold (e.g. --threshold 1.5)[/dim]")
        sys.exit(0)

    title = f"Sharp Changes — {direction_label} (threshold={threshold}×)"

    if no_pager:
        console.print(render_change_table(events, iso=iso, title=title))
    else:
        paged_print(
            events,
            lambda chunk, iso=False: render_change_table(chunk, iso=iso, title=title),
            page_size=page_size,
            iso=iso,
        )

    render_summary({
        "Signals scanned": len(numeric_cols),
        "Sharp changes": len(events),
        "Threshold": f"{threshold}×",
    })


def format_ts(unix: float, iso: bool) -> str:
    from timeutils import format_timestamp
    return format_timestamp(unix, iso=iso)


if __name__ == "__main__":
    whatchanged()

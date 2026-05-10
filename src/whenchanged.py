"""
whenchanged — Find when a signal crossed a value threshold (edge detection).

If beforevalue < aftervalue → rising edge search (signal goes low→high)
If beforevalue > aftervalue → falling edge search (signal goes high→low)
"""

import sys
import click
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from loader import resolve_parquet_path, load_window, parse_window_args
from derivatives import find_edge
from display import render_edge_table, render_summary, paged_print, console


@click.command(
    name="whenchanged",
    help="""Find when a signal crossed a value threshold (edge detection).

\b
Edge direction is inferred from argument ordering:
  beforevalue < aftervalue  →  rising edge  (↑)
  beforevalue > aftervalue  →  falling edge (↓)

\b
Examples:
  # When did battery voltage rise from ≤48V to ≥52V?
  mise whenchanged -- --signal battery_voltage --beforevalue 48.0 --aftervalue 52.0

  # When did motor temperature fall from ≥90°C to ≤80°C?
  mise whenchanged -- --signal motor_temp --beforevalue 90.0 --aftervalue 80.0

  # Search only after a specific time, output ISO timestamps
  mise whenchanged -- --signal dc_link_voltage --beforevalue 380 --aftervalue 400 \\
    --windowstart 2024-03-14T08:00:00Z --iso

  # Adjust debounce (more tolerant of noise)
  mise whenchanged -- --signal bus_current --beforevalue -5.0 --aftervalue 5.0 --debounce 1
""",
)
@click.option("--file", "file_path", envvar="PARQUET_FILE",
              help="Path to the .parquet file [env: PARQUET_FILE]")
@click.option("--signal", required=True,
              help="Signal (column) name to analyse. Partial match supported — will error if ambiguous.")
@click.option("--beforevalue", required=True, type=float,
              help="Value on the 'before' side of the edge. "
                   "beforevalue < aftervalue → rising edge; beforevalue > aftervalue → falling edge.")
@click.option("--aftervalue", required=True, type=float,
              help="Value on the 'after' side of the edge.")
@click.option("--windowstart", default=None,
              help="Begin search from this timestamp. Unix seconds or ISO 8601 UTC.")
@click.option("--windowend", default=None,
              help="End search at this timestamp. Unix seconds or ISO 8601 UTC.")
@click.option("--debounce", default=3, show_default=True,
              help="Number of consecutive samples that must sustain the new value to confirm the transition.")
@click.option("--iso", is_flag=True, default=False,
              help="Output timestamps as ISO 8601 UTC instead of Unix decimal seconds.")
@click.option("--no-pager", is_flag=True, default=False,
              help="Print all results without interactive paging.")
def whenchanged(file_path, signal, beforevalue, aftervalue,
                windowstart, windowend, debounce, iso, no_pager):

    if beforevalue == aftervalue:
        console.print("[red]ERROR:[/red] --beforevalue and --aftervalue must differ.")
        sys.exit(1)

    path = resolve_parquet_path(file_path)
    start, end = parse_window_args(windowstart, windowend)

    console.print(f"[dim]Loading[/dim] [cyan]{path.name}[/cyan] …")
    df, schema = load_window(path, start=start, end=end)

    if schema.timestamp_col is None:
        console.print("[red]ERROR:[/red] No timestamp column detected.")
        sys.exit(2)

    # Resolve signal name — allow partial match if unambiguous
    all_cols = list(df.columns)
    matched = [c for c in all_cols if signal.lower() in c.lower()]
    if not matched:
        console.print(f"[red]ERROR:[/red] No column matching [cyan]{signal!r}[/cyan] found.")
        console.print(f"[dim]Available columns: {', '.join(all_cols[:20])}{'…' if len(all_cols) > 20 else ''}[/dim]")
        sys.exit(1)
    if len(matched) > 1:
        console.print(f"[yellow]Ambiguous signal name[/yellow] [cyan]{signal!r}[/cyan] — matches:")
        for m in matched:
            console.print(f"  [cyan]{m}[/cyan]")
        console.print("[dim]Use a more specific name.[/dim]")
        sys.exit(1)

    signal_col = matched[0]
    edge_type = "rising" if beforevalue < aftervalue else "falling"
    arrow = "↑" if edge_type == "rising" else "↓"

    console.print(
        f"[dim]Searching[/dim] [cyan]{signal_col}[/cyan] "
        f"for {arrow} edge {beforevalue} → {aftervalue} "
        f"[dim](debounce={debounce})[/dim]"
    )

    events = find_edge(
        df, signal_col, schema.timestamp_col,
        before_value=beforevalue,
        after_value=aftervalue,
        debounce_samples=debounce,
        start_time=start,
    )

    if not events:
        console.print(f"[yellow]No {edge_type} edge found for[/yellow] [cyan]{signal_col}[/cyan].")
        sys.exit(0)

    title = f"Edge Detections — {signal_col}  {beforevalue} {arrow} {aftervalue}"

    if no_pager:
        console.print(render_edge_table(events, iso=iso, title=title))
    else:
        paged_print(events, lambda chunk, iso=False: render_edge_table(chunk, iso=iso, title=title), iso=iso)

    render_summary({"Signal": signal_col, "Edges found": len(events), "Type": edge_type})


if __name__ == "__main__":
    whenchanged()

"""
display.py — Shared terminal rendering via rich.

Colour conventions (from CLAUDE.md):
  bold red    = alert SET (0→1)
  bold green  = alert CLEAR (1→0)
  yellow      = warning / unknown state
  cyan        = signal name
  dim         = timestamp
  orange1     = sharp dsdt spike (whatchanged)
"""

from __future__ import annotations

from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.rule import Rule
from rich.prompt import Prompt
from rich import box

from timeutils import format_timestamp

console = Console()


# ─── Alert Event Rendering ────────────────────────────────────────────────────

def alert_event_style(transition: str) -> tuple[str, str]:
    """
    Return (event_label, rich_style) for a boolean alert transition.

    transition: "set" (0→1) or "clear" (1→0)
    """
    if transition == "set":
        return "● SET", "bold red"
    elif transition == "clear":
        return "● CLR", "bold green"
    else:
        return "? UNK", "dim yellow"


def render_alert_table(
    events: list[dict],
    iso: bool = False,
    title: str = "Fault / Alert Log",
) -> Table:
    """
    Render a list of alert transition events as a rich Table.

    Each event dict must have:
      timestamp (float Unix seconds)
      signal (str)
      transition ("set" | "clear")
      value_before (0 or 1)
      value_after  (0 or 1)
    """
    table = Table(
        title=title,
        box=box.ROUNDED,
        header_style="bold white on grey23",
    )
    table.add_column("#", style="dim", width=5, justify="right")
    table.add_column("Timestamp", style="dim", min_width=20)
    table.add_column("Event", min_width=8, justify="center")
    table.add_column("Signal", style="cyan", min_width=80, no_wrap=True)
    table.add_column("Before→After", justify="center", min_width=12)

    for i, ev in enumerate(events, 1):
        label, style = alert_event_style(ev.get("transition", "unknown"))
        table.add_row(
            str(i),
            format_timestamp(ev["timestamp"], iso=iso),
            Text(label, style=style),
            ev["signal"],
            f"{ev['value_before']}→{ev['value_after']}",
        )

    return table


# ─── Sharp Change Rendering ────────────────────────────────────────────────────

def render_change_table(
    events: list,   # list[ChangeEvent]
    iso: bool = False,
    title: str = "Sharp Signal Changes",
) -> Table:
    """Render a list of ChangeEvent objects as a rich Table."""
    table = Table(
        title=title,
        box=box.ROUNDED,
        header_style="bold white on grey23",
    )
    table.add_column("#", style="dim", width=5, justify="right")
    table.add_column("Timestamp", style="dim", min_width=20)
    table.add_column("Signal", style="cyan", min_width=80, no_wrap=True)
    table.add_column("Before", justify="right", min_width=10)
    table.add_column("After", justify="right", min_width=10)
    table.add_column("Δ", justify="right", min_width=10)
    table.add_column("|d/dt|", justify="right", style="orange1", min_width=10)
    table.add_column("×baseline", justify="right", style="dim", min_width=10)

    for i, ev in enumerate(events, 1):
        delta = ev.value_after - ev.value_before
        delta_str = f"+{delta:.4g}" if delta >= 0 else f"{delta:.4g}"
        table.add_row(
            str(i),
            format_timestamp(ev.timestamp_start, iso=iso),
            ev.signal,
            f"{ev.value_before:.4g}",
            f"{ev.value_after:.4g}",
            delta_str,
            f"{ev.dsdt_magnitude:.4g}",
            f"{ev.spike_ratio:.1f}×",
        )

    return table


# ─── Edge Event Rendering ─────────────────────────────────────────────────────

def render_edge_table(
    events: list,   # list[EdgeEvent]
    iso: bool = False,
    title: str = "Signal Threshold Crossings",
) -> Table:
    """Render a list of EdgeEvent objects as a rich Table."""
    table = Table(
        title=title,
        box=box.ROUNDED,
        header_style="bold white on grey23",
    )
    table.add_column("#", style="dim", width=5, justify="right")
    table.add_column("Timestamp", style="dim", min_width=20)
    table.add_column("Signal", style="cyan", min_width=80, no_wrap=True)
    table.add_column("Edge", min_width=10, justify="center")
    table.add_column("Value Before", justify="right", min_width=12)
    table.add_column("Value After", justify="right", min_width=12)

    for i, ev in enumerate(events, 1):
        if ev.edge_type == "rising":
            edge_text = Text("↑ RISING", style="bold red")
        else:
            edge_text = Text("↓ FALLING", style="bold green")

        table.add_row(
            str(i),
            format_timestamp(ev.timestamp, iso=iso),
            ev.signal,
            edge_text,
            f"{ev.value_before:.4g}",
            f"{ev.value_after:.4g}",
        )

    return table


# ─── Summary Panel ────────────────────────────────────────────────────────────

def render_summary(counts: dict[str, int], extra: Optional[str] = None) -> None:
    """Print a compact summary panel."""
    parts = [f"[white]{k}:[/white] [bold]{v}[/bold]" for k, v in counts.items()]
    if extra:
        parts.append(extra)
    console.print(Panel("  |  ".join(parts), title="Summary", border_style="dim"))


# ─── Paged View ───────────────────────────────────────────────────────────────

def paged_print(
    all_rows: list,
    render_fn,
    page_size: int = 40,
    iso: bool = False,
) -> None:
    """
    Generic paged display. render_fn(chunk, iso) → rich Table.
    Supports n/p navigation and q to quit.
    """
    total = len(all_rows)
    if total == 0:
        console.print("[yellow]No results.[/yellow]")
        return

    page = 0
    max_page = (total - 1) // page_size

    while True:
        console.clear()
        start = page * page_size
        end = min(start + page_size, total)
        chunk = all_rows[start:end]

        table = render_fn(chunk, iso=iso)
        # Annotate title with page info
        table.title = f"{table.title}  [dim][page {page+1}/{max_page+1}  •  {start+1}–{end} of {total}][/dim]"
        console.print(table)
        console.print("\n[dim]  n[/dim]=next  [dim]p[/dim]=prev  [dim]q[/dim]=quit")

        key = Prompt.ask("  >", default="n").strip().lower()
        if key in ("n", ""):
            page = min(page + 1, max_page)
        elif key == "p":
            page = max(page - 1, 0)
        elif key == "q":
            break

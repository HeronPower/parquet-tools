"""
schema_interactive — Interactively build schema.py signal lists from .dbc or Modbus files.

Usage:
  mise schema-interactive -- --dbc path/to/signals.dbc
  mise schema-interactive -- --modbus path/to/registers.csv
  mise schema-interactive -- --parquet path/to/sample.parquet  (schema from data directly)

Outputs:
  1. Rich table of detected signals and their proposed categories
  2. Python snippet to paste into schema.py KNOWN_* sets
  3. docs/fw_naming_conventions.md — feedback for the FW team
"""

import sys
import re
from pathlib import Path
from typing import Optional

import click

sys.path.insert(0, str(Path(__file__).parent))


# ─── DBC Parser ───────────────────────────────────────────────────────────────

def parse_dbc(dbc_path: Path) -> list[dict]:
    """
    Parse a .dbc file using cantools and return a flat list of signal dicts.

    Each dict: {frame_name, signal_name, full_name, length_bits, is_bool_candidate}
    """
    try:
        import cantools
    except ImportError:
        print("ERROR: cantools not installed. Run: uv sync --extra dev")
        sys.exit(1)

    db = cantools.database.load_file(str(dbc_path))
    signals = []
    for msg in db.messages:
        for sig in msg.signals:
            is_bool = sig.length == 1 or (
                sig.minimum == 0 and sig.maximum == 1
            )
            signals.append({
                "frame_name": msg.name,
                "signal_name": sig.name,
                "full_name": f"{msg.name}__{sig.name}",
                "length_bits": sig.length,
                "is_bool_candidate": is_bool,
                "unit": getattr(sig, "unit", "") or "",
                "comment": getattr(sig, "comment", "") or "",
            })
    return signals


def parse_modbus_csv(csv_path: Path) -> list[dict]:
    """
    Parse a Modbus register map CSV.
    Assumes columns: register_address, name, type, description (flexible).
    """
    import pandas as pd
    df = pd.read_csv(csv_path)
    df.columns = [c.lower().strip() for c in df.columns]

    name_col = next((c for c in df.columns if "name" in c), df.columns[0])
    type_col = next((c for c in df.columns if "type" in c), None)
    desc_col = next((c for c in df.columns if "desc" in c or "comment" in c), None)

    signals = []
    for _, row in df.iterrows():
        signals.append({
            "frame_name": "modbus",
            "signal_name": str(row[name_col]),
            "full_name": str(row[name_col]),
            "length_bits": None,
            "is_bool_candidate": str(row.get(type_col, "")).lower() in ("bool", "bit", "coil"),
            "unit": "",
            "comment": str(row[desc_col]) if desc_col else "",
        })
    return signals


# ─── Classification ───────────────────────────────────────────────────────────

from schema import (
    SignalCategory, ALERT_KEYWORDS, CONFIG_SUFFIXES, COMMAND_SUFFIXES,
    CONTROL_SUFFIXES, ALERT_FRAME_KEYWORDS,
)


def classify_dbc_signal(sig: dict) -> SignalCategory:
    """Classify a DBC/Modbus signal dict into a SignalCategory."""
    name_lower = sig["signal_name"].lower()
    frame_lower = sig["frame_name"].lower()
    comment_lower = sig.get("comment", "").lower()

    # Alert: parent frame keyword or signal name keyword + bool candidate
    if any(kw.lower() in frame_lower for kw in ALERT_FRAME_KEYWORDS):
        return SignalCategory.ALERT
    if any(kw in name_lower for kw in ALERT_KEYWORDS) and sig.get("is_bool_candidate"):
        return SignalCategory.ALERT
    if any(kw in comment_lower for kw in ["fault", "alert", "alarm"]) and sig.get("is_bool_candidate"):
        return SignalCategory.ALERT

    # Config / command / control by suffix
    if any(name_lower.endswith(s) for s in CONFIG_SUFFIXES):
        return SignalCategory.CONFIG
    if any(name_lower.endswith(s) for s in COMMAND_SUFFIXES):
        return SignalCategory.COMMAND
    if any(name_lower.endswith(s) for s in CONTROL_SUFFIXES):
        return SignalCategory.CONTROL

    if sig.get("is_bool_candidate"):
        return SignalCategory.UNKNOWN  # boolean but no alert keyword — needs review

    return SignalCategory.MEASUREMENT


# ─── Naming Convention Analysis ───────────────────────────────────────────────

def analyse_naming_conventions(signals: list[dict]) -> dict:
    """
    Analyse signal names for FW naming consistency.
    Returns a dict of findings to include in fw_naming_conventions.md.
    """
    findings = {
        "total": len(signals),
        "mixed_case_frames": [],
        "alert_without_keyword": [],
        "bool_without_alert_keyword": [],
        "inconsistent_suffixes": [],
        "suggested_renames": [],
    }

    for sig in signals:
        name = sig["signal_name"]
        frame = sig["frame_name"]

        # Mixed case in frame names
        if not (frame == frame.upper() or frame == frame.lower() or
                re.match(r'^[a-z][a-zA-Z0-9_]*$', frame)):
            findings["mixed_case_frames"].append(frame)

        # Boolean signals without standard alert keyword
        if sig.get("is_bool_candidate"):
            has_kw = any(kw in name.lower() for kw in ALERT_KEYWORDS)
            has_frame_kw = any(kw.lower() in frame.lower() for kw in ALERT_FRAME_KEYWORDS)
            if not has_kw and not has_frame_kw:
                findings["bool_without_alert_keyword"].append(f"{frame}__{name}")

    findings["mixed_case_frames"] = list(set(findings["mixed_case_frames"]))
    return findings


# ─── Output Generation ────────────────────────────────────────────────────────

def generate_schema_snippet(classified: list[tuple[dict, SignalCategory]]) -> str:
    """Generate Python code snippet to paste into schema.py."""
    by_cat: dict[SignalCategory, list[str]] = {}
    for sig, cat in classified:
        by_cat.setdefault(cat, []).append(sig["signal_name"].lower())

    lines = ["# ── Paste into src/schema.py ──────────────────────────────────────────\n"]
    cat_var = {
        SignalCategory.ALERT: "KNOWN_ALERT_SIGNALS",
        SignalCategory.MEASUREMENT: "KNOWN_MEASUREMENT_SIGNALS",
        SignalCategory.CONFIG: "KNOWN_CONFIG_SIGNALS",
        SignalCategory.COMMAND: "KNOWN_COMMAND_SIGNALS",
        SignalCategory.CONTROL: "KNOWN_CONTROL_SIGNALS",
    }
    for cat, var in cat_var.items():
        names = sorted(by_cat.get(cat, []))
        lines.append(f"{var}: set[str] = {{")
        for n in names:
            lines.append(f'    "{n}",')
        lines.append("}\n")

    return "\n".join(lines)


def generate_fw_report(signals: list[dict], classified: list[tuple[dict, SignalCategory]],
                       findings: dict) -> str:
    """Generate Markdown FW naming convention feedback."""
    lines = [
        "# FW Signal Naming Convention Feedback",
        "",
        f"Generated by `mise schema-interactive` — {findings['total']} signals analysed.",
        "",
        "## Summary",
        "",
        f"| Category | Count |",
        f"|----------|-------|",
    ]
    from collections import Counter
    cat_counts = Counter(cat.value for _, cat in classified)
    for cat, count in sorted(cat_counts.items()):
        lines.append(f"| {cat} | {count} |")

    lines += [
        "",
        "## Recommended Naming Conventions",
        "",
        "### Alert / Fault Signals (boolean)",
        "- Parent CAN frame name should contain `_ALERTS` or `_FAULTS` suffix",
        "- Signal name should contain one of: `fault`, `alert`, `oc` (overcurrent),",
        "  `uvlo`, `ovlo`, `overtemp`, `mia`, `lost`, `tripped`, `trip`",
        "- Signal should be 1-bit (boolean) in the DBC definition",
        "- Example: `dcacModulator_ALERTS_A_alerts` frame → `dcac_bot_overtemp` signal ✓",
        "",
        "### Measurement Signals (numeric sensor readbacks)",
        "- No specific suffix required; avoid alert keywords in the name",
        "- Include unit in DBC `unit` field, not in signal name",
        "",
        "### Config / Threshold Signals",
        "- Must end in `_cfg` or `_config`",
        "- Example: `fault_threshold_cfg` ✓, `fault_threshold` ✗",
        "",
        "### Command Signals",
        "- Must end in `_cmd` or `_command`",
        "",
        "### Control Signals (internal FW)",
        "- Must end in `_ctrl` or `_control`",
        "",
    ]

    if findings["bool_without_alert_keyword"]:
        lines += [
            "## ⚠ Boolean Signals Missing Alert Keywords",
            "",
            "These signals are 1-bit but have no alert keyword in name or frame — "
            "add to a `_ALERTS` frame or rename:",
            "",
        ]
        for s in sorted(set(findings["bool_without_alert_keyword"])):
            lines.append(f"- `{s}`")
        lines.append("")

    if findings["mixed_case_frames"]:
        lines += [
            "## ⚠ Inconsistent Frame Name Casing",
            "",
            "Use camelCase consistently for frame names:",
            "",
        ]
        for f in sorted(set(findings["mixed_case_frames"])):
            lines.append(f"- `{f}`")
        lines.append("")

    return "\n".join(lines)


# ─── CLI ──────────────────────────────────────────────────────────────────────

@click.command(
    name="schema-interactive",
    help="""Interactively build schema.py signal lists from a .dbc or Modbus file.

\b
Analyses signal names, classifies each into alert/measurement/config/command/control,
generates a Python snippet to paste into schema.py, and produces FW naming
convention feedback for your firmware team.

\b
Examples:
  mise schema-interactive -- --dbc path/to/signals.dbc
  mise schema-interactive -- --modbus path/to/registers.csv
  mise schema-interactive -- --parquet path/to/sample.parquet
""",
)
@click.option("--dbc", "dbc_path", default=None, help="Path to .dbc CAN database file.")
@click.option("--modbus", "modbus_path", default=None, help="Path to Modbus register CSV.")
@click.option("--parquet", "parquet_path", default=None,
              help="Path to a sample .parquet file (analyses column names directly).")
@click.option("--output-dir", default="docs", help="Where to write fw_naming_conventions.md [default: docs]")
def schema_interactive(dbc_path, modbus_path, parquet_path, output_dir):
    from rich.console import Console
    from rich.table import Table
    from rich.prompt import Confirm
    from rich import box

    c = Console()
    c.print("[bold cyan]Schema Interactive Builder[/bold cyan]\n")

    signals: list[dict] = []

    if dbc_path:
        c.print(f"[dim]Parsing DBC:[/dim] {dbc_path}")
        signals = parse_dbc(Path(dbc_path))
    elif modbus_path:
        c.print(f"[dim]Parsing Modbus CSV:[/dim] {modbus_path}")
        signals = parse_modbus_csv(Path(modbus_path))
    elif parquet_path:
        c.print(f"[dim]Analysing parquet schema:[/dim] {parquet_path}")
        from loader import load_parquet
        df, schema = load_parquet(Path(parquet_path))
        signals = [
            {
                "frame_name": info.parent_frame or "",
                "signal_name": info.name,
                "full_name": info.name,
                "length_bits": None,
                "is_bool_candidate": info.is_boolean,
                "unit": "",
                "comment": "",
            }
            for info in schema.columns.values()
            if info.category.value != "timestamp"
        ]
    else:
        c.print("[red]ERROR:[/red] Provide --dbc, --modbus, or --parquet.")
        sys.exit(1)

    c.print(f"[green]✓[/green] Found [bold]{len(signals)}[/bold] signals\n")

    # Classify all signals
    classified = [(sig, classify_dbc_signal(sig)) for sig in signals]

    # Render classification table
    table = Table(title="Signal Classification", box=box.ROUNDED,
                  header_style="bold white on grey23")
    table.add_column("Frame", style="dim", max_width=35)
    table.add_column("Signal", style="cyan", max_width=35)
    table.add_column("Category", min_width=12)
    table.add_column("Bool?", justify="center")

    cat_styles = {
        SignalCategory.ALERT: "bold red",
        SignalCategory.MEASUREMENT: "green",
        SignalCategory.CONFIG: "blue",
        SignalCategory.COMMAND: "magenta",
        SignalCategory.CONTROL: "yellow",
        SignalCategory.UNKNOWN: "bold yellow",
    }
    for sig, cat in classified:
        style = cat_styles.get(cat, "")
        table.add_row(
            sig["frame_name"],
            sig["signal_name"],
            f"[{style}]{cat.value}[/{style}]",
            "✓" if sig.get("is_bool_candidate") else "",
        )
    c.print(table)

    # Naming convention analysis
    findings = analyse_naming_conventions(signals)

    if findings["bool_without_alert_keyword"]:
        c.print(f"\n[bold yellow]⚠ {len(findings['bool_without_alert_keyword'])} boolean signal(s) lack alert keywords[/bold yellow]")

    # Generate and display schema.py snippet
    snippet = generate_schema_snippet(classified)
    c.print("\n[bold]Python snippet for schema.py:[/bold]")
    c.print(f"[dim]{snippet}[/dim]")

    # Write docs
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    fw_report = generate_fw_report(signals, classified, findings)
    fw_path = out_dir / "fw_naming_conventions.md"
    fw_path.write_text(fw_report)
    c.print(f"\n[green]✓[/green] FW feedback written to [cyan]{fw_path}[/cyan]")

    snippet_path = out_dir / "schema_snippet.py"
    snippet_path.write_text(snippet)
    c.print(f"[green]✓[/green] Schema snippet written to [cyan]{snippet_path}[/cyan]")
    c.print("[dim]Copy the contents of schema_snippet.py into src/schema.py KNOWN_* sets.[/dim]")


if __name__ == "__main__":
    schema_interactive()

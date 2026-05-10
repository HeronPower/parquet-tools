"""
plot_server — Browser-based interactive signal plots via FastAPI + Plotly.

Serves a live plot at http://localhost:5050 and also saves a standalone
HTML file that works offline (no server needed).

Plotly is used (not matplotlib) for interactive browser charts.
"""

import sys
import os
import webbrowser
import threading
from pathlib import Path
from typing import Optional

import click

sys.path.insert(0, str(Path(__file__).parent))

from loader import resolve_parquet_path, load_window, parse_window_args
from schema import SchemaInfo
from timeutils import format_timestamp


def _load_server_config() -> dict:
    """Load host/port/output_dir from config.toml if present."""
    config_path = Path("config.toml")
    if config_path.exists():
        try:
            import tomllib
            with open(config_path, "rb") as f:
                cfg = tomllib.load(f)
            return cfg.get("server", {})
        except Exception:
            pass
    return {}


def build_plotly_figure(df, schema: SchemaInfo, signal_names: list[str],
                        iso: bool = False, show_alerts: bool = False):
    """Build a plotly Figure object with traces for each requested signal."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    ts_col = schema.timestamp_col
    ts = df[ts_col]

    # Format x-axis labels
    if iso:
        x_vals = ts.apply(lambda v: format_timestamp(v, iso=True))
        x_title = "Timestamp (UTC)"
    else:
        x_vals = ts
        x_title = "Timestamp (Unix seconds)"

    # Separate alert cols (boolean step plot) from numeric (line)
    alert_cols = [c for c in signal_names if c in schema.alert_cols]
    numeric_cols = [c for c in signal_names if c in schema.numeric_cols]

    if show_alerts:
        alert_cols = schema.alert_cols

    n_rows = (1 if not alert_cols else 2) if numeric_cols else 1
    if alert_cols and not numeric_cols:
        n_rows = 1

    row_titles = []
    if numeric_cols:
        row_titles.append("Signals")
    if alert_cols:
        row_titles.append("Alerts / Faults")

    fig = make_subplots(
        rows=n_rows, cols=1,
        shared_xaxes=True,
        subplot_titles=row_titles,
        vertical_spacing=0.08,
    )

    # Numeric signal traces
    for i, col in enumerate(numeric_cols):
        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=df[col],
                name=col,
                mode="lines",
                line=dict(width=1.5),
            ),
            row=1, col=1,
        )

    # Alert traces (step plot, red/green fill)
    alert_row = 2 if numeric_cols else 1
    for col in alert_cols:
        y = df[col].fillna(0).astype(float)
        fig.add_trace(
            go.Scatter(
                x=x_vals,
                y=y,
                name=col,
                mode="lines",
                line=dict(shape="hv", width=1),  # step interpolation
                fill="tozeroy",
                fillcolor="rgba(220,50,50,0.15)",
                line_color="rgba(220,50,50,0.8)",
            ),
            row=alert_row, col=1,
        )

    fig.update_layout(
        title="Parquet Signal Plot",
        xaxis_title=x_title,
        hovermode="x unified",
        template="plotly_dark",
        height=600 if n_rows == 1 else 900,
        legend=dict(orientation="v", x=1.01),
    )

    return fig


@click.command(
    name="plot",
    help="""Launch browser-based interactive signal plot (FastAPI + Plotly).

\b
Also saves a standalone HTML file that works offline (no server needed).

\b
Examples:
  # Plot a single numeric signal
  mise plot -- --signal battery_voltage

  # Plot multiple signals
  mise plot -- --signals battery_voltage,motor_temp,dc_link_current

  # Plot all alert/fault signals in a time window
  mise plot -- --alerts --windowstart 1710403200 --windowend 1710406800

  # Plot with ISO timestamps and save to custom output path
  mise plot -- --signal bus_voltage --iso --output-dir ./reports

  # Skip the browser (just save HTML)
  mise plot -- --signal motor_temp --no-browser
""",
)
@click.option("--file", "file_path", envvar="PARQUET_FILE",
              help="Path to the .parquet file [env: PARQUET_FILE]")
@click.option("--signal", default=None,
              help="Single signal name to plot.")
@click.option("--signals", default=None,
              help="Comma-separated list of signal names to plot.")
@click.option("--alerts", is_flag=True, default=False,
              help="Include all detected alert/fault boolean signals in the plot.")
@click.option("--windowstart", default=None,
              help="Start of time window. Unix seconds or ISO 8601 UTC.")
@click.option("--windowend", default=None,
              help="End of time window. Unix seconds or ISO 8601 UTC.")
@click.option("--iso", is_flag=True, default=False,
              help="Use ISO 8601 UTC timestamps on x-axis.")
@click.option("--output-dir", default=None,
              help="Directory to save standalone HTML plot [default: ./plots]")
@click.option("--no-browser", is_flag=True, default=False,
              help="Skip opening the browser; just save the HTML file.")
@click.option("--port", default=None, type=int,
              help="Port for the live server [default: 5050 from config.toml]")
def plot_server(file_path, signal, signals, alerts, windowstart, windowend,
                iso, output_dir, no_browser, port):
    try:
        import plotly
        import plotly.io as pio
    except ImportError:
        print("ERROR: plotly not installed. Run: uv sync")
        sys.exit(1)

    server_cfg = _load_server_config()
    host = server_cfg.get("host", "127.0.0.1")
    port = port or server_cfg.get("port", 5050)
    out_dir = Path(output_dir or server_cfg.get("plot_output_dir", "./plots"))
    out_dir.mkdir(parents=True, exist_ok=True)

    path = resolve_parquet_path(file_path)
    start, end = parse_window_args(windowstart, windowend)

    from rich.console import Console
    c = Console()
    c.print(f"[dim]Loading[/dim] [cyan]{path.name}[/cyan] …")
    df, schema = load_window(path, start=start, end=end)

    if schema.timestamp_col is None:
        c.print("[red]ERROR:[/red] No timestamp column detected.")
        sys.exit(2)

    # Resolve requested signals
    requested: list[str] = []
    if signal:
        matched = [col for col in df.columns if signal.lower() in col.lower()]
        if not matched:
            c.print(f"[red]ERROR:[/red] Signal not found: {signal}")
            sys.exit(1)
        requested.extend(matched[:1])
    if signals:
        for s in signals.split(","):
            s = s.strip()
            matched = [col for col in df.columns if s.lower() in col.lower()]
            if matched:
                requested.extend(matched[:1])
            else:
                c.print(f"[yellow]Warning:[/yellow] Signal not found: {s}")

    if not requested and not alerts:
        # Default: plot first 5 numeric signals
        requested = schema.numeric_cols[:5]
        c.print(f"[dim]No signals specified — plotting first {len(requested)} numeric signal(s)[/dim]")

    fig = build_plotly_figure(df, schema, requested, iso=iso, show_alerts=alerts)

    # Save standalone HTML
    html_path = out_dir / f"{path.stem}_plot.html"
    pio.write_html(fig, file=str(html_path), include_plotlyjs="cdn", full_html=True)
    c.print(f"[green]✓[/green] Standalone HTML saved: [cyan]{html_path}[/cyan]")

    if no_browser:
        c.print("[dim]--no-browser set; skipping server.[/dim]")
        return

    # Launch FastAPI server
    try:
        from fastapi import FastAPI
        from fastapi.responses import HTMLResponse
        import uvicorn
    except ImportError:
        c.print("[yellow]FastAPI/uvicorn not installed — opening HTML file directly.[/yellow]")
        webbrowser.open(html_path.resolve().as_uri())
        return

    app = FastAPI(title="Parquet Plot Server")

    @app.get("/", response_class=HTMLResponse)
    async def root():
        return html_path.read_text()

    @app.get("/health")
    async def health():
        return {"status": "ok", "file": str(path)}

    url = f"http://{host}:{port}"
    c.print(f"[green]✓[/green] Serving at [cyan]{url}[/cyan]  (Ctrl+C to stop)")

    # Open browser after short delay
    def open_browser():
        import time
        time.sleep(0.8)
        webbrowser.open(url)

    threading.Thread(target=open_browser, daemon=True).start()

    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    plot_server()

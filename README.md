# Parquet Tools

CLI and browser tools for analysing embedded system logs stored as Parquet files.
Built for CAN bus / Modbus time-series data with fault/alert boolean signals.

## Quick Start

```bash
# 1. Install mise (once, any OS)
curl https://mise.run | sh   # or: brew install mise

# 2. Install Python + deps
mise install
mise run install

# 3. Point at your data (or use --file per command)
echo 'PARQUET_FILE=/path/to/your/logfile.parquet' > .env

# 4. Copy and fill in config
cp config.toml.example config.toml
```

## Tools

### `mise listalerts`
List fault/alert boolean transitions in chronological order.
Red = fault SET (0→1), Green = fault CLEAR (1→0).

```bash
mise listalerts -- --help

# Examples:
mise listalerts -- --file data.parquet
mise listalerts -- --filter 'overtemp.*' --set
mise listalerts -- --filter 'mv_.*' --clear --iso
mise listalerts -- --windowstart 1710403200 --windowend 1710406800 --set
```

### `mise whatchanged`
Find signals with sharp d(signal)/dt spikes around a timestamp.

```bash
mise whatchanged -- --help

# Examples:
mise whatchanged -- --before 1710403267.123
mise whatchanged -- --after 2024-03-14T08:00:00Z
mise whatchanged -- --before 1710403267 --threshold 3.0
```

### `mise whenchanged`
Find when a signal crossed a value threshold (rising or falling edge).

```bash
mise whenchanged -- --help

# Examples:
mise whenchanged -- --signal battery_voltage --beforevalue 48.0 --aftervalue 52.0
mise whenchanged -- --signal motor_temp --beforevalue 90.0 --aftervalue 80.0 --iso
```

### `mise plot`
Launch browser-based interactive plot (FastAPI + Plotly).
Also saves a standalone HTML file that works offline.

```bash
mise plot -- --help

# Examples:
mise plot -- --signal battery_voltage
mise plot -- --signals battery_voltage,motor_temp
mise plot -- --alerts --windowstart 1710403200
```

## Timestamp Format

All tools accept timestamps in either format:
- **Unix seconds** (default output): `1710403267.123`
- **ISO 8601 UTC**: `2024-03-14T08:01:07.123Z`

Add `--iso` to any command to switch output to ISO format.

## Schema Detection

Alert columns are detected automatically — any boolean column whose name or
parent CAN frame name contains `fault`, `alert`, or related keywords.

To build schema classification from your `.dbc` files and generate FW naming
convention feedback:

```bash
mise schema-interactive -- --dbc path/to/signals.dbc
# Outputs: docs/fw_naming_conventions.md + docs/schema_snippet.py
```

## Development

```bash
mise run test          # run all tests
mise run test-fast     # skip slow/NAS tests
mise run docs          # regenerate docs/tool_reference.md
mise run mount-check   # verify NAS path is reachable
```

## Data Files

Parquet log files are **never committed to this repo**.
Set `PARQUET_FILE` in `.env` or `nas_path` in `config.toml`.
See `config.toml.example` for all configuration options.

## Signal Naming Conventions

See [docs/fw_naming_conventions.md](docs/fw_naming_conventions.md) for the
signal naming standard and FW team guidance.

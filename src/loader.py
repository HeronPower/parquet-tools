"""
loader.py — Parquet file loading with time-window filtering and chunking.

All tools should load data through this module to ensure:
  - Consistent timestamp normalisation (always float64 Unix seconds)
  - Memory-efficient chunked reading for large files
  - NAS path resolution from config.toml / env
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional, Iterator

import pandas as pd
import pyarrow.parquet as pq

from timeutils import parse_timestamp, series_to_unix
from schema import analyse_schema, SchemaInfo


# ─── Path Resolution ──────────────────────────────────────────────────────────

def resolve_parquet_path(file_arg: Optional[str]) -> Path:
    """
    Resolve the parquet file path from (in priority order):
      1. --file argument
      2. PARQUET_FILE environment variable
      3. Abort with a helpful error
    """
    path_str = file_arg or os.environ.get("PARQUET_FILE", "")
    if not path_str:
        print(
            "ERROR: No parquet file specified.\n"
            "  Pass --file path/to/data.parquet\n"
            "  or set PARQUET_FILE in .env / environment",
            file=sys.stderr,
        )
        sys.exit(1)

    path = Path(path_str)
    if not path.exists():
        print(f"ERROR: File not found: {path}", file=sys.stderr)
        sys.exit(1)

    return path


# ─── Full Load ────────────────────────────────────────────────────────────────

def load_parquet(
    path: Path,
    columns: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, SchemaInfo]:
    """
    Load an entire parquet file into a DataFrame.

    Returns (df, schema) where:
      - df has timestamps normalised to float64 Unix seconds in the
        original timestamp column
      - schema describes column classifications
    """
    table = pq.read_table(path, columns=columns)
    df = table.to_pandas()
    schema = analyse_schema(df)

    if schema.timestamp_col:
        df[schema.timestamp_col] = series_to_unix(df[schema.timestamp_col])
        df = df.sort_values(schema.timestamp_col).reset_index(drop=True)

    return df, schema


def load_window(
    path: Path,
    start: Optional[float] = None,
    end: Optional[float] = None,
    columns: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, SchemaInfo]:
    """
    Load a parquet file filtered to a UTC time window [start, end].

    start / end: Unix seconds (float). None = no bound.

    For large files this still reads the full file — for row-group
    pushdown you'd need the timestamp column to be the partition key.
    Use chunked_load() for very large files.
    """
    df, schema = load_parquet(path, columns=columns)

    if schema.timestamp_col is None:
        return df, schema

    ts = df[schema.timestamp_col]
    mask = pd.Series([True] * len(df), index=df.index)

    if start is not None:
        mask &= ts >= start
    if end is not None:
        mask &= ts <= end

    return df[mask].reset_index(drop=True), schema


# ─── Chunked Load ─────────────────────────────────────────────────────────────

def chunked_load(
    path: Path,
    chunk_rows: int = 100_000,
    start: Optional[float] = None,
    end: Optional[float] = None,
    columns: Optional[list[str]] = None,
) -> Iterator[tuple[pd.DataFrame, SchemaInfo]]:
    """
    Iterate over a parquet file in chunks. Yields (chunk_df, schema).

    schema is computed once from the first chunk and reused.
    Use for large files where loading everything into memory is impractical.
    """
    pf = pq.ParquetFile(path)
    schema: Optional[SchemaInfo] = None

    for batch in pf.iter_batches(batch_size=chunk_rows, columns=columns):
        chunk = batch.to_pandas()

        if schema is None:
            schema = analyse_schema(chunk)
            if schema.timestamp_col:
                chunk[schema.timestamp_col] = series_to_unix(chunk[schema.timestamp_col])
        else:
            if schema.timestamp_col:
                chunk[schema.timestamp_col] = series_to_unix(chunk[schema.timestamp_col])

        if schema.timestamp_col:
            ts = chunk[schema.timestamp_col]
            mask = pd.Series([True] * len(chunk), index=chunk.index)
            if start is not None:
                mask &= ts >= start
            if end is not None:
                mask &= ts <= end
            chunk = chunk[mask]

        if len(chunk):
            yield chunk.sort_values(schema.timestamp_col).reset_index(drop=True), schema


# ─── Convenience: parse CLI timestamp args ────────────────────────────────────

def parse_window_args(
    windowstart: Optional[str],
    windowend: Optional[str],
) -> tuple[Optional[float], Optional[float]]:
    """Parse --windowstart / --windowend CLI args to Unix seconds."""
    start = parse_timestamp(windowstart) if windowstart else None
    end = parse_timestamp(windowend) if windowend else None
    return start, end

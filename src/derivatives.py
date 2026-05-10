"""
derivatives.py — d(signal)/dt computation and sharp change detection.

The term 'dsdt' is used throughout (d of any signal / dt) — not voltage-specific.

Sharp change detection algorithm:
  1. Compute dsdt[i] = (signal[i+1] - signal[i]) / (t[i+1] - t[i])
  2. baseline = mean(|dsdt|) for the signal over the analysis window
  3. A sample is a "spike" if |dsdt[i]| > threshold * baseline
  4. Consecutive spike samples are merged into a single change event

Results are returned as plain dicts/dataframes — no display logic here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


# ─── Data Structures ──────────────────────────────────────────────────────────

@dataclass
class ChangeEvent:
    """A detected sharp change in a signal."""
    signal: str
    timestamp_start: float     # Unix seconds, start of transition
    timestamp_end: float       # Unix seconds, end of transition (often same row)
    value_before: float
    value_after: float
    dsdt_magnitude: float      # |d(signal)/dt| at the peak of the change
    baseline_dsdt: float       # mean |dsdt| for this signal (context)
    spike_ratio: float         # dsdt_magnitude / baseline_dsdt

    @property
    def delta(self) -> float:
        return self.value_after - self.value_before

    def to_dict(self) -> dict:
        return {
            "signal": self.signal,
            "timestamp_start": self.timestamp_start,
            "timestamp_end": self.timestamp_end,
            "value_before": self.value_before,
            "value_after": self.value_after,
            "delta": self.delta,
            "dsdt_magnitude": self.dsdt_magnitude,
            "baseline_dsdt": self.baseline_dsdt,
            "spike_ratio": self.spike_ratio,
        }


@dataclass
class EdgeEvent:
    """A detected value threshold crossing (for whenchanged)."""
    signal: str
    timestamp: float
    value_before: float
    value_after: float
    edge_type: str             # "rising" or "falling"


# ─── dsdt Computation ─────────────────────────────────────────────────────────

def compute_dsdt(
    signal: pd.Series,
    time: pd.Series,
) -> pd.Series:
    """
    Compute d(signal)/dt element-wise.

    Uses (signal[i+1] - signal[i]) / (t[i+1] - t[i]) for irregular timestamps.
    Returns a Series aligned to the input index; last element is NaN.
    """
    sig_vals = signal.to_numpy(dtype=float)
    t_vals = time.to_numpy(dtype=float)

    dt = np.diff(t_vals)
    ds = np.diff(sig_vals)

    # Avoid division by zero for duplicate timestamps
    with np.errstate(divide="ignore", invalid="ignore"):
        dsdt = np.where(dt != 0, ds / dt, 0.0)

    # Pad to original length (last value is NaN — no future point to diff against)
    padded = np.append(dsdt, np.nan)
    return pd.Series(padded, index=signal.index, name=f"dsdt_{signal.name}")


def compute_baseline(dsdt: pd.Series) -> float:
    """
    Compute the baseline as mean(|dsdt|), ignoring NaN.
    Returns 0.0 if the signal is flat (no variation).
    """
    abs_vals = dsdt.abs().dropna()
    if len(abs_vals) == 0:
        return 0.0
    return float(abs_vals.mean())


# ─── Sharp Change Detection ───────────────────────────────────────────────────

def find_sharp_changes(
    df: pd.DataFrame,
    signal_col: str,
    time_col: str,
    threshold: float = 2.0,
    start_time: Optional[float] = None,
    end_time: Optional[float] = None,
) -> list[ChangeEvent]:
    """
    Find all sharp changes in a single signal column.

    threshold: spike = |dsdt| > threshold * baseline
    start_time / end_time: narrow the analysis window (Unix seconds)

    Returns a list of ChangeEvent sorted by timestamp.
    """
    df_work = df[[time_col, signal_col]].dropna().sort_values(time_col).reset_index(drop=True)

    # Compute baseline over the full loaded window
    dsdt = compute_dsdt(df_work[signal_col], df_work[time_col])
    baseline = compute_baseline(dsdt)

    # No variation → nothing to report
    if baseline == 0.0:
        return []

    # Now restrict to the requested window for reporting
    mask = pd.Series([True] * len(df_work))
    if start_time is not None:
        mask &= df_work[time_col] >= start_time
    if end_time is not None:
        mask &= df_work[time_col] <= end_time

    df_window = df_work[mask].reset_index(drop=True)
    dsdt_window = dsdt[mask].reset_index(drop=True)

    spike_mask = dsdt_window.abs() > threshold * baseline

    events: list[ChangeEvent] = []
    i = 0
    while i < len(df_window):
        if spike_mask.iloc[i]:
            # Merge consecutive spike samples into one event
            j = i
            while j < len(df_window) - 1 and spike_mask.iloc[j + 1]:
                j += 1

            t_start = float(df_window[time_col].iloc[i])
            t_end = float(df_window[time_col].iloc[j])
            v_before = float(df_window[signal_col].iloc[i])
            v_after = float(df_window[signal_col].iloc[j])
            peak_dsdt = float(dsdt_window.iloc[i:j+1].abs().max())

            events.append(ChangeEvent(
                signal=signal_col,
                timestamp_start=t_start,
                timestamp_end=t_end,
                value_before=v_before,
                value_after=v_after,
                dsdt_magnitude=peak_dsdt,
                baseline_dsdt=baseline,
                spike_ratio=peak_dsdt / baseline if baseline > 0 else float("inf"),
            ))
            i = j + 1
        else:
            i += 1

    return events


def find_all_sharp_changes(
    df: pd.DataFrame,
    signal_cols: list[str],
    time_col: str,
    threshold: float = 2.0,
    start_time: Optional[float] = None,
    end_time: Optional[float] = None,
) -> list[ChangeEvent]:
    """
    Run sharp change detection across multiple signals.
    Returns all events merged and sorted chronologically.
    """
    all_events: list[ChangeEvent] = []
    for col in signal_cols:
        try:
            events = find_sharp_changes(df, col, time_col, threshold, start_time, end_time)
            all_events.extend(events)
        except Exception:
            # Skip columns that fail (non-numeric, all-NaN, etc.)
            pass

    return sorted(all_events, key=lambda e: e.timestamp_start)


# ─── Edge Detection (whenchanged) ─────────────────────────────────────────────

def find_edge(
    df: pd.DataFrame,
    signal_col: str,
    time_col: str,
    before_value: float,
    after_value: float,
    debounce_samples: int = 3,
    start_time: Optional[float] = None,
) -> list[EdgeEvent]:
    """
    Find threshold crossings in a signal.

    If before_value < after_value: rising edge (signal goes from ≤before to ≥after)
    If before_value > after_value: falling edge (signal goes from ≥before to ≤after)

    debounce_samples: the signal must stay on the far side of the threshold
                      for this many samples to be reported as a real transition.

    start_time: begin search from this Unix timestamp (inclusive).
    """
    df_work = df[[time_col, signal_col]].dropna().sort_values(time_col).reset_index(drop=True)

    if start_time is not None:
        df_work = df_work[df_work[time_col] >= start_time].reset_index(drop=True)

    vals = df_work[signal_col].to_numpy(dtype=float)
    times = df_work[time_col].to_numpy(dtype=float)

    rising = before_value < after_value
    edge_type = "rising" if rising else "falling"

    events: list[EdgeEvent] = []
    in_before_zone = False
    candidate_idx: Optional[int] = None

    for i, v in enumerate(vals):
        if rising:
            in_before_zone = v <= before_value
        else:
            in_before_zone = v >= before_value

        if in_before_zone:
            candidate_idx = i

        if candidate_idx is not None:
            if rising and v >= after_value:
                # Debounce: check subsequent samples stay above threshold
                if i + debounce_samples < len(vals):
                    sustained = all(vals[i:i + debounce_samples] >= after_value)
                else:
                    sustained = True  # near end of data
                if sustained:
                    events.append(EdgeEvent(
                        signal=signal_col,
                        timestamp=float(times[i]),
                        value_before=float(vals[candidate_idx]),
                        value_after=float(v),
                        edge_type=edge_type,
                    ))
                    candidate_idx = None
                    in_before_zone = False

            elif not rising and v <= after_value:
                if i + debounce_samples < len(vals):
                    sustained = all(vals[i:i + debounce_samples] <= after_value)
                else:
                    sustained = True
                if sustained:
                    events.append(EdgeEvent(
                        signal=signal_col,
                        timestamp=float(times[i]),
                        value_before=float(vals[candidate_idx]),
                        value_after=float(v),
                        edge_type=edge_type,
                    ))
                    candidate_idx = None
                    in_before_zone = False

    return events

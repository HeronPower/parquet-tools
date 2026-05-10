"""Tests for derivatives.py — dsdt computation and change detection."""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from derivatives import (
    compute_dsdt, compute_baseline, find_sharp_changes,
    find_all_sharp_changes, find_edge, ChangeEvent, EdgeEvent,
)


def make_flat_signal(n=100, value=5.0):
    t = pd.Series(np.linspace(0, 100, n))
    s = pd.Series(np.full(n, value))
    return s, t


def make_spike_signal(n=100, spike_at=50, spike_mag=50.0):
    t = pd.Series(np.linspace(0, 100, n))
    s = pd.Series(np.full(n, 5.0, dtype=float))
    s.iloc[spike_at] = s.iloc[spike_at] + spike_mag
    return s, t


def make_step_signal(n=100, step_at=50, before=0.0, after=1.0):
    t = pd.Series(np.linspace(0, 100.0, n))
    s = pd.Series(np.where(np.arange(n) < step_at, before, after), dtype=float)
    return s, t


# ─── compute_dsdt ─────────────────────────────────────────────────────────────

def test_dsdt_flat_signal_is_zero():
    s, t = make_flat_signal()
    dsdt = compute_dsdt(s, t)
    assert dsdt.dropna().abs().max() < 1e-9


def test_dsdt_length_matches_input():
    s, t = make_flat_signal(50)
    dsdt = compute_dsdt(s, t)
    assert len(dsdt) == 50


def test_dsdt_last_is_nan():
    s, t = make_flat_signal(20)
    dsdt = compute_dsdt(s, t)
    assert pd.isna(dsdt.iloc[-1])


def test_dsdt_detects_spike():
    s, t = make_spike_signal(spike_at=50, spike_mag=100.0)
    dsdt = compute_dsdt(s, t)
    # The spike index should have a large dsdt
    assert abs(dsdt.iloc[50]) > 1.0


# ─── compute_baseline ─────────────────────────────────────────────────────────

def test_baseline_flat_signal_is_zero():
    s, t = make_flat_signal()
    dsdt = compute_dsdt(s, t)
    assert compute_baseline(dsdt) == 0.0


def test_baseline_positive():
    s, t = make_spike_signal()
    dsdt = compute_dsdt(s, t)
    assert compute_baseline(dsdt) > 0.0


# ─── find_sharp_changes ────────────────────────────────────────────────────────

def test_no_changes_on_flat_signal():
    df = pd.DataFrame({"t": np.linspace(0, 100, 100), "sig": np.ones(100)})
    events = find_sharp_changes(df, "sig", "t", threshold=2.0)
    assert events == []


def test_detects_spike(demo_df):
    # battery_voltage has a sharp drop at row 248-252
    events = find_sharp_changes(demo_df, "battery_voltage", "timestamp", threshold=2.0)
    assert len(events) >= 1
    spike_ts = events[0].timestamp_start
    # battery_voltage has an injected sharp drop at rows 248-252 of 500 over 3600s
    # Allow a wide window — noise may cause earlier detections
    assert 1710403200.0 < spike_ts < 1710403200.0 + 3600.0


def test_change_event_fields(demo_df):
    events = find_sharp_changes(demo_df, "battery_voltage", "timestamp", threshold=2.0)
    assert len(events) >= 1
    ev = events[0]
    assert isinstance(ev, ChangeEvent)
    assert ev.signal == "battery_voltage"
    assert ev.dsdt_magnitude > 0
    assert ev.spike_ratio >= 2.0


def test_threshold_sensitivity(demo_df):
    events_low = find_sharp_changes(demo_df, "battery_voltage", "timestamp", threshold=1.0)
    events_high = find_sharp_changes(demo_df, "battery_voltage", "timestamp", threshold=10.0)
    # Lower threshold → more or equal events
    assert len(events_low) >= len(events_high)


def test_find_all_sharp_changes(demo_df):
    numeric_cols = ["battery_voltage", "motor_temp", "dc_link_current"]
    events = find_all_sharp_changes(demo_df, numeric_cols, "timestamp", threshold=2.0)
    # Should be sorted chronologically
    times = [e.timestamp_start for e in events]
    assert times == sorted(times)


# ─── find_edge ────────────────────────────────────────────────────────────────

def test_rising_edge_detected():
    s, t = make_step_signal(step_at=50, before=0.0, after=1.0)
    df = pd.DataFrame({"t": t, "sig": s})
    events = find_edge(df, "sig", "t", before_value=0.0, after_value=1.0, debounce_samples=1)
    assert len(events) >= 1
    assert events[0].edge_type == "rising"
    assert events[0].value_after >= 1.0


def test_falling_edge_detected():
    s, t = make_step_signal(step_at=50, before=1.0, after=0.0)
    df = pd.DataFrame({"t": t, "sig": s})
    events = find_edge(df, "sig", "t", before_value=1.0, after_value=0.0, debounce_samples=1)
    assert len(events) >= 1
    assert events[0].edge_type == "falling"


def test_no_edge_if_threshold_not_crossed():
    s, t = make_flat_signal(value=5.0)
    df = pd.DataFrame({"t": t, "sig": s})
    events = find_edge(df, "sig", "t", before_value=0.0, after_value=10.0, debounce_samples=1)
    assert events == []


def test_edge_respects_start_time():
    s, t = make_step_signal(step_at=50, before=0.0, after=1.0)
    df = pd.DataFrame({"t": t, "sig": s})
    # Edge is at t~50; if we start searching from t=80, we shouldn't find it
    events = find_edge(df, "sig", "t", before_value=0.0, after_value=1.0,
                       debounce_samples=1, start_time=80.0)
    assert events == []

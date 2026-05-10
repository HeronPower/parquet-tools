"""Tests for schema.py column classification."""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from schema import (
    analyse_schema, SignalCategory, detect_timestamp_col,
    filter_alert_cols_by_pattern, _is_boolean_column,
)


def test_timestamp_detection(demo_df):
    ts_col = detect_timestamp_col(demo_df)
    assert ts_col == "timestamp"


def test_alert_classification_by_signal_name(demo_df):
    schema = analyse_schema(demo_df)
    # overtemp and uvlo signal names contain alert keywords
    assert schema.columns["overtemp_dcdc_bot"].category == SignalCategory.ALERT
    assert schema.columns["mv_aux_uvlo"].category == SignalCategory.ALERT
    assert schema.columns["cell_mia"].category == SignalCategory.ALERT


def test_alert_classification_by_frame_prefix(demo_df_prefixed):
    schema = analyse_schema(demo_df_prefixed)
    alert_cols = schema.alert_cols
    # All three alert columns should be detected regardless of prefix
    assert any("overtemp_dcdc_bot" in c for c in alert_cols)
    assert any("mv_aux_uvlo" in c for c in alert_cols)
    assert any("cell_mia" in c for c in alert_cols)


def test_numeric_signals_are_measurements(demo_df):
    schema = analyse_schema(demo_df)
    assert schema.columns["battery_voltage"].category == SignalCategory.MEASUREMENT
    assert schema.columns["motor_temp"].category == SignalCategory.MEASUREMENT
    assert schema.columns["dc_link_current"].category == SignalCategory.MEASUREMENT


def test_is_boolean_column():
    assert _is_boolean_column(pd.Series([0, 1, 0, 1]))
    assert _is_boolean_column(pd.Series([0.0, 1.0, 0.0]))
    assert not _is_boolean_column(pd.Series([0, 1, 2]))
    assert not _is_boolean_column(pd.Series([3.14, 2.71]))


def test_alert_cols_list(demo_df):
    schema = analyse_schema(demo_df)
    assert len(schema.alert_cols) >= 3
    assert "battery_voltage" not in schema.alert_cols


def test_filter_alert_cols_regex(demo_df):
    schema = analyse_schema(demo_df)
    filtered = filter_alert_cols_by_pattern(schema, r"overtemp.*")
    assert "overtemp_dcdc_bot" in filtered
    assert "mv_aux_uvlo" not in filtered


def test_filter_alert_cols_no_match(demo_df):
    schema = analyse_schema(demo_df)
    filtered = filter_alert_cols_by_pattern(schema, r"nonexistent_xyz")
    assert filtered == []


def test_config_suffix_detection():
    df = pd.DataFrame({
        "timestamp": [1.0, 2.0],
        "fault_threshold_cfg": [100.0, 100.0],
        "motor_speed_cmd": [50.0, 60.0],
        "fan_enable_ctrl": [0, 1],
    })
    schema = analyse_schema(df)
    assert schema.columns["fault_threshold_cfg"].category == SignalCategory.CONFIG
    assert schema.columns["motor_speed_cmd"].category == SignalCategory.COMMAND
    assert schema.columns["fan_enable_ctrl"].category == SignalCategory.CONTROL

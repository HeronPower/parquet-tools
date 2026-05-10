"""
conftest.py — Pytest fixtures.

Demo parquet data is generated in-memory (never written to disk in CI).
For tests against real NAS data, use the @pytest.mark.slow marker and
set PARQUET_FILE in your environment.
"""

import pytest
import numpy as np
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def make_demo_df(n_rows: int = 500, seed: int = 42) -> pd.DataFrame:
    """
    Generate a realistic demo DataFrame with:
      - Unix timestamp column
      - Several numeric measurement signals
      - Several boolean alert signals (named to trigger auto-detection)
      - One sharp change event in battery_voltage around row 250
    """
    rng = np.random.default_rng(seed)
    t_start = 1710403200.0  # 2024-03-14 08:00:00 UTC
    t_end = t_start + 3600  # 1 hour
    timestamps = np.linspace(t_start, t_end, n_rows)

    # Numeric signals
    battery_voltage = 48.0 + rng.normal(0, 0.1, n_rows)
    # Sharp change at row 250: voltage drops sharply
    battery_voltage[248:252] = [48.0, 44.0, 38.0, 36.5]

    motor_temp = 60.0 + rng.normal(0, 0.5, n_rows)
    dc_link_current = 10.0 + rng.normal(0, 0.2, n_rows)

    # Boolean alert signals (parent frame naming convention)
    # Naming: bare signal names; tests can also test with frame-prefix naming
    overtemp_dcdc_bot = np.zeros(n_rows, dtype=int)
    overtemp_dcdc_bot[300:320] = 1   # fault active for 20 samples

    mv_aux_uvlo = np.zeros(n_rows, dtype=int)
    mv_aux_uvlo[100] = 1
    mv_aux_uvlo[101] = 1
    mv_aux_uvlo[150] = 1

    cell_mia = np.zeros(n_rows, dtype=int)

    return pd.DataFrame({
        "timestamp": timestamps,
        "battery_voltage": battery_voltage,
        "motor_temp": motor_temp,
        "dc_link_current": dc_link_current,
        # Alert columns — names contain alert keywords for auto-detection
        "overtemp_dcdc_bot": overtemp_dcdc_bot,
        "mv_aux_uvlo": mv_aux_uvlo,
        "cell_mia": cell_mia,
    })


def make_demo_df_with_frame_prefix(n_rows: int = 500) -> pd.DataFrame:
    """
    Demo DataFrame where alert columns have CAN frame prefix encoding:
      cabinet__dcacCascadedControls_ALERTS__cell_mia
    Tests that parent frame keyword detection works.
    """
    base = make_demo_df(n_rows)
    rename = {
        "overtemp_dcdc_bot": "cell6__measurement_ALERTS_A_alerts__overtemp_dcdc_bot",
        "mv_aux_uvlo": "cell6__circuitController_ALERTS_B_alerts__mv_aux_uvlo",
        "cell_mia": "cabinet__dcacCascadedControls_ALERTS__cell_mia",
    }
    return base.rename(columns=rename)


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def demo_df():
    return make_demo_df()


@pytest.fixture
def demo_df_prefixed():
    return make_demo_df_with_frame_prefix()


@pytest.fixture
def demo_schema(demo_df):
    from schema import analyse_schema
    return analyse_schema(demo_df)


@pytest.fixture
def demo_schema_prefixed(demo_df_prefixed):
    from schema import analyse_schema
    return analyse_schema(demo_df_prefixed)

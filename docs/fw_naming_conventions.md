# FW Signal Naming Convention Guide

> This document is auto-updated by `mise schema-interactive` when you provide
> a `.dbc` or Modbus register file. The section below is the baseline standard;
> run schema-interactive to generate signal-specific feedback.

---

## Signal Categories

Every signal in the CAN/Modbus interface must belong to exactly one category.
The tooling uses signal name and frame name to auto-classify signals.

| Category | Description | Required naming |
|----------|-------------|-----------------|
| **alert** | Boolean 0/1 — fault or alert active | Parent frame ends in `_ALERTS` or `_FAULTS`, OR signal name contains an alert keyword (see below) |
| **measurement** | Numeric sensor readback | No specific suffix required |
| **config** | Fixed FW threshold or parameter | Signal name ends in `_cfg` or `_config` |
| **command** | User/operator command | Signal name ends in `_cmd` or `_command` |
| **control** | Internal FW control signal | Signal name ends in `_ctrl` or `_control` |

---

## Alert Signal Naming Rules

### Rule 1: Parent Frame Naming
If a CAN message/frame contains only alert/fault signals, its name **must**
end in `_ALERTS` or `_FAULTS`. The tooling uses this to automatically
classify all child signals as alerts regardless of their individual names.

✓ `dcacCascadedControls_ALERTS`
✓ `measurement_ALERTS_A_alerts`
✗ `dcacCascadedControls` (tooling cannot auto-detect children as alerts)

### Rule 2: Signal Name Keywords
For alert signals in mixed frames, the signal name **must** contain one of:

| Keyword | Meaning |
|---------|---------|
| `fault` | Generic fault |
| `alert` | Generic alert |
| `alarm` | Alarm condition |
| `oc` | Overcurrent |
| `uvlo` | Undervoltage lockout |
| `ovlo` | Overvoltage lockout |
| `uvp` | Undervoltage protection |
| `ovp` | Overvoltage protection |
| `overtemp` | Over-temperature |
| `mia` | Missing in action (communication loss) |
| `lost` | Communication / signal lost |
| `tripped` | Protection circuit tripped |
| `trip` | Trip event |

### Rule 3: Boolean Encoding
All alert signals **must** be 1-bit in the DBC definition:
- `1` = fault/alert ACTIVE
- `0` = fault/alert INACTIVE (normal)

✓ `cell_mia` (1-bit, in `dcacCascadedControls_ALERTS` frame)
✗ `cell_status` (ambiguous — rename to `cell_mia` or put in `_ALERTS` frame)

---

## Measurement Signal Naming

- Use descriptive names without alert keywords
- Include the physical unit in the DBC `unit` field, NOT in the signal name
- Use snake_case

✓ `battery_voltage` (unit: V in DBC)
✓ `motor_temperature` (unit: °C in DBC)
✗ `battery_voltage_v` (unit suffix in name is redundant)
✗ `motorTempSensor` (camelCase — use snake_case)

---

## Config / Command / Control Naming

| Type | Suffix | Example |
|------|--------|---------|
| Config | `_cfg` | `fault_threshold_cfg` |
| Command | `_cmd` | `motor_speed_cmd` |
| Control | `_ctrl` | `fan_enable_ctrl` |

Signals of these types should not be in `_ALERTS` frames.

---

## Hierarchy Encoding in Column Names

When parquet files are generated, the CAN frame hierarchy is encoded as a
column name prefix using `__` as separator:

```
{top_node}__{frame_name}__{signal_name}
```

Example:
```
cabinet__dcacCascadedControls_ALERTS__cell_mia
cell6__circuitController_ALERTS_B_alerts__mv_aux_uvlo
cell6__measurement_ALERTS_A_alerts__overtemp_dcdc_bot
```

**The tooling relies on this `__` separator being consistent.**
If your data pipeline uses a different separator (`.`, `/`, `:`), update
`_extract_parent_frame()` in `src/schema.py` accordingly.

---

## Signals Requiring Review

> This section is populated by `mise schema-interactive` when run against
> your actual .dbc files. It lists signals that are boolean but lack alert
> keywords — these need to be either renamed or placed in an `_ALERTS` frame.

*Run `mise schema-interactive -- --dbc your_signals.dbc` to populate this section.*

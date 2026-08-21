"""Kavach 22-Aug BT checkpoint constants — research only."""
from __future__ import annotations

from datetime import date

DATE_FROM = date(2026, 7, 22)
DATE_TO = date(2026, 8, 21)

EXCLUDED_SYMBOLS = frozenset({"EXIDEIND", "NUVAMA", "SAMMAANCAP"})

# BT-1
PULLBACK_HARD_BLOCK_N = 5
EMA5_LEN = 5
EMA10_LEN = 10

# BT-2
PIVOT_ZONE_PCT = 0.2  # ±0.2% around pivot
PIVOT_LEFT = 2
PIVOT_RIGHT = 2
CLUSTER_MIN_INTERACTIONS = 2
DANGER_ZONE_PCT = 0.35  # entry within this % of pivot counts as nearby

# BT-3
DYNAMIC_TRAIL_ARM_R = 2.0
DYNAMIC_TRAIL_STEP_R = 1.0  # trail stop by 1R for every additional 1R
FORCE_EXIT_HM = (15, 15)  # Rule 27

# BT-4
GARUDA_TOP_N = 6

RUN_ID_PREFIX = "kavach_bt_22aug"

# Rule ID labels (research tagging only)
RULE_15_ENTRY = "R15_ema5_pullback_entry"
RULE_24_GARUDA_SHADOW = "R24_garuda_shadow"
RULE_25_RESISTANCE_WARN = "R25_resistance_warning"
RULE_26_DYNAMIC_TRAIL = "R26_dynamic_trail_2r"
RULE_27_FORCE_1515 = "R27_force_exit_1515"
RULE_28_PB_HARD_BLOCK = "R28_pullback_5plus_hard_block"

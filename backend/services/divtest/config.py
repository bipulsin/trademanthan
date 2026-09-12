"""Runtime settings for divtest."""
from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = Path(os.getenv("DIVTEST_DATA_DIR", str(ROOT / "data" / "divtest")))
CACHE_DIR = Path(os.getenv("DIVTEST_CACHE_DIR", str(ROOT / "data" / "divtest_cache")))

TIMEFRAMES = (
    {"id": "10min", "interval": "minutes/10", "label": "10 min"},
    {"id": "15min", "interval": "minutes/15", "label": "15 min"},
    {"id": "1hr", "interval": "hours/1", "label": "1 hour"},
)

_DEFAULTS: Dict[str, Any] = {
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal": 9,
    "divergence_lookback": 25,
    "histogram_flip_window": 10,
    "entry_mode": "next_open",  # next_open | same_close
    "exit_opposite_flip": True,
    "exit_stop_loss": True,
    "exit_time_based": True,
    "atr_period": 14,
    "atr_stop_multiplier": 1.5,
    "atr_target_multiplier": 2.5,
    "max_holding_bars": 40,
    "brokerage_per_side": 20.0,
    "brokerage_enabled": True,
    "equity_default_qty": 1,
    "swing_order": 3,
    "demo_mode": os.getenv("DIVTEST_DEMO_MODE", "").lower() in ("1", "true", "yes"),
}

_runtime: Dict[str, Any] = deepcopy(_DEFAULTS)


def get_settings() -> Dict[str, Any]:
    return deepcopy(_runtime)


def update_settings(partial: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if not partial:
        return get_settings()
    for key, val in partial.items():
        snake = _to_snake(key)
        if snake not in _DEFAULTS:
            continue
        default = _DEFAULTS[snake]
        if isinstance(default, bool):
            _runtime[snake] = bool(val)
        elif isinstance(default, int) and not isinstance(default, bool):
            _runtime[snake] = int(val)
        elif isinstance(default, float):
            _runtime[snake] = float(val)
        else:
            _runtime[snake] = val
    return get_settings()


def reset_settings() -> Dict[str, Any]:
    _runtime.clear()
    _runtime.update(deepcopy(_DEFAULTS))
    return get_settings()


def _to_snake(key: str) -> str:
    if "_" in key:
        return key
    out = []
    for ch in key:
        if ch.isupper():
            out.append("_")
            out.append(ch.lower())
        else:
            out.append(ch)
    return "".join(out).lstrip("_")


def settings_public(s: Dict[str, Any] | None = None) -> Dict[str, Any]:
    src = s or get_settings()
    return {
        "macdFast": src["macd_fast"],
        "macdSlow": src["macd_slow"],
        "macdSignal": src["macd_signal"],
        "divergenceLookback": src["divergence_lookback"],
        "histogramFlipWindow": src["histogram_flip_window"],
        "entryMode": src["entry_mode"],
        "exitOppositeFlip": src["exit_opposite_flip"],
        "exitStopLoss": src["exit_stop_loss"],
        "exitTimeBased": src["exit_time_based"],
        "atrPeriod": src["atr_period"],
        "atrStopMultiplier": src["atr_stop_multiplier"],
        "atrTargetMultiplier": src["atr_target_multiplier"],
        "maxHoldingBars": src["max_holding_bars"],
        "brokeragePerSide": src["brokerage_per_side"],
        "brokerageEnabled": src["brokerage_enabled"],
        "equityDefaultQty": src["equity_default_qty"],
        "demoMode": src["demo_mode"],
    }

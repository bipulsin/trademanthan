"""Configurable weights/thresholds for RS Conviction Score board + Setup Radar."""
from __future__ import annotations

import json
import logging
from copy import deepcopy
from typing import Any, Dict

from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

DEFAULTS: Dict[str, Any] = {
    # Conviction composite weights (sum of positive weights = 1.0; whip is penalty)
    "W_rs": 0.30,
    "W_anchor": 0.20,
    "W_persist": 0.20,
    "W_slope": 0.15,
    "W_accum": 0.15,
    "W_whip": 0.10,
    # Opening anchor values by rank band
    "anchor_rank12": 100,
    "anchor_rank35": 80,
    # Persistence credit
    "persist_increment": 15,
    "persist_cap": 100,
    "persist_half_life_min": 50,
    # VWAP slope
    "slope_ref_atr_per_30m": 0.5,
    "slope_mult_morning": 1.0,
    "slope_mult_midday": 0.6,
    "slope_mult_late": 0.4,
    "slope_morning_end_min": 11 * 60,
    "slope_midday_end_min": 13 * 60,
    # Accumulation
    "accum_vol_multiple": 1.5,
    # Whipsaw penalty thresholds (cross count -> penalty 0-100)
    "whip_cross_2": 30,
    "whip_cross_3": 60,
    "whip_cross_4": 100,
    # Board hysteresis
    "promotion_cycles_required": 2,
    "hard_eject_score_floor": 40,
    "hard_eject_whip_min": 100,
    # Auto-refresh cutoff (minutes since midnight IST)
    "board_cutoff_min": 15 * 60 + 15,
    # Setup radar
    "convergence_atr": 0.35,
    "convergence_bars": 2,
    "expiry_atr": 1.5,
    "sl_buffer_atr": 0.1,
    "sl_late_pct": 0.6,
    "chop_warning_crosses": 3,
    "bench_persist_floor": 30,
    # Alerts
    "alert_sound_enabled": True,
    "alert_window_start_min": 9 * 60 + 25,
    "alert_window_end_min": 14 * 60 + 30,
    # Display
    "show_ema10_passive": True,
    # Momentum ignition (Phase 1) — disabled until validation gate passes
    "ignition_ui_enabled": False,
    "ignition_conviction_enabled": False,
    "ignition_flag_threshold": 65,
    "W_ignition_orderflow": 0.35,
    "W_ignition_oi_tri": 0.25,
    "W_ignition_absorption": 0.15,
    "W_ignition_slope": 0.10,
    "W_ignition_pullback": 0.10,
    "W_ignition_confirm": 0.05,
    "W_ignition_fii_context": 0.05,
    "W_ignition_conviction": 0.08,
}


def get_config() -> Dict[str, Any]:
    cfg = deepcopy(DEFAULTS)
    db = SessionLocal()
    try:
        rows = db.execute(text("SELECT key, value FROM rs_conviction_config")).fetchall()
        for r in rows:
            try:
                cfg[r.key] = json.loads(r.value)
            except (json.JSONDecodeError, TypeError):
                cfg[r.key] = r.value
    except Exception as exc:
        logger.debug("rs_conviction_config load: %s", exc)
    finally:
        db.close()
    return cfg


def save_config(updates: Dict[str, Any]) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        for key, val in updates.items():
            if key not in DEFAULTS:
                continue
            db.execute(
                text(
                    """
                    INSERT INTO rs_conviction_config (key, value, updated_at)
                    VALUES (:k, :v, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    """
                ),
                {"k": key, "v": json.dumps(val)},
            )
        db.commit()
    finally:
        db.close()
    return get_config()


def reset_config() -> Dict[str, Any]:
    db = SessionLocal()
    try:
        db.execute(text("DELETE FROM rs_conviction_config"))
        db.commit()
    finally:
        db.close()
    return get_config()


def persist_decay_factor(cfg: Dict[str, Any]) -> float:
    """Multiplicative decay per 10-min cycle from half-life in minutes."""
    half = float(cfg.get("persist_half_life_min") or 50)
    return 0.5 ** (10.0 / max(half, 1.0))

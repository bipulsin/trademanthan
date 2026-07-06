"""BTST strategy configuration — defaults + DB persistence."""
from __future__ import annotations

import json
import logging
from copy import deepcopy
from typing import Any, Dict

from sqlalchemy import text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)

DEFAULTS: Dict[str, Any] = {
    "supertrend_period": 10,
    "supertrend_multiplier": 3.0,
    "hull_length": 32,
    "snapshot_hhmm": "14:45",
    "atm_hhmm": "15:00",
    "entry_hhmm": "15:00",
    "premium_gate_hhmm": "15:15",
    "exit_a_hhmm": "15:30",
    "exit_b_hhmm": "09:30",
    "premium_history_trading_days": 24,
}


def get_config() -> Dict[str, Any]:
    cfg = deepcopy(DEFAULTS)
    db = SessionLocal()
    try:
        rows = db.execute(text("SELECT key, value FROM btst_strategy_config")).fetchall()
        for r in rows:
            if r.key not in DEFAULTS:
                continue
            try:
                cfg[r.key] = json.loads(r.value)
            except (json.JSONDecodeError, TypeError):
                cfg[r.key] = r.value
    except Exception as exc:
        logger.debug("btst_strategy_config load: %s", exc)
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
                    INSERT INTO btst_strategy_config (key, value, updated_at)
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

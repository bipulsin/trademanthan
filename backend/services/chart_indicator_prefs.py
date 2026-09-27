"""Per-user indicator selection for the shared security chart modal."""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

EMA_PERIOD_MIN = 2
EMA_PERIOD_MAX = 200
EMA_SLOT_DEFAULTS = (9, 30, 100)

_PREF_KEYS = ("vwap", "volume", "hm")


def sanitize_indicator_prefs(raw: Any) -> Dict[str, Any]:
    """
    Keep only the indicator on/off set the chart modal can render.

    Three EMA slots (enabled + period) plus VWAP, volume, Hilega-Milega, and
    MACD divergence. Periods are clamped to the same bounds as the chart UI.
    """
    if not isinstance(raw, dict):
        raise ValueError("indicator prefs must be an object")
    emas_in = raw.get("emas")
    if not isinstance(emas_in, list) or len(emas_in) != len(EMA_SLOT_DEFAULTS):
        raise ValueError("indicator prefs need 3 EMA slots")
    emas = []
    for idx, row in enumerate(emas_in):
        if not isinstance(row, dict):
            raise ValueError("each EMA slot must be an object")
        period = row.get("period", EMA_SLOT_DEFAULTS[idx])
        try:
            period_n = int(period)
        except (TypeError, ValueError):
            period_n = EMA_SLOT_DEFAULTS[idx]
        period_n = max(EMA_PERIOD_MIN, min(EMA_PERIOD_MAX, period_n))
        emas.append({"enabled": bool(row.get("enabled")), "period": period_n})
    out: Dict[str, Any] = {"emas": emas}
    for key in _PREF_KEYS:
        out[key] = bool(raw.get(key))
    # The chart client stores MACD Divergence as "macd".
    out["macd"] = bool(raw.get("macd", raw.get("md")))
    return out


def get_indicator_prefs(db: Session, user_id: int) -> Optional[Dict[str, Any]]:
    row = db.execute(
        text("SELECT prefs FROM chart_indicator_prefs WHERE user_id = :uid"),
        {"uid": int(user_id)},
    ).fetchone()
    if not row or row[0] in (None, ""):
        return None
    try:
        parsed = json.loads(row[0])
    except (TypeError, ValueError):
        return None
    try:
        return sanitize_indicator_prefs(parsed)
    except ValueError:
        return None


def save_indicator_prefs(db: Session, user_id: int, raw: Any) -> Dict[str, Any]:
    prefs = sanitize_indicator_prefs(raw)
    payload = json.dumps(prefs, separators=(",", ":"))
    db.execute(
        text(
            """
            INSERT INTO chart_indicator_prefs (user_id, prefs, updated_at)
            VALUES (:uid, :prefs, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) DO UPDATE
            SET prefs = excluded.prefs,
                updated_at = CURRENT_TIMESTAMP
            """
        ),
        {"uid": int(user_id), "prefs": payload},
    )
    db.commit()
    return prefs

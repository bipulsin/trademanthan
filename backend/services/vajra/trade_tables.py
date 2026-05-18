"""DDL for Vajra discretionary trade workflow."""
from __future__ import annotations

import json
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


def ensure_vajra_discretionary_tables(db: Session) -> None:
    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS vajra_discretionary_trade (
                id BIGSERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                platform TEXT NOT NULL DEFAULT 'daily_futures',
                session_date DATE NOT NULL,
                stock TEXT NOT NULL,
                future_symbol TEXT,
                instrument_key TEXT,
                direction TEXT NOT NULL,
                lots INTEGER NOT NULL DEFAULT 1,
                entry_price DOUBLE PRECISION NOT NULL,
                entry_time TIMESTAMPTZ NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                exit_price DOUBLE PRECISION,
                exit_time TIMESTAMPTZ,
                exit_reasons JSONB,
                realized_pnl DOUBLE PRECISION,
                discovery_snapshot JSONB NOT NULL DEFAULT '{}',
                checklist JSONB NOT NULL DEFAULT '{}',
                metrics_at_entry JSONB NOT NULL DEFAULT '{}',
                lifecycle_state TEXT DEFAULT 'Early Transition',
                trade_health DOUBLE PRECISION DEFAULT 50,
                structure_status TEXT,
                momentum_status TEXT,
                ema_status TEXT,
                vwap_status TEXT,
                current_price DOUBLE PRECISION,
                alerts JSONB NOT NULL DEFAULT '[]',
                lifecycle_history JSONB NOT NULL DEFAULT '[]',
                warnings_at_entry JSONB NOT NULL DEFAULT '[]',
                journal JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                closed_at TIMESTAMPTZ
            )
            """
        )
    )
    db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_vajra_disc_user_status "
            "ON vajra_discretionary_trade (user_id, status, session_date DESC)"
        )
    )


def _json_load(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except (TypeError, json.JSONDecodeError):
        return val


def row_to_dict(row) -> Dict[str, Any]:
    keys = row._mapping.keys() if hasattr(row, "_mapping") else row.keys()
    out: Dict[str, Any] = {}
    for k in keys:
        v = row._mapping[k] if hasattr(row, "_mapping") else row[k]
        if k.endswith("_at") or k in ("entry_time", "exit_time", "session_date"):
            out[k] = v.isoformat() if v is not None and hasattr(v, "isoformat") else v
        elif k in (
            "discovery_snapshot",
            "checklist",
            "metrics_at_entry",
            "alerts",
            "lifecycle_history",
            "warnings_at_entry",
            "journal",
            "exit_reasons",
        ):
            out[k] = _json_load(v) or ({} if k != "alerts" and k != "lifecycle_history" else [])
        else:
            out[k] = v
    return out

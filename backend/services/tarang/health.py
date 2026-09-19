"""Data-health aggregation for Kosmic Tarang Phase 1 UI."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.adapters.delta_india import DeltaIndiaAdapter
from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
from backend.services.tarang.chain_snapshots import chain_coverage
from backend.services.tarang.config import get_events, get_profiles, get_risk
from backend.services.tarang.iv_snapshots import recent_iv_snapshot_counts
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.tarang.sizing import min_max_loss_table

logger = logging.getLogger(__name__)


def _settings() -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        rows = db.execute(text("SELECT key, value FROM tarang_settings")).mappings().all()
        out = {r["key"]: r["value"] for r in rows}
        # json scalars may come back already decoded
        mode = out.get("mode", "PAPER")
        if isinstance(mode, str):
            mode = mode.strip('"')
        auto = out.get("auto", False)
        if isinstance(auto, str):
            auto = auto.lower() in ("true", "1", "yes")
        return {"mode": mode or "PAPER", "auto": bool(auto), "raw": {k: v for k, v in out.items()}}
    finally:
        db.close()


def build_health_payload() -> Dict[str, Any]:
    ensure_tarang_tables()
    settings = _settings()
    upstox = UpstoxMcxAdapter().health()
    delta = DeltaIndiaAdapter().health()
    iv_counts = recent_iv_snapshot_counts()
    risk = get_risk()
    token_state = upstox.get("token") or "unknown"
    token_expired = token_state in ("missing", "expired")
    return {
        "product": "Kosmic Tarang",
        "phase": 3,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "mode": settings.get("mode") or "PAPER",
        "auto": bool(settings.get("auto")),
        "paper_badge": True,
        "token_expired": token_expired,
        "feeds": {
            "upstox_mcx": upstox,
            "delta_india": delta,
        },
        "iv_snapshot_counts": iv_counts,
        "chain_coverage": chain_coverage(),
        "risk": risk,
        "profiles": get_profiles(),
        "events": get_events(),
        "min_max_loss": min_max_loss_table(),
        "upstox_ws": (risk.get("upstox_ws") or {}),
    }

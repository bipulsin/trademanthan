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
from backend.services.tarang.config import get_events, get_profiles, get_risk, validate_profiles
from backend.services.tarang.data_gaps import recent_gaps
from backend.services.tarang.eod_reconstruct import liquidity_report
from backend.services.tarang.expiry_eligibility import expiry_eligibility
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
    hold = dict(risk.get("phase4_hold") or {})
    hold.setdefault("approved", False)
    hold.setdefault("min_clean_snapshot_weeks", 4)
    hold.setdefault("min_paper_trades_reviewed", 20)
    cov = chain_coverage()
    wide_days = max((u.get("days_wide") or 0) for u in (cov.get("underlyings") or [])) if cov.get("underlyings") else 0
    paper_n = 0
    last_probes: list = []
    analytics = settings.get("raw", {}).get("upstox_analytics_probe")
    db = SessionLocal()
    try:
        row = db.execute(text("SELECT COUNT(*)::int AS n FROM tarang_trades WHERE mode = 'PAPER' AND status IN ('CLOSED','REPORTED')")).mappings().first()
        paper_n = int((row or {}).get("n") or 0)
        probes = db.execute(
            text(
                """
                SELECT captured_at, ist_hour, underlying_symbol, expiry_date, payload
                FROM tarang_liquidity_probes
                ORDER BY captured_at DESC
                LIMIT 16
                """
            )
        ).mappings().all()
        last_probes = []
        for p in probes:
            d = dict(p)
            if d.get("captured_at") is not None:
                d["captured_at"] = d["captured_at"].isoformat()
            if d.get("expiry_date") is not None:
                d["expiry_date"] = str(d["expiry_date"])
            last_probes.append(d)
    finally:
        db.close()
    weeks_clean = wide_days / 7.0
    hold["wide_days"] = wide_days
    hold["paper_trades_closed"] = paper_n
    hold["held"] = not bool(hold.get("approved"))
    hold["conditions_met"] = weeks_clean >= float(hold["min_clean_snapshot_weeks"]) and paper_n >= int(hold["min_paper_trades_reviewed"]) and bool(hold.get("approved"))
    hold["note"] = (
        "Phase 4 is held until you approve. Conditions: at least 4 weeks of clean (delta-window) snapshots "
        "and at least 20 forward-test trades reviewed. Live broker send stays locked (TARANG_LIVE_ENABLED defaults false)."
    )
    try:
        eligibility = expiry_eligibility()
    except Exception as e:
        eligibility = {"error": str(e)[:200], "rows": []}
    try:
        eod_liq = liquidity_report()
    except Exception as e:
        eod_liq = {"error": str(e)[:200], "rows": []}
    return {
        "product": "Kosmic Tarang",
        "phase": 3,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "mode": settings.get("mode") or "PAPER",
        "display_mode": "Forward test",
        "display_auto": "Auto orders: locked",
        "auto": False,
        "auto_orders_locked": True,
        "live_enabled": False,
        "token_expired": token_expired,
        "feeds": {
            "upstox_mcx": upstox,
            "delta_india": delta,
        },
        "upstox_analytics_token": analytics if isinstance(analytics, dict) else {"configured": False, "note": "No probe stored yet. 08:45 IST check writes this."},
        "iv_snapshot_counts": iv_counts,
        "chain_coverage": cov,
        "data_gaps": recent_gaps(20),
        "liquidity_probes": last_probes,
        "eod_liquidity": eod_liq,
        "expiry_eligibility": eligibility,
        "profile_validation": validate_profiles(),
        "phase4": hold,
        "risk": risk,
        "profiles": get_profiles(),
        "events": get_events(),
        "min_max_loss": min_max_loss_table(),
        "upstox_ws": (risk.get("upstox_ws") or {}),
    }

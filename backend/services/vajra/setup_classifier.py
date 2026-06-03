"""Setup type + execution workflow state (WAIT → PREPARE → EXECUTABLE → ACTIVE → EXIT RISK)."""
from __future__ import annotations

from typing import Any, Dict, Optional, Set

from backend.services.vajra.qualification_config import (
    STATE_ARMED,
    STATE_DISCOVERY,
    STATE_EXECUTABLE,
    STATE_REJECT,
)
from backend.services.vajra.trade_state import resolve_market_phase
from backend.services.vajra.validation_engine import extension_risk_level

WF_WAIT = "WAIT"
WF_PREPARE = "PREPARE"
WF_EXECUTABLE = "EXECUTABLE"
WF_ACTIVE = "ACTIVE"
WF_EXIT_RISK = "EXIT_RISK"

SETUP_BREAKOUT = "Breakout Continuation"
SETUP_PULLBACK = "Pullback Continuation"
SETUP_RECLAIM = "Reclaim Setup"
SETUP_EXHAUSTION = "Exhaustion Risk"
SETUP_CHOP = "Chop / Avoid"


def _f(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = row.get(key)
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _qual(row: Dict[str, Any]) -> str:
    return str(
        row.get("qualification_state") or row.get("qualification") or row.get("entry_state") or ""
    ).upper()


def classify_setup_type(row: Dict[str, Any]) -> str:
    phase = str(row.get("breakout_phase") or row.get("breakout_lifecycle") or "").lower()
    ts = str(row.get("transition_state") or row.get("momentum") or "").upper()
    ext = _f(row, "extension_risk_score", 50.0)
    pb = _f(row, "pullback_quality_score")
    brk = _f(row, "breakout_score") or _f(row, "ecs_score") * 0.5
    vwap = str(row.get("vwap_reclaim_status") or "").upper()

    if _qual(row) == STATE_REJECT:
        return SETUP_CHOP
    if ext >= 72 or "EXHAUST" in ts or "CLIMAX" in ts:
        return SETUP_EXHAUSTION
    if resolve_market_phase(row) in ("Compression", "Weakening") and brk < 50:
        return SETUP_CHOP
    if "RECLAIM" in vwap or "RECLAIM" in ts:
        return SETUP_RECLAIM
    if "BREAKOUT" in phase or "INITIAT" in phase or brk >= 58:
        return SETUP_BREAKOUT
    if pb >= 48 or "PULLBACK" in ts:
        return SETUP_PULLBACK
    if brk >= 45:
        return SETUP_BREAKOUT
    return SETUP_PULLBACK


def quality_grade(row: Dict[str, Any]) -> str:
    """A+ / A / B+ / B / C discretionary execution quality."""
    if _qual(row) == STATE_REJECT:
        return "C"
    conv = _f(row, "conviction_score") or _f(row, "confidence")
    sq = _f(row, "setup_quality_score") or _f(row, "trade_quality_score")
    ess = _f(row, "ess_score", 50.0)
    sss = _f(row, "sector_stability_score", 50.0)
    sector_ok = bool(row.get("sector_in_top_gainers_rank")) or row.get("sector_trade_badge") == "SECTOR_ALIGNED"
    ext = _f(row, "extension_risk_score", 50.0)

    if ext >= 75 or classify_setup_type(row) == SETUP_EXHAUSTION:
        return "C" if _qual(row) != STATE_EXECUTABLE else "B"
    if _qual(row) == STATE_EXECUTABLE and conv >= 80 and sq >= 75 and (sector_ok or sss >= 58) and ess >= 65:
        return "A+"
    if _qual(row) == STATE_EXECUTABLE and conv >= 72 and sq >= 68:
        return "A"
    if _qual(row) in (STATE_EXECUTABLE, STATE_ARMED) and conv >= 62 and sq >= 58:
        return "B+"
    if _qual(row) in (STATE_EXECUTABLE, STATE_ARMED) and conv >= 55:
        return "B"
    if _qual(row) == STATE_DISCOVERY:
        return "C"
    return "B"


SCREENER_ALLOWED_GRADES = frozenset({"A+", "A", "B+", "B"})


def screener_grade_allowed(row: Dict[str, Any]) -> bool:
    """Screener / Top 3: A+, A, B+, and B setups (LONG or SHORT)."""
    grade = str(row.get("quality_grade") or quality_grade(row)).strip().upper()
    return grade in SCREENER_ALLOWED_GRADES


def resolve_execution_workflow_state(
    row: Dict[str, Any],
    *,
    active_stocks: Optional[Set[str]] = None,
    trade_health: Optional[float] = None,
    trade_lifecycle: Optional[str] = None,
) -> str:
    stock = str(row.get("stock") or row.get("security") or "").strip().upper()
    active_stocks = active_stocks or set()

    if stock and stock in active_stocks:
        lc = str(trade_lifecycle or row.get("lifecycle_state") or "").lower()
        health = trade_health if trade_health is not None else _f(row, "trade_health", 70.0)
        if health < 45 or "breakdown" in lc or "exhaustion" in lc or "failed" in lc:
            return WF_EXIT_RISK
        return WF_ACTIVE

    q = _qual(row)
    ext = _f(row, "extension_risk_score", 50.0)
    ext_lvl = extension_risk_level(ext)

    if q == STATE_REJECT:
        return WF_WAIT
    setup = classify_setup_type(row)
    if setup == SETUP_EXHAUSTION and q == STATE_EXECUTABLE:
        return WF_EXIT_RISK
    if ext_lvl == "HIGH" and q == STATE_EXECUTABLE:
        return WF_EXIT_RISK
    if q == STATE_EXECUTABLE:
        return WF_EXECUTABLE
    if q == STATE_ARMED:
        return WF_PREPARE
    return WF_WAIT

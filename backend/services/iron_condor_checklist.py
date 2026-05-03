"""
Pre-entry checklist for Iron Condor (chips: PASS / FAIL / WARN).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.upstox_service import upstox_service as vwap_service
from backend.services.iron_condor_service import option_chain_underlying, ensure_iron_condor_tables

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _chip(status: str, code: str, message: str, detail: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"status": status, "code": code, "message": message, "detail": detail or {}}


def fetch_india_vix() -> Tuple[Optional[float], Optional[str]]:
    key = getattr(vwap_service, "INDIA_VIX_KEY", "NSE_INDEX|India VIX")
    try:
        q = vwap_service.get_market_quote_by_key(key) or {}
        v = q.get("last_price") or q.get("ltp")
        if v is not None and float(v) > 0:
            return float(v), None
    except Exception as e:
        logger.warning("iron_condor: VIX fetch failed: %s", e)
    return None, "Could not fetch India VIX"


def fetch_gap_check(eq_key: str) -> Dict[str, Any]:
    try:
        candles = vwap_service.get_historical_candles_by_instrument_key(eq_key, interval="days/1", days_back=14) or []
        if len(candles) < 6:
            return _chip(
                "WARN",
                "GAP_MOVE",
                "Less than 6 daily bars available; gap check inconclusive.",
                {"bars": len(candles)},
            )
        bars = sorted(candles, key=lambda c: str(c.get("timestamp") or ""))
        worst = 0.0
        worst_day = None
        for i in range(max(1, len(bars) - 5), len(bars)):
            o = float(bars[i].get("open") or 0)
            prev_c = float(bars[i - 1].get("close") or 0)
            if prev_c <= 0:
                continue
            gap_pct = abs(o - prev_c) / prev_c * 100.0
            if gap_pct > worst:
                worst = gap_pct
                worst_day = str(bars[i].get("timestamp"))
        if worst > 4.0:
            return _chip(
                "FAIL",
                "GAP_MOVE",
                f"Largest 1D gap in last 5 sessions: {worst:.2f}% (>4%).",
                {"worst_gap_pct": worst, "day": worst_day},
            )
        return _chip("PASS", "GAP_MOVE", f"Largest gap in window: {worst:.2f}% (≤4%).", {"worst_gap_pct": worst})
    except Exception as e:
        return _chip("WARN", "GAP_MOVE", f"Gap check error: {e}", {})


def fetch_earnings_chip(symbol: str, declared_next_earnings_iso: Optional[str] = None) -> Dict[str, Any]:
    """
    Optional user-declared next result date from checklist POST (manual truth).
    If absent: WARN (automated feed not wired). If within 25 days: FAIL.
    """
    if declared_next_earnings_iso:
        try:
            d = datetime.strptime(str(declared_next_earnings_iso).strip()[:10], "%Y-%m-%d").date()
        except ValueError:
            return _chip("WARN", "EARNINGS_25D", "Invalid earnings date format; use YYYY-MM-DD.", {})
        today = datetime.now(IST).date()
        days = (d - today).days
        if days < 0:
            return _chip("PASS", "EARNINGS_25D", "Declared result date is in the past — verify if already announced.", {"date": str(d)})
        if days <= 25:
            return _chip(
                "FAIL",
                "EARNINGS_25D",
                f"Declared next result ~{days} day(s) away (≤25). Avoid new short-vol entries.",
                {"days": days, "date": str(d)},
            )
        return _chip("PASS", "EARNINGS_25D", f"Declared next result ~{days} day(s) out (>25).", {"days": days, "date": str(d)})
    return _chip(
        "WARN",
        "EARNINGS_25D",
        "Enter optional next known result date to enforce the 25-day rule; otherwise verify manually.",
        {"symbol": symbol, "automated": False},
    )


def iv_rank_proxy_chip(symbol: str) -> Dict[str, Any]:
    api_sym = option_chain_underlying(symbol)
    chain = vwap_service.get_option_chain(api_sym)
    payload = chain
    if isinstance(chain, dict) and chain.get("status") == "success":
        payload = chain.get("data") or chain
    strikes = payload.get("strikes") if isinstance(payload, dict) else None
    if not strikes or not isinstance(strikes, list):
        return _chip("WARN", "IVR", "IV rank unavailable (no IV on chain). Review manually.", {})
    ivs: List[float] = []
    for sd in strikes:
        if not isinstance(sd, dict):
            continue
        for side in ("call_options", "put_options"):
            node = sd.get(side)
            od = node.get("market_data", node) if isinstance(node, dict) else None
            if isinstance(od, dict):
                iv = od.get("iv") or od.get("implied_volatility")
                try:
                    if iv is not None:
                        ivs.append(float(iv))
                except (TypeError, ValueError):
                    pass
    if len(ivs) < 10:
        return _chip("WARN", "IVR", "Insufficient IV samples; discretionary IV review.", {"samples": len(ivs)})
    lo, hi = min(ivs), max(ivs)
    mid_iv = sorted(ivs)[len(ivs) // 2]
    if hi <= lo:
        return _chip("WARN", "IVR", "IV range flat on chain snapshot.", {})
    ivr_pct = (mid_iv - lo) / (hi - lo + 1e-9) * 100.0
    if ivr_pct < 30:
        return _chip(
            "WARN",
            "IVR",
            f"IV proxy rank is low ({ivr_pct:.0f}%).",
            {"ivr_proxy": round(ivr_pct, 2)},
        )
    return _chip("PASS", "IVR", f"IV proxy rank OK (~{ivr_pct:.0f}%).", {"ivr_proxy": round(ivr_pct, 2)})


def macro_event_chip(db: Session) -> Dict[str, Any]:
    ensure_iron_condor_tables()
    today = datetime.now(IST).date()
    horizon = today + timedelta(days=7)
    rows = db.execute(
        text(
            """
            SELECT TO_CHAR(event_date, 'YYYY-MM-DD'), event_type, description
            FROM iron_condor_macro_calendar
            WHERE event_date >= CAST(:td AS DATE) AND event_date <= CAST(:hz AS DATE)
            ORDER BY event_date ASC
            LIMIT 20
            """
        ),
        {"td": str(today), "hz": str(horizon)},
    ).fetchall()
    if not rows:
        return _chip("PASS", "MACRO_EVENTS", "No macro/policy calendar hits in DB within 7 days.", {})
    detail = [{"date": r[0], "type": r[1], "desc": r[2]} for r in rows]
    return _chip(
        "FAIL",
        "MACRO_EVENTS",
        "Major macro / policy / scheduled event within 7 days.",
        {"events": detail},
    )


def sector_concentration_chip(db: Session, user_id: int, sector: str) -> Dict[str, Any]:
    ensure_iron_condor_tables()
    n = db.execute(
        text(
            """
            SELECT COUNT(*) FROM iron_condor_position
            WHERE user_id = :uid AND UPPER(status) IN ('ACTIVE', 'OPEN', 'ADJUSTED')
              AND UPPER(TRIM(sector)) = UPPER(TRIM(:sec))
            """
        ),
        {"uid": user_id, "sec": sector},
    ).scalar()
    if int(n or 0) >= 1:
        return _chip(
            "WARN",
            "SECTOR_POSITION",
            "Existing open advisory in this sector (concentration).",
            {"open_same_sector": int(n)},
        )
    return _chip("PASS", "SECTOR_POSITION", "No other open position in this sector.", {})


def capital_chip(db: Session, user_id: int, trading_capital: float, new_alloc_estimate: float) -> Dict[str, Any]:
    ensure_iron_condor_tables()
    if trading_capital <= 0:
        return _chip("FAIL", "CAPITAL", "Set trading capital in settings (>0).", {})
    deployed = db.execute(
        text(
            """
            SELECT COALESCE(SUM(COALESCE(suggested_capital_rupees, 0)), 0)
            FROM iron_condor_position
            WHERE user_id = :uid AND UPPER(status) IN ('ACTIVE', 'OPEN', 'ADJUSTED')
            """
        ),
        {"uid": user_id},
    ).scalar()
    dep = float(deployed or 0)
    after = dep + float(new_alloc_estimate or 0)
    ratio = after / trading_capital if trading_capital else 0
    if ratio > 0.85:
        return _chip(
            "FAIL",
            "CAPITAL_DEPLOYED",
            f"Deployed+new would exceed 85% of capital ({ratio*100:.1f}% of ₹{trading_capital:.0f}).",
            {"deployed": dep, "after_est": after, "ratio": round(ratio, 4)},
        )
    return _chip(
        "PASS",
        "CAPITAL_DEPLOYED",
        f"Estimated deploy {(ratio*100):.1f}% of capital after new position.",
        {"deployed": dep, "after_est": after},
    )


def calendar_month_warn_chip(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = now or datetime.now(IST)
    m = now.month
    if m == 2 or m in (6, 7):
        return _chip("WARN", "CALENDAR_MONTH", "Feb / Jun-Jul flagged as elevated event-risk months.", {"month": m})
    return _chip("PASS", "CALENDAR_MONTH", "Outside flagged high-risk months.", {"month": m})


def vix_chip(vix_val: Optional[float]) -> Dict[str, Any]:
    if vix_val is None:
        return _chip("WARN", "INDIA_VIX", "India VIX not available.", {})
    if vix_val > 20:
        return _chip("FAIL", "INDIA_VIX", f"India VIX {vix_val:.2f} > 20 (elevated fear).", {"vix": vix_val})
    if vix_val > 18:
        return _chip("WARN", "INDIA_VIX", f"India VIX {vix_val:.2f} > 18.", {"vix": vix_val})
    return _chip("PASS", "INDIA_VIX", f"India VIX {vix_val:.2f}.", {"vix": vix_val})


def spot_change_chip(eq_key: str) -> Dict[str, Any]:
    """Today's % vs prev close — display-only chip (INFO)."""
    try:
        ohlc = vwap_service.get_ohlc_data(eq_key) or {}
        lp = float(ohlc.get("last_price") or ohlc.get("close") or 0)
        nested = ohlc.get("ohlc") if isinstance(ohlc.get("ohlc"), dict) else {}
        prev = float(nested.get("close") or ohlc.get("close_price") or 0)
        pct = None
        msg = "Intraday % change unavailable"
        if lp and prev:
            pct = (lp - prev) / prev * 100.0
            msg = "Spot vs prior session close ~ {:.2f}%.".format(pct)
        return _chip("INFO", "SPOT_CHG", msg, {"ltp": lp, "change_pct_day": pct})
    except Exception as e:
        return _chip("INFO", "SPOT_CHG", "Spot change unavailable: {}".format(e), {})


def active_same_symbol_chip(db: Session, user_id: int, symbol: str) -> Dict[str, Any]:
    ensure_iron_condor_tables()
    n = db.execute(
        text(
            """
            SELECT COUNT(*) FROM iron_condor_position
            WHERE user_id = :uid AND UPPER(TRIM(underlying)) = UPPER(TRIM(:sym))
              AND UPPER(status) IN ('ACTIVE', 'OPEN', 'ADJUSTED')
            """
        ),
        {"uid": user_id, "sym": symbol},
    ).scalar()
    if int(n or 0) >= 1:
        return _chip("WARN", "ACTIVE_SAME_STOCK", "You already have an active Iron Condor on this underlying.", {})
    return _chip("PASS", "ACTIVE_SAME_STOCK", "No active position on this stock.", {})


def run_pre_entry_checklist(
    db: Session,
    user_id: int,
    symbol: str,
    sector: str,
    new_capital_estimate: float,
    declared_next_earnings_iso: Optional[str] = None,
) -> Dict[str, Any]:
    api_sym = option_chain_underlying(symbol)
    eq_key = vwap_service.get_instrument_key(api_sym)
    chips: List[Dict[str, Any]] = []
    vix, verr = fetch_india_vix()
    chips.append(vix_chip(vix))

    rows = db.execute(
        text("SELECT trading_capital FROM iron_condor_user_settings WHERE user_id = :uid LIMIT 1"),
        {"uid": user_id},
    ).mappings().first()
    tc = float((rows or {}).get("trading_capital") or 0)

    if eq_key:
        chips.append(fetch_gap_check(eq_key))
        chips.append(spot_change_chip(eq_key))
    else:
        chips.append(_chip("WARN", "EQUITY", "No equity key for underlying.", {"symbol": api_sym}))

    chips.append(active_same_symbol_chip(db, user_id, symbol))
    chips.append(fetch_earnings_chip(symbol, declared_next_earnings_iso))
    chips.append(iv_rank_proxy_chip(symbol))
    chips.append(macro_event_chip(db))
    chips.append(sector_concentration_chip(db, user_id, sector))
    chips.append(capital_chip(db, user_id, tc, new_capital_estimate))
    chips.append(calendar_month_warn_chip())

    fails = [c for c in chips if c["status"] == "FAIL"]
    warns = [c for c in chips if c["status"] == "WARN"]
    return {
        "chips": chips,
        "may_proceed_blocked": len(fails) > 0,
        "warnings_require_ack": len(warns) > 0,
        "vix_value": vix,
        "vix_error": verr,
    }

"""
Pre-entry checklist for Iron Condor (chips: PASS / FAIL / WARN).
"""
from __future__ import annotations

import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta, date
from typing import Any, Dict, Iterator, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.services.upstox_service import upstox_service as vwap_service
from backend.services.iron_condor_service import option_chain_underlying, ensure_iron_condor_tables
from backend.services.iron_condor_earnings import fetch_nse_results_hint
from backend.services.iron_condor_iv_vol import iv_context_chip
from backend.services.market_sentiment_dials import build_dial_rows

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


def gap_chip_from_daily_candles(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Gap rule on pre-fetched daily bars (sorted by broker timestamp); same logic as fetch_gap_check."""
    try:
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


def fetch_gap_check(eq_key: str) -> Dict[str, Any]:
    try:
        candles = vwap_service.get_historical_candles_by_instrument_key(eq_key, interval="days/1", days_back=14) or []
        return gap_chip_from_daily_candles(candles)
    except Exception as e:
        return _chip("WARN", "GAP_MOVE", f"Gap check error: {e}", {})


def fetch_earnings_chip(symbol: str, declared_next_earnings_iso: Optional[str] = None) -> Dict[str, Any]:
    """
    Prefer user-declared result date when supplied.
    Else best-effort NSE corporate-announcement scan for forward-looking filings text.
    If within 25 calendar days → FAIL advisory entry.
    """
    today = datetime.now(IST).date()
    if declared_next_earnings_iso:
        try:
            d = datetime.strptime(str(declared_next_earnings_iso).strip()[:10], "%Y-%m-%d").date()
        except ValueError:
            return _chip("WARN", "EARNINGS_25D", "Invalid earnings date format; use YYYY-MM-DD.", {})
        days_d = (d - today).days
        if days_d < 0:
            return _chip(
                "PASS",
                "EARNINGS_25D",
                "Declared result date is in the past — verify corporate calendar.",
                {"date": str(d), "source": "declared"},
            )
        if days_d <= 25:
            return _chip(
                "FAIL",
                "EARNINGS_25D",
                f"Declared result ~{days_d} calendar day(s) away (≤25). Avoid fresh short‑vol initiation.",
                {"days": days_d, "date": str(d), "source": "declared"},
            )
        return _chip(
            "PASS",
            "EARNINGS_25D",
            f"Declared result ~{days_d} calendar day(s) out (>25 horizon).",
            {"days": days_d, "date": str(d), "source": "declared"},
        )

    auto_date, rationale, diag = fetch_nse_results_hint(symbol)
    detail = dict(diag)
    detail["hint"] = rationale
    if auto_date is None:
        return _chip(
            "WARN",
            "EARNINGS_25D",
            "Could not confidently resolve next earnings / board-results window ("
            + (rationale[:180] + ("…" if len(rationale) > 180 else ""))
            + "). Optionally enter manual YYYY‑MM‑DD.",
            detail,
        )
    gap = (auto_date - today).days
    if gap <= 25:
        merged = dict(detail)
        merged.update({"date": str(auto_date), "calendar_days": gap, "source": "nse_text_parse"})
        return _chip(
            "FAIL",
            "EARNINGS_25D",
            f"Estimated result-adjacent event on {auto_date.isoformat()} (~{gap} day(s)). Rule blocks entry ≤25.",
            merged,
        )
    merged = dict(detail)
    merged.update({"date": str(auto_date), "calendar_days": gap, "source": "nse_text_parse"})
    return _chip(
        "PASS",
        "EARNINGS_25D",
        f"No parsed result-risk window ≤25 days. Next hinted corporate marker ≈ {auto_date.isoformat()}. Verify on NSE / broker.",
        merged,
    )


def get_nxt_earning_iso_from_master(db: Session, symbol: str) -> Optional[str]:
    ensure_iron_condor_tables()
    row = db.execute(
        text(
            """
            SELECT nxt_earning_date FROM iron_condor_universe_master
            WHERE UPPER(TRIM(symbol)) = UPPER(TRIM(:sym))
            LIMIT 1
            """
        ),
        {"sym": symbol.strip()},
    ).fetchone()
    if not row or row[0] is None:
        return None
    d = row[0]
    if hasattr(d, "isoformat"):
        return str(d.isoformat())[:10]
    s = str(d).strip()
    return s[:10] if s else None


def earnings_chip_from_master(db: Session, symbol: str) -> Dict[str, Any]:
    iso = get_nxt_earning_iso_from_master(db, symbol)
    if not iso:
        return _chip(
            "WARN",
            "EARNINGS_25D",
            "No nxt_earning_date in iron condor universe master for this symbol.",
            {},
        )
    return fetch_earnings_chip(symbol, declared_next_earnings_iso=iso)


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


def _checklist_daily_candles_bundle(eq_key: Optional[str]) -> List[Dict[str, Any]]:
    """One equity daily history pull for checklist (gap + realised-vol reuse)."""
    if not eq_key:
        return []
    try:
        return (
            vwap_service.get_historical_candles_by_instrument_key(
                eq_key, interval="days/1", days_back=320
            )
            or []
        )
    except Exception as ex:
        logger.warning("Iron Condor checklist daily bundle fetch failed: %s", ex)
        return []


def _iv_context_worker(sym: str, iv_precached: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Per-thread Session — checklist parallel workers must not share request-scoped Session."""
    from backend.database import SessionLocal

    s = SessionLocal()
    try:
        return iv_context_chip(sym, s, precached_daily=iv_precached)
    finally:
        s.close()


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


def iter_pre_entry_checklist_events(
    db: Session,
    user_id: int,
    symbol: str,
    sector: str,
) -> Iterator[Dict[str, Any]]:
    """
    Yields NDJSON-friendly events: {"kind":"chip", ...} as each piece is ready, then {"kind":"done", ...}.
    Earnings date comes from iron_condor_universe_master.nxt_earning_date only.
    """
    chips_acc: List[Dict[str, Any]] = []

    def emit_chip(c: Dict[str, Any]) -> Dict[str, Any]:
        chips_acc.append(c)
        fails = any(x["status"] == "FAIL" for x in chips_acc)
        warns = any(x["status"] == "WARN" for x in chips_acc)
        return {
            "kind": "chip",
            "chip": c,
            "may_proceed_blocked": fails,
            "warnings_require_ack": warns,
        }

    from backend.services.iron_condor_snapshot_cache import read_india_vix_session
    from backend.config import settings as _settings

    api_sym = option_chain_underlying(symbol)
    eq_key = vwap_service.get_instrument_key(api_sym)

    # Same India VIX spot as dashboard "Market Sentiments" (Upstox → Yahoo); fallback to legacy quote/cache.
    vix: Optional[float] = None
    verr: Optional[str] = None
    try:
        for row in build_dial_rows(vwap_service, basis="today"):
            if str(row.get("id") or "").lower() == "indiavix":
                vv = row.get("vix_value")
                if vv is None:
                    vv = row.get("last")
                if vv is not None:
                    try:
                        vf = float(vv)
                        if math.isfinite(vf) and vf > 0:
                            vix = vf
                    except (TypeError, ValueError):
                        pass
                break
    except Exception as ex:
        logger.warning("checklist: build_dial_rows (India VIX): %s", ex)
    if vix is None:
        td_chk = datetime.now(IST).date()
        vix_live_cache = read_india_vix_session(db, td_chk)
        if vix_live_cache is not None and vix_live_cache > 0:
            vix = float(vix_live_cache)
            verr = None
        else:
            vix, verr = fetch_india_vix()
    yield emit_chip(vix_chip(vix))
    yield emit_chip(active_same_symbol_chip(db, user_id, symbol))
    yield emit_chip(sector_concentration_chip(db, user_id, sector))
    yield emit_chip(earnings_chip_from_master(db, symbol))

    daily_bundle: List[Dict[str, Any]] = []
    iv_precached: Optional[List[Dict[str, Any]]] = None
    if eq_key:
        raw = _checklist_daily_candles_bundle(eq_key)
        daily_bundle = [dict(x) for x in raw if isinstance(x, dict)]
        iv_precached = daily_bundle if len(daily_bundle) >= 42 else None

    _pool_wall = float(getattr(_settings, "IRON_CONDOR_CHECKLIST_POOL_TIMEOUT_SEC", 72))

    if eq_key:

        def gap_fn() -> Dict[str, Any]:
            if len(daily_bundle) >= 6:
                return gap_chip_from_daily_candles(daily_bundle)
            return fetch_gap_check(eq_key)

        with ThreadPoolExecutor(max_workers=3) as pool:
            f_gap = pool.submit(gap_fn)
            f_spot = pool.submit(spot_change_chip, eq_key)
            f_iv = pool.submit(_iv_context_worker, symbol, iv_precached)
            futures_set = {f_gap, f_spot, f_iv}
            _done_x, pend_x = wait(futures_set, timeout=_pool_wall)
            for xf in pend_x:
                try:
                    xf.cancel()
                except Exception:
                    pass
            fut_code = {f_gap: "GAP_MOVE", f_spot: "SPOT_CHG", f_iv: "IV_VOL"}
            for fut in as_completed(tuple(_done_x)):
                code = fut_code.get(fut, "CHK")
                try:
                    yield emit_chip(fut.result(timeout=0))
                except Exception as e:
                    yield emit_chip(_chip("WARN", code, "Check interrupted: %s" % str(e)[:160], {}))
            for fut in pend_x:
                cd = fut_code.get(fut, "CHK")
                yield emit_chip(_chip("WARN", cd, "Check timed out.", {}))
    else:
        yield emit_chip(
            _chip("INFO", "GAP_MOVE", "N/A — no equity instrument key for quotes.", {"symbol": api_sym})
        )
        yield emit_chip(
            _chip("INFO", "SPOT_CHG", "N/A — no equity instrument key for quotes.", {"symbol": api_sym})
        )
        yield emit_chip(_chip("WARN", "EQUITY", "No equity key for underlying.", {"symbol": api_sym}))
        with ThreadPoolExecutor(max_workers=1) as pool:
            f_iv = pool.submit(_iv_context_worker, symbol, None)
            _done_i, pend_i = wait({f_iv}, timeout=_pool_wall)
            for xf in pend_i:
                try:
                    xf.cancel()
                except Exception:
                    pass
            if f_iv in _done_i:
                try:
                    yield emit_chip(f_iv.result(timeout=0))
                except Exception as e:
                    yield emit_chip(_chip("WARN", "IV_VOL", "IV/volatility check interrupted: %s" % str(e)[:160], {}))
            else:
                yield emit_chip(_chip("WARN", "IV_VOL", "IV check timed out.", {}))

    fails = any(x["status"] == "FAIL" for x in chips_acc)
    warns = any(x["status"] == "WARN" for x in chips_acc)
    yield {
        "kind": "done",
        "may_proceed_blocked": fails,
        "warnings_require_ack": warns,
        "vix_value": vix,
        "vix_error": verr,
    }


def run_pre_entry_checklist(
    db: Session,
    user_id: int,
    symbol: str,
    sector: str,
) -> Dict[str, Any]:
    chips: List[Dict[str, Any]] = []
    out: Dict[str, Any] = {}
    for ev in iter_pre_entry_checklist_events(db, user_id, symbol, sector):
        if ev.get("kind") == "chip":
            chips.append(ev["chip"])
        elif ev.get("kind") == "done":
            out = {
                "chips": chips,
                "may_proceed_blocked": ev["may_proceed_blocked"],
                "warnings_require_ack": ev["warnings_require_ack"],
                "vix_value": ev.get("vix_value"),
                "vix_error": ev.get("vix_error"),
            }
    return out or {
        "chips": chips,
        "may_proceed_blocked": False,
        "warnings_require_ack": False,
        "vix_value": None,
        "vix_error": None,
    }

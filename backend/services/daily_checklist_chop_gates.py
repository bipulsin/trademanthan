"""Chop-day / whipsaw / pullback / re-entry gates for RS Daily Checklist trade-state.

Display-only downgrades — no auto-execution. Thresholds via env (defaults below).
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import bindparam, text

from backend.database import SessionLocal

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

NIFTY50_KEY = "NSE_INDEX|Nifty 50"

# Env-tunable thresholds (defaults match 2026-07-13 runbook addendum)
CHOP_RANGE_RATIO = float(os.getenv("KAVACH_CHOP_RANGE_RATIO", "0.4"))
CHOP_ADX_MAX = float(os.getenv("KAVACH_CHOP_ADX_MAX", "15"))
TREND_ADX_MIN = float(os.getenv("KAVACH_TREND_ADX_MIN", "20"))
CHOP_VIX_DELTA_PCT = float(os.getenv("KAVACH_CHOP_VIX_DELTA_PCT", "5"))
RANGE_LOOKBACK_DAYS = int(os.getenv("KAVACH_RANGE_LOOKBACK_DAYS", "5"))
NIFTY_FLAT_PCT = float(os.getenv("KAVACH_NIFTY_FLAT_PCT", "0.15"))  # |day change| for "flat"
WHIPSAW_REVERSALS = int(os.getenv("KAVACH_WHIPSAW_REVERSALS", "2"))
PULLBACK_EXTENDED = int(os.getenv("KAVACH_PULLBACK_EXTENDED", "3"))

REGIME_TREND = "TREND"
REGIME_CHOP = "CHOP"
REGIME_TRANSITION = "TRANSITION"

FLIP_CLASSIFICATION = "RS confirmed but not promoted / direction flip"

_SL_EXIT_REASONS = frozenset(
    {
        "SL_HIT",
        "SL HIT",
        "STOP_LOSS",
        "STOP LOSS",
        "TRAIL_STOP",
        "EMA10",
        "EMA10_CLOSE",
        "EMA10 REVERSE",
        "INDICATOR_2OF4",
    }
)


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _session_day_bars_10m(candles: List[Dict], session_date: str) -> List[Dict[str, Any]]:
    """Confirmed 10m bars for session_date only."""
    from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx
    from backend.services.relative_strength_scanner import _parse_ist_date, _sorted_candles

    if not candles:
        return []
    candles = _sorted_candles(candles)
    pair_end = last_closed_10m_pair_end_idx(candles)
    bars = [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
    out = []
    for b in bars:
        # map end_5m_idx → candle timestamp
        idx = int(b.get("end_5m_idx") or -1)
        if idx < 0 or idx >= len(candles):
            continue
        d = _parse_ist_date(candles[idx].get("timestamp"))
        if d == session_date:
            out.append(b)
    return out


def count_whipsaw_reversals(
    candles: List[Dict],
    *,
    session_date: str,
    is_long: bool,
    near_atr: float,
    atr: Optional[float],
) -> int:
    """EMA5 touch then close against lock direction within next 1–2 confirmed 10m bars."""
    from backend.services.vajra.indicators import ema_series

    bars = _session_day_bars_10m(candles, session_date)
    if len(bars) < 3 or not atr or atr <= 0:
        return 0
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    ema5_s = ema_series(closes, 5)
    if len(ema5_s) < len(closes):
        return 0
    count = 0
    i = 4  # need EMA5 warm-up
    while i < len(bars) - 1:
        e5 = ema5_s[i]
        touched = (lows[i] <= e5 + near_atr * atr) and (highs[i] >= e5 - near_atr * atr)
        if not touched:
            i += 1
            continue
        # next 1–2 closes against direction
        reversed_ok = False
        for j in (i + 1, i + 2):
            if j >= len(bars):
                break
            c = closes[j]
            if is_long and c < ema5_s[j]:
                reversed_ok = True
                break
            if (not is_long) and c > ema5_s[j]:
                reversed_ok = True
                break
        if reversed_ok:
            count += 1
            i = j + 1  # skip past this reverse sequence
        else:
            i += 1
    return count


def count_pullback_attempts(
    candles: List[Dict],
    *,
    session_date: str,
    is_long: bool,
    near_atr: float,
    atr: Optional[float],
) -> int:
    """Distinct EMA5 proximity visits in lock direction (pullback ordinal)."""
    from backend.services.vajra.indicators import ema_series

    bars = _session_day_bars_10m(candles, session_date)
    if len(bars) < 5 or not atr or atr <= 0:
        return 0
    closes = [float(b["close"]) for b in bars]
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    ema5_s = ema_series(closes, 5)
    attempts = 0
    in_zone = False
    for i in range(4, len(bars)):
        e5 = ema5_s[i]
        near = abs(closes[i] - e5) <= near_atr * atr or (
            lows[i] <= e5 <= highs[i]
        )
        # Directional context: LONG pullback = price was above and came to EMA5
        if is_long:
            context_ok = highs[i] >= e5 or (i > 0 and closes[i - 1] > e5)
        else:
            context_ok = lows[i] <= e5 or (i > 0 and closes[i - 1] < e5)
        if near and context_ok:
            if not in_zone:
                attempts += 1
                in_zone = True
        else:
            in_zone = False
    return attempts


def _load_nifty_candles() -> List[Dict]:
    try:
        from backend.services.rs_conviction_candles import candles_cache_only

        cached = candles_cache_only(NIFTY50_KEY)
        if cached and len(cached) >= 40:
            return cached
    except Exception as exc:
        logger.debug("nifty cache miss: %s", exc)
    try:
        from backend.config import settings
        from backend.services.relative_strength_scanner import (
            CANDLE_DAYS_BACK,
            CANDLE_INTERVAL,
            MIN_BARS,
            _sorted_candles,
        )
        from backend.services.upstox_service import UpstoxService

        raw = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET).get_historical_candles_by_instrument_key(
            NIFTY50_KEY, interval=CANDLE_INTERVAL, days_back=max(CANDLE_DAYS_BACK, RANGE_LOOKBACK_DAYS + 2)
        )
        if raw and len(raw) >= MIN_BARS:
            return _sorted_candles(raw)
    except Exception as exc:
        logger.debug("nifty candle fetch failed: %s", exc)
    return []


def _daily_ranges(candles: List[Dict], lookback: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (today_range_pct, avg_prior_range_pct, today_change_pct)."""
    from backend.services.relative_strength_scanner import _parse_ist_date, _sorted_candles

    if not candles:
        return None, None, None
    candles = _sorted_candles(candles)
    by_day: Dict[str, Dict[str, float]] = {}
    for c in candles:
        d = _parse_ist_date(c.get("timestamp"))
        if not d:
            continue
        h, l, o, cl = _f(c.get("high")), _f(c.get("low")), _f(c.get("open")), _f(c.get("close"))
        if h is None or l is None:
            continue
        slot = by_day.setdefault(d, {"hi": h, "lo": l, "open": o or cl or h, "close": cl or o or h})
        slot["hi"] = max(slot["hi"], h)
        slot["lo"] = min(slot["lo"], l)
        if o is not None and slot.get("_o_set") is None:
            slot["open"] = o
            slot["_o_set"] = True
        if cl is not None:
            slot["close"] = cl
    days = sorted(by_day.keys())
    if not days:
        return None, None, None
    today = days[-1]
    t = by_day[today]
    mid = (t["hi"] + t["lo"]) / 2.0 if t["hi"] > 0 else t["close"]
    today_range_pct = ((t["hi"] - t["lo"]) / mid * 100.0) if mid else None
    today_chg = None
    if t.get("open"):
        today_chg = (t["close"] - t["open"]) / t["open"] * 100.0
    prior = days[-(lookback + 1) : -1] if len(days) > 1 else []
    ranges = []
    for d in prior[-lookback:]:
        p = by_day[d]
        m = (p["hi"] + p["lo"]) / 2.0
        if m > 0:
            ranges.append((p["hi"] - p["lo"]) / m * 100.0)
    avg = sum(ranges) / len(ranges) if ranges else None
    return today_range_pct, avg, today_chg


def _nifty_adx(candles: List[Dict]) -> Optional[float]:
    from backend.services.kavach_10m import aggregate_10m_bars, last_closed_10m_pair_end_idx
    from backend.services.relative_strength_scanner import _sorted_candles
    from backend.services.smart_futures_picker.indicators import adx_value

    if not candles or len(candles) < 40:
        return None
    candles = _sorted_candles(candles)
    pair_end = last_closed_10m_pair_end_idx(candles)
    bars = [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
    if len(bars) < 20:
        return None
    highs = [float(b["high"]) for b in bars]
    lows = [float(b["low"]) for b in bars]
    closes = [float(b["close"]) for b in bars]
    return adx_value(highs, lows, closes, 14)


def _vix_snapshot() -> Tuple[Optional[float], Optional[float]]:
    """Return (ltp, prev_close) for India VIX when available."""
    try:
        from backend.services import vwap_service

        key = getattr(vwap_service, "INDIA_VIX_KEY", "NSE_INDEX|India VIX")
        q = vwap_service.get_market_quote_by_key(key) or {}
        ltp = _f(q.get("last_price") or q.get("ltp"))
        ohlc = q.get("ohlc") or {}
        prev = _f(ohlc.get("close") or ohlc.get("previous_close") or q.get("previous_close"))
        return ltp, prev
    except Exception as exc:
        logger.debug("vix snapshot skipped: %s", exc)
        return None, None


def compute_market_regime(session_date: str) -> Dict[str, Any]:
    """Index-level TREND / CHOP / TRANSITION for checklist badge."""
    candles = _load_nifty_candles()
    today_range, avg_range, day_chg = _daily_ranges(candles, RANGE_LOOKBACK_DAYS)
    adx = _nifty_adx(candles)
    vix, vix_prev = _vix_snapshot()
    vix_delta = None
    if vix is not None and vix_prev and vix_prev > 0:
        vix_delta = (vix - vix_prev) / vix_prev * 100.0

    chop_reasons: List[str] = []
    if today_range is not None and avg_range and avg_range > 0:
        if today_range < CHOP_RANGE_RATIO * avg_range:
            chop_reasons.append(
                f"NIFTY range {today_range:.2f}% < {CHOP_RANGE_RATIO:.1f}× avg {avg_range:.2f}%"
            )
    if adx is not None and adx < CHOP_ADX_MAX:
        chop_reasons.append(f"NIFTY ADX {adx:.0f} < {CHOP_ADX_MAX:.0f}")
    if (
        vix_delta is not None
        and vix_delta > CHOP_VIX_DELTA_PCT
        and day_chg is not None
        and abs(day_chg) <= NIFTY_FLAT_PCT
    ):
        chop_reasons.append(f"VIX Δ {vix_delta:+.1f}% with NIFTY flat {day_chg:+.2f}%")

    trend_ok = (
        adx is not None
        and adx >= TREND_ADX_MIN
        and today_range is not None
        and avg_range is not None
        and avg_range > 0
        and today_range > avg_range  # current range > recent-N-day average
        and not chop_reasons
    )

    if chop_reasons:
        regime = REGIME_CHOP
        label = "CHOP DAY — setup win rate historically lower, criteria tightened"
    elif trend_ok:
        regime = REGIME_TREND
        label = "TREND — READY signals at full weight"
    else:
        regime = REGIME_TRANSITION
        label = "TRANSITION — unconfirmed regime"

    return {
        "market_regime": regime,
        "market_regime_label": label,
        "nifty_adx": round(adx, 1) if adx is not None else None,
        "nifty_range_pct": round(today_range, 3) if today_range is not None else None,
        "nifty_avg_range_pct": round(avg_range, 3) if avg_range is not None else None,
        "nifty_day_chg_pct": round(day_chg, 3) if day_chg is not None else None,
        "vix": round(vix, 2) if vix is not None else None,
        "vix_delta_pct": round(vix_delta, 2) if vix_delta is not None else None,
        "chop_reasons": chop_reasons,
        "exit_rule_reminder": "Exit rule: 10m close beyond EMA10 reverse — not VWAP break",
    }


def direction_unstable_flags(
    db,
    session_date: str,
    symbols: List[str],
    current_dirs: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """DIRECTION UNSTABLE when lock sides flip, current≠morning lock, or RSCD flip tag."""
    out: Dict[str, Dict[str, Any]] = {
        s.upper(): {"unstable": False, "reason": None} for s in symbols
    }
    if not symbols:
        return out
    syms = [s.upper() for s in symbols]
    current_dirs = {k.upper(): v for k, v in (current_dirs or {}).items()}
    morning: Dict[str, str] = {}
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(symbol) AS symbol, direction, event_type, rule
                FROM rs_lock_membership_audit
                WHERE session_date = CAST(:d AS date)
                  AND UPPER(symbol) IN :syms
                ORDER BY event_at, id
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"d": session_date, "syms": syms},
        ).fetchall()
        sides: Dict[str, set] = {}
        for r in rows:
            sym = str(r.symbol).upper()
            side = (r.direction or "").upper()
            if side in ("BULL", "BEAR", "LONG", "SHORT"):
                norm = "BULL" if side in ("BULL", "LONG") else "BEAR"
                sides.setdefault(sym, set()).add(norm)
                et = (r.event_type or "").lower()
                rule = (r.rule or "").lower()
                if et == "entry" and (rule == "morning_lock" or sym not in morning):
                    if rule == "morning_lock" or sym not in morning:
                        morning[sym] = norm
        for sym, ss in sides.items():
            if len(ss) >= 2:
                out[sym] = {
                    "unstable": True,
                    "reason": "lock BULL↔BEAR same day",
                }
        for sym, mside in morning.items():
            cur = (current_dirs.get(sym) or "").upper()
            if not cur:
                continue
            cur_n = "BEAR" if cur in ("SHORT", "BEAR") else "BULL"
            if cur_n != mside and not out[sym].get("unstable"):
                out[sym] = {
                    "unstable": True,
                    "reason": f"flipped from morning {mside} to {cur_n}",
                }
    except Exception as exc:
        logger.debug("direction lock-side check skipped: %s", exc)

    try:
        from backend.services.rs_confidence_divergence_lookup import lookup_symbol_day

        for sym in syms:
            if out.get(sym, {}).get("unstable"):
                continue
            try:
                r = lookup_symbol_day(sym, session_date)
                cls = (r or {}).get("classification") or ""
                if cls == FLIP_CLASSIFICATION or "direction flip" in cls.lower():
                    out[sym] = {"unstable": True, "reason": "RS direction flip"}
                    continue
                cps = (r or {}).get("rs_checkpoints") or []
                top_sides = set()
                for cp in cps:
                    s = (cp.get("ranking_type") or cp.get("side") or "").upper()
                    if "BEAR" in s:
                        top_sides.add("BEAR")
                    elif "BULL" in s:
                        top_sides.add("BULL")
                if len(top_sides) >= 2:
                    out[sym] = {
                        "unstable": True,
                        "reason": "Top-5 both sides today",
                    }
            except Exception:
                continue
    except Exception as exc:
        logger.debug("RSCD flip check skipped: %s", exc)
    return out


def stopped_out_today(db, session_date: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """Same-day SL / EMA10-style exit → hard re-entry block both directions."""
    out: Dict[str, Dict[str, Any]] = {}
    if not symbols:
        return out
    syms = [s.upper() for s in symbols]
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(t.underlying) AS underlying, t.direction_type, t.exit_reason,
                       t.exit_time, t.entry_time, t.pnl_rupees, t.order_status
                FROM daily_futures_user_trade t
                LEFT JOIN daily_futures_screening s ON s.id = t.screening_id
                WHERE UPPER(t.underlying) IN :syms
                  AND t.order_status = 'sold'
                  AND (
                        s.trade_date = CAST(:d AS date)
                     OR t.created_at::date = CAST(:d AS date)
                     OR (t.updated_at IS NOT NULL AND t.updated_at::date = CAST(:d AS date))
                  )
                ORDER BY COALESCE(t.updated_at, t.created_at) DESC
                """
            ).bindparams(bindparam("syms", expanding=True)),
            {"d": session_date, "syms": syms},
        ).fetchall()
        for r in rows:
            sym = str(r.underlying).upper()
            if sym in out:
                continue
            reason = (r.exit_reason or "").strip().upper().replace("-", "_").replace("  ", " ")
            reason_compact = reason.replace(" ", "_")
            is_sl = (
                reason_compact in _SL_EXIT_REASONS
                or "SL" in reason
                or "EMA10" in reason
                or "STOP" in reason
            )
            if not is_sl:
                # Treat any same-day closed loss as stop-out for re-entry block
                pnl = _f(r.pnl_rupees)
                if pnl is not None and pnl < 0:
                    is_sl = True
                    reason = reason or "LOSS_EXIT"
            if is_sl:
                out[sym] = {
                    "blocked": True,
                    "exit_reason": r.exit_reason or reason,
                    "exit_time": r.exit_time,
                    "direction": r.direction_type,
                    "label": "SL hit earlier today · no re-entry regardless of direction",
                }
    except Exception as exc:
        logger.debug("stopped-out lookup skipped: %s", exc)

    # Kavach Open Trades panel exits (checklist Take Trade → EXIT)
    try:
        from backend.services.kavach_open_trades import closed_symbols_today

        for sym, meta in (closed_symbols_today(session_date) or {}).items():
            if sym.upper() not in out and sym.upper() in {s.upper() for s in symbols}:
                out[sym.upper()] = meta
    except Exception as exc:
        logger.debug("kavach closed-trade block skipped: %s", exc)
    return out


def apply_state_downgrades(
    *,
    state: str,
    market_regime: str,
    direction_unstable: bool,
    unstable_reason: Optional[str],
    whipsaw_count: int,
    pullback_count: int,
    stopped: Optional[Dict[str, Any]],
) -> Tuple[str, Optional[str], List[str]]:
    """Return (new_state, reason_label, badge_tags)."""
    STATE_READY = "READY"
    STATE_READY_RECHECK = "READY(RECHECK)"
    STATE_WAIT = "WAIT FOR PULLBACK"
    STATE_EXPIRED = "EXPIRED"
    STATE_BLOCKED = "BLOCKED"

    badges: List[str] = []
    reason = None
    st = state

    def _tier_down(s: str) -> str:
        if s in (STATE_READY, STATE_READY_RECHECK):
            return STATE_WAIT
        if s == STATE_WAIT:
            return STATE_EXPIRED
        return s

    if stopped and stopped.get("blocked"):
        badges.append("RE-ENTRY BLOCKED")
        return STATE_BLOCKED, "BLOCKED · " + (stopped.get("label") or "SL hit earlier today"), badges

    if direction_unstable:
        badges.append("DIRECTION UNSTABLE")
        return (
            STATE_BLOCKED,
            "BLOCKED · DIRECTION UNSTABLE" + (f" · {unstable_reason}" if unstable_reason else ""),
            badges,
        )

    if whipsaw_count >= WHIPSAW_REVERSALS:
        badges.append(f"WHIPSAW · {whipsaw_count} reversals")
        if st in (STATE_READY, STATE_READY_RECHECK):
            st = STATE_WAIT
            reason = f"WAIT · WHIPSAW · {whipsaw_count} reversals"

    if pullback_count >= PULLBACK_EXTENDED:
        badges.append(f"{pullback_count}th+ pullback")
        if st in (STATE_READY, STATE_READY_RECHECK):
            st = STATE_WAIT
            reason = "WAIT · EXTENDED — 3rd+ pullback today, historical win rate declining"
    elif pullback_count == 1:
        badges.append("1st pullback")
    elif pullback_count == 2:
        badges.append("2nd pullback")

    if market_regime == REGIME_CHOP:
        badges.append("CHOP DAY")
        prev = st
        st = _tier_down(st)
        if st != prev:
            reason = (reason + " · " if reason else "") + "CHOP DAY tier-down"

    return st, reason, badges

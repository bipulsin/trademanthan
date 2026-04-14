"""
Smart Futures: index alignment (NIFTY + BANKNIFTY) and exit rules (divergence / VWAP / regime).

Used by the picker context and by /daily exit hints for open positions.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

from backend.services.smart_futures_config import (
    ADX_LENGTH,
    TRAIL_LOCK_ATR_MULT,
    TRAIL_STAGE1_ATR_MULT,
    TRAIL_STAGE2_ATR_MULT,
    TRAILING_STOP_ENABLED,
)
from backend.services.smart_futures_picker.indicators import (
    adx_last_two,
    divergence_bundle,
    session_vwap,
    wilder_atr,
    wilder_atr_14,
)

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
NIFTY50_KEY = "NSE_INDEX|Nifty 50"
BANKNIFTY_KEY = "NSE_INDEX|Nifty Bank"


def _ema_last(series: Sequence[float], span: int) -> Optional[float]:
    if not series:
        return None
    k = 2.0 / (float(span) + 1.0)
    e = float(series[0])
    for v in series[1:]:
        e = float(v) * k + e * (1.0 - k)
    return float(e)


def _supertrend_dir_last_two(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 10,
    multiplier: float = 3.0,
) -> Tuple[Optional[int], Optional[int]]:
    """
    Returns (current_dir, previous_dir) where:
    +1 = green/uptrend, -1 = red/downtrend.
    """
    n = len(closes)
    if n < max(20, period + 3):
        return None, None
    fub: List[float] = [0.0] * n
    flb: List[float] = [0.0] * n
    st: List[float] = [0.0] * n
    direction: List[int] = [1] * n
    for i in range(n):
        atr_i = wilder_atr(highs[: i + 1], lows[: i + 1], closes[: i + 1], period)
        if atr_i is None:
            continue
        hl2 = (float(highs[i]) + float(lows[i])) / 2.0
        bub = hl2 + float(multiplier) * float(atr_i)
        blb = hl2 - float(multiplier) * float(atr_i)
        if i == 0:
            fub[i], flb[i], st[i], direction[i] = bub, blb, blb, 1
            continue
        fub[i] = bub if (bub < fub[i - 1] or float(closes[i - 1]) > fub[i - 1]) else fub[i - 1]
        flb[i] = blb if (blb > flb[i - 1] or float(closes[i - 1]) < flb[i - 1]) else flb[i - 1]
        if st[i - 1] == fub[i - 1]:
            st[i] = fub[i] if float(closes[i]) <= fub[i] else flb[i]
        else:
            st[i] = flb[i] if float(closes[i]) >= flb[i] else fub[i]
        direction[i] = 1 if float(closes[i]) >= st[i] else -1
    return int(direction[-1]), int(direction[-2])


def _supertrend_dir_last(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = 10,
    multiplier: float = 3.0,
) -> Optional[int]:
    cur, _ = _supertrend_dir_last_two(highs, lows, closes, period=period, multiplier=multiplier)
    return cur


def _ema_series_last(series: Sequence[float], span: int) -> Optional[float]:
    if not series:
        return None
    k = 2.0 / (float(span) + 1.0)
    e = float(series[0])
    for v in series[1:]:
        e = float(v) * k + e * (1.0 - k)
    return float(e)


def _to_dt(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    except Exception:
        try:
            dt = datetime.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S")
            return IST.localize(dt)
        except Exception:
            return None


def _bucket_15m(dt: datetime) -> datetime:
    m = (dt.minute // 15) * 15
    return dt.replace(minute=m, second=0, microsecond=0)


def build_15m_from_1m(m1: Sequence[dict]) -> List[dict]:
    """
    Build completed 15-minute candles from sorted 1-minute candles.
    """
    out: List[dict] = []
    sorted_1m = sorted([c for c in (m1 or []) if c.get("timestamp")], key=lambda c: str(c.get("timestamp")))
    cur_key: Optional[datetime] = None
    buf: List[dict] = []
    for c in sorted_1m:
        dt = _to_dt(str(c.get("timestamp") or ""))
        if not dt:
            continue
        key = _bucket_15m(dt)
        if cur_key is None:
            cur_key = key
        if key != cur_key:
            if len(buf) >= 1:
                out.append(
                    {
                        "timestamp": buf[-1].get("timestamp"),
                        "open": float(buf[0].get("open") or 0.0),
                        "high": max(float(x.get("high") or 0.0) for x in buf),
                        "low": min(float(x.get("low") or 0.0) for x in buf),
                        "close": float(buf[-1].get("close") or 0.0),
                        "volume": sum(float(x.get("volume") or 0.0) for x in buf),
                    }
                )
            buf = []
            cur_key = key
        buf.append(c)
    if len(buf) >= 1:
        out.append(
            {
                "timestamp": buf[-1].get("timestamp"),
                "open": float(buf[0].get("open") or 0.0),
                "high": max(float(x.get("high") or 0.0) for x in buf),
                "low": min(float(x.get("low") or 0.0) for x in buf),
                "close": float(buf[-1].get("close") or 0.0),
                "volume": sum(float(x.get("volume") or 0.0) for x in buf),
            }
        )
    return out


@dataclass
class ProfitProtectionState:
    entry_price: float
    entry_time: str
    entry_qty: int
    side: str
    atr14_entry: float
    hard_stop_loss: float
    breakeven_activated: bool = False
    breakeven_activation_time: Optional[str] = None
    profit_locking_activated: bool = False
    profit_locking_activation_time: Optional[str] = None
    profit_locking_stop_level: Optional[float] = None
    trailing_stop_activated: bool = False
    trailing_stop_activation_time: Optional[str] = None
    initial_trailing_stop_level: Optional[float] = None
    current_trailing_stop_level: Optional[float] = None
    current_active_stop_loss_level: Optional[float] = None
    max_profit_achieved: float = 0.0


def _favorable_stop(side: str, candidates: Sequence[Optional[float]]) -> Optional[float]:
    vals = [float(x) for x in candidates if x is not None]
    if not vals:
        return None
    return max(vals) if side == "LONG" else min(vals)


def _rupee_profit(side: str, entry: float, px: float, lot: int) -> float:
    if side == "LONG":
        return (float(px) - float(entry)) * float(lot)
    return (float(entry) - float(px)) * float(lot)


def evaluate_exit_with_profit_protection(
    side: str,
    entry_price: float,
    entry_time: str,
    lot_size: int,
    m1_post_entry: Sequence[dict],
) -> Dict[str, Any]:
    """
    Full-position (no partial exits) exit manager:
    - Primary exit on 15m closes
    - Emergency + stop checks on each 1m bar
    - Tiered profit protection + dynamic trailing
    """
    sd = str(side or "").strip().upper()
    if sd not in {"LONG", "SHORT"}:
        return {"exit": False, "reason": "invalid_side"}
    seq1 = sorted(list(m1_post_entry or []), key=lambda c: str(c.get("timestamp") or ""))
    if len(seq1) < 5:
        return {"exit": False, "reason": "insufficient_1m_data"}

    highs1 = [float(c.get("high") or 0.0) for c in seq1]
    lows1 = [float(c.get("low") or 0.0) for c in seq1]
    closes1 = [float(c.get("close") or 0.0) for c in seq1]
    atr14_entry = float(wilder_atr_14(highs1[: min(len(highs1), 30)], lows1[: min(len(lows1), 30)], closes1[: min(len(closes1), 30)]) or 0.0)
    if atr14_entry <= 0:
        atr14_entry = max(0.01, abs(float(entry_price)) * 0.002)
    hard_sl = float(entry_price) - 1.2 * atr14_entry if sd == "LONG" else float(entry_price) + 1.2 * atr14_entry
    st = ProfitProtectionState(
        entry_price=float(entry_price),
        entry_time=str(entry_time),
        entry_qty=max(1, int(lot_size)),
        side=sd,
        atr14_entry=float(atr14_entry),
        hard_stop_loss=float(hard_sl),
        current_active_stop_loss_level=float(hard_sl),
    )

    m15_done: List[dict] = []
    last_15_count = 0
    pending_primary_reason: Optional[str] = None
    last_primary_signal_ts: Optional[str] = None

    for i, c1 in enumerate(seq1):
        ts = str(c1.get("timestamp") or "")
        dt1 = _to_dt(ts)
        if not dt1:
            continue
        h1 = float(c1.get("high") or c1.get("close") or 0.0)
        l1 = float(c1.get("low") or c1.get("close") or 0.0)
        px1 = float(c1.get("close") or 0.0)
        if px1 <= 0:
            continue

        # Update max profit and tier activations
        unreal = _rupee_profit(sd, st.entry_price, px1, st.entry_qty)
        st.max_profit_achieved = max(float(st.max_profit_achieved), float(unreal))
        move = (px1 - st.entry_price) if sd == "LONG" else (st.entry_price - px1)
        if (not st.breakeven_activated) and move >= 0.5 * st.atr14_entry:
            st.breakeven_activated = True
            st.breakeven_activation_time = ts
        if (not st.profit_locking_activated) and move >= 1.0 * st.atr14_entry:
            st.profit_locking_activated = True
            st.profit_locking_activation_time = ts
            st.profit_locking_stop_level = (
                st.entry_price + 0.5 * st.atr14_entry if sd == "LONG" else st.entry_price - 0.5 * st.atr14_entry
            )
        if (not st.trailing_stop_activated) and move >= 1.5 * st.atr14_entry:
            st.trailing_stop_activated = True
            st.trailing_stop_activation_time = ts

        # Rebuild 15m completions from data up to current bar
        m15_now = build_15m_from_1m(seq1[: i + 1])
        if len(m15_now) > last_15_count:
            m15_done = m15_now
            last_15_count = len(m15_now)
            highs15 = [float(x.get("high") or 0.0) for x in m15_done]
            lows15 = [float(x.get("low") or 0.0) for x in m15_done]
            closes15 = [float(x.get("close") or 0.0) for x in m15_done]
            vols15 = [float(x.get("volume") or 0.0) for x in m15_done]
            if len(closes15) >= 10:
                vwap15 = float(session_vwap(highs15, lows15, closes15, vols15))
                ema9_15 = _ema_series_last(closes15, 9)
                atr5_15 = wilder_atr(highs15, lows15, closes15, 5)
                atr14_15 = wilder_atr_14(highs15, lows15, closes15)
                st_dir_15 = _supertrend_dir_last(highs15, lows15, closes15)
                c15 = float(closes15[-1])

                # Update trailing stop on each new 15m close once activated
                if st.trailing_stop_activated and atr14_15 is not None and atr14_15 > 0:
                    candidate = (vwap15 - 0.5 * float(atr14_15)) if sd == "LONG" else (vwap15 + 0.5 * float(atr14_15))
                    if st.current_trailing_stop_level is None:
                        st.current_trailing_stop_level = float(candidate)
                        st.initial_trailing_stop_level = float(candidate)
                    else:
                        if sd == "LONG":
                            st.current_trailing_stop_level = max(float(st.current_trailing_stop_level), float(candidate))
                        else:
                            st.current_trailing_stop_level = min(float(st.current_trailing_stop_level), float(candidate))

                # Primary 15m exit conditions (exit on next 1m candle)
                if sd == "LONG":
                    if (
                        c15 < vwap15
                        and st_dir_15 == -1
                        and ema9_15 is not None
                        and c15 < float(ema9_15)
                    ):
                        pending_primary_reason = "15-min Close Below VWAP+Supertrend+EMA"
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts)
                    elif (
                        atr5_15 is not None
                        and atr14_15 is not None
                        and atr5_15 < atr14_15
                        and c15 < vwap15
                        and st_dir_15 == -1
                    ):
                        pending_primary_reason = "15-min Momentum Weak + Trend Breakdown"
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts)
                else:
                    if (
                        c15 > vwap15
                        and st_dir_15 == 1
                        and ema9_15 is not None
                        and c15 > float(ema9_15)
                    ):
                        pending_primary_reason = "15-min Close Above VWAP+Supertrend+EMA"
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts)
                    elif (
                        atr5_15 is not None
                        and atr14_15 is not None
                        and atr5_15 < atr14_15
                        and c15 > vwap15
                        and st_dir_15 == 1
                    ):
                        pending_primary_reason = "15-min Momentum Weak + Trend Breakdown"
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts)

        # Active stop hierarchy (most favorable)
        be_stop = st.entry_price if st.breakeven_activated else None
        st.current_active_stop_loss_level = _favorable_stop(
            sd,
            [
                st.hard_stop_loss,
                be_stop,
                st.profit_locking_stop_level if st.profit_locking_activated else None,
                st.current_trailing_stop_level if st.trailing_stop_activated else None,
            ],
        )

        # Priority 1: emergency stop
        emergency_stop = st.entry_price - 2.0 * st.atr14_entry if sd == "LONG" else st.entry_price + 2.0 * st.atr14_entry
        if (sd == "LONG" and l1 <= emergency_stop) or (sd == "SHORT" and h1 >= emergency_stop):
            exit_px = emergency_stop
            pnl = _rupee_profit(sd, st.entry_price, exit_px, st.entry_qty)
            roi = (pnl / max(1e-9, st.entry_price * st.entry_qty)) * 100.0
            return {
                "exit": True,
                "final_exit_price": round(float(exit_px), 4),
                "final_exit_time": ts,
                "final_exit_reason": "Emergency Stop Hit (2.0×ATR)",
                "final_exit_profit": round(float(pnl), 2),
                "total_roi_pct": round(float(roi), 4),
                "holding_time_minutes": None,
                "state": st.__dict__,
                "primary_signal_time": last_primary_signal_ts,
            }

        # Priority 2/3: active stop / trailing hit (on 1m)
        if st.current_active_stop_loss_level is not None:
            sl = float(st.current_active_stop_loss_level)
            hit = (sd == "LONG" and l1 <= sl) or (sd == "SHORT" and h1 >= sl)
            if hit:
                reason = "Stop Loss Hit"
                if st.trailing_stop_activated and st.current_trailing_stop_level is not None and abs(sl - float(st.current_trailing_stop_level)) < 1e-8:
                    reason = (
                        "Trailing Stop Hit (15-min VWAP - 0.5×ATR)"
                        if sd == "LONG"
                        else "Trailing Stop Hit (15-min VWAP + 0.5×ATR)"
                    )
                exit_px = sl
                pnl = _rupee_profit(sd, st.entry_price, exit_px, st.entry_qty)
                roi = (pnl / max(1e-9, st.entry_price * st.entry_qty)) * 100.0
                return {
                    "exit": True,
                    "final_exit_price": round(float(exit_px), 4),
                    "final_exit_time": ts,
                    "final_exit_reason": reason,
                    "final_exit_profit": round(float(pnl), 2),
                    "total_roi_pct": round(float(roi), 4),
                    "holding_time_minutes": None,
                    "state": st.__dict__,
                    "primary_signal_time": last_primary_signal_ts,
                }

        # Priority 4: 15m primary signal exits on next 1m candle
        if pending_primary_reason:
            exit_px = px1
            pnl = _rupee_profit(sd, st.entry_price, exit_px, st.entry_qty)
            roi = (pnl / max(1e-9, st.entry_price * st.entry_qty)) * 100.0
            return {
                "exit": True,
                "final_exit_price": round(float(exit_px), 4),
                "final_exit_time": ts,
                "final_exit_reason": pending_primary_reason,
                "final_exit_profit": round(float(pnl), 2),
                "total_roi_pct": round(float(roi), 4),
                "holding_time_minutes": None,
                "state": st.__dict__,
                "primary_signal_time": last_primary_signal_ts,
            }

    # Fallback: close at last bar
    last = seq1[-1]
    last_px = float(last.get("close") or entry_price)
    pnl = _rupee_profit(sd, entry_price, last_px, max(1, int(lot_size)))
    roi = (pnl / max(1e-9, float(entry_price) * max(1, int(lot_size)))) * 100.0
    return {
        "exit": True,
        "final_exit_price": round(float(last_px), 4),
        "final_exit_time": str(last.get("timestamp") or ""),
        "final_exit_reason": "Session End Exit",
        "final_exit_profit": round(float(pnl), 2),
        "total_roi_pct": round(float(roi), 4),
        "holding_time_minutes": None,
        "state": st.__dict__,
        "primary_signal_time": last_primary_signal_ts,
    }


def _sort_candles(candles: Optional[List[dict]]) -> List[dict]:
    if not candles:
        return []
    return sorted(candles, key=lambda c: str(c.get("timestamp") or ""))


def _ist_date_from_ts(ts: str) -> Optional[date]:
    if not ts or len(ts) < 10:
        return None
    try:
        dt = datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.replace(tzinfo=IST).date()
    except ValueError:
        try:
            return datetime.strptime(ts[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def m5_bars_for_session(candles: Optional[List[dict]], session_date: date) -> List[dict]:
    s = _sort_candles(candles)
    return [b for b in s if _ist_date_from_ts(str(b.get("timestamp") or "")) == session_date]


def session_last_close_and_vwap(m5: List[dict]) -> Tuple[Optional[float], Optional[float]]:
    if len(m5) < 10:
        return None, None
    highs = [float(b["high"]) for b in m5]
    lows = [float(b["low"]) for b in m5]
    closes = [float(b["close"]) for b in m5]
    vols = [float(b.get("volume") or 0) for b in m5]
    lc = closes[-1]
    vw = session_vwap(highs, lows, closes, vols)
    return float(lc), float(vw)


def index_session_long_short_flags(
    upstox: Any,
    session_date: date,
    *,
    range_end_date: Optional[date] = None,
    days_back: int = 5,
) -> Tuple[bool, bool]:
    """Fetch NIFTY50 + BANKNIFTY session 5m and return (supports_long, supports_short). Fail-closed."""
    end_d = range_end_date or session_date
    try:
        nm = upstox.get_historical_candles_by_instrument_key(
            NIFTY50_KEY, interval="minutes/5", days_back=days_back, range_end_date=end_d
        )
        bm = upstox.get_historical_candles_by_instrument_key(
            BANKNIFTY_KEY, interval="minutes/5", days_back=days_back, range_end_date=end_d
        )
        n_today = m5_bars_for_session(nm, session_date)
        b_today = m5_bars_for_session(bm, session_date)
        ok, lg, sh = index_alignment_supports(n_today, b_today)
        if not ok:
            return False, False
        return lg, sh
    except Exception as e:
        logger.warning("index_session_long_short_flags: %s", e)
        return False, False


def index_alignment_supports(
    nifty_m5: List[dict],
    bank_m5: List[dict],
) -> Tuple[bool, bool, bool]:
    """
    Returns ``(data_ok, supports_long, supports_short)`` using last close vs session VWAP on each index.
    Long: both closes above VWAP. Short: both below. Strict inequalities.
    """
    nc, nv = session_last_close_and_vwap(nifty_m5)
    bc, bv = session_last_close_and_vwap(bank_m5)
    if nc is None or nv is None or bc is None or bv is None:
        return False, False, False
    sup_long = nc > nv and bc > bv
    sup_short = nc < nv and bc < bv
    return True, sup_long, sup_short


def should_exit_position(
    side: str,
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    vwap: float,
    adx_curr: Optional[float],
    adx_prev: Optional[float],
    atr5: Optional[float],
    atr14: Optional[float],
    md: float,
    rd: float,
    sd: float,
    entry_price: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Multi-factor exit confirmation:
    - Hard exits: VWAP adverse break, supertrend reversal, hard stop-loss.
    - Soft warnings (ATR contraction/divergence) require confirmation.
    """
    sd_u = str(side or "").strip().upper()
    lc = float(closes[-1]) if closes else 0.0
    ll = float(lows[-1]) if lows else lc
    hh = float(highs[-1]) if highs else lc
    div_sum = float(md) + float(rd) + float(sd)
    ema9 = _ema_last(closes, 9)
    st_curr, st_prev = _supertrend_dir_last_two(highs, lows, closes)

    # HARD EXIT 1: price adverse to VWAP
    if sd_u == "LONG" and lc < float(vwap):
        return True, "Exit: VWAP Breakdown"
    if sd_u == "SHORT" and lc > float(vwap):
        return True, "Exit: VWAP Breakout Against Short"

    # HARD EXIT 2: supertrend flip
    if sd_u == "LONG" and st_prev == 1 and st_curr == -1:
        return True, "Exit: Supertrend Reversal"
    if sd_u == "SHORT" and st_prev == -1 and st_curr == 1:
        return True, "Exit: Supertrend Reversal"

    # HARD EXIT 3: hard stop loss hit
    if entry_price is not None and atr14 is not None and atr14 > 0:
        stop_dist = 1.2 * float(atr14)
        if sd_u == "LONG" and ll <= float(entry_price) - stop_dist:
            return True, "Exit: Hard Stop Loss Hit"
        if sd_u == "SHORT" and hh >= float(entry_price) + stop_dist:
            return True, "Exit: Hard Stop Loss Hit"

    atr_contracting = bool(atr5 is not None and atr14 is not None and atr14 > 0 and atr5 < atr14)

    if sd_u == "LONG":
        # SOFT EXIT A: momentum weakening needs VWAP+supertrend confirmation
        if atr_contracting and lc < float(vwap) and st_curr == -1:
            return True, "Exit: Momentum Weakening + VWAP Breakdown"
        # SOFT EXIT B: bearish divergence needs VWAP or EMA9 breach
        if div_sum <= -0.5 and (lc < float(vwap) or (ema9 is not None and lc < float(ema9))):
            if lc < float(vwap):
                return True, "Exit: Bearish Divergence + VWAP Breakdown"
            return True, "Exit: Bearish Divergence + EMA 9 Breach"
    elif sd_u == "SHORT":
        if atr_contracting and lc > float(vwap) and st_curr == 1:
            return True, "Exit: Momentum Weakening + VWAP Breakout"
        if div_sum >= 0.5 and (lc > float(vwap) or (ema9 is not None and lc > float(ema9))):
            if lc > float(vwap):
                return True, "Exit: Bullish Divergence + VWAP Breakout"
            return True, "Exit: Bullish Divergence + EMA 9 Breach"
    return False, ""


def exit_evaluation_from_m5_dicts(
    side: str,
    m5_today: List[dict],
    entry_price: Optional[float] = None,
) -> Tuple[bool, str]:
    """Build OHLC series from session 5m bars and run ``should_exit_position``."""
    if len(m5_today) < 15:
        return False, ""
    highs = [float(b["high"]) for b in m5_today]
    lows = [float(b["low"]) for b in m5_today]
    closes = [float(b["close"]) for b in m5_today]
    vols = [float(b.get("volume") or 0) for b in m5_today]
    vwap = session_vwap(highs, lows, closes, vols)
    atr14 = wilder_atr_14(highs, lows, closes)
    atr5 = wilder_atr(highs, lows, closes, 5)
    adx_c, adx_p = adx_last_two(highs, lows, closes, ADX_LENGTH)
    md, rd, sd = divergence_bundle(highs, lows, closes)
    return should_exit_position(
        side, highs, lows, closes, vwap, adx_c, adx_p, atr5, atr14, md, rd, sd, entry_price=entry_price
    )


def compute_trailing_stop_levels(
    side: str,
    entry_price: float,
    last_close: float,
    atr14: float,
    lot_size: int,
    *,
    current_stop_price: Optional[float] = None,
    stop_stage: Optional[str] = None,
) -> Tuple[float, str]:
    """
    Stage 1: PnL >= TRAIL_STAGE1_ATR_MULT * ATR * lot → stop at entry (breakeven).
    Stage 2: PnL >= TRAIL_STAGE2_ATR_MULT * ATR * lot → trail 1 ATR from entry (favorable side).
    Never loosen stop (only move in favor).
    """
    if not TRAILING_STOP_ENABLED or atr14 <= 0 or lot_size <= 0 or entry_price <= 0:
        return (
            float(current_stop_price or entry_price),
            str(stop_stage or "INITIAL"),
        )
    sd = str(side or "").strip().upper()
    pnl_r = (last_close - entry_price) * lot_size if sd == "LONG" else (entry_price - last_close) * lot_size
    s1 = float(TRAIL_STAGE1_ATR_MULT) * float(atr14) * float(lot_size)
    s2 = float(TRAIL_STAGE2_ATR_MULT) * float(atr14) * float(lot_size)
    lock = float(TRAIL_LOCK_ATR_MULT) * float(atr14)

    cur = float(current_stop_price) if current_stop_price is not None else (
        entry_price - 1.2 * atr14 if sd == "LONG" else entry_price + 1.2 * atr14
    )
    stage = str(stop_stage or "INITIAL")

    if pnl_r >= s2:
        if sd == "LONG":
            new_stop = max(cur, entry_price + lock, entry_price)
        else:
            new_stop = min(cur, entry_price - lock, entry_price)
        return new_stop, "TRAILING"
    if pnl_r >= s1:
        new_stop = entry_price
        if sd == "LONG":
            new_stop = max(cur, entry_price)
        else:
            new_stop = min(cur, entry_price)
        return new_stop, "BREAKEVEN"
    return cur, stage

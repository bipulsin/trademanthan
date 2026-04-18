"""
Smart Futures: index alignment (NIFTY + BANKNIFTY) and exit rules (divergence / VWAP / regime).

Used by the picker context and by /daily exit hints for open positions.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
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


def _as_ist(dt: datetime) -> datetime:
    return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)


def first_15m_bucket_close_after_entry(entry_dt: Optional[datetime]) -> Optional[datetime]:
    """
    IST instant when the 15m candle *that contains* entry_time closes.

    NSE-style alignment: bucket starts at :00/:15/:30/:45; the candle that "opens" at 09:30
    closes at 09:45 (first 1m of the next segment is when the previous bucket is complete).

    Primary / 15m-trailing logic should not run before this time.
    """
    if entry_dt is None:
        return None
    e = _as_ist(entry_dt)
    bk = _bucket_15m(e)
    return bk + timedelta(minutes=15)


def _bucket_5m(dt: datetime) -> datetime:
    m = (dt.minute // 5) * 5
    return dt.replace(minute=m, second=0, microsecond=0)


def first_5m_bucket_close_after_entry(entry_dt: Optional[datetime]) -> Optional[datetime]:
    """
    IST instant when the 5m candle *that contains* entry_time closes (:00/:05/.../:55).
    Emergency and active stops use 5m closes only after this time.
    """
    if entry_dt is None:
        return None
    e = _as_ist(entry_dt)
    bk = _bucket_5m(e)
    return bk + timedelta(minutes=5)


def build_15m_from_5m(m5: Sequence[dict]) -> List[dict]:
    """Build 15-minute candles from sorted 5-minute candles."""
    out: List[dict] = []
    sorted_5m = sorted([c for c in (m5 or []) if c.get("timestamp")], key=lambda c: str(c.get("timestamp")))
    cur_key: Optional[datetime] = None
    buf: List[dict] = []
    for c in sorted_5m:
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
                        "bucket_complete": True,
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
                "bucket_complete": False,
            }
        )
    return out


# Scan-time LTP vs 15m session VWAP (same candles as Smart Futures UI). Higher score = more likely
# VWAP is reclaimed soon — avoid panic exit; lower score forces exit suggestion.
RECLAIM_SCORE_PANIC_THRESHOLD = 45.0

# Entry-time gates (pre-entry quality filter). Signals that fail these should remain
# visible on the UI but must not be promoted to "entry recommended" — they're greyed out.
RECLAIM_ENTRY_SCORE_THRESHOLD = 55.0
ENTRY_TIME_CUTOFF_HHMM = (14, 0)  # hard block for new intraday entries at/after 14:00 IST
ENTRY_OPTIMAL_WINDOW_MINUTES = 5  # operator "enter within" informational window

RECLAIM_SUPPRESSIBLE_EXIT_REASONS = frozenset(
    {
        "15-min Close Below VWAP+Supertrend+EMA",
        "15-min Momentum Weak + Trend Breakdown",
        "15-min Close Above VWAP+Supertrend+EMA",
    }
)


def _is_hard_smart_futures_exit_reason(reason: str) -> bool:
    r = (reason or "").strip()
    if not r:
        return False
    if r.startswith("Emergency"):
        return True
    if r.startswith("Stop Loss Hit"):
        return True
    if r.startswith("Trailing Stop Hit"):
        return True
    return False


def compute_reclaim_probability_score(
    side: str,
    scan_price: float,
    vwap15: float,
    m5_session: Sequence[dict],
    *,
    sector_score: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Heuristic 0–100 score: higher = better odds of reclaiming 15m session VWAP soon.

    Meaningful when price is on the **adverse** side of VWAP (LONG: below; SHORT: above).
    Inputs should match the UI: session 5m bars for microstructure; VWAP from 15m snapshot.
    """
    sd = str(side or "").strip().upper()
    out: Dict[str, Any] = {
        "score": None,
        "vwap_adverse": False,
        "applicable": False,
    }
    if sd not in {"LONG", "SHORT"} or scan_price <= 0 or vwap15 <= 0 or not m5_session:
        return out

    seq = sorted([c for c in m5_session if c.get("timestamp")], key=lambda c: str(c.get("timestamp") or ""))
    if len(seq) < 3:
        return out

    highs5 = [float(c.get("high") or 0.0) for c in seq]
    lows5 = [float(c.get("low") or 0.0) for c in seq]
    closes5 = [float(c.get("close") or 0.0) for c in seq]
    atr14 = float(wilder_atr_14(highs5, lows5, closes5) or 0.0)
    if atr14 <= 0:
        atr14 = max(0.01, abs(float(scan_price)) * 0.002)

    if sd == "LONG":
        adverse = scan_price < vwap15
        gap = float(vwap15) - float(scan_price)
    else:
        adverse = scan_price > vwap15
        gap = float(scan_price) - float(vwap15)

    if not adverse:
        out["applicable"] = True
        out["vwap_adverse"] = False
        return out

    out["applicable"] = True
    out["vwap_adverse"] = True

    gap_atr = gap / atr14
    dist_pts = max(0.0, 40.0 * (1.0 - min(gap_atr / 2.5, 1.0)))

    struct_pts = 0.0
    for c in seq[-2:]:
        h = float(c.get("high") or 0.0)
        l = float(c.get("low") or 0.0)
        cl = float(c.get("close") or 0.0)
        rng = h - l
        if rng <= 1e-12:
            continue
        loc = (cl - l) / rng
        if sd == "LONG":
            if loc >= 0.55:
                struct_pts += 10.0
            elif loc >= 0.4:
                struct_pts += 4.0
        else:
            loc_s = 1.0 - loc
            if loc_s >= 0.55:
                struct_pts += 10.0
            elif loc_s >= 0.4:
                struct_pts += 4.0

    if len(closes5) >= 2:
        if sd == "LONG" and closes5[-1] > closes5[-2]:
            struct_pts += 8.0
        elif sd == "SHORT" and closes5[-1] < closes5[-2]:
            struct_pts += 8.0
    struct_pts = min(25.0, struct_pts)

    mom_pts = 0.0
    if len(closes5) >= 4:
        a, b, c_ = closes5[-3], closes5[-2], closes5[-1]
        if sd == "LONG" and c_ > b > a:
            mom_pts = 15.0
        elif sd == "SHORT" and c_ < b < a:
            mom_pts = 15.0
        elif (sd == "LONG" and c_ > b) or (sd == "SHORT" and c_ < b):
            mom_pts = 8.0

    sect_pts = 10.0
    if sector_score is not None:
        try:
            ss = float(sector_score)
            sect_pts = max(0.0, min(20.0, 10.0 + ss * 10.0))
        except (TypeError, ValueError):
            sect_pts = 10.0

    total = dist_pts + struct_pts + mom_pts + sect_pts
    out["score"] = round(max(0.0, min(100.0, total)), 1)
    return out


def apply_reclaim_vwap_gate(
    *,
    side: str,
    exit_suggested: bool,
    exit_reason: str,
    scan_price: Optional[float],
    vwap15: Optional[float],
    m5_session: Sequence[dict],
    sector_score: Optional[float] = None,
    entry_at: Optional[str] = None,
    scan_time_ist: Optional[datetime] = None,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    After ``evaluate_exit_with_profit_protection``, adjust exit hints when scan price is on the
    wrong side of 15m session VWAP: low reclaim score → force exit (panic); high score →
    suppress soft 15m primary VWAP exits only (never overrides emergency / stops / trailing).

    The **panic** branch only arms after the first 15m bucket that contains ``entry_at`` has
    closed — same gating as the 15m primary exit. Calm suppression is independent of this gate.
    """
    try:
        spx = float(scan_price or 0.0)
        vw = float(vwap15 or 0.0)
    except (TypeError, ValueError):
        return exit_suggested, exit_reason, {
            "score": None,
            "vwap_adverse": False,
            "applicable": False,
            "panic_armed": False,
        }

    detail = compute_reclaim_probability_score(
        side,
        spx,
        vw,
        m5_session,
        sector_score=sector_score,
    )

    entry_dt = _to_dt(str(entry_at or ""))
    first_close = first_15m_bucket_close_after_entry(entry_dt)
    if scan_time_ist is None or first_close is None:
        panic_armed = True
    else:
        st_ist = _as_ist(scan_time_ist)
        panic_armed = st_ist >= first_close
    detail["panic_armed"] = bool(panic_armed)
    if first_close is not None:
        detail["panic_arm_time"] = first_close.isoformat()

    sp = detail.get("score")
    adverse = bool(detail.get("vwap_adverse"))

    ex = bool(exit_suggested)
    reason = str(exit_reason or "")

    if not adverse or sp is None:
        return ex, reason, detail

    if float(sp) >= RECLAIM_SCORE_PANIC_THRESHOLD:
        if ex and (reason in RECLAIM_SUPPRESSIBLE_EXIT_REASONS) and not _is_hard_smart_futures_exit_reason(reason):
            return False, "", detail
        return ex, reason, detail

    if not panic_armed:
        return ex, reason, detail

    if _is_hard_smart_futures_exit_reason(reason) and ex:
        return ex, reason, detail

    panic_reason = (
        "Below 15m VWAP — low reclaim probability (panic)"
        if str(side).strip().upper() == "LONG"
        else "Above 15m VWAP — low reclaim probability (panic)"
    )
    return True, panic_reason, detail


def _bars_upto_ist(m5_session: Sequence[dict], cutoff_ist: Optional[datetime]) -> List[dict]:
    """Return m5 bars whose timestamp is ≤ cutoff_ist (IST). Input bars are timestamp strings."""
    if not m5_session:
        return []
    if cutoff_ist is None:
        return [b for b in m5_session if b.get("timestamp")]
    out: List[dict] = []
    for b in m5_session:
        ts = b.get("timestamp")
        if not ts:
            continue
        dt = _to_dt(str(ts))
        if dt is None:
            continue
        if dt <= cutoff_ist:
            out.append(b)
    return out


def is_entry_permitted(
    *,
    side: str,
    entry_price: Optional[float],
    vwap15: Optional[float],
    entry_at_ist: Optional[datetime],
    m5_session: Sequence[dict],
    sector_score: Optional[float] = None,
    now_ist: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Evaluate the three hard entry gates for a fresh Smart Futures signal.

    - Score gate: reclaim probability ≥ 55 when price is adverse to 15m VWAP at trigger.
      When price is on the favorable side of VWAP the score gate passes automatically
      (reclaim is only meaningful for adverse prices). When m5 is too thin to compute the
      score and price is adverse, gate is treated as **fail** to stay conservative.
    - Time gate: entry_at_ist must be strictly before 14:00 IST on its own session day.
    - VWAP gate: LONG → entry_price > vwap15; SHORT → entry_price < vwap15.

    Velocity and the optional 5-minute window are informational only and returned alongside
    the hard gates so the UI can surface them next to the Order button.
    """
    sd = str(side or "").strip().upper()
    gate_score = True
    gate_time = True
    gate_vwap = True
    reasons: List[str] = []

    trig_ist = _as_ist(entry_at_ist) if entry_at_ist is not None else None
    try:
        px = float(entry_price) if entry_price is not None else None
    except (TypeError, ValueError):
        px = None
    try:
        vw = float(vwap15) if vwap15 is not None else None
    except (TypeError, ValueError):
        vw = None

    score_at_trigger: Optional[float] = None
    adverse_at_trigger = False
    score_applicable = False
    if sd in {"LONG", "SHORT"} and px is not None and vw is not None and px > 0 and vw > 0:
        bars_at_trigger = _bars_upto_ist(m5_session, trig_ist)
        detail = compute_reclaim_probability_score(
            sd,
            px,
            vw,
            bars_at_trigger,
            sector_score=sector_score,
        )
        score_applicable = bool(detail.get("applicable"))
        adverse_at_trigger = bool(detail.get("vwap_adverse"))
        sp = detail.get("score")
        if isinstance(sp, (int, float)):
            score_at_trigger = float(sp)

    if sd in {"LONG", "SHORT"} and px is not None and vw is not None and px > 0 and vw > 0:
        if sd == "LONG":
            gate_vwap = px > vw
            if not gate_vwap:
                reasons.append(
                    "Price below 15m VWAP at trigger — long entry suppressed "
                    f"(entry {px:.2f}, VWAP {vw:.2f})"
                )
        else:
            gate_vwap = px < vw
            if not gate_vwap:
                reasons.append(
                    "Price above 15m VWAP at trigger — short entry suppressed "
                    f"(entry {px:.2f}, VWAP {vw:.2f})"
                )
    else:
        gate_vwap = True

    if adverse_at_trigger:
        if score_at_trigger is None:
            gate_score = False
            reasons.append(
                "Reclaim score not computable at trigger (insufficient 5m data) — weak signal"
            )
        else:
            gate_score = score_at_trigger >= RECLAIM_ENTRY_SCORE_THRESHOLD
            if not gate_score:
                reasons.append(
                    f"Weak signal — reclaim score {score_at_trigger:.0f}/100 "
                    f"(minimum {int(RECLAIM_ENTRY_SCORE_THRESHOLD)} required for entry)"
                )
    else:
        gate_score = True

    if trig_ist is not None:
        cutoff = trig_ist.replace(
            hour=ENTRY_TIME_CUTOFF_HHMM[0],
            minute=ENTRY_TIME_CUTOFF_HHMM[1],
            second=0,
            microsecond=0,
        )
        gate_time = trig_ist < cutoff
        if not gate_time:
            reasons.append(
                f"Late session — entry blocked (triggered at "
                f"{trig_ist.strftime('%H:%M')} IST, cutoff "
                f"{ENTRY_TIME_CUTOFF_HHMM[0]:02d}:{ENTRY_TIME_CUTOFF_HHMM[1]:02d})"
            )

    permitted = bool(gate_score and gate_time and gate_vwap)
    minutes_since_trigger: Optional[float] = None
    if trig_ist is not None and now_ist is not None:
        delta = _as_ist(now_ist) - trig_ist
        minutes_since_trigger = delta.total_seconds() / 60.0

    return {
        "permitted": permitted,
        "gate_score_pass": bool(gate_score),
        "gate_time_pass": bool(gate_time),
        "gate_vwap_pass": bool(gate_vwap),
        "score_at_trigger": score_at_trigger,
        "score_threshold": RECLAIM_ENTRY_SCORE_THRESHOLD,
        "score_applicable": bool(score_applicable),
        "vwap_adverse_at_trigger": bool(adverse_at_trigger),
        "time_cutoff_hhmm": (
            f"{ENTRY_TIME_CUTOFF_HHMM[0]:02d}:{ENTRY_TIME_CUTOFF_HHMM[1]:02d}"
        ),
        "entry_price": px,
        "vwap_at_trigger": vw,
        "reasons": reasons,
        "minutes_since_trigger": minutes_since_trigger,
        "optimal_window_minutes": ENTRY_OPTIMAL_WINDOW_MINUTES,
        "eligible_watchlist": bool(gate_score and gate_vwap and not gate_time),
    }


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
    m5_post_entry: Sequence[dict],
    *,
    m5_pre_entry: Optional[Sequence[dict]] = None,
    force_close_at_end: bool = True,
) -> Dict[str, Any]:
    """
    Full-position (no partial exits) exit manager.

    All **exit decisions** (emergency, active stops, primary) are evaluated only on **completed
    5-minute** candles (closes), sourced directly from the 5m feed.

    - Primary: 15m signal; execution on the **next** completed 5m close after the signal bar.
    - Emergency (2×ATR) and active stops (initial SL, breakeven, profit lock, trailing): vs **5m close**.
    - Tier activations: updated on each **5m close** after the first 5m bucket containing entry closes.

    ``m5_pre_entry``: same-day 5m candles before entry so 15m buckets align with the session.
    """
    sd = str(side or "").strip().upper()
    if sd not in {"LONG", "SHORT"}:
        return {"exit": False, "reason": "invalid_side"}
    seq5 = sorted(list(m5_post_entry or []), key=lambda c: str(c.get("timestamp") or ""))
    if len(seq5) < 2:
        return {"exit": False, "reason": "insufficient_5m_data"}

    highs5 = [float(c.get("high") or 0.0) for c in seq5]
    lows5 = [float(c.get("low") or 0.0) for c in seq5]
    closes5 = [float(c.get("close") or 0.0) for c in seq5]
    atr14_entry = float(wilder_atr_14(highs5[: min(len(highs5), 30)], lows5[: min(len(lows5), 30)], closes5[: min(len(closes5), 30)]) or 0.0)
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

    entry_dt_parsed = _to_dt(str(entry_time))
    first_15m_close_ist = first_15m_bucket_close_after_entry(entry_dt_parsed)
    first_5m_close_ist = first_5m_bucket_close_after_entry(entry_dt_parsed)
    pre5 = sorted([c for c in (m5_pre_entry or []) if c.get("timestamp")], key=lambda c: str(c.get("timestamp")))
    combined = sorted(pre5 + seq5, key=lambda c: str(c.get("timestamp") or ""))
    m5_closed = [b for b in combined if bool(b.get("timestamp"))]
    if not m5_closed:
        return {"exit": False, "reason": "insufficient_5m_data"}

    m15_done: List[dict] = []
    last_closed_15m_count = 0
    pending_primary_reason: Optional[str] = None
    pending_primary_m5_idx: Optional[int] = None
    last_primary_signal_ts: Optional[str] = None

    def _ret_exit(
        exit_px: float,
        ts_out: str,
        reason: str,
    ) -> Dict[str, Any]:
        pnl = _rupee_profit(sd, st.entry_price, exit_px, st.entry_qty)
        roi = (pnl / max(1e-9, st.entry_price * st.entry_qty)) * 100.0
        return {
            "exit": True,
            "final_exit_price": round(float(exit_px), 4),
            "final_exit_time": ts_out,
            "final_exit_reason": reason,
            "final_exit_profit": round(float(pnl), 2),
            "total_roi_pct": round(float(roi), 4),
            "holding_time_minutes": None,
            "state": st.__dict__,
            "primary_signal_time": last_primary_signal_ts,
        }

    emergency_stop = st.entry_price - 2.0 * st.atr14_entry if sd == "LONG" else st.entry_price + 2.0 * st.atr14_entry

    for k, c5 in enumerate(m5_closed):
        ts5 = str(c5.get("timestamp") or "")
        dt5 = _to_dt(ts5)
        if not dt5:
            continue
        close5 = float(c5.get("close") or 0.0)
        if close5 <= 0:
            continue
        combined_upto = [c for c in combined if str(c.get("timestamp") or "") <= ts5]

        allow_5m = first_5m_close_ist is None or (dt5 >= first_5m_close_ist)

        # Tier updates on 5m close (after first 5m bucket close)
        if allow_5m:
            unreal = _rupee_profit(sd, st.entry_price, close5, st.entry_qty)
            st.max_profit_achieved = max(float(st.max_profit_achieved), float(unreal))
            move = (close5 - st.entry_price) if sd == "LONG" else (st.entry_price - close5)
            if (not st.breakeven_activated) and move >= 0.5 * st.atr14_entry:
                st.breakeven_activated = True
                st.breakeven_activation_time = ts5
            if (not st.profit_locking_activated) and move >= 1.0 * st.atr14_entry:
                st.profit_locking_activated = True
                st.profit_locking_activation_time = ts5
                st.profit_locking_stop_level = (
                    st.entry_price + 0.5 * st.atr14_entry if sd == "LONG" else st.entry_price - 0.5 * st.atr14_entry
                )
            if (not st.trailing_stop_activated) and move >= 1.5 * st.atr14_entry:
                st.trailing_stop_activated = True
                st.trailing_stop_activation_time = ts5

        # Snapshot before 15m block clears pending on new closed bar (primary exit is due on this 5m close)
        saved_primary_exit: Optional[str] = None
        if (
            pending_primary_reason
            and pending_primary_m5_idx is not None
            and k == pending_primary_m5_idx + 1
        ):
            saved_primary_exit = pending_primary_reason

        # 15m primary + trailing (completed 15m from real 5m feed only)
        m15_now = build_15m_from_5m(combined_upto)
        closed15 = [b for b in m15_now if bool(b.get("bucket_complete"))]
        if len(closed15) > last_closed_15m_count:
            pending_primary_reason = None
            pending_primary_m5_idx = None
            m15_done = closed15
            last_closed_15m_count = len(closed15)
            allow_15m = first_15m_close_ist is None or (dt5 >= first_15m_close_ist)
            highs15 = [float(x.get("high") or 0.0) for x in m15_done]
            lows15 = [float(x.get("low") or 0.0) for x in m15_done]
            closes15 = [float(x.get("close") or 0.0) for x in m15_done]
            vols15 = [float(x.get("volume") or 0.0) for x in m15_done]
            if allow_15m and len(closes15) >= 10:
                vwap15 = float(session_vwap(highs15, lows15, closes15, vols15))
                ema9_15 = _ema_series_last(closes15, 9)
                atr5_15 = wilder_atr(highs15, lows15, closes15, 5)
                atr14_15 = wilder_atr_14(highs15, lows15, closes15)
                st_dir_15 = _supertrend_dir_last(highs15, lows15, closes15)
                c15 = float(closes15[-1])

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

                if sd == "LONG":
                    if (
                        c15 < vwap15
                        and st_dir_15 == -1
                        and ema9_15 is not None
                        and c15 < float(ema9_15)
                    ):
                        pending_primary_reason = "15-min Close Below VWAP+Supertrend+EMA"
                        pending_primary_m5_idx = k
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts5)
                    elif (
                        atr5_15 is not None
                        and atr14_15 is not None
                        and atr5_15 < atr14_15
                        and c15 < vwap15
                        and st_dir_15 == -1
                    ):
                        pending_primary_reason = "15-min Momentum Weak + Trend Breakdown"
                        pending_primary_m5_idx = k
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts5)
                else:
                    if (
                        c15 > vwap15
                        and st_dir_15 == 1
                        and ema9_15 is not None
                        and c15 > float(ema9_15)
                    ):
                        pending_primary_reason = "15-min Close Above VWAP+Supertrend+EMA"
                        pending_primary_m5_idx = k
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts5)
                    elif (
                        atr5_15 is not None
                        and atr14_15 is not None
                        and atr5_15 < atr14_15
                        and c15 > vwap15
                        and st_dir_15 == 1
                    ):
                        pending_primary_reason = "15-min Momentum Weak + Trend Breakdown"
                        pending_primary_m5_idx = k
                        last_primary_signal_ts = str(m15_done[-1].get("timestamp") or ts5)

        # Exit checks on 5m close after tier + 15m updates (emergency → active → primary)
        if allow_5m:
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
            if (sd == "LONG" and close5 <= emergency_stop) or (sd == "SHORT" and close5 >= emergency_stop):
                return _ret_exit(close5, ts5, "Emergency Stop Hit (2.0×ATR)")
            if st.current_active_stop_loss_level is not None:
                sl = float(st.current_active_stop_loss_level)
                hit = (sd == "LONG" and close5 <= sl) or (sd == "SHORT" and close5 >= sl)
                if hit:
                    reason = "Stop Loss Hit"
                    if (
                        st.trailing_stop_activated
                        and st.current_trailing_stop_level is not None
                        and abs(sl - float(st.current_trailing_stop_level)) < 1e-8
                    ):
                        reason = (
                            "Trailing Stop Hit (15-min VWAP - 0.5×ATR)"
                            if sd == "LONG"
                            else "Trailing Stop Hit (15-min VWAP + 0.5×ATR)"
                        )
                    return _ret_exit(close5, ts5, reason)
            if saved_primary_exit:
                return _ret_exit(close5, ts5, saved_primary_exit)

    # Optional fallback: close at last bar (backtest mode).
    if not force_close_at_end:
        return {
            "exit": False,
            "final_exit_price": None,
            "final_exit_time": None,
            "final_exit_reason": "",
            "final_exit_profit": None,
            "total_roi_pct": None,
            "holding_time_minutes": None,
            "state": st.__dict__,
            "primary_signal_time": last_primary_signal_ts,
        }

    last5 = m5_closed[-1]
    last_px = float(last5.get("close") or entry_price)
    last_ts = str(last5.get("timestamp") or "")
    pnl = _rupee_profit(sd, entry_price, last_px, max(1, int(lot_size)))
    roi = (pnl / max(1e-9, float(entry_price) * max(1, int(lot_size)))) * 100.0
    return {
        "exit": True,
        "final_exit_price": round(float(last_px), 4),
        "final_exit_time": last_ts,
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

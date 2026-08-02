"""Structural Quality composite score (additive v2) for READY NOW promotion.

Total = 0.15*(RS + Garuda + OW + VW + EW) + Grade_Bonus
Grade_Bonus: A+=25, A=20, B=15, C=10, D/D!=0

OW/VW/EW match structural-quality v1.2 (native-10m path). RS = trade_score
(0–100) from rs_universe_score_snapshot; Garuda = LOCF rank_score.
LIVE PROMOTION: see ``structural_quality_ready``.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

from backend.services.kavach_10m import aggregate_10m_bars
from backend.services.kavach_volume import _f, _parse_ist
from backend.services.relative_strength_scanner import _sorted_candles
from backend.services.vajra.indicators import cumulative_vwap, ema_series

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

COMPONENT_WEIGHT = 0.15
SQ_PROMOTE_THRESHOLD = float(os.getenv("SQ_PROMOTE_THRESHOLD", "75"))
SQ_PROMOTE_LIVE = os.getenv("SQ_PROMOTE_LIVE", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

GRADE_BONUS: Dict[str, float] = {
    "A+": 25.0,
    "A": 20.0,
    "A*": 20.0,
    "B": 15.0,
    "B*": 15.0,
    "C": 10.0,
    "D": 0.0,
    "D!": 0.0,
}
STRETCH_CAP_PCT = 6.0


def grade_bonus(grade: Optional[str]) -> float:
    if not grade:
        return 0.0
    g = str(grade).strip().upper()
    if g in GRADE_BONUS:
        return GRADE_BONUS[g]
    base = g.rstrip("!*")
    return float(GRADE_BONUS.get(base, 0.0))


def grade_ab_ok(grade: Optional[str]) -> bool:
    """Confidence grade A+/A/B (stretch ! stripped) — grade-is-the-gate."""
    if not grade:
        return False
    base = str(grade).strip().upper().rstrip("!")
    if base.endswith("*"):
        base = base[:-1]
    return base in ("A+", "A", "B")


def composite_total(
    *,
    rs_score: float,
    garuda_score: float,
    ow: float,
    vw: float,
    ew: float,
    grade: Optional[str],
) -> float:
    return round(
        COMPONENT_WEIGHT
        * (
            float(rs_score)
            + float(garuda_score)
            + float(ow)
            + float(vw)
            + float(ew)
        )
        + grade_bonus(grade),
        4,
    )


def overextension_weight(price: float, session_open: float) -> Tuple[float, float]:
    if session_open is None or session_open <= 0 or price is None:
        return 0.0, 0.0
    stretch = abs(price - session_open) / session_open * 100.0
    ow = max(0.0, 100.0 - (stretch / STRETCH_CAP_PCT) * 100.0)
    return round(stretch, 4), round(ow, 4)


def _side(val: float, ref: float) -> int:
    if val > ref:
        return 1
    if val < ref:
        return -1
    return 0


def classify_vwap_candle(open_: float, close: float, vwap: float, dir_sign: int) -> str:
    if dir_sign == 0:
        return "no_direction"
    so, sc = _side(open_, vwap), _side(close, vwap)
    if so != 0 and sc != 0 and so != sc:
        return "crossed"
    if so == dir_sign and sc == dir_sign:
        return "same_side"
    return "mixed"


def step_vw(
    prev_vw: float,
    *,
    open_: float,
    close: float,
    vwap: float,
    dir_sign: int,
    is_first_candle: bool,
) -> Tuple[float, str]:
    if is_first_candle:
        return 50.0, "baseline"
    cls = classify_vwap_candle(open_, close, vwap, dir_sign)
    if cls == "crossed":
        return 50.0, cls
    if cls == "same_side":
        return float(min(100.0, prev_vw + 10.0)), cls
    return float(prev_vw), cls


EMA_RELIABLE_AFTER_BARS = 0  # removed 2026-08-02: prior-session EMA seed is exact from bar 1


def ema_seeded(values: Sequence[float], period: int, seed: float) -> List[float]:
    """Close-only EMA continuing from ``seed`` (prior-session final EMA)."""
    if not values:
        return []
    k = 2.0 / (max(1, int(period)) + 1.0)
    out: List[float] = []
    ema_v = float(seed)
    for v in values:
        ema_v = float(v) * k + ema_v * (1.0 - k)
        out.append(ema_v)
    return out


def prior_session_10m_ema_seed(
    candles: List[Dict[str, Any]], session_date: str, period: int = 5
) -> Optional[float]:
    """Final close-EMA on aggregated 10m bars strictly before ``session_date``.

    Matches the v1.2 backtest seed: recursive EMA continued from prior history,
    so today's bar-1 EMA is the mathematically correct continuation (no warm-up).
    """
    prior_closes: List[float] = []
    for b in aggregate_10m_bars(_sorted_candles(candles or [])):
        dt = b.get("bar_end")
        if not isinstance(dt, datetime):
            dt = _parse_ist(b.get("timestamp"))
            if dt is not None:
                dt = dt + timedelta(minutes=5)
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        d = dt.strftime("%Y-%m-%d")
        if d < session_date:
            cl = _f(b.get("close"))
            if cl is not None:
                prior_closes.append(float(cl))
        elif d >= session_date:
            break
    if len(prior_closes) < max(1, int(period)):
        return None
    return float(ema_series(prior_closes, period)[-1])


def step_ew_v12(
    state: Dict[str, Any],
    *,
    ema5: float,
    vwap: float,
    dir_sign: int,
    is_first_eval: bool = False,
    ema_reliable: bool = True,
) -> Tuple[float, Optional[str]]:
    """EW arms only on a genuine observed EMA5/VWAP crossover in qualifying direction.

    No ``start_aligned`` shortcut: already-on-side at first bar stays EW=0 until a
    real cross is seen. Bars with ``ema_reliable=False`` never arm/decay/event —
    only silently refresh ``prev_side`` so the first reliable bar does not invent
    a false crossover from an unreliable seed.
    """
    side = _side(ema5, vwap)
    if not ema_reliable:
        if side != 0:
            state["prev_side"] = side
        return float(state.get("ew") or 0.0), None

    # First eval (or any bar with no prior side): seed prev_side only — no free 100.
    if is_first_eval and not state.get("armed"):
        if side != 0:
            state["prev_side"] = side
        return float(state.get("ew") or 0.0), None

    event = None
    prev = int(state.get("prev_side") or 0)
    if prev != 0 and side != 0 and side != prev:
        event = "bullish" if side > 0 else "bearish"
        if not state.get("armed"):
            if dir_sign != 0 and side == dir_sign:
                state["armed"] = True
                state["cross_count"] = 1
                state["ew"] = 100.0
            # opposite first cross: ignore for arming
        else:
            state["cross_count"] = int(state.get("cross_count") or 0) + 1
            state["ew"] = float(max(0.0, float(state.get("ew") or 0.0) - 20.0))
    if side != 0:
        state["prev_side"] = side
    return float(state.get("ew") or 0.0), event


def _dir_sign(side: Optional[str]) -> int:
    s = str(side or "").upper()
    if s in ("LONG", "BULL", "BULLISH"):
        return 1
    if s in ("SHORT", "BEAR", "BEARISH"):
        return -1
    return 0


def enrich_session_10m_bars(
    candles: List[Dict[str, Any]],
    session_date: str,
    *,
    now: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Build today's closed 10m bars with session VWAP + EMA5 from 5m cache candles."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)
    else:
        now = now.astimezone(IST)
    candles = _sorted_candles(candles or [])
    bars_10 = aggregate_10m_bars(candles)
    day: List[Dict[str, Any]] = []
    for b in bars_10:
        dt = b.get("bar_end")
        if not isinstance(dt, datetime):
            dt = _parse_ist(b.get("timestamp"))
            if dt is not None:
                dt = dt + timedelta(minutes=5)
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = IST.localize(dt)
        else:
            dt = dt.astimezone(IST)
        if dt.strftime("%Y-%m-%d") != session_date:
            continue
        if dt > now:
            continue  # forming bar
        o, h, l, c = _f(b.get("open")), _f(b.get("high")), _f(b.get("low")), _f(b.get("close"))
        if None in (o, h, l, c):
            continue
        day.append(
            {
                "bar_end": dt,
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(_f(b.get("volume")) or 0),
            }
        )
    if not day:
        return []
    day.sort(key=lambda x: x["bar_end"])
    closes = [b["close"] for b in day]
    highs = [b["high"] for b in day]
    lows = [b["low"] for b in day]
    vols = [b["volume"] for b in day]
    vwap_s = cumulative_vwap(highs, lows, closes, vols)
    seed5 = prior_session_10m_ema_seed(candles, session_date, 5)
    if seed5 is not None:
        ema5_s = ema_seeded(closes, 5, seed5)
    else:
        # No prior history: cold-start EMA. Buffer stays 0; start_aligned is gone so
        # bar 1 only seeds prev_side — no free EW=100 from an unreliable first print.
        ema5_s = ema_series(closes, 5)
    session_open = float(day[0]["open"])
    out = []
    for i, b in enumerate(day):
        out.append(
            {
                **b,
                "vwap": float(vwap_s[i]),
                "ema5": float(ema5_s[i]),
                "session_open": session_open,
                "bar_hhmm": b["bar_end"].strftime("%H:%M"),
                # Prior-session seed → EMA exact from bar 1; fixed buffer removed.
                "ema_reliable": i >= EMA_RELIABLE_AFTER_BARS,
            }
        )
    return out


def score_bars_through(
    bars: List[Dict[str, Any]],
    *,
    dir_sign: int,
    rs_score: float,
    garuda_score: float,
    grade: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Replay OW/VW/EW across session bars; return latest composite breakdown."""
    if not bars or dir_sign == 0:
        return None
    vw_state = 50.0
    ew_state: Dict[str, Any] = {
        "ew": 0.0,
        "armed": False,
        "cross_count": 0,
        "prev_side": 0,
    }
    last: Optional[Dict[str, Any]] = None
    first_eval = True
    for i, b in enumerate(bars):
        stretch, ow = overextension_weight(float(b["close"]), float(b["session_open"]))
        vw_state, vw_cls = step_vw(
            vw_state,
            open_=float(b["open"]),
            close=float(b["close"]),
            vwap=float(b["vwap"]),
            dir_sign=dir_sign,
            is_first_candle=(i == 0),
        )
        reliable = bool(b.get("ema_reliable", i >= EMA_RELIABLE_AFTER_BARS))
        ew, ew_event = step_ew_v12(
            ew_state,
            ema5=float(b["ema5"]),
            vwap=float(b["vwap"]),
            dir_sign=dir_sign,
            is_first_eval=first_eval,
            ema_reliable=reliable,
        )
        if reliable:
            first_eval = False
        total = composite_total(
            rs_score=rs_score,
            garuda_score=garuda_score,
            ow=ow,
            vw=vw_state,
            ew=ew,
            grade=grade,
        )
        last = {
            "bar_end": b["bar_end"],
            "bar_hhmm": b.get("bar_hhmm"),
            "OW": ow,
            "VW": vw_state,
            "EW": ew,
            "ew_event": ew_event,
            "ew_armed": bool(ew_state.get("armed")),
            "ema_reliable": reliable,
            "vw_classification": vw_cls,
            "stretch_pct": stretch,
            "rs_score": float(rs_score),
            "garuda_score": float(garuda_score),
            "grade_bonus": grade_bonus(grade),
            "confidence_grade": grade,
            "total": total,
            "dir_sign": dir_sign,
        }
    return last


def promote_enabled() -> bool:
    return SQ_PROMOTE_LIVE


def promote_threshold() -> float:
    return SQ_PROMOTE_THRESHOLD

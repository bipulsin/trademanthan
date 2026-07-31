"""Per-symbol chart choppiness (body-cross vs VWAP + EMA5/VWAP crosses).

Replaces the live RS-lock churn *banner* as the session-time chop signal.
Lock-membership cycles remain in ``rs_lock_membership_audit`` for post-session
analysis — they are not this metric.

Condition A (stateful body-cross vs session VWAP on 10m bars):
  - Bootstrap (first 4 session 10m bars): ON only if ≥2 body-crosses with at
    least one bullish AND one bearish.
  - After bootstrap: ON if any body-cross in the last 4 bars; OFF after 5
    consecutive same-side (no-cross) bars; re-triggers on a new cross.

Condition B: cumulative EMA5 vs VWAP line crosses > 2 (i.e. ≥3) same session.

Combined flag = Condition A (current state) OR Condition B.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from backend.services.kavach_10m import aggregate_10m_bars
from backend.services.kavach_volume import _f
from backend.services.relative_strength_scanner import _parse_ist_date, _sorted_candles
from backend.services.vajra.indicators import cumulative_vwap, ema_series

BOOTSTRAP_BARS = 4
ROLLING_CROSS_LOOKBACK = 4
SAME_SIDE_EXIT_BARS = 5
COND_B_CROSS_THRESHOLD = 2  # flag when count > 2 ⇒ ≥3


def _side(price: float, vwap: float) -> int:
    """+1 above VWAP, -1 below, 0 on VWAP."""
    if price > vwap:
        return 1
    if price < vwap:
        return -1
    return 0


def body_cross_event(open_: float, close: float, vwap: float) -> Optional[str]:
    """Intra-bar body cross vs VWAP, or None.

    Bullish: open below, close above. Bearish: open above, close below.
    """
    so, sc = _side(open_, vwap), _side(close, vwap)
    if so < 0 and sc > 0:
        return "bullish"
    if so > 0 and sc < 0:
        return "bearish"
    return None


def body_entire_side(open_: float, close: float, vwap: float) -> int:
    """+1 / -1 if open and close are both strictly on that side; else 0."""
    so, sc = _side(open_, vwap), _side(close, vwap)
    if so > 0 and sc > 0:
        return 1
    if so < 0 and sc < 0:
        return -1
    return 0


@dataclass
class BodyCross:
    bar_idx: int
    timestamp: Optional[str]
    direction: str  # bullish | bearish
    kind: str  # intra_bar | inter_bar
    open: float
    close: float
    vwap: float


@dataclass
class ChopStateSnapshot:
    bar_idx: int
    timestamp: Optional[str]
    cond_a_on: bool
    cond_b_count: int
    cond_b_on: bool
    combined_on: bool
    body_cross: Optional[str] = None
    note: str = ""


@dataclass
class ChopEvaluation:
    session_date: str
    symbol: str
    bars_n: int
    bootstrap_crosses: List[BodyCross] = field(default_factory=list)
    bootstrap_flagged: bool = False
    bootstrap_note: str = ""
    all_body_crosses: List[BodyCross] = field(default_factory=list)
    ema5_vwap_crosses: List[Dict[str, Any]] = field(default_factory=list)
    timeline: List[ChopStateSnapshot] = field(default_factory=list)
    cond_a_final: bool = False
    cond_b_count: int = 0
    cond_b_final: bool = False
    combined_final: bool = False


def session_10m_ohlcv_vwap_ema5(
    candles: List[Dict],
    session_date: str,
) -> List[Dict[str, Any]]:
    """Confirmed session 10m bars with cumulative session VWAP and EMA5(close)."""
    candles = _sorted_candles(candles or [])
    bars_all = aggregate_10m_bars(candles)
    day: List[Dict[str, Any]] = []
    for b in bars_all:
        d = _parse_ist_date(b.get("timestamp"))
        if d == session_date:
            day.append(dict(b))
    if not day:
        return []
    highs = [float(b["high"]) for b in day]
    lows = [float(b["low"]) for b in day]
    closes = [float(b["close"]) for b in day]
    vols = [float(b.get("volume") or 0.0) for b in day]
    vwaps = cumulative_vwap(highs, lows, closes, vols)
    ema5s = ema_series(closes, 5)
    out: List[Dict[str, Any]] = []
    for i, b in enumerate(day):
        out.append(
            {
                **b,
                "vwap": float(vwaps[i]) if i < len(vwaps) else float(b["close"]),
                "ema5": float(ema5s[i]) if i < len(ema5s) else float(b["close"]),
            }
        )
    return out


def _detect_body_crosses(bars: List[Dict[str, Any]]) -> List[BodyCross]:
    """Intra-bar straddles + inter-bar entire-body side flips."""
    events: List[BodyCross] = []
    prev_entire = 0
    for i, b in enumerate(bars):
        o, c, v = float(b["open"]), float(b["close"]), float(b["vwap"])
        ts = str(b.get("timestamp") or b.get("bar_end") or "")
        intra = body_cross_event(o, c, v)
        if intra:
            events.append(
                BodyCross(
                    bar_idx=i,
                    timestamp=ts or None,
                    direction=intra,
                    kind="intra_bar",
                    open=o,
                    close=c,
                    vwap=v,
                )
            )
            prev_entire = 0
            continue
        entire = body_entire_side(o, c, v)
        if prev_entire != 0 and entire != 0 and entire != prev_entire:
            events.append(
                BodyCross(
                    bar_idx=i,
                    timestamp=ts or None,
                    direction="bullish" if entire > 0 else "bearish",
                    kind="inter_bar",
                    open=o,
                    close=c,
                    vwap=v,
                )
            )
        if entire != 0:
            prev_entire = entire
        # Mixed non-straddle (rare): do not update prev_entire
    return events


def _ema5_vwap_crosses(bars: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Line crosses: EMA5 side of VWAP flips bar-to-bar (needs EMA5 warm-up)."""
    out: List[Dict[str, Any]] = []
    prev_side = 0
    for i, b in enumerate(bars):
        e5, v = float(b["ema5"]), float(b["vwap"])
        side = _side(e5, v)
        if i == 0 or side == 0:
            if side != 0:
                prev_side = side
            continue
        if prev_side != 0 and side != prev_side:
            out.append(
                {
                    "bar_idx": i,
                    "timestamp": str(b.get("timestamp") or ""),
                    "direction": "bullish" if side > 0 else "bearish",
                    "ema5": e5,
                    "vwap": v,
                }
            )
        if side != 0:
            prev_side = side
    return out


def evaluate_chart_choppiness(
    candles: List[Dict],
    *,
    session_date: str,
    symbol: str = "",
) -> ChopEvaluation:
    """Run full-session Condition A + B evaluation on 10m bars."""
    bars = session_10m_ohlcv_vwap_ema5(candles, session_date)
    ev = ChopEvaluation(session_date=session_date, symbol=symbol, bars_n=len(bars))
    if len(bars) < 1:
        ev.bootstrap_note = "no_session_10m_bars"
        return ev

    body_crosses = _detect_body_crosses(bars)
    ev.all_body_crosses = body_crosses
    cross_by_bar: Dict[int, BodyCross] = {c.bar_idx: c for c in body_crosses}

    boot = [c for c in body_crosses if c.bar_idx < BOOTSTRAP_BARS]
    ev.bootstrap_crosses = boot
    dirs = {c.direction for c in boot}
    if (
        len(boot) >= 2
        and "bullish" in dirs
        and "bearish" in dirs
    ):
        ev.bootstrap_flagged = True
        ev.bootstrap_note = (
            f"{len(boot)} crosses in first {BOOTSTRAP_BARS} bars "
            f"with both directions ({sorted(dirs)})"
        )
    else:
        ev.bootstrap_flagged = False
        ev.bootstrap_note = (
            f"{len(boot)} cross(es) in first {BOOTSTRAP_BARS} bars; "
            f"dirs={sorted(dirs) or ['none']} — need ≥2 with both bullish+bearish"
        )

    ema_x = _ema5_vwap_crosses(bars)
    ev.ema5_vwap_crosses = ema_x

    # Opposite-direction requirement: bootstrap only (Part 2 default).
    cond_a = False
    same_side_no_cross = 0
    prev_entire_side = 0

    for i, b in enumerate(bars):
        ts = str(b.get("timestamp") or "")
        bc = cross_by_bar.get(i)
        o, c, v = float(b["open"]), float(b["close"]), float(b["vwap"])
        entire = body_entire_side(o, c, v)
        note = ""

        if bc is not None:
            same_side_no_cross = 0
        else:
            if entire != 0 and (prev_entire_side == 0 or entire == prev_entire_side):
                same_side_no_cross += 1
            elif entire != 0 and entire != prev_entire_side:
                # Side flip without counting as body-cross (shouldn't happen if
                # inter_bar detection is consistent) — reset streak.
                same_side_no_cross = 1
            else:
                # Mixed/on-vwap body without a counted cross: break same-side streak.
                same_side_no_cross = 0
        if entire != 0:
            prev_entire_side = entire

        if i < BOOTSTRAP_BARS:
            so_far = [x for x in body_crosses if x.bar_idx <= i]
            dirs_so_far = {x.direction for x in so_far}
            cond_a = (
                len(so_far) >= 2
                and "bullish" in dirs_so_far
                and "bearish" in dirs_so_far
            )
            note = "bootstrap"
        else:
            # Rolling lookback never includes bootstrap bars (0..BOOTSTRAP_BARS-1).
            # Same-direction bootstrap crosses must not leak into the post-bootstrap
            # "last 4" window and flip A ON immediately after a non-chop bootstrap.
            window_start = max(
                BOOTSTRAP_BARS, i - ROLLING_CROSS_LOOKBACK + 1
            )
            recent_cross = any(
                j in cross_by_bar for j in range(window_start, i + 1)
            )
            if recent_cross:
                cond_a = True
                note = "rolling_on" if bc else "hold_on_lookback"
            elif same_side_no_cross >= SAME_SIDE_EXIT_BARS:
                cond_a = False
                note = f"exit_after_{SAME_SIDE_EXIT_BARS}_same_side"
            else:
                note = "hold_on" if cond_a else "hold_off"

        b_count = sum(1 for x in ema_x if int(x["bar_idx"]) <= i)
        b_on = b_count > COND_B_CROSS_THRESHOLD
        ev.timeline.append(
            ChopStateSnapshot(
                bar_idx=i,
                timestamp=ts or None,
                cond_a_on=cond_a,
                cond_b_count=b_count,
                cond_b_on=b_on,
                combined_on=cond_a or b_on,
                body_cross=(bc.direction if bc else None),
                note=note,
            )
        )

    if ev.timeline:
        last = ev.timeline[-1]
        ev.cond_a_final = last.cond_a_on
        ev.cond_b_count = last.cond_b_count
        ev.cond_b_final = last.cond_b_on
        ev.combined_final = last.combined_on
    return ev


def choppiness_summary(ev: ChopEvaluation) -> Dict[str, Any]:
    """JSON-friendly summary for APIs / validation reports."""
    return {
        "symbol": ev.symbol,
        "session_date": ev.session_date,
        "bars_n": ev.bars_n,
        "bootstrap_flagged": ev.bootstrap_flagged,
        "bootstrap_note": ev.bootstrap_note,
        "bootstrap_cross_n": len(ev.bootstrap_crosses),
        "bootstrap_dirs": sorted({c.direction for c in ev.bootstrap_crosses}),
        "body_cross_n": len(ev.all_body_crosses),
        "ema5_vwap_cross_n": len(ev.ema5_vwap_crosses),
        "cond_a_final": ev.cond_a_final,
        "cond_b_count": ev.cond_b_count,
        "cond_b_final": ev.cond_b_final,
        "combined_final": ev.combined_final,
        "cond_a_on_bars": sum(1 for t in ev.timeline if t.cond_a_on),
        "combined_on_bars": sum(1 for t in ev.timeline if t.combined_on),
        "state_toggles": _count_toggles(ev.timeline),
    }


def _count_toggles(timeline: List[ChopStateSnapshot]) -> int:
    n = 0
    prev = None
    for t in timeline:
        if prev is not None and t.combined_on != prev:
            n += 1
        prev = t.combined_on
    return n

"""Kavach unified confidence grade — trade score + volume + VWAP purity.

Stretch penalty (Pine v2.8.7 / v13): nearer of EMA10/VWAP distance as % of price
penalizes Trade Score and letter grade. Live by default; set
``STRETCH_PENALTY_LIVE=0`` to revert to shadow-only.

EMA5 pullback stretch reset (Pine v3.5 parity, live): when price recently
touched/pierced EMA5 (bar range includes EMA5), stretch penalties are reduced
for ``STRETCH_EMA5_RESET_BARS`` bars (default 2). Mode via
``STRETCH_EMA5_RESET_MODE``: ``off`` | ``partial`` (50%, default) | ``full``.
"""
from __future__ import annotations

import math
import os
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

REGIME_TREND = "TREND"
REGIME_TRANSITION = "TRANSITION"

VolumeLabel = Literal["High", "Average", "Low"]
PurityProven = bool  # >= 60%

DEFAULT_SOFT_STRETCH_PCT = 0.35
DEFAULT_HARD_STRETCH_PCT = 0.50
DEFAULT_EMA5_RESET_MODE = "partial"  # match Pine default; not Off
DEFAULT_EMA5_RESET_BARS = 2
DEFAULT_EMA5_RESET_PARTIAL_FACTOR = 0.50

_LETTER_RANK = {"A+": 0, "A": 1, "B": 2, "C": 3, "D": 4}
_RANK_LETTER = {0: "A+", 1: "A", 2: "B", 3: "C", 4: "D"}


def soft_stretch_pct() -> float:
    raw = os.environ.get("SOFT_STRETCH_PCT", str(DEFAULT_SOFT_STRETCH_PCT))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return DEFAULT_SOFT_STRETCH_PCT


def hard_stretch_pct() -> float:
    raw = os.environ.get("HARD_STRETCH_PCT", str(DEFAULT_HARD_STRETCH_PCT))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return DEFAULT_HARD_STRETCH_PCT


def stretch_penalty_live_enabled() -> bool:
    """When True, written trade_score / confidence_grade are post-stretch.

    Default on after shadow review. Set ``STRETCH_PENALTY_LIVE=0`` to disable.
    """
    return os.environ.get("STRETCH_PENALTY_LIVE", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def compute_vwap_purity_pct(
    closes: List[float],
    vwap_series: List[float],
    *,
    direction: str,
    num_bars: int = 8,
    bar_size: int = 2,
) -> float:
    """Count last ``num_bars`` completed synthetic 10m bars on signal side of VWAP.

    Resamples ``bar_size`` consecutive closes (default 2×5m → 10m). Uses bar close
    vs session VWAP at that bar's end.
    """
    if not closes or not vwap_series or len(closes) != len(vwap_series):
        return 0.0
    is_long = (direction or "LONG").upper() != "SHORT"
    n = len(closes)
    if n < bar_size:
        return 0.0
    # Build 10m-equivalent bar indices (end index of each pair).
    ends: List[int] = []
    i = bar_size - 1
    while i < n:
        ends.append(i)
        i += bar_size
    if not ends:
        return 0.0
    sample = ends[-num_bars:]
    on_side = 0
    for idx in sample:
        c = closes[idx]
        v = vwap_series[idx]
        if v <= 0:
            continue
        if is_long and c > v:
            on_side += 1
        elif not is_long and c < v:
            on_side += 1
    return round(on_side / len(sample) * 100.0, 1) if sample else 0.0


def detect_market_regime(
    *,
    st_prev: Optional[bool],
    st_curr: Optional[bool],
    macd_prev: float,
    macd_sig_prev: float,
    macd_curr: float,
    macd_sig_curr: float,
    ema5_prev: float,
    vwap_prev: float,
    ema5_curr: float,
    vwap_curr: float,
) -> str:
    """TRANSITION if ST, MACD, or EMA/VWAP relationship flipped this bar."""
    if st_prev is not None and st_curr is not None and st_prev != st_curr:
        return REGIME_TRANSITION
    macd_bull_prev = macd_prev > macd_sig_prev
    macd_bull_curr = macd_curr > macd_sig_curr
    if macd_bull_prev != macd_bull_curr:
        return REGIME_TRANSITION
    ema_above_prev = ema5_prev > vwap_prev if vwap_prev else False
    ema_above_curr = ema5_curr > vwap_curr if vwap_curr else False
    if ema_above_prev != ema_above_curr:
        return REGIME_TRANSITION
    return REGIME_TREND


def _purity_proven(purity_pct: float) -> bool:
    return purity_pct >= 60.0


# Same window length as compute_vwap_purity_pct (Layer 3 VWAP consistency).
VWAP_CONSISTENCY_BARS = 8


def vwap_opposite_side_consecutive(
    closes: List[float],
    vwap_series: List[float],
    *,
    lock_direction: str,
    num_bars: int = VWAP_CONSISTENCY_BARS,
    bar_size: int = 2,
) -> bool:
    """True when last ``num_bars`` completed 10m closes are ALL opposite VWAP vs lock.

    Reuses the same N / bar-resampling definition as :func:`compute_vwap_purity_pct`.
    Opposite = below VWAP for LONG/BULL lock, above VWAP for SHORT/BEAR lock.
    Confirmed closes only (caller must pass closed-bar series).
    """
    if not closes or not vwap_series or len(closes) != len(vwap_series):
        return False
    is_long = (lock_direction or "LONG").upper() not in ("SHORT", "BEAR", "BEARISH")
    n = len(closes)
    if n < bar_size:
        return False
    ends: List[int] = []
    i = bar_size - 1
    while i < n:
        ends.append(i)
        i += bar_size
    if len(ends) < num_bars:
        return False
    sample = ends[-num_bars:]
    for idx in sample:
        c = closes[idx]
        v = vwap_series[idx]
        if v is None or v <= 0:
            return False
        if is_long:
            # Opposite of LONG lock = close below VWAP
            if not (c < v):
                return False
        else:
            # Opposite of SHORT lock = close above VWAP
            if not (c > v):
                return False
    return True


def compute_stretch_pct(
    close: Optional[float],
    ema10: Optional[float],
    vwap: Optional[float],
) -> Optional[float]:
    """Nearer-stop stretch as % of price. None when levels are missing/invalid."""
    try:
        c = float(close) if close is not None else None
        e = float(ema10) if ema10 is not None else None
        v = float(vwap) if vwap is not None else None
    except (TypeError, ValueError):
        return None
    if c is None or c <= 0 or e is None or v is None:
        return None
    if math.isnan(c) or math.isnan(e) or math.isnan(v):
        return None
    if math.isinf(c) or math.isinf(e) or math.isinf(v):
        return None
    nearer = min(abs(c - e), abs(c - v))
    return round(nearer / c * 100.0, 4)


def stretch_penalties(
    stretch_pct: Optional[float],
    *,
    soft: Optional[float] = None,
    hard: Optional[float] = None,
) -> Tuple[int, int]:
    """Return (score_penalty, letter_penalty) where letter is 0, 2, or 99 (force D)."""
    soft_v = soft if soft is not None else soft_stretch_pct()
    hard_v = hard if hard is not None else hard_stretch_pct()
    if stretch_pct is None:
        return 0, 0
    try:
        sp = float(stretch_pct)
    except (TypeError, ValueError):
        return 0, 0
    if math.isnan(sp) or sp < 0:
        return 0, 0
    if sp > hard_v:
        return 50, 99
    if sp > soft_v:
        return 20, 2
    return 0, 0


def stretch_ema5_reset_mode() -> str:
    """Pine v3.5 stretch-reset mode: off | partial | full.

    Default ``partial`` (50% reduction) — do not default to off (dashboard↔TV parity).
    Override with ``STRETCH_EMA5_RESET_MODE``.
    """
    raw = os.environ.get("STRETCH_EMA5_RESET_MODE", DEFAULT_EMA5_RESET_MODE).strip().lower()
    if raw in ("off", "0", "false", "none"):
        return "off"
    if raw in ("full", "reset", "1"):
        return "full"
    return "partial"


def stretch_ema5_reset_bars() -> int:
    raw = os.environ.get("STRETCH_EMA5_RESET_BARS", str(DEFAULT_EMA5_RESET_BARS))
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_EMA5_RESET_BARS


def stretch_ema5_reset_partial_factor() -> float:
    raw = os.environ.get(
        "STRETCH_EMA5_RESET_PARTIAL_FACTOR", str(DEFAULT_EMA5_RESET_PARTIAL_FACTOR)
    )
    try:
        return min(1.0, max(0.0, float(raw)))
    except (TypeError, ValueError):
        return DEFAULT_EMA5_RESET_PARTIAL_FACTOR


def _f_num(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def _ema_series(closes: Sequence[float], length: int = 5) -> List[float]:
    if not closes:
        return []
    k = 2.0 / (length + 1.0)
    out: List[float] = []
    ema = float(closes[0])
    for c in closes:
        ema = float(c) * k + ema * (1.0 - k)
        out.append(ema)
    return out


def detect_ema5_touch_reset(
    candles: Optional[Sequence[Dict[str, Any]]],
    *,
    persist_bars: Optional[int] = None,
) -> Dict[str, Any]:
    """True when a confirmed bar's range includes EMA5 within persist window.

    ``candles`` are chronological OHLC dicts (uses high/low/close). Last bar is
    treated as the evaluation bar (confirmed / closed in caller context).
    """
    persist = stretch_ema5_reset_bars() if persist_bars is None else int(persist_bars)
    mode = stretch_ema5_reset_mode()
    empty = {
        "ema5_reset_active": False,
        "ema5_reset_mode": mode,
        "ema5_reset_bars_since_touch": None,
        "ema5_reset_persist_bars": persist,
        "ema5_reset_factor": 1.0,
        "ema5_at_touch": None,
    }
    if mode == "off" or not candles or persist < 0:
        return empty

    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    for c in candles:
        cl = _f_num(c.get("close"))
        hi = _f_num(c.get("high"))
        lo = _f_num(c.get("low"))
        if cl is None:
            continue
        closes.append(cl)
        highs.append(hi if hi is not None else cl)
        lows.append(lo if lo is not None else cl)
    if len(closes) < 5:
        return empty

    ema5 = _ema_series(closes, 5)
    last_i = len(closes) - 1
    touch_i = None
    for i in range(last_i, -1, -1):
        e = ema5[i]
        if lows[i] <= e <= highs[i]:
            touch_i = i
            break
    if touch_i is None:
        return empty

    bars_since = last_i - touch_i
    active = bars_since <= persist
    if not active:
        return {
            **empty,
            "ema5_reset_bars_since_touch": bars_since,
            "ema5_at_touch": round(ema5[touch_i], 4),
        }

    if mode == "full":
        factor = 0.0
    else:
        factor = stretch_ema5_reset_partial_factor()
    return {
        "ema5_reset_active": True,
        "ema5_reset_mode": mode,
        "ema5_reset_bars_since_touch": bars_since,
        "ema5_reset_persist_bars": persist,
        "ema5_reset_factor": factor,
        "ema5_at_touch": round(ema5[touch_i], 4),
    }


def apply_ema5_reset_to_penalties(
    score_pen: int,
    letter_pen: int,
    reset: Dict[str, Any],
) -> Tuple[int, int]:
    """Scale stretch penalties by EMA5 reset factor (partial/full)."""
    if not reset.get("ema5_reset_active"):
        return score_pen, letter_pen
    try:
        factor = float(reset.get("ema5_reset_factor", 1.0))
    except (TypeError, ValueError):
        factor = 1.0
    if factor >= 1.0:
        return score_pen, letter_pen
    if factor <= 0.0:
        return 0, 0
    # Partial: scale score pts; letter 99 (hard D) → treat as soft 2-step then scale.
    new_score = int(round(score_pen * factor))
    base_letter = 2 if letter_pen >= 99 else max(0, int(letter_pen))
    new_letter = max(0, int(round(base_letter * factor)))
    return new_score, new_letter


def _downgrade_letters(grade: str, steps: int) -> str:
    idx = _LETTER_RANK.get((grade or "D").replace("*", "").replace("!", ""), 4)
    return _RANK_LETTER[min(4, idx + max(0, int(steps)))]


def _band_base_grade(
    s: int,
    vol: VolumeLabel,
    pure: bool,
) -> Tuple[str, str]:
    """Score/volume/purity banding only (no transition floor, no stretch letter)."""
    if s < 65:
        return "D", "score_lt_65"
    if vol == "Low" and not pure:
        return "D", "low_vol_not_pure"
    if vol == "High" and pure and s >= 95:
        return "A+", "high_pure_score_ge_95"
    if vol == "High" and pure and s >= 85:
        return "A", "high_pure_score_ge_85"
    if vol == "High" and pure and s >= 75:
        return "B", "high_pure_score_ge_75"
    if vol == "Average" and pure and s >= 85:
        return "B", "avg_pure_score_ge_85"
    if vol == "High" and not pure and s >= 85:
        return "C", "high_not_pure_score_ge_85"
    if vol == "Average" and pure and s >= 75:
        return "C", "avg_pure_score_ge_75"
    if vol == "Low" and pure and s >= 85:
        return "C", "low_pure_score_ge_85"
    return "D", "else_D"


def compute_confidence_grade(
    score: float,
    volume_label: VolumeLabel,
    purity_pct: float,
    regime: str = REGIME_TREND,
    *,
    close: Optional[float] = None,
    ema10: Optional[float] = None,
    vwap: Optional[float] = None,
    stretch_pct: Optional[float] = None,
) -> Tuple[str, bool]:
    """Return (grade, transition_floor_applied).

    Grades: A+, A, B, C, D. Display C* when transition floor applied.
    """
    explained = explain_confidence_grade(
        score,
        volume_label,
        purity_pct,
        regime,
        close=close,
        ema10=ema10,
        vwap=vwap,
        stretch_pct=stretch_pct,
    )
    return explained["grade"], bool(explained["transition_floor"])


def explain_confidence_grade(
    score: float,
    volume_label: VolumeLabel,
    purity_pct: float,
    regime: str = REGIME_TREND,
    *,
    close: Optional[float] = None,
    ema10: Optional[float] = None,
    vwap: Optional[float] = None,
    stretch_pct: Optional[float] = None,
    soft: Optional[float] = None,
    hard: Optional[float] = None,
    apply_live: Optional[bool] = None,
    candles: Optional[Sequence[Dict[str, Any]]] = None,
    ema5_reset: Optional[Dict[str, Any]] = None,
) -> dict:
    """Banding + stretch penalty (Pine v13) + EMA5 pullback reset (Pine v3.5).

    When ``apply_live`` is False (or env ``STRETCH_PENALTY_LIVE=0``),
    ``grade`` / ``score_int`` / ``display_grade`` stay on the pre-stretch path.
    """
    raw_s = int(round(score))
    vol = volume_label
    pure = _purity_proven(purity_pct)
    soft_v = soft if soft is not None else soft_stretch_pct()
    hard_v = hard if hard is not None else hard_stretch_pct()
    live = stretch_penalty_live_enabled() if apply_live is None else bool(apply_live)

    if stretch_pct is None and (close is not None or ema10 is not None or vwap is not None):
        stretch_pct = compute_stretch_pct(close, ema10, vwap)

    score_pen, letter_pen = stretch_penalties(stretch_pct, soft=soft_v, hard=hard_v)
    reset_info = ema5_reset if ema5_reset is not None else detect_ema5_touch_reset(candles)
    score_pen_pre_reset, letter_pen_pre_reset = score_pen, letter_pen
    score_pen, letter_pen = apply_ema5_reset_to_penalties(score_pen, letter_pen, reset_info)
    post_s = max(0, raw_s - score_pen)

    base_pre, rule_pre = _band_base_grade(raw_s, vol, pure)
    would_tf_pre = (
        base_pre == "D" and regime == REGIME_TRANSITION and raw_s >= 75
    )
    grade_pre = "C" if would_tf_pre else base_pre
    floor_pre = would_tf_pre

    base_from_post_score, rule_post = _band_base_grade(post_s, vol, pure)
    # letter_pen: 99 = hard force-D; 1–2 = soft steps (EMA5 partial may yield 1).
    if letter_pen >= 99:
        base_stretched = "D"
        stretch_letter_steps = 2  # Pine still applies soft +2 before hard force
    elif letter_pen > 0:
        stretch_letter_steps = int(letter_pen)
        base_stretched = _downgrade_letters(base_from_post_score, stretch_letter_steps)
    else:
        base_stretched = base_from_post_score
        stretch_letter_steps = 0

    # Hard stretch = pre-reset hard band still active (full force-D), not partial-softened.
    hard_stretch = (
        stretch_pct is not None
        and stretch_pct > hard_v
        and letter_pen >= 99
    )
    # Stretch before transition-floor; hard stretch never rescued to C*.
    transition_floor_post = (
        base_stretched == "D"
        and regime == REGIME_TRANSITION
        and post_s >= 75
        and not hard_stretch
    )
    grade_post = "C" if transition_floor_post else base_stretched
    rule_final_post = (
        "transition_floor_to_C" if transition_floor_post else rule_post
    )
    if letter_pen > 0 and not transition_floor_post:
        if letter_pen >= 99:
            rule_final_post = "stretch_hard_force_D"
        else:
            rule_final_post = "stretch_soft_letter_downgrade"

    stretch_marked = stretch_letter_steps > 0
    display_pre = format_confidence_display(grade_pre, floor_pre, stretch_marked=False)
    display_post = format_confidence_display(
        grade_post, transition_floor_post, stretch_marked=stretch_marked
    )

    if live:
        grade = grade_post
        transition_floor = transition_floor_post
        score_int = post_s
        display_grade = display_post
        banding_rule = (
            "transition_floor_to_C" if transition_floor_post else rule_final_post
        )
        if would_tf_pre and not transition_floor_post and hard_stretch:
            banding_rule = "stretch_blocks_transition_floor"
    else:
        grade = grade_pre
        transition_floor = floor_pre
        score_int = raw_s
        display_grade = display_pre
        banding_rule = "transition_floor_to_C" if floor_pre else rule_pre

    stretch_block = {
        "stretch_pct": stretch_pct,
        "stretch_score_penalty": score_pen,
        "stretch_letter_penalty": letter_pen,
        "stretch_score_penalty_pre_ema5_reset": score_pen_pre_reset,
        "stretch_letter_penalty_pre_ema5_reset": letter_pen_pre_reset,
        "trade_score_pre_stretch": raw_s,
        "trade_score_post_stretch": post_s,
        "base_grade_pre_stretch": display_pre,
        "base_grade_post_stretch": display_post,
        "promote_transition_floor_would_have_fired_pre_penalty": would_tf_pre,
        "soft_stretch_pct": soft_v,
        "hard_stretch_pct": hard_v,
        "stretch_penalty_live": live,
        "base_grade_from_post_score": base_from_post_score,
        "base_grade_stretched": base_stretched,
        **{k: reset_info.get(k) for k in (
            "ema5_reset_active",
            "ema5_reset_mode",
            "ema5_reset_bars_since_touch",
            "ema5_reset_persist_bars",
            "ema5_reset_factor",
            "ema5_at_touch",
        )},
    }

    return {
        "grade": grade,
        "display_grade": display_grade,
        "transition_floor": transition_floor,
        "banding_rule": banding_rule,
        "score_int": score_int,
        "volume_label": vol,
        "purity_pct": float(purity_pct) if purity_pct is not None else None,
        "purity_proven": pure,
        "regime": regime,
        "stretch": stretch_block,
        # Flat aliases for log writers
        **{k: stretch_block[k] for k in (
            "stretch_pct",
            "stretch_score_penalty",
            "stretch_letter_penalty",
            "trade_score_pre_stretch",
            "trade_score_post_stretch",
            "base_grade_pre_stretch",
            "base_grade_post_stretch",
            "promote_transition_floor_would_have_fired_pre_penalty",
        )},
    }


def resolve_score_and_grade(
    raw_score: float,
    volume_label: VolumeLabel,
    purity_pct: float,
    regime: str = REGIME_TREND,
    *,
    close: Optional[float] = None,
    ema10: Optional[float] = None,
    vwap: Optional[float] = None,
    stretch_pct: Optional[float] = None,
    apply_live: Optional[bool] = None,
    candles: Optional[Sequence[Dict[str, Any]]] = None,
    ema5_reset: Optional[Dict[str, Any]] = None,
) -> dict:
    """One-shot: raw Trade Score → live-aware score + display grade + stretch dict."""
    explained = explain_confidence_grade(
        raw_score,
        volume_label,
        purity_pct,
        regime,
        close=close,
        ema10=ema10,
        vwap=vwap,
        stretch_pct=stretch_pct,
        apply_live=apply_live,
        candles=candles,
        ema5_reset=ema5_reset,
    )
    return {
        "trade_score": int(explained["score_int"]),
        "confidence_grade": explained["display_grade"],
        "grade": explained["grade"],
        "transition_floor": bool(explained["transition_floor"]),
        "stretch": explained.get("stretch") or {},
        "explained": explained,
    }


def format_confidence_display(
    grade: str,
    transition_floor: bool,
    stretch_marked: bool = False,
) -> str:
    if transition_floor and grade == "C":
        return "C*"
    if stretch_marked and grade:
        return f"{grade}!"
    return grade


def format_quality_row(
    volume_label: VolumeLabel,
    purity_pct: float,
    score: float,
    regime: str,
) -> str:
    vol_s = "Vol+" if volume_label == "High" else ("Vol~" if volume_label == "Average" else "Vol-")
    pure_s = "Pure+" if _purity_proven(purity_pct) else "Pure-"
    regime_s = " TRANS" if regime == REGIME_TRANSITION else ""
    return f"{vol_s} {pure_s} {int(round(score))}{regime_s}".strip()


def confidence_passes_gate(
    grade: str,
    *,
    counter_rs: bool = False,
    maturity_a_only: bool = False,
    climactic: bool = False,
) -> bool:
    """Whether confidence satisfies checklist gate for with-RS entries."""
    g = (grade or "").replace("*", "").replace("!", "").upper()
    if g == "D":
        return False
    if counter_rs or maturity_a_only or climactic:
        return g in ("A+", "A")
    return g in ("A+", "A", "B", "C")

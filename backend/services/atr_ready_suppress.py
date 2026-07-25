"""ATR-consumed READY display suppress (Part 1) + DI-override shadow (Part 2).

Part 1 (toggleable live display-only):
  Locked/promoted READY-family → WATCHING when atr_consumed_pct ≥ 85% and
  progression is NOT increasing. Does not touch ranking, lock, or promotion.
  Always shadow-logs evaluation into kavach_ready_consistency_log.inputs.

Part 2 (shadow only — never live):
  would_override_di when grade A/A+, READY-family, no hard gate, and the only
  soft blocker is direction_imbalance (warning_stack excluded).

Toggle: ATR_READY_SUPPRESS_LIVE=1 (default on). Set 0/false/off to disable
display mutation instantly; shadow fields still log.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

ATR_READY_SUPPRESS_THRESHOLD_PCT = 85.0
PROGRESSION_NOISE_PP = 0.5  # match scripts/backtest_scoring_gate_v2.py

STATE_READY = "READY"
STATE_READY_RECHECK = "READY(RECHECK)"
STATE_WATCHING = "WATCHING"
READY_FAMILY = (STATE_READY, STATE_READY_RECHECK)

_TRUE = ("1", "true", "yes", "on")


def atr_ready_suppress_live_enabled() -> bool:
    """When True, READY→WATCHING display mutation is applied.

    Default on after scoring-gate v2 GO. Set ``ATR_READY_SUPPRESS_LIVE=0`` to
    disable instantly (shadow logging continues).
    """
    return os.environ.get("ATR_READY_SUPPRESS_LIVE", "1").strip().lower() in _TRUE


def atr_ready_suppress_threshold_pct() -> float:
    raw = os.environ.get("ATR_READY_SUPPRESS_THRESHOLD_PCT", str(ATR_READY_SUPPRESS_THRESHOLD_PCT))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return ATR_READY_SUPPRESS_THRESHOLD_PCT


def is_ready_family(state: Optional[str]) -> bool:
    s = (state or "").upper()
    return s.startswith("READY")


def progression_increasing(
    hist_ac: Sequence[Optional[float]],
    hist_signed: Sequence[Optional[float]],
    direction: Optional[str],
) -> bool:
    """True if atr_consumed rising vs 1-2 bars prior AND move still with direction.

    Matches ``scripts/backtest_scoring_gate_v2.progression_increasing``.
    """
    if len(hist_ac) < 2:
        return False
    cur, prev = hist_ac[-1], hist_ac[-2]
    if cur is None or prev is None:
        return False
    rising = cur > prev + PROGRESSION_NOISE_PP
    if len(hist_ac) >= 3 and hist_ac[-3] is not None:
        rising = rising or (cur > hist_ac[-3] + PROGRESSION_NOISE_PP and cur >= prev)
    if not rising:
        return False
    if not hist_signed or hist_signed[-1] is None:
        return rising
    sig = hist_signed[-1]
    if (direction or "LONG").upper() == "SHORT":
        return sig < 0
    return sig > 0


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        if x != x:  # NaN
            return None
        return x
    except (TypeError, ValueError):
        return None


def atr_consumed_pct_from_bar(
    *,
    close: Optional[float],
    session_open: Optional[float],
    atr_pct: Optional[float],
) -> Optional[float]:
    """% of daily ATR (atr14_pct × open) consumed from session open — same as backtest derive."""
    if close is None or session_open is None or not atr_pct or atr_pct <= 0:
        return None
    atr_pts = atr_pct / 100.0 * session_open
    if atr_pts <= 0:
        return None
    return abs(close - session_open) / atr_pts * 100.0


def build_atr_progression_hist(
    candles: List[Dict[str, Any]],
    *,
    session_open: Optional[float],
    atr_pct: Optional[float],
    live_atr_consumed_pct: Optional[float] = None,
    live_price: Optional[float] = None,
) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    """Build atr_consumed + signed-move histories from closed 10m bars.

    Appends the live production atr_consumed_pct when it differs from the last bar
    (same pattern as the v2 backtest).
    """
    hist_ac: List[Optional[float]] = []
    hist_signed: List[Optional[float]] = []
    if not candles or session_open is None:
        if live_atr_consumed_pct is not None:
            hist_ac.append(live_atr_consumed_pct)
            mv = None
            if live_price is not None and session_open:
                mv = (live_price - session_open) / session_open * 100.0
            hist_signed.append(mv)
        return hist_ac, hist_signed

    try:
        from backend.services.kavach_10m import (
            aggregate_10m_bars,
            last_closed_10m_pair_end_idx,
        )

        pair_end = last_closed_10m_pair_end_idx(candles)
        bars = (
            [b for b in aggregate_10m_bars(candles) if b["end_5m_idx"] <= pair_end]
            if pair_end >= 0
            else []
        )
    except Exception as exc:
        logger.debug("atr progression hist bars skipped: %s", exc)
        bars = []

    for b in bars:
        close = _f(b.get("close"))
        ac = atr_consumed_pct_from_bar(
            close=close, session_open=session_open, atr_pct=atr_pct
        )
        hist_ac.append(ac)
        if close is not None and session_open:
            hist_signed.append((close - session_open) / session_open * 100.0)
        else:
            hist_signed.append(None)

    ac_live = _f(live_atr_consumed_pct)
    if ac_live is not None:
        if not hist_ac or hist_ac[-1] != ac_live:
            hist_ac.append(ac_live)
            mv = None
            if live_price is not None and session_open:
                mv = (live_price - session_open) / session_open * 100.0
            hist_signed.append(mv)

    return hist_ac, hist_signed


def evaluate_atr_ready_suppress(
    *,
    atr_consumed_pct: Optional[float],
    hist_ac: Sequence[Optional[float]],
    hist_signed: Sequence[Optional[float]],
    direction: Optional[str],
    threshold_pct: Optional[float] = None,
) -> Dict[str, Any]:
    """Return suppress decision + fields for consistency_log.inputs."""
    thr = threshold_pct if threshold_pct is not None else atr_ready_suppress_threshold_pct()
    ac = _f(atr_consumed_pct)
    progressing = progression_increasing(hist_ac, hist_signed, direction)
    would = ac is not None and ac >= thr and (not progressing)
    prior = list(hist_ac[-3:]) if hist_ac else []
    return {
        "atr_ready_suppress": True,  # rule evaluated (instrumented)
        "atr_ready_suppress_threshold_pct": thr,
        "atr_consumed_pct": round(ac, 2) if ac is not None else None,
        "atr_progression_increasing": bool(progressing),
        "atr_progression_hist": [round(x, 2) if x is not None else None for x in prior],
        "atr_ready_suppress_would": bool(would),
        "atr_ready_suppress_fired": False,  # set by apply when live mutates
    }


def classify_soft_hard_gates(
    *,
    trade_take_disable_reason: Optional[str] = None,
    trade_state_reason: Optional[str] = None,
    zone_downgrade: Optional[str] = None,
    dwell_soft_hold: Any = None,
    dist_would_block: Any = None,
    check1_beyond_ema10: Any = None,
) -> Dict[str, List[str]]:
    """Mirror ``scripts/backtest_scoring_gate_v2.classify_gates`` for live shadow."""
    hard: List[str] = []
    soft: List[str] = []
    reason = trade_take_disable_reason or trade_state_reason or ""
    ru = reason.upper()
    zd = (zone_downgrade or "").lower()
    if "WINDOW" in ru or "14:30" in reason or ("AFTER" in ru and "WINDOW" in ru):
        hard.append("entry_window_closed")
    if (
        str(dist_would_block).lower() == "true"
        or dist_would_block is True
        or "TOO CLOSE" in ru
        or "DISTANCE" in ru
    ):
        hard.append("entry_distance_stop_validity")
    if str(check1_beyond_ema10).lower() == "true" or check1_beyond_ema10 is True:
        hard.append("beyond_ema10")
    if "WARNING" in ru or "warning_stack" in zd or "dwell hold (warning" in reason.lower():
        soft.append("warning_stack")
    if "DIRECTION" in ru or "IMBALANCE" in ru or "direction_imbalance" in zd:
        soft.append("direction_imbalance")
    if not soft and (
        str(dwell_soft_hold).lower() == "true" or dwell_soft_hold is True
    ):
        if "direction" in reason.lower():
            soft.append("direction_imbalance")
        else:
            soft.append("warning_stack")
    return {"hard": sorted(set(hard)), "soft": soft, "reason": reason}  # type: ignore[return-value]


def evaluate_would_override_di(
    *,
    rendered_state: Optional[str],
    grade: Optional[str],
    trade_take_enabled: bool,
    trade_take_disable_reason: Optional[str] = None,
    trade_state_reason: Optional[str] = None,
    zone_downgrade: Optional[str] = None,
    dwell_soft_hold: Any = None,
    dist_would_block: Any = None,
    check1_beyond_ema10: Any = None,
) -> Dict[str, Any]:
    """Shadow-only Part 2 candidate flag (never mutates take_enabled)."""
    g = (grade or "").strip().upper().replace("!", "").replace("*", "")
    grade_ok = g in ("A", "A+")
    ready = is_ready_family(rendered_state)
    out: Dict[str, Any] = {
        "would_override_di": False,
        "would_override_di_eligible_ready": bool(ready and grade_ok),
    }
    if not ready or not grade_ok:
        return out
    if trade_take_enabled:
        out["would_override_di_skip"] = "already_take_enabled"
        return out
    gates = classify_soft_hard_gates(
        trade_take_disable_reason=trade_take_disable_reason,
        trade_state_reason=trade_state_reason,
        zone_downgrade=zone_downgrade,
        dwell_soft_hold=dwell_soft_hold,
        dist_would_block=dist_would_block,
        check1_beyond_ema10=check1_beyond_ema10,
    )
    out["would_override_di_hard"] = gates["hard"]
    out["would_override_di_soft"] = gates["soft"]
    if gates["hard"]:
        out["would_override_di_skip"] = "hard_blocked"
        return out
    soft = set(gates["soft"])
    if soft == {"direction_imbalance"} or (
        "direction_imbalance" in soft and "warning_stack" not in soft
    ):
        out["would_override_di"] = True
    elif "warning_stack" in soft:
        out["would_override_di_skip"] = "warning_stack"
    else:
        out["would_override_di_skip"] = "other_soft_or_none"
    return out


def apply_atr_ready_suppress_display(
    stock: Dict[str, Any],
    decision: Dict[str, Any],
    *,
    live_enabled: Optional[bool] = None,
) -> bool:
    """Mutate stock READY→WATCHING when live enabled and would_suppress.

    Returns True if display was mutated. Always updates decision['atr_ready_suppress_fired'].
    """
    live = atr_ready_suppress_live_enabled() if live_enabled is None else bool(live_enabled)
    would = bool(decision.get("atr_ready_suppress_would"))
    if not (live and would):
        decision["atr_ready_suppress_fired"] = False
        decision["atr_ready_suppress_live_enabled"] = live
        return False

    thr = decision.get("atr_ready_suppress_threshold_pct") or atr_ready_suppress_threshold_pct()
    ac = decision.get("atr_consumed_pct")
    reason = (
        f"WATCHING · ATR {ac}% ≥ {thr:g}% not progressing — display suppress"
    )
    stock["trade_state"] = STATE_WATCHING
    stock["trade_state_reason"] = reason
    stock["trade_take_enabled"] = False
    stock["zone_downgrade"] = stock.get("zone_downgrade") or "atr_ready_suppress"
    stock["trade_take_disable_reason"] = reason
    stock["atr_ready_suppress"] = dict(decision)
    stock["atr_ready_suppress"]["atr_ready_suppress_fired"] = True
    decision["atr_ready_suppress_fired"] = True
    decision["atr_ready_suppress_live_enabled"] = True
    return True


def evaluate_and_maybe_apply_for_stock(
    stock: Dict[str, Any],
    *,
    candles: Optional[List[Dict[str, Any]]] = None,
    atr_pct: Optional[float] = None,
    in_lock: bool = False,
    live_enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    """Evaluate Part 1 for a locked/promoted READY-family stock; apply if live.

    Skips evaluation (returns empty stub) when not locked/promoted or not READY-family.
    """
    promoted = bool(stock.get("promoted_at"))
    locked_or_promo = bool(in_lock or stock.get("in_lock") or promoted)
    state = stock.get("trade_state")
    stub = {
        "atr_ready_suppress": False,
        "atr_ready_suppress_fired": False,
        "atr_ready_suppress_would": False,
        "atr_ready_suppress_live_enabled": (
            atr_ready_suppress_live_enabled() if live_enabled is None else bool(live_enabled)
        ),
        "atr_consumed_pct": None,
        "atr_progression_increasing": None,
    }
    if not locked_or_promo or not is_ready_family(state):
        stub["atr_ready_suppress_skip"] = (
            "not_locked_or_promoted" if not locked_or_promo else "not_ready_family"
        )
        return stub

    atr_blob = stock.get("atr_consumed") or {}
    ac = _f(atr_blob.get("atr_consumed_pct_from_open"))
    session_open = _f(atr_blob.get("session_open"))
    live_px = _f(stock.get("live_candle_price") or stock.get("trade_entry"))

    hist_ac, hist_signed = build_atr_progression_hist(
        candles or [],
        session_open=session_open,
        atr_pct=atr_pct,
        live_atr_consumed_pct=ac,
        live_price=live_px,
    )
    decision = evaluate_atr_ready_suppress(
        atr_consumed_pct=ac,
        hist_ac=hist_ac,
        hist_signed=hist_signed,
        direction=stock.get("direction"),
    )
    apply_atr_ready_suppress_display(stock, decision, live_enabled=live_enabled)
    return decision

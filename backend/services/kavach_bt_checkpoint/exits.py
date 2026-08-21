"""BT-3 — exit A (baseline EMA trail) vs B (2R dynamic trail) vs C (actual)."""
from __future__ import annotations

from datetime import datetime, time
from typing import Any, Dict, List, Optional, Sequence

import pytz

from backend.services.kavach_bt_checkpoint.config import (
    DYNAMIC_TRAIL_ARM_R,
    DYNAMIC_TRAIL_STEP_R,
    FORCE_EXIT_HM,
)
from backend.services.kavach_exit_candidate_shadow import (
    Bar,
    TradeSpec,
    evaluate_baseline_ema_trail,
    price_r,
    bar_favorable_extreme,
)

IST = pytz.timezone("Asia/Kolkata")


def _is_long(direction: str) -> bool:
    return str(direction).upper() in ("LONG", "BUY", "B")


def _parse_bar_at(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        try:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def bars_to_shadow(bars: Sequence[Dict[str, Any]]) -> List[Bar]:
    out: List[Bar] = []
    for b in bars:
        be = b.get("bar_end")
        bar_at = be.isoformat() if hasattr(be, "isoformat") else (str(be) if be else None)
        out.append(
            Bar(
                open=float(b["open"]),
                high=float(b["high"]),
                low=float(b["low"]),
                close=float(b["close"]),
                ema5=float(b["ema5"]) if b.get("ema5") is not None else None,
                ema10=float(b["ema10"]) if b.get("ema10") is not None else None,
                bar_at=bar_at,
            )
        )
    return out


def path_mfe_mae(
    bars: Sequence[Dict[str, Any]],
    *,
    entry: float,
    risk_pts: float,
    direction: str,
) -> Dict[str, Optional[float]]:
    is_long = _is_long(direction)
    if risk_pts <= 0 or not bars:
        return {"mfe_r": None, "mae_r": None}
    mfe = 0.0
    mae = 0.0
    for b in bars:
        hi = float(b["high"])
        lo = float(b["low"])
        if is_long:
            mfe = max(mfe, (hi - entry) / risk_pts)
            mae = min(mae, (lo - entry) / risk_pts)
        else:
            mfe = max(mfe, (entry - lo) / risk_pts)
            mae = min(mae, (entry - hi) / risk_pts)
    return {"mfe_r": round(mfe, 4), "mae_r": round(mae, 4)}


def simulate_dynamic_trail_exit(
    bars: Sequence[Dict[str, Any]],
    *,
    entry: float,
    risk_pts: float,
    direction: str,
    arm_r: float = DYNAMIC_TRAIL_ARM_R,
    step_r: float = DYNAMIC_TRAIL_STEP_R,
) -> Optional[Dict[str, Any]]:
    """After 2R, trail stop by 1R for every additional 1R of MFE (close breach).

    Stop = entry + (floor(peak_r) - 1) * risk for long once peak_r >= 2
    i.e. at 2R stop locks 1R; at 3R locks 2R; at 4R locks 3R…
    Force exit at 15:15 IST (Rule 27) if still open.
    """
    is_long = _is_long(direction)
    if risk_pts <= 0 or not bars:
        return None
    force_t = time(FORCE_EXIT_HM[0], FORCE_EXIT_HM[1])
    peak_r = 0.0
    stop: Optional[float] = None
    armed = False

    for i, b in enumerate(bars):
        shadow = Bar(
            open=float(b["open"]),
            high=float(b["high"]),
            low=float(b["low"]),
            close=float(b["close"]),
            ema5=None,
            ema10=None,
            bar_at=None,
        )
        ext = bar_favorable_extreme(shadow, is_long)
        r_ext = price_r(ext, entry=entry, risk_pts=risk_pts, is_long=is_long)
        if r_ext > peak_r:
            peak_r = r_ext
        if peak_r >= arm_r:
            armed = True
            # locked_r = floor(peak_r) - 1  (at 2.x → 1R, at 3.x → 2R, …)
            locked = max(arm_r - step_r, float(int(peak_r)) - step_r)
            if locked < step_r:
                locked = step_r
            if is_long:
                stop = entry + locked * risk_pts
            else:
                stop = entry - locked * risk_pts

        close = float(b["close"])
        be = _parse_bar_at(b.get("bar_end"))
        force = be is not None and be.timetz().replace(tzinfo=None) >= force_t if be else False
        # compare clock in IST
        if be is not None:
            force = be.time() >= force_t

        hit = False
        reason = ""
        if armed and stop is not None:
            if is_long and close <= stop:
                hit = True
                reason = "R26_dynamic_trail"
            elif not is_long and close >= stop:
                hit = True
                reason = "R26_dynamic_trail"
        if force and not hit:
            hit = True
            reason = "R27_force_exit_1515"

        if hit:
            exit_r = price_r(close, entry=entry, risk_pts=risk_pts, is_long=is_long)
            return {
                "method": "B_dynamic_trail",
                "exit_price": round(close, 4),
                "exit_time": be.isoformat() if be else None,
                "exit_r": round(exit_r, 4),
                "peak_r": round(peak_r, 4),
                "reason": reason,
                "bar_index": i,
                "stop_at_exit": round(stop, 4) if stop is not None else None,
            }

    # open through last bar — mark last close as mark-to-market exit for research
    last = bars[-1]
    close = float(last["close"])
    be = _parse_bar_at(last.get("bar_end"))
    exit_r = price_r(close, entry=entry, risk_pts=risk_pts, is_long=is_long)
    return {
        "method": "B_dynamic_trail",
        "exit_price": round(close, 4),
        "exit_time": be.isoformat() if be else None,
        "exit_r": round(exit_r, 4),
        "peak_r": round(peak_r, 4),
        "reason": "session_end_open",
        "bar_index": len(bars) - 1,
        "stop_at_exit": round(stop, 4) if stop is not None else None,
    }


def simulate_exit_a_baseline(
    bars: Sequence[Dict[str, Any]],
    *,
    entry: float,
    risk_pts: float,
    direction: str,
    symbol: str = "",
) -> Optional[Dict[str, Any]]:
    shadow_bars = bars_to_shadow(bars)
    trade = TradeSpec(
        symbol=symbol or "X",
        direction=direction,
        entry=entry,
        risk_pts=risk_pts,
    )
    ev = evaluate_baseline_ema_trail(shadow_bars, trade)
    force_t = time(FORCE_EXIT_HM[0], FORCE_EXIT_HM[1])

    # Also scan for 15:15 force if baseline never fired
    if ev is None:
        for i, b in enumerate(bars):
            be = _parse_bar_at(b.get("bar_end"))
            if be is not None and be.time() >= force_t:
                close = float(b["close"])
                exit_r = price_r(close, entry=entry, risk_pts=risk_pts, is_long=_is_long(direction))
                return {
                    "method": "A_baseline_ema",
                    "exit_price": round(close, 4),
                    "exit_time": be.isoformat(),
                    "exit_r": round(exit_r, 4),
                    "peak_r": None,
                    "reason": "R27_force_exit_1515",
                    "bar_index": i,
                }
        if bars:
            last = bars[-1]
            close = float(last["close"])
            be = _parse_bar_at(last.get("bar_end"))
            exit_r = price_r(close, entry=entry, risk_pts=risk_pts, is_long=_is_long(direction))
            return {
                "method": "A_baseline_ema",
                "exit_price": round(close, 4),
                "exit_time": be.isoformat() if be else None,
                "exit_r": round(exit_r, 4),
                "peak_r": None,
                "reason": "session_end_open",
                "bar_index": len(bars) - 1,
            }
        return None

    return {
        "method": "A_baseline_ema",
        "exit_price": round(float(ev.exit_price), 4),
        "exit_time": ev.bar_at,
        "exit_r": round(float(ev.exit_r), 4),
        "peak_r": round(float(ev.peak_r), 4) if ev.peak_r is not None else None,
        "reason": ev.reason,
        "bar_index": ev.bar_index,
    }


def exit_c_actual(row: Dict[str, Any]) -> Dict[str, Any]:
    """Actual trade_log exit (discretionary or rule_compliant)."""
    return {
        "method": "C_actual",
        "exit_price": row.get("exit_price"),
        "exit_time": str(row.get("exit_time")) if row.get("exit_time") is not None else None,
        "exit_r": row.get("r_realized"),
        "peak_r": row.get("mfe_r") or row.get("peak_unrealized_r"),
        "reason": row.get("exit_trigger") or row.get("exit_trigger_type") or "actual",
        "exit_trigger_type": row.get("exit_trigger_type"),
    }


def pick_best_exit(a: Optional[Dict], b: Optional[Dict], c: Optional[Dict]) -> str:
    cands = []
    for label, d in (("A", a), ("B", b), ("C", c)):
        if d and d.get("exit_r") is not None:
            try:
                cands.append((float(d["exit_r"]), label))
            except (TypeError, ValueError):
                pass
    if not cands:
        return "NA"
    cands.sort(reverse=True)
    return cands[0][1]

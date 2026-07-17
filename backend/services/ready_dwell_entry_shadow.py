"""READY dwell (10m) + entry distance guard.

Live flip: ``READY_DWELL_ENTRY_LIVE=1`` applies dwell + distance guard.
Live threshold: ``READY_DWELL_ENTRY_OPTION`` (default **B**):
  B → max(0.3% * price, 500 / lot)   ← live decision
  A → max(0.3% * price, 300 / lot)   ← shadow only
  C → max(0.3% * price, 0.25 * ATR) ← shadow only

Checks (both kept on every option):
  1. entry at/beyond EMA10 (side-aware)
  2. |entry − EMA10| < min_gap_pts
  3. |EMA5 − EMA10| < min_gap_pts  (stack collapse; caught CUMMINSIND)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

MIN_GAP_PCT = 0.003
MIN_INR_RISK_FLOOR_A = 300.0
MIN_INR_RISK_FLOOR_B = 500.0
OPTION_C_ATR_MULT = 0.25
READY_DWELL_MINUTES = 10
ENTRY_EMA5_WARN_PCT = 0.5  # same band as ENTRY_EMA5_TOL_PCT; WARN only
# Back-compat alias.
MIN_INR_RISK_FLOOR = MIN_INR_RISK_FLOOR_A
# Live decision default = Option B (owner go-live 2026-07-18).
DEFAULT_LIVE_OPTION = "B"

_STATE_ENSURED = False

SOFT_ZONE_DOWNGRADES = frozenset(
    {
        "warning_stack",
        "vwap_quality",
        "direction_imbalance",
    }
)


def ready_dwell_entry_live_enabled() -> bool:
    """Live flip of dwell + distance guard. Default OFF until explicit go-live."""
    return os.environ.get("READY_DWELL_ENTRY_LIVE", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def live_distance_option() -> str:
    """Active live threshold letter. Default B (500/lot)."""
    raw = os.environ.get("READY_DWELL_ENTRY_OPTION", DEFAULT_LIVE_OPTION).strip().upper()
    return raw if raw in ("A", "B", "C") else DEFAULT_LIVE_OPTION


def min_gap_pts(
    price: Optional[float],
    lot: int,
    *,
    option: str = "A",
    atr_pts: Optional[float] = None,
) -> Optional[float]:
    """Per-symbol floor for Option A / B / C."""
    if price is None or price <= 0:
        return None
    lot_i = max(int(lot or 1), 1)
    pct_leg = float(MIN_GAP_PCT) * float(price)
    opt = (option or "A").upper()
    if opt == "B":
        return max(pct_leg, float(MIN_INR_RISK_FLOOR_B) / lot_i)
    if opt == "C":
        atr_leg = (
            float(OPTION_C_ATR_MULT) * float(atr_pts)
            if atr_pts is not None and atr_pts > 0
            else None
        )
        if atr_leg is None:
            # No ATR → fall back to A so C never silently passes everything.
            return max(pct_leg, float(MIN_INR_RISK_FLOOR_A) / lot_i)
        return max(pct_leg, atr_leg)
    # Option A (default / live)
    return max(pct_leg, float(MIN_INR_RISK_FLOOR_A) / lot_i)


def evaluate_entry_distance_guard(
    *,
    is_long: bool,
    entry: Optional[float],
    ema5: Optional[float],
    ema10: Optional[float],
    price: Optional[float],
    lot: int,
    ema5_tol_pct: float = ENTRY_EMA5_WARN_PCT,
    option: str = "A",
    atr_pts: Optional[float] = None,
) -> Dict[str, Any]:
    """Render-time distance / side / stack checks (shadow or live Option A)."""
    ref_price = entry if entry is not None else (ema5 if ema5 is not None else price)
    opt = (option or "A").upper()
    gap_floor = min_gap_pts(ref_price, lot, option=opt, atr_pts=atr_pts)

    check1_beyond = False
    check2_entry_thin = False
    check3_stack_thin = False
    warn_entry_off_ema5 = False

    entry_to_ema10 = None
    ema5_to_ema10 = None
    entry_vs_ema5_pct = None

    if entry is not None and ema10 is not None:
        entry_to_ema10 = abs(float(entry) - float(ema10))
        if is_long:
            check1_beyond = float(entry) <= float(ema10)
        else:
            check1_beyond = float(entry) >= float(ema10)
        if gap_floor is not None and entry_to_ema10 < gap_floor:
            check2_entry_thin = True

    if ema5 is not None and ema10 is not None:
        ema5_to_ema10 = abs(float(ema5) - float(ema10))
        if gap_floor is not None and ema5_to_ema10 < gap_floor:
            check3_stack_thin = True

    if entry is not None and ema5 is not None and abs(float(ema5)) > 1e-9:
        entry_vs_ema5_pct = abs(float(entry) - float(ema5)) / abs(float(ema5)) * 100.0
        warn_entry_off_ema5 = entry_vs_ema5_pct > float(ema5_tol_pct)

    would_block = bool(check1_beyond or check2_entry_thin or check3_stack_thin)
    reasons: List[str] = []
    if check1_beyond:
        reasons.append("entry at/beyond EMA10")
    if check2_entry_thin:
        reasons.append("entry too close to exit zone")
    if check3_stack_thin:
        reasons.append("EMA5–EMA10 gap too thin for valid stop")
    if warn_entry_off_ema5:
        reasons.append(f"entry off live EMA5 (>{ema5_tol_pct}%)")

    floor_label = {
        "A": MIN_INR_RISK_FLOOR_A,
        "B": MIN_INR_RISK_FLOOR_B,
        "C": None,
    }.get(opt)

    return {
        "option": opt,
        "min_gap_pct": MIN_GAP_PCT,
        "min_inr_risk_floor": floor_label,
        "option_c_atr_mult": OPTION_C_ATR_MULT if opt == "C" else None,
        "atr_pts": round(float(atr_pts), 4) if atr_pts is not None else None,
        "min_gap_pts": round(gap_floor, 4) if gap_floor is not None else None,
        "ref_price": round(float(ref_price), 4) if ref_price is not None else None,
        "lot": max(int(lot or 1), 1),
        "entry": round(float(entry), 4) if entry is not None else None,
        "ema5": round(float(ema5), 4) if ema5 is not None else None,
        "ema10": round(float(ema10), 4) if ema10 is not None else None,
        "entry_to_ema10": round(entry_to_ema10, 4) if entry_to_ema10 is not None else None,
        "ema5_to_ema10": round(ema5_to_ema10, 4) if ema5_to_ema10 is not None else None,
        "entry_vs_ema5_pct": round(entry_vs_ema5_pct, 4) if entry_vs_ema5_pct is not None else None,
        "check1_beyond_ema10": check1_beyond,
        "check2_entry_thin": check2_entry_thin,
        "check3_stack_thin": check3_stack_thin,
        "warn_entry_off_ema5": warn_entry_off_ema5,
        "would_block": would_block,
        "would_warn": warn_entry_off_ema5 and not would_block,
        "block_checks": [
            c
            for c, hit in (
                ("check1", check1_beyond),
                ("check2", check2_entry_thin),
                ("check3", check3_stack_thin),
            )
            if hit
        ],
        "reasons": reasons,
        # Research flag: stack collapsed while entry→EMA10 alone looked ok.
        "check3_only": bool(check3_stack_thin and not check2_entry_thin and not check1_beyond),
    }


def evaluate_threshold_sensitivity(
    *,
    is_long: bool,
    entry: Optional[float],
    ema5: Optional[float],
    ema10: Optional[float],
    price: Optional[float],
    lot: int,
    atr_pts: Optional[float] = None,
) -> Dict[str, Any]:
    """Run A (decision) + B/C (shadow-only) on the same levels every refresh."""
    common = dict(
        is_long=is_long,
        entry=entry,
        ema5=ema5,
        ema10=ema10,
        price=price,
        lot=lot,
        atr_pts=atr_pts,
    )
    a = evaluate_entry_distance_guard(**common, option="A")
    b = evaluate_entry_distance_guard(**common, option="B")
    c = evaluate_entry_distance_guard(**common, option="C")
    return {
        "A": {
            "min_gap_pts": a.get("min_gap_pts"),
            "would_block": a.get("would_block"),
            "check2": a.get("check2_entry_thin"),
            "check3": a.get("check3_stack_thin"),
            "check3_only": a.get("check3_only"),
            "block_checks": a.get("block_checks"),
        },
        "B": {
            "min_gap_pts": b.get("min_gap_pts"),
            "would_block": b.get("would_block"),
            "check2": b.get("check2_entry_thin"),
            "check3": b.get("check3_stack_thin"),
            "check3_only": b.get("check3_only"),
            "block_checks": b.get("block_checks"),
        },
        "C": {
            "min_gap_pts": c.get("min_gap_pts"),
            "would_block": c.get("would_block"),
            "check2": c.get("check2_entry_thin"),
            "check3": c.get("check3_stack_thin"),
            "check3_only": c.get("check3_only"),
            "block_checks": c.get("block_checks"),
            "atr_pts": c.get("atr_pts"),
        },
        # Diffs vs live Option A — tuning signal without re-investigation.
        "B_stricter_than_A": bool(b.get("would_block") and not a.get("would_block")),
        "C_stricter_than_A": bool(c.get("would_block") and not a.get("would_block")),
        "A_stricter_than_B": bool(a.get("would_block") and not b.get("would_block")),
        "A_stricter_than_C": bool(a.get("would_block") and not c.get("would_block")),
    }


def confirmed_close_beyond_ema10(
    candles: Optional[List[Any]],
    *,
    is_long: bool,
    nifty_pct: float = 0.0,
    direction: str = "LONG",
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Hard invalidation (a): last *closed* 10m close beyond EMA10 reverse."""
    if not candles:
        return False, None
    try:
        from backend.services.kavach_10m import metrics_from_10m_candles
        from backend.services.relative_strength_scanner import RANKING_BEARISH, RANKING_BULLISH

        ranking = RANKING_BEARISH if (direction or "LONG").upper() == "SHORT" else RANKING_BULLISH
        m = metrics_from_10m_candles(
            candles,
            ranking_type=ranking,
            nifty_pct=float(nifty_pct or 0.0),
            include_forming=False,
        )
        if not m:
            return False, None
        px = m.get("price")
        ema10 = m.get("ema10_10m")
        if px is None or ema10 is None:
            return False, None
        beyond = (float(px) < float(ema10)) if is_long else (float(px) > float(ema10))
        detail = {
            "closed_price": round(float(px), 4),
            "ema10": round(float(ema10), 4),
            "beyond": beyond,
            "bar_evaluated_at": str(m.get("bar_evaluated_at") or ""),
        }
        return beyond, detail
    except Exception as exc:
        logger.debug("shadow EMA10 close check skipped: %s", exc)
        return False, None


def soft_hide_reason(stock: Dict[str, Any]) -> Optional[str]:
    """Soft signals that may disable Take Trade but must not end dwell early."""
    zd = (stock.get("zone_downgrade") or "").strip()
    if zd in SOFT_ZONE_DOWNGRADES:
        return zd
    reason = str(stock.get("trade_state_reason") or "").lower()
    if "warning stack" in reason:
        return "warning_stack"
    if "vwap quality" in reason or "vwap slope" in reason or "vwap flip" in reason:
        return "vwap_quality"
    return None


def hard_invalidate_reason(
    stock: Dict[str, Any],
    *,
    in_lock: bool,
    candles: Optional[List[Any]],
    is_long: bool,
    nifty_pct: float = 0.0,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    """Hard ends for dwell: EMA10 close, lock removal, EXIT NOW / PLAN EXIT."""
    if not in_lock:
        return "lock_removed", {"in_lock": False}

    # Open-trade / panel exit labels when present on the enrich stock.
    for key in ("exit_now", "exit_now_alarm", "plan_exit", "open_trade_exit_label"):
        val = stock.get(key)
        if val is True or (isinstance(val, str) and val.strip()):
            label = str(val).upper() if isinstance(val, str) else key
            if "EXIT NOW" in label or key in ("exit_now", "exit_now_alarm"):
                return "exit_now", {"field": key, "value": val}
            if "PLAN EXIT" in label or key == "plan_exit":
                return "plan_exit", {"field": key, "value": val}

    status = str(stock.get("open_trade_status") or stock.get("trade_status") or "").upper()
    if "EXIT NOW" in status:
        return "exit_now", {"status": status}
    if "PLAN EXIT" in status:
        return "plan_exit", {"status": status}

    beyond, detail = confirmed_close_beyond_ema10(
        candles,
        is_long=is_long,
        nifty_pct=nifty_pct,
        direction=stock.get("direction") or ("LONG" if is_long else "SHORT"),
    )
    if beyond:
        return "ema10_close", detail
    return None, detail


def ensure_dwell_entry_shadow_state() -> None:
    global _STATE_ENSURED
    if _STATE_ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kavach_ready_dwell_entry_shadow (
                    session_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    shadow_ready_since TIMESTAMPTZ,
                    last_outcome VARCHAR(64),
                    distance_blocked BOOLEAN NOT NULL DEFAULT FALSE,
                    check3_only BOOLEAN NOT NULL DEFAULT FALSE,
                    inputs JSONB NOT NULL DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (session_date, symbol)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_dwell_entry_shadow_session
                ON kavach_ready_dwell_entry_shadow (session_date, updated_at)
                """
            )
        )
    _STATE_ENSURED = True


def _load_shadow_since(db, session_date: str, symbol: str) -> Optional[datetime]:
    try:
        ensure_dwell_entry_shadow_state()
        row = db.execute(
            text(
                """
                SELECT shadow_ready_since
                FROM kavach_ready_dwell_entry_shadow
                WHERE session_date = CAST(:d AS date) AND symbol = :sym
                """
            ),
            {"d": session_date, "sym": symbol.upper()},
        ).fetchone()
        return row.shadow_ready_since if row else None
    except Exception as exc:
        logger.debug("dwell shadow since load skipped: %s", exc)
        return None


def _upsert_shadow_state(
    db,
    *,
    session_date: str,
    symbol: str,
    shadow_ready_since: Optional[datetime],
    last_outcome: str,
    distance_blocked: bool,
    check3_only: bool,
    inputs: Dict[str, Any],
) -> None:
    try:
        ensure_dwell_entry_shadow_state()
        db.execute(
            text(
                """
                INSERT INTO kavach_ready_dwell_entry_shadow (
                    session_date, symbol, shadow_ready_since, last_outcome,
                    distance_blocked, check3_only, inputs, updated_at
                ) VALUES (
                    CAST(:d AS date), :sym, :since, :out,
                    :db, :c3, CAST(:inp AS jsonb), NOW()
                )
                ON CONFLICT (session_date, symbol) DO UPDATE SET
                    shadow_ready_since = EXCLUDED.shadow_ready_since,
                    last_outcome = EXCLUDED.last_outcome,
                    distance_blocked = EXCLUDED.distance_blocked,
                    check3_only = EXCLUDED.check3_only,
                    inputs = EXCLUDED.inputs,
                    updated_at = NOW()
                """
            ),
            {
                "d": session_date,
                "sym": symbol.upper(),
                "since": shadow_ready_since,
                "out": last_outcome,
                "db": distance_blocked,
                "c3": check3_only,
                "inp": json.dumps(inputs or {}),
            },
        )
    except Exception as exc:
        logger.debug("dwell shadow state upsert skipped: %s", exc)


def build_dwell_entry_shadow(
    stock: Dict[str, Any],
    *,
    db,
    session_date: str,
    candles: Optional[List[Any]],
    lot: int,
    in_lock: bool,
    audit_levels: Optional[Dict[str, Any]],
    pre_gate_state: Optional[str],
    rendered_state: Optional[str],
    nifty_pct: float = 0.0,
    atr_pct: float = 0.0,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Compute shadow outcome for one symbol; persist dwell since when eligible."""
    from backend.services.daily_checklist_trade_state import (
        STATE_READY,
        STATE_READY_RECHECK,
    )

    clock = now or datetime.now(IST)
    if clock.tzinfo is None:
        clock = IST.localize(clock)
    else:
        clock = clock.astimezone(IST)

    direction = (stock.get("direction") or "LONG").upper()
    is_long = direction != "SHORT"
    ready_like = pre_gate_state in (STATE_READY, STATE_READY_RECHECK)

    live_ema5 = _f(stock.get("live_candle_ema5"))
    live_ema10 = _f(stock.get("live_candle_ema10"))
    live_price = _f(stock.get("live_candle_price"))
    live_bar_at = stock.get("live_candle_bar_at")

    # Fallback: compute once if overlay did not stash levels.
    if (live_ema5 is None or live_ema10 is None) and candles:
        try:
            from backend.services.kavach_10m import metrics_from_10m_candles
            from backend.services.relative_strength_scanner import (
                RANKING_BEARISH,
                RANKING_BULLISH,
            )

            ranking = RANKING_BEARISH if not is_long else RANKING_BULLISH
            m = metrics_from_10m_candles(
                candles,
                ranking_type=ranking,
                nifty_pct=float(nifty_pct or 0.0),
                include_forming=True,
            )
            if m:
                live_ema5 = _f(m.get("ema5"))
                live_ema10 = _f(m.get("ema10_10m"))
                live_price = _f(m.get("price"))
                live_bar_at = str(m.get("bar_evaluated_at") or "")
        except Exception as exc:
            logger.debug("shadow live levels fallback skipped: %s", exc)

    # Shadow / future-live entry = live 10m EMA5, not lagging audit alone.
    shadow_entry = round(float(live_ema5), 2) if live_ema5 is not None else None
    audit = audit_levels or {}
    audit_ema5 = _f(audit.get("ema5"))
    audit_entry = _f(stock.get("trade_entry"))
    audit_vs_live_ema5_pts = None
    if audit_ema5 is not None and live_ema5 is not None:
        audit_vs_live_ema5_pts = round(abs(float(audit_ema5) - float(live_ema5)), 4)
    entry_vs_live_pts = None
    if audit_entry is not None and live_ema5 is not None:
        entry_vs_live_pts = round(abs(float(audit_entry) - float(live_ema5)), 4)

    ref_px = live_price if live_price is not None else shadow_entry
    atr_pts = None
    if ref_px is not None and atr_pct and float(atr_pct) > 0:
        atr_pts = float(ref_px) * float(atr_pct) / 100.0

    # Decision path = live option (default B); A/C always in sensitivity.
    decision_opt = live_distance_option()
    dist = evaluate_entry_distance_guard(
        is_long=is_long,
        entry=shadow_entry,
        ema5=live_ema5,
        ema10=live_ema10,
        price=ref_px,
        lot=lot,
        option=decision_opt,
        atr_pts=atr_pts,
    )
    sensitivity = evaluate_threshold_sensitivity(
        is_long=is_long,
        entry=shadow_entry,
        ema5=live_ema5,
        ema10=live_ema10,
        price=ref_px,
        lot=lot,
        atr_pts=atr_pts,
    )

    hard_reason, hard_detail = hard_invalidate_reason(
        stock,
        in_lock=in_lock,
        candles=candles,
        is_long=is_long,
        nifty_pct=nifty_pct,
    )
    soft = soft_hide_reason(stock)

    prev_since = _load_shadow_since(db, session_date, stock.get("symbol") or "")
    shadow_ready_since = prev_since
    outcome = "idle"
    would_enter_dwell = False
    would_extend_dwell = False
    would_deny_dwell = False
    would_end_hard = False
    would_end_distance = False
    would_card_visible = False
    would_trade_take_enabled = False
    dwell_elapsed_sec = None
    dwell_remaining_sec = None

    if dist.get("would_block"):
        would_deny_dwell = True
        shadow_ready_since = None
        if ready_like:
            outcome = "would_block_distance"
            # check3-only research signal (possible thin pullback, not junk alone)
            if dist.get("check3_only"):
                outcome = "would_block_check3_only"
        else:
            outcome = "distance_blocked_not_ready"
    elif hard_reason:
        would_end_hard = True
        shadow_ready_since = None
        outcome = f"would_end_hard:{hard_reason}"
    elif ready_like and not soft:
        # Eligible for dwell under new rules.
        would_enter_dwell = True
        if shadow_ready_since is None:
            shadow_ready_since = clock
            outcome = "would_start_dwell"
        else:
            outcome = "would_continue_dwell"
        would_card_visible = True
        would_trade_take_enabled = True
    elif ready_like and soft:
        # Pre-stack was READY; soft hide applied — dwell may extend visibility.
        if shadow_ready_since is None:
            # First observation already soft-hidden: still start the clock so
            # shadow can measure would-have-kept-visible window.
            shadow_ready_since = clock
        elapsed = (clock - _as_ist(shadow_ready_since)).total_seconds()
        dwell_elapsed_sec = int(elapsed)
        floor_sec = READY_DWELL_MINUTES * 60
        dwell_remaining_sec = max(0, int(floor_sec - elapsed))
        if elapsed < floor_sec:
            would_extend_dwell = True
            would_card_visible = True
            would_trade_take_enabled = False
            outcome = f"would_extend_dwell:{soft}"
        else:
            outcome = f"dwell_floor_elapsed:{soft}"
            would_card_visible = False
            would_trade_take_enabled = False
    elif shadow_ready_since is not None and soft:
        # Was in dwell; still soft-hidden after leaving READY pre-gate path.
        # Re-check distance (may have collapsed mid-dwell).
        if dist.get("would_block"):
            would_end_distance = True
            shadow_ready_since = None
            outcome = "would_end_distance_mid_dwell"
        else:
            elapsed = (clock - _as_ist(shadow_ready_since)).total_seconds()
            dwell_elapsed_sec = int(elapsed)
            floor_sec = READY_DWELL_MINUTES * 60
            dwell_remaining_sec = max(0, int(floor_sec - elapsed))
            if elapsed < floor_sec:
                would_extend_dwell = True
                would_card_visible = True
                would_trade_take_enabled = False
                outcome = f"would_extend_dwell:{soft}"
            else:
                outcome = f"dwell_floor_elapsed:{soft}"
    elif shadow_ready_since is not None and not ready_like and not soft:
        # Left READY for a non-soft reason without hard flag — treat as natural end.
        shadow_ready_since = None
        outcome = "would_end_natural"
    else:
        if prev_since is not None and not ready_like:
            shadow_ready_since = None
        outcome = outcome if outcome != "idle" else "idle"

    if shadow_ready_since is not None and dwell_elapsed_sec is None:
        dwell_elapsed_sec = int((clock - _as_ist(shadow_ready_since)).total_seconds())
        dwell_remaining_sec = max(0, READY_DWELL_MINUTES * 60 - dwell_elapsed_sec)

    payload: Dict[str, Any] = {
        "shadow_mode": not ready_dwell_entry_live_enabled(),
        "live_flip_enabled": ready_dwell_entry_live_enabled(),
        "live_threshold": decision_opt,
        "formula_A": "max(0.003*price, 300/lot)",
        "formula_B": "max(0.003*price, 500/lot)",
        "formula_C": "max(0.003*price, 0.25*ATR)",
        "dwell_minutes": READY_DWELL_MINUTES,
        "live_levels": {
            "ema5": live_ema5,
            "ema10": live_ema10,
            "price": live_price,
            "bar_at": live_bar_at,
            "source": "live_10m_forming",
        },
        "audit_levels": {
            "ema5": audit_ema5,
            "entry": audit_entry,
            "source": audit.get("source"),
        },
        "audit_vs_live_ema5_pts": audit_vs_live_ema5_pts,
        "displayed_entry_vs_live_ema5_pts": entry_vs_live_pts,
        "atr_pct": float(atr_pct or 0.0) or None,
        "atr_pts": round(atr_pts, 4) if atr_pts is not None else None,
        "distance": dist,  # Option A decision
        "threshold_sensitivity": sensitivity,  # A + B + C every refresh
        "hard_reason": hard_reason,
        "hard_detail": hard_detail,
        "soft_hide": soft,
        "pre_gate_state": pre_gate_state,
        "rendered_state": rendered_state,
        "in_lock": in_lock,
        "shadow_ready_since": shadow_ready_since.isoformat() if shadow_ready_since else None,
        "dwell_elapsed_sec": dwell_elapsed_sec,
        "dwell_remaining_sec": dwell_remaining_sec,
        "outcome": outcome,
        "would_enter_dwell": would_enter_dwell,
        "would_extend_dwell": would_extend_dwell,
        "would_deny_dwell": would_deny_dwell,
        "would_end_hard": would_end_hard,
        "would_end_distance": would_end_distance,
        "would_card_visible": would_card_visible,
        "would_trade_take_enabled": would_trade_take_enabled,
        # Explicit breakout for session report (Option A)
        "block_check2": bool(dist.get("check2_entry_thin")),
        "block_check3": bool(dist.get("check3_stack_thin")),
        "block_check1": bool(dist.get("check1_beyond_ema10")),
        "check3_only": bool(dist.get("check3_only")),
    }

    _upsert_shadow_state(
        db,
        session_date=session_date,
        symbol=str(stock.get("symbol") or ""),
        shadow_ready_since=shadow_ready_since,
        last_outcome=outcome,
        distance_blocked=bool(dist.get("would_block")),
        check3_only=bool(dist.get("check3_only")),
        inputs=payload,
    )
    return payload


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def levels_with_live_candle_emas(
    stock: Dict[str, Any],
    audit_levels: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Prefer live 10m forming EMA5/EMA10 over lagging audit when live flip is on."""
    levels = dict(audit_levels or {})
    if not ready_dwell_entry_live_enabled():
        return levels
    e5 = _f(stock.get("live_candle_ema5"))
    e10 = _f(stock.get("live_candle_ema10"))
    px = _f(stock.get("live_candle_price"))
    if e5 is not None:
        levels["ema5"] = e5
        levels["source"] = "live_10m_forming"
    if e10 is not None:
        levels["ema10"] = e10
        levels["source"] = "live_10m_forming"
    if px is not None:
        levels["price"] = px
    if stock.get("live_candle_bar_at"):
        levels["bar_evaluated_at"] = stock.get("live_candle_bar_at")
    return levels


def apply_ready_dwell_entry_live(
    stocks: List[Dict[str, Any]],
    *,
    db,
    session_date: str,
    candle_cache: Dict[str, Any],
    lot_cache: Dict[str, int],
    atr_pct_map: Dict[str, float],
    nifty_pct: float = 0.0,
    now: Optional[datetime] = None,
) -> Dict[str, int]:
    """Apply dwell + Option-B distance guard when ``READY_DWELL_ENTRY_LIVE=1``.

    Soft warning_stack / imbalance: keep ``trade_state`` READY inside the 10-min
    floor (``card_visible=True``) but disable Take Trade. Hard invalidation and
    distance failures clear dwell and hide the card.
    """
    from backend.services.daily_checklist_trade_state import (
        STATE_READY,
        STATE_READY_RECHECK,
        STATE_WAIT,
        entry_window_open_ist,
        risk_cap_blocks_ready,
        rr_below_minimum,
        take_trade_structurally_ok,
        trade_take_disable_reason,
    )

    stats = {
        "live_applied": 0,
        "entry_overridden": 0,
        "distance_blocked": 0,
        "dwell_soft_kept": 0,
        "dwell_started": 0,
        "warn_entry_drift": 0,
    }
    if not ready_dwell_entry_live_enabled():
        return stats

    opt = live_distance_option()
    clock = now or datetime.now(IST)
    if clock.tzinfo is None:
        clock = IST.localize(clock)
    else:
        clock = clock.astimezone(IST)

    for s in stocks:
        sym = (s.get("symbol") or "").upper()
        if not sym:
            continue
        stats["live_applied"] += 1

        live_e5 = _f(s.get("live_candle_ema5"))
        live_e10 = _f(s.get("live_candle_ema10"))
        live_px = _f(s.get("live_candle_price"))
        lot = max(int(lot_cache.get(sym) or s.get("trade_lot") or 1), 1)

        if live_e5 is not None:
            s["trade_entry"] = round(float(live_e5), 2)
            stats["entry_overridden"] += 1
        if live_e10 is not None:
            s["trade_sl"] = round(float(live_e10), 2)
        if s.get("trade_entry") is not None and s.get("trade_sl") is not None:
            risk_pts = abs(float(s["trade_entry"]) - float(s["trade_sl"]))
            s["trade_risk_inr"] = int(round(risk_pts * lot, 0))

        direction = (s.get("direction") or "LONG").upper()
        is_long = direction != "SHORT"
        atr_pct = float(atr_pct_map.get(sym) or 0.0)
        ref = live_px if live_px is not None else _f(s.get("trade_entry"))
        atr_pts = (float(ref) * atr_pct / 100.0) if ref is not None and atr_pct > 0 else None

        dist = evaluate_entry_distance_guard(
            is_long=is_long,
            entry=_f(s.get("trade_entry")),
            ema5=live_e5,
            ema10=live_e10,
            price=ref,
            lot=lot,
            option=opt,
            atr_pts=atr_pts,
        )
        s["entry_distance_guard"] = dist
        s["dwell_live_option"] = opt

        if dist.get("warn_entry_off_ema5"):
            badges = list(s.get("gate_badges") or [])
            if "ENTRY DRIFT" not in badges:
                badges.append("ENTRY DRIFT")
            s["gate_badges"] = badges
            stats["warn_entry_drift"] += 1

        pre_stack = s.get("_pre_stack_state") or s.get("trade_state")
        was_ready = pre_stack in (STATE_READY, STATE_READY_RECHECK)
        soft = soft_hide_reason(s)
        in_lock = bool(s.get("in_lock"))
        hard_reason, hard_detail = hard_invalidate_reason(
            s,
            in_lock=in_lock,
            candles=candle_cache.get(sym) or [],
            is_long=is_long,
            nifty_pct=nifty_pct,
        )
        prev_since = _load_shadow_since(db, session_date, sym)

        def _persist(since: Optional[datetime], outcome: str, blocked: bool) -> None:
            _upsert_shadow_state(
                db,
                session_date=session_date,
                symbol=sym,
                shadow_ready_since=since,
                last_outcome=outcome,
                distance_blocked=blocked,
                check3_only=bool(dist.get("check3_only")),
                inputs={
                    "live": True,
                    "option": opt,
                    "distance": dist,
                    "soft": soft,
                    "hard_reason": hard_reason,
                    "hard_detail": hard_detail,
                    "trade_state": s.get("trade_state"),
                    "card_visible": s.get("card_visible"),
                    "trade_take_enabled": s.get("trade_take_enabled"),
                },
            )

        # Distance failure: never READY, no dwell guarantee.
        if dist.get("would_block") and (
            s.get("trade_state") in (STATE_READY, STATE_READY_RECHECK) or was_ready
        ):
            if dist.get("check1_beyond_ema10"):
                reason = "WAIT · entry at/beyond EMA10"
            elif dist.get("check3_only"):
                reason = "WAIT · EMA5–EMA10 gap too thin for valid stop"
            else:
                reason = "WAIT · entry too close to exit zone"
            s["trade_state"] = STATE_WAIT
            s["trade_state_reason"] = reason
            s["trade_take_enabled"] = False
            s["card_visible"] = False
            s["zone_downgrade"] = "entry_distance"
            s["ready_visible_since"] = None
            s["trade_take_disable_reason"] = trade_take_disable_reason(
                trade_state=STATE_WAIT,
                trade_take_enabled=False,
                trade_state_reason=reason,
                trade_entry_window_open=s.get("trade_entry_window_open"),
                entry=s.get("trade_entry"),
                sl=s.get("trade_sl"),
                risk_inr=s.get("trade_risk_inr"),
                rr=s.get("trade_rr"),
                zone_downgrade="entry_distance",
            )
            _persist(None, "live_block_distance", True)
            stats["distance_blocked"] += 1
            continue

        if hard_reason:
            s["card_visible"] = s.get("trade_state") in (STATE_READY, STATE_READY_RECHECK)
            s["ready_visible_since"] = None
            _persist(None, f"live_end_hard:{hard_reason}", False)
            continue

        # Soft hide after warning_stack/imbalance: hold READY card inside 10-min floor.
        if (
            was_ready
            and soft
            and s.get("trade_state") == STATE_WAIT
            and s.get("zone_downgrade") in (
                "warning_stack",
                "vwap_quality",
                "direction_imbalance",
            )
        ):
            since = prev_since or clock
            elapsed = (clock - _as_ist(since)).total_seconds()
            if elapsed < READY_DWELL_MINUTES * 60:
                s["trade_state"] = pre_stack
                s["trade_take_enabled"] = False
                s["card_visible"] = True
                s["dwell_soft_hold"] = True
                s["ready_visible_since"] = _as_ist(since).isoformat()
                s["trade_state_reason"] = (
                    f"READY · dwell hold ({soft}) — Take Trade disabled"
                )
                s["trade_take_disable_reason"] = trade_take_disable_reason(
                    trade_state=pre_stack,
                    trade_take_enabled=False,
                    trade_state_reason=s["trade_state_reason"],
                    trade_entry_window_open=s.get("trade_entry_window_open"),
                    entry=s.get("trade_entry"),
                    sl=s.get("trade_sl"),
                    risk_inr=s.get("trade_risk_inr"),
                    rr=s.get("trade_rr"),
                    zone_downgrade=s.get("zone_downgrade"),
                )
                _persist(_as_ist(since), f"live_dwell_soft:{soft}", False)
                stats["dwell_soft_kept"] += 1
                continue
            s["card_visible"] = False
            s["ready_visible_since"] = None
            _persist(None, f"live_dwell_floor_elapsed:{soft}", False)
            continue

        if s.get("trade_state") in (STATE_READY, STATE_READY_RECHECK):
            since = prev_since or clock
            since = _as_ist(since)
            s["card_visible"] = True
            s["ready_visible_since"] = since.isoformat()
            s["trade_take_enabled"] = bool(
                entry_window_open_ist()
                and take_trade_structurally_ok(
                    entry=s.get("trade_entry"),
                    sl=s.get("trade_sl"),
                    risk_inr=s.get("trade_risk_inr"),
                )
                and not risk_cap_blocks_ready(s.get("trade_risk_inr"), s.get("trade_rr"))
                and not rr_below_minimum(s.get("trade_rr"))
            )
            s["trade_take_disable_reason"] = trade_take_disable_reason(
                trade_state=s.get("trade_state"),
                trade_take_enabled=bool(s.get("trade_take_enabled")),
                trade_state_reason=s.get("trade_state_reason"),
                trade_entry_window_open=s.get("trade_entry_window_open"),
                entry=s.get("trade_entry"),
                sl=s.get("trade_sl"),
                risk_inr=s.get("trade_risk_inr"),
                rr=s.get("trade_rr"),
                zone_downgrade=s.get("zone_downgrade"),
            )
            _persist(since, "live_dwell_active", False)
            if prev_since is None:
                stats["dwell_started"] += 1
            continue

        s["card_visible"] = False
        if prev_since is not None and not soft:
            _persist(None, "live_end_natural", False)

    return stats

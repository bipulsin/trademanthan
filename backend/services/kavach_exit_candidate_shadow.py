"""Shadow-only full-exit candidates for high-R give-back protection.

Constraint (confirmed 20-Jul-2026): single-lot, binary exit only — no
partial/scale-out. All three candidates are FULL-EXIT rule changes.

Live gating: OFF by default. ``EXIT_CANDIDATE_LIVE`` must stay unset/0 until
the 22-Jul checkpoint review. Shadow rows are written regardless so live
sessions accumulate would-fire evidence.

Candidates
----------
C1 ``faster_ratchet``
    Once peak R (intrabar) ≥ ``HIGH_R_ARM`` (default 2.0), trail stop becomes
    ``peak_extreme − RATCHET_GIVEBACK_R × risk_pts`` (floored at entry+1R for
    longs / entry−1R for shorts). Exit on candle **close** beyond that stop.

C2 ``spike_reverse``
    If price touches ≥ ``HIGH_R_ARM`` intrabar and the **same or next** candle
    closes with retained R ≤ ``SPIKE_RETAIN_R`` (default 1.5), exit at that
    close. Targets FEDERALBNK-style spike-and-fade within 1–2 bars.

C3 ``intrabar_ema5_fast``
    Once peak R ≥ ``HIGH_R_ARM``, if a candle **pierces** EMA5 intrabar (low
    below EMA5 for long / high above for short) and closes weaker than the
    prior close (or closes beyond EMA5), exit at that close — without waiting
    for a later confirmed EMA5 break on a subsequent bar.

Baseline (live, unchanged): EMA10 trail → PROFIT_LOCKED at R≥2 on close →
EMA5 reverse **close** only.
"""
from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# --- Tunables (shadow defaults for 22-Jul review) ---
HIGH_R_ARM = 2.0
RATCHET_GIVEBACK_R = 1.0  # C1: stop = peak − 1.0R
RATCHET_FLOOR_R = 1.0  # C1: never trail below +1R retained
SPIKE_RETAIN_R = 1.5  # C2: exit if close R ≤ 1.5 after ≥2R touch
CANDIDATE_IDS = ("C1_faster_ratchet", "C2_spike_reverse", "C3_intrabar_ema5_fast")

_ENSURED = False
IST = None  # lazy Asia/Kolkata via _ist()


def _ist():
    global IST
    if IST is None:
        import pytz

        IST = pytz.timezone("Asia/Kolkata")
    return IST


def exit_candidate_live_enabled() -> bool:
    """Live flip. Default OFF — shadow-only until explicit go-live."""
    return os.environ.get("EXIT_CANDIDATE_LIVE", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def live_candidate_id() -> Optional[str]:
    """Which candidate would gate live when EXIT_CANDIDATE_LIVE=1.

    Default none (shadow all three). Set EXIT_CANDIDATE_LIVE_ID=C2_spike_reverse
    (etc.) only after checkpoint selection.
    """
    raw = (os.environ.get("EXIT_CANDIDATE_LIVE_ID") or "").strip()
    return raw if raw in CANDIDATE_IDS else None


@dataclass
class Bar:
    """One confirmed 10m OHLC bar with EMA levels at bar close."""

    open: float
    high: float
    low: float
    close: float
    ema5: Optional[float] = None
    ema10: Optional[float] = None
    bar_at: Optional[str] = None


@dataclass
class TradeSpec:
    symbol: str
    direction: str  # LONG | SHORT
    entry: float
    risk_pts: float  # |entry − initial EMA10 SL|
    session_date: Optional[str] = None
    notes: str = ""


@dataclass
class ExitEvent:
    candidate_id: str
    bar_index: int
    bar_at: Optional[str]
    exit_price: float
    exit_r: float
    peak_r_at_exit: float
    reason: str
    detail: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BaselineExit:
    bar_index: int
    bar_at: Optional[str]
    exit_price: float
    exit_r: float
    peak_r: float
    reason: str
    state_at_exit: str


@dataclass
class ReplayResult:
    trade: TradeSpec
    peak_r: float
    peak_bar_index: Optional[int]
    peak_price: Optional[float]
    baseline: Optional[BaselineExit]
    candidates: Dict[str, Optional[ExitEvent]]
    giveback_r: Optional[float]  # peak_r − baseline exit_r (if baseline)
    shadow_mode: bool = True


def _f(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _is_long(direction: str) -> bool:
    return (direction or "LONG").upper() != "SHORT"


def bar_favorable_extreme(bar: Bar, is_long: bool) -> float:
    return float(bar.high if is_long else bar.low)


def price_r(price: float, *, entry: float, risk_pts: float, is_long: bool) -> float:
    if risk_pts <= 0:
        return 0.0
    pts = (price - entry) if is_long else (entry - price)
    return pts / risk_pts


def peak_r_through(
    bars: Sequence[Bar],
    *,
    entry: float,
    risk_pts: float,
    is_long: bool,
    through_idx: int,
) -> float:
    peak = 0.0
    for i in range(max(0, through_idx + 1)):
        ext = bar_favorable_extreme(bars[i], is_long)
        peak = max(peak, price_r(ext, entry=entry, risk_pts=risk_pts, is_long=is_long))
    return peak


def evaluate_baseline_ema_trail(
    bars: Sequence[Bar],
    trade: TradeSpec,
) -> Optional[BaselineExit]:
    """Mirror live kavach_open_trades EMA trail (close-confirmed only)."""
    is_long = _is_long(trade.direction)
    entry = float(trade.entry)
    risk = float(trade.risk_pts)
    if risk <= 0 or not bars:
        return None
    state = "TRAILING"
    peak = 0.0
    for i, bar in enumerate(bars):
        close = float(bar.close)
        ema5 = _f(bar.ema5)
        ema10 = _f(bar.ema10)
        peak = max(
            peak,
            price_r(
                bar_favorable_extreme(bar, is_long),
                entry=entry,
                risk_pts=risk,
                is_long=is_long,
            ),
        )
        close_r = price_r(close, entry=entry, risk_pts=risk, is_long=is_long)
        if state == "TRAILING":
            if ema10 is not None:
                beyond = (close < ema10) if is_long else (close > ema10)
                if beyond:
                    return BaselineExit(
                        bar_index=i,
                        bar_at=bar.bar_at,
                        exit_price=close,
                        exit_r=close_r,
                        peak_r=peak,
                        reason="EMA10 reverse close",
                        state_at_exit="TRAILING",
                    )
            if close_r >= HIGH_R_ARM:
                state = "PROFIT_LOCKED"
        elif state == "PROFIT_LOCKED":
            if ema5 is not None:
                beyond = (close < ema5) if is_long else (close > ema5)
                if beyond:
                    return BaselineExit(
                        bar_index=i,
                        bar_at=bar.bar_at,
                        exit_price=close,
                        exit_r=close_r,
                        peak_r=peak,
                        reason="EMA5 reverse close after profit protection",
                        state_at_exit="PROFIT_LOCKED",
                    )
    return None


def _eval_c1_faster_ratchet(
    bars: Sequence[Bar],
    trade: TradeSpec,
    *,
    arm_r: float = HIGH_R_ARM,
    giveback_r: float = RATCHET_GIVEBACK_R,
    floor_r: float = RATCHET_FLOOR_R,
) -> Optional[ExitEvent]:
    is_long = _is_long(trade.direction)
    entry = float(trade.entry)
    risk = float(trade.risk_pts)
    if risk <= 0:
        return None
    armed = False
    peak_ext: Optional[float] = None
    peak_r = 0.0
    for i, bar in enumerate(bars):
        ext = bar_favorable_extreme(bar, is_long)
        r_ext = price_r(ext, entry=entry, risk_pts=risk, is_long=is_long)
        if r_ext >= peak_r:
            peak_r = r_ext
            peak_ext = ext
        if peak_r >= arm_r:
            armed = True
        if not armed or peak_ext is None:
            continue
        # Ratchet stop from peak extreme; floor keeps ≥ floor_r retained.
        if is_long:
            ratchet = peak_ext - giveback_r * risk
            floor_px = entry + floor_r * risk
            stop = max(ratchet, floor_px)
            hit = float(bar.close) <= stop
        else:
            ratchet = peak_ext + giveback_r * risk
            floor_px = entry - floor_r * risk
            stop = min(ratchet, floor_px)
            hit = float(bar.close) >= stop
        if hit:
            close_r = price_r(float(bar.close), entry=entry, risk_pts=risk, is_long=is_long)
            return ExitEvent(
                candidate_id="C1_faster_ratchet",
                bar_index=i,
                bar_at=bar.bar_at,
                exit_price=float(bar.close),
                exit_r=close_r,
                peak_r_at_exit=peak_r,
                reason="Faster ratchet close beyond peak−giveback stop",
                detail={
                    "arm_r": arm_r,
                    "giveback_r": giveback_r,
                    "floor_r": floor_r,
                    "peak_extreme": peak_ext,
                    "stop": round(stop, 4),
                },
            )
    return None


def _eval_c2_spike_reverse(
    bars: Sequence[Bar],
    trade: TradeSpec,
    *,
    arm_r: float = HIGH_R_ARM,
    retain_r: float = SPIKE_RETAIN_R,
) -> Optional[ExitEvent]:
    is_long = _is_long(trade.direction)
    entry = float(trade.entry)
    risk = float(trade.risk_pts)
    if risk <= 0:
        return None
    touch_idx: Optional[int] = None
    peak_r = 0.0
    for i, bar in enumerate(bars):
        r_ext = price_r(
            bar_favorable_extreme(bar, is_long),
            entry=entry,
            risk_pts=risk,
            is_long=is_long,
        )
        peak_r = max(peak_r, r_ext)
        if touch_idx is None and r_ext >= arm_r:
            touch_idx = i
        if touch_idx is None:
            continue
        # Same or next candle only.
        if i > touch_idx + 1:
            break
        close_r = price_r(float(bar.close), entry=entry, risk_pts=risk, is_long=is_long)
        if close_r <= retain_r:
            return ExitEvent(
                candidate_id="C2_spike_reverse",
                bar_index=i,
                bar_at=bar.bar_at,
                exit_price=float(bar.close),
                exit_r=close_r,
                peak_r_at_exit=peak_r,
                reason="Spike≥arm then close back ≤ retain R (same/next bar)",
                detail={
                    "arm_r": arm_r,
                    "retain_r": retain_r,
                    "touch_bar_index": touch_idx,
                    "bars_after_touch": i - touch_idx,
                },
            )
    return None


def _eval_c3_intrabar_ema5_fast(
    bars: Sequence[Bar],
    trade: TradeSpec,
    *,
    arm_r: float = HIGH_R_ARM,
) -> Optional[ExitEvent]:
    is_long = _is_long(trade.direction)
    entry = float(trade.entry)
    risk = float(trade.risk_pts)
    if risk <= 0:
        return None
    armed = False
    peak_r = 0.0
    prev_close: Optional[float] = None
    for i, bar in enumerate(bars):
        peak_r = max(
            peak_r,
            price_r(
                bar_favorable_extreme(bar, is_long),
                entry=entry,
                risk_pts=risk,
                is_long=is_long,
            ),
        )
        if peak_r >= arm_r:
            armed = True
        ema5 = _f(bar.ema5)
        close = float(bar.close)
        if not armed or ema5 is None:
            prev_close = close
            continue
        if is_long:
            pierced = float(bar.low) < ema5
            beyond_close = close < ema5
            weaker = prev_close is not None and close < prev_close
        else:
            pierced = float(bar.high) > ema5
            beyond_close = close > ema5
            weaker = prev_close is not None and close > prev_close
        if pierced and (beyond_close or weaker):
            close_r = price_r(close, entry=entry, risk_pts=risk, is_long=is_long)
            return ExitEvent(
                candidate_id="C3_intrabar_ema5_fast",
                bar_index=i,
                bar_at=bar.bar_at,
                exit_price=close,
                exit_r=close_r,
                peak_r_at_exit=peak_r,
                reason="Intrabar EMA5 pierce + weak/beyond close after ≥arm R",
                detail={
                    "arm_r": arm_r,
                    "ema5": ema5,
                    "pierced": True,
                    "beyond_close": beyond_close,
                    "weaker_than_prior": weaker,
                    "prior_close": prev_close,
                },
            )
        prev_close = close
    return None


def evaluate_candidates_on_bars(
    bars: Sequence[Bar],
    trade: TradeSpec,
    *,
    arm_r: float = HIGH_R_ARM,
    ratchet_giveback_r: float = RATCHET_GIVEBACK_R,
    ratchet_floor_r: float = RATCHET_FLOOR_R,
    spike_retain_r: float = SPIKE_RETAIN_R,
) -> ReplayResult:
    """Replay baseline + all three candidates on a closed 10m bar series."""
    is_long = _is_long(trade.direction)
    entry = float(trade.entry)
    risk = float(trade.risk_pts)
    peak_r = 0.0
    peak_idx: Optional[int] = None
    peak_px: Optional[float] = None
    for i, bar in enumerate(bars):
        ext = bar_favorable_extreme(bar, is_long)
        r = price_r(ext, entry=entry, risk_pts=risk, is_long=is_long)
        if r >= peak_r:
            peak_r = r
            peak_idx = i
            peak_px = ext

    baseline = evaluate_baseline_ema_trail(bars, trade)
    candidates: Dict[str, Optional[ExitEvent]] = {
        "C1_faster_ratchet": _eval_c1_faster_ratchet(
            bars,
            trade,
            arm_r=arm_r,
            giveback_r=ratchet_giveback_r,
            floor_r=ratchet_floor_r,
        ),
        "C2_spike_reverse": _eval_c2_spike_reverse(
            bars, trade, arm_r=arm_r, retain_r=spike_retain_r
        ),
        "C3_intrabar_ema5_fast": _eval_c3_intrabar_ema5_fast(
            bars, trade, arm_r=arm_r
        ),
    }
    giveback = None
    if baseline is not None:
        giveback = round(baseline.peak_r - baseline.exit_r, 4)
    return ReplayResult(
        trade=trade,
        peak_r=round(peak_r, 4),
        peak_bar_index=peak_idx,
        peak_price=peak_px,
        baseline=baseline,
        candidates=candidates,
        giveback_r=giveback,
        shadow_mode=not exit_candidate_live_enabled(),
    )


def evaluate_bar_snapshot(
    *,
    is_long: bool,
    entry: float,
    risk_pts: float,
    peak_r_so_far: float,
    bar: Bar,
    prev_close: Optional[float] = None,
    touch_bar_index: Optional[int] = None,
    current_bar_index: int = 0,
    peak_extreme: Optional[float] = None,
    arm_r: float = HIGH_R_ARM,
    ratchet_giveback_r: float = RATCHET_GIVEBACK_R,
    ratchet_floor_r: float = RATCHET_FLOOR_R,
    spike_retain_r: float = SPIKE_RETAIN_R,
) -> Dict[str, Any]:
    """Single-bar would-fire snapshot for live shadow logging (no state change)."""
    risk = float(risk_pts or 0)
    entry_f = float(entry)
    if risk <= 0:
        return {"armed": False, "would_exit": {}, "peak_r": peak_r_so_far}

    ext = bar_favorable_extreme(bar, is_long)
    r_ext = price_r(ext, entry=entry_f, risk_pts=risk, is_long=is_long)
    peak_r = max(float(peak_r_so_far or 0), r_ext)
    if peak_extreme is None or r_ext >= float(peak_r_so_far or 0):
        peak_extreme = ext
    armed = peak_r >= arm_r
    close_r = price_r(float(bar.close), entry=entry_f, risk_pts=risk, is_long=is_long)
    would: Dict[str, Any] = {}

    # C1
    if armed and peak_extreme is not None:
        if is_long:
            stop = max(
                float(peak_extreme) - ratchet_giveback_r * risk,
                entry_f + ratchet_floor_r * risk,
            )
            hit = float(bar.close) <= stop
        else:
            stop = min(
                float(peak_extreme) + ratchet_giveback_r * risk,
                entry_f - ratchet_floor_r * risk,
            )
            hit = float(bar.close) >= stop
        if hit:
            would["C1_faster_ratchet"] = {
                "exit_price": float(bar.close),
                "exit_r": round(close_r, 4),
                "stop": round(stop, 4),
                "reason": "Faster ratchet close beyond peak−giveback stop",
            }

    # C2
    touch_i = touch_bar_index
    if touch_i is None and r_ext >= arm_r:
        touch_i = current_bar_index
    if touch_i is not None and current_bar_index <= touch_i + 1 and close_r <= spike_retain_r:
        if peak_r >= arm_r:
            would["C2_spike_reverse"] = {
                "exit_price": float(bar.close),
                "exit_r": round(close_r, 4),
                "touch_bar_index": touch_i,
                "reason": "Spike≥arm then close back ≤ retain R (same/next bar)",
            }

    # C3
    ema5 = _f(bar.ema5)
    if armed and ema5 is not None:
        if is_long:
            pierced = float(bar.low) < ema5
            beyond_close = float(bar.close) < ema5
            weaker = prev_close is not None and float(bar.close) < float(prev_close)
        else:
            pierced = float(bar.high) > ema5
            beyond_close = float(bar.close) > ema5
            weaker = prev_close is not None and float(bar.close) > float(prev_close)
        if pierced and (beyond_close or weaker):
            would["C3_intrabar_ema5_fast"] = {
                "exit_price": float(bar.close),
                "exit_r": round(close_r, 4),
                "ema5": ema5,
                "beyond_close": beyond_close,
                "weaker_than_prior": weaker,
                "reason": "Intrabar EMA5 pierce + weak/beyond close after ≥arm R",
            }

    return {
        "armed": armed,
        "peak_r": round(peak_r, 4),
        "close_r": round(close_r, 4),
        "peak_extreme": peak_extreme,
        "touch_bar_index": touch_i,
        "would_exit": would,
        "shadow_mode": not exit_candidate_live_enabled(),
        "live_enabled": exit_candidate_live_enabled(),
        "live_candidate_id": live_candidate_id(),
    }


def replay_result_to_dict(result: ReplayResult) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "trade": asdict(result.trade),
        "peak_r": result.peak_r,
        "peak_bar_index": result.peak_bar_index,
        "peak_price": result.peak_price,
        "giveback_r": result.giveback_r,
        "shadow_mode": result.shadow_mode,
        "baseline": asdict(result.baseline) if result.baseline else None,
        "candidates": {},
    }
    for cid, ev in result.candidates.items():
        out["candidates"][cid] = asdict(ev) if ev else None
    return out


# --- DB shadow log (research-only writes; never mutates trade state) ---


def ensure_exit_candidate_shadow_log() -> None:
    global _ENSURED
    if _ENSURED:
        return
    try:
        from sqlalchemy import text

        from backend.database import engine

        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS kavach_exit_candidate_shadow_log (
                        id SERIAL PRIMARY KEY,
                        session_date DATE NOT NULL,
                        trade_id VARCHAR(64),
                        symbol VARCHAR(32) NOT NULL,
                        direction VARCHAR(8),
                        logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        bar_at TIMESTAMPTZ,
                        entry_price NUMERIC(16,4),
                        risk_pts NUMERIC(16,6),
                        peak_r NUMERIC(12,4),
                        close_px NUMERIC(16,4),
                        close_r NUMERIC(12,4),
                        ema5 NUMERIC(16,4),
                        ema10 NUMERIC(16,4),
                        live_state VARCHAR(32),
                        armed BOOLEAN NOT NULL DEFAULT FALSE,
                        would_c1 BOOLEAN NOT NULL DEFAULT FALSE,
                        would_c2 BOOLEAN NOT NULL DEFAULT FALSE,
                        would_c3 BOOLEAN NOT NULL DEFAULT FALSE,
                        c1_exit_r NUMERIC(12,4),
                        c2_exit_r NUMERIC(12,4),
                        c3_exit_r NUMERIC(12,4),
                        payload JSONB,
                        shadow_mode BOOLEAN NOT NULL DEFAULT TRUE,
                        live_enabled BOOLEAN NOT NULL DEFAULT FALSE
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE INDEX IF NOT EXISTS idx_exit_cand_shadow_session
                    ON kavach_exit_candidate_shadow_log (session_date, symbol, logged_at)
                    """
                )
            )
        _ENSURED = True
    except Exception as exc:
        logger.debug("exit candidate shadow ensure failed: %s", exc)


def log_exit_candidate_shadow(
    db,
    *,
    session_date: str,
    symbol: str,
    snapshot: Dict[str, Any],
    trade_id: Optional[str] = None,
    direction: Optional[str] = None,
    entry_price: Optional[float] = None,
    risk_pts: Optional[float] = None,
    live_state: Optional[str] = None,
    bar_at: Optional[datetime] = None,
    ema5: Optional[float] = None,
    ema10: Optional[float] = None,
    close_px: Optional[float] = None,
    logged_at: Optional[datetime] = None,
) -> None:
    """Best-effort insert; never raises into the live eval path."""
    if not snapshot:
        return
    would = snapshot.get("would_exit") or {}
    # Only persist when armed or any candidate would fire (keep table sparse).
    if not snapshot.get("armed") and not would:
        return
    try:
        import json

        from sqlalchemy import text

        ensure_exit_candidate_shadow_log()
        tz = _ist()
        now = logged_at or datetime.now(tz)
        if isinstance(now, datetime) and now.tzinfo is None:
            now = tz.localize(now)
        bat = bar_at or now
        if isinstance(bat, datetime) and bat.tzinfo is None:
            bat = tz.localize(bat)
        c1 = would.get("C1_faster_ratchet") or {}
        c2 = would.get("C2_spike_reverse") or {}
        c3 = would.get("C3_intrabar_ema5_fast") or {}
        db.execute(
            text(
                """
                INSERT INTO kavach_exit_candidate_shadow_log (
                    session_date, trade_id, symbol, direction, logged_at, bar_at,
                    entry_price, risk_pts, peak_r, close_px, close_r, ema5, ema10,
                    live_state, armed, would_c1, would_c2, would_c3,
                    c1_exit_r, c2_exit_r, c3_exit_r, payload, shadow_mode, live_enabled
                ) VALUES (
                    CAST(:d AS date), :tid, :sym, :dir, :lat, :bat,
                    :entry, :risk, :peak, :close, :cr, :e5, :e10,
                    :lst, :armed, :w1, :w2, :w3,
                    :r1, :r2, :r3, CAST(:payload AS jsonb), :shadow, :live
                )
                """
            ),
            {
                "d": session_date,
                "tid": trade_id,
                "sym": (symbol or "").upper(),
                "dir": (direction or "").upper() or None,
                "lat": now,
                "bat": bat,
                "entry": entry_price,
                "risk": risk_pts,
                "peak": snapshot.get("peak_r"),
                "close": close_px,
                "cr": snapshot.get("close_r"),
                "e5": ema5,
                "e10": ema10,
                "lst": live_state,
                "armed": bool(snapshot.get("armed")),
                "w1": bool(c1),
                "w2": bool(c2),
                "w3": bool(c3),
                "r1": c1.get("exit_r"),
                "r2": c2.get("exit_r"),
                "r3": c3.get("exit_r"),
                "payload": json.dumps(snapshot),
                "shadow": bool(snapshot.get("shadow_mode", True)),
                "live": bool(snapshot.get("live_enabled", False)),
            },
        )
    except Exception as exc:
        logger.debug("exit candidate shadow log failed %s: %s", symbol, exc)

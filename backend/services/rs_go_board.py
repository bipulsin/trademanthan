"""GO Board — curated entry panel (max 2–3 symbols, two windows/day).

Reversal candidates skip confidence-grade hard gate (logged for research).
All evaluations are shadow-logged with shown/filtered reason.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.daily_checklist_snapshot import get_locked_symbols, locked_direction_map
from backend.services.kavach_10m import metrics_from_10m_candles, timeline_states
from backend.services.kavach_confidence import REGIME_TREND
from backend.services.kavach_engine import RANKING_BEARISH, RANKING_BULLISH
from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps
from backend.services.rs_conviction_config import get_config
from backend.services.rs_conviction_signals import normalized_vwap_slope
from backend.services.rs_fast_watch import is_edge_flip, kavach_direction, _is_reversal
from backend.services.smart_futures_picker.position_sizing import get_futures_lot_size_by_instrument_key

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

WINDOWS = (
    ("W1", 9 * 60 + 35, 11 * 60 + 15),
    ("W2", 11 * 60 + 15, 14 * 60 + 30),
)


def _window_label(now: datetime) -> Optional[str]:
    m = now.hour * 60 + now.minute
    for label, start, end in WINDOWS:
        if start <= m < end:
            return label
    return None


def _stop_distance(price: float, ema10: Optional[float], vwap: float, side: str) -> Tuple[float, float]:
    refs = [v for v in (ema10, vwap) if v is not None and v > 0]
    if not refs or price <= 0:
        return 0.0, 0.0
    dists = [(abs(price - r), r) for r in refs]
    dist, level = min(dists, key=lambda x: x[0])
    return dist, level


def evaluate_go_candidate(
    *,
    symbol: str,
    side: str,
    metrics: Dict[str, Any],
    flip_price: float,
    is_reversal: bool,
    prev_kavach: Optional[str],
    new_kavach: str,
    atr_daily_pct: float,
    instrument_key: str,
    cfg: Dict[str, Any],
    evaluated_at: datetime,
) -> Tuple[bool, str, Dict[str, Any]]:
    """Return (show, filter_reason, detail)."""
    price = float(metrics.get("price") or flip_price or 0)
    if price <= 0:
        return False, "no_price", {}

    if not is_edge_flip(prev_kavach, new_kavach):
        return False, "no_edge_flip", {}

    kdir = kavach_direction(new_kavach)
    if kdir != side:
        return False, "direction_mismatch", {}

    regime = (metrics.get("market_regime") or "").upper()
    edge = is_edge_flip(prev_kavach, new_kavach)
    if regime != REGIME_TREND and not (is_reversal and edge):
        return False, "regime_transition", {"regime": regime}

    adx = float(metrics.get("adx") or 0)
    adx_min = float(cfg.get("go_board_adx_min") or 20)
    adx_max = float(cfg.get("go_board_adx_max") or 45)
    if is_reversal:
        adx_max = float(cfg.get("go_board_reversal_adx_max") or 60)
    if adx < adx_min or adx > adx_max:
        return False, "adx_out_of_band", {"adx": adx}

    candles = candles_cache_only(instrument_key) if instrument_key else None
    slope = 0.0
    if candles:
        slope = normalized_vwap_slope(candles, atr_daily_pct, cfg)
    slope_min = float(cfg.get("go_board_slope_min") or 25)
    if not is_reversal:
        if side == "LONG" and slope < slope_min:
            return False, "vwap_slope_weak", {"slope": slope}
        if side == "SHORT" and slope < slope_min:
            return False, "vwap_slope_weak", {"slope": slope}

    origin = float(flip_price or price)
    freshness_pct = abs(price - origin) / origin * 100.0 if origin > 0 else 999.0
    fresh_cap = float(cfg.get("go_board_freshness_pct") or 2.5)
    if freshness_pct > fresh_cap:
        return False, "freshness_exceeded", {"freshness_pct": round(freshness_pct, 3)}

    ema10 = metrics.get("ema10_10m")
    vwap = float(metrics.get("vwap") or price)
    stop_dist, stop_level = _stop_distance(price, ema10, vwap, side)
    stop_pct = stop_dist / price * 100.0 if price else 999.0
    stop_cap = float(cfg.get("go_board_stop_pct_cap") or 1.0)
    if stop_pct > stop_cap:
        return False, "stop_too_wide", {"stop_pct": round(stop_pct, 3)}

    grade = (metrics.get("confidence_grade") or "").replace("*", "")
    if not is_reversal and grade == "D":
        return False, "grade_d_blocked", {"grade": grade}

    lot = get_futures_lot_size_by_instrument_key(instrument_key) if instrument_key else 1
    stop_inr = round(stop_dist * max(lot, 1), 2)

    detail = {
        "symbol": symbol,
        "side": side,
        "is_reversal": is_reversal,
        "confidence_grade": metrics.get("confidence_grade"),
        "kavach_state": new_kavach,
        "price": round(price, 2),
        "freshness_pct": round(freshness_pct, 3),
        "stop_pct": round(stop_pct, 3),
        "stop_inr_1lot": stop_inr,
        "stop_level": round(stop_level, 2),
        "vwap_slope": round(slope, 1),
        "adx": round(adx, 1),
        "regime": regime,
        "ema10": round(ema10, 2) if ema10 else None,
        "vwap": round(vwap, 2),
    }
    return True, "shown", detail


def _log_shadow(
    db,
    session_date: str,
    evaluated_at: datetime,
    symbol: str,
    side: str,
    outcome: str,
    filter_reason: str,
    detail: Dict[str, Any],
    window_label: Optional[str],
) -> None:
    db.execute(
        text(
            """
            INSERT INTO rs_go_board_shadow_log (
                session_date, evaluated_at, symbol, side, outcome, filter_reason,
                is_reversal, confidence_grade, kavach_state, price,
                freshness_pct, stop_pct, stop_inr_1lot, vwap_slope, adx, regime,
                window_label, detail_json
            ) VALUES (
                :sd, :at, :sym, :side, :outcome, :reason,
                :rev, :grade, :kav, :price,
                :fresh, :stop_pct, :stop_inr, :slope, :adx, :regime,
                :win, :detail
            )
            """
        ),
        {
            "sd": session_date,
            "at": evaluated_at,
            "sym": symbol,
            "side": side,
            "outcome": outcome,
            "reason": filter_reason,
            "rev": detail.get("is_reversal"),
            "grade": detail.get("confidence_grade"),
            "kav": detail.get("kavach_state"),
            "price": detail.get("price"),
            "fresh": detail.get("freshness_pct"),
            "stop_pct": detail.get("stop_pct"),
            "stop_inr": detail.get("stop_inr_1lot"),
            "slope": detail.get("vwap_slope"),
            "adx": detail.get("adx"),
            "regime": detail.get("regime"),
            "win": window_label,
            "detail": json.dumps(detail),
        },
    )


def get_go_board(session_date: Optional[str] = None) -> Dict[str, Any]:
    """Live GO Board payload for UI."""
    sd = session_date or datetime.now(IST).strftime("%Y-%m-%d")
    now = datetime.now(IST)
    wl = _window_label(now)
    cfg = get_config()
    max_syms = int(cfg.get("go_board_max_symbols") or 3)

    if not wl:
        return {"session_date": sd, "window": None, "symbols": [], "empty": True}

    panel, _ = _build_panel_at(sd, now, cfg, max_syms=max_syms, persist_shadow=False)
    return {
        "session_date": sd,
        "window": wl,
        "symbols": panel,
        "empty": len(panel) == 0,
    }


def _build_panel_at(
    session_date: str,
    now: datetime,
    cfg: Dict[str, Any],
    *,
    max_syms: int,
    persist_shadow: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    wl = _window_label(now)
    if not wl:
        return [], []

    db = SessionLocal()
    shown: List[Dict[str, Any]] = []
    shadow_rows: List[Dict[str, Any]] = []
    try:
        locked = set(get_locked_symbols(db, session_date))
        lock_dirs = locked_direction_map(db, session_date)
        flips = db.execute(
            text(
                """
                SELECT symbol, direction, kavach_state, prev_kavach_state,
                       flip_price, trade_score, confidence_grade, is_reversal, first_flip_at
                FROM rs_fast_watch
                WHERE session_date = CAST(:d AS date)
                ORDER BY first_flip_at
                """
            ),
            {"d": session_date},
        ).fetchall()

        syms = {r.symbol for r in flips} | locked
        ikey_map, atr_map = load_instrument_atr_maps(db, syms)

        for r in flips:
            flip_at = r.first_flip_at
            if flip_at and flip_at.astimezone(IST) > now:
                continue
            sym = r.symbol
            side = (r.direction or "LONG").upper()
            ikey = ikey_map.get(sym, "")
            candles = candles_cache_only(ikey) if ikey else None
            if not candles:
                continue
            ranking = RANKING_BEARISH if side == "SHORT" else RANKING_BULLISH
            metrics = metrics_from_10m_candles(candles, ranking_type=ranking, nifty_pct=0.0, now=now)
            if not metrics:
                continue
            ok, reason, detail = evaluate_go_candidate(
                symbol=sym,
                side=side,
                metrics=metrics,
                flip_price=float(r.flip_price or metrics.get("price") or 0),
                is_reversal=bool(r.is_reversal),
                prev_kavach=r.prev_kavach_state,
                new_kavach=r.kavach_state,
                atr_daily_pct=float(atr_map.get(sym) or 1.5),
                instrument_key=ikey,
                cfg=cfg,
                evaluated_at=now,
            )
            row = {**detail, "filter_reason": reason, "outcome": "shown" if ok else "filtered"}
            shadow_rows.append(row)
            if persist_shadow:
                _log_shadow(db, session_date, now, sym, side, row["outcome"], reason, detail, wl)
            if ok:
                shown.append(detail)

        shown = sorted(shown, key=lambda x: (-float(x.get("vwap_slope") or 0), x.get("symbol", "")))[:max_syms]
        if persist_shadow:
            db.commit()
    finally:
        db.close()
    return shown, shadow_rows


def replay_go_board_day(session_date: str) -> Dict[str, Any]:
    """Replay GO Board decisions at key timestamps for verification."""
    cfg = get_config()
    checkpoints = ["09:45", "10:00", "10:35", "11:05"]
    db = SessionLocal()
    results: Dict[str, Any] = {"session_date": session_date, "checkpoints": {}}
    try:
        ikey_map, atr_map = load_instrument_atr_maps(db, {"TRENT", "LAURUSLABS"})
        for ck in checkpoints:
            h, m = map(int, ck.split(":"))
            now = IST.localize(datetime.strptime(session_date, "%Y-%m-%d").replace(hour=h, minute=m))
            wl = _window_label(now)
            ck_out: Dict[str, Any] = {"window": wl, "symbols": {}}
            for sym in ("TRENT", "LAURUSLABS"):
                ikey = ikey_map.get(sym)
                candles = candles_cache_only(ikey) if ikey else None
                if not candles:
                    ck_out["symbols"][sym] = {"error": "no_candles"}
                    continue
                lock_dirs = locked_direction_map(db, session_date)
                lock = lock_dirs.get(sym, "SHORT")
                side = "LONG" if sym == "TRENT" else "SHORT"
                ranking = RANKING_BULLISH if side == "LONG" else RANKING_BEARISH
                metrics = metrics_from_10m_candles(candles, ranking_type=ranking, nifty_pct=0.0, now=now)
                if not metrics:
                    ck_out["symbols"][sym] = {"error": "no_metrics"}
                    continue
                rows = timeline_states(candles, ranking_type=ranking)
                prev, new = None, None
                for row in rows:
                    if row["bar_end_ist"] <= ck:
                        new = row["kavach_state"]
                for row in rows:
                    if row["bar_end_ist"] < ck:
                        prev = row["kavach_state"]
                is_rev = _is_reversal(new, lock) if new else False
                flip_price = metrics.get("price")
                ok, reason, detail = evaluate_go_candidate(
                    symbol=sym,
                    side=side if sym == "TRENT" else "SHORT",
                    metrics=metrics,
                    flip_price=float(flip_price or 0),
                    is_reversal=is_rev,
                    prev_kavach=prev,
                    new_kavach=new or metrics.get("kavach_state"),
                    atr_daily_pct=float(atr_map.get(sym) or 1.5),
                    instrument_key=ikey or "",
                    cfg=cfg,
                    evaluated_at=now,
                )
                ck_out["symbols"][sym] = {
                    "show": ok,
                    "reason": reason,
                    "kavach": metrics.get("kavach_state"),
                    "prev": prev,
                    "detail": detail,
                }
            results["checkpoints"][ck] = ck_out
    finally:
        db.close()
    return results

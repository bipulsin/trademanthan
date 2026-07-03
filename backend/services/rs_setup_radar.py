"""Setup Radar — EMA(5) vs VWAP entry pattern on completed 5-min bars."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.rs_conviction_candles import candles_cache_only, load_instrument_atr_maps
from backend.services.rs_conviction_config import get_config
from backend.services.rs_conviction_signals import compute_symbol_signals

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

STATE_NEUTRAL = "NEUTRAL"
STATE_CONVERGING = "CONVERGING"
STATE_TRIGGERED = "TRIGGERED"
STATE_TRIGGERED_CHOP = "TRIGGERED_CHOP"
STATE_EXPIRED = "EXPIRED"
STATE_LATE = "LATE"


def today_ist() -> str:
    return datetime.now(IST).strftime("%Y-%m-%d")


def _gap_atr(ema5: float, vwap: float, atr: float) -> float:
    if not atr or atr <= 0:
        return 0.0
    return (ema5 - vwap) / atr


def _sl_cost(price: float, vwap: float, atr: float, cfg: Dict[str, Any]) -> Dict[str, Any]:
    buf = float(cfg.get("sl_buffer_atr") or 0.1) * (atr or 0)
    stop = vwap - buf if vwap else price
    dist = abs(price - stop)
    pct = (dist / price * 100.0) if price else 0.0
    late = pct > float(cfg.get("sl_late_pct") or 0.6)
    return {"sl_rupees": round(dist, 2), "sl_pct": round(pct, 2), "is_late": late}


def run_setup_radar_cycle() -> Dict[str, Any]:
    sd = today_ist()
    cfg = get_config()
    now = datetime.now(IST)
    db = SessionLocal()
    updated = 0
    try:
        from backend.services.rs_conviction_board import SIDE_BEAR, SIDE_BULL, get_bench_symbols, _load_core_board

        bull_core = _load_core_board(db, sd, SIDE_BULL)
        bear_core = _load_core_board(db, sd, SIDE_BEAR)
        syms = {c["symbol"] for c in bull_core + bear_core}
        for side in (SIDE_BULL, SIDE_BEAR):
            for b in get_bench_symbols(db, sd, side, cfg):
                syms.add(b["symbol"])

        if not syms:
            return {"ok": True, "updated": 0}

        ikey_map, atr_map = load_instrument_atr_maps(db, syms)

        rows = db.execute(
            text(
                """
                SELECT s.symbol, s.current_price, s.ema5, s.vwap, s.ranking_type, h.atr14_pct
                FROM relative_strength_snapshot s
                LEFT JOIN rs_scanner_history h ON h.symbol = s.symbol AND h.date = :d::date
                WHERE s.scan_time = (SELECT MAX(scan_time) FROM relative_strength_snapshot)
                  AND s.symbol = ANY(:syms)
                """
            ),
            {"d": sd, "syms": list(syms)},
        ).fetchall()

        for r in rows:
            price = float(r.current_price or 0)
            ema5 = float(r.ema5 or price)
            vwap = float(r.vwap or price)
            atr_pct = float(r.atr14_pct or 1.0)
            atr = price * atr_pct / 100.0 if price else 1.0
            gap = _gap_atr(ema5, vwap, atr)
            is_bull = (r.ranking_type or "").upper() != "BEARISH"

            prev = db.execute(
                text(
                    "SELECT setup_state, gap_atr, gap_prev1, state_since "
                    "FROM rs_setup_radar WHERE session_date = :d AND symbol = :sym"
                ),
                {"d": sd, "sym": r.symbol},
            ).fetchone()
            prev_state = prev.setup_state if prev else STATE_NEUTRAL
            prev_gap = float(prev.gap_atr or 0) if prev else gap
            prev_gap2 = float(prev.gap_prev1 or 0) if prev and prev.gap_prev1 is not None else None

            candles = candles_cache_only(ikey_map.get(r.symbol, ""))
            sig = compute_symbol_signals(
                candles, side="BULL" if is_bull else "BEAR",
                atr_daily_pct=atr_pct, cfg=cfg,
            )
            gap_hist = sig.get("gap_history") or []

            conv_thr = float(cfg.get("convergence_atr") or 0.35)
            conv_bars = int(cfg.get("convergence_bars") or 2)
            gap_shrinking = abs(gap) < abs(prev_gap)
            if len(gap_hist) >= conv_bars + 1:
                gap_shrinking = all(
                    abs(gap_hist[i]) < abs(gap_hist[i - 1])
                    for i in range(1, len(gap_hist))
                )
            elif prev_gap2 is not None and conv_bars >= 2:
                gap_shrinking = abs(gap) < abs(prev_gap) < abs(prev_gap2)
            side_ok = (is_bull and ema5 <= vwap * 1.002) or (not is_bull and ema5 >= vwap * 0.998)
            crossed = (is_bull and prev_gap < 0 and gap >= 0) or (not is_bull and prev_gap > 0 and gap <= 0)
            close_ok = (is_bull and price > vwap) or (not is_bull and price < vwap)

            whip = db.execute(
                text("SELECT whipsaw_cross_count FROM rs_conviction_state WHERE session_date = :d AND symbol = :sym LIMIT 1"),
                {"d": sd, "sym": r.symbol},
            ).fetchone()
            whip_n = int(whip.whipsaw_cross_count or 0) if whip else 0
            chop_thr = int(cfg.get("chop_warning_crosses") or 3)

            new_state = STATE_NEUTRAL
            if abs(gap) <= conv_thr and gap_shrinking and side_ok:
                new_state = STATE_CONVERGING
            if crossed and close_ok:
                new_state = STATE_TRIGGERED_CHOP if whip_n >= chop_thr else STATE_TRIGGERED
            if prev_state in (STATE_TRIGGERED, STATE_TRIGGERED_CHOP):
                expiry = float(cfg.get("expiry_atr") or 1.5)
                if abs(_gap_atr(price, vwap, atr)) > expiry:
                    new_state = STATE_EXPIRED

            sl = _sl_cost(price, vwap, atr, cfg)
            display = new_state
            if new_state in (STATE_CONVERGING, STATE_TRIGGERED, STATE_TRIGGERED_CHOP) and sl["is_late"]:
                display = STATE_LATE

            db.execute(
                text(
                    """
                    INSERT INTO rs_setup_radar (
                        session_date, symbol, side, setup_state, display_state, gap_atr, gap_prev1,
                        sl_rupees, sl_pct, ema5, vwap, price, state_since, updated_at
                    ) VALUES (
                        :d, :sym, :side, :state, :display, :gap, NULL, :slr, :slp, :ema, :vwap, :px, :since, NOW()
                    )
                    ON CONFLICT (session_date, symbol) DO UPDATE SET
                        side = EXCLUDED.side, setup_state = EXCLUDED.setup_state,
                        display_state = EXCLUDED.display_state, gap_atr = EXCLUDED.gap_atr,
                        gap_prev1 = rs_setup_radar.gap_atr,
                        sl_rupees = EXCLUDED.sl_rupees, sl_pct = EXCLUDED.sl_pct,
                        ema5 = EXCLUDED.ema5, vwap = EXCLUDED.vwap, price = EXCLUDED.price,
                        state_since = CASE WHEN rs_setup_radar.setup_state != EXCLUDED.setup_state
                                      THEN NOW() ELSE rs_setup_radar.state_since END,
                        updated_at = NOW()
                    """
                ),
                {
                    "d": sd, "sym": r.symbol, "side": "BULL" if is_bull else "BEAR",
                    "state": new_state, "display": display, "gap": round(gap, 4),
                    "slr": sl["sl_rupees"], "slp": sl["sl_pct"],
                    "ema": ema5, "vwap": vwap, "px": price,
                    "since": now if new_state != prev_state else (prev.state_since if prev else now),
                },
            )
            if new_state != prev_state:
                db.execute(
                    text(
                        """
                        INSERT INTO rs_setup_radar_log
                            (session_date, event_time, symbol, side, state_from, state_to, gap_atr, sl_pct, whipsaw_count)
                        VALUES (:d, :t, :sym, :side, :sf, :st, :gap, :slp, :w)
                        """
                    ),
                    {"d": sd, "t": now, "sym": r.symbol, "side": "BULL" if is_bull else "BEAR",
                     "sf": prev_state, "st": new_state, "gap": gap, "slp": sl["sl_pct"], "w": whip_n},
                )
            updated += 1
        db.commit()
    finally:
        db.close()
    return {"ok": True, "updated": updated}


def get_radar_for_symbols(symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    if not symbols:
        return {}
    sd = today_ist()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, setup_state, display_state, gap_atr, sl_rupees, sl_pct, ema5, vwap, state_since
                FROM rs_setup_radar WHERE session_date = :d AND symbol = ANY(:syms)
                """
            ),
            {"d": sd, "syms": symbols},
        ).fetchall()
    finally:
        db.close()
    return {
        r.symbol: {
            "setup_state": r.display_state or r.setup_state or STATE_NEUTRAL,
            "gap_atr": float(r.gap_atr or 0),
            "sl_rupees": float(r.sl_rupees or 0),
            "sl_pct": float(r.sl_pct or 0),
            "ema5": float(r.ema5 or 0),
            "vwap": float(r.vwap or 0),
            "state_since": r.state_since.isoformat() if r.state_since else None,
        }
        for r in rows
    }


def get_live_setups() -> List[Dict[str, Any]]:
    sd = today_ist()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, side, display_state, setup_state, sl_rupees, sl_pct, state_since
                FROM rs_setup_radar
                WHERE session_date = :d AND setup_state IN ('CONVERGING', 'TRIGGERED', 'TRIGGERED_CHOP', 'LATE')
                ORDER BY state_since DESC
                """
            ),
            {"d": sd},
        ).fetchall()
    finally:
        db.close()
    out = []
    for r in rows:
        since = r.state_since
        mins = 0
        if since:
            mins = int((datetime.now(IST) - since.astimezone(IST)).total_seconds() / 60)
        out.append({
            "symbol": r.symbol, "side": r.side,
            "state": r.display_state or r.setup_state,
            "sl_rupees": float(r.sl_rupees or 0),
            "sl_pct": float(r.sl_pct or 0),
            "minutes_since": mins,
        })
    return out

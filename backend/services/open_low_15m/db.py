"""DB persistence for Open-Low 15m backtest trades."""
from __future__ import annotations

import json
from typing import Any, Dict, List

from sqlalchemy import text

from backend.database import engine

_ENSURED = False


def ensure_open_low_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS open_low_15m_backtest_trade (
                    id BIGSERIAL PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    session_date DATE NOT NULL,
                    symbol TEXT NOT NULL,
                    future_symbol TEXT,
                    instrument_key TEXT,
                    tp_variant TEXT NOT NULL,
                    is_top_gainer BOOLEAN NOT NULL DEFAULT FALSE,
                    setup_open DOUBLE PRECISION,
                    setup_high DOUBLE PRECISION,
                    setup_low DOUBLE PRECISION,
                    setup_close DOUBLE PRECISION,
                    entry_time TIMESTAMPTZ,
                    entry_price DOUBLE PRECISION,
                    sl_type TEXT,
                    sl_price DOUBLE PRECISION,
                    tp_r DOUBLE PRECISION,
                    tp_price DOUBLE PRECISION,
                    exit_time TIMESTAMPTZ,
                    exit_price DOUBLE PRECISION,
                    exit_reason TEXT,
                    r_realized DOUBLE PRECISION,
                    pnl_inr DOUBLE PRECISION,
                    holding_minutes INTEGER,
                    risk_inr DOUBLE PRECISION,
                    lot_size INTEGER,
                    tp_hit BOOLEAN NOT NULL DEFAULT FALSE,
                    trail_stop_used BOOLEAN NOT NULL DEFAULT FALSE,
                    exit_vwap DOUBLE PRECISION,
                    exit_ema5 DOUBLE PRECISION,
                    exit_ema10 DOUBLE PRECISION,
                    exit_supertrend_dir INTEGER,
                    components JSONB,
                    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE (run_id, session_date, symbol, tp_variant)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_open_low_bt_run
                ON open_low_15m_backtest_trade (run_id, session_date, symbol)
                """
            )
        )
    _ENSURED = True


def upsert_trades(run_id: str, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    ensure_open_low_tables()
    n = 0
    with engine.begin() as conn:
        for r in rows:
            conn.execute(
                text(
                    """
                    INSERT INTO open_low_15m_backtest_trade (
                        run_id, session_date, symbol, future_symbol, instrument_key,
                        tp_variant, is_top_gainer,
                        setup_open, setup_high, setup_low, setup_close,
                        entry_time, entry_price, sl_type, sl_price, tp_r, tp_price,
                        exit_time, exit_price, exit_reason, r_realized, pnl_inr,
                        holding_minutes, risk_inr, lot_size, tp_hit, trail_stop_used,
                        exit_vwap, exit_ema5, exit_ema10, exit_supertrend_dir, components
                    ) VALUES (
                        :run_id, :session_date, :symbol, :future_symbol, :instrument_key,
                        :tp_variant, :is_top_gainer,
                        :setup_open, :setup_high, :setup_low, :setup_close,
                        :entry_time, :entry_price, :sl_type, :sl_price, :tp_r, :tp_price,
                        :exit_time, :exit_price, :exit_reason, :r_realized, :pnl_inr,
                        :holding_minutes, :risk_inr, :lot_size, :tp_hit, :trail_stop_used,
                        :exit_vwap, :exit_ema5, :exit_ema10, :exit_supertrend_dir, CAST(:components AS jsonb)
                    )
                    ON CONFLICT (run_id, session_date, symbol, tp_variant) DO UPDATE SET
                        entry_time = EXCLUDED.entry_time,
                        entry_price = EXCLUDED.entry_price,
                        exit_time = EXCLUDED.exit_time,
                        exit_price = EXCLUDED.exit_price,
                        exit_reason = EXCLUDED.exit_reason,
                        r_realized = EXCLUDED.r_realized,
                        pnl_inr = EXCLUDED.pnl_inr,
                        components = EXCLUDED.components,
                        computed_at = NOW()
                    """
                ),
                {
                    "run_id": run_id,
                    "session_date": r.get("session_date"),
                    "symbol": r.get("symbol"),
                    "future_symbol": r.get("future_symbol"),
                    "instrument_key": r.get("instrument_key"),
                    "tp_variant": r.get("tp_variant"),
                    "is_top_gainer": bool(r.get("is_top_gainer")),
                    "setup_open": r.get("setup_open"),
                    "setup_high": r.get("setup_high"),
                    "setup_low": r.get("setup_low"),
                    "setup_close": r.get("setup_close"),
                    "entry_time": r.get("entry_time"),
                    "entry_price": r.get("entry_price"),
                    "sl_type": r.get("sl_type"),
                    "sl_price": r.get("sl_price"),
                    "tp_r": r.get("tp_r"),
                    "tp_price": r.get("tp_price"),
                    "exit_time": r.get("exit_time"),
                    "exit_price": r.get("exit_price"),
                    "exit_reason": r.get("exit_reason"),
                    "r_realized": r.get("r_realized"),
                    "pnl_inr": r.get("pnl_inr"),
                    "holding_minutes": r.get("holding_minutes"),
                    "risk_inr": r.get("risk_inr"),
                    "lot_size": r.get("lot_size"),
                    "tp_hit": bool(r.get("tp_hit")),
                    "trail_stop_used": bool(r.get("trail_stop_used")),
                    "exit_vwap": r.get("exit_vwap"),
                    "exit_ema5": r.get("exit_ema5"),
                    "exit_ema10": r.get("exit_ema10"),
                    "exit_supertrend_dir": r.get("exit_supertrend_dir"),
                    "components": json.dumps(r),
                },
            )
            n += 1
    return n

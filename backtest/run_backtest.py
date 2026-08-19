"""Run HA Momentum signal + trade simulation. Stores PostgreSQL tables (not MySQL)."""
from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.engine import run_symbol
from backtest.fetch_candles import BACKTEST_FROM, BACKTEST_TO, load_cached, load_universe

LOG_DIR = ROOT / "logs"
RESULTS_JSON = ROOT / "data" / "ha_backtest_results.json"

logger = logging.getLogger("ha_backtest")

DDL = """
CREATE TABLE IF NOT EXISTS ha_backtest_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(30),
    instrument_key VARCHAR(60),
    direction VARCHAR(8),
    signal_time TIMESTAMP,
    entry_price NUMERIC(12,2),
    sl_price NUMERIC(12,2),
    sl_distance NUMERIC(12,4),
    sl_rs NUMERIC(12,2),
    lot_qty INTEGER,
    t1_price NUMERIC(12,2),
    t2_price NUMERIC(12,2),
    t1_hit SMALLINT,
    t2_hit SMALLINT,
    sl_hit SMALLINT,
    t1_exit_price NUMERIC(12,2),
    t2_exit_price NUMERIC(12,2),
    actual_exit_price NUMERIC(12,2),
    actual_exit_time TIMESTAMP,
    exit_reason VARCHAR(30),
    pnl_t1_rs NUMERIC(12,2),
    pnl_t2_rs NUMERIC(12,2),
    actual_pnl_rs NUMERIC(12,2),
    max_favorable NUMERIC(12,2),
    entry_candle_size_pct NUMERIC(8,4),
    sl_used_prev_candle SMALLINT,
    holding_min INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS ha_skipped_trades (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(30),
    signal_time TIMESTAMP,
    direction VARCHAR(8),
    entry_price NUMERIC(12,2),
    sl_price NUMERIC(12,2),
    sl_distance NUMERIC(12,4),
    sl_rs NUMERIC(12,2),
    lot_qty INTEGER,
    reason VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_TRADE = text(
    """
    INSERT INTO ha_backtest_trades (
        symbol, instrument_key, direction, signal_time, entry_price, sl_price,
        sl_distance, sl_rs, lot_qty, t1_price, t2_price, t1_hit, t2_hit, sl_hit,
        t1_exit_price, t2_exit_price, actual_exit_price, actual_exit_time, exit_reason,
        pnl_t1_rs, pnl_t2_rs, actual_pnl_rs, max_favorable, entry_candle_size_pct,
        sl_used_prev_candle, holding_min
    ) VALUES (
        :symbol, :instrument_key, :direction, :signal_time, :entry_price, :sl_price,
        :sl_distance, :sl_rs, :lot_qty, :t1_price, :t2_price, :t1_hit, :t2_hit, :sl_hit,
        :t1_exit_price, :t2_exit_price, :actual_exit_price, :actual_exit_time, :exit_reason,
        :pnl_t1_rs, :pnl_t2_rs, :actual_pnl_rs, :max_favorable, :entry_candle_size_pct,
        :sl_used_prev_candle, :holding_min
    )
    """
)
INSERT_SKIP = text(
    """
    INSERT INTO ha_skipped_trades (
        symbol, signal_time, direction, entry_price, sl_price, sl_distance,
        sl_rs, lot_qty, reason
    ) VALUES (
        :symbol, :signal_time, :direction, :entry_price, :sl_price, :sl_distance,
        :sl_rs, :lot_qty, :reason
    )
    """
)


def _setup_log() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "backtest.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _lot_index() -> Dict[str, int]:
    from backend.config import get_instruments_file_path

    path = get_instruments_file_path()
    out: Dict[str, int] = {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return out
    for inst in data if isinstance(data, list) else []:
        if not isinstance(inst, dict):
            continue
        key = str(inst.get("instrument_key") or "").strip()
        lot = inst.get("lot_size") or inst.get("lotSize")
        if key and lot:
            out[key] = int(lot)
    return out


def _ts(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=None) if val.tzinfo else val
    return val


def _trade_params(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": row.get("symbol"),
        "instrument_key": row.get("instrument_key"),
        "direction": row.get("direction"),
        "signal_time": _ts(row.get("signal_time")),
        "entry_price": row.get("entry_price"),
        "sl_price": row.get("sl_price"),
        "sl_distance": row.get("sl_distance"),
        "sl_rs": row.get("sl_rs"),
        "lot_qty": row.get("lot_qty"),
        "t1_price": row.get("t1_price"),
        "t2_price": row.get("t2_price"),
        "t1_hit": 1 if row.get("t1_hit") else 0,
        "t2_hit": 1 if row.get("t2_hit") else 0,
        "sl_hit": 1 if row.get("sl_hit") else 0,
        "t1_exit_price": row.get("t1_exit_price"),
        "t2_exit_price": row.get("t2_exit_price"),
        "actual_exit_price": row.get("actual_exit_price"),
        "actual_exit_time": _ts(row.get("actual_exit_time")),
        "exit_reason": row.get("exit_reason"),
        "pnl_t1_rs": row.get("pnl_t1_rs"),
        "pnl_t2_rs": row.get("pnl_t2_rs"),
        "actual_pnl_rs": row.get("actual_pnl_rs"),
        "max_favorable": row.get("max_favorable"),
        "entry_candle_size_pct": row.get("entry_candle_size_pct"),
        "sl_used_prev_candle": 1 if row.get("sl_used_prev_candle") else 0,
        "holding_min": row.get("holding_min"),
    }


def _json_safe(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        item = dict(r)
        for k, v in list(item.items()):
            if isinstance(v, datetime):
                item[k] = v.isoformat()
        item.pop("skipped", None)
        out.append(item)
    return out


def main() -> None:
    _setup_log()
    from_d = date.fromisoformat(BACKTEST_FROM)
    to_d = date.fromisoformat(BACKTEST_TO)
    universe = load_universe()
    lots = _lot_index()
    trades: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    from backend.database import SessionLocal, engine

    if engine is not None:
        with engine.begin() as conn:
            for stmt in DDL.split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
            conn.execute(text("TRUNCATE ha_backtest_trades RESTART IDENTITY"))
            conn.execute(text("TRUNCATE ha_skipped_trades RESTART IDENTITY"))

    db = SessionLocal() if SessionLocal is not None else None
    try:
        for i, u in enumerate(universe, 1):
            sym, ikey = u["stock"], u["ikey"]
            cached = load_cached(sym)
            if not cached or not cached.get("candles"):
                logger.warning("no cache for %s — skip", sym)
                continue
            lot = lots.get(ikey) or 0
            if lot <= 0:
                logger.warning("no lot size for %s %s", sym, ikey)
            trows, srows = run_symbol(
                cached["candles"],
                symbol=sym,
                instrument_key=ikey,
                lot_qty=lot,
                from_d=from_d,
                to_d=to_d,
            )
            for row in trows:
                trades.append(row)
                if db is not None:
                    try:
                        db.execute(INSERT_TRADE, _trade_params(row))
                        db.commit()
                    except Exception as exc:
                        db.rollback()
                        logger.error("trade insert failed %s: %s", sym, exc)
            for row in srows:
                skipped.append(row)
                if db is not None:
                    try:
                        db.execute(
                            INSERT_SKIP,
                            {
                                "symbol": row.get("symbol"),
                                "signal_time": _ts(row.get("signal_time")),
                                "direction": row.get("direction"),
                                "entry_price": row.get("entry_price"),
                                "sl_price": row.get("sl_price"),
                                "sl_distance": row.get("sl_distance"),
                                "sl_rs": row.get("sl_rs"),
                                "lot_qty": row.get("lot_qty"),
                                "reason": row.get("reason") or "SL_EXCEEDS_5K",
                            },
                        )
                        db.commit()
                    except Exception as exc:
                        db.rollback()
                        logger.error("skip insert failed %s: %s", sym, exc)
            if i % 25 == 0:
                logger.info("processed %s/%s  trades=%s skipped=%s", i, len(universe), len(trades), len(skipped))
    finally:
        if db is not None:
            db.close()

    RESULTS_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.write_text(
        json.dumps({"trades": _json_safe(trades), "skipped": _json_safe(skipped)}, indent=0),
        encoding="utf-8",
    )
    n = len(trades)
    t1w = sum(1 for r in trades if r.get("t1_hit"))
    t2w = sum(1 for r in trades if r.get("t2_hit"))
    t1_pct = (100.0 * t1w / n) if n else 0.0
    t2_pct = (100.0 * t2w / n) if n else 0.0
    print(
        f"Backtest complete: {n} trades | {len(skipped)} skipped | "
        f"Win rate T1: {t1_pct:.1f}% | T2: {t2_pct:.1f}%"
    )


if __name__ == "__main__":
    main()

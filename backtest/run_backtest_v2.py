"""Run 8+ HA Momentum variants against cached 15m candles."""
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

from backtest.engine_v2 import candles_to_df, nifty_session_vwap, run_prepared
from backtest.fetch_candles import BACKTEST_FROM, BACKTEST_TO, load_cached, load_universe
from backtest.fetch_nifty_candles import NIFTY_SYMBOL

LOG_DIR = ROOT / "logs"
RESULTS_JSON = ROOT / "data" / "ha_backtest_results_v2.json"
logger = logging.getLogger("ha_v2")

VARIANTS: List[Dict[str, Any]] = [
    {"name": "v1_baseline", "rr_t1": 2.0, "rr_t2": 3.0, "cutoff": "14:45", "use_fixed_sl": False, "sl_cap": 5000, "nifty_filter": False, "short_only": False, "description": "Original strategy (baseline)"},
    {"name": "v2_rr_fix", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "14:45", "use_fixed_sl": False, "sl_cap": 5000, "nifty_filter": False, "short_only": False, "description": "Fix 1 only: R:R changed to 1:1.5 / 1:2"},
    {"name": "v3_time_fix", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "13:30", "use_fixed_sl": False, "sl_cap": 5000, "nifty_filter": False, "short_only": False, "description": "Fix 1+2: R:R 1:1.5 + No entries after 13:30"},
    {"name": "v4_nifty_filter", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "13:30", "use_fixed_sl": False, "sl_cap": 5000, "nifty_filter": True, "short_only": False, "description": "Fix 1+2+3: R:R + Time + Nifty VWAP filter"},
    {"name": "v5_short_only", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "13:30", "use_fixed_sl": False, "sl_cap": 5000, "nifty_filter": True, "short_only": True, "description": "Fix 1+2+3+4: All fixes + SHORT only"},
    {"name": "v6_fixed_sl_no_skip", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.004, "sl_cap": 999999, "nifty_filter": True, "short_only": False, "description": "Fix 1+2+3+5: Fixed 0.4% SL, NO trade skip"},
    {"name": "v6b_fixed_sl_03pct", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.003, "sl_cap": 999999, "nifty_filter": True, "short_only": False, "description": "Fixed 0.3% SL, no skip (secondary)"},
    {"name": "v7_all_fixes", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.004, "sl_cap": 999999, "nifty_filter": True, "short_only": False, "description": "ALL FIXES COMBINED (v7 master)"},
    {"name": "v8_all_fixes_short_only", "rr_t1": 1.5, "rr_t2": 2.0, "cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.004, "sl_cap": 999999, "nifty_filter": True, "short_only": True, "description": "ALL FIXES + SHORT ONLY (v8)"},
]


def _setup_log() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_DIR / "backtest_v2.log"), logging.StreamHandler(sys.stdout)],
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
        if isinstance(inst, dict) and inst.get("instrument_key") and (inst.get("lot_size") or inst.get("lotSize")):
            out[str(inst["instrument_key"]).strip()] = int(inst.get("lot_size") or inst.get("lotSize"))
    return out


def _migrate(conn) -> None:
    alters = [
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS variant VARCHAR(40)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS sl_logic_used VARCHAR(20)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS nifty_close_signal NUMERIC(12,2)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS nifty_vwap_signal NUMERIC(12,2)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS nifty_above_vwap SMALLINT",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS max_adverse NUMERIC(12,2)",
        "ALTER TABLE ha_skipped_trades ADD COLUMN IF NOT EXISTS variant VARCHAR(40)",
        "ALTER TABLE ha_skipped_trades ALTER COLUMN reason TYPE VARCHAR(60)",
    ]
    for sql in alters:
        try:
            conn.execute(text(sql))
        except Exception as exc:
            logger.warning("migrate skip: %s", exc)
    conn.execute(text("UPDATE ha_backtest_trades SET variant='v1_baseline' WHERE variant IS NULL"))
    names = [v["name"] for v in VARIANTS]
    listed = ",".join("'" + n.replace("'", "") + "'" for n in names)
    conn.execute(text(f"DELETE FROM ha_backtest_trades WHERE variant IN ({listed})"))
    conn.execute(text(f"DELETE FROM ha_skipped_trades WHERE variant IN ({listed}) OR variant IS NULL"))


INSERT_TRADE = text(
    """
    INSERT INTO ha_backtest_trades (
        symbol, instrument_key, direction, signal_time, entry_price, sl_price,
        sl_distance, sl_rs, lot_qty, t1_price, t2_price, t1_hit, t2_hit, sl_hit,
        t1_exit_price, t2_exit_price, actual_exit_price, actual_exit_time, exit_reason,
        pnl_t1_rs, pnl_t2_rs, actual_pnl_rs, max_favorable, max_adverse, entry_candle_size_pct,
        sl_used_prev_candle, holding_min, variant, sl_logic_used,
        nifty_close_signal, nifty_vwap_signal, nifty_above_vwap
    ) VALUES (
        :symbol, :instrument_key, :direction, :signal_time, :entry_price, :sl_price,
        :sl_distance, :sl_rs, :lot_qty, :t1_price, :t2_price, :t1_hit, :t2_hit, :sl_hit,
        :t1_exit_price, :t2_exit_price, :actual_exit_price, :actual_exit_time, :exit_reason,
        :pnl_t1_rs, :pnl_t2_rs, :actual_pnl_rs, :max_favorable, :max_adverse, :entry_candle_size_pct,
        :sl_used_prev_candle, :holding_min, :variant, :sl_logic_used,
        :nifty_close_signal, :nifty_vwap_signal, :nifty_above_vwap
    )
    """
)
INSERT_SKIP = text(
    """
    INSERT INTO ha_skipped_trades (
        symbol, signal_time, direction, entry_price, sl_price, sl_distance,
        sl_rs, lot_qty, reason, variant
    ) VALUES (
        :symbol, :signal_time, :direction, :entry_price, :sl_price, :sl_distance,
        :sl_rs, :lot_qty, :reason, :variant
    )
    """
)


def _ts(val: Any) -> Any:
    if isinstance(val, datetime):
        return val.replace(tzinfo=None) if val.tzinfo else val
    return val


def _trade_params(row: Dict[str, Any]) -> Dict[str, Any]:
    above = row.get("nifty_above_vwap")
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
        "max_adverse": row.get("max_adverse"),
        "entry_candle_size_pct": row.get("entry_candle_size_pct"),
        "sl_used_prev_candle": 1 if row.get("sl_used_prev_candle") else 0,
        "holding_min": row.get("holding_min"),
        "variant": row.get("variant"),
        "sl_logic_used": row.get("sl_logic_used"),
        "nifty_close_signal": row.get("nifty_close_signal"),
        "nifty_vwap_signal": row.get("nifty_vwap_signal"),
        "nifty_above_vwap": None if above is None else (1 if above else 0),
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


def _summarize_variant(name: str, trades: List[Dict[str, Any]], skipped: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    t1w = sum(1 for t in trades if t.get("t1_hit"))
    t2w = sum(1 for t in trades if t.get("t2_hit"))
    pnl = round(sum(float(t.get("actual_pnl_rs") or 0) for t in trades), 2)
    holds = [int(t.get("holding_min") or 0) for t in trades]
    best = max(trades, key=lambda t: float(t.get("actual_pnl_rs") or 0), default=None)
    worst = min(trades, key=lambda t: float(t.get("actual_pnl_rs") or 0), default=None)
    return {
        "name": name,
        "trades": n,
        "skipped": len(skipped),
        "wr_t1": round(100.0 * t1w / n, 1) if n else 0.0,
        "wr_t2": round(100.0 * t2w / n, 1) if n else 0.0,
        "pnl": pnl,
        "avg_hold": round(sum(holds) / len(holds), 1) if holds else 0,
        "best": f"{(best or {}).get('symbol','')} {float((best or {}).get('actual_pnl_rs') or 0):.0f}",
        "worst": f"{(worst or {}).get('symbol','')} {float((worst or {}).get('actual_pnl_rs') or 0):.0f}",
    }


def main() -> None:
    _setup_log()
    from_d = date.fromisoformat(BACKTEST_FROM)
    to_d = date.fromisoformat(BACKTEST_TO)
    universe = load_universe()
    lots = _lot_index()
    nifty = load_cached(NIFTY_SYMBOL) or {}
    n_closes, n_vwaps, used_vol = nifty_session_vwap(nifty.get("candles") or [])
    if not used_vol:
        logger.warning("Nifty volume missing or zero — using typical-price expanding mean VWAP")
    logger.info("nifty bars=%s vwap points=%s", len(nifty.get("candles") or []), len(n_vwaps))

    frames = {}
    for u in universe:
        cached = load_cached(u["stock"])
        if not cached or not cached.get("candles"):
            continue
        frames[u["stock"]] = {
            "df": candles_to_df(cached["candles"]),
            "ikey": u["ikey"],
            "lot": lots.get(u["ikey"]) or 0,
        }
    logger.info("loaded %s symbol dataframes", len(frames))

    all_trades: List[Dict[str, Any]] = []
    all_skips: List[Dict[str, Any]] = []
    summaries = []
    for variant in VARIANTS:
        trows: List[Dict[str, Any]] = []
        srows: List[Dict[str, Any]] = []
        for sym, pack in frames.items():
            if pack["lot"] <= 0:
                continue
            tr, sk = run_prepared(
                pack["df"],
                symbol=sym,
                instrument_key=pack["ikey"],
                lot_qty=pack["lot"],
                from_d=from_d,
                to_d=to_d,
                variant=variant,
                nifty_closes=n_closes,
                nifty_vwaps=n_vwaps,
            )
            trows.extend(tr)
            srows.extend(sk)
            for row in tr:
                if float(row.get("sl_rs") or 0) > 20000:
                    logger.warning("SL_RS outlier %s %s ₹%s", variant["name"], sym, row.get("sl_rs"))
        all_trades.extend(trows)
        all_skips.extend(srows)
        summaries.append({**_summarize_variant(variant["name"], trows, srows), "description": variant["description"]})
        logger.info("%s trades=%s skipped=%s", variant["name"], len(trows), len(srows))

    from backend.database import engine

    if engine is not None:
        with engine.begin() as conn:
            from backtest.run_backtest import DDL as V1_DDL

            for stmt in V1_DDL.split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
            _migrate(conn)
            for row in all_trades:
                conn.execute(INSERT_TRADE, _trade_params(row))
            for row in all_skips:
                conn.execute(
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
                        "reason": row.get("reason"),
                        "variant": row.get("variant"),
                    },
                )

    RESULTS_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.write_text(
        json.dumps(
            {"variants": VARIANTS, "summaries": summaries, "trades": _json_safe(all_trades), "skipped": _json_safe(all_skips)},
            indent=0,
        ),
        encoding="utf-8",
    )

    print("┌──────────────────────────┬────────┬────────┬────────┬───────────────┐")
    print("│  Variant                 │ Trades │ Win%T1 │ Win%T2 │ Net P&L (Rs)  │")
    print("├──────────────────────────┼────────┼────────┼────────┼───────────────┤")
    for s in summaries:
        print(f"│  {s['name']:<22} │ {s['trades']:>6} │ {s['wr_t1']:>5.1f}% │ {s['wr_t2']:>5.1f}% │ {s['pnl']:>13,.0f} │")
    print("└──────────────────────────┴────────┴────────┴────────┴───────────────┘")


if __name__ == "__main__":
    main()

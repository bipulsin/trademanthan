"""Run HA Momentum v3 variants against cached 15m candles."""
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
from backtest.run_backtest_v2 import _json_safe, _lot_index, _ts

LOG_DIR = ROOT / "logs"
RESULTS_JSON = ROOT / "data" / "ha_backtest_results_v3.json"
logger = logging.getLogger("ha_v3")

VARIANTS: List[Dict[str, Any]] = [
    {"name": "v1_baseline", "tier": "ORIGINAL", "rr_t1": 2.0, "rr_t2": 3.0, "entry_cutoff": "14:45", "use_fixed_sl": False, "fixed_sl_pct": None, "sl_cap_rs": 5000, "nifty_filter": False, "short_only": False, "forced_exit": "15:00", "description": "v1: Original (candle SL, 1:2/1:3 RR, ₹5K cap)"},
    {"name": "v2_rr_fix", "tier": "INCREMENTAL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "14:45", "use_fixed_sl": False, "fixed_sl_pct": None, "sl_cap_rs": 5000, "nifty_filter": False, "short_only": False, "forced_exit": "15:00", "description": "v2: Fix 1 — R:R 1:1.5/1:2 (vs 2/3)"},
    {"name": "v3_time_fix", "tier": "INCREMENTAL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": False, "fixed_sl_pct": None, "sl_cap_rs": 5000, "nifty_filter": False, "short_only": False, "forced_exit": "15:00", "description": "v3: Fix 1+2 — R:R 1:1.5 + cutoff 13:30"},
    {"name": "v4_nifty_filter", "tier": "INCREMENTAL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": False, "fixed_sl_pct": None, "sl_cap_rs": 5000, "nifty_filter": True, "short_only": False, "forced_exit": "15:00", "description": "v4: Fix 1+2+3 — R:R + time + Nifty-VWAP LONG filter"},
    {"name": "v5_short_only", "tier": "INCREMENTAL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": False, "fixed_sl_pct": None, "sl_cap_rs": 5000, "nifty_filter": False, "short_only": True, "forced_exit": "15:00", "description": "v5: Fix 1+2+4 — R:R + time + SHORT only (candle SL)"},
    {"name": "v6_fixed_sl_04pct", "tier": "FIXED_SL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.004, "sl_cap_rs": 999999, "nifty_filter": True, "short_only": False, "forced_exit": "15:00", "description": "v6: Fixed 0.4% SL + all filters (both directions)"},
    {"name": "v6b_fixed_sl_03pct", "tier": "FIXED_SL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.003, "sl_cap_rs": 999999, "nifty_filter": True, "short_only": False, "forced_exit": "15:00", "description": "v6b: Fixed 0.3% SL + all filters (best current)"},
    {"name": "v7_corrected", "tier": "FIXED_SL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.003, "sl_cap_rs": 999999, "nifty_filter": True, "short_only": False, "forced_exit": "15:00", "description": "v7 CORRECTED: Fixed 0.3% SL (v7=v6b, was mislabeled)"},
    {"name": "v8_all_fixes_short", "tier": "FIXED_SL", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.004, "sl_cap_rs": 999999, "nifty_filter": False, "short_only": True, "forced_exit": "15:00", "description": "v8: Fixed 0.4% SL + SHORT only"},
    {"name": "v9_short_fixed_03pct", "tier": "NEW_HIGH_CONVICTION", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.003, "sl_cap_rs": 999999, "nifty_filter": False, "short_only": True, "forced_exit": "15:00", "description": "v9 NEW: SHORT-only + 0.3% fixed SL (untested combo)"},
    {"name": "v10_fixed_sl_02pct", "tier": "NEW_HIGH_CONVICTION", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.002, "sl_cap_rs": 999999, "nifty_filter": True, "short_only": False, "forced_exit": "15:00", "description": "v10: Fixed 0.2% SL (tighter target stress test, both dir)"},
    {"name": "v11_short_fixed_02pct", "tier": "NEW_HIGH_CONVICTION", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.002, "sl_cap_rs": 999999, "nifty_filter": False, "short_only": True, "forced_exit": "15:00", "description": "v11 NEW: SHORT-only + 0.2% fixed SL (max edge)"},
    {"name": "v12_exit_1515", "tier": "NEW_HIGH_CONVICTION", "rr_t1": 1.5, "rr_t2": 2.0, "entry_cutoff": "13:30", "use_fixed_sl": True, "fixed_sl_pct": 0.003, "sl_cap_rs": 999999, "nifty_filter": True, "short_only": False, "forced_exit": "15:15", "description": "v12: All v6b fixes + forced exit at 15:15 (vs 15:00)"},
]

LEGACY_VARIANT_NAMES = [
    "v6_fixed_sl_no_skip",
    "v7_all_fixes",
    "v8_all_fixes_short_only",
]


def _setup_log() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_DIR / "backtest_v3.log"), logging.StreamHandler(sys.stdout)],
    )


def _assert_configs() -> None:
    by_name = {v["name"]: v for v in VARIANTS}
    assert by_name["v7_corrected"]["fixed_sl_pct"] == 0.003
    assert by_name["v9_short_fixed_03pct"]["short_only"] is True
    assert by_name["v9_short_fixed_03pct"]["fixed_sl_pct"] == 0.003
    assert by_name["v11_short_fixed_02pct"]["short_only"] is True
    assert by_name["v11_short_fixed_02pct"]["fixed_sl_pct"] == 0.002
    assert by_name["v12_exit_1515"]["forced_exit"] == "15:15"
    logger.info("config checklist passed: v7=0.3%% v9 short+0.3%% v11 short+0.2%% v12 exit 15:15")
    logger.info("variant configs: %s", {v["name"]: {k: v[k] for k in ("fixed_sl_pct", "short_only", "forced_exit", "nifty_filter")} for v in VARIANTS})


def _migrate(conn) -> None:
    alters = [
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS variant VARCHAR(40)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS sl_logic_used VARCHAR(20)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS nifty_close_signal NUMERIC(12,2)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS nifty_vwap_signal NUMERIC(12,2)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS nifty_above_vwap SMALLINT",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS max_adverse NUMERIC(12,2)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS use_fixed_sl SMALLINT",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS fixed_sl_pct NUMERIC(5,4)",
        "ALTER TABLE ha_backtest_trades ADD COLUMN IF NOT EXISTS forced_exit_time VARCHAR(8)",
        "ALTER TABLE ha_skipped_trades ADD COLUMN IF NOT EXISTS variant VARCHAR(40)",
        "ALTER TABLE ha_skipped_trades ALTER COLUMN reason TYPE VARCHAR(100)",
    ]
    for sql in alters:
        try:
            conn.execute(text(sql))
        except Exception as exc:
            logger.warning("migrate skip: %s", exc)
    names = [v["name"] for v in VARIANTS] + LEGACY_VARIANT_NAMES
    listed = ",".join("'" + n.replace("'", "") + "'" for n in names)
    conn.execute(text(f"DELETE FROM ha_backtest_trades WHERE variant IN ({listed})"))
    conn.execute(text(f"DELETE FROM ha_skipped_trades WHERE variant IN ({listed})"))


INSERT_TRADE = text(
    """
    INSERT INTO ha_backtest_trades (
        symbol, instrument_key, direction, signal_time, entry_price, sl_price,
        sl_distance, sl_rs, lot_qty, t1_price, t2_price, t1_hit, t2_hit, sl_hit,
        t1_exit_price, t2_exit_price, actual_exit_price, actual_exit_time, exit_reason,
        pnl_t1_rs, pnl_t2_rs, actual_pnl_rs, max_favorable, max_adverse, entry_candle_size_pct,
        sl_used_prev_candle, holding_min, variant, sl_logic_used,
        nifty_close_signal, nifty_vwap_signal, nifty_above_vwap,
        use_fixed_sl, fixed_sl_pct, forced_exit_time
    ) VALUES (
        :symbol, :instrument_key, :direction, :signal_time, :entry_price, :sl_price,
        :sl_distance, :sl_rs, :lot_qty, :t1_price, :t2_price, :t1_hit, :t2_hit, :sl_hit,
        :t1_exit_price, :t2_exit_price, :actual_exit_price, :actual_exit_time, :exit_reason,
        :pnl_t1_rs, :pnl_t2_rs, :actual_pnl_rs, :max_favorable, :max_adverse, :entry_candle_size_pct,
        :sl_used_prev_candle, :holding_min, :variant, :sl_logic_used,
        :nifty_close_signal, :nifty_vwap_signal, :nifty_above_vwap,
        :use_fixed_sl, :fixed_sl_pct, :forced_exit_time
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
        "use_fixed_sl": 1 if row.get("use_fixed_sl") else 0,
        "fixed_sl_pct": row.get("fixed_sl_pct"),
        "forced_exit_time": row.get("forced_exit_time"),
    }


def _summarize_variant(name: str, trades: List[Dict[str, Any]], skipped: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    t1w = sum(1 for t in trades if t.get("t1_hit"))
    t2w = sum(1 for t in trades if t.get("t2_hit"))
    pnl = round(sum(float(t.get("actual_pnl_rs") or 0) for t in trades), 2)
    holds = [int(t.get("holding_min") or 0) for t in trades]
    sls = [float(t.get("sl_rs") or 0) for t in trades]
    return {
        "name": name,
        "trades": n,
        "skipped": len(skipped),
        "wr_t1": round(100.0 * t1w / n, 1) if n else 0.0,
        "wr_t2": round(100.0 * t2w / n, 1) if n else 0.0,
        "pnl": pnl,
        "avg_hold": round(sum(holds) / len(holds), 1) if holds else 0,
        "avg_sl": round(sum(sls) / len(sls), 1) if sls else 0,
    }


def main() -> None:
    _setup_log()
    _assert_configs()
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
    items = [(sym, pack) for sym, pack in frames.items() if pack["lot"] > 0]
    for variant in VARIANTS:
        print(f"Running {variant['name']}... { {k: variant[k] for k in ('fixed_sl_pct', 'short_only', 'forced_exit', 'nifty_filter')} }")
        trows: List[Dict[str, Any]] = []
        srows: List[Dict[str, Any]] = []
        for i, (sym, pack) in enumerate(items, 1):
            if i % 10 == 0:
                print(f"  {variant['name']}: {i}/{len(items)} symbols")
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
        summ = {
            **_summarize_variant(variant["name"], trows, srows),
            "description": variant["description"],
            "tier": variant["tier"],
            "forced_exit": variant["forced_exit"],
        }
        summaries.append(summ)
        print(
            f"  → {summ['trades']} trades | Win%T1: {summ['wr_t1']:.1f}% | P&L: ₹{summ['pnl']:.0f}"
        )

    from backend.database import engine

    if engine is not None:
        with engine.begin() as conn:
            from backtest.run_backtest import DDL as V1_DDL

            for stmt in V1_DDL.split(";"):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
            _migrate(conn)
            if all_trades:
                conn.execute(INSERT_TRADE, [_trade_params(r) for r in all_trades])
            if all_skips:
                conn.execute(
                    INSERT_SKIP,
                    [
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
                        }
                        for row in all_skips
                    ],
                )

    RESULTS_JSON.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_JSON.write_text(
        json.dumps(
            {"variants": VARIANTS, "summaries": summaries, "trades": _json_safe(all_trades), "skipped": _json_safe(all_skips)},
            indent=0,
        ),
        encoding="utf-8",
    )

    ranked = sorted(summaries, key=lambda s: float(s["pnl"]), reverse=True)
    print("┌──────────────────────────┬────────┬────────┬────────┬───────────────┐")
    print("│  Variant                 │ Trades │ Win%T1 │ Win%T2 │ Net P&L (Rs)  │")
    print("├──────────────────────────┼────────┼────────┼────────┼───────────────┤")
    for s in summaries:
        print(f"│  {s['name']:<22} │ {s['trades']:>6} │ {s['wr_t1']:>5.1f}% │ {s['wr_t2']:>5.1f}% │ {s['pnl']:>13,.0f} │")
    print("└──────────────────────────┴────────┴────────┴────────┴───────────────┘")
    best = ranked[0]
    print(f"BEST VARIANT: {best['name']}  Net P&L ₹{best['pnl']:,.0f}  Win%T1 {best['wr_t1']}%  trades {best['trades']}")


if __name__ == "__main__":
    main()

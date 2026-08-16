"""Orchestrate raw harvest → path labels → walk-forward score → filtered replay."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

from rocket.engine.backtester import RocketBacktester
from rocket.ml.feature_extractor import RocketFeatureExtractor
from rocket.ml.meta_filter import MetaModelConfig, RocketMetaFilter
from rocket.ml.trade_selector import DailyTradeRanker
from rocket.strategies.base_strategy import Bias, Signal
from rocket.strategies.ml_institutional import MLInstitutionalStrategy

logger = logging.getLogger(__name__)

SignalKey = Tuple[str, str, str]  # symbol, timestamp iso, side


def _ts_iso(ts: Any) -> str:
    if hasattr(ts, "isoformat"):
        return ts.isoformat()
    return str(ts)


def _side_from_bias(bias: Bias) -> str:
    return "BUY" if bias == Bias.LONG else "SELL"


def path_label_signal(
    df: pd.DataFrame,
    trigger_idx: int,
    side: str,
    stop_loss: float,
    take_profit: float,
    entry_price: float,
) -> Dict[str, Any]:
    """
    Simulate SL/TP path on subsequent bars (no portfolio coupling).

    Entry assumed next-bar open when available, else trigger close.
    """
    side_u = side.upper()
    direction = 1 if side_u in ("BUY", "LONG") else -1
    if trigger_idx + 1 < len(df):
        entry = float(df.iloc[trigger_idx + 1]["open"])
        start = trigger_idx + 1
    else:
        entry = float(entry_price)
        start = trigger_idx

    # Re-anchor SL/TP distance from actual fill if possible
    sl_dist = abs(float(entry_price) - float(stop_loss))
    tp_dist = abs(float(take_profit) - float(entry_price))
    if sl_dist <= 0:
        sl_dist = abs(entry) * 0.003
    if direction > 0:
        sl = entry - sl_dist
        tp = entry + tp_dist
    else:
        sl = entry + sl_dist
        tp = entry - tp_dist

    hit_target = False
    hit_stop = False
    exit_px = float(df.iloc[-1]["close"])
    for j in range(start, len(df)):
        bar = df.iloc[j]
        hi, lo, close = float(bar["high"]), float(bar["low"]), float(bar["close"])
        tclock = pd.Timestamp(bar["timestamp"]).tz_convert("Asia/Kolkata").time()
        if direction > 0:
            if lo <= sl:
                hit_stop, exit_px = True, sl
                break
            if hi >= tp:
                hit_target, exit_px = True, tp
                break
        else:
            if hi >= sl:
                hit_stop, exit_px = True, sl
                break
            if lo <= tp:
                hit_target, exit_px = True, tp
                break
        if tclock >= datetime.strptime("15:20", "%H:%M").time():
            exit_px = close
            break

    pnl = (exit_px - entry) * direction
    r_mult = pnl / sl_dist if sl_dist > 0 else 0.0
    target_met = int(hit_target or r_mult >= 1.5)
    return {
        "entry_price": entry,
        "exit_price": exit_px,
        "pnl": pnl,
        "pnl_r": r_mult,
        "hit_target_first": int(hit_target),
        "hit_stop_first": int(hit_stop),
        "target_met": target_met,
        "stop_distance": sl_dist,
    }


class AllowedSignalGate:
    """Only queue signals whose (symbol, ts, side) is in the selected set."""

    def __init__(self, keys: Set[SignalKey]):
        self.keys = set(keys)

    def __call__(self, ts: datetime, signals: List[Signal]) -> List[Signal]:
        kept: List[Signal] = []
        for sig in signals:
            key = (sig.symbol, _ts_iso(ts), _side_from_bias(sig.bias))
            if key in self.keys:
                kept.append(sig)
        return kept


def harvest_raw_signals(
    bt: RocketBacktester,
    start: date,
    end: date,
    *,
    min_confidence: float = 0.58,
) -> pd.DataFrame:
    """Walk the event timeline and collect every strategy candidate (no fills)."""
    if not bt.series:
        bt.fetch_data(start, end)
    enriched = RocketFeatureExtractor.enrich_universe(bt.series)
    bt.series = enriched

    strategy = MLInstitutionalStrategy(
        min_confidence=min_confidence,
        max_signals_per_bar=10_000,  # do not truncate; meta layer selects daily
    )
    contract_by_sym = {c.symbol: c for c in bt.contracts if c.symbol in bt.series}

    events: Dict[pd.Timestamp, List[str]] = {}
    indexed: Dict[str, pd.DataFrame] = {}
    for sym, df in bt.series.items():
        dfi = df.set_index("timestamp").sort_index()
        indexed[sym] = dfi
        for ts in dfi.index:
            events.setdefault(ts, []).append(sym)
    timeline = sorted(events.keys())

    rows: List[Dict[str, Any]] = []
    for ts in timeline:
        snapshot: Dict[str, Dict[str, Any]] = {}
        for sym in events[ts]:
            bar = indexed[sym].loc[ts]
            c = contract_by_sym[sym]
            row = bar.to_dict()
            row["instrument_key"] = c.instrument_key
            row["lot_size"] = c.lot_size
            row["tick_size"] = c.tick_size
            row["position"] = None
            # Strategy feature columns may already exist from enrich_features
            snapshot[sym] = row

        signals = strategy.generate_signals(ts.to_pydatetime(), snapshot)
        for sig in signals:
            df = bt.series[sig.symbol]
            ts_vals = pd.to_datetime(df["timestamp"], utc=True)
            target = pd.Timestamp(ts)
            if target.tzinfo is None:
                target = target.tz_localize("UTC")
            else:
                target = target.tz_convert("UTC")
            eq = ts_vals == target
            if eq.any():
                trigger_idx = int(np.flatnonzero(eq.to_numpy())[0])
            else:
                trigger_idx = int((ts_vals - target).abs().to_numpy().argmin())

            side = _side_from_bias(sig.bias)
            feats = RocketFeatureExtractor.extract_trade_features(df, trigger_idx, side)
            label_info = path_label_signal(
                df,
                trigger_idx,
                side,
                float(sig.stop_loss or 0.0),
                float(sig.target or 0.0),
                float(snapshot[sig.symbol]["close"]),
            )
            rec = {
                "timestamp": ts.to_pydatetime(),
                "trade_date": ts.date() if hasattr(ts, "date") else pd.Timestamp(ts).date(),
                "symbol": sig.symbol,
                "instrument_key": sig.instrument_key,
                "side": side,
                "bias": sig.bias.value,
                "strategy_confidence": float(sig.confidence),
                "stop_loss": sig.stop_loss,
                "take_profit": sig.target,
                "trigger_idx": trigger_idx,
                **feats,
                **label_info,
            }
            rows.append(rec)

    out = pd.DataFrame(rows)
    logger.info("Harvested %s raw candidate signals", len(out))
    return out


def run_comparative_meta_backtest(
    bt: RocketBacktester,
    start: date,
    end: date,
    *,
    min_probability: float = 0.65,
    min_per_day: int = 2,
    max_per_day: int = 4,
    min_confidence: float = 0.58,
) -> Dict[str, Any]:
    """
    1) Baseline backtest (existing strategy truncation)
    2) Harvest all candidates + path labels
    3) Walk-forward score + daily top-K
    4) Filtered backtest reusing same engine/PnL path
    """
    if not bt.series:
        bt.fetch_data(start, end)

    # Ensure strategy features exist (enrich_features already applied in fetch)
    # Add meta indicators on top
    bt.series = RocketFeatureExtractor.enrich_universe(bt.series)

    # --- Baseline ---
    baseline_strategy = MLInstitutionalStrategy(min_confidence=min_confidence, max_signals_per_bar=3)
    bt.strategy = baseline_strategy
    bt.signal_filter = None
    baseline = bt.run(start, end)
    baseline["label"] = "raw_baseline"

    # --- Harvest + score ---
    raw = harvest_raw_signals(bt, start, end, min_confidence=min_confidence)
    meta = RocketMetaFilter(MetaModelConfig(scoring_threshold=min_probability))
    if raw.empty:
        filtered = dict(baseline)
        filtered["label"] = "ml_filtered"
        filtered["total_trades"] = 0
        return {
            "baseline": baseline,
            "filtered": filtered,
            "comparison": build_comparison_table(baseline, filtered),
            "meta_metrics": {"note": "no_raw_signals"},
            "selected_count": 0,
            "raw_signal_count": 0,
        }

    scored = meta.score_walk_forward(raw)
    ranker = DailyTradeRanker(
        meta,
        min_probability_threshold=min_probability,
        min_trades_per_day=min_per_day,
        max_trades_per_day=max_per_day,
    )
    selected = ranker.rank_and_select(scored)
    keys = ranker.selected_keys(selected)
    logger.info(
        "Meta selected %s / %s signals (%.1f%%)",
        len(selected),
        len(raw),
        100.0 * len(selected) / max(1, len(raw)),
    )

    # --- Filtered replay: emit all candidates, gate by selected keys ---
    filtered_strategy = MLInstitutionalStrategy(
        min_confidence=min_confidence,
        max_signals_per_bar=10_000,
    )
    bt.strategy = filtered_strategy
    bt.signal_filter = AllowedSignalGate(keys)
    filtered = bt.run(start, end)
    filtered["label"] = "ml_filtered"
    filtered["selected_signals"] = len(selected)
    filtered["raw_signals"] = len(raw)

    comparison = build_comparison_table(baseline, filtered)
    return {
        "baseline": baseline,
        "filtered": filtered,
        "comparison": comparison,
        "meta_metrics": meta.train_metrics,
        "selected_count": len(selected),
        "raw_signal_count": int(len(raw)),
        "selected_trades": selected,
        "avg_trades_per_day_baseline": _avg_trades_per_day(baseline),
        "avg_trades_per_day_filtered": _avg_trades_per_day(filtered),
    }


def _avg_trades_per_day(metrics: Dict[str, Any]) -> float:
    trades = metrics.get("trades") or []
    if not trades:
        return 0.0
    days = set()
    for t in trades:
        et = t.get("entry_time") or ""
        days.add(str(et)[:10])
    return float(len(trades) / max(1, len(days)))


def build_comparison_table(
    baseline: Dict[str, Any],
    filtered: Dict[str, Any],
) -> List[Dict[str, Any]]:
    def _pf(m: Dict[str, Any]) -> str:
        pf = m.get("profit_factor")
        if pf is None and m.get("profit_factor_raw") == float("inf"):
            return "∞"
        return f"{pf}" if pf is not None else "—"

    rows = [
        {
            "metric": "Total Trades",
            "baseline": baseline.get("total_trades"),
            "filtered": filtered.get("total_trades"),
        },
        {
            "metric": "Win Rate (%)",
            "baseline": baseline.get("win_rate_pct"),
            "filtered": filtered.get("win_rate_pct"),
        },
        {
            "metric": "Profit Factor",
            "baseline": _pf(baseline),
            "filtered": _pf(filtered),
        },
        {
            "metric": "Max Drawdown (%)",
            "baseline": baseline.get("max_drawdown_pct"),
            "filtered": filtered.get("max_drawdown_pct"),
        },
        {
            "metric": "Expectancy / Trade (₹)",
            "baseline": baseline.get("expectancy"),
            "filtered": filtered.get("expectancy"),
        },
        {
            "metric": "Net Return (%)",
            "baseline": baseline.get("net_return_pct"),
            "filtered": filtered.get("net_return_pct"),
        },
        {
            "metric": "Final Equity (₹)",
            "baseline": baseline.get("final_equity"),
            "filtered": filtered.get("final_equity"),
        },
        {
            "metric": "Avg Trades / Day",
            "baseline": round(_avg_trades_per_day(baseline), 2),
            "filtered": round(_avg_trades_per_day(filtered), 2),
        },
        {
            "metric": "Total Costs (₹)",
            "baseline": float((baseline.get("costs") or {}).get("total", 0)),
            "filtered": float((filtered.get("costs") or {}).get("total", 0)),
        },
    ]
    return rows


def print_comparison(comparison: Sequence[Dict[str, Any]], console: Any = None) -> None:
    from rich.console import Console
    from rich.table import Table

    con = console or Console()
    table = Table(title="Rocket — Baseline vs ML Meta-Filter", show_header=True)
    table.add_column("Metric")
    table.add_column("Raw Strategy (Baseline)", justify="right")
    table.add_column("ML-Filtered Strategy", justify="right")
    for row in comparison:
        table.add_row(str(row["metric"]), str(row["baseline"]), str(row["filtered"]))
    con.print(table)

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


class SizedSignalGate:
    """Admit selected signals and overlay Kelly lots + tiered SL/TP."""

    def __init__(self, by_key: Dict[SignalKey, Dict[str, Any]]):
        self.by_key = dict(by_key)

    def __call__(self, ts: datetime, signals: List[Signal]) -> List[Signal]:
        kept: List[Signal] = []
        for sig in signals:
            key = (sig.symbol, _ts_iso(ts), _side_from_bias(sig.bias))
            meta = self.by_key.get(key)
            if not meta:
                continue
            lots = int(meta.get("lots") or 1)
            sig.lots = max(1, lots)
            if meta.get("stop_loss") is not None:
                sig.stop_loss = float(meta["stop_loss"])
            if meta.get("take_profit") is not None:
                sig.target = float(meta["take_profit"])
            elif meta.get("target_price") is not None:
                sig.target = float(meta["target_price"])
            if meta.get("win_probability") is not None:
                sig.confidence = float(meta["win_probability"])
            if meta.get("atr") is not None:
                try:
                    atr_v = float(meta["atr"])
                    if atr_v > 0:
                        sig.atr = atr_v
                except (TypeError, ValueError):
                    pass
            tier = meta.get("tier")
            sig.reason = f"ml_meta_kelly_t{tier}" if tier is not None else "ml_meta_kelly"
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
        atr_stop_mult=1.8,
        atr_target_mult=3.2,
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
            bar_close = float(snapshot[sig.symbol]["close"])
            contract = contract_by_sym.get(sig.symbol)
            atr_val = float(
                snapshot[sig.symbol].get("atr")
                or snapshot[sig.symbol].get("safe_atr")
                or snapshot[sig.symbol].get("atr_14")
                or bar_close * 0.005
            )
            label_info = path_label_signal(
                df,
                trigger_idx,
                side,
                float(sig.stop_loss or 0.0),
                float(sig.target or 0.0),
                bar_close,
            )
            snap = snapshot[sig.symbol]
            rec = {
                "timestamp": ts.to_pydatetime(),
                "trade_date": ts.date() if hasattr(ts, "date") else pd.Timestamp(ts).date(),
                "symbol": sig.symbol,
                "instrument_key": sig.instrument_key,
                "side": side,
                "bias": sig.bias.value,
                "strategy_confidence": float(sig.confidence),
                "entry_price": bar_close,
                "close": bar_close,
                "atr": atr_val,
                "safe_atr": float(snap.get("safe_atr") or atr_val),
                "lot_size": int(snap.get("lot_size") or (contract.lot_size if contract else 1)),
                "ema_5": snap.get("ema_5"),
                "ema_10": snap.get("ema_10"),
                "vwap": snap.get("vwap"),
                "ema5_dist_atr": snap.get("ema5_dist_atr"),
                "raw_rsi_14": snap.get("rsi_14"),
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
    min_probability: float = 0.50,
    min_per_day: int = 2,
    max_per_day: int = 3,
    min_confidence: float = 0.58,
    kelly_factor: float = 0.35,
) -> Dict[str, Any]:
    """
    1) Baseline backtest (existing strategy truncation)
    2) Harvest all candidates + path labels
    3) Walk-forward score + daily top-K with Kelly sizing
    4) Filtered backtest reusing same engine/PnL path
    """
    if not bt.series:
        bt.fetch_data(start, end)

    # Ensure strategy features exist (enrich_features already applied in fetch)
    # Add meta indicators on top
    bt.series = RocketFeatureExtractor.enrich_universe(bt.series)

    # --- Baseline ---
    baseline_strategy = MLInstitutionalStrategy(
        min_confidence=min_confidence,
        max_signals_per_bar=3,
        atr_stop_mult=1.8,
        atr_target_mult=3.2,
    )
    bt.strategy = baseline_strategy
    bt.signal_filter = None
    baseline = bt.run(start, end)
    baseline["label"] = "raw_baseline"
    baseline["interval"] = bt.interval

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
        kelly_factor=kelly_factor,
    )
    selected = ranker.rank_and_select(scored)
    by_key = ranker.selected_by_key(selected)
    logger.info(
        "Meta selected %s / %s signals (%.1f%%); tier1=%s tier2=%s avg_lots=%.2f",
        len(selected),
        len(raw),
        100.0 * len(selected) / max(1, len(raw)),
        sum(1 for s in selected if int(s.get("tier") or 0) == 1),
        sum(1 for s in selected if int(s.get("tier") or 0) == 2),
        float(np.mean([s.get("lots") or 1 for s in selected])) if selected else 0.0,
    )

    # --- Filtered replay: emit all candidates, gate by selected keys + Kelly overlay ---
    filtered_strategy = MLInstitutionalStrategy(
        min_confidence=min_confidence,
        max_signals_per_bar=10_000,
        atr_stop_mult=1.8,
        atr_target_mult=3.2,
    )
    bt.strategy = filtered_strategy
    bt.signal_filter = SizedSignalGate(by_key)
    filtered = bt.run(start, end)
    filtered["label"] = "ml_filtered"
    filtered["interval"] = bt.interval
    filtered["selected_signals"] = len(selected)
    filtered["raw_signals"] = len(raw)
    filtered["kelly_factor"] = kelly_factor

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


def run_timeframe_comparison(
    *,
    start: date,
    end: date,
    intervals: Sequence[str] = ("5minute", "15minute"),
    capital: float = 10_000_000.0,
    limit: int = 200,
    min_probability: float = 0.50,
    kelly_factor: float = 0.35,
    min_per_day: int = 2,
    max_per_day: int = 3,
) -> Dict[str, Any]:
    """Run meta-filtered Kelly backtests across intervals and build a side-by-side report."""
    results: Dict[str, Any] = {}
    for interval in intervals:
        iv = str(interval).strip()
        logger.info("=== Timeframe comparison: %s ===", iv)
        bt = RocketBacktester(interval=iv, capital=capital, max_symbols=limit)
        bt.load_universe()
        bt.fetch_data(start, end)
        results[iv] = run_comparative_meta_backtest(
            bt,
            start,
            end,
            min_probability=min_probability,
            min_per_day=min_per_day,
            max_per_day=max_per_day,
            kelly_factor=kelly_factor,
        )

    timeframe_comparison = build_timeframe_comparison_table(results)
    # Primary metrics for HTML = first interval's filtered run, with extras
    primary_iv = str(intervals[0]).strip()
    primary = dict(results[primary_iv]["filtered"])
    primary["comparison"] = results[primary_iv]["comparison"]
    primary["baseline"] = results[primary_iv]["baseline"]
    primary["raw_signal_count"] = results[primary_iv].get("raw_signal_count")
    primary["selected_count"] = results[primary_iv].get("selected_count")
    primary["timeframe_results"] = {
        iv: {
            "baseline": res["baseline"],
            "filtered": res["filtered"],
            "comparison": res["comparison"],
            "selected_count": res.get("selected_count"),
            "raw_signal_count": res.get("raw_signal_count"),
        }
        for iv, res in results.items()
    }
    primary["timeframe_comparison"] = timeframe_comparison
    primary["intervals"] = list(results.keys())
    return {"primary_metrics": primary, "by_interval": results, "timeframe_comparison": timeframe_comparison}


def build_timeframe_comparison_table(by_interval: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Rows with one column per interval (filtered Kelly meta path)."""

    def _pf(m: Dict[str, Any]) -> Any:
        pf = m.get("profit_factor")
        if pf is None and m.get("profit_factor_raw") == float("inf"):
            return "∞"
        return pf if pf is not None else "—"

    intervals = list(by_interval.keys())
    specs = [
        ("Total Trades", lambda m: m.get("total_trades")),
        ("Win Rate (%)", lambda m: m.get("win_rate_pct")),
        ("Profit Factor", _pf),
        ("Max Drawdown (%)", lambda m: m.get("max_drawdown_pct")),
        ("Expectancy / Trade (₹)", lambda m: m.get("expectancy")),
        ("Net Return (%)", lambda m: m.get("net_return_pct")),
        ("Final Equity (₹)", lambda m: m.get("final_equity")),
        ("Avg Trades / Day", lambda m: round(_avg_trades_per_day(m), 2)),
        ("Total Costs (₹)", lambda m: float((m.get("costs") or {}).get("total", 0))),
        ("Selected Signals", lambda m: m.get("selected_signals") or m.get("selected_count")),
    ]
    rows: List[Dict[str, Any]] = []
    for label, fn in specs:
        row: Dict[str, Any] = {"metric": label}
        for iv in intervals:
            filtered = by_interval[iv]["filtered"]
            row[iv] = fn(filtered)
        rows.append(row)
    return rows


def print_timeframe_comparison(
    comparison: Sequence[Dict[str, Any]],
    intervals: Sequence[str],
    console: Any = None,
) -> None:
    from rich.console import Console
    from rich.table import Table

    con = console or Console()
    table = Table(title="Rocket — 5m vs 15m (ML Meta + Kelly)", show_header=True)
    table.add_column("Metric")
    for iv in intervals:
        table.add_column(str(iv), justify="right")
    for row in comparison:
        table.add_row(str(row["metric"]), *[str(row.get(iv, "—")) for iv in intervals])
    con.print(table)


def _exit_label(n: Optional[int]) -> str:
    if n is None or int(n) <= 0:
        return "none"
    n = int(n)
    return f"{n}bars/{n * 5}m"


def _stagnation_exit_share(metrics: Dict[str, Any]) -> float:
    trades = metrics.get("trades") or []
    if not trades:
        return 0.0
    n = sum(1 for t in trades if str(t.get("reason") or "") == "time_stagnation_exit")
    return 100.0 * n / len(trades)


def run_time_exit_comparison(
    *,
    start: date,
    end: date,
    bars: Sequence[int] = (2, 4, 6),
    interval: str = "5minute",
    capital: float = 10_000_000.0,
    limit: int = 200,
    min_probability: float = 0.50,
    kelly_factor: float = 0.35,
    min_per_day: int = 2,
    max_per_day: int = 3,
    min_confidence: float = 0.58,
    time_exit_atr_min: float = 0.5,
    include_none: bool = True,
) -> Dict[str, Any]:
    """
    Harvest + score once, then replay the same meta-selected signals under each
    stagnation horizon (and optionally a no-time-exit control).
    """
    bt = RocketBacktester(
        interval=interval,
        capital=capital,
        max_symbols=limit,
        time_exit_bars=None,
        time_exit_atr_min=time_exit_atr_min,
    )
    bt.load_universe()
    bt.fetch_data(start, end)
    bt.series = RocketFeatureExtractor.enrich_universe(bt.series)

    # Baseline once (no meta gate, no time exit)
    baseline_strategy = MLInstitutionalStrategy(
        min_confidence=min_confidence,
        max_signals_per_bar=3,
        atr_stop_mult=1.8,
        atr_target_mult=3.2,
    )
    bt.strategy = baseline_strategy
    bt.signal_filter = None
    bt.time_exit_bars = None
    baseline = bt.run(start, end)
    baseline["label"] = "raw_baseline"

    raw = harvest_raw_signals(bt, start, end, min_confidence=min_confidence)
    meta = RocketMetaFilter(MetaModelConfig(scoring_threshold=min_probability))
    if raw.empty:
        empty = dict(baseline)
        empty["label"] = "ml_filtered"
        empty["total_trades"] = 0
        return {
            "primary_metrics": empty,
            "by_horizon": {},
            "time_exit_comparison": [],
            "baseline": baseline,
        }

    scored = meta.score_walk_forward(raw)
    ranker = DailyTradeRanker(
        meta,
        min_probability_threshold=min_probability,
        min_trades_per_day=min_per_day,
        max_trades_per_day=max_per_day,
        kelly_factor=kelly_factor,
    )
    selected = ranker.rank_and_select(scored)
    by_key = ranker.selected_by_key(selected)
    logger.info(
        "Time-exit sweep: %s selected / %s raw; horizons=%s",
        len(selected),
        len(raw),
        list(bars),
    )

    filtered_strategy = MLInstitutionalStrategy(
        min_confidence=min_confidence,
        max_signals_per_bar=10_000,
        atr_stop_mult=1.8,
        atr_target_mult=3.2,
    )
    horizons: List[Optional[int]] = []
    if include_none:
        horizons.append(None)
    horizons.extend(int(n) for n in bars)

    results: Dict[str, Any] = {}
    for n in horizons:
        label = _exit_label(n)
        logger.info("=== Time-exit comparison: %s ===", label)
        bt.strategy = filtered_strategy
        bt.signal_filter = SizedSignalGate(by_key)
        bt.time_exit_bars = n
        bt.time_exit_atr_min = float(time_exit_atr_min)
        filtered = bt.run(start, end)
        filtered["label"] = f"ml_filtered_{label}"
        filtered["selected_signals"] = len(selected)
        filtered["raw_signals"] = len(raw)
        filtered["kelly_factor"] = kelly_factor
        filtered["stagnation_exit_pct"] = _stagnation_exit_share(filtered)
        results[label] = {
            "baseline": baseline,
            "filtered": filtered,
            "comparison": build_comparison_table(baseline, filtered),
            "selected_count": len(selected),
            "raw_signal_count": int(len(raw)),
        }

    time_exit_comparison = build_time_exit_comparison_table(results)
    # Prefer 4bars/20m as primary dashboard view when present; else first horizon
    primary_key = "4bars/20m" if "4bars/20m" in results else next(iter(results))
    primary = dict(results[primary_key]["filtered"])
    primary["comparison"] = results[primary_key]["comparison"]
    primary["baseline"] = baseline
    primary["raw_signal_count"] = results[primary_key].get("raw_signal_count")
    primary["selected_count"] = results[primary_key].get("selected_count")
    primary["time_exit_results"] = results
    primary["time_exit_comparison"] = time_exit_comparison
    primary["time_exit_horizons"] = list(results.keys())
    primary["meta_metrics"] = meta.train_metrics
    return {
        "primary_metrics": primary,
        "by_horizon": results,
        "time_exit_comparison": time_exit_comparison,
        "baseline": baseline,
    }


def build_time_exit_comparison_table(by_horizon: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _pf(m: Dict[str, Any]) -> Any:
        pf = m.get("profit_factor")
        if pf is None and m.get("profit_factor_raw") == float("inf"):
            return "∞"
        return pf if pf is not None else "—"

    horizons = list(by_horizon.keys())
    specs = [
        ("Total Trades", lambda m: m.get("total_trades")),
        ("Win Rate (%)", lambda m: m.get("win_rate_pct")),
        ("Profit Factor", _pf),
        ("Max Drawdown (%)", lambda m: m.get("max_drawdown_pct")),
        ("Expectancy / Trade (₹)", lambda m: m.get("expectancy")),
        ("Net Return (%)", lambda m: m.get("net_return_pct")),
        ("Final Equity (₹)", lambda m: m.get("final_equity")),
        ("Avg Trades / Day", lambda m: round(_avg_trades_per_day(m), 2)),
        ("Stagnation Exits (%)", lambda m: round(float(m.get("stagnation_exit_pct") or 0.0), 2)),
        ("Total Costs (₹)", lambda m: float((m.get("costs") or {}).get("total", 0))),
    ]
    rows: List[Dict[str, Any]] = []
    for label, fn in specs:
        row: Dict[str, Any] = {"metric": label}
        for h in horizons:
            row[h] = fn(by_horizon[h]["filtered"])
        rows.append(row)
    return rows


def print_time_exit_comparison(
    comparison: Sequence[Dict[str, Any]],
    horizons: Sequence[str],
    console: Any = None,
) -> None:
    from rich.console import Console
    from rich.table import Table

    con = console or Console()
    table = Table(title="Rocket — Dynamic Time / Stagnation Exits", show_header=True)
    table.add_column("Metric")
    for h in horizons:
        table.add_column(str(h), justify="right")
    for row in comparison:
        table.add_row(str(row["metric"]), *[str(row.get(h, "—")) for h in horizons])
    con.print(table)


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

"""Orchestrate raw harvest → path labels → walk-forward score → filtered replay."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd

from rocket.engine.backtester import RocketBacktester, compute_structural_stop_target
from rocket.engine.order_book import Side
from rocket.engine.portfolio import Position
from rocket.ml.feature_extractor import RocketFeatureExtractor
from rocket.ml.meta_filter import MetaModelConfig, RocketMetaFilter
from rocket.ml.trade_selector import (
    ConfluenceGatesConfig,
    DailyTradeRanker,
    ENTRY_CURFEW_END,
    ENTRY_CURFEW_START,
)
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


def _ist_time(ts: Any):
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    return t.tz_convert("Asia/Kolkata").time()


def label_simulated_trade(trade_pnl_r: float, exit_reason: str) -> int:
    """Expansion-only labels: y=1 iff take-profit or realized R ≥ 1.5."""
    if str(exit_reason or "") == "take_profit":
        return 1
    try:
        if float(trade_pnl_r) >= 1.5:
            return 1
    except (TypeError, ValueError):
        return 0
    return 0


def path_label_signal(
    df: pd.DataFrame,
    trigger_idx: int,
    side: str,
    stop_loss: float,
    take_profit: float,
    entry_price: float,
    *,
    safe_atr: Optional[float] = None,
    ema_20: Optional[float] = None,
    ema_10: Optional[float] = None,
    vwap: Optional[float] = None,
    time_exit_bars: int = 4,
    time_exit_atr_min: float = 0.5,
) -> Dict[str, Any]:
    """
    Replay the profit-taking state machine for expansion-only labels.

    Uses structural 1.2–1.6×ATR stops, max(1.8R, 2.2×ATR) targets, hard
    breakeven at +1.0×ATR, trail +1.6/1.2×ATR, 4-bar stagnation, and 15:00 IST
    EOD. Early EMA20 invalidation is an execution cut (backtester only) so
    training labels keep the ~0.30–0.35 expansion base rate.
    ``y=1`` only for take_profit or R≥1.5; BE/scratch/stop = 0.
    Legacy ``stop_loss`` / ``take_profit`` args are ignored when ATR is available.
    """
    side_u = side.upper()
    direction = 1 if side_u in ("BUY", "LONG") else -1
    if trigger_idx + 1 < len(df):
        entry = float(df.iloc[trigger_idx + 1]["open"])
        start = trigger_idx + 1
    else:
        entry = float(entry_price)
        start = trigger_idx

    atr = float(safe_atr) if safe_atr is not None and float(safe_atr) > 0 else abs(entry) * 0.005
    levels = compute_structural_stop_target(
        side=side_u,
        entry_price=entry,
        ema_20=ema_20,
        ema_10=ema_10,
        vwap=vwap,
        safe_atr=atr,
    )
    sl = float(levels["stop_loss"])
    tp = float(levels["take_profit"])
    sl_dist = float(levels["stop_distance"])
    # Fallback if caller forced legacy distances without ATR context
    if atr <= 0 and stop_loss and take_profit:
        sl_dist = abs(float(entry_price) - float(stop_loss)) or abs(entry) * 0.003
        tp_dist = abs(float(take_profit) - float(entry_price))
        if direction > 0:
            sl, tp = entry - sl_dist, entry + tp_dist
        else:
            sl, tp = entry + sl_dist, entry - tp_dist

    pos = Position(
        symbol="_label",
        instrument_key="",
        side=Side.BUY if direction > 0 else Side.SELL,
        quantity=1,
        avg_price=entry,
        lot_size=1,
        stop_loss=sl,
        take_profit=tp,
        atr=atr,
        peak_favorable_price=entry,
        bars_in_trade=0,
        time_exit_armed=True,
    )

    hit_target = False
    hit_stop = False
    exit_reason = "backtest_end"
    exit_px = float(df.iloc[-1]["close"])

    for j in range(start, len(df)):
        bar = df.iloc[j]
        # Skip exit checks on the fill bar (matches backtester)
        if j == start:
            continue
        hi, lo, close = float(bar["high"]), float(bar["low"]), float(bar["close"])
        pos.update_mfe(high=hi, low=lo)
        pos.maybe_disarm_time_exit(time_exit_atr_min)
        pos.bars_in_trade += 1
        pos.update_breakeven_stop(high=hi, low=lo, trigger_atr_mult=1.0, buffer=0.05)
        pos.update_trailing_stop(high=hi, low=lo, activate_at_r=1.6, trail_atr_mult=1.2)

        if direction > 0:
            if pos.take_profit is not None and hi >= pos.take_profit:
                hit_target, exit_px, exit_reason = True, float(pos.take_profit), "take_profit"
                break
            if pos.stop_loss is not None and lo <= pos.stop_loss:
                hit_stop, exit_px = True, float(pos.stop_loss)
                exit_reason = pos.stop_exit_reason()
                break
        else:
            if pos.take_profit is not None and lo <= pos.take_profit:
                hit_target, exit_px, exit_reason = True, float(pos.take_profit), "take_profit"
                break
            if pos.stop_loss is not None and hi >= pos.stop_loss:
                hit_stop, exit_px = True, float(pos.stop_loss)
                exit_reason = pos.stop_exit_reason()
                break

        if pos.should_stagnation_exit(
            time_exit_bars=time_exit_bars, time_exit_atr_min=time_exit_atr_min
        ):
            exit_px, exit_reason = close, "time_stagnation_exit"
            break

        tclock = _ist_time(bar["timestamp"])
        if tclock >= datetime.strptime("15:00", "%H:%M").time():
            exit_px, exit_reason = close, "eod_flat"
            break

    pnl = (exit_px - entry) * direction
    r_mult = pnl / sl_dist if sl_dist > 0 else 0.0
    target_met = label_simulated_trade(r_mult, exit_reason)
    return {
        "entry_price": entry,
        "exit_price": exit_px,
        "pnl": pnl,
        "pnl_r": r_mult,
        "hit_target_first": int(hit_target),
        "hit_stop_first": int(hit_stop),
        "target_met": target_met,
        "stop_distance": sl_dist,
        "exit_reason": exit_reason,
        "stop_loss": sl,
        "take_profit": tp,
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
        tclock = _ist_time(ts)
        # Entry curfew: only harvest labels in the tradable window
        if tclock < ENTRY_CURFEW_START or tclock > ENTRY_CURFEW_END:
            continue
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
            snap = snapshot[sig.symbol]
            atr_val = float(
                snap.get("atr")
                or snap.get("safe_atr")
                or snap.get("atr_14")
                or bar_close * 0.005
            )
            label_info = path_label_signal(
                df,
                trigger_idx,
                side,
                float(sig.stop_loss or 0.0),
                float(sig.target or 0.0),
                bar_close,
                safe_atr=atr_val,
                ema_20=snap.get("ema_20"),
                ema_10=snap.get("ema_10"),
                vwap=snap.get("vwap"),
                time_exit_bars=4,
                time_exit_atr_min=0.5,
            )
            rec = {
                "timestamp": ts.to_pydatetime(),
                "trade_date": ts.date() if hasattr(ts, "date") else pd.Timestamp(ts).date(),
                "symbol": sig.symbol,
                "instrument_key": sig.instrument_key,
                "side": side,
                "bias": sig.bias.value,
                "strategy_confidence": float(sig.confidence),
                "entry_price": label_info.get("entry_price", bar_close),
                "close": bar_close,
                "atr": atr_val,
                "safe_atr": float(snap.get("safe_atr") or atr_val),
                "lot_size": int(snap.get("lot_size") or (contract.lot_size if contract else 1)),
                "ema_5": snap.get("ema_5"),
                "ema_10": snap.get("ema_10"),
                "ema_20": snap.get("ema_20"),
                "vwap": snap.get("vwap"),
                "ema5_dist_atr": snap.get("ema5_dist_atr"),
                "ema20_dist_atr": snap.get("ema20_dist_atr"),
                "raw_rsi_14": snap.get("rsi_14"),
                "rvol": snap.get("rvol"),
                "rvol_raw": snap.get("rvol"),
                "ema_20_15m": snap.get("ema_20_15m"),
                "vwap_15m": snap.get("vwap_15m"),
                "close_15m": snap.get("close_15m"),
                "clv": snap.get("clv"),
                "market_breadth": snap.get("market_breadth"),
                "stop_loss": label_info.get("stop_loss", sig.stop_loss),
                "take_profit": label_info.get("take_profit", sig.target),
                "trigger_idx": trigger_idx,
                **feats,
                **label_info,
            }
            rows.append(rec)

    out = pd.DataFrame(rows)
    pos_rate = float(out["target_met"].mean()) if not out.empty and "target_met" in out.columns else 0.0
    logger.info("Harvested %s raw candidate signals (expansion-only pos_rate=%.3f)", len(out), pos_rate)
    if not out.empty and "exit_reason" in out.columns:
        logger.info("Harvest exit reasons:\n%s", out["exit_reason"].value_counts().to_string())
    return out


def run_comparative_meta_backtest(
    bt: RocketBacktester,
    start: date,
    end: date,
    *,
    min_probability: float = 0.12,
    min_per_day: int = 2,
    max_per_day: int = 3,
    min_confidence: float = 0.58,
    kelly_factor: float = 0.35,
    gates: Optional[ConfluenceGatesConfig] = None,
) -> Dict[str, Any]:
    """
    1) Baseline backtest (existing strategy truncation)
    2) Harvest all candidates + path labels
    3) Walk-forward score + daily top-K with Kelly sizing
    4) Filtered backtest reusing same engine/PnL path
    """
    cfg = gates if gates is not None else ConfluenceGatesConfig(p_min=float(min_probability))
    if gates is not None and min_probability is not None and abs(float(min_probability) - float(gates.p_min)) > 1e-12:
        # Explicit CLI --min-prob overrides gates.p_min
        from dataclasses import replace as _replace

        cfg = _replace(gates, p_min=float(min_probability))

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
    meta = RocketMetaFilter(MetaModelConfig(scoring_threshold=cfg.p_min))
    if raw.empty:
        filtered = dict(baseline)
        filtered["label"] = "ml_filtered"
        filtered["total_trades"] = 0
        filtered["confluence_gates"] = cfg.as_dict()
        return {
            "baseline": baseline,
            "filtered": filtered,
            "comparison": build_comparison_table(baseline, filtered),
            "meta_metrics": {"note": "no_raw_signals"},
            "selected_count": 0,
            "raw_signal_count": 0,
            "gates": cfg,
        }

    scored = meta.score_walk_forward(raw)
    ranker = DailyTradeRanker(
        meta,
        min_probability_threshold=cfg.p_min,
        min_trades_per_day=min_per_day,
        max_trades_per_day=max_per_day,
        kelly_factor=kelly_factor,
        gates=cfg,
    )
    selected = ranker.rank_and_select(scored)
    by_key = ranker.selected_by_key(selected)
    logger.info(
        "Meta selected %s / %s signals (%.1f%%); gates=%s; tier1=%s tier2=%s avg_lots=%.2f",
        len(selected),
        len(raw),
        100.0 * len(selected) / max(1, len(raw)),
        cfg.label(),
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
    filtered["confluence_gates"] = cfg.as_dict()

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
        "gates": cfg,
    }


def run_timeframe_comparison(
    *,
    start: date,
    end: date,
    intervals: Sequence[str] = ("5minute", "15minute"),
    capital: float = 10_000_000.0,
    limit: int = 200,
    min_probability: float = 0.12,
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
            gates=ConfluenceGatesConfig(p_min=float(min_probability)),
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
    min_probability: float = 0.12,
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
    gates = ConfluenceGatesConfig(p_min=float(min_probability))
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
    meta = RocketMetaFilter(MetaModelConfig(scoring_threshold=gates.p_min))
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
        min_probability_threshold=gates.p_min,
        min_trades_per_day=min_per_day,
        max_trades_per_day=max_per_day,
        kelly_factor=kelly_factor,
        gates=gates,
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


# Calibrated confluence sweep grid (Jul 15–Aug 14 tuning matrix)
CONFLUENCE_SWEEP_P_MIN = (0.33, 0.34, 0.36)
CONFLUENCE_SWEEP_CLV = (0.15, 0.20, 0.25)
CONFLUENCE_SWEEP_BREADTH = ((0.50, 0.50), (0.52, 0.48))
CONFLUENCE_SWEEP_RVOL = (1.15, 1.25)
TARGET_TRADE_LO = 18
TARGET_TRADE_HI = 35


def iter_confluence_sweep_grid() -> List[ConfluenceGatesConfig]:
    grid: List[ConfluenceGatesConfig] = []
    for p_min in CONFLUENCE_SWEEP_P_MIN:
        for clv in CONFLUENCE_SWEEP_CLV:
            for b_long, b_short in CONFLUENCE_SWEEP_BREADTH:
                for rvol in CONFLUENCE_SWEEP_RVOL:
                    grid.append(
                        ConfluenceGatesConfig(
                            p_min=float(p_min),
                            p_max=0.85,
                            clv_threshold=float(clv),
                            breadth_long_min=float(b_long),
                            breadth_short_max=float(b_short),
                            rvol_min=float(rvol),
                        )
                    )
    return grid


def _pf_numeric(m: Dict[str, Any]) -> float:
    pf = m.get("profit_factor")
    if pf is None and m.get("profit_factor_raw") == float("inf"):
        return float("inf")
    try:
        return float(pf) if pf is not None else float("-inf")
    except (TypeError, ValueError):
        return float("-inf")


def pick_best_confluence_config(
    rows: Sequence[Dict[str, Any]],
    *,
    trade_lo: int = TARGET_TRADE_LO,
    trade_hi: int = TARGET_TRADE_HI,
) -> Optional[Dict[str, Any]]:
    """Prefer 18–35 trades with highest PF; else closest trade count then highest PF."""
    in_band = [r for r in rows if trade_lo <= int(r.get("total_trades") or 0) <= trade_hi]
    pool = in_band if in_band else list(rows)
    if not pool:
        return None

    def _key(r: Dict[str, Any]) -> Tuple[float, float, int]:
        n = int(r.get("total_trades") or 0)
        pf = float(r.get("profit_factor_num") or _pf_numeric(r))
        if in_band:
            return (pf, -abs(n - 26), n)
        # Outside band: minimize distance to midpoint, then maximize PF
        mid = 0.5 * (trade_lo + trade_hi)
        return (-abs(n - mid), pf, n)

    return max(pool, key=_key)


def run_confluence_sweep(
    *,
    start: date,
    end: date,
    interval: str = "5minute",
    capital: float = 10_000_000.0,
    limit: int = 200,
    kelly_factor: float = 0.35,
    min_per_day: int = 0,
    max_per_day: int = 3,
    min_confidence: float = 0.58,
    time_exit_bars: Optional[int] = 4,
    time_exit_atr_min: float = 0.5,
    grid: Optional[Sequence[ConfluenceGatesConfig]] = None,
) -> Dict[str, Any]:
    """
    Harvest + walk-forward score once, then replay each confluence gate combo.
    Identifies the config in the 18–35 trade band with the highest profit factor.
    """
    configs = list(grid) if grid is not None else iter_confluence_sweep_grid()
    te_bars = int(time_exit_bars) if time_exit_bars and int(time_exit_bars) > 0 else None

    bt = RocketBacktester(
        interval=interval,
        capital=capital,
        max_symbols=limit,
        time_exit_bars=te_bars,
        time_exit_atr_min=time_exit_atr_min,
    )
    bt.load_universe()
    bt.fetch_data(start, end)
    bt.series = RocketFeatureExtractor.enrich_universe(bt.series)

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

    raw = harvest_raw_signals(bt, start, end, min_confidence=min_confidence)
    meta = RocketMetaFilter(MetaModelConfig(scoring_threshold=0.36))
    if raw.empty:
        return {
            "baseline": baseline,
            "rows": [],
            "best": None,
            "raw_signal_count": 0,
            "primary_metrics": baseline,
        }

    scored = meta.score_walk_forward(raw)
    filtered_strategy = MLInstitutionalStrategy(
        min_confidence=min_confidence,
        max_signals_per_bar=10_000,
        atr_stop_mult=1.8,
        atr_target_mult=3.2,
    )

    rows: List[Dict[str, Any]] = []
    by_label: Dict[str, Any] = {}
    for i, cfg in enumerate(configs, start=1):
        label = cfg.label()
        logger.info("=== Confluence sweep %s/%s: %s ===", i, len(configs), label)
        ranker = DailyTradeRanker(
            meta,
            min_probability_threshold=cfg.p_min,
            min_trades_per_day=min_per_day,
            max_trades_per_day=max_per_day,
            kelly_factor=kelly_factor,
            gates=cfg,
        )
        selected = ranker.rank_and_select(scored)
        by_key = ranker.selected_by_key(selected)
        bt.strategy = filtered_strategy
        bt.signal_filter = SizedSignalGate(by_key)
        bt.time_exit_bars = te_bars
        bt.time_exit_atr_min = float(time_exit_atr_min)
        filtered = bt.run(start, end)
        filtered["label"] = f"sweep_{label}"
        filtered["selected_signals"] = len(selected)
        filtered["raw_signals"] = len(raw)
        filtered["kelly_factor"] = kelly_factor
        filtered["confluence_gates"] = cfg.as_dict()

        n_trades = int(filtered.get("total_trades") or 0)
        pf_num = _pf_numeric(filtered)
        row = {
            "label": label,
            "p_min": cfg.p_min,
            "clv": cfg.clv_threshold,
            "breadth_long": cfg.breadth_long_min,
            "breadth_short": cfg.breadth_short_max,
            "rvol_min": cfg.rvol_min,
            "selected": len(selected),
            "total_trades": n_trades,
            "win_rate_pct": filtered.get("win_rate_pct"),
            "profit_factor": filtered.get("profit_factor"),
            "profit_factor_num": pf_num,
            "net_return_pct": filtered.get("net_return_pct"),
            "max_drawdown_pct": filtered.get("max_drawdown_pct"),
            "expectancy": filtered.get("expectancy"),
            "in_target_band": TARGET_TRADE_LO <= n_trades <= TARGET_TRADE_HI,
            "gates": cfg,
            "filtered": filtered,
            "comparison": build_comparison_table(baseline, filtered),
        }
        rows.append(row)
        by_label[label] = row

    best = pick_best_confluence_config(rows)
    if best is not None:
        logger.info(
            "Best confluence config: %s | trades=%s PF=%s in_band=%s",
            best["label"],
            best["total_trades"],
            best.get("profit_factor"),
            best.get("in_target_band"),
        )

    primary = dict((best or rows[0])["filtered"]) if (best or rows) else dict(baseline)
    if best or rows:
        chosen = best or rows[0]
        primary["comparison"] = chosen["comparison"]
        primary["baseline"] = baseline
        primary["raw_signal_count"] = int(len(raw))
        primary["selected_count"] = chosen.get("selected")
        primary["confluence_gates"] = chosen["gates"].as_dict()
        primary["confluence_sweep_best"] = chosen["label"]
        primary["meta_metrics"] = meta.train_metrics

    return {
        "baseline": baseline,
        "rows": rows,
        "best": best,
        "by_label": by_label,
        "raw_signal_count": int(len(raw)),
        "primary_metrics": primary,
        "meta_metrics": meta.train_metrics,
        "grid_size": len(configs),
    }


def print_confluence_sweep(rows: Sequence[Dict[str, Any]], console: Any = None) -> None:
    from rich.console import Console
    from rich.table import Table

    con = console or Console()
    table = Table(title="Rocket — Confluence Gate Sweep", show_header=True)
    table.add_column("P_min", justify="right")
    table.add_column("CLV±", justify="right")
    table.add_column("Breadth L/S", justify="right")
    table.add_column("RVOL", justify="right")
    table.add_column("Selected", justify="right")
    table.add_column("Trades", justify="right")
    table.add_column("WR%", justify="right")
    table.add_column("PF", justify="right")
    table.add_column("Net%", justify="right")
    table.add_column("Band", justify="center")
    for r in rows:
        band = "✓" if r.get("in_target_band") else ""
        pf = r.get("profit_factor")
        pf_s = "∞" if pf is None and r.get("profit_factor_num") == float("inf") else str(pf if pf is not None else "—")
        table.add_row(
            f"{float(r['p_min']):.2f}",
            f"{float(r['clv']):.2f}",
            f"{float(r['breadth_long']):.2f}/{float(r['breadth_short']):.2f}",
            f"{float(r['rvol_min']):.2f}",
            str(r.get("selected")),
            str(r.get("total_trades")),
            str(r.get("win_rate_pct") if r.get("win_rate_pct") is not None else "—"),
            pf_s,
            str(r.get("net_return_pct") if r.get("net_return_pct") is not None else "—"),
            band,
        )
    con.print(table)
    best = pick_best_confluence_config(rows)
    if best is not None:
        con.print(
            f"[bold green]Best[/bold green] {best['label']} · "
            f"trades={best['total_trades']} · PF={best.get('profit_factor')} · "
            f"in_band={best.get('in_target_band')}"
        )


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

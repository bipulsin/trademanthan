"""Event-driven multi-asset futures backtester."""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import pytz

from rocket.analytics.performance import compute_performance
from rocket.data_feed.upstox_client import UpstoxCandleClient
from rocket.database.connection import session_scope
from rocket.database.models import FuturesContract, load_active_current_month_contracts
from rocket.engine.costs import FuturesCostModel
from rocket.engine.order_book import Order, OrderBook, Side
from rocket.engine.portfolio import Portfolio
from rocket.strategies.base_strategy import BaseStrategy, Bias, Signal
from rocket.strategies.ml_institutional import MLInstitutionalStrategy
from rocket.config.settings import get_settings

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    return tr.rolling(n, min_periods=max(3, n // 2)).mean()


def enrich_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    c = out["close"]
    out["ret_1"] = c.pct_change()
    out["ret_5"] = c.pct_change(5)
    out["mom_10"] = c.pct_change(10)
    tp = (out["high"] + out["low"] + out["close"]) / 3.0
    vol = out["volume"].fillna(0.0).clip(lower=0.0)
    cum_vol = vol.cumsum().replace(0, np.nan)
    out["vwap"] = (tp * vol).cumsum() / cum_vol
    out["vwap_dist_pct"] = (c - out["vwap"]) / c.replace(0, np.nan)
    vol_mean = vol.rolling(20, min_periods=5).mean()
    vol_std = vol.rolling(20, min_periods=5).std().replace(0, np.nan)
    out["vol_z"] = (vol - vol_mean) / vol_std
    out["atr"] = _atr(out["high"], out["low"], out["close"])
    out["safe_atr"] = np.where(out["atr"] > 0, out["atr"], c * 0.002)
    out["atr_pct"] = out["atr"] / c.replace(0, np.nan)
    out["range_pct"] = (out["high"] - out["low"]) / c.replace(0, np.nan)
    out["ema_5"] = c.ewm(span=5, adjust=False).mean()
    out["ema_10"] = c.ewm(span=10, adjust=False).mean()
    out["ema_20"] = c.ewm(span=20, adjust=False).mean()
    out["ema5_dist_atr"] = (c - out["ema_5"]).abs() / out["safe_atr"].replace(0, np.nan)
    out["ema20_dist_atr"] = (c - out["ema_20"]).abs() / out["safe_atr"].replace(0, np.nan)
    oi = out["oi"] if "oi" in out.columns else pd.Series(0.0, index=out.index)
    out["oi_chg_pct"] = oi.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out


def compute_structural_stop_target(
    *,
    side: str,
    entry_price: float,
    ema_10: Optional[float] = None,
    ema_20: Optional[float] = None,
    vwap: Optional[float] = None,
    safe_atr: float,
) -> Dict[str, float]:
    """
    Volatility-buffered structural stop (EMA20/VWAP) with 1.4–2.0×ATR band.

    SELL: SL = max(entry+1.4ATR, min(struct, entry+2.0ATR))
          struct = max(EMA20, VWAP) if above entry else entry+1.4ATR
    BUY:  SL = min(entry-1.4ATR, max(struct, entry-2.0ATR))
          struct = min(EMA20, VWAP) if below entry else entry-1.4ATR
    Target = entry ± max(2.5×|entry−SL|, 3.0×ATR)
    """
    side_u = str(side).upper()
    entry = float(entry_price)
    atr = float(safe_atr) if safe_atr and safe_atr > 0 else abs(entry) * 0.005
    e20 = float(ema_20) if ema_20 is not None and np.isfinite(float(ema_20)) else None
    # Backward-compat: callers that only pass ema_10 still work as a structural hint
    if e20 is None and ema_10 is not None and np.isfinite(float(ema_10)):
        e20 = float(ema_10)
    vw = float(vwap) if vwap is not None and np.isfinite(float(vwap)) else None

    if side_u in ("SELL", "SHORT"):
        candidates = [x for x in (e20, vw) if x is not None and x > entry]
        structural = float(max(candidates)) if candidates else (entry + 1.4 * atr)
        floor_sl = entry + 1.4 * atr
        cap_sl = entry + 2.0 * atr
        stop_loss = max(floor_sl, min(structural, cap_sl))
        stop_kind = "vol_buffered" if candidates else "atr_floor"
        stop_dist = abs(stop_loss - entry)
        target_dist = max(2.5 * stop_dist, 3.0 * atr)
        take_profit = entry - target_dist
    else:
        candidates = [x for x in (e20, vw) if x is not None and x < entry]
        structural = float(min(candidates)) if candidates else (entry - 1.4 * atr)
        floor_sl = entry - 1.4 * atr  # closest allowed (highest price for long SL)
        cap_sl = entry - 2.0 * atr  # farthest allowed
        stop_loss = min(floor_sl, max(structural, cap_sl))
        stop_kind = "vol_buffered" if candidates else "atr_floor"
        stop_dist = abs(entry - stop_loss)
        target_dist = max(2.5 * stop_dist, 3.0 * atr)
        take_profit = entry + target_dist

    return {
        "stop_loss": float(stop_loss),
        "take_profit": float(take_profit),
        "stop_distance": float(stop_dist),
        "target_distance": float(target_dist),
        "stop_kind": 1.0 if stop_kind == "vol_buffered" else 0.0,
    }


class RocketBacktester:
    def __init__(
        self,
        *,
        strategy: Optional[BaseStrategy] = None,
        capital: Optional[float] = None,
        interval: str = "5minute",
        max_symbols: int = 200,
        max_positions: Optional[int] = None,
        signal_filter: Optional[Callable[[datetime, List[Signal]], List[Signal]]] = None,
        time_exit_bars: Optional[int] = None,
        time_exit_atr_min: float = 0.5,
    ):
        self.settings = get_settings()
        self.strategy = strategy or MLInstitutionalStrategy()
        self.capital = float(capital if capital is not None else self.settings.rocket_initial_capital)
        self.interval = interval
        self.max_symbols = int(max_symbols)
        self.max_positions = int(
            max_positions if max_positions is not None else self.settings.rocket_max_positions
        )
        self.signal_filter = signal_filter
        self.time_exit_bars = int(time_exit_bars) if time_exit_bars is not None else None
        self.time_exit_atr_min = float(time_exit_atr_min)
        self.client = UpstoxCandleClient()
        self.contracts: List[FuturesContract] = []
        self.series: Dict[str, pd.DataFrame] = {}  # symbol -> enriched df

    def load_universe(self) -> List[FuturesContract]:
        with session_scope() as session:
            self.contracts = load_active_current_month_contracts(
                session, limit=self.max_symbols
            )
        return self.contracts

    def fetch_data(self, start: date, end: date) -> Dict[str, pd.DataFrame]:
        if not self.contracts:
            self.load_universe()
        keys = [c.instrument_key for c in self.contracts]
        raw = self.client.fetch_universe(keys, self.interval, start, end)
        by_ik = {c.instrument_key: c for c in self.contracts}
        self.series = {}
        for ik, df in raw.items():
            c = by_ik.get(ik)
            if c is None or df is None or df.empty:
                continue
            enriched = enrich_features(df)
            # filter to backtest window (inclusive session days)
            start_ts = IST.localize(datetime.combine(start, time(0, 0)))
            end_ts = IST.localize(datetime.combine(end, time(23, 59, 59)))
            mask = (enriched["timestamp"] >= start_ts) & (enriched["timestamp"] <= end_ts)
            enriched = enriched.loc[mask].reset_index(drop=True)
            if enriched.empty:
                continue
            self.series[c.symbol] = enriched
        logger.info("Series ready for %s symbols", len(self.series))
        return self.series

    def run(self, start: date, end: date) -> Dict[str, Any]:
        if not self.series:
            self.fetch_data(start, end)
        if not self.series:
            raise RuntimeError("No candle data available for Rocket backtest")

        contract_by_sym = {c.symbol: c for c in self.contracts if c.symbol in self.series}
        book = OrderBook(
            default_slippage_ticks=self.settings.rocket_slippage_ticks,
            default_slippage_bps=self.settings.rocket_slippage_bps,
        )
        portfolio = Portfolio(
            initial_capital=self.capital,
            margin_pct=self.settings.rocket_initial_margin_pct,
            max_margin_utilization_pct=self.settings.rocket_max_margin_utilization_pct,
            cost_model=FuturesCostModel(self.settings.rocket_brokerage_per_order),
        )

        # Build unified event timeline
        events: Dict[pd.Timestamp, List[str]] = {}
        indexed: Dict[str, pd.DataFrame] = {}
        for sym, df in self.series.items():
            dfi = df.set_index("timestamp").sort_index()
            indexed[sym] = dfi
            for ts in dfi.index:
                events.setdefault(ts, []).append(sym)

        timeline = sorted(events.keys())
        self.strategy.on_start({"start": start, "end": end, "symbols": list(indexed)})

        pending_entries: List[Signal] = []  # fill next bar
        last_day: Optional[date] = None

        for ts in timeline:
            day = ts.date() if hasattr(ts, "date") else pd.Timestamp(ts).date()
            # EOD flatten previous day at last mark
            if last_day is not None and day != last_day and portfolio.positions:
                marks = {
                    s: float(indexed[s].loc[:ts].iloc[-1]["close"])
                    for s in list(portfolio.positions)
                    if s in indexed and not indexed[s].loc[:ts].empty
                }
                # use previous day's last available — approximate with current marks
                portfolio.force_close_all(marks, ts.to_pydatetime(), reason="session_close")
            last_day = day

            # Execute pending entries at this bar's open
            still_pending: List[Signal] = []
            for sig in pending_entries:
                if sig.symbol not in indexed or ts not in indexed[sig.symbol].index:
                    still_pending.append(sig)
                    continue
                if sig.symbol in portfolio.positions:
                    continue
                if len(portfolio.positions) >= self.max_positions:
                    continue
                bar = indexed[sig.symbol].loc[ts]
                c = contract_by_sym[sig.symbol]
                lots = max(1, int(sig.lots or 1))
                px = float(bar["open"])
                qty = c.lot_size * lots
                # Margin gate: degrade multi-lot size before skipping
                if not portfolio.can_open(px, qty):
                    degraded = False
                    while lots > 1 and not portfolio.can_open(px, c.lot_size * lots):
                        lots -= 1
                        degraded = True
                    qty = c.lot_size * lots
                    if not portfolio.can_open(px, qty):
                        continue
                    if degraded:
                        logger.debug(
                            "margin degrade %s lots→%s @ %s",
                            sig.symbol,
                            lots,
                            ts,
                        )
                side = Side.BUY if sig.bias == Bias.LONG else Side.SELL
                order = Order(
                    symbol=sig.symbol,
                    instrument_key=c.instrument_key,
                    side=side,
                    quantity=qty,
                    stop_loss=sig.stop_loss,
                    take_profit=sig.target,
                    confidence=sig.confidence,
                    reason=sig.reason,
                )
                book.submit(order)
                fill = book.match_market(
                    order,
                    ref_price=px,
                    tick_size=c.tick_size,
                    timestamp=ts.to_pydatetime(),
                )
                if fill:
                    portfolio.apply_fill(
                        fill,
                        lot_size=c.lot_size,
                        stop_loss=sig.stop_loss,
                        take_profit=sig.target,
                        confidence=sig.confidence,
                        reason="entry",
                        atr=getattr(sig, "atr", None),
                    )
            pending_entries = still_pending

            # Build snapshot + manage exits
            snapshot: Dict[str, Dict[str, Any]] = {}
            marks: Dict[str, float] = {}
            for sym in events[ts]:
                bar = indexed[sym].loc[ts]
                c = contract_by_sym[sym]
                row = bar.to_dict()
                row["instrument_key"] = c.instrument_key
                row["lot_size"] = c.lot_size
                row["tick_size"] = c.tick_size
                pos = portfolio.positions.get(sym)
                row["position"] = pos
                snapshot[sym] = row
                marks[sym] = float(bar["close"])

            # Exits (skip same bar as entry). Priority: TP → SL/trail → stagnation → EOD
            for sym, pos in list(portfolio.positions.items()):
                if sym not in snapshot:
                    continue
                if pos.opened_at is not None and pos.opened_at == ts.to_pydatetime():
                    continue
                bar = snapshot[sym]
                hi, lo, close = float(bar["high"]), float(bar["low"]), float(bar["close"])
                c = contract_by_sym[sym]

                pos.update_mfe(high=hi, low=lo)
                pos.maybe_disarm_time_exit(self.time_exit_atr_min)
                pos.bars_in_trade += 1

                # After +1.0×ATR profit, ratchet stop to 2.0×ATR from bar extreme
                pos.update_trailing_stop(
                    high=hi,
                    low=lo,
                    activate_at_r=1.0,
                    trail_atr_mult=2.0,
                )
                exit_px = None
                reason = ""

                # 1) Take profit
                if pos.side == Side.BUY:
                    if pos.take_profit is not None and hi >= pos.take_profit:
                        exit_px, reason = pos.take_profit, "take_profit"
                    # 2) Stop / trailing stop
                    elif pos.stop_loss is not None and lo <= pos.stop_loss:
                        exit_px, reason = (
                            pos.stop_loss,
                            "trailing_stop" if pos.trail_activated else "stop_loss",
                        )
                else:
                    if pos.take_profit is not None and lo <= pos.take_profit:
                        exit_px, reason = pos.take_profit, "take_profit"
                    elif pos.stop_loss is not None and hi >= pos.stop_loss:
                        exit_px, reason = (
                            pos.stop_loss,
                            "trailing_stop" if pos.trail_activated else "stop_loss",
                        )

                # 3) Dynamic stagnation exit at close of bar N
                if exit_px is None and pos.should_stagnation_exit(
                    time_exit_bars=self.time_exit_bars,
                    time_exit_atr_min=self.time_exit_atr_min,
                ):
                    exit_px, reason = close, "time_stagnation_exit"

                # 4) Flatten near session end
                tclock = ts.timetz().replace(tzinfo=None) if hasattr(ts, "timetz") else ts.time()
                if exit_px is None and tclock >= time(15, 0):
                    exit_px, reason = close, "eod_flat"

                if exit_px is not None:
                    exit_side = Side.SELL if pos.side == Side.BUY else Side.BUY
                    order = Order(
                        symbol=sym,
                        instrument_key=pos.instrument_key,
                        side=exit_side,
                        quantity=pos.quantity,
                        reason=reason,
                    )
                    book.submit(order)
                    fill = book.match_market(
                        order,
                        ref_price=float(exit_px),
                        tick_size=c.tick_size,
                        timestamp=ts.to_pydatetime(),
                    )
                    if fill:
                        portfolio.apply_fill(fill, lot_size=c.lot_size, reason=reason)

            portfolio.update_marks(marks, ts.to_pydatetime())

            # New signals → optional meta-filter → queue for next bar open
            signals = self.strategy.generate_signals(ts.to_pydatetime(), snapshot)
            if self.signal_filter is not None:
                signals = self.signal_filter(ts.to_pydatetime(), signals)
            for sig in signals:
                if sig.symbol in portfolio.positions:
                    continue
                if any(p.symbol == sig.symbol for p in pending_entries):
                    continue
                pending_entries.append(sig)

        # Final flatten
        if portfolio.positions:
            last_ts = timeline[-1]
            marks = {
                s: float(indexed[s].iloc[-1]["close"])
                for s in list(portfolio.positions)
                if s in indexed
            }
            portfolio.force_close_all(marks, last_ts.to_pydatetime(), reason="backtest_end")

        self.strategy.on_finish({"portfolio": portfolio})
        metrics = compute_performance(
            initial_capital=self.capital,
            equity_curve=portfolio.equity_curve,
            trades=portfolio.closed_trades,
            costs=portfolio.costs,
        )
        metrics["start_date"] = start.isoformat()
        metrics["end_date"] = end.isoformat()
        metrics["interval"] = self.interval
        metrics["universe_size"] = len(self.series)
        metrics["strategy"] = self.strategy.name
        metrics["time_exit_bars"] = self.time_exit_bars
        metrics["time_exit_atr_min"] = self.time_exit_atr_min
        return metrics

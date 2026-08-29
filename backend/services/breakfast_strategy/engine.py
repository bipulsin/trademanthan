"""Per-day Breakfast Strategy simulation."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from backend.services.breakfast_strategy.candles import (
    IST,
    _bar_dt,
    candle_ohlcv,
    bar_move_pct,
    anchor_bar,
    first_5m_bar,
    ist_ts,
    monitor_from_after_anchor,
    resolve_nifty_first_5m_bar,
    session_5m_bars_after_entry,
    session_has_stock_bars,
    session_vwap_by_bar_time,
    setup_bar_vs_prev_close,
)
from backend.services.breakfast_strategy.config import (
    PNL_CAP_INR,
    SECTORS_TO_PICK,
    SLIPPAGE_PCT,
    SL_PCT,
    STOCK_MOVE_CAP_PCT,
    STOCKS_PER_SECTOR,
    TIME_EXIT,
    TP_PCT,
)
from backend.services.breakfast_strategy.universe import (
    StockRow,
    display_symbol_for,
    display_symbol_spot_proxy,
    fo_eligible_sector_keys,
    format_instrument_label,
    pick_stocks_in_sector,
    rank_sectors,
    resolve_eq_spot_with_fut_lot,
    resolve_stock_instrument,
)

from backend.services.upstox_service import UpstoxService

NIFTY50_KEY = UpstoxService.NIFTY50_KEY


@dataclass
class TradeResult:
    session_date: date
    symbol: str
    underlying_symbol: str
    instrument_label: str
    direction: str
    sector: str
    sector_index: str
    sector_rank: int
    stock_rank: int
    nifty_bias: str
    nifty_bias_pct: Optional[float]
    nifty_open_5m: Optional[float]
    nifty_close_5m: Optional[float]
    stock_move_pct_at_entry: float
    setup_open_5m: float
    setup_high_5m: float
    setup_low_5m: float
    setup_close_5m: float
    setup_volume_5m: float
    instrument_key: str
    lot_size: int
    entry_time: datetime
    entry_price: float
    anchor_price: float
    sl_price: float
    tp_price: float
    pre_exit_extreme: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_trigger_type: Optional[str] = None
    pnl_inr: Optional[float] = None
    pnl_points: Optional[float] = None
    price_source: str = "futures"
    notes: List[str] = field(default_factory=list)

    def to_db_row(self, *, mode: str = "backtest") -> Dict[str, Any]:
        return {
            "session_date": self.session_date.isoformat(),
            "symbol": self.symbol,
            "underlying_symbol": self.underlying_symbol,
            "instrument_label": self.instrument_label,
            "direction": self.direction,
            "mode": mode,
            "strategy_status": "shadow",
            "sector": self.sector,
            "sector_index": self.sector_index,
            "sector_rank": self.sector_rank,
            "stock_rank": self.stock_rank,
            "nifty_bias": self.nifty_bias,
            "nifty_bias_pct": self.nifty_bias_pct,
            "nifty_open_5m": self.nifty_open_5m,
            "nifty_close_5m": self.nifty_close_5m,
            "stock_move_pct_at_entry": self.stock_move_pct_at_entry,
            "setup_open_5m": self.setup_open_5m,
            "setup_high_5m": self.setup_high_5m,
            "setup_low_5m": self.setup_low_5m,
            "setup_close_5m": self.setup_close_5m,
            "setup_volume_5m": self.setup_volume_5m,
            "instrument_key": self.instrument_key,
            "lot_size": self.lot_size,
            "price_source": self.price_source,
            "entry_time": self.entry_time.isoformat(),
            "entry_price": self.entry_price,
            "anchor_price": self.anchor_price,
            "sl_price": self.sl_price,
            "tp_price": self.tp_price,
            "pre_exit_extreme": self.pre_exit_extreme,
            "exit_time": self.exit_time.isoformat() if self.exit_time else None,
            "exit_price": self.exit_price,
            "exit_trigger_type": self.exit_trigger_type,
            "pnl_inr": self.pnl_inr,
            "pnl_points": self.pnl_points,
            "notes": "; ".join(self.notes) if self.notes else None,
        }


def nifty_bias_from_bar(bar: Dict[str, Any]) -> Tuple[str, float]:
    """NIFTY first-5m close vs open; flat (0%) → long branch."""
    pct = bar_move_pct(bar)
    if pct is None:
        return "positive", 0.0
    if pct < 0:
        return "negative", float(pct)
    return "positive", float(pct)


def _nifty_bias(bar: Dict[str, Any]) -> Tuple[str, float]:
    return nifty_bias_from_bar(bar)


@dataclass
class BreakfastStockPick:
    row: StockRow
    stock_rank: int
    move_pct: float
    signal_bar: Dict[str, Any]
    anchor_bar: Dict[str, Any]
    candles: List[Dict[str, Any]]


@dataclass
class BreakfastSectorPick:
    sector_key: str
    sector_rank: int
    sector_move_pct: float
    sector_volume: float
    stocks: List[BreakfastStockPick]


@dataclass
class BreakfastSelection:
    nifty_bar: Dict[str, Any]
    nifty_bias: str
    nifty_bias_pct: float
    long_side: bool
    ranked_sectors: List[Tuple[str, float, float]]
    sector_picks: List[BreakfastSectorPick]
    sym_to_candles: Dict[str, List[Dict[str, Any]]]
    stock_bars: Dict[str, Dict[str, Any]]
    anchor_bars: Dict[str, Dict[str, Any]]
    stock_move_pcts: Dict[str, float]
    session_rows: Dict[str, StockRow]


def select_breakfast_picks(
    session_date: date,
    *,
    nifty_candles: List[Dict[str, Any]],
    sector_candles: Dict[str, List[Dict[str, Any]]],
    stock_candles_by_key: Dict[str, List[Dict[str, Any]]],
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    upstox: Optional[Any] = None,
    nifty_bar: Optional[Dict[str, Any]] = None,
    sector_bar_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    stock_signal_overrides: Optional[Dict[str, Tuple[Dict[str, Any], float]]] = None,
    anchor_bar_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
    sectors_to_pick: int = SECTORS_TO_PICK,
    stocks_per_sector: int = STOCKS_PER_SECTOR,
    spot_proxy_fallback: bool = False,
) -> Optional[BreakfastSelection]:
    """Rank sectors and pick stocks — shared by backtest and live display."""
    resolved_nifty = nifty_bar or first_5m_bar(nifty_candles, session_date)
    if not resolved_nifty and upstox is not None:
        resolved_nifty = resolve_nifty_first_5m_bar(
            nifty_candles, session_date, upstox, instrument_key=NIFTY50_KEY
        )
    if not resolved_nifty:
        return None

    bias, bias_pct = nifty_bias_from_bar(resolved_nifty)
    long_side = bias == "positive"

    eligible = fo_eligible_sector_keys(
        stocks_by_sector, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    sector_bars: Dict[str, Dict[str, Any]] = {}
    overrides = sector_bar_overrides or {}
    for skey in eligible:
        bar = overrides.get(skey) or first_5m_bar(sector_candles.get(skey, []), session_date)
        if bar:
            sector_bars[skey] = bar

    ranked = rank_sectors(sector_bars, eligible_keys=eligible, descending=long_side)
    top_sectors = ranked[: max(1, int(sectors_to_pick))]
    if not top_sectors:
        return None

    stock_bars: Dict[str, Dict[str, Any]] = {}
    anchor_bars: Dict[str, Dict[str, Any]] = {}
    stock_move_pcts: Dict[str, float] = {}
    sym_to_candles: Dict[str, List[Dict[str, Any]]] = {}
    session_rows: Dict[str, StockRow] = {}
    signal_overrides = stock_signal_overrides or {}
    anchor_overrides = anchor_bar_overrides or {}

    for members in stocks_by_sector.values():
        for m in members:
            sym = str(m.get("stock") or "").upper()
            if sym in sym_to_candles:
                continue
            resolved = _resolve_session_stock_candles(
                sym,
                session_date,
                stock_candles_by_key=stock_candles_by_key,
                fut_by_und=fut_by_und,
                eq_by_symbol=eq_by_symbol,
                spot_proxy_fallback=spot_proxy_fallback,
            )
            if not resolved:
                continue
            candles, row_tpl = resolved
            sym_to_candles[sym] = candles
            session_rows[sym] = row_tpl
            if sym in signal_overrides:
                sig_bar, pct = signal_overrides[sym]
                stock_bars[sym] = sig_bar
                stock_move_pcts[sym] = pct
            else:
                setup = setup_bar_vs_prev_close(candles, session_date)
                if setup:
                    sig_bar, _prev, pct = setup
                    stock_bars[sym] = sig_bar
                    stock_move_pcts[sym] = pct
            ab = anchor_overrides.get(sym) or anchor_bar(candles, session_date)
            if ab:
                anchor_bars[sym] = ab

    sector_picks: List[BreakfastSectorPick] = []
    for s_rank, (skey, spct, svol) in enumerate(top_sectors, start=1):
        members = stocks_by_sector.get(skey, [])
        picks = pick_stocks_in_sector(
            members,
            stock_bars,
            stock_move_pcts,
            session_date=session_date,
            fut_by_und=fut_by_und,
            eq_by_symbol=eq_by_symbol,
            long_side=long_side,
            move_cap=STOCK_MOVE_CAP_PCT,
            top_n=stocks_per_sector,
            session_rows=session_rows if spot_proxy_fallback else None,
        )
        stock_picks: List[BreakfastStockPick] = []
        for st_rank, row in enumerate(picks, start=1):
            setup = stock_bars.get(row.stock)
            anchor = anchor_bars.get(row.stock)
            move_pct = stock_move_pcts.get(row.stock)
            if not setup or not anchor or move_pct is None:
                continue
            stock_picks.append(
                BreakfastStockPick(
                    row=row,
                    stock_rank=st_rank,
                    move_pct=float(move_pct),
                    signal_bar=setup,
                    anchor_bar=anchor,
                    candles=sym_to_candles.get(row.stock, []),
                )
            )
        sector_picks.append(
            BreakfastSectorPick(
                sector_key=skey,
                sector_rank=s_rank,
                sector_move_pct=float(spct),
                sector_volume=float(svol),
                stocks=stock_picks,
            )
        )

    return BreakfastSelection(
        nifty_bar=resolved_nifty,
        nifty_bias=bias,
        nifty_bias_pct=bias_pct,
        long_side=long_side,
        ranked_sectors=ranked,
        sector_picks=sector_picks,
        sym_to_candles=sym_to_candles,
        stock_bars=stock_bars,
        anchor_bars=anchor_bars,
        stock_move_pcts=stock_move_pcts,
        session_rows=session_rows,
    )


def _simulate_exit_long(
    candles: List[Dict[str, Any]],
    session_date: date,
    *,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    lot_size: int,
    pnl_cap_enabled: bool = False,
    monitor_from: Tuple[int, int] = (9, 25),
) -> Tuple[datetime, float, str, Optional[float]]:
    bars = session_5m_bars_after_entry(candles, session_date, from_hhmm=monitor_from, to_hhmm=TIME_EXIT)
    vwap_by_t = session_vwap_by_bar_time(candles, session_date)
    max_hi: Optional[float] = None
    cap_px: Optional[float] = None
    if pnl_cap_enabled and lot_size > 0:
        cap_px = entry_price + PNL_CAP_INR / lot_size
    for t, c in bars:
        o, h, _l, cl, _v = candle_ohlcv(c)
        max_hi = h if max_hi is None else max(max_hi, h)
        if h >= tp_price:
            return t, tp_price, "target_hit", max_hi
        if cap_px is not None and h >= cap_px:
            return t, cap_px, "pnl_cap", max_hi
        vwap = vwap_by_t.get(t)
        if vwap and vwap > 0 and cl < vwap:
            px = cl if cl > 0 else entry_price
            return t, px, "vwap_breach", max_hi
        if cl <= sl_price:
            return t, sl_price, "sl_hit", max_hi
        if (t.hour, t.minute) >= TIME_EXIT:
            px = cl if cl > 0 else entry_price
            return t, px, "time_exit", max_hi
    if bars:
        t, c = bars[-1]
        _, _, _, cl, _ = candle_ohlcv(c)
        px = cl if cl > 0 else entry_price
        return t, px, "time_exit", max_hi
    return ist_ts(session_date, *TIME_EXIT), entry_price, "data_gap", max_hi


def _simulate_exit_short(
    candles: List[Dict[str, Any]],
    session_date: date,
    *,
    entry_price: float,
    sl_price: float,
    tp_price: float,
    lot_size: int,
    pnl_cap_enabled: bool = False,
    monitor_from: Tuple[int, int] = (9, 25),
) -> Tuple[datetime, float, str, Optional[float]]:
    bars = session_5m_bars_after_entry(candles, session_date, from_hhmm=monitor_from, to_hhmm=TIME_EXIT)
    vwap_by_t = session_vwap_by_bar_time(candles, session_date)
    min_lo: Optional[float] = None
    cap_px: Optional[float] = None
    if pnl_cap_enabled and lot_size > 0:
        cap_px = entry_price - PNL_CAP_INR / lot_size
    for t, c in bars:
        o, _h, lo, cl, _v = candle_ohlcv(c)
        min_lo = lo if min_lo is None else min(min_lo, lo)
        if lo <= tp_price:
            return t, tp_price, "target_hit", min_lo
        if cap_px is not None and lo <= cap_px:
            return t, cap_px, "pnl_cap", min_lo
        vwap = vwap_by_t.get(t)
        if vwap and vwap > 0 and cl > vwap:
            px = cl if cl > 0 else entry_price
            return t, px, "vwap_breach", min_lo
        if cl >= sl_price:
            return t, sl_price, "sl_hit", min_lo
        if (t.hour, t.minute) >= TIME_EXIT:
            px = cl if cl > 0 else entry_price
            return t, px, "time_exit", min_lo
    if bars:
        t, c = bars[-1]
        _, _, _, cl, _ = candle_ohlcv(c)
        px = cl if cl > 0 else entry_price
        return t, px, "time_exit", min_lo
    return ist_ts(session_date, *TIME_EXIT), entry_price, "data_gap", min_lo


def _build_trade(
    *,
    session_date: date,
    row: StockRow,
    stock_move_pct: float,
    anchor_setup_bar: Dict[str, Any],
    signal_bar: Dict[str, Any],
    all_candles: List[Dict[str, Any]],
    long_side: bool,
    sector_rank: int,
    stock_rank: int,
    nifty_bias: str,
    nifty_bias_pct: Optional[float],
    nifty_bar: Optional[Dict[str, Any]],
    pnl_cap_enabled: bool = False,
) -> Optional[TradeResult]:
    a_o, a_h, a_lo, a_cl, a_vol = candle_ohlcv(anchor_setup_bar)
    s_o, s_h, s_lo, s_cl, s_vol = candle_ohlcv(signal_bar)
    if a_cl <= 0:
        return None
    move = stock_move_pct
    if move is None:
        return None

    anchor_price = a_cl
    lot = row.lot_size
    monitor_from = monitor_from_after_anchor(anchor_setup_bar)
    entry_dt = _bar_dt(anchor_setup_bar)
    entry_time = entry_dt.astimezone(IST) if entry_dt else ist_ts(session_date, 9, 20)
    if long_side:
        entry_price = a_cl * (1.0 + SLIPPAGE_PCT)
        sl_price = anchor_price * (1.0 - SL_PCT)
        tp_price = anchor_price * (1.0 + TP_PCT)
        exit_t, exit_px, exit_kind, pre_extreme = _simulate_exit_long(
            all_candles,
            session_date,
            entry_price=entry_price,
            sl_price=sl_price,
            tp_price=tp_price,
            lot_size=lot,
            pnl_cap_enabled=pnl_cap_enabled,
            monitor_from=monitor_from,
        )
        pnl_pts = exit_px - entry_price
    else:
        entry_price = a_cl * (1.0 - SLIPPAGE_PCT)
        sl_price = anchor_price * (1.0 + SL_PCT)
        tp_price = anchor_price * (1.0 - TP_PCT)
        exit_t, exit_px, exit_kind, pre_extreme = _simulate_exit_short(
            all_candles,
            session_date,
            entry_price=entry_price,
            sl_price=sl_price,
            tp_price=tp_price,
            lot_size=lot,
            pnl_cap_enabled=pnl_cap_enabled,
            monitor_from=monitor_from,
        )
        pnl_pts = entry_price - exit_px

    notes: List[str] = []
    if row.price_source == "spot_proxy":
        notes.append("spot_proxy; futures-equivalent lot sizing")

    n_o, n_c = None, None
    if nifty_bar:
        n_o, _, _, n_c, _ = candle_ohlcv(nifty_bar)

    return TradeResult(
        session_date=session_date,
        symbol=row.display_symbol,
        underlying_symbol=row.stock,
        instrument_label=row.instrument_label,
        direction="long" if long_side else "short",
        sector=row.sector,
        sector_index=row.sector_index,
        sector_rank=sector_rank,
        stock_rank=stock_rank,
        nifty_bias=nifty_bias,
        nifty_bias_pct=nifty_bias_pct,
        nifty_open_5m=n_o,
        nifty_close_5m=n_c,
        stock_move_pct_at_entry=float(move),
        setup_open_5m=s_o,
        setup_high_5m=s_h,
        setup_low_5m=s_lo,
        setup_close_5m=s_cl,
        setup_volume_5m=s_vol,
        instrument_key=row.instrument_key,
        lot_size=lot,
        price_source=row.price_source,
        entry_time=entry_time,
        entry_price=round(entry_price, 4),
        anchor_price=round(anchor_price, 4),
        sl_price=round(sl_price, 4),
        tp_price=round(tp_price, 4),
        pre_exit_extreme=round(pre_extreme, 4) if pre_extreme is not None else None,
        exit_time=exit_t,
        exit_price=round(exit_px, 4),
        exit_trigger_type=exit_kind,
        pnl_points=round(pnl_pts, 4),
        pnl_inr=round(pnl_pts * lot, 2),
        notes=notes,
    )


def _resolve_session_stock_candles(
    sym: str,
    session_date: date,
    *,
    stock_candles_by_key: Dict[str, List[Dict[str, Any]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    spot_proxy_fallback: bool,
) -> Optional[Tuple[List[Dict[str, Any]], StockRow]]:
    """Pick futures candles when available; optional spot fallback for OOS periods."""
    fut_ref = resolve_stock_instrument(sym, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
    if not fut_ref or not fut_ref.instrument_key:
        return None
    lot = int(fut_ref.fut_lot_size or fut_ref.lot_size or 0)
    if lot <= 0:
        return None

    fut_candles = stock_candles_by_key.get(fut_ref.instrument_key, [])
    if session_has_stock_bars(fut_candles, session_date):
        row = StockRow(
            stock=sym,
            display_symbol=display_symbol_for(sym, fut_ref),
            instrument_label=format_instrument_label(sym, fut_ref),
            sector="",
            sector_index="",
            instrument_key=str(fut_ref.instrument_key),
            lot_size=lot,
            price_source="futures",
        )
        return fut_candles, row

    if not spot_proxy_fallback:
        return None

    eq_ref = resolve_eq_spot_with_fut_lot(sym, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol)
    if not eq_ref or not eq_ref.instrument_key:
        return None
    eq_candles = stock_candles_by_key.get(eq_ref.instrument_key, [])
    if not session_has_stock_bars(eq_candles, session_date):
        return None
    row = StockRow(
        stock=sym,
        display_symbol=display_symbol_spot_proxy(sym),
        instrument_label="SPOT*",
        sector="",
        sector_index="",
        instrument_key=str(eq_ref.instrument_key),
        lot_size=lot,
        price_source="spot_proxy",
    )
    return eq_candles, row


def simulate_session_day(
    session_date: date,
    *,
    nifty_candles: List[Dict[str, Any]],
    sector_candles: Dict[str, List[Dict[str, Any]]],
    stock_candles_by_key: Dict[str, List[Dict[str, Any]]],
    stocks_by_sector: Dict[str, List[Dict[str, str]]],
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    upstox: Optional[Any] = None,
    pnl_cap_enabled: bool = False,
    spot_proxy_fallback: bool = False,
) -> List[TradeResult]:
    sel = select_breakfast_picks(
        session_date,
        nifty_candles=nifty_candles,
        sector_candles=sector_candles,
        stock_candles_by_key=stock_candles_by_key,
        stocks_by_sector=stocks_by_sector,
        fut_by_und=fut_by_und,
        eq_by_symbol=eq_by_symbol,
        upstox=upstox,
        spot_proxy_fallback=spot_proxy_fallback,
    )
    if not sel:
        return []

    trades: List[TradeResult] = []
    for sp in sel.sector_picks:
        for stk in sp.stocks:
            tr = _build_trade(
                session_date=session_date,
                row=stk.row,
                stock_move_pct=stk.move_pct,
                anchor_setup_bar=stk.anchor_bar,
                signal_bar=stk.signal_bar,
                all_candles=stk.candles,
                long_side=sel.long_side,
                sector_rank=sp.sector_rank,
                stock_rank=stk.stock_rank,
                nifty_bias=sel.nifty_bias,
                nifty_bias_pct=sel.nifty_bias_pct,
                nifty_bar=sel.nifty_bar,
                pnl_cap_enabled=pnl_cap_enabled,
            )
            if tr:
                trades.append(tr)
    return trades

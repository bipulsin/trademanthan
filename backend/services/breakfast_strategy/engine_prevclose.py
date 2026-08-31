"""Experimental Breakfast path: Nifty/sector rank vs prev session close.

Do not import from live.py / live_tick.py. Live lock and Primary/History
ranking stay on engine.nifty_bias_from_bar + universe.rank_sectors.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple

from backend.services.breakfast_strategy.candles import (
    anchor_bar,
    bar_volume,
    candle_ohlcv,
    first_5m_bar,
    move_pct_vs_prev_close,
    prev_session_close,
    resolve_nifty_first_5m_bar,
    setup_bar_vs_prev_close,
)
from backend.services.breakfast_strategy.config import (
    PREVCLOSE_SECTORS_TO_PICK,
    STOCK_MOVE_CAP_PCT,
    STOCKS_PER_SECTOR,
)
from backend.services.breakfast_strategy.engine import (
    NIFTY50_KEY,
    BreakfastSectorPick,
    BreakfastSelection,
    BreakfastStockPick,
    TradeResult,
    _build_trade,
    _resolve_session_stock_candles,
)
from backend.services.breakfast_strategy.universe import (
    SECTOR_UNIVERSE,
    StockRow,
    fo_eligible_sector_keys,
    pick_stocks_in_sector,
    sector_index_key_for_label,
)


def nifty_bias_from_bar_vs_prev_close(
    bar: Dict[str, Any],
    prev_close: Optional[float],
) -> Tuple[str, float]:
    """NIFTY opening 5m close vs previous session close; flat (0%) → long branch."""
    if prev_close is None or prev_close <= 0:
        return "positive", 0.0
    _, _, _, cl, _ = candle_ohlcv(bar)
    pct = move_pct_vs_prev_close(float(cl), float(prev_close))
    if pct is None:
        return "positive", 0.0
    if pct < 0:
        return "negative", float(pct)
    return "positive", float(pct)


def rank_sectors_vs_prev_close(
    sector_bars: Dict[str, Dict[str, Any]],
    prev_closes: Dict[str, float],
    *,
    eligible_keys: Set[str],
    descending: bool,
) -> List[Tuple[str, float, float]]:
    rows: List[Tuple[str, float, float]] = []
    for label, _yahoo in SECTOR_UNIVERSE:
        ikey = sector_index_key_for_label(label)
        if not ikey or ikey not in eligible_keys:
            continue
        bar = sector_bars.get(ikey)
        if not bar:
            continue
        prev = prev_closes.get(ikey)
        if prev is None or prev <= 0:
            continue
        _, _, _, cl, _ = candle_ohlcv(bar)
        pct = move_pct_vs_prev_close(float(cl), float(prev))
        if pct is None:
            continue
        rows.append((ikey, float(pct), float(bar_volume(bar))))
    rows.sort(key=lambda x: (-x[1], -x[2]) if descending else (x[1], -x[2]))
    return rows


def select_breakfast_picks_prevclose(
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
    sectors_to_pick: int = PREVCLOSE_SECTORS_TO_PICK,
    stocks_per_sector: int = STOCKS_PER_SECTOR,
    spot_proxy_fallback: bool = False,
) -> Optional[BreakfastSelection]:
    """Same stock picker/exits as engine.select_breakfast_picks; Nifty/sectors vs prev close."""
    resolved_nifty = nifty_bar or first_5m_bar(nifty_candles, session_date)
    if not resolved_nifty and upstox is not None:
        resolved_nifty = resolve_nifty_first_5m_bar(
            nifty_candles, session_date, upstox, instrument_key=NIFTY50_KEY
        )
    if not resolved_nifty:
        return None

    nifty_prev = prev_session_close(nifty_candles, session_date)
    bias, bias_pct = nifty_bias_from_bar_vs_prev_close(resolved_nifty, nifty_prev)
    long_side = bias == "positive"

    eligible = fo_eligible_sector_keys(
        stocks_by_sector, session_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    sector_bars: Dict[str, Dict[str, Any]] = {}
    sector_prev: Dict[str, float] = {}
    for skey in eligible:
        bar = first_5m_bar(sector_candles.get(skey, []), session_date)
        if bar:
            sector_bars[skey] = bar
        prev = prev_session_close(sector_candles.get(skey, []), session_date)
        if prev is not None:
            sector_prev[skey] = prev

    ranked = rank_sectors_vs_prev_close(
        sector_bars, sector_prev, eligible_keys=eligible, descending=long_side
    )
    top_sectors = ranked[: max(1, int(sectors_to_pick))]
    if not top_sectors:
        return None

    stock_bars: Dict[str, Dict[str, Any]] = {}
    anchor_bars: Dict[str, Dict[str, Any]] = {}
    stock_move_pcts: Dict[str, float] = {}
    sym_to_candles: Dict[str, List[Dict[str, Any]]] = {}
    session_rows: Dict[str, StockRow] = {}

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
            setup = setup_bar_vs_prev_close(candles, session_date)
            if setup:
                sig_bar, _prev, pct = setup
                stock_bars[sym] = sig_bar
                stock_move_pcts[sym] = pct
            ab = anchor_bar(candles, session_date)
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


def simulate_session_day_prevclose(
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
    sectors_to_pick: int = PREVCLOSE_SECTORS_TO_PICK,
    stocks_per_sector: int = STOCKS_PER_SECTOR,
) -> List[TradeResult]:
    sel = select_breakfast_picks_prevclose(
        session_date,
        nifty_candles=nifty_candles,
        sector_candles=sector_candles,
        stock_candles_by_key=stock_candles_by_key,
        stocks_by_sector=stocks_by_sector,
        fut_by_und=fut_by_und,
        eq_by_symbol=eq_by_symbol,
        upstox=upstox,
        sectors_to_pick=sectors_to_pick,
        stocks_per_sector=stocks_per_sector,
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

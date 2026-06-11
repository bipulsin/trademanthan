"""Fresh market snapshots for Volume Mismatch — refresh stale arbitrage_master rows."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

from backend.services.market_data.indicators import indicators_from_5m_candles
from backend.services.market_data.reads import get_row_market_snapshot, is_market_data_fresh

logger = logging.getLogger(__name__)


def symbols_with_stale_market_data(symbols: Sequence[str]) -> List[str]:
    """Underlyings whose arbitrage_master snapshot is missing or older than threshold."""
    stale: List[str] = []
    for sym in symbols:
        s = str(sym or "").strip().upper()
        if not s:
            continue
        snap = get_row_market_snapshot(s)
        if not snap:
            stale.append(s)
            continue
        lu = snap.get("market_data_last_updated") or snap.get("currmth_future_last_updated")
        if not is_market_data_fresh(lu):
            stale.append(s)
    return stale


def refresh_stale_arbitrage_master(symbols: Sequence[str]) -> int:
    """
    Overwrite stale arbitrage_master LTP/VWAP/EMA for ``symbols``.

    Returns count of rows updated (0 if none stale or refresh failed).
    """
    stale = symbols_with_stale_market_data(symbols)
    if not stale:
        return 0
    try:
        from backend.services.market_data.engine import refresh_arbitrage_master_market_data
        from backend.services.market_data.reads import invalidate_read_cache

        out = refresh_arbitrage_master_market_data(
            execution="volume_mismatch_fresh",
            fetch_candles=True,
            stocks=stale,
        )
        invalidate_read_cache()
        written = int(out.get("rows_updated") or 0)
        logger.info("VM fresh market refresh: stale=%s updated=%s", len(stale), written)
        return written
    except Exception as e:
        logger.warning("VM arbitrage_master refresh failed: %s", e)
        return 0


def fut_indicators_from_candles(
    candles_5m: Sequence[Dict[str, Any]],
) -> Optional[Dict[str, float]]:
    """Session VWAP, EMA(5), and last bar close from live 5m candles."""
    return indicators_from_5m_candles(list(candles_5m or []))


def resolve_fut_price_and_indicators(
    *,
    instrument_key: str,
    symbol: str,
    ltp_map: Dict[str, float],
    candles_5m: Sequence[Dict[str, Any]],
) -> Dict[str, Optional[float]]:
    """
    Prefer freshly fetched 5m candles + LTP; fall back to arbitrage_master after refresh.
    """
    ik = str(instrument_key or "").strip()
    sym = str(symbol or "").strip().upper()
    ind = fut_indicators_from_candles(candles_5m)

    price: Optional[float] = None
    if ik:
        px = ltp_map.get(ik)
        if px is not None and px > 0:
            price = float(px)
    if price is None and ind:
        close = ind.get("candle_close")
        if close is not None and close > 0:
            price = float(close)

    vwap: Optional[float] = None
    ema5: Optional[float] = None
    if ind:
        vwap = ind.get("vwap")
        ema5 = ind.get("ema5")

    if vwap is None or ema5 is None or price is None:
        snap = get_row_market_snapshot(sym)
        if snap:
            if price is None:
                try:
                    price = float(snap.get("currmth_future_ltp") or 0) or None
                except (TypeError, ValueError):
                    price = None
            if vwap is None:
                try:
                    vwap = float(snap.get("currmth_future_vwap") or 0) or None
                except (TypeError, ValueError):
                    vwap = None
            if ema5 is None:
                try:
                    ema5 = float(snap.get("currmth_future_ema5") or 0) or None
                except (TypeError, ValueError):
                    ema5 = None

    return {"price": price, "vwap": vwap, "ema5": ema5}

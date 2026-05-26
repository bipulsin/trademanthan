"""5m-bar VWAP and EMA(5) from OHLCV lists."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.vajra.indicators import cumulative_vwap, ema_series


def indicators_from_5m_candles(candles: Sequence[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    """Latest session VWAP (cumulative), EMA(5) on closes, and last bar OHLCV."""
    if not candles or len(candles) < 2:
        return None
    highs = [float(c.get("high") or 0) for c in candles]
    lows = [float(c.get("low") or 0) for c in candles]
    closes = [float(c.get("close") or 0) for c in candles]
    volumes = [float(c.get("volume") or 0) for c in candles]
    opens = [float(c.get("open") or closes[i]) for i, c in enumerate(candles)]
    if not closes or closes[-1] <= 0:
        return None

    vwap_s = cumulative_vwap(highs, lows, closes, volumes)
    ema5 = ema_series(closes, 5)
    i = len(closes) - 1
    return {
        "vwap": round(float(vwap_s[i]), 4),
        "ema5": round(float(ema5[i]), 4) if ema5 else round(closes[i], 4),
        "candle_open": round(opens[i], 4),
        "candle_high": round(highs[i], 4),
        "candle_low": round(lows[i], 4),
        "candle_close": round(closes[i], 4),
        "candle_volume": round(volumes[i], 2),
    }


def parse_candle_list(raw: Any) -> List[Dict[str, Any]]:
    if not raw:
        return []
    if isinstance(raw, list) and raw and isinstance(raw[0], dict):
        return list(raw)
    return []

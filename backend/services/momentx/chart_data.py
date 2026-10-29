"""OHLCV + EMA/RSI/Williams series for MomentX chart modal."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from backend.services.momentx.candles import fetch_candles, splits_ohlcv
from backend.services.momentx.indicators import ema_series, wilder_rsi, williams_r

logger = logging.getLogger(__name__)


def _bar_ts(c: Dict[str, Any]) -> Optional[int]:
    from backend.services.momentx.candles import _parse_ts

    ts = _parse_ts(c.get("timestamp"))
    if ts is None:
        return None
    return int(ts.timestamp())


def build_chart_payload(
    *,
    instrument_key: str,
    interval: str = "minutes/5",
    days_back: int = 10,
) -> Dict[str, Any]:
    ik = (instrument_key or "").strip()
    if not ik:
        return {"ok": False, "error": "instrument_key required", "bars": []}
    try:
        raw = fetch_candles(ik, interval, days_back)
    except Exception as e:
        logger.exception("momentx chart fetch failed")
        return {"ok": False, "error": str(e)[:200], "bars": []}
    if not raw:
        return {"ok": True, "bars": [], "instrument_key": ik}

    opens, highs, lows, closes, vols = splits_ohlcv(raw)
    e5 = ema_series(closes, 5)
    e15 = ema_series(closes, 15)
    e50 = ema_series(closes, 50)

    bars: List[Dict[str, Any]] = []
    for i, c in enumerate(raw):
        t = _bar_ts(c)
        if t is None or i >= len(closes):
            continue
        # Rolling RSI / Williams ending at i (need history)
        rsi_v = wilder_rsi(closes[: i + 1], 14) if i + 1 >= 16 else None
        wr_v = (
            williams_r(highs[: i + 1], lows[: i + 1], closes[: i + 1], 14)
            if i + 1 >= 14
            else None
        )
        bars.append(
            {
                "time": t,
                "open": float(opens[i]) if i < len(opens) else None,
                "high": float(highs[i]) if i < len(highs) else None,
                "low": float(lows[i]) if i < len(lows) else None,
                "close": float(closes[i]),
                "volume": float(vols[i]) if i < len(vols) else 0,
                "ema5": e5[i] if i < len(e5) else None,
                "ema15": e15[i] if i < len(e15) else None,
                "ema50": e50[i] if i < len(e50) else None,
                "rsi": rsi_v,
                "williams_r": wr_v,
            }
        )
    return {
        "ok": True,
        "instrument_key": ik,
        "interval": interval,
        "bars": bars,
    }

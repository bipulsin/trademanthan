"""Volume mismatch detection + trade level calculation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from backend.services.volume_mismatch.constants import (
    DEFAULT_GAP_THRESHOLD_PCT,
    PREFERRED_ENTRY_BUFFER_PCT,
)
from backend.services.volume_mismatch.scoring import total_score


@dataclass
class MismatchSignal:
    symbol: str
    future_symbol: str
    instrument_key: str
    direction: str  # LONG | SHORT
    gap_percent: float
    first_15m_volume: float
    relative_volume: Optional[float]
    net_volume: float
    score: float
    entry_price: float
    preferred_entry: float
    stop_loss: float
    target1: float
    target2: float
    first_15m_high: float
    first_15m_low: float
    first_15m_open: float
    first_15m_close: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "future_symbol": self.future_symbol,
            "instrument_key": self.instrument_key,
            "direction": self.direction,
            "gap_percent": self.gap_percent,
            "first_15m_volume": self.first_15m_volume,
            "relative_volume": self.relative_volume,
            "net_volume": self.net_volume,
            "score": self.score,
            "entry_price": self.entry_price,
            "preferred_entry": self.preferred_entry,
            "stop_loss": self.stop_loss,
            "target1": self.target1,
            "target2": self.target2,
            "first_15m_high": self.first_15m_high,
            "first_15m_low": self.first_15m_low,
            "first_15m_open": self.first_15m_open,
            "first_15m_close": self.first_15m_close,
            "entry_status": "WAITING",
        }


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def compute_net_volume(first_bar: Dict[str, Any], previous_close: float) -> float:
    """
    Signed volume for mismatch detection.

    Primary rule (spec): close vs previous day close.
    When the first 15m bar has range, use close position in the bar
    (upper half → buying pressure, lower half → selling) so gap-down red
    absorption / gap-up green distribution can qualify.
    """
    close = _f(first_bar.get("close"))
    low = _f(first_bar.get("low"))
    high = _f(first_bar.get("high"))
    vol = _f(first_bar.get("volume"))
    if vol <= 0:
        return 0.0
    if high > low:
        pos = (close - low) / (high - low)
        return vol * (2.0 * pos - 1.0)
    if close > previous_close:
        return vol
    if close < previous_close:
        return -vol
    return 0.0


def compute_gap_percent(today_open: float, previous_close: float) -> Optional[float]:
    if previous_close <= 0 or today_open <= 0:
        return None
    return ((today_open - previous_close) / previous_close) * 100.0


def _trade_levels_long(high: float, low: float) -> Dict[str, float]:
    entry = high
    preferred = high * (1.0 + PREFERRED_ENTRY_BUFFER_PCT / 100.0)
    sl = low
    risk = preferred - sl
    if risk <= 0:
        risk = max(preferred * 0.002, 0.01)
    return {
        "entry_price": round(entry, 4),
        "preferred_entry": round(preferred, 4),
        "stop_loss": round(sl, 4),
        "target1": round(preferred + risk, 4),
        "target2": round(preferred + 2 * risk, 4),
    }


def _trade_levels_short(high: float, low: float) -> Dict[str, float]:
    entry = low
    preferred = low * (1.0 - PREFERRED_ENTRY_BUFFER_PCT / 100.0)
    sl = high
    risk = sl - preferred
    if risk <= 0:
        risk = max(preferred * 0.002, 0.01)
    return {
        "entry_price": round(entry, 4),
        "preferred_entry": round(preferred, 4),
        "stop_loss": round(sl, 4),
        "target1": round(preferred - risk, 4),
        "target2": round(preferred - 2 * risk, 4),
    }


def evaluate_mismatch(
    *,
    symbol: str,
    future_symbol: str,
    instrument_key: str,
    first_bar: Dict[str, Any],
    previous_close: float,
    relative_volume: Optional[float],
    gap_threshold: float = DEFAULT_GAP_THRESHOLD_PCT,
) -> Optional[MismatchSignal]:
    o = _f(first_bar.get("open"))
    h = _f(first_bar.get("high"))
    l = _f(first_bar.get("low"))
    c = _f(first_bar.get("close"))
    vol = _f(first_bar.get("volume"))
    if o <= 0 or h <= 0 or l <= 0 or c <= 0 or previous_close <= 0:
        return None

    gap = compute_gap_percent(o, previous_close)
    if gap is None:
        return None

    net_vol = compute_net_volume(first_bar, previous_close)

    # Bullish mismatch -> LONG
    if gap <= -gap_threshold and c < o and net_vol > 0:
        direction = "LONG"
        levels = _trade_levels_long(h, l)
    # Bearish mismatch -> SHORT
    elif gap >= gap_threshold and c > o and net_vol < 0:
        direction = "SHORT"
        levels = _trade_levels_short(h, l)
    else:
        return None

    score = total_score(
        gap,
        net_vol,
        vol,
        relative_volume,
        o,
        h,
        l,
        gap_threshold=gap_threshold,
    )

    return MismatchSignal(
        symbol=symbol,
        future_symbol=future_symbol,
        instrument_key=instrument_key,
        direction=direction,
        gap_percent=round(gap, 4),
        first_15m_volume=round(vol, 2),
        relative_volume=round(relative_volume, 4) if relative_volume is not None else None,
        net_volume=round(net_vol, 2),
        score=score,
        first_15m_high=h,
        first_15m_low=l,
        first_15m_open=o,
        first_15m_close=c,
        **levels,
    )

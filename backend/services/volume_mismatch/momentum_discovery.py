"""Intraday momentum discovery for symbols missed by the 09:30 gap/BB scan."""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set

import pytz

from backend.config import settings
from backend.services.market_data.reads import ltp_map_with_fallback
from backend.services.upstox_service import UpstoxService
from backend.services.volume_mismatch.candles import (
    batch_fetch_candles,
    clear_candle_cache,
    first_15m_bar_for_session,
    last_two_completed_5m_bars,
    previous_day_close,
)
from backend.services.volume_mismatch.constants import (
    MOMENTUM_DISCOVERY_END_MINUTES,
    MOMENTUM_DISCOVERY_MIN_REL_VOL,
    MOMENTUM_DISCOVERY_START_MINUTES,
)
from backend.services.volume_mismatch.fresh_market import resolve_fut_price_and_indicators
from backend.services.volume_mismatch.repository import fetch_signals_for_date, upsert_signal
from backend.services.volume_mismatch.session_trend import assess_session_trend
from backend.services.volume_mismatch.signal_engine import trade_levels_for_direction
from backend.services.volume_mismatch.signal_rules import (
    bollinger_bands_as_of_session,
    compute_gap_percent,
    compute_net_volume,
    compute_relative_volume,
)
from backend.services.volume_mismatch.universe import load_volume_mismatch_universe

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def _ist_minutes(now: datetime) -> int:
    t = now.astimezone(IST) if now.tzinfo else IST.localize(now)
    return t.hour * 60 + t.minute


def is_momentum_discovery_window(now: Optional[datetime] = None) -> bool:
    t = now or datetime.now(IST)
    if t.weekday() >= 5:
        return False
    m = _ist_minutes(t)
    return MOMENTUM_DISCOVERY_START_MINUTES <= m <= MOMENTUM_DISCOVERY_END_MINUTES


def _discovered_payload(
    u: Dict[str, Any],
    *,
    trade_date: date,
    first_bar: Dict[str, Any],
    prev_close: float,
    bb: Dict[str, float],
    rel_vol: Optional[float],
    price: float,
    vwap: float,
    entry_status: str,
) -> Dict[str, Any]:
    o = float(first_bar.get("open") or 0)
    h = float(first_bar.get("high") or 0)
    l = float(first_bar.get("low") or 0)
    c = float(first_bar.get("close") or 0)
    gap = compute_gap_percent(o, prev_close) or 0.0
    net_vol = compute_net_volume(first_bar, prev_close)
    levels = trade_levels_for_direction("LONG", h, l)
    from backend.services.volume_mismatch.scoring import total_score

    vol = float(first_bar.get("volume") or 0)
    score = total_score(gap, net_vol, vol, rel_vol, o, h, l)
    return {
        "symbol": u["symbol"],
        "future_symbol": u.get("future_symbol") or u["symbol"],
        "instrument_key": u["instrument_key"],
        "direction": "LONG",
        "gap_percent": round(gap, 4),
        "first_15m_volume": round(vol, 2),
        "relative_volume": round(rel_vol, 4) if rel_vol is not None else None,
        "net_volume": round(net_vol, 2),
        "score": score,
        "first_15m_high": round(h, 4),
        "first_15m_low": round(l, 4),
        "first_15m_open": round(o, 4),
        "first_15m_close": round(c, 4),
        "bb_upper": bb.get("bb_upper"),
        "bb_middle": bb.get("bb_middle"),
        "bb_lower": bb.get("bb_lower"),
        "current_price": round(price, 4),
        "vwap": round(vwap, 4),
        "entry_status": entry_status,
        "signal_path": "momentum_intraday",
        "trade_date": trade_date.isoformat(),
        **levels,
    }


def discover_intraday_momentum_signals(
    db,
    trade_date: date,
    *,
    now: Optional[datetime] = None,
    max_new: int = 12,
) -> List[Dict[str, Any]]:
    """
    Scan universe symbols not yet in ``volume_mismatch_signals`` for live bull momentum:
    price > VWAP + EMA5, breakout above first-15m high, rising 5m volume, bullish structure.
    """
    now = now or datetime.now(IST)
    if not is_momentum_discovery_window(now):
        return []

    existing: Set[str] = {
        str(r.get("symbol") or "").strip().upper()
        for r in fetch_signals_for_date(db, trade_date)
    }
    candidates = [u for u in load_volume_mismatch_universe() if u["symbol"] not in existing]
    if not candidates:
        return []

    keys = [u["instrument_key"] for u in candidates]
    clear_candle_cache()
    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    candles_5m = batch_fetch_candles(upstox, keys, "minutes/5", days_back=2)
    candles_15m = batch_fetch_candles(upstox, keys, "minutes/15", days_back=35, range_end_date=trade_date)
    candles_1d = batch_fetch_candles(upstox, keys, "days/1", days_back=45, range_end_date=trade_date)
    ltp_map = ltp_map_with_fallback(keys, allow_broker_fallback=True, allow_stale=False)

    discovered: List[Dict[str, Any]] = []
    for u in candidates:
        if len(discovered) >= max_new:
            break
        ik = u["instrument_key"]
        sym = u["symbol"]
        bars_5 = candles_5m.get(ik) or []
        bars_15 = candles_15m.get(ik) or []
        bars_1d = candles_1d.get(ik) or []
        first_bar = first_15m_bar_for_session(bars_15, trade_date)
        prev_close = previous_day_close(bars_1d, trade_date)
        if not first_bar or not prev_close or prev_close <= 0:
            continue

        first_high = float(first_bar.get("high") or 0)
        if first_high <= 0:
            continue

        resolved = resolve_fut_price_and_indicators(
            instrument_key=ik,
            symbol=sym,
            ltp_map=ltp_map,
            candles_5m=bars_5,
        )
        price = resolved.get("price")
        vwap = resolved.get("vwap")
        ema5 = resolved.get("ema5")
        if not price or not vwap or not ema5:
            continue
        if price <= vwap or price <= ema5:
            continue
        if price < first_high * 0.999:
            continue

        cur_5m, prev_5m = last_two_completed_5m_bars(bars_5, now=now)
        cur_vol = float((cur_5m or {}).get("volume") or 0)
        prev_vol = float((prev_5m or {}).get("volume") or 0)
        if prev_vol > 0 and cur_vol <= prev_vol:
            continue

        trend = assess_session_trend(price, vwap, ema5, cur_5m or {}, prev_5m or {})
        if trend != "BULLISH":
            continue

        rel_vol = compute_relative_volume(first_bar, bars_15, trade_date)
        if rel_vol is not None and rel_vol < MOMENTUM_DISCOVERY_MIN_REL_VOL:
            continue

        bb = bollinger_bands_as_of_session(bars_1d, trade_date)
        if not bb:
            continue

        entry_status = "READY" if price > first_high and price > vwap else "WAITING"
        payload = _discovered_payload(
            u,
            trade_date=trade_date,
            first_bar=first_bar,
            prev_close=prev_close,
            bb=bb,
            rel_vol=rel_vol,
            price=float(price),
            vwap=float(vwap),
            entry_status=entry_status,
        )
        upsert_signal(db, trade_date, payload)
        discovered.append(payload)
        logger.info(
            "VM momentum discovery %s %s: price=%.2f vwap=%.2f first_high=%.2f rel_vol=%s status=%s",
            trade_date,
            sym,
            price,
            vwap,
            first_high,
            rel_vol,
            entry_status,
        )
    return discovered

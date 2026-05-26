"""
Read centralized market data from arbitrage_master.

Algos use these helpers instead of direct Upstox LTP calls for shared fields.
Falls back to broker API only when ``allow_broker_fallback=True`` and data is stale/missing.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz

from backend.services.market_data.constants import (
    DATA_SOURCE_DB,
    DEFAULT_LTP_MAX_AGE_SEC,
)
from backend.services.market_data.repository import load_key_index, load_row_by_stock

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_key_index_cache: Optional[Dict[str, Dict[str, Any]]] = None
_key_index_ts: Optional[datetime] = None
_CACHE_TTL_SEC = 15


def _now_ist() -> datetime:
    return datetime.now(IST)


def _parse_ts(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(IST) if ts.tzinfo else IST.localize(ts)
    try:
        s = str(ts).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
    except Exception:
        return None


def is_market_data_fresh(
    updated_at: Any,
    *,
    max_age_sec: int = DEFAULT_LTP_MAX_AGE_SEC,
) -> bool:
    ts = _parse_ts(updated_at)
    if not ts:
        return False
    age = (_now_ist() - ts).total_seconds()
    return age <= max_age_sec


def _get_key_index() -> Dict[str, Dict[str, Any]]:
    global _key_index_cache, _key_index_ts
    now = _now_ist()
    if (
        _key_index_cache is not None
        and _key_index_ts is not None
        and (now - _key_index_ts).total_seconds() < _CACHE_TTL_SEC
    ):
        return _key_index_cache
    try:
        _key_index_cache = load_key_index()
    except Exception as e:
        logger.warning("market_data key index load failed: %s", e)
        _key_index_cache = {}
    _key_index_ts = now
    return _key_index_cache


def invalidate_read_cache() -> None:
    global _key_index_cache, _key_index_ts
    _key_index_cache = None
    _key_index_ts = None


def get_ltp_for_instrument_key(
    instrument_key: str,
    *,
    max_age_sec: int = DEFAULT_LTP_MAX_AGE_SEC,
    allow_stale: bool = True,
) -> Optional[float]:
    ik = (instrument_key or "").strip()
    if not ik:
        return None
    hit = _get_key_index().get(ik)
    if not hit:
        return None
    ltp = hit.get("ltp")
    if ltp is None:
        return None
    try:
        px = float(ltp)
    except (TypeError, ValueError):
        return None
    if px <= 0:
        return None
    fresh = is_market_data_fresh(hit.get("updated"), max_age_sec=max_age_sec)
    if fresh or allow_stale:
        return round(px, 4)
    return None


def get_ltps_for_instrument_keys(
    instrument_keys: List[str],
    *,
    max_age_sec: int = DEFAULT_LTP_MAX_AGE_SEC,
    allow_stale: bool = True,
) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for ik in instrument_keys or []:
        px = get_ltp_for_instrument_key(ik, max_age_sec=max_age_sec, allow_stale=allow_stale)
        if px is not None:
            out[ik] = px
    return out


def get_row_market_snapshot(stock: str) -> Optional[Dict[str, Any]]:
    row = load_row_by_stock(stock)
    if not row:
        return None
    return {
        "stock": row.get("stock"),
        "stock_ltp": row.get("stock_ltp"),
        "stock_vwap": row.get("stock_vwap"),
        "stock_ema5": row.get("stock_ema5"),
        "currmth_future_ltp": row.get("currmth_future_ltp"),
        "currmth_future_vwap": row.get("currmth_future_vwap"),
        "currmth_future_ema5": row.get("currmth_future_ema5"),
        "nextmth_future_ltp": row.get("nextmth_future_ltp"),
        "nextmth_future_vwap": row.get("nextmth_future_vwap"),
        "nextmth_future_ema5": row.get("nextmth_future_ema5"),
        "market_data_last_updated": row.get("market_data_last_updated"),
        "market_data_refresh_status": row.get("market_data_refresh_status"),
        "source": DATA_SOURCE_DB,
    }


def _broker_ltp_fallback(keys: List[str]) -> Dict[str, float]:
    if not keys:
        return {}
    try:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        if not getattr(u, "access_token", None):
            return {}
        return u.get_market_quotes_batch_by_keys(list(dict.fromkeys(keys))) or {}
    except Exception as e:
        logger.debug("market_data broker fallback: %s", e)
        return {}


def ltp_map_with_fallback(
    instrument_keys: List[str],
    *,
    max_age_sec: int = DEFAULT_LTP_MAX_AGE_SEC,
    allow_broker_fallback: bool = True,
    allow_stale: bool = True,
) -> Dict[str, float]:
    """
    Build instrument_key -> LTP map from DB first, optional Upstox batch for gaps.
    """
    keys = [str(k).strip() for k in (instrument_keys or []) if str(k).strip()]
    out: Dict[str, float] = get_ltps_for_instrument_keys(
        keys, max_age_sec=max_age_sec, allow_stale=allow_stale
    )
    if not allow_broker_fallback:
        return out
    missing = [k for k in keys if k not in out]
    if not missing:
        return out
    fb = _broker_ltp_fallback(missing)
    for k, v in fb.items():
        try:
            fv = float(v)
            if fv > 0 and k not in out:
                out[k] = round(fv, 4)
        except (TypeError, ValueError):
            continue
    return out

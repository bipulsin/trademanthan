"""
NSE F&O open-interest fetch (session cookie) + interpretation for Smart Futures gate.
Self-contained; failures never raise into the main loop.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests

from backend.services.smart_futures_config import (
    OI_FETCH_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nseindia.com"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "application/json",
    "Referer": f"{BASE_URL}/",
}


@dataclass
class OIQuote:
    symbol: str
    oi: int
    change_in_oi: int
    last_price: float
    prev_close: float
    prev_oi: int
    fetched_at: float


# symbol -> (unix_ts, OIQuote)
_oi_cache: Dict[str, Tuple[float, OIQuote]] = {}


def interpret_oi_signal(price_change: float, oi_change: float) -> str:
    """Four-state OI / price interpretation."""
    if price_change > 0 and oi_change > 0:
        return "LONG_BUILDUP"
    if price_change < 0 and oi_change > 0:
        return "SHORT_BUILDUP"
    if price_change < 0 and oi_change < 0:
        return "LONG_UNWINDING"
    if price_change > 0 and oi_change < 0:
        return "SHORT_COVERING"
    return "NEUTRAL"


class NSEOIFetcher:
    BASE_URL = BASE_URL
    HEADERS = DEFAULT_HEADERS

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._init_session()

    def _init_session(self) -> None:
        try:
            self.session.get(self.BASE_URL, timeout=10)
        except Exception as e:
            logger.warning("oi_integration: session init failed: %s", e)

    def get_oi(self, symbol: str) -> Dict[str, Any]:
        sym = (symbol or "").strip().upper()
        url = f"{self.BASE_URL}/api/quote-derivative?symbol={sym}"
        response = self.session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return self._parse_oi(data, sym)

    def _parse_oi(self, data: dict, symbol: str) -> Dict[str, Any]:
        """
        Return normalized dict for near-month futures row.
        NSE JSON shape varies; we scan common containers.
        """
        rows: list = []
        if isinstance(data.get("stocks"), list):
            rows = data["stocks"]
        fd = data.get("filtered") or {}
        if isinstance(fd, dict) and isinstance(fd.get("data"), list):
            rows = fd["data"]

        best: Optional[dict] = None
        for row in rows:
            if not isinstance(row, dict):
                continue
            inst = (row.get("instrumentType") or row.get("instrument_type") or "").upper()
            if "FUT" in inst or "FUTIDX" in inst or inst == "FUTSTK":
                if best is None:
                    best = row
                # Prefer current month / near expiry if expiry field exists
                exp = str(row.get("expiryDate") or row.get("expiry") or "")
                bexp = str(best.get("expiryDate") or best.get("expiry") or "")
                if exp and bexp and exp < bexp:
                    best = row

        if best is None and rows:
            best = rows[0] if isinstance(rows[0], dict) else None

        if best is None:
            return {
                "symbol": symbol,
                "oi": 0,
                "change_in_oi": 0,
                "last_price": 0.0,
                "prev_close": 0.0,
                "prev_oi": 0,
            }

        def _i(x: Any) -> int:
            try:
                return int(float(x))
            except (TypeError, ValueError):
                return 0

        def _f(x: Any) -> float:
            try:
                return float(x)
            except (TypeError, ValueError):
                return 0.0

        oi = _i(best.get("openInterest") or best.get("oi") or best.get("open_interest"))
        chg = _i(best.get("changeinOpenInterest") or best.get("chg_oi") or best.get("change_in_oi"))
        lp = _f(best.get("lastPrice") or best.get("last_price") or best.get("ltp"))
        pc = _f(best.get("previousClose") or best.get("prev_close") or best.get("closePrice"))
        prev_oi = max(0, oi - chg)

        return {
            "symbol": symbol,
            "oi": oi,
            "change_in_oi": chg,
            "last_price": lp,
            "prev_close": pc,
            "prev_oi": prev_oi,
        }


def get_cached_oi_quote(symbol: str, fetcher: Optional[NSEOIFetcher] = None) -> Optional[OIQuote]:
    """
    Return cached OIQuote if fresh enough; else fetch and cache.
    """
    sym = (symbol or "").strip().upper()
    if not sym:
        return None
    now = time.time()
    if sym in _oi_cache:
        ts, q = _oi_cache[sym]
        if now - ts <= OI_FETCH_INTERVAL_SECONDS:
            return q

    if fetcher is None:
        fetcher = NSEOIFetcher()
    try:
        raw = fetcher.get_oi(sym)
        oi = int(raw.get("oi") or 0)
        chg = int(raw.get("change_in_oi") or 0)
        lp = float(raw.get("last_price") or 0.0)
        pc = float(raw.get("prev_close") or 0.0)
        prev_oi = int(raw.get("prev_oi") or max(0, oi - chg))
        q = OIQuote(
            symbol=sym,
            oi=oi,
            change_in_oi=chg,
            last_price=lp,
            prev_close=pc,
            prev_oi=prev_oi,
            fetched_at=now,
        )
        _oi_cache[sym] = (now, q)
        return q
    except Exception as e:
        logger.warning("oi_integration: fetch failed for %s: %s", sym, e)
        return None


def oi_signal_from_quote(q: OIQuote) -> str:
    dp = q.last_price - q.prev_close
    return interpret_oi_signal(float(dp), float(q.change_in_oi))


def normalized_oi_score_for_side(q: OIQuote, side: str) -> float:
    """Optional CMS feature: directional OI change clipped to [-1, 1]."""
    prev = max(1, q.prev_oi)
    pct = max(-1.0, min(1.0, q.change_in_oi / float(prev)))
    s = str(side or "").strip().upper()
    sign = 1.0 if s == "LONG" else -1.0
    return max(-1.0, min(1.0, pct * sign))

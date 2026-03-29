"""
Intraday % change from session open for NIFTY50, BANKNIFTY, INDIA VIX.
Upstox market quotes first; Yahoo Finance quote API as fallback.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

YAHOO_SYMBOLS: Dict[str, str] = {
    "nifty50": "^NSEI",
    "banknifty": "^NSEBANK",
    "indiavix": "^INDIAVIX",
}

_YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _pct_from_open(last: float, open_: float) -> Optional[float]:
    if open_ and open_ > 0 and last is not None and last > 0:
        return round((float(last) - float(open_)) / float(open_) * 100.0, 4)
    return None


def _quote_from_upstox(upstox_service, instrument_key: str) -> Optional[Dict[str, Any]]:
    try:
        q = upstox_service.get_market_quote_by_key(instrument_key)
        if not q:
            return None
        last = float(q.get("last_price") or 0)
        ohlc = q.get("ohlc") or {}
        day_open = float(q.get("open") or ohlc.get("open") or 0)
        if day_open <= 0:
            return None
        if last <= 0:
            return None
        pct = _pct_from_open(last, day_open)
        if pct is None:
            return None
        return {
            "last": last,
            "open": day_open,
            "pct_change": pct,
            "source": "upstox",
        }
    except Exception as e:
        logger.warning("Upstox quote failed for %s: %s", instrument_key, e)
        return None


def _yahoo_chart_pct(yahoo_symbol: str) -> Optional[Dict[str, Any]]:
    """Intraday % from session open via Yahoo Finance chart API (v8)."""
    from urllib.parse import quote

    try:
        enc = quote(yahoo_symbol, safe="")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{enc}"
        r = requests.get(
            url,
            params={"interval": "1d", "range": "5d", "includePrePost": "false"},
            headers=_YAHOO_HEADERS,
            timeout=14,
        )
        if r.status_code != 200:
            logger.debug("Yahoo chart %s HTTP %s", yahoo_symbol, r.status_code)
            return None
        data = r.json()
        results = (data.get("chart") or {}).get("result") or []
        if not results:
            return None
        meta = results[0].get("meta") or {}
        price = meta.get("regularMarketPrice")
        if price is None:
            price = meta.get("previousClose")
        if price is None:
            return None
        price = float(price)
        open_f = meta.get("regularMarketOpen")
        if open_f is None or float(open_f) <= 0:
            open_f = meta.get("chartPreviousClose") or meta.get("previousClose")
        open_f = float(open_f) if open_f is not None else 0.0
        if open_f <= 0:
            return None
        pct = _pct_from_open(price, open_f)
        if pct is None:
            return None
        return {
            "last": price,
            "open": open_f,
            "pct_change": pct,
            "source": "yahoo",
        }
    except Exception as e:
        logger.debug("Yahoo chart %s failed: %s", yahoo_symbol, e)
        return None


def _fetch_yahoo_batch() -> Dict[str, Dict[str, Any]]:
    """Map id (nifty50, banknifty, indiavix) -> {last, open, pct_change, source}."""
    out: Dict[str, Dict[str, Any]] = {}
    for id_key, ysym in YAHOO_SYMBOLS.items():
        row = _yahoo_chart_pct(ysym)
        if row:
            out[id_key] = row
    return out


def build_dial_rows(upstox_service) -> List[Dict[str, Any]]:
    """
    Returns three rows: NIFTY 50, BANKNIFTY, INDIA VIX with intraday % from session open.
    """
    specs = [
        ("nifty50", "NIFTY 50", upstox_service.NIFTY50_KEY),
        ("banknifty", "BANKNIFTY", upstox_service.BANKNIFTY_KEY),
        ("indiavix", "INDIA VIX", getattr(upstox_service, "INDIA_VIX_KEY", "NSE_INDEX|India VIX")),
    ]
    yahoo_map = _fetch_yahoo_batch()
    rows: List[Dict[str, Any]] = []
    for id_key, label, ust_key in specs:
        u = _quote_from_upstox(upstox_service, ust_key)
        if u is not None:
            rows.append(
                {
                    "id": id_key,
                    "label": label,
                    "pct_change": u["pct_change"],
                    "last": u["last"],
                    "open": u["open"],
                    "source": u["source"],
                }
            )
            continue
        y = yahoo_map.get(id_key)
        if y is not None:
            rows.append(
                {
                    "id": id_key,
                    "label": label,
                    "pct_change": y["pct_change"],
                    "last": y["last"],
                    "open": y["open"],
                    "source": y["source"],
                }
            )
        else:
            rows.append(
                {
                    "id": id_key,
                    "label": label,
                    "pct_change": None,
                    "last": None,
                    "open": None,
                    "source": "unavailable",
                }
            )
    return rows


def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

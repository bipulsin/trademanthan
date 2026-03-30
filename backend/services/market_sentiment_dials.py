"""
Intraday % change from session open for NIFTY50, BANKNIFTY, INDIA VIX.
Upstox market quotes first; Yahoo Finance quote API as fallback.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
import pytz

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


def _pct_from_ref(last: float, ref_price: float) -> Optional[float]:
    if ref_price and ref_price > 0 and last is not None and last > 0:
        return round((float(last) - float(ref_price)) / float(ref_price) * 100.0, 4)
    return None


def _vix_level_from_upstox(upstox_service, instrument_key: str) -> Optional[Dict[str, Any]]:
    """India VIX spot (LTP) for 0–35 dial; open/pct optional."""
    try:
        q = upstox_service.get_market_quote_by_key(instrument_key)
        if not q:
            return None
        last = float(q.get("last_price") or 0)
        if last <= 0:
            return None
        ohlc = q.get("ohlc") or {}
        day_open = float(q.get("open") or ohlc.get("open") or 0)
        pct = _pct_from_open(last, day_open) if day_open > 0 else None
        return {
            "last": last,
            "open": day_open if day_open > 0 else None,
            "pct_change": pct,
            "source": "upstox",
        }
    except Exception as e:
        logger.warning("Upstox VIX quote failed for %s: %s", instrument_key, e)
        return None


def _yahoo_chart_last_only(yahoo_symbol: str) -> Optional[Dict[str, Any]]:
    """Spot price from Yahoo chart meta (for VIX when % vs open is unavailable)."""
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
        if price <= 0:
            return None
        open_f = meta.get("regularMarketOpen")
        open_f = float(open_f) if open_f is not None else 0.0
        pct = _pct_from_open(price, open_f) if open_f > 0 else None
        return {
            "last": price,
            "open": open_f if open_f > 0 else None,
            "pct_change": pct,
            "source": "yahoo",
        }
    except Exception as e:
        logger.debug("Yahoo chart last-only %s failed: %s", yahoo_symbol, e)
        return None


def _quote_from_upstox(upstox_service, instrument_key: str, basis: str = "today") -> Optional[Dict[str, Any]]:
    try:
        q = upstox_service.get_market_quote_by_key(instrument_key)
        if not q:
            return None
        last = float(q.get("last_price") or 0)
        ohlc = q.get("ohlc") or {}
        if last <= 0:
            return None

        basis_norm = str(basis or "today").strip().lower()
        if basis_norm == "yesterday":
            # For "Basis Yesterday", do NOT use market-quote close directly because it may
            # represent today's close/last post-market on some feeds.
            ref = float(_previous_trading_close_from_upstox(upstox_service, instrument_key) or 0)
            ref_label = "previous_close"
        else:
            ref = float(q.get("open") or ohlc.get("open") or 0)
            ref_label = "today_open"

        if ref <= 0:
            return None
        pct = _pct_from_ref(last, ref)
        if pct is None:
            return None
        return {
            "last": last,
            "open": ref,
            "reference_price": ref,
            "reference_label": ref_label,
            "pct_change": pct,
            "source": "upstox",
        }
    except Exception as e:
        logger.warning("Upstox quote failed for %s: %s", instrument_key, e)
        return None


def _previous_trading_close_from_upstox(upstox_service, instrument_key: str) -> Optional[float]:
    """
    Previous trading-day close from daily candles.
    Prefers the most recent candle strictly before today IST.
    """
    try:
        candles = upstox_service.get_historical_candles_by_instrument_key(
            instrument_key, interval="days/1", days_back=10
        )
        if not candles:
            return None

        ist_today = datetime.now(pytz.timezone("Asia/Kolkata")).date()
        dated: List[tuple[Any, float]] = []
        for c in candles:
            ts = str(c.get("timestamp") or "")
            cl = float(c.get("close") or 0)
            if len(ts) < 10 or cl <= 0:
                continue
            d = datetime.strptime(ts[:10], "%Y-%m-%d").date()
            dated.append((d, cl))

        if not dated:
            return None

        # Prefer latest candle date < today (true previous trading day close)
        prev = [(d, cl) for d, cl in dated if d < ist_today]
        if prev:
            prev.sort(key=lambda x: x[0])
            return float(prev[-1][1])

        # Fallback: second latest close when all candles are "today/unknown"
        if len(dated) >= 2:
            dated.sort(key=lambda x: x[0])
            return float(dated[-2][1])
        return None
    except Exception:
        return None


def _yahoo_chart_pct(yahoo_symbol: str, basis: str = "today") -> Optional[Dict[str, Any]]:
    """% from today's open or previous close via Yahoo Finance chart API (v8)."""
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
        result0 = results[0] or {}
        meta = result0.get("meta") or {}
        price = meta.get("regularMarketPrice")
        if price is None:
            price = meta.get("previousClose")
        if price is None:
            return None
        price = float(price)
        basis_norm = str(basis or "today").strip().lower()
        if basis_norm == "yesterday":
            ref_f = meta.get("previousClose") or meta.get("chartPreviousClose")
            ref_label = "previous_close"
        else:
            ref_f = meta.get("regularMarketOpen")
            # NSE index symbols often return regularMarketOpen=None after market hours.
            # Fall back to latest available daily candle open (today's open).
            if ref_f is None or float(ref_f) <= 0:
                indicators = (result0.get("indicators") or {})
                quotes = (indicators.get("quote") or [])
                q0 = quotes[0] if quotes and isinstance(quotes[0], dict) else {}
                opens = q0.get("open") or []
                if isinstance(opens, list):
                    for v in reversed(opens):
                        try:
                            if v is not None and float(v) > 0:
                                ref_f = float(v)
                                break
                        except (TypeError, ValueError):
                            continue
            ref_label = "today_open"

        ref_f = float(ref_f) if ref_f is not None else 0.0
        if ref_f <= 0:
            return None
        pct = _pct_from_ref(price, ref_f)
        if pct is None:
            return None
        return {
            "last": price,
            "open": ref_f,
            "reference_price": ref_f,
            "reference_label": ref_label,
            "pct_change": pct,
            "source": "yahoo",
        }
    except Exception as e:
        logger.debug("Yahoo chart %s failed: %s", yahoo_symbol, e)
        return None


def _fetch_yahoo_batch(basis: str = "today") -> Dict[str, Dict[str, Any]]:
    """Map id (nifty50, banknifty, indiavix) -> {last, open, pct_change, source}."""
    out: Dict[str, Dict[str, Any]] = {}
    for id_key, ysym in YAHOO_SYMBOLS.items():
        # Basis switch applies only to NIFTY/BANKNIFTY; VIX stays as current behavior.
        row_basis = "today" if id_key == "indiavix" else basis
        row = _yahoo_chart_pct(ysym, basis=row_basis)
        if row:
            out[id_key] = row
    return out


def _row_payload(
    id_key: str,
    label: str,
    data: Optional[Dict[str, Any]],
    unavailable: bool = False,
) -> Dict[str, Any]:
    if unavailable or not data:
        base = {
            "id": id_key,
            "label": label,
            "pct_change": None,
            "last": None,
            "open": None,
            "source": "unavailable",
        }
        if id_key == "indiavix":
            base["dial_mode"] = "vix"
            base["vix_value"] = None
            base["vix_scale"] = {"min": 0, "max": 35}
        else:
            base["dial_mode"] = "pct"
        return base
    if id_key == "indiavix":
        v = float(data["last"])
        return {
            "id": id_key,
            "label": label,
            "dial_mode": "vix",
            "pct_change": data.get("pct_change"),
            "last": v,
            "open": data.get("open"),
            "vix_value": v,
            "vix_scale": {"min": 0, "max": 35},
            "source": data["source"],
        }
    return {
        "id": id_key,
        "label": label,
        "dial_mode": "pct",
        "pct_change": data["pct_change"],
        "last": data["last"],
        "open": data["open"],
        "reference_price": data.get("reference_price", data.get("open")),
        "reference_label": data.get("reference_label", "today_open"),
        "source": data["source"],
    }


def build_dial_rows(upstox_service, basis: str = "today") -> List[Dict[str, Any]]:
    """
    Returns three rows: NIFTY 50, BANKNIFTY (intraday % vs open), INDIA VIX (spot level for 0–35 dial).
    """
    nifty_key = upstox_service.NIFTY50_KEY
    bank_key = upstox_service.BANKNIFTY_KEY
    vix_key = getattr(upstox_service, "INDIA_VIX_KEY", "NSE_INDEX|India VIX")
    basis_norm = str(basis or "today").strip().lower()
    if basis_norm not in ("today", "yesterday"):
        basis_norm = "today"
    yahoo_map = _fetch_yahoo_batch(basis=basis_norm)
    rows: List[Dict[str, Any]] = []

    if basis_norm == "yesterday":
        # For yesterday-basis, prefer Upstox daily candles (previous trading day close).
        # Yahoo previousClose can occasionally be stale/shifted for NSE index symbols.
        u = _quote_from_upstox(upstox_service, nifty_key, basis=basis_norm) or yahoo_map.get("nifty50")
    else:
        u = _quote_from_upstox(upstox_service, nifty_key, basis=basis_norm) or yahoo_map.get("nifty50")
    r = _row_payload("nifty50", "NIFTY 50", u)
    r["basis"] = basis_norm
    rows.append(r)

    if basis_norm == "yesterday":
        u = _quote_from_upstox(upstox_service, bank_key, basis=basis_norm) or yahoo_map.get("banknifty")
    else:
        u = _quote_from_upstox(upstox_service, bank_key, basis=basis_norm) or yahoo_map.get("banknifty")
    r = _row_payload("banknifty", "BANKNIFTY", u)
    r["basis"] = basis_norm
    rows.append(r)

    u = _vix_level_from_upstox(upstox_service, vix_key)
    if u is None:
        u = yahoo_map.get("indiavix") or _yahoo_chart_last_only(YAHOO_SYMBOLS["indiavix"])
    rows.append(_row_payload("indiavix", "INDIA VIX", u))

    return rows


def utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

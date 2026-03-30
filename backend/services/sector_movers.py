"""
Nifty sector index movers: % change vs previous trading-day close (Yahoo Finance v8 chart).
Used by dashboard Top Gainers & Losers (sectors).
"""
from __future__ import annotations

import logging
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.services.market_sentiment_dials import _yahoo_chart_pct
from backend.config import get_instruments_file_path
from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

# Major Nifty sector / strategic indices on Yahoo Finance (display name, symbol)
# Representative NSE equities per sector (Yahoo symbols) for drill-down: top/bottom 3 by % vs previous close.
# Lists are indicative constituents / liquid names; not exhaustive index replication.
SECTOR_STOCK_UNIVERSE: Dict[str, List[str]] = {
    "Nifty Bank": [
        "HDFCBANK.NS",
        "ICICIBANK.NS",
        "SBIN.NS",
        "KOTAKBANK.NS",
        "AXISBANK.NS",
        "INDUSINDBK.NS",
        "BANKBARODA.NS",
        "FEDERALBNK.NS",
        "IDFCFIRSTB.NS",
        "PNB.NS",
    ],
    "Nifty IT": [
        "TCS.NS",
        "INFY.NS",
        "HCLTECH.NS",
        "WIPRO.NS",
        "TECHM.NS",
        "LTIM.NS",
        "MPHASIS.NS",
        "COFORGE.NS",
    ],
    "Nifty Auto": [
        "MARUTI.NS",
        "TATAMOTORS.NS",
        "M&M.NS",
        "BAJAJ-AUTO.NS",
        "EICHERMOT.NS",
        "HEROMOTOCO.NS",
        "BOSCHLTD.NS",
        "ASHOKLEY.NS",
    ],
    "Nifty Pharma": [
        "SUNPHARMA.NS",
        "DRREDDY.NS",
        "CIPLA.NS",
        "DIVISLAB.NS",
        "LUPIN.NS",
        "AUROPHARMA.NS",
        "BIOCON.NS",
        "TORNTPHARM.NS",
    ],
    "Nifty FMCG": [
        "HINDUNILVR.NS",
        "ITC.NS",
        "NESTLEIND.NS",
        "BRITANNIA.NS",
        "DABUR.NS",
        "MARICO.NS",
        "COLPAL.NS",
        "GODREJCP.NS",
    ],
    "Nifty Metal": [
        "TATASTEEL.NS",
        "JSWSTEEL.NS",
        "HINDALCO.NS",
        "VEDL.NS",
        "SAIL.NS",
        "JINDALSTEL.NS",
        "NMDC.NS",
        "RATNAMANI.NS",
    ],
    "Nifty Realty": [
        "DLF.NS",
        "GODREJPROP.NS",
        "OBEROIRLTY.NS",
        "PRESTIGE.NS",
        "BRIGADE.NS",
        "PHOENIXLTD.NS",
        "MAHLIFE.NS",
        "SOBHA.NS",
    ],
    "Nifty Energy": [
        "RELIANCE.NS",
        "ONGC.NS",
        "COALINDIA.NS",
        "NTPC.NS",
        "POWERGRID.NS",
        "IOC.NS",
        "BPCL.NS",
        "GAIL.NS",
    ],
    "Nifty Infra": [
        "LT.NS",
        "ADANIPORTS.NS",
        "POWERGRID.NS",
        "NTPC.NS",
        "SIEMENS.NS",
        "ABB.NS",
        "HAL.NS",
        "BHEL.NS",
    ],
    "Nifty PSU Bank": [
        "SBIN.NS",
        "BANKBARODA.NS",
        "PNB.NS",
        "CANBK.NS",
        "UNIONBANK.NS",
        "IOB.NS",
        "CENTRALBK.NS",
        "MAHABANK.NS",
    ],
    "Nifty Media": [
        "ZEEL.NS",
        "SUNTV.NS",
        "PVRINOX.NS",
        "NETWORK18.NS",
        "DBCORP.NS",
        "JAGRAN.NS",
    ],
    "Nifty Healthcare": [
        "SUNPHARMA.NS",
        "DRREDDY.NS",
        "CIPLA.NS",
        "DIVISLAB.NS",
        "LALPATHLAB.NS",
        "APOLLOHOSP.NS",
        "MAXHEALTH.NS",
        "FORTIS.NS",
    ],
    "Nifty Consumer Durables": [
        "VOLTAS.NS",
        "WHIRLPOOL.NS",
        "CROMPTON.NS",
        "ORIENTELEC.NS",
        "BLUESTARCO.NS",
        "SYMPHONY.NS",
    ],
    "Nifty Oil & Gas": [
        "RELIANCE.NS",
        "ONGC.NS",
        "IOC.NS",
        "BPCL.NS",
        "HPCL.NS",
        "GAIL.NS",
        "PETRONET.NS",
        "OIL.NS",
    ],
    "Nifty Financial Services": [
        "HDFCBANK.NS",
        "ICICIBANK.NS",
        "SBIN.NS",
        "KOTAKBANK.NS",
        "AXISBANK.NS",
        "BAJFINANCE.NS",
        "BAJAJFINSV.NS",
        "CHOLAFIN.NS",
    ],
}

NIFTY_SECTOR_INDICES: List[Tuple[str, str]] = [
    ("Nifty Bank", "^NSEBANK"),
    ("Nifty IT", "^CNXIT"),
    ("Nifty Auto", "^CNXAUTO"),
    ("Nifty Pharma", "^CNXPHARMA"),
    ("Nifty FMCG", "^CNXFMCG"),
    ("Nifty Metal", "^CNXMETAL"),
    ("Nifty Realty", "^CNXREALTY"),
    ("Nifty Energy", "^CNXENERGY"),
    ("Nifty Infra", "^CNXINFRA"),
    ("Nifty PSU Bank", "^CNXPSUBANK"),
    ("Nifty Media", "^CNXMEDIA"),
    # Some NSE sector indices use .NS on Yahoo; ^CNX* tickers are inconsistent across Yahoo.
    ("Nifty Healthcare", "NIFTY_HEALTHCARE.NS"),
    ("Nifty Consumer Durables", "NIFTY_CONSR_DURBL.NS"),
    ("Nifty Oil & Gas", "NIFTY_OIL_AND_GAS.NS"),
    ("Nifty Financial Services", "^CNXFIN"),
]


def _fetch_one_sector(label: str, yahoo_symbol: str) -> Optional[Dict[str, Any]]:
    row = _sector_pct_from_upstox(label)
    if not row:
        row = _yahoo_chart_pct(yahoo_symbol, basis="yesterday")
    if not row or row.get("pct_change") is None:
        return None
    return {
        "sector": label,
        "yahoo_symbol": yahoo_symbol,
        "pct_change": float(row["pct_change"]),
        "last": float(row["last"]),
        "open": float(row["open"]) if row.get("open") is not None else None,
        "source": row.get("source", "yahoo"),
    }


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s or "").upper() if ch.isalnum())


# Static sector label -> Upstox index key map (avoid heavy runtime instruments-file scan).
# Any unmapped sector automatically falls back to Yahoo in _fetch_one_sector.
UPSTOX_SECTOR_INDEX_KEYS: Dict[str, str] = {
    "Nifty Bank": "NSE_INDEX|Nifty Bank",
    "Nifty IT": "NSE_INDEX|Nifty IT",
    "Nifty Auto": "NSE_INDEX|Nifty Auto",
    "Nifty Pharma": "NSE_INDEX|Nifty Pharma",
    "Nifty FMCG": "NSE_INDEX|Nifty FMCG",
    "Nifty Metal": "NSE_INDEX|Nifty Metal",
    "Nifty Realty": "NSE_INDEX|Nifty Realty",
    "Nifty Energy": "NSE_INDEX|Nifty Energy",
    "Nifty Infra": "NSE_INDEX|Nifty Infra",
    "Nifty PSU Bank": "NSE_INDEX|Nifty PSU Bank",
    "Nifty Media": "NSE_INDEX|Nifty Media",
    "Nifty Healthcare": "NSE_INDEX|NIFTY HEALTHCARE",
    "Nifty Oil & Gas": "NSE_INDEX|NIFTY OIL AND GAS",
    "Nifty Financial Services": "NSE_INDEX|Nifty Fin Service",
}


def _previous_trading_close_from_upstox_index(instrument_key: str) -> Optional[float]:
    try:
        if not upstox_service:
            return None
        candles = upstox_service.get_historical_candles_by_instrument_key(
            instrument_key, interval="days/1", days_back=10
        ) or []
        if not candles:
            return None
        import pytz

        ist_today = datetime.now(pytz.timezone("Asia/Kolkata")).date()
        parsed: List[tuple[Any, float]] = []
        for c in candles:
            ts = str(c.get("timestamp") or "")
            cl = float(c.get("close") or 0)
            if len(ts) < 10 or cl <= 0:
                continue
            d = datetime.strptime(ts[:10], "%Y-%m-%d").date()
            parsed.append((d, cl))
        if not parsed:
            return None
        prev = [(d, cl) for d, cl in parsed if d < ist_today]
        if prev:
            prev.sort(key=lambda x: x[0])
            return float(prev[-1][1])
        if len(parsed) >= 2:
            parsed.sort(key=lambda x: x[0])
            return float(parsed[-2][1])
        return None
    except Exception:
        return None


def _sector_pct_from_upstox(label: str) -> Optional[Dict[str, Any]]:
    """
    Compute sector index % change vs previous close using Upstox:
    pct = (last - prev_close) / prev_close * 100
    """
    try:
        if not upstox_service or not getattr(upstox_service, "access_token", None):
            return None
        ikey = UPSTOX_SECTOR_INDEX_KEYS.get(str(label or "").strip())
        if not ikey:
            return None
        q = upstox_service.get_market_quote_by_key(ikey)
        if not q:
            return None
        last = float(q.get("last_price") or 0)
        if last <= 0:
            return None
        prev_close = _previous_trading_close_from_upstox_index(ikey)
        if not prev_close or prev_close <= 0:
            return None
        pct = round((last - prev_close) / prev_close * 100.0, 4)
        return {
            "last": last,
            "open": prev_close,
            "pct_change": pct,
            "source": "upstox",
        }
    except Exception as e:
        logger.debug("Upstox sector fetch failed for %s: %s", label, e)
        return None


def build_sector_movers(top_n: int = 3) -> Dict[str, Any]:
    """
    Top ``top_n`` gaining and losing Nifty sector indices by % vs previous close.
    """
    rows: List[Dict[str, Any]] = []
    max_workers = min(16, max(4, len(NIFTY_SECTOR_INDICES)))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_fetch_one_sector, label, sym) for label, sym in NIFTY_SECTOR_INDICES]
        for fut in as_completed(futs):
            try:
                r = fut.result()
                if r:
                    rows.append(r)
            except Exception as e:
                logger.debug("sector mover fetch failed: %s", e)

    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if not rows:
        return {
            "success": True,
            "updated_at": ts,
            "gainers": [],
            "losers": [],
            "source": "yahoo",
            "universe_size": 0,
        }

    by_hi = sorted(rows, key=lambda x: x["pct_change"], reverse=True)
    gainers = by_hi[:top_n]
    by_lo = sorted(rows, key=lambda x: x["pct_change"])
    losers = by_lo[:top_n]

    return {
        "success": True,
        "updated_at": ts,
        "gainers": gainers,
        "losers": losers,
        "source": "yahoo",
        "universe_size": len(rows),
    }


def _yahoo_display_symbol(yahoo_sym: str) -> str:
    base = (yahoo_sym or "").split(".")[0].strip()
    return base.upper() if base else yahoo_sym


def _fetch_one_equity_row(yahoo_sym: str) -> Optional[Dict[str, Any]]:
    row = _yahoo_chart_pct(yahoo_sym, basis="yesterday")
    if not row or row.get("pct_change") is None:
        return None
    return {
        "symbol": _yahoo_display_symbol(yahoo_sym),
        "yahoo_symbol": yahoo_sym,
        "ltp": round(float(row["last"]), 2),
        "pct_change": float(row["pct_change"]),
    }


@lru_cache(maxsize=1)
def _load_fo_underlyings() -> frozenset[str]:
    """
    Load equity underlyings available in NSE F&O from instruments file.
    Uses option/future contracts to infer whether underlying stock is F&O tradable.
    """
    fo_names: set[str] = set()
    try:
        instruments_file: Path = get_instruments_file_path()
        if not instruments_file.exists():
            return frozenset()
        import json

        with open(instruments_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return frozenset()

        for inst in data:
            if not isinstance(inst, dict):
                continue
            seg = str(inst.get("segment") or "").upper()
            it = str(inst.get("instrument_type") or "").upper()
            if "NSE_FO" not in seg and "NFO" not in seg:
                continue
            if it not in ("CE", "PE", "FUT"):
                continue
            u = (inst.get("underlying_symbol") or inst.get("name") or "").strip().upper()
            if u:
                fo_names.add(u)
    except Exception as e:
        logger.debug("Could not load F&O underlyings: %s", e)
    return frozenset(fo_names)


def _is_fo_stock(symbol: str) -> bool:
    base = (symbol or "").strip().upper()
    if not base:
        return False
    return base in _load_fo_underlyings()


def build_sector_stock_detail(sector_label: str, mode: str) -> Dict[str, Any]:
    """
    Stocks in ``SECTOR_STOCK_UNIVERSE`` for a sector label: top 3 by % vs previous close
    (mode=gainers) or bottom 3 (mode=losers).
    """
    label = (sector_label or "").strip()
    syms = SECTOR_STOCK_UNIVERSE.get(label)
    if not syms:
        return {
            "success": False,
            "message": "Unknown or unsupported sector",
            "sector": label,
            "mode": mode,
            "stocks": [],
        }

    m = (mode or "gainers").strip().lower()
    if m not in ("gainers", "losers"):
        m = "gainers"

    rows: List[Dict[str, Any]] = []
    max_workers = min(12, max(4, len(syms)))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_fetch_one_equity_row, s) for s in syms]
        for fut in as_completed(futs):
            try:
                r = fut.result()
                if r:
                    rows.append(r)
            except Exception as e:
                logger.debug("sector stock fetch failed: %s", e)

    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if not rows:
        return {
            "success": True,
            "updated_at": ts,
            "sector": label,
            "mode": m,
            "stocks": [],
        }

    for r in rows:
        r["is_fo"] = _is_fo_stock(str(r.get("symbol") or ""))

    fo_rows = [r for r in rows if r.get("is_fo")]
    src_rows = fo_rows if fo_rows else rows
    src_rows.sort(key=lambda x: x["pct_change"], reverse=True)
    if m == "gainers":
        pick = src_rows[:3]
    else:
        pick = sorted(src_rows, key=lambda x: x["pct_change"])[:3]

    return {
        "success": True,
        "updated_at": ts,
        "sector": label,
        "mode": m,
        "fo_only": bool(fo_rows),
        "stocks": pick,
    }

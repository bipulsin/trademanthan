"""
Nifty sector index movers: intraday % change from session open (Yahoo Finance v8 chart).
Used by dashboard Top Gainers & Losers (sectors).
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from backend.services.market_sentiment_dials import _yahoo_chart_pct

logger = logging.getLogger(__name__)

# Major Nifty sector / strategic indices on Yahoo Finance (display name, symbol)
# Representative NSE equities per sector (Yahoo symbols) for drill-down: top/bottom 3 by intraday % vs open.
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
    row = _yahoo_chart_pct(yahoo_symbol)
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


def build_sector_movers(top_n: int = 3) -> Dict[str, Any]:
    """
    Top ``top_n`` gaining and losing Nifty sector indices by intraday % from open.
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
    row = _yahoo_chart_pct(yahoo_sym)
    if not row or row.get("pct_change") is None:
        return None
    return {
        "symbol": _yahoo_display_symbol(yahoo_sym),
        "yahoo_symbol": yahoo_sym,
        "ltp": round(float(row["last"]), 2),
        "pct_change": float(row["pct_change"]),
    }


def build_sector_stock_detail(sector_label: str, mode: str) -> Dict[str, Any]:
    """
    Stocks in ``SECTOR_STOCK_UNIVERSE`` for a sector label: top 3 by intraday % vs open
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

    rows.sort(key=lambda x: x["pct_change"], reverse=True)
    if m == "gainers":
        pick = rows[:3]
    else:
        pick = sorted(rows, key=lambda x: x["pct_change"])[:3]

    return {
        "success": True,
        "updated_at": ts,
        "sector": label,
        "mode": m,
        "stocks": pick,
    }

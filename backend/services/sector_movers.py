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

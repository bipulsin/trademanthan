"""Sector cluster detection and S1/W1 badges for RS checklist."""
from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List

from sqlalchemy import text

from backend.services.sector_movers import get_sector_movers_cached, nifty_sector_label_for_nse_equity


def compute_sector_badges(db) -> Dict[str, str]:
    """Map symbol → S1/S2/S3 (top gainers) or W1/W2/W3 (top losers)."""
    try:
        mv = get_sector_movers_cached(top_n=3)
        gsectors = [r.get("sector") for r in (mv.get("gainers") or []) if r.get("sector")]
        lsectors = [r.get("sector") for r in (mv.get("losers") or []) if r.get("sector")]
        gmap = {lbl: f"S{idx + 1}" for idx, lbl in enumerate(gsectors[:3])}
        lmap = {lbl: f"W{idx + 1}" for idx, lbl in enumerate(lsectors[:3])}
    except Exception:
        return {}
    rows = db.execute(
        text("SELECT stock FROM arbitrage_master WHERE stock IS NOT NULL")
    ).fetchall()
    out: Dict[str, str] = {}
    for r in rows:
        sym = str(r.stock).strip().upper()
        lbl = nifty_sector_label_for_nse_equity(sym)
        if not lbl:
            continue
        badge = gmap.get(lbl) or lmap.get(lbl)
        if badge:
            out[sym] = badge
    return out


def detect_sector_clusters(
    ranked: List[Dict[str, Any]], *, direction: str
) -> List[Dict[str, Any]]:
    """If 3+ of Top 5 share a sector, return cluster info."""
    bucket = [r for r in ranked if (r.get("ranking_type") or "").upper() == direction.upper()]
    if len(bucket) < 3:
        return []
    sectors: List[str] = []
    for r in bucket[:5]:
        sym = str(r.get("symbol") or "").upper()
        lbl = nifty_sector_label_for_nse_equity(sym)
        sectors.append(lbl or "Unknown")
    counts = Counter(sectors)
    clusters: List[Dict[str, Any]] = []
    for sector, cnt in counts.items():
        if cnt >= 3:
            members = [
                r for r in bucket[:5]
                if nifty_sector_label_for_nse_equity(str(r.get("symbol") or "")) == sector
            ]
            leader = max(members, key=lambda x: float(x.get("trade_score") or 0))
            clusters.append(
                {
                    "sector": sector,
                    "count": cnt,
                    "direction": direction,
                    "leader_symbol": leader.get("symbol"),
                    "leader_score": leader.get("trade_score"),
                }
            )
    return clusters

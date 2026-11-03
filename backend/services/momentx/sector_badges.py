"""Sector strength badges for MomentX cards / trade log (dashboard Top Gainers/Losers)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from backend.services.sector_movers import (
    get_sector_movers_cached,
    get_sector_stock_detail_cached,
    nifty_sector_label_for_nse_equity,
)

logger = logging.getLogger(__name__)


def _sector_rank_maps(top_n: int = 3) -> tuple[Dict[str, str], Dict[str, str]]:
    """Return (gainer_label→S1..S3, loser_label→W1..W3)."""
    try:
        mv = get_sector_movers_cached(top_n=top_n)
    except Exception:
        logger.exception("momentx sector movers fetch failed")
        return {}, {}
    gmap: Dict[str, str] = {}
    lmap: Dict[str, str] = {}
    for idx, row in enumerate((mv.get("gainers") or [])[:top_n]):
        lbl = str(row.get("sector") or "").strip()
        if lbl:
            gmap[lbl] = f"S{idx + 1}"
    for idx, row in enumerate((mv.get("losers") or [])[:top_n]):
        lbl = str(row.get("sector") or "").strip()
        if lbl:
            lmap[lbl] = f"W{idx + 1}"
    return gmap, lmap


def _top_symbols_in_sector(sector_label: str, mode: str) -> set[str]:
    try:
        detail = get_sector_stock_detail_cached(sector_label, mode)
    except Exception:
        logger.exception("momentx sector stock detail failed %s %s", sector_label, mode)
        return set()
    out: set[str] = set()
    for row in detail.get("stocks") or []:
        sym = str(row.get("symbol") or "").strip().upper()
        if sym:
            out.add(sym)
    return out


def badges_for_symbols(symbols: Iterable[str], *, top_n: int = 3) -> Dict[str, Dict[str, Any]]:
    """Map STOCK → badge fields for MomentX UI.

    ``sector_badge`` is ``S1``..``S3`` or ``W1``..``W3`` when the stock's Nifty sector
    is among dashboard Top Gainers / Top Losers. ``sector_star`` is True when the
    stock itself is also among that sector's top-3 gainers (S*) or losers (W*).
    """
    gmap, lmap = _sector_rank_maps(top_n=top_n)
    if not gmap and not lmap:
        return {}

    uniq = sorted({str(s or "").strip().upper() for s in symbols if str(s or "").strip()})
    pending: Dict[str, Dict[str, Any]] = {}
    sectors_needed: Dict[str, str] = {}  # label → gainers|losers

    for sym in uniq:
        lbl = nifty_sector_label_for_nse_equity(sym)
        if not lbl:
            continue
        badge = gmap.get(lbl) or lmap.get(lbl)
        if not badge:
            continue
        mode = "gainers" if badge.startswith("S") else "losers"
        pending[sym] = {
            "nifty_sector_label": lbl,
            "sector_badge": badge,
            "sector_star": False,
            "sector_badge_display": badge,
        }
        sectors_needed[lbl] = mode

    top_by_sector: Dict[str, set[str]] = {}
    for lbl, mode in sectors_needed.items():
        top_by_sector[lbl] = _top_symbols_in_sector(lbl, mode)

    for sym, info in pending.items():
        lbl = str(info["nifty_sector_label"])
        if sym in top_by_sector.get(lbl, set()):
            info["sector_star"] = True
            info["sector_badge_display"] = f"{info['sector_badge']}*"

    return pending


def apply_badges_to_cards(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return copies of cards with sector badge fields filled (or cleared)."""
    if not cards:
        return cards
    by = badges_for_symbols(str(c.get("stock") or "") for c in cards)
    out: List[Dict[str, Any]] = []
    for c in cards:
        card = dict(c)
        sym = str(card.get("stock") or "").strip().upper()
        info = by.get(sym)
        if info:
            card["nifty_sector_label"] = info.get("nifty_sector_label")
            card["sector_badge"] = info.get("sector_badge")
            card["sector_star"] = bool(info.get("sector_star"))
            card["sector_badge_display"] = info.get("sector_badge_display") or info.get("sector_badge")
        else:
            card.pop("nifty_sector_label", None)
            card.pop("sector_badge", None)
            card.pop("sector_star", None)
            card.pop("sector_badge_display", None)
        out.append(card)
    return out


def badge_for_stock(stock: str) -> Optional[Dict[str, Any]]:
    info = badges_for_symbols([stock]).get(str(stock or "").strip().upper())
    return info


_BADGE_RANK = {
    "S1": 0,
    "S2": 1,
    "S3": 2,
    "W1": 3,
    "W2": 4,
    "W3": 5,
}


def _badge_sort_key(card: Dict[str, Any]) -> int:
    raw = str(card.get("sector_badge") or card.get("sector_badge_display") or "").strip().upper()
    # Strip trailing star from display form.
    if raw.endswith("*"):
        raw = raw[:-1]
    return _BADGE_RANK.get(raw, 99)


def _first_active_sort_key(card: Dict[str, Any]) -> float:
    """Epoch seconds for first_active_at; missing → 0 so they sort last when descending."""
    raw = card.get("first_active_at")
    if not raw:
        return 0.0
    try:
        from datetime import datetime

        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return float(dt.timestamp())
    except Exception:
        return 0.0


def sort_ready_cards(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Desk order: Take Trade → sector badge (S1…W3) → 1st Activ newest first."""
    def key(c: Dict[str, Any]) -> tuple:
        status = str(c.get("status") or "").lower()
        can_trade = 0 if (status == "active" and bool(c.get("trigger"))) else 1
        return (
            can_trade,
            _badge_sort_key(c),
            -_first_active_sort_key(c),
            str(c.get("stock") or ""),
        )

    return sorted(list(cards or []), key=key)

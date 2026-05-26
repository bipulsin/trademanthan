"""Batch LTP enrichment for Vajra rating rows (trade plans)."""
from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def enrich_rows_with_ltp(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Set ltp / last_price on rows from Upstox batch quotes."""
    if not rows:
        return rows
    keys: List[str] = []
    key_by_idx: Dict[int, str] = {}
    for i, r in enumerate(rows):
        ik = str(r.get("instrument_key") or "").strip()
        if not ik:
            continue
        keys.append(ik)
        key_by_idx[i] = ik

    if not keys:
        return rows

    ltp_map: Dict[str, float] = {}
    try:
        from backend.config import settings
        from backend.services.upstox_service import UpstoxService

        u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        if getattr(u, "access_token", None):
            ltp_map = u.get_market_quotes_batch_by_keys(list(dict.fromkeys(keys))) or {}
    except Exception as e:
        logger.debug("vajra ltp enrich failed: %s", e)

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        row = dict(r)
        ik = key_by_idx.get(i)
        if ik and ik in ltp_map:
            px = float(ltp_map[ik])
            if px > 0:
                row["ltp"] = round(px, 2)
                row["last_price"] = row["ltp"]
        out.append(row)
    return out

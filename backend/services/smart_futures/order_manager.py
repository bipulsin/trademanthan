"""Broker orders for Smart Futures (intraday MIS)."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from backend.services.smart_futures import repository
from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

# Intraday product for NFO MIS (no overnight)
PRODUCT_INTRADAY = "I"


def place_entry(
    *,
    user_id: Optional[int],
    instrument_key: str,
    direction: str,
    quantity_lots: int,
    tag: str = "SMART_FUTURES_ENTRY",
) -> Dict[str, Any]:
    """
    Place market order for futures. quantity_lots = configured position size (Upstox uses lots as quantity for F&O).
    """
    txn = "BUY" if direction == "LONG" else "SELL"
    res = upstox_service.place_order(
        instrument_key=instrument_key,
        quantity=max(1, int(quantity_lots)),
        transaction_type=txn,
        order_type="MARKET",
        product=PRODUCT_INTRADAY,
        tag=tag,
    )
    if res.get("success"):
        oid = res.get("order_id")
        logger.info("smart_futures entry placed %s %s qty=%s order_id=%s", txn, instrument_key, quantity_lots, oid)
    else:
        logger.warning("smart_futures entry failed %s: %s", instrument_key, res.get("error"))
    return res


def place_exit(
    *,
    user_id: Optional[int],
    instrument_key: str,
    direction: str,
    quantity_lots: int,
    tag: str = "SMART_FUTURES_EXIT",
) -> Dict[str, Any]:
    """Square-off: opposite side."""
    txn = "SELL" if direction == "LONG" else "BUY"
    res = upstox_service.place_order(
        instrument_key=instrument_key,
        quantity=max(1, int(quantity_lots)),
        transaction_type=txn,
        order_type="MARKET",
        product=PRODUCT_INTRADAY,
        tag=tag,
    )
    if res.get("success"):
        logger.info("smart_futures exit placed %s %s qty=%s order_id=%s", txn, instrument_key, quantity_lots, res.get("order_id"))
    else:
        logger.warning("smart_futures exit failed %s: %s", instrument_key, res.get("error"))
    return res


def audit(user_id: Optional[int], position_id: Optional[int], side: str, order_id: Optional[str], qty: int) -> None:
    try:
        repository.insert_order_audit(user_id, position_id, side, order_id, qty)
    except Exception as e:
        logger.error("smart_futures audit insert failed: %s", e)

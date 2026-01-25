import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional

from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

# System-wide live trading switch (default: NO)
TRADING_LIVE_FILE = Path("/home/ubuntu/trademanthan/data/trading_live.json")
trading_live = "NO"


def normalize_trading_live(value: str) -> str:
    return "YES" if str(value).strip().upper() == "YES" else "NO"

def _load_trading_live_from_file() -> None:
    global trading_live
    try:
        if TRADING_LIVE_FILE.exists():
            with open(TRADING_LIVE_FILE, "r") as f:
                data = json.load(f)
            trading_live = normalize_trading_live(data.get("trading_live", "NO"))
            logger.info(f"✅ Loaded trading_live from file: {trading_live}")
        else:
            trading_live = "NO"
    except Exception as e:
        trading_live = "NO"
        logger.warning(f"⚠️ Failed to load trading_live config: {str(e)}")

def _save_trading_live_to_file(value: str) -> None:
    try:
        TRADING_LIVE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "trading_live": value
        }
        with open(TRADING_LIVE_FILE, "w") as f:
            json.dump(payload, f)
        logger.info(f"✅ Saved trading_live to file: {value}")
    except Exception as e:
        logger.error(f"❌ Failed to save trading_live config: {str(e)}")

# Load persisted value at import
_load_trading_live_from_file()


def get_trading_live_value() -> str:
    return trading_live


def set_trading_live_value(value: str) -> str:
    global trading_live
    trading_live = normalize_trading_live(value)
    _save_trading_live_to_file(trading_live)
    return trading_live


def is_trading_live_enabled() -> bool:
    return trading_live == "YES"


def place_live_upstox_order(
    action: str,
    instrument_key: str,
    qty: int,
    stock_name: str,
    option_contract: str,
    tag: Optional[str] = None
) -> Dict[str, Any]:
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}
    if not instrument_key:
        return {"success": False, "error": "Missing instrument_key"}
    if not qty or qty <= 0:
        return {"success": False, "error": "Invalid quantity"}

    result = upstox_service.place_order(
        instrument_key=instrument_key,
        quantity=qty,
        transaction_type=action,
        order_type="MARKET",
        product="I",
        validity="DAY",
        tag=tag
    )

    if result.get("success"):
        logger.warning(
            f"🚨 LIVE ORDER PLACED ({action}): {stock_name} {option_contract} | Qty={qty} | Instrument={instrument_key}"
        )
    else:
        logger.error(
            f"❌ LIVE ORDER FAILED ({action}): {stock_name} {option_contract} | Qty={qty} | Instrument={instrument_key} | Error={result.get('error')}"
        )
    return result

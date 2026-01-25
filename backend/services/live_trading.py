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

def place_live_upstox_gtt_entry(
    instrument_key: str,
    qty: int,
    stock_name: str,
    option_contract: str,
    buy_price: float,
    stop_loss: float,
    risk_reward: float = 2.5
) -> Dict[str, Any]:
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}
    if not instrument_key:
        return {"success": False, "error": "Missing instrument_key"}
    if not qty or qty <= 0:
        return {"success": False, "error": "Invalid quantity"}
    if not buy_price or buy_price <= 0:
        return {"success": False, "error": "Invalid buy_price"}
    if not stop_loss or stop_loss <= 0:
        return {"success": False, "error": "Invalid stop_loss"}

    tick_size = upstox_service.get_tick_size_by_instrument_key(instrument_key) or 0.05

    def _round_to_tick(price: float) -> float:
        return round(round(price / tick_size) * tick_size, 2)

    buy_price = _round_to_tick(buy_price)
    stop_loss = _round_to_tick(stop_loss)
    if stop_loss >= buy_price:
        stop_loss = _round_to_tick(buy_price - tick_size)

    risk = buy_price - stop_loss
    if risk <= 0:
        return {"success": False, "error": "Stop loss must be below buy price"}

    target_price = _round_to_tick(buy_price + (risk_reward * risk))

    result = upstox_service.place_gtt_order(
        instrument_key=instrument_key,
        quantity=qty,
        transaction_type="BUY",
        entry_price=buy_price,
        stop_loss=stop_loss,
        target_price=target_price,
        product="D"
    )

    if result.get("success"):
        logger.warning(
            f"🚨 LIVE GTT ENTRY PLACED: {stock_name} {option_contract} | "
            f"Qty={qty} | Entry=₹{buy_price:.2f} | SL=₹{stop_loss:.2f} | Target=₹{target_price:.2f}"
        )
    else:
        logger.error(
            f"❌ LIVE GTT ENTRY FAILED: {stock_name} {option_contract} | "
            f"Error={result.get('error')}"
        )
    return result

def _extract_order_status(details: Dict[str, Any]) -> Optional[str]:
    data = details.get("data") if isinstance(details, dict) else None
    if isinstance(data, dict):
        payload = data.get("data")
        if isinstance(payload, list) and payload:
            return payload[0].get("status")
        if isinstance(payload, dict):
            return payload.get("status")
    return None

def place_live_upstox_exit(
    instrument_key: str,
    qty: int,
    stock_name: str,
    option_contract: str,
    buy_order_id: Optional[str],
    tag: Optional[str] = None
) -> Dict[str, Any]:
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not buy_order_id:
        return {"success": False, "skipped": False, "error": "Missing buy_order_id"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}

    details = upstox_service.get_order_details(buy_order_id)
    if not details.get("success"):
        return {"success": False, "error": details.get("error", "Order details failed")}

    status = _extract_order_status(details.get("data", {})) or ""
    status = status.upper()

    if status == "COMPLETE":
        return place_live_upstox_order(
            action="SELL",
            instrument_key=instrument_key,
            qty=qty,
            stock_name=stock_name,
            option_contract=option_contract,
            tag=tag
        )
    if status == "OPEN":
        cancel = upstox_service.cancel_order(buy_order_id)
        if cancel.get("success"):
            return {"success": False, "skipped": True, "info": "Buy order open; canceled"}
        return {"success": False, "error": cancel.get("error", "Cancel failed")}
    if status in {"REJECTED", "CANCELLED", "CANCELED"}:
        return {"success": False, "skipped": True, "info": f"Buy order {status}"}

    return {"success": False, "skipped": True, "info": f"Buy order status {status or 'UNKNOWN'}"}

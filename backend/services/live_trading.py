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
    tag: Optional[str] = None,
    product: str = "I",
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
        product=product,
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


def place_live_upstox_entry_market_first(
    instrument_key: str,
    qty: int,
    stock_name: str,
    option_contract: str,
    buy_price: float,
    stop_loss: float,
    risk_reward: float = 2.5,
) -> Dict[str, Any]:
    """
    Primary: MARKET BUY at LTP (executes at exchange; order_id is a normal order id).
    Backup: GTT (entry + SL + target) if market order fails and buy_price/stop_loss are valid.
    """
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}
    if not instrument_key:
        return {"success": False, "error": "Missing instrument_key"}
    if not qty or qty <= 0:
        return {"success": False, "error": "Invalid quantity"}

    market_tag = f"entry_mkt|{stock_name}"[:200]
    market_res = place_live_upstox_order(
        "BUY",
        instrument_key=instrument_key,
        qty=qty,
        stock_name=stock_name,
        option_contract=option_contract,
        tag=market_tag,
        product="I",
    )
    if market_res.get("success"):
        oid = market_res.get("order_id")
        logger.warning(
            "🚨 LIVE MARKET ENTRY: %s %s | buy_order_id=%s | method=market",
            stock_name,
            option_contract,
            oid,
        )
        return {
            "success": True,
            "order_id": oid,
            "method": "market",
            "market_response": market_res,
        }
    if market_res.get("skipped"):
        return market_res

    m_err = market_res.get("error") or "Market BUY failed"
    logger.warning(
        "Market entry failed for %s (%s), trying GTT backup: %s",
        stock_name,
        option_contract,
        m_err,
    )
    if not buy_price or buy_price <= 0 or not stop_loss or stop_loss <= 0:
        return {
            "success": False,
            "error": f"Market failed ({m_err}); GTT backup skipped (invalid buy_price/stop_loss)",
            "market_error": m_err,
        }

    gtt_res = place_live_upstox_gtt_entry(
        instrument_key=instrument_key,
        qty=qty,
        stock_name=stock_name,
        option_contract=option_contract,
        buy_price=buy_price,
        stop_loss=stop_loss,
        risk_reward=risk_reward,
    )
    if gtt_res.get("success"):
        logger.warning(
            "🚨 LIVE GTT BACKUP ENTRY: %s %s | gtt_id=%s",
            stock_name,
            option_contract,
            gtt_res.get("order_id"),
        )
        return {
            "success": True,
            "order_id": gtt_res.get("order_id"),
            "method": "gtt_backup",
            "market_error": m_err,
            "gtt_response": gtt_res,
        }
    return {
        "success": False,
        "error": gtt_res.get("error") or "GTT backup failed",
        "market_error": m_err,
        "gtt_error": gtt_res.get("error"),
    }


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

def place_live_upstox_exit(
    instrument_key: str,
    qty: int,
    stock_name: str,
    option_contract: str,
    buy_order_id: Optional[str],
    tag: Optional[str] = None
) -> Dict[str, Any]:
    """
    Exit: always use a MARKET SELL at the broker for the open option qty.

    - Market entry positions: `buy_order_id` is the exchange order id from the BUY; we pass it in the
      order tag for traceability (`exit|ref_buy:<id>`). Exit order id is the new SELL's order_id.
    - Legacy GTT-only rows (`GTT-...`): cancel GTT bundle, then market SELL (product D to match GTT).
    """
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not buy_order_id:
        return {"success": False, "skipped": False, "error": "Missing buy_order_id"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}

    oid = str(buy_order_id).strip()
    # Legacy GTT entry ids
    if oid.upper().startswith("GTT"):
        cancel_result = upstox_service.cancel_gtt_order(oid)
        if not cancel_result.get("success"):
            logger.warning(
                "GTT cancel non-success (order may already be complete): %s — still attempting SELL",
                cancel_result.get("error"),
            )
        sell_result = place_live_upstox_order(
            action="SELL",
            instrument_key=instrument_key,
            qty=qty,
            stock_name=stock_name,
            option_contract=option_contract,
            tag=tag or f"exit_gtt_legacy|ref:{oid}"[:256],
            product="D",
        )
        if sell_result.get("success"):
            return {
                "success": True,
                "order_id": sell_result.get("order_id"),
                "gtt_cancel": cancel_result,
                "exit_method": "market_sell_after_gtt_cancel",
            }
        err = sell_result.get("error") or "Market SELL failed after GTT cancel"
        logger.error(
            "❌ GTT legacy exit: SELL failed for %s %s | err=%s",
            stock_name,
            option_contract,
            err,
        )
        return {
            "success": False,
            "error": err,
            "gtt_cancel": cancel_result,
            "sell": sell_result,
        }

    # Primary path: market SELL (same product as market BUY entry)
    exit_tag = tag or f"exit|ref_buy:{oid}"
    if len(exit_tag) > 250:
        exit_tag = exit_tag[:250]
    sell_result = place_live_upstox_order(
        action="SELL",
        instrument_key=instrument_key,
        qty=qty,
        stock_name=stock_name,
        option_contract=option_contract,
        tag=exit_tag,
        product="I",
    )
    if sell_result.get("success"):
        logger.warning(
            "🚨 LIVE MARKET EXIT: %s %s | sell_order_id=%s | ref_buy_order_id=%s",
            stock_name,
            option_contract,
            sell_result.get("order_id"),
            oid,
        )
        return {
            "success": True,
            "order_id": sell_result.get("order_id"),
            "ref_buy_order_id": oid,
            "exit_method": "market_sell",
        }
    return sell_result

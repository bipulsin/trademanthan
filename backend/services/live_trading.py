import logging
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

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


def _market_orders_disallowed_by_upstox(order_result: Dict[str, Any]) -> bool:
    """True when Upstox rejects MARKET because the scrip disallows market orders (e.g. UDAPI100500)."""
    parts: list[str] = []
    err = order_result.get("error")
    if err:
        parts.append(str(err).lower())
    data = order_result.get("data")
    if isinstance(data, dict):
        parts.append(str(data).lower())
        errs = data.get("errors")
        if isinstance(errs, list):
            for er in errs:
                if isinstance(er, dict):
                    parts.append(str(er.get("message", "")).lower())
                    parts.append(str(er.get("error_code", "")).lower())
                    parts.append(str(er.get("errorCode", "")).lower())
    blob = " ".join(parts)
    if "udapi100500" in blob:
        return True
    if "market" in blob and "not allowed" in blob:
        return True
    return False


def place_live_upstox_limit_buy_at_ltp(
    instrument_key: str,
    qty: int,
    stock_name: str,
    option_contract: str,
    buy_price: float,
    tag: Optional[str] = None,
    product: str = "I",
) -> Dict[str, Any]:
    """
    LIMIT BUY at the smallest tick >= LTP (aggressive) for scrips that reject MARKET orders.
    """
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}
    if not instrument_key:
        return {"success": False, "error": "Missing instrument_key"}
    if not buy_price or buy_price <= 0:
        return {"success": False, "error": "Invalid buy_price for limit entry"}

    tick = float(upstox_service.get_tick_size_by_instrument_key(instrument_key) or 0.05)
    if tick <= 0:
        tick = 0.05
    limit_px = round(math.ceil(float(buy_price) / tick - 1e-12) * tick, 2)
    if limit_px <= 0:
        limit_px = round(buy_price, 2)

    lim_tag = (tag or f"entry_lmt|{stock_name}")[:200]
    result = upstox_service.place_order(
        instrument_key=instrument_key,
        quantity=qty,
        transaction_type="BUY",
        order_type="LIMIT",
        product=product,
        validity="DAY",
        price=limit_px,
        tag=lim_tag,
    )
    out = dict(result)
    out["limit_price"] = limit_px
    if result.get("success"):
        logger.warning(
            "🚨 LIVE LIMIT ENTRY (LTP ceiling): %s %s | limit_price=₹%s | buy_order_id=%s",
            stock_name,
            option_contract,
            limit_px,
            result.get("order_id"),
        )
    else:
        logger.error(
            "❌ LIVE LIMIT ENTRY FAILED: %s %s | limit_price=₹%s | Error=%s",
            stock_name,
            option_contract,
            limit_px,
            result.get("error"),
        )
    return out


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


def _row_from_order_details_api(api_res: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not api_res or not api_res.get("success"):
        return None
    top = api_res.get("data")
    if not isinstance(top, dict):
        return None
    inner = top.get("data")
    if isinstance(inner, dict) and (
        "average_price" in inner or inner.get("order_id") is not None
    ):
        return inner
    return top


def _broker_buy_row_average_price(row: Dict[str, Any]) -> Optional[float]:
    for k in ("average_price", "average_traded_price"):
        v = row.get(k)
        try:
            if v is not None and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _order_row_is_complete_buy(row: Dict[str, Any]) -> bool:
    if (row.get("transaction_type") or "").upper() != "BUY":
        return False
    st = (row.get("status") or "").lower().strip()
    if "cancel" in st or "reject" in st:
        return False
    if st in ("complete", "filled"):
        return True
    return "complete" in st


def sync_trade_buy_fill_from_broker(db, trade) -> bool:
    """
    Align trade.buy_price with the broker's average BUY fill (GTT child leg, limit slip, etc.).
    Keeps buy_order_id unchanged when it is GTT-* so legacy exit (cancel GTT + sell) still works.
    """
    from sqlalchemy.orm.attributes import flag_modified
    import pytz

    _ = db  # session used by caller for commit scope

    if not is_trading_live_enabled() or not upstox_service:
        return False
    if getattr(trade, "status", None) != "bought":
        return False
    if not getattr(trade, "instrument_key", None):
        return False
    if not getattr(trade, "buy_price", None) or float(trade.buy_price) <= 0:
        return False

    qty = int(getattr(trade, "qty", None) or 0)
    if qty <= 0:
        return False

    oid = (getattr(trade, "buy_order_id", None) or "").strip()
    row: Optional[Dict[str, Any]] = None

    ob = upstox_service.get_order_book_today()
    orders_list: List[Dict[str, Any]] = [
        o for o in (ob.get("orders") or []) if isinstance(o, dict)
    ]

    if oid and not oid.upper().startswith("GTT"):
        det = upstox_service.get_order_details(oid)
        cand = _row_from_order_details_api(det)
        if cand and _order_row_is_complete_buy(cand) and _broker_buy_row_average_price(cand):
            fq = int(float(cand.get("filled_quantity") or 0))
            pq = int(float(cand.get("quantity") or 0))
            if fq >= qty * 0.99 or (fq == 0 and pq >= qty * 0.99):
                row = cand
        if row is None and orders_list:
            for o in orders_list:
                if str(o.get("order_id", "")) != str(oid):
                    continue
                if not (_order_row_is_complete_buy(o) and _broker_buy_row_average_price(o)):
                    continue
                fq = int(float(o.get("filled_quantity") or 0))
                if fq < qty * 0.99:
                    continue
                row = o
                break

    if row is None and orders_list:
        ist = pytz.timezone("Asia/Kolkata")
        buy_time = getattr(trade, "buy_time", None)
        if buy_time:
            if buy_time.tzinfo is None:
                buy_time = ist.localize(buy_time)
            else:
                buy_time = buy_time.astimezone(ist)
        buy_naive = buy_time.replace(tzinfo=None) if buy_time else None

        def _ob_ts(o: Dict[str, Any]) -> datetime:
            s = o.get("order_timestamp") or o.get("exchange_timestamp") or ""
            try:
                return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                return datetime.min

        candidates: List[Dict[str, Any]] = []
        for o in orders_list:
            if (o.get("instrument_token") or "") != trade.instrument_key:
                continue
            if (o.get("transaction_type") or "").upper() != "BUY":
                continue
            if not _order_row_is_complete_buy(o):
                continue
            ap = _broker_buy_row_average_price(o)
            if not ap:
                continue
            fq = int(float(o.get("filled_quantity") or 0))
            if fq < qty * 0.99:
                continue
            candidates.append(o)

        if not candidates:
            return False

        candidates.sort(key=_ob_ts, reverse=True)
        if buy_naive:
            after_alert = [c for c in candidates if _ob_ts(c) >= buy_naive - timedelta(minutes=15)]
            if after_alert:
                candidates = after_alert
        row = candidates[0]

    if row is None:
        return False

    new_avg = _broker_buy_row_average_price(row)
    if not new_avg or new_avg <= 0:
        return False

    old = float(trade.buy_price)
    if abs(new_avg - old) < 0.005:
        return False

    trade.buy_price = round(new_avg, 2)
    trade.option_ltp = trade.buy_price
    if trade.stop_loss and float(trade.stop_loss) >= float(trade.buy_price) * 0.999:
        trade.stop_loss = round(float(trade.buy_price) * 0.95, 2)
    if getattr(trade, "sell_price", None) is not None and trade.qty:
        try:
            trade.pnl = (float(trade.sell_price) - float(trade.buy_price)) * int(trade.qty)
            flag_modified(trade, "pnl")
        except (TypeError, ValueError):
            pass

    flag_modified(trade, "buy_price")
    flag_modified(trade, "option_ltp")
    flag_modified(trade, "stop_loss")

    fill_oid = row.get("order_id") or row.get("order_ref_id")
    logger.warning(
        "📌 BUY fill sync: %s buy_price ₹%.2f → ₹%.2f (broker avg, fill_order_id=%s, stored_buy_order_id=%s)",
        getattr(trade, "stock_name", "?"),
        old,
        new_avg,
        fill_oid,
        oid or "none",
    )
    return True


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
    Primary: MARKET BUY at LTP.
    If Upstox returns market-not-allowed on this scrip: LIMIT BUY at LTP (tick-rounded up).
    Else if market fails for other reasons: GTT backup when buy_price/stop_loss are valid.
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

    if _market_orders_disallowed_by_upstox(market_res):
        limit_tag = f"entry_lmt_mktblk|{stock_name}"[:200]
        limit_res = place_live_upstox_limit_buy_at_ltp(
            instrument_key=instrument_key,
            qty=qty,
            stock_name=stock_name,
            option_contract=option_contract,
            buy_price=buy_price,
            tag=limit_tag,
            product="I",
        )
        if limit_res.get("skipped"):
            return limit_res
        if limit_res.get("success"):
            oid = limit_res.get("order_id")
            logger.warning(
                "🚨 LIVE ENTRY via LIMIT (market disallowed on scrip): %s %s | buy_order_id=%s",
                stock_name,
                option_contract,
                oid,
            )
            return {
                "success": True,
                "order_id": oid,
                "method": "limit_after_market_disallowed",
                "market_error": m_err,
                "limit_price": limit_res.get("limit_price"),
                "limit_response": limit_res,
            }
        m_err = f"{m_err}; limit fallback failed: {limit_res.get('error') or 'unknown'}"

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

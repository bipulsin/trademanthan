import logging
import json
import math
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

# After broker accepts a BUY, poll until filled or cancel and fall back (LIMIT can stay open).
ENTRY_FILL_WAIT_SEC = 90.0
ENTRY_FILL_POLL_SEC = 1.5
MARKET_FILL_WAIT_SEC = 45.0

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


def place_live_upstox_limit_buy_at_price(
    instrument_key: str,
    qty: int,
    stock_name: str,
    option_contract: str,
    limit_price: float,
    tag: Optional[str] = None,
    product: str = "I",
) -> Dict[str, Any]:
    """
    LIMIT BUY at an explicit price (rounded to instrument tick).
    Used for bid + (ask - bid) * 0.6 style entries.
    """
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}
    if not instrument_key:
        return {"success": False, "error": "Missing instrument_key"}
    if not limit_price or limit_price <= 0:
        return {"success": False, "error": "Invalid limit_price for limit entry"}

    tick = float(upstox_service.get_tick_size_by_instrument_key(instrument_key) or 0.05)
    if tick <= 0:
        tick = 0.05
    limit_px = round(round(float(limit_price) / tick) * tick, 2)
    if limit_px <= 0:
        limit_px = round(float(limit_price), 2)

    lim_tag = (tag or f"entry_lmt_px|{stock_name}")[:200]
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
            "🚨 LIVE LIMIT ENTRY (explicit): %s %s | limit_price=₹%s | buy_order_id=%s",
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


def cancel_open_buy_if_pending(order_id: Optional[str]) -> None:
    """Best-effort cancel when a BUY did not fill (e.g. open LIMIT). No-op if already filled or gone."""
    if not order_id or not upstox_service:
        return
    det = upstox_service.get_order_details(str(order_id))
    row = _row_from_order_details_api(det)
    if row:
        fq = int(float(row.get("filled_quantity") or 0))
        if _order_row_is_complete_buy(row) and fq > 0:
            return
        st = (row.get("status") or "").lower()
        if "cancel" in st or "reject" in st:
            return
    r = upstox_service.cancel_order(str(order_id))
    if r.get("success"):
        logger.warning("Cancelled unfilled/pending BUY order %s", order_id)
    else:
        logger.warning("Could not cancel BUY order %s: %s", order_id, r.get("error"))


def wait_for_buy_order_fill(
    order_id: str,
    expected_qty: int,
    timeout_sec: float = ENTRY_FILL_WAIT_SEC,
    poll_interval: float = ENTRY_FILL_POLL_SEC,
) -> Dict[str, Any]:
    """
    Poll order details until BUY shows complete with filled qty (>= 99% of expected) and average price,
    or terminal failure / timeout.
    """
    if not order_id or not upstox_service:
        return {"filled": False, "error": "missing order_id or service"}
    deadline = time.monotonic() + float(timeout_sec)
    while time.monotonic() < deadline:
        det = upstox_service.get_order_details(str(order_id))
        row = _row_from_order_details_api(det)
        if not row:
            time.sleep(poll_interval)
            continue
        st = (row.get("status") or "").lower()
        if "cancel" in st or "reject" in st:
            return {"filled": False, "error": "order_cancelled_or_rejected", "status": st, "row": row}
        fq = int(float(row.get("filled_quantity") or 0))
        ap = _broker_buy_row_average_price(row)
        if _order_row_is_complete_buy(row):
            if fq < int(expected_qty) * 0.99:
                return {
                    "filled": False,
                    "error": "partial_fill_incomplete",
                    "filled_quantity": fq,
                    "row": row,
                }
            if ap and ap > 0:
                return {
                    "filled": True,
                    "average_price": ap,
                    "filled_quantity": fq,
                    "row": row,
                }
        time.sleep(poll_interval)
    return {"filled": False, "error": "timeout_waiting_for_fill", "order_id": order_id}


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

    fill_oid = str(row.get("order_id") or row.get("order_ref_id") or "").strip()
    oid_stored = (getattr(trade, "buy_order_id", None) or "").strip()
    old = float(trade.buy_price)

    updated = False

    # Persist broker BUY leg id so API exit (place_live_upstox_exit) is not blocked when GTT place returned no id
    if not oid_stored and fill_oid:
        trade.buy_order_id = fill_oid
        flag_modified(trade, "buy_order_id")
        updated = True
        logger.warning(
            "📌 BUY fill sync: %s buy_order_id set → %s (was empty; broker fill leg)",
            getattr(trade, "stock_name", "?"),
            fill_oid,
        )

    if abs(new_avg - old) >= 0.005:
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

        logger.warning(
            "📌 BUY fill sync: %s buy_price ₹%.2f → ₹%.2f (broker avg, fill_order_id=%s, stored_buy_order_id=%s)",
            getattr(trade, "stock_name", "?"),
            old,
            new_avg,
            fill_oid or "none",
            oid or "none",
        )
        updated = True

    return updated


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
    Primary: LIMIT BUY at bid_price + (ask_price - bid_price) * 0.6 when bid/ask exist and
    spread_pct = (ask-bid)/ask*100 <= 1%. Polls until the order is actually filled; if it stays open,
    cancels and falls through.
    Then MARKET BUY (poll for fill); if Upstox disallows market: LIMIT BUY at LTP (tick ceiling, poll).
    Else if those fail: GTT backup when buy_price/stop_loss are valid (no immediate fill — caller may
    still see LTP until GTT child fills).
    On success with an exchange fill, returns average_fill_price / filled_quantity.
    """
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}
    if not instrument_key:
        return {"success": False, "error": "Missing instrument_key"}
    if not qty or qty <= 0:
        return {"success": False, "error": "Invalid quantity"}

    entry_spread_max_pct = 1.0
    quote = upstox_service.get_market_quote_by_key(instrument_key)
    if quote:
        bid = quote.get("bid_price")
        ask = quote.get("ask_price")
        sp = quote.get("spread_pct")
        if bid is not None and ask is not None:
            try:
                bid_f = float(bid)
                ask_f = float(ask)
            except (TypeError, ValueError):
                bid_f = ask_f = 0.0
            else:
                if bid_f > 0 and ask_f > 0 and bid_f <= ask_f:
                    if sp is None:
                        sp = (ask_f - bid_f) / ask_f * 100.0
                    if sp <= entry_spread_max_pct:
                        raw_limit = bid_f + (ask_f - bid_f) * 0.6
                        lim_tag = f"entry_lmt_spread|{stock_name}"[:200]
                        lim_first = place_live_upstox_limit_buy_at_price(
                            instrument_key=instrument_key,
                            qty=qty,
                            stock_name=stock_name,
                            option_contract=option_contract,
                            limit_price=raw_limit,
                            tag=lim_tag,
                            product="I",
                        )
                        if lim_first.get("skipped"):
                            return lim_first
                        if lim_first.get("success"):
                            oid = lim_first.get("order_id")
                            logger.warning(
                                "🚨 LIVE LIMIT ENTRY (bid+0.6×spread): %s %s | buy_order_id=%s | method=limit_first (awaiting fill)",
                                stock_name,
                                option_contract,
                                oid,
                            )
                            wf = wait_for_buy_order_fill(
                                str(oid), qty, timeout_sec=ENTRY_FILL_WAIT_SEC
                            )
                            if wf.get("filled"):
                                logger.warning(
                                    "✅ BUY FILLED %s %s | order_id=%s | avg=₹%s qty=%s",
                                    stock_name,
                                    option_contract,
                                    oid,
                                    wf.get("average_price"),
                                    wf.get("filled_quantity"),
                                )
                                return {
                                    "success": True,
                                    "order_id": oid,
                                    "method": "limit_bid_ask",
                                    "average_fill_price": wf.get("average_price"),
                                    "filled_quantity": wf.get("filled_quantity"),
                                    "limit_price": lim_first.get("limit_price"),
                                    "limit_response": lim_first,
                                }
                            cancel_open_buy_if_pending(str(oid))
                            logger.warning(
                                "Limit-first not filled for %s (%s) within %ss (%s); trying MARKET",
                                stock_name,
                                option_contract,
                                int(ENTRY_FILL_WAIT_SEC),
                                wf.get("error") or "no_fill",
                            )
                        else:
                            logger.warning(
                                "Limit-first failed for %s (%s): %s; trying MARKET",
                                stock_name,
                                option_contract,
                                lim_first.get("error"),
                            )
                    else:
                        logger.info(
                            "Limit-first skipped for %s: spread_pct=%.4f%% > %.1f%%",
                            stock_name,
                            sp,
                            entry_spread_max_pct,
                        )

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
    m_err = market_res.get("error") or "Market BUY failed"
    market_accepted_no_fill = False
    if market_res.get("success"):
        oid = market_res.get("order_id")
        logger.warning(
            "🚨 LIVE MARKET ENTRY: %s %s | buy_order_id=%s | method=market (awaiting fill)",
            stock_name,
            option_contract,
            oid,
        )
        wf = wait_for_buy_order_fill(str(oid), qty, timeout_sec=MARKET_FILL_WAIT_SEC)
        if wf.get("filled"):
            return {
                "success": True,
                "order_id": oid,
                "method": "market",
                "average_fill_price": wf.get("average_price"),
                "filled_quantity": wf.get("filled_quantity"),
                "market_response": market_res,
            }
        cancel_open_buy_if_pending(str(oid))
        m_err = wf.get("error") or "Market BUY not filled in time"
        market_accepted_no_fill = True
        logger.warning(
            "Market BUY not filled for %s (%s): %s; will try LIMIT fallback if applicable",
            stock_name,
            option_contract,
            m_err,
        )

    if market_res.get("skipped"):
        return market_res

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
                "🚨 LIVE ENTRY via LIMIT (market disallowed on scrip): %s %s | buy_order_id=%s (awaiting fill)",
                stock_name,
                option_contract,
                oid,
            )
            wf = wait_for_buy_order_fill(str(oid), qty, timeout_sec=ENTRY_FILL_WAIT_SEC)
            if wf.get("filled"):
                return {
                    "success": True,
                    "order_id": oid,
                    "method": "limit_after_market_disallowed",
                    "average_fill_price": wf.get("average_price"),
                    "filled_quantity": wf.get("filled_quantity"),
                    "market_error": m_err,
                    "limit_price": limit_res.get("limit_price"),
                    "limit_response": limit_res,
                }
            cancel_open_buy_if_pending(str(oid))
            m_err = f"{m_err}; limit fallback not filled: {wf.get('error') or 'timeout'}"
        else:
            m_err = f"{m_err}; limit fallback failed: {limit_res.get('error') or 'unknown'}"
    elif market_accepted_no_fill:
        limit_tag = f"entry_lmt_mktunfilled|{stock_name}"[:200]
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
                "🚨 LIVE LIMIT after market unfilled: %s %s | buy_order_id=%s (awaiting fill)",
                stock_name,
                option_contract,
                oid,
            )
            wf = wait_for_buy_order_fill(str(oid), qty, timeout_sec=ENTRY_FILL_WAIT_SEC)
            if wf.get("filled"):
                return {
                    "success": True,
                    "order_id": oid,
                    "method": "limit_after_market_unfilled",
                    "average_fill_price": wf.get("average_price"),
                    "filled_quantity": wf.get("filled_quantity"),
                    "market_error": m_err,
                    "limit_price": limit_res.get("limit_price"),
                    "limit_response": limit_res,
                }
            cancel_open_buy_if_pending(str(oid))
            m_err = f"{m_err}; limit-after-market not filled: {wf.get('error') or 'timeout'}"
        else:
            m_err = f"{m_err}; limit-after-market failed: {limit_res.get('error') or 'unknown'}"

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

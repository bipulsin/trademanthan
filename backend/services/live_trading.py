import logging
import json
import math
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, List

from backend.services.upstox_service import upstox_service

logger = logging.getLogger(__name__)

# Upstox `product` on v2/order/place: D = Delivery (CNC equity / NRML-style carry for F&O), I = Intraday (MIS).
# All live MARKET/LIMIT option orders must use Delivery (D), not Intraday (I).
UPSTOX_ORDER_PRODUCT = "D"

# Serialize live exits per (instrument_key, buy_order_id) to prevent duplicate MARKET SELLs when
# multiple schedulers fire (hourly refresh + VWAP cycle) or overlapping HTTP handlers.
_exit_lock_guard = threading.Lock()
_exit_locks: Dict[str, threading.Lock] = {}

# Throttle broker reconcile for scan.html (GET /scan/latest)
_reconcile_scan_last_mono: float = 0.0


def _live_exit_lock_key(instrument_key: str, buy_order_id: str) -> str:
    return f"{(instrument_key or '').strip().upper()}|{(buy_order_id or '').strip()}"


def _acquire_live_exit_lock(key: str) -> threading.Lock:
    with _exit_lock_guard:
        if key not in _exit_locks:
            _exit_locks[key] = threading.Lock()
        return _exit_locks[key]


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


def _normalize_instrument_key_for_match(s: Optional[str]) -> str:
    """Normalize Upstox keys: pipe/slash/colon variants (see upstox_service market-quote matching)."""
    if not s:
        return ""
    t = str(s).strip().replace("/", "|").replace(":", "|")
    return t.upper()


def _instrument_key_suffix_token(norm: str) -> str:
    """Segment after last | or : — matches NSE_FO|84833 to bare 84833 from API."""
    if not norm:
        return ""
    for sep in ("|", ":"):
        if sep in norm:
            return norm.split(sep)[-1].strip()
    return norm.strip()


def _instrument_key_match(stored: Optional[str], api_instrument_token: Any) -> bool:
    """Match DB instrument_key to Upstox order instrument_token (pipe/slash/colon/suffix)."""
    a = _normalize_instrument_key_for_match(stored)
    b = _normalize_instrument_key_for_match(api_instrument_token)
    if a and b and a == b:
        return True
    sa = _instrument_key_suffix_token(a)
    sb = _instrument_key_suffix_token(b)
    if sa and sb and sa == sb:
        return True
    return False


def clamp_option_stop_loss_below_buy(trade) -> bool:
    """
    Long option: stop_loss must be strictly below entry. Fixes rows where SL was set from
    candle open / prev-day low that lies above the actual fill (e.g. buy 6.93 vs SL from open 15).
    """
    from sqlalchemy.orm.attributes import flag_modified

    bp = float(getattr(trade, "buy_price", None) or 0)
    if bp <= 0:
        return False
    sl = getattr(trade, "stop_loss", None)
    if sl is None:
        return False
    try:
        slf = float(sl)
    except (TypeError, ValueError):
        return False
    if slf < bp * 0.999:
        return False
    trade.stop_loss = round(bp * 0.95, 2)
    flag_modified(trade, "stop_loss")
    logger.info(
        "📌 SL clamp: %s stop_loss invalid vs buy ₹%.2f → SL ₹%.2f",
        getattr(trade, "stock_name", "?"),
        bp,
        trade.stop_loss,
    )
    return True


def _order_row_instrument_token(row: Optional[Dict[str, Any]]) -> str:
    """Upstox v2 order rows may expose instrument_token or instrument_key."""
    if not row or not isinstance(row, dict):
        return ""
    return str(row.get("instrument_token") or row.get("instrument_key") or "").strip()


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
    product: str = UPSTOX_ORDER_PRODUCT,
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
    product: str = UPSTOX_ORDER_PRODUCT,
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
    product: str = UPSTOX_ORDER_PRODUCT,
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
            if not _instrument_key_match(
                getattr(trade, "instrument_key", None), _order_row_instrument_token(o)
            ):
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
        buy_missing = float(trade.buy_price or 0) < 0.005
        if buy_naive and not buy_missing:
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


def final_reconciliation_refresh_trade_from_broker(db, trade) -> bool:
    """
    End-of-day broker alignment (3:45 / 4:00 PM jobs): refresh buy_price and sell_price from
    today's Upstox order book / order details, then PnL. Does not require trading_live YES.

    For no_entry: tries apply_broker_buy_fill_to_intraday_trade first (same-day BUY on Upstox),
    then continues as bought/sold. cancelled: same (recover rows that filled on broker but app shows cancelled).
    exit_reason (e.g. Exit-Slip) does not block refresh.
    """
    from sqlalchemy.orm.attributes import flag_modified

    _ = db
    if not upstox_service or not getattr(upstox_service, "access_token", None):
        return False
    st = getattr(trade, "status", None)
    if st == "no_entry":
        apply_broker_buy_fill_to_intraday_trade(db, trade)
        st = getattr(trade, "status", None)
    if st == "no_entry":
        return False
    ikey = (getattr(trade, "instrument_key", None) or "").strip()
    if not ikey:
        return False
    qty = int(getattr(trade, "qty", None) or 0)
    if qty <= 0:
        return False

    changed = False

    if st in ("bought", "sold", "cancelled"):
        if _final_recon_sync_buy_avg_from_broker(trade, qty):
            changed = True
            st = getattr(trade, "status", None)

    if st == "sold":
        if _final_recon_sync_sell_avg_from_broker(trade, qty):
            changed = True

    if trade.buy_price and trade.sell_price is not None and trade.qty:
        try:
            trade.pnl = round(
                (float(trade.sell_price) - float(trade.buy_price)) * int(trade.qty),
                2,
            )
            flag_modified(trade, "pnl")
        except (TypeError, ValueError):
            pass

    if clamp_option_stop_loss_below_buy(trade):
        changed = True

    return changed


def _final_recon_sync_buy_avg_from_broker(trade, qty: int) -> bool:
    """Like sync_trade_buy_fill_from_broker but no trading_live gate; status bought, sold, or cancelled."""
    from sqlalchemy.orm.attributes import flag_modified
    import pytz

    if getattr(trade, "status", None) not in ("bought", "sold", "cancelled"):
        return False

    ikey = (getattr(trade, "instrument_key", None) or "").strip()

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

    if row is None and orders_list and ikey:
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
            if not _instrument_key_match(ikey, _order_row_instrument_token(o)):
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

        if candidates:
            candidates.sort(key=_ob_ts, reverse=True)
            buy_missing = float(trade.buy_price or 0) < 0.005
            if buy_naive and not buy_missing:
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
    old = float(trade.buy_price or 0)
    updated = False

    if not oid_stored and fill_oid:
        trade.buy_order_id = fill_oid
        flag_modified(trade, "buy_order_id")
        updated = True

    price_changed = abs(old - new_avg) >= 0.005 or (old < 0.005 and new_avg > 0.005)
    if price_changed:
        trade.buy_price = round(new_avg, 2)
        if getattr(trade, "status", None) in ("bought", "cancelled"):
            trade.option_ltp = trade.buy_price
        if getattr(trade, "status", None) == "cancelled":
            trade.status = "bought"
            trade.no_entry_reason = None
            flag_modified(trade, "status")
            flag_modified(trade, "no_entry_reason")
        if trade.stop_loss and float(trade.stop_loss) >= float(trade.buy_price) * 0.999:
            trade.stop_loss = round(float(trade.buy_price) * 0.95, 2)
            flag_modified(trade, "stop_loss")
        flag_modified(trade, "buy_price")
        if getattr(trade, "status", None) == "bought":
            flag_modified(trade, "option_ltp")
        logger.info(
            "📌 Final recon BUY: %s buy_price ₹%.2f → ₹%.2f",
            getattr(trade, "stock_name", "?"),
            old,
            new_avg,
        )
        updated = True

    return updated


def _final_recon_sync_sell_avg_from_broker(trade, qty: int) -> bool:
    """Like sync_manual_exit sell sync but any sold row; GTT-safe order details."""
    from sqlalchemy.orm.attributes import flag_modified
    import pytz

    if getattr(trade, "status", None) != "sold":
        return False
    ikey = (getattr(trade, "instrument_key", None) or "").strip()
    if not ikey:
        return False

    new_sell: Optional[float] = None
    sell_oid = (getattr(trade, "sell_order_id", None) or "").strip()

    if sell_oid and not sell_oid.upper().startswith("GTT"):
        det = upstox_service.get_order_details(sell_oid)
        cand = _row_from_order_details_api(det)
        if cand and _order_row_is_complete_sell(cand):
            fq = int(float(cand.get("filled_quantity") or 0))
            if fq >= qty * 0.99:
                new_sell = _broker_buy_row_average_price(cand)

    ob = upstox_service.get_order_book_today()
    orders_list: List[Dict[str, Any]] = [
        o for o in (ob.get("orders") or []) if isinstance(o, dict)
    ]

    if new_sell is None and sell_oid and orders_list:
        for o in orders_list:
            if str(o.get("order_id", "")) != str(sell_oid):
                continue
            if _order_row_is_complete_sell(o):
                ap = _broker_buy_row_average_price(o)
                fq = int(float(o.get("filled_quantity") or 0))
                if ap and ap > 0 and fq >= qty * 0.99:
                    new_sell = ap
                    break

    if new_sell is None and orders_list:
        ist = pytz.timezone("Asia/Kolkata")
        sell_time = getattr(trade, "sell_time", None)
        if sell_time:
            if sell_time.tzinfo is None:
                sell_time = ist.localize(sell_time)
            else:
                sell_time = sell_time.astimezone(ist)
        sell_naive = sell_time.replace(tzinfo=None) if sell_time else None

        def _ob_ts(o: Dict[str, Any]) -> datetime:
            s = o.get("order_timestamp") or o.get("exchange_timestamp") or ""
            try:
                return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                return datetime.min

        candidates: List[Dict[str, Any]] = []
        for o in orders_list:
            if not _instrument_key_match(ikey, _order_row_instrument_token(o)):
                continue
            if (o.get("transaction_type") or "").upper() != "SELL":
                continue
            if not _order_row_is_complete_sell(o):
                continue
            ap = _broker_buy_row_average_price(o)
            if not ap:
                continue
            fq = int(float(o.get("filled_quantity") or 0))
            if fq < qty * 0.99:
                continue
            candidates.append(o)

        if candidates:
            # Wrong sell_time + sell_price=0 would narrow away the real fill; skip narrow then.
            sell_missing = float(trade.sell_price or 0) < 0.005
            if sell_naive and not sell_missing:
                narrowed = [
                    c
                    for c in candidates
                    if _ob_ts(c) >= sell_naive - timedelta(minutes=180)
                ]
                if narrowed:
                    candidates = narrowed
            candidates.sort(key=_ob_ts, reverse=True)
            best = candidates[0]
            new_sell = _broker_buy_row_average_price(best)

    if new_sell is None or new_sell <= 0:
        return False

    old = float(trade.sell_price or 0)
    if abs(old - new_sell) < 0.005:
        return False

    trade.sell_price = round(new_sell, 2)
    trade.option_ltp = trade.sell_price
    if trade.buy_price and trade.qty:
        trade.pnl = round(
            (float(trade.sell_price) - float(trade.buy_price)) * int(trade.qty),
            2,
        )
        flag_modified(trade, "pnl")
    flag_modified(trade, "sell_price")
    flag_modified(trade, "option_ltp")
    logger.info(
        "📌 Final recon SELL: %s sell_price ₹%.2f → ₹%.2f (broker)",
        getattr(trade, "stock_name", "?"),
        old,
        new_sell,
    )
    return True


def _broker_net_long_qty_for_positions(
    positions: List[Dict[str, Any]], instrument_key: str
) -> int:
    """Sum quantity for a short-term position row matching instrument_token (Upstox)."""
    total = 0
    ik = (instrument_key or "").strip()
    for row in positions:
        if not isinstance(row, dict):
            continue
        tok = (row.get("instrument_token") or row.get("instrument_key") or "").strip()
        if tok != ik:
            continue
        try:
            total += int(float(row.get("quantity") or 0))
        except (TypeError, ValueError):
            continue
    return total


def get_broker_net_long_qty_for_instrument(instrument_key: str) -> Optional[int]:
    """
    Net long quantity for this instrument from Upstox short-term positions.
    Returns None if the positions API failed (caller may still attempt exit).
    """
    if not upstox_service or not getattr(upstox_service, "access_token", None):
        return None
    pos_res = upstox_service.get_short_term_positions()
    if not pos_res.get("success"):
        logger.warning(
            "get_broker_net_long_qty: short-term positions failed: %s",
            pos_res.get("error"),
        )
        return None
    positions = [p for p in (pos_res.get("positions") or []) if isinstance(p, dict)]
    return _broker_net_long_qty_for_positions(positions, instrument_key)


def _load_instrument_dict_by_key(instrument_key: str) -> Optional[Dict[str, Any]]:
    """Return one instruments.json row for this Upstox instrument_key (NSE_FO|…)."""
    try:
        import json
        from backend.config import get_instruments_file_path

        p = get_instruments_file_path()
        if not p.exists():
            return None
        with open(p, "r") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return None
        want = (instrument_key or "").strip()
        for inst in data:
            if not isinstance(inst, dict):
                continue
            if (inst.get("instrument_key") or "").strip() == want:
                return inst
    except Exception:
        return None
    return None


def _broker_short_term_position_row_is_option_leg(pos: Dict[str, Any]) -> bool:
    """
    True if this Upstox short-term position row is an F&O option (CE/PE), not a future (FUT).

    Scan reconciliation only aligns intraday option rows; futures must not create orphans or
    affect qty aggregation for the same book.
    """
    if not isinstance(pos, dict):
        return False
    ikey = (pos.get("instrument_token") or pos.get("instrument_key") or "").strip()
    if not ikey:
        return False
    u = ikey.upper()
    if "NSE_FO" not in u and "BSE_FO" not in u:
        return False
    inst = _load_instrument_dict_by_key(ikey)
    if isinstance(inst, dict):
        it = str(inst.get("instrument_type") or "").strip().upper()
        if it == "FUT":
            return False
        if it in ("CE", "PE"):
            return True
    tsym = str(pos.get("trading_symbol") or "").strip().upper()
    if not tsym and isinstance(inst, dict):
        tsym = str(inst.get("trading_symbol") or inst.get("tradingsymbol") or "").strip().upper()
    if tsym.endswith("FUT"):
        return False
    if tsym.endswith("CE") or tsym.endswith("PE"):
        return True
    return False


def apply_broker_buy_fill_to_intraday_trade(db, trade) -> bool:
    """
    Find today's completed BUY on Upstox for this row's instrument_key (or buy_order_id),
    then set status=bought, buy_price (average fill), buy_time, buy_order_id, option_ltp,
    qty when missing on the row, stop_loss default 5% below buy, pnl=0, clear no_entry_reason.

    Use when the broker filled the order but the app row is still no_entry / alert_received
    or needs alignment with the actual fill. Skips when status is already sold.
    Does not require trading_live YES — only a valid API token.
    """
    from sqlalchemy.orm.attributes import flag_modified
    import pytz

    _ = db

    if not upstox_service or not getattr(upstox_service, "access_token", None):
        return False
    if getattr(trade, "status", None) == "sold":
        return False
    ikey = (getattr(trade, "instrument_key", None) or "").strip()
    if not ikey:
        return False

    qty_target = int(getattr(trade, "qty", None) or 0)

    row: Optional[Dict[str, Any]] = None
    oid = (getattr(trade, "buy_order_id", None) or "").strip()

    # GTT parent ids are not valid for v2/order/details — use order book below
    if oid and not oid.upper().startswith("GTT"):
        det = upstox_service.get_order_details(oid)
        cand = _row_from_order_details_api(det)
        if cand and _order_row_is_complete_buy(cand) and _broker_buy_row_average_price(cand):
            fq = int(float(cand.get("filled_quantity") or 0))
            if qty_target <= 0 or fq >= qty_target * 0.99:
                row = cand

    ob = upstox_service.get_order_book_today()
    orders_list: List[Dict[str, Any]] = [
        o for o in (ob.get("orders") or []) if isinstance(o, dict)
    ]

    if row is None and oid and orders_list:
        for o in orders_list:
            if str(o.get("order_id", "")) != str(oid):
                continue
            if _order_row_is_complete_buy(o) and _broker_buy_row_average_price(o):
                fq = int(float(o.get("filled_quantity") or 0))
                if qty_target <= 0 or fq >= qty_target * 0.99:
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
            if not _instrument_key_match(ikey, _order_row_instrument_token(o)):
                continue
            if (o.get("transaction_type") or "").upper() != "BUY":
                continue
            if not _order_row_is_complete_buy(o):
                continue
            ap = _broker_buy_row_average_price(o)
            if not ap:
                continue
            fq = int(float(o.get("filled_quantity") or 0))
            if qty_target > 0 and fq < qty_target * 0.99:
                continue
            candidates.append(o)

        if candidates:
            candidates.sort(key=_ob_ts, reverse=True)
            buy_missing = float(getattr(trade, "buy_price", None) or 0) < 0.005
            if buy_naive and not buy_missing:
                narrowed = [
                    c
                    for c in candidates
                    if _ob_ts(c) >= buy_naive - timedelta(minutes=15)
                ]
                if narrowed:
                    candidates = narrowed
            row = candidates[0]

    if row is None:
        return False

    new_avg = _broker_buy_row_average_price(row)
    if not new_avg or new_avg <= 0:
        return False

    fill_oid = str(row.get("order_id") or row.get("order_ref_id") or "").strip()
    fq = int(float(row.get("filled_quantity") or 0))
    if fq <= 0:
        return False

    if qty_target <= 0:
        trade.qty = fq
        flag_modified(trade, "qty")

    ts = row.get("order_timestamp") or row.get("exchange_timestamp") or ""
    ist = pytz.timezone("Asia/Kolkata")
    try:
        buy_dt = datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S")
        buy_dt = ist.localize(buy_dt)
    except (TypeError, ValueError):
        buy_dt = datetime.now(ist)

    trade.status = "bought"
    trade.buy_price = round(new_avg, 2)
    trade.option_ltp = trade.buy_price
    if fill_oid:
        trade.buy_order_id = fill_oid
    trade.buy_time = buy_dt
    trade.pnl = 0.0
    trade.no_entry_reason = None
    trade.exit_reason = None
    if not getattr(trade, "stop_loss", None) or float(trade.stop_loss or 0) <= 0:
        trade.stop_loss = round(float(trade.buy_price) * 0.95, 2)
        flag_modified(trade, "stop_loss")
    elif float(trade.stop_loss) >= float(trade.buy_price) * 0.999:
        trade.stop_loss = round(float(trade.buy_price) * 0.95, 2)
        flag_modified(trade, "stop_loss")

    flag_modified(trade, "status")
    flag_modified(trade, "buy_price")
    flag_modified(trade, "option_ltp")
    flag_modified(trade, "buy_order_id")
    flag_modified(trade, "buy_time")
    flag_modified(trade, "pnl")
    flag_modified(trade, "no_entry_reason")
    flag_modified(trade, "exit_reason")

    logger.info(
        "📌 apply_broker_buy_fill: %s marked bought @ ₹%.2f qty=%s order_id=%s",
        getattr(trade, "stock_name", "?"),
        new_avg,
        trade.qty,
        fill_oid or "none",
    )
    return True


def _order_row_is_complete_sell(row: Dict[str, Any]) -> bool:
    if (row.get("transaction_type") or "").upper() != "SELL":
        return False
    st = (row.get("status") or "").lower().strip()
    if "cancel" in st or "reject" in st:
        return False
    if st in ("complete", "filled"):
        return True
    return "complete" in st


def sync_manual_exit_sell_price_from_broker(db, trade) -> bool:
    """
    For exit_reason='manual', refresh sell_price and pnl from Upstox SELL fill average.
    Uses sell_order_id via get_order_details when set; else matches today's order book
    (same instrument_key, SELL, filled qty).
    Does not require trading_live YES — only a valid API token.
    """
    from sqlalchemy.orm.attributes import flag_modified
    import pytz

    _ = db

    if not upstox_service or not getattr(upstox_service, "access_token", None):
        return False
    if getattr(trade, "exit_reason", None) != "manual":
        return False
    if getattr(trade, "status", None) != "sold":
        return False
    qty = int(getattr(trade, "qty", None) or 0)
    if qty <= 0:
        return False
    ikey = (getattr(trade, "instrument_key", None) or "").strip()
    if not ikey:
        return False

    new_sell: Optional[float] = None
    sell_oid = (getattr(trade, "sell_order_id", None) or "").strip()

    if sell_oid:
        det = upstox_service.get_order_details(sell_oid)
        cand = _row_from_order_details_api(det)
        if cand and _order_row_is_complete_sell(cand):
            fq = int(float(cand.get("filled_quantity") or 0))
            if fq >= qty * 0.99:
                new_sell = _broker_buy_row_average_price(cand)

    ob = upstox_service.get_order_book_today()
    orders_list: List[Dict[str, Any]] = [
        o for o in (ob.get("orders") or []) if isinstance(o, dict)
    ]

    if new_sell is None and sell_oid and orders_list:
        for o in orders_list:
            if str(o.get("order_id", "")) != str(sell_oid):
                continue
            if _order_row_is_complete_sell(o):
                ap = _broker_buy_row_average_price(o)
                fq = int(float(o.get("filled_quantity") or 0))
                if ap and ap > 0 and fq >= qty * 0.99:
                    new_sell = ap
                    break

    if new_sell is None and orders_list:
        ist = pytz.timezone("Asia/Kolkata")
        sell_time = getattr(trade, "sell_time", None)
        if sell_time:
            if sell_time.tzinfo is None:
                sell_time = ist.localize(sell_time)
            else:
                sell_time = sell_time.astimezone(ist)
        sell_naive = sell_time.replace(tzinfo=None) if sell_time else None

        def _ob_ts(o: Dict[str, Any]) -> datetime:
            s = o.get("order_timestamp") or o.get("exchange_timestamp") or ""
            try:
                return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                return datetime.min

        candidates: List[Dict[str, Any]] = []
        for o in orders_list:
            if not _instrument_key_match(ikey, _order_row_instrument_token(o)):
                continue
            if (o.get("transaction_type") or "").upper() != "SELL":
                continue
            if not _order_row_is_complete_sell(o):
                continue
            ap = _broker_buy_row_average_price(o)
            if not ap:
                continue
            fq = int(float(o.get("filled_quantity") or 0))
            if fq < qty * 0.99:
                continue
            candidates.append(o)

        if candidates:
            if sell_naive:
                narrowed = [
                    c
                    for c in candidates
                    if _ob_ts(c) >= sell_naive - timedelta(minutes=180)
                ]
                if narrowed:
                    candidates = narrowed
            candidates.sort(key=_ob_ts, reverse=True)
            best = candidates[0]
            new_sell = _broker_buy_row_average_price(best)

    if new_sell is None or new_sell <= 0:
        return False

    old = float(trade.sell_price or 0)
    if abs(old - new_sell) < 0.005:
        return False

    trade.sell_price = round(new_sell, 2)
    trade.option_ltp = trade.sell_price
    if trade.buy_price and trade.qty:
        trade.pnl = round(
            (float(trade.sell_price) - float(trade.buy_price)) * int(trade.qty),
            2,
        )
        flag_modified(trade, "pnl")
    flag_modified(trade, "sell_price")
    flag_modified(trade, "option_ltp")
    logger.info(
        "📌 Manual exit sell sync: %s sell_price ₹%.2f → ₹%.2f (broker)",
        getattr(trade, "stock_name", "?"),
        old,
        new_sell,
    )
    return True


def reconcile_intraday_exit_from_broker(
    db,
    trade,
    default_exit_reason: str = "stock_vwap_cross",
) -> bool:
    """
    Restore or refresh exit from Upstox when a row was wrongly set back to 'bought' (e.g. after
    /process-all-today-stocks) or when sell_price needs alignment with the broker.

    - If status is 'bought' and a completed SELL exists for this instrument/qty: set sold,
      sell_price, sell_time, sell_order_id, pnl. Preserves exit_reason if already set; else uses
      default_exit_reason (typically stop_loss or stock_vwap_cross from caller).
    - If status is already 'sold': only updates sell_price/pnl if broker average differs (any exit_reason).

    Does not require trading_live YES — only API token.
    """
    from sqlalchemy.orm.attributes import flag_modified
    import pytz

    _ = db

    if not upstox_service or not getattr(upstox_service, "access_token", None):
        return False
    ikey = (getattr(trade, "instrument_key", None) or "").strip()
    if not ikey:
        return False
    qty = int(getattr(trade, "qty", None) or 0)
    if qty <= 0:
        return False
    st = getattr(trade, "status", None)
    if st not in ("bought", "sold"):
        return False

    new_sell: Optional[float] = None
    best_row: Optional[Dict[str, Any]] = None
    sell_oid = (getattr(trade, "sell_order_id", None) or "").strip()

    if sell_oid and not sell_oid.upper().startswith("GTT"):
        det = upstox_service.get_order_details(sell_oid)
        cand = _row_from_order_details_api(det)
        if (
            cand
            and _order_row_is_complete_sell(cand)
            and _instrument_key_match(ikey, _order_row_instrument_token(cand))
        ):
            fq = int(float(cand.get("filled_quantity") or 0))
            if fq >= qty * 0.99:
                ap = _broker_buy_row_average_price(cand)
                if ap and ap > 0:
                    new_sell = ap
                    best_row = cand

    ob = upstox_service.get_order_book_today()
    orders_list: List[Dict[str, Any]] = [
        o for o in (ob.get("orders") or []) if isinstance(o, dict)
    ]

    if new_sell is None and sell_oid and orders_list:
        for o in orders_list:
            if str(o.get("order_id", "")) != str(sell_oid):
                continue
            if not _instrument_key_match(ikey, _order_row_instrument_token(o)):
                continue
            if _order_row_is_complete_sell(o):
                ap = _broker_buy_row_average_price(o)
                fq = int(float(o.get("filled_quantity") or 0))
                if ap and ap > 0 and fq >= qty * 0.99:
                    new_sell = ap
                    best_row = o
                    break

    if new_sell is None and orders_list:
        ist = pytz.timezone("Asia/Kolkata")
        sell_time = getattr(trade, "sell_time", None)
        if sell_time:
            if sell_time.tzinfo is None:
                sell_time = ist.localize(sell_time)
            else:
                sell_time = sell_time.astimezone(ist)
        sell_naive = sell_time.replace(tzinfo=None) if sell_time else None

        def _ob_ts(o: Dict[str, Any]) -> datetime:
            s = o.get("order_timestamp") or o.get("exchange_timestamp") or ""
            try:
                return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                return datetime.min

        candidates: List[Dict[str, Any]] = []
        for o in orders_list:
            if not _instrument_key_match(ikey, _order_row_instrument_token(o)):
                continue
            if (o.get("transaction_type") or "").upper() != "SELL":
                continue
            if not _order_row_is_complete_sell(o):
                continue
            ap = _broker_buy_row_average_price(o)
            if not ap:
                continue
            fq = int(float(o.get("filled_quantity") or 0))
            if fq < qty * 0.99:
                continue
            candidates.append(o)

        if candidates:
            if st == "bought":
                buy_time_bt = getattr(trade, "buy_time", None)
                if buy_time_bt:
                    if buy_time_bt.tzinfo is None:
                        buy_time_bt = ist.localize(buy_time_bt)
                    else:
                        buy_time_bt = buy_time_bt.astimezone(ist)
                    buy_naive_bt = buy_time_bt.replace(tzinfo=None)
                    after_buy = [
                        c
                        for c in candidates
                        if _ob_ts(c) >= buy_naive_bt - timedelta(minutes=2)
                    ]
                    if after_buy:
                        candidates = after_buy
            sell_missing = float(trade.sell_price or 0) < 0.005
            if sell_naive and not sell_missing:
                narrowed = [
                    c
                    for c in candidates
                    if _ob_ts(c) >= sell_naive - timedelta(minutes=180)
                ]
                if narrowed:
                    candidates = narrowed
            candidates.sort(key=_ob_ts, reverse=True)
            best = candidates[0]
            new_sell = _broker_buy_row_average_price(best)
            best_row = best

    if new_sell is None or new_sell <= 0:
        return False
    if st == "bought" and not best_row:
        return False

    ist = pytz.timezone("Asia/Kolkata")
    ts = (best_row or {}).get("order_timestamp") or (best_row or {}).get("exchange_timestamp") or ""
    try:
        sell_dt = datetime.strptime(str(ts)[:19], "%Y-%m-%d %H:%M:%S")
        sell_dt = ist.localize(sell_dt)
    except (TypeError, ValueError):
        sell_dt = datetime.now(ist)

    fill_sell_oid = str(
        (best_row or {}).get("order_id")
        or (best_row or {}).get("order_ref_id")
        or sell_oid
        or ""
    ).strip()

    if st == "sold":
        old = float(trade.sell_price or 0)
        if abs(old - new_sell) < 0.005:
            return False
        trade.sell_price = round(new_sell, 2)
        trade.option_ltp = trade.sell_price
        if trade.buy_price and trade.qty:
            trade.pnl = round(
                (float(trade.sell_price) - float(trade.buy_price)) * int(trade.qty),
                2,
            )
            flag_modified(trade, "pnl")
        flag_modified(trade, "sell_price")
        flag_modified(trade, "option_ltp")
        logger.info(
            "📌 Reconcile exit (sold): %s sell_price ₹%.2f → ₹%.2f (broker)",
            getattr(trade, "stock_name", "?"),
            old,
            new_sell,
        )
        return True

    # status == 'bought' but broker shows a SELL fill — close the row
    trade.status = "sold"
    trade.sell_price = round(new_sell, 2)
    trade.option_ltp = trade.sell_price
    trade.sell_time = sell_dt
    if fill_sell_oid:
        trade.sell_order_id = fill_sell_oid
    if not getattr(trade, "exit_reason", None):
        trade.exit_reason = (default_exit_reason or "stock_vwap_cross")[:50]
    if trade.buy_price and trade.qty:
        trade.pnl = round(
            (float(trade.sell_price) - float(trade.buy_price)) * int(trade.qty),
            2,
        )
        flag_modified(trade, "pnl")
    flag_modified(trade, "status")
    flag_modified(trade, "sell_price")
    flag_modified(trade, "option_ltp")
    flag_modified(trade, "sell_time")
    flag_modified(trade, "sell_order_id")
    flag_modified(trade, "exit_reason")
    flag_modified(trade, "no_entry_reason")
    logger.warning(
        "📌 Reconcile exit (bought→sold): %s broker SELL avg ₹%.2f order_id=%s exit_reason=%s",
        getattr(trade, "stock_name", "?"),
        new_sell,
        fill_sell_oid or "none",
        trade.exit_reason,
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
    Primary: always attempt DELIVERY LIMIT BUY first at intended buy_price.
    Poll until filled; if it stays open, cancel and fall through.
    Then MARKET BUY (poll for fill); if Upstox disallows market, try DELIVERY LIMIT BUY at LTP.
    If those fail: GTT backup when buy_price/stop_loss are valid (no immediate fill — caller may
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

    # Step 1 (always): Delivery LIMIT BUY first, using intended buy_price.
    # If not filled within wait window, cancel and fall through to MARKET.
    lim_tag = f"entry_lmt_primary|{stock_name}"[:200]
    lim_first = place_live_upstox_limit_buy_at_price(
        instrument_key=instrument_key,
        qty=qty,
        stock_name=stock_name,
        option_contract=option_contract,
        limit_price=buy_price,
        tag=lim_tag,
        product=UPSTOX_ORDER_PRODUCT,
    )
    if lim_first.get("skipped"):
        return lim_first
    if lim_first.get("success"):
        oid = lim_first.get("order_id")
        logger.warning(
            "🚨 LIVE LIMIT ENTRY (primary): %s %s | buy_order_id=%s | method=limit_first (awaiting fill)",
            stock_name,
            option_contract,
            oid,
        )
        wf = wait_for_buy_order_fill(str(oid), qty, timeout_sec=ENTRY_FILL_WAIT_SEC)
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
                "method": "limit_primary",
                "average_fill_price": wf.get("average_price"),
                "filled_quantity": wf.get("filled_quantity"),
                "limit_price": lim_first.get("limit_price"),
                "limit_response": lim_first,
            }
        cancel_open_buy_if_pending(str(oid))
        logger.warning(
            "Primary limit not filled for %s (%s) within %ss (%s); trying MARKET",
            stock_name,
            option_contract,
            int(ENTRY_FILL_WAIT_SEC),
            wf.get("error") or "no_fill",
        )
    else:
        logger.warning(
            "Primary limit failed for %s (%s): %s; trying MARKET",
            stock_name,
            option_contract,
            lim_first.get("error"),
        )

    market_tag = f"entry_mkt|{stock_name}"[:200]
    market_res = place_live_upstox_order(
        "BUY",
        instrument_key=instrument_key,
        qty=qty,
        stock_name=stock_name,
        option_contract=option_contract,
        tag=market_tag,
        product=UPSTOX_ORDER_PRODUCT,
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
            product=UPSTOX_ORDER_PRODUCT,
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
            product=UPSTOX_ORDER_PRODUCT,
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
        product=UPSTOX_ORDER_PRODUCT,
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
    tag: Optional[str] = None,
    existing_sell_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Exit: always use a MARKET SELL at the broker for the open option qty.

    - Market entry positions: `buy_order_id` is the exchange order id from the BUY; we pass it in the
      order tag for traceability (`exit|ref_buy:<id>`). Exit order id is the new SELL's order_id.
    - Legacy GTT-only rows (`GTT-...`): cancel GTT bundle, then market SELL (product D to match GTT).
    - If `existing_sell_order_id` is already set on the row, does not place another SELL (idempotent).
    - Concurrent exits for the same buy/instrument are serialized (in-process lock).
    """
    if not is_trading_live_enabled():
        return {"success": False, "skipped": True, "error": "Live trading disabled"}
    if not upstox_service:
        return {"success": False, "error": "Upstox service unavailable"}

    ik = (instrument_key or "").strip()
    if not ik:
        return {"success": False, "skipped": False, "error": "Missing instrument_key"}

    # Market SELL only needs instrument_key + qty; buy_order_id is for tags, GTT legacy, and dedupe locks.
    # Rows missing buy_order_id (sync gap / manual mark) must still be able to exit at VWAP/SL/TM.
    raw_buy = (buy_order_id or "").strip()
    if raw_buy:
        oid = raw_buy
    else:
        tail = ik[-24:] if len(ik) > 24 else ik
        oid = f"NOBUY|{tail}"
        logger.warning(
            "⚠️ LIVE EXIT: missing buy_order_id for %s %s — using synthetic ref for tag/lock; placing MARKET SELL anyway",
            stock_name,
            option_contract,
        )
    ex = (existing_sell_order_id or "").strip()
    if ex:
        logger.warning(
            "⏭️ LIVE EXIT skipped — sell_order_id already stored (%s); not placing another SELL | %s %s",
            ex,
            stock_name,
            option_contract,
        )
        return {
            "success": True,
            "order_id": ex,
            "skipped_duplicate": True,
            "ref_buy_order_id": oid,
            "exit_method": "already_had_sell_order_id",
        }

    def _finalize_sell_result(sell_result: Dict[str, Any], gtt_cancel: Any = None) -> Dict[str, Any]:
        if sell_result.get("skipped"):
            return sell_result
        amb = bool(sell_result.get("ambiguous"))
        err = (sell_result.get("error") or "").lower()
        exit_manually = amb or "no_response" in err
        out = dict(sell_result)
        out["exit_manually"] = exit_manually
        if gtt_cancel is not None:
            out["gtt_cancel"] = gtt_cancel
        return out

    lk = _acquire_live_exit_lock(_live_exit_lock_key(instrument_key, oid))
    with lk:
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
                product=UPSTOX_ORDER_PRODUCT,
            )
            if sell_result.get("success"):
                logger.warning(
                    "🚨 LIVE MARKET EXIT (GTT legacy): %s %s | sell_order_id=%s | ref_buy_order_id=%s",
                    stock_name,
                    option_contract,
                    sell_result.get("order_id"),
                    oid,
                )
                return {
                    "success": True,
                    "order_id": sell_result.get("order_id"),
                    "gtt_cancel": cancel_result,
                    "exit_method": "market_sell_after_gtt_cancel",
                    "exit_manually": False,
                }
            err = sell_result.get("error") or "Market SELL failed after GTT cancel"
            logger.error(
                "❌ GTT legacy exit: SELL failed for %s %s | err=%s",
                stock_name,
                option_contract,
                err,
            )
            return _finalize_sell_result(
                {
                    "success": False,
                    "error": err,
                    "sell": sell_result,
                },
                gtt_cancel=cancel_result,
            )

        # Primary path: market SELL (Delivery — must match BUY product at broker)
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
            product=UPSTOX_ORDER_PRODUCT,
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
                "exit_manually": False,
            }
        return _finalize_sell_result(sell_result)


def reconcile_scan_algo_today_with_broker(db, force: bool = False) -> Dict[str, Any]:
    """
    Align today's `intraday_stock_options` with Upstox short-term positions + order book
    (options only — futures positions are ignored).

    - Promotes no_entry / alert_received / cancelled when a same-day BUY exists (apply_broker_buy_fill).
    - Refreshes buy avg / ids for bought rows (final_reconciliation_refresh_trade_from_broker).
    - Syncs qty to broker net long when the row is open bought.
    - Inserts a minimal row for a long option position at Upstox with no matching DB row (orphan fill).

    Does not require trading_live. Commits on success when changes were made.
    """
    import pytz
    from sqlalchemy import and_
    from sqlalchemy.orm.attributes import flag_modified

    from backend.models.trading import IntradayStockOption

    out: Dict[str, Any] = {
        "success": True,
        "updated_rows": 0,
        "inserted_orphans": 0,
        "qty_synced": 0,
        "errors": [],
    }

    if not upstox_service or not getattr(upstox_service, "access_token", None):
        out["success"] = False
        out["skipped"] = True
        out["error"] = "no_upstox_token"
        return out

    import time as _time

    global _reconcile_scan_last_mono
    if not force and (_time.monotonic() - float(_reconcile_scan_last_mono)) < 25.0:
        out["skipped"] = True
        out["reason"] = "throttled"
        return out

    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)

    pos_res = upstox_service.get_short_term_positions()
    if not pos_res.get("success"):
        out["success"] = False
        out["error"] = pos_res.get("error") or "positions_failed"
        return out
    positions = [p for p in (pos_res.get("positions") or []) if isinstance(p, dict)]
    positions = [p for p in positions if _broker_short_term_position_row_is_option_leg(p)]

    rows = (
        db.query(IntradayStockOption)
        .filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.trade_date < tomorrow,
            )
        )
        .all()
    )

    touched = False

    for trade in rows:
        if getattr(trade, "status", None) == "sold":
            continue
        try:
            st = getattr(trade, "status", None)
            if st in ("no_entry", "alert_received", "cancelled"):
                if apply_broker_buy_fill_to_intraday_trade(db, trade):
                    touched = True
                    out["updated_rows"] += 1
            st2 = getattr(trade, "status", None)
            if st2 in ("bought", "cancelled") and getattr(trade, "exit_reason", None) is None:
                if final_reconciliation_refresh_trade_from_broker(db, trade):
                    touched = True
                    out["updated_rows"] += 1

            if (
                getattr(trade, "status", None) == "bought"
                and getattr(trade, "exit_reason", None) is None
            ):
                ikey = (getattr(trade, "instrument_key", None) or "").strip()
                if ikey and positions:
                    net = _broker_net_long_qty_for_positions(positions, ikey)
                    cq = int(getattr(trade, "qty", None) or 0)
                    if net > 0 and cq != net:
                        trade.qty = net
                        flag_modified(trade, "qty")
                        touched = True
                        out["qty_synced"] += 1
        except Exception as ex:
            out["errors"].append(str(ex)[:220])
            logger.warning("reconcile row id=%s: %s", getattr(trade, "id", "?"), ex)

    try:
        db.flush()
    except Exception:
        pass
    rows = (
        db.query(IntradayStockOption)
        .filter(
            and_(
                IntradayStockOption.trade_date >= today,
                IntradayStockOption.trade_date < tomorrow,
            )
        )
        .all()
    )

    # Orphan long option positions: broker has qty, DB has no row for this instrument_key
    def _row_exists_for_ikey(ikey: str) -> bool:
        for tr in rows:
            ik = (getattr(tr, "instrument_key", None) or "").strip()
            if ik and _instrument_key_match(ik, ikey):
                return True
        return False

    for pos in positions:
        ikey = (pos.get("instrument_token") or pos.get("instrument_key") or "").strip()
        if not ikey:
            continue
        u = ikey.upper()
        if "NSE_FO" not in u and "BSE_FO" not in u:
            continue
        try:
            qty_b = int(float(pos.get("quantity") or 0))
        except (TypeError, ValueError):
            continue
        if qty_b <= 0:
            continue
        if _row_exists_for_ikey(ikey):
            continue

        try:
            inst = _load_instrument_dict_by_key(ikey)
            tsym = (
                (inst or {}).get("trading_symbol")
                or (inst or {}).get("tradingsymbol")
                or pos.get("trading_symbol")
                or ""
            )
            tsym_u = str(tsym).strip().upper()
            underlying = (
                (inst or {}).get("underlying_symbol")
                or (inst or {}).get("underlying")
                or ""
            )
            if not (underlying or "").strip():
                underlying = tsym_u.split("-")[0] if tsym_u else "UNKNOWN"
            opt_type = "PE" if tsym_u.endswith("PE") else "CE"
            strike = (inst or {}).get("strike_price") or (inst or {}).get("strike") or 0.0
            try:
                strike_f = float(strike) if strike else 0.0
            except (TypeError, ValueError):
                strike_f = 0.0

            avg = 0.0
            for k in ("average_price", "avg_price", "average", "day_buy_average_price"):
                v = pos.get(k)
                if v is not None:
                    try:
                        avg = float(v)
                        if avg > 0:
                            break
                    except (TypeError, ValueError):
                        pass
            if avg <= 0:
                q = upstox_service.get_market_quote_by_key(ikey)
                if q and float(q.get("last_price") or 0) > 0:
                    avg = float(q.get("last_price"))

            rec = IntradayStockOption(
                alert_time=now,
                alert_type="Bullish" if opt_type == "CE" else "Bearish",
                scan_name="Broker reconciliation",
                stock_name=str(underlying)[:100],
                stock_ltp=0.0,
                stock_vwap=0.0,
                option_contract=(tsym or ikey)[:255],
                option_type=opt_type,
                option_strike=strike_f,
                option_ltp=round(avg, 2) if avg > 0 else None,
                qty=qty_b,
                trade_date=today,
                status="bought",
                buy_price=round(avg, 2) if avg > 0 else None,
                buy_time=now,
                buy_order_id=None,
                instrument_key=ikey,
                stop_loss=round(avg * 0.95, 2) if avg > 0 else None,
                sell_price=None,
                pnl=0.0,
                no_entry_reason=None,
                exit_reason=None,
            )
            db.add(rec)
            rows.append(rec)
            touched = True
            out["inserted_orphans"] += 1
            logger.warning(
                "📌 Reconcile: inserted orphan broker position %s qty=%s @ ₹%.2f",
                ikey,
                qty_b,
                avg,
            )
        except Exception as ex:
            out["errors"].append(f"orphan {ikey}: {str(ex)[:180]}")
            logger.warning("reconcile orphan %s: %s", ikey, ex)

    if touched:
        try:
            db.commit()
        except Exception as ex:
            db.rollback()
            out["success"] = False
            out["error"] = str(ex)
            return out

    # Successful run (positions API ok): throttle future calls
    _reconcile_scan_last_mono = _time.monotonic()
    return out

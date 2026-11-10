"""Sync one day's stock and index futures into trade_log.

Upstox and Dhan fills are kept only when they are NSE/BSE equity-stock futures
or index futures (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, BANKEX, and
other F&O futures). Equity cash, options, currency, and MCX/commodity fills
are dropped. A round trip becomes one trade_log row with entry and exit. A
single side is stored as an open trade (exit empty). A row already stored for
the same instrument, entry date-time, and entry price is not inserted again.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence

import pytz
from sqlalchemy import text

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_COMPLETE = {"complete", "completed", "traded", "filled", "success"}
_SKIP_STATUS = {"rejected", "cancelled", "canceled", "open", "pending", "trigger pending"}
_FNO_EXCHANGES = {"NSE_FNO", "BSE_FNO", "NSE_FO", "BSE_FO"}
_FO_SEGMENTS = {"FO", "FNO", "NSE_FO", "BSE_FO", "NSE_FNO", "BSE_FNO"}
_MONTHS = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC"
_CURRENCY_ROOTS = ("USDINR", "EURINR", "GBPINR", "JPYINR", "EURUSD", "GBPUSD", "USDJPY")
_DMY = re.compile(r"^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})")


class TradeLogBrokerSyncError(Exception):
    def __init__(self, message: str, *, login_url: Optional[str] = None):
        super().__init__(message)
        self.login_url = login_url


@dataclass
class Fill:
    instrument: str
    symbol_raw: str
    underlying: str
    side: str
    qty: int
    price: float
    ts: datetime
    fill_id: str


@dataclass
class PlannedTrade:
    instrument: str
    symbol_raw: str
    underlying: str
    direction: str
    qty: int
    entry_price: float
    entry_time: str
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    open_trade: bool = True


def parse_trade_date(raw: Any) -> date:
    """IST calendar day. YYYY-MM-DD, or DD/MM/YYYY so 09/11/2026 is 9 Nov."""
    text_raw = str(raw or "").strip()
    if not text_raw:
        raise TradeLogBrokerSyncError("Choose a date to sync.")
    parsed: Optional[date] = None
    iso_head = text_raw[:10]
    if len(iso_head) == 10 and iso_head[4] == "-" and iso_head[7] == "-":
        try:
            parsed = date.fromisoformat(iso_head)
        except ValueError:
            parsed = None
    if parsed is None:
        match = _DMY.match(text_raw)
        if match:
            day_n, month_n, year_n = int(match.group(1)), int(match.group(2)), int(match.group(3))
            try:
                parsed = date(year_n, month_n, day_n)
            except ValueError as exc:
                raise TradeLogBrokerSyncError("Choose a date as YYYY-MM-DD or DD/MM/YYYY.") from exc
    if parsed is None:
        raise TradeLogBrokerSyncError("Choose a date as YYYY-MM-DD or DD/MM/YYYY.")
    today = datetime.now(IST).date()
    if parsed > today:
        raise TradeLogBrokerSyncError("Choose a date that is not in the future.")
    return parsed


def normalize_broker(raw: Any) -> str:
    name = str(raw or "").strip().lower()
    if name in ("upstox", "dhan"):
        return name
    raise TradeLogBrokerSyncError("Choose Upstox or Dhan.")


def underlying_from_future_symbol(symbol: str) -> str:
    """RELIANCE25NOVFUT / NIFTY-Nov2026-FUT / NIFTY NOV FUT → underlying."""
    compact = re.sub(r"[^A-Z0-9]", "", (symbol or "").upper())
    compact = re.sub(r"FUT$", "", compact)
    if not compact:
        return ""
    matched = re.match(rf"^([A-Z]+?)\d{{1,2}}(?:{_MONTHS})\d{{0,4}}$", compact)
    if matched:
        return matched.group(1)
    matched = re.match(rf"^([A-Z]+?)(?:{_MONTHS})\d{{2,4}}$", compact)
    if matched:
        return matched.group(1)
    matched = re.match(rf"^([A-Z]+?)(?:{_MONTHS})$", compact)
    if matched:
        return matched.group(1)
    matched = re.match(r"^([A-Z]+)", compact)
    return matched.group(1) if matched else compact


def _first(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _num(raw: Any) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _upper(raw: Any) -> str:
    return str(raw or "").strip().upper()


def is_stock_or_index_future(row: Dict[str, Any], *, segment_hint: str = "") -> bool:
    """True for NSE/BSE stock futures and index futures. Drops the rest.

    Dhan uses exchangeSegment NSE_FNO / BSE_FNO and drvOptionType NA (not CALL/PUT).
    Upstox uses instrument keys NSE_FO|… / BSE_FO|… and segment FO, with a FUT symbol.
    Options (OPT*, CE/PE/CALL/PUT), equity cash, currency, and MCX are rejected.
    """
    if not isinstance(row, dict):
        return False
    exchange = _upper(_first(row, "exchange", "exchangeSegment", "exchange_segment"))
    segment = _upper(_first(row, "segment", "exchange_segment") or segment_hint)
    key = _upper(_first(row, "instrument_token", "instrument_key", "instrumentToken"))
    symbol = _upper(
        _first(
            row,
            "trading_symbol",
            "tradingsymbol",
            "tradingSymbol",
            "customSymbol",
            "symbol",
            "scrip_name",
        )
    )
    compact = re.sub(r"[^A-Z0-9]", "", symbol)
    option_type = _upper(_first(row, "drvOptionType", "option_type", "optionType"))
    instrument_type = _upper(
        _first(row, "instrument_type", "instrumentType", "drvInstrumentType", "security_type")
    )
    blob = f"{exchange} {segment} {key}"

    if "MCX" in blob or segment in {"COM", "MCX_COMM"} or exchange in {"MCX", "MCX_COMM", "MCX_FO"}:
        return False
    if any(part in blob for part in ("CURRENCY", "NSE_CDS", "BSE_CDS", "NCD_FO", "BCD_FO", "CDS_FO")):
        return False
    if segment in {"CD", "CDS", "CURRENCY"} or exchange in {"NSE_CURRENCY", "BSE_CURRENCY", "NSE_CURR"}:
        return False
    if exchange in {"NSE_EQ", "BSE_EQ"} or segment in {"EQ", "NSE_EQ", "BSE_EQ"}:
        return False
    if key.startswith("NSE_EQ") or key.startswith("BSE_EQ"):
        return False
    if option_type in {"CE", "PE", "CALL", "PUT"}:
        return False
    if instrument_type.startswith("OPT") or instrument_type in {"CE", "PE", "CALL", "PUT"}:
        return False
    if compact.endswith(("CE", "PE", "CALL", "PUT")):
        return False
    root = underlying_from_future_symbol(symbol)
    if root.startswith(_CURRENCY_ROOTS):
        return False

    fno = (
        exchange in _FNO_EXCHANGES
        or segment in _FO_SEGMENTS
        or key.startswith(("NSE_FO", "BSE_FO", "NSE_FNO", "BSE_FNO"))
        or (exchange in {"NSE", "BSE"} and segment in {"FO", "FNO"})
    )
    if not fno:
        return False
    if "FUT" in compact or instrument_type.startswith("FUT"):
        return True
    if option_type in {"", "NA", "XX", "NONE", "FUT"} and instrument_type in {
        "",
        "NA",
        "FUT",
        "FUTSTK",
        "FUTIDX",
        "FUTURE",
        "FUTURES",
    }:
        return True
    return False


def _parse_ts(raw: Any, trade_day: date) -> datetime:
    text_raw = str(raw or "").strip()
    if text_raw and text_raw.upper() != "NA":
        cleaned = text_raw.replace("T", " ")
        for fmt, width in (
            ("%Y-%m-%d %H:%M:%S", 19),
            ("%Y-%m-%d %H:%M", 16),
            ("%d-%m-%Y %H:%M:%S", 19),
            ("%d/%m/%Y %H:%M:%S", 19),
        ):
            try:
                return datetime.strptime(cleaned[:width], fmt)
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(text_raw.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(IST).replace(tzinfo=None)
            return parsed
        except ValueError:
            pass
    return datetime(trade_day.year, trade_day.month, trade_day.day, 15, 30, 0)


def _on_trade_day(stamp: datetime, row: Dict[str, Any], trade_day: date) -> bool:
    row_day = str(_first(row, "trade_date") or "")[:10]
    if row_day and row_day != trade_day.isoformat():
        return False
    if stamp.date() != trade_day:
        if row_day == trade_day.isoformat():
            return True
        return False
    return True


def row_to_fill(row: Dict[str, Any], trade_day: date, *, segment_hint: str = "") -> Optional[Fill]:
    if not is_stock_or_index_future(row, segment_hint=segment_hint):
        return None
    status = _upper(_first(row, "status", "order_status", "orderStatus")).lower()
    traded_qty = _num(_first(row, "filled_quantity", "tradedQuantity"))
    if status in _SKIP_STATUS:
        return None
    if status and status not in _COMPLETE and "complete" not in status and traded_qty <= 0:
        return None
    side = _upper(_first(row, "transaction_type", "transactionType", "side"))
    if side not in ("BUY", "SELL"):
        return None
    qty = int(round(_num(_first(row, "filled_quantity", "tradedQuantity", "quantity", "qty"))))
    price = _num(_first(row, "average_price", "tradedPrice", "price", "trade_price", "averagePrice"))
    if qty <= 0 or price <= 0:
        return None
    symbol = str(
        _first(
            row,
            "trading_symbol",
            "tradingsymbol",
            "tradingSymbol",
            "customSymbol",
            "symbol",
            "scrip_name",
        )
        or ""
    ).strip()
    if not symbol:
        return None
    stamp = _parse_ts(
        _first(
            row,
            "order_timestamp",
            "exchange_timestamp",
            "trade_timestamp",
            "exchangeTime",
            "updateTime",
            "createTime",
            "trade_time",
        ),
        trade_day,
    )
    if not _on_trade_day(stamp, row, trade_day):
        return None
    fill_id = str(
        _first(row, "trade_id", "exchangeTradeId", "order_id", "orderId")
        or f"{stamp.isoformat()}|{side}|{qty}|{price}"
    ).strip()
    instrument = re.sub(r"[^A-Z0-9]", "", symbol.upper())
    underlying = underlying_from_future_symbol(symbol) or instrument
    return Fill(
        instrument=instrument,
        symbol_raw=symbol,
        underlying=underlying,
        side=side,
        qty=qty,
        price=round(price, 4),
        ts=stamp,
        fill_id=fill_id,
    )


def _vwap(chunks: Sequence[tuple]) -> float:
    qty = sum(int(chunk[0]) for chunk in chunks)
    if qty <= 0:
        return 0.0
    cost = sum(int(chunk[0]) * float(chunk[1]) for chunk in chunks)
    return round(cost / qty, 4)


def _clock(ts: datetime) -> str:
    return ts.strftime("%H:%M:%S")


def _pair_group(fills: Sequence[Fill]) -> List[PlannedTrade]:
    ordered = sorted(fills, key=lambda fill: (fill.ts, fill.fill_id, fill.side))
    if not ordered:
        return []
    sample = ordered[0]
    side: Optional[str] = None
    book: List[tuple] = []
    plans: List[PlannedTrade] = []

    def _make(
        *,
        direction: str,
        qty: int,
        entry_price: float,
        entry_ts: datetime,
        exit_price: Optional[float],
        exit_ts: Optional[datetime],
        open_trade: bool,
    ) -> PlannedTrade:
        return PlannedTrade(
            instrument=sample.instrument,
            symbol_raw=sample.symbol_raw,
            underlying=sample.underlying,
            direction=direction,
            qty=int(qty),
            entry_price=float(entry_price),
            entry_time=_clock(entry_ts),
            exit_price=None if open_trade else exit_price,
            exit_time=None if open_trade or exit_ts is None else _clock(exit_ts),
            open_trade=open_trade,
        )

    for fill in ordered:
        if side is None or not book:
            side = fill.side
            book.append((fill.qty, fill.price, fill.ts))
            continue
        if fill.side == side:
            book.append((fill.qty, fill.price, fill.ts))
            continue
        remaining = fill.qty
        entry_side = side
        consumed: List[tuple] = []
        while remaining > 0 and book:
            lot_qty, lot_price, lot_ts = book[0]
            take = min(int(lot_qty), remaining)
            consumed.append((take, lot_price, lot_ts))
            left = int(lot_qty) - take
            remaining -= take
            if left <= 0:
                book.pop(0)
            else:
                book[0] = (left, lot_price, lot_ts)
        if consumed:
            plans.append(
                _make(
                    direction="LONG" if entry_side == "BUY" else "SHORT",
                    qty=sum(int(chunk[0]) for chunk in consumed),
                    entry_price=_vwap(consumed),
                    entry_ts=min(chunk[2] for chunk in consumed),
                    exit_price=fill.price,
                    exit_ts=fill.ts,
                    open_trade=False,
                )
            )
        if remaining > 0:
            side = fill.side
            book.append((remaining, fill.price, fill.ts))
        elif not book:
            side = None
    if book and side:
        plans.append(
            _make(
                direction="LONG" if side == "BUY" else "SHORT",
                qty=sum(int(chunk[0]) for chunk in book),
                entry_price=_vwap(book),
                entry_ts=min(chunk[2] for chunk in book),
                exit_price=None,
                exit_ts=None,
                open_trade=True,
            )
        )
    return _disambiguate_entry_times(plans)


def _disambiguate_entry_times(plans: List[PlannedTrade]) -> List[PlannedTrade]:
    """trade_log is unique on date + symbol + side + entry time."""
    used = set()
    for plan in plans:
        clock = plan.entry_time
        for _ in range(60):
            key = (plan.underlying, plan.direction, clock)
            if key not in used:
                plan.entry_time = clock
                used.add(key)
                break
            hh, mm, ss = (int(part) for part in clock.split(":"))
            total = (hh * 3600 + mm * 60 + ss + 1) % 86400
            hour, rem = divmod(total, 3600)
            minute, sec = divmod(rem, 60)
            clock = f"{hour:02d}:{minute:02d}:{sec:02d}"
        else:
            used.add((plan.underlying, plan.direction, plan.entry_time))
    return plans


def _unique_fills(fills: Sequence[Fill]) -> List[Fill]:
    """Drop the same fill when today's trade book and order book both return it."""
    seen_ids = set()
    seen_minute = set()
    unique: List[Fill] = []
    for fill in fills:
        if fill.fill_id and fill.fill_id in seen_ids:
            continue
        minute_key = (
            fill.instrument,
            fill.side,
            fill.ts.strftime("%Y-%m-%d %H:%M"),
            round(float(fill.price), 2),
            int(fill.qty),
        )
        if minute_key in seen_minute:
            continue
        if fill.fill_id:
            seen_ids.add(fill.fill_id)
        seen_minute.add(minute_key)
        unique.append(fill)
    return unique


def plans_from_fills(fills: Sequence[Fill]) -> List[PlannedTrade]:
    grouped: Dict[str, List[Fill]] = {}
    for fill in fills:
        if fill.qty <= 0 or fill.price <= 0:
            continue
        grouped.setdefault(fill.instrument or fill.underlying, []).append(fill)
    plans: List[PlannedTrade] = []
    for instrument in sorted(grouped):
        plans.extend(_pair_group(grouped[instrument]))
    return plans


def _clock_text(val: Any) -> str:
    if val is None or val == "":
        return ""
    if isinstance(val, timedelta):
        total = int(val.total_seconds()) % 86400
        if total < 0:
            total += 86400
        hour, rem = divmod(total, 3600)
        minute, sec = divmod(rem, 60)
        return f"{hour:02d}:{minute:02d}:{sec:02d}"
    if isinstance(val, time):
        return val.strftime("%H:%M:%S")
    if isinstance(val, datetime):
        return val.strftime("%H:%M:%S")
    text_raw = str(val).strip()
    if "T" in text_raw:
        text_raw = text_raw.split("T", 1)[-1]
    text_raw = text_raw.split(".", 1)[0].split("+", 1)[0].replace("Z", "").strip()
    matched = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", text_raw)
    if not matched:
        return ""
    sec = matched.group(3) or "00"
    return f"{int(matched.group(1)):02d}:{int(matched.group(2)):02d}:{int(sec):02d}"


def _price_key(val: Any) -> Optional[float]:
    try:
        if val is None or val == "":
            return None
        return round(float(val), 4)
    except (TypeError, ValueError):
        return None


def _instrument_names(row: Dict[str, Any]) -> set:
    names = set()
    for key in ("symbol", "contract", "instrument"):
        raw = str(row.get(key) or "").strip()
        if not raw:
            continue
        compact = re.sub(r"[^A-Z0-9]", "", raw.upper())
        if compact:
            names.add(compact)
        root = underlying_from_future_symbol(raw)
        if root:
            names.add(root)
    return names


def is_duplicate(row: Dict[str, Any], plan: PlannedTrade, session_date: date) -> bool:
    """Same instrument, same entry date-time, and same entry price."""
    row_day = str(row.get("session_date") or "")[:10]
    if row_day and row_day != session_date.isoformat():
        return False
    if _clock_text(row.get("entry_time")) != plan.entry_time:
        return False
    row_price = _price_key(row.get("entry_price"))
    if row_price is None or row_price != _price_key(plan.entry_price):
        return False
    names = _instrument_names(row)
    wanted = {plan.instrument, plan.underlying, re.sub(r"[^A-Z0-9]", "", plan.symbol_raw.upper())}
    wanted.discard("")
    return bool(names.intersection(wanted))


def trade_payload(plan: PlannedTrade, session_date: date, broker: str) -> Dict[str, Any]:
    label = "Dhan" if broker == "dhan" else "Upstox"
    return {
        "session_date": session_date.isoformat(),
        "symbol": plan.underlying,
        "contract": plan.symbol_raw,
        "direction": plan.direction,
        "qty": int(plan.qty),
        "entry_time": plan.entry_time,
        "entry_price": float(plan.entry_price),
        "exit_time": None if plan.open_trade else plan.exit_time,
        "exit_price": None if plan.open_trade else plan.exit_price,
        "notes": f"Synced from {label}",
        "source": f"broker_{broker}",
    }


def apply_plans(
    plans: Sequence[PlannedTrade],
    existing: Sequence[Dict[str, Any]],
    session_date: date,
    broker: str,
    insert_fn: Callable[[Dict[str, Any]], None],
    update_fn: Optional[Callable[[Dict[str, Any], PlannedTrade], None]] = None,
) -> Dict[str, Any]:
    inserted = 0
    updated = 0
    skipped = 0
    open_trades = 0
    closed_trades = 0
    seen: List[Dict[str, Any]] = [dict(row) for row in existing]
    for plan in plans:
        duplicate = next((row for row in seen if is_duplicate(row, plan, session_date)), None)
        if duplicate is not None:
            can_close = (
                update_fn is not None
                and not plan.open_trade
                and duplicate.get("id") is not None
                and _price_key(duplicate.get("exit_price")) is None
                and not _clock_text(duplicate.get("exit_time"))
            )
            if can_close:
                update_fn(duplicate, plan)
                duplicate["exit_time"] = plan.exit_time
                duplicate["exit_price"] = plan.exit_price
                updated += 1
                closed_trades += 1
            else:
                skipped += 1
            continue
        payload = trade_payload(plan, session_date, broker)
        insert_fn(payload)
        inserted += 1
        seen.append(payload)
        if plan.open_trade:
            open_trades += 1
        else:
            closed_trades += 1
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "open_trades": open_trades,
        "closed_trades": closed_trades,
    }


def _summary(broker: str, trade_day: date, fill_count: int, applied: Dict[str, Any]) -> Dict[str, Any]:
    label = "Dhan" if broker == "dhan" else "Upstox"
    inserted = int(applied["inserted"])
    updated = int(applied["updated"])
    skipped = int(applied["skipped"])
    if fill_count <= 0:
        message = f"No stock or index futures on {trade_day.isoformat()} for {label}."
    elif inserted == 0 and updated == 0:
        message = (
            f"No new futures trades from {label} on {trade_day.isoformat()}. "
            f"{skipped} already in the trade log."
        )
    else:
        message = f"Synced {inserted} futures trade(s) from {label} for {trade_day.isoformat()}."
        if updated:
            message += f" {updated} open trade(s) closed with an exit."
        if skipped:
            message += f" {skipped} already in the trade log."
    return {
        "ok": True,
        "broker": broker,
        "date": trade_day.isoformat(),
        "futures_fills": fill_count,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "open_trades": int(applied["open_trades"]),
        "closed_trades": int(applied["closed_trades"]),
        "message": message.strip(),
    }


def _payload_rows(response: Any) -> List[Dict[str, Any]]:
    if not isinstance(response, dict):
        return []
    data = response.get("data")
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        for key in ("trades", "orders", "data"):
            nested = data.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _upstox_message(response: Any) -> str:
    if not isinstance(response, dict):
        return "Upstox trade request failed."
    message = response.get("message") or response.get("error") or ""
    errors = response.get("errors")
    if isinstance(errors, list) and errors and isinstance(errors[0], dict):
        message = errors[0].get("message") or message
    text_msg = str(message or "Upstox trade request failed.").strip()
    low = text_msg.lower()
    if any(part in low for part in ("token", "unauthor", "udapi", "expired", "invalid")):
        return "Upstox access token is missing or expired. Sign in to Upstox and try again."
    return text_msg[:400]


def _fetch_upstox(trade_day: date) -> List[Fill]:
    from backend.services.upstox_service import upstox_service

    ux = upstox_service
    if not getattr(ux, "access_token", None):
        try:
            ux.reload_token_from_storage()
        except Exception:
            logger.info("trade_log broker sync: upstox token reload failed")
    if not getattr(ux, "access_token", None):
        raise TradeLogBrokerSyncError(
            "Upstox access token is missing or expired. Sign in to Upstox and try again."
        )
    if trade_day == datetime.now(IST).date():
        response = ux.make_api_request(
            url="https://api.upstox.com/v2/order/trades/get-trades-for-day",
            method="GET",
            timeout=20,
        )
        trades_ok = isinstance(response, dict) and response.get("status") == "success"
        trade_rows = _payload_rows(response) if trades_ok else []
        book = ux.get_order_book_today()
        book_ok = isinstance(book, dict) and book.get("success")
        order_rows = book.get("orders") if book_ok and isinstance(book.get("orders"), list) else []
        if not trades_ok and not book_ok:
            err = book.get("error") if isinstance(book, dict) else None
            raise TradeLogBrokerSyncError(
                _upstox_message(response if isinstance(response, dict) else {"message": err or "Upstox order book failed"})
            )
        fills = [row_to_fill(row, trade_day) for row in list(trade_rows) + list(order_rows)]
        return [fill for fill in fills if fill]
    page = 1
    total_pages = 1
    raw_rows: List[Dict[str, Any]] = []
    while page <= total_pages and page <= 40:
        response = ux.make_api_request(
            url="https://api.upstox.com/v2/charges/historical-trades",
            method="GET",
            params={
                "segment": "FO",
                "start_date": trade_day.isoformat(),
                "end_date": trade_day.isoformat(),
                "page_number": page,
                "page_size": 500,
            },
            timeout=20,
        )
        if not isinstance(response, dict) or response.get("status") != "success":
            if page == 1 and not raw_rows:
                raise TradeLogBrokerSyncError(_upstox_message(response))
            break
        data = response.get("data") or []
        if isinstance(data, list):
            raw_rows.extend(item for item in data if isinstance(item, dict))
        meta = response.get("metadata") if isinstance(response.get("metadata"), dict) else {}
        page_meta = meta.get("page") if isinstance(meta.get("page"), dict) else {}
        try:
            total_pages = int(page_meta.get("total_pages") or 1)
        except (TypeError, ValueError):
            total_pages = 1
        page += 1
    fills = []
    for row in raw_rows:
        fill = row_to_fill(row, trade_day, segment_hint="FO")
        if fill:
            fills.append(fill)
    return fills


def _dhan_browser_login_url() -> Optional[str]:
    try:
        from backend.services.dhan_service import dhan_service

        return dhan_service.start_consent_login()
    except Exception:
        logger.info("trade_log broker sync: Dhan browser login fallback unavailable")
        return None


def _fetch_dhan(trade_day: date) -> List[Fill]:
    """Stock and index futures via get_valid_token(). Auth failures retry once inside DhanClient."""
    from backend.dhan.auth import DhanAuthError
    from backend.dhan.client import DhanApiError, DhanClient

    day = trade_day.isoformat()
    try:
        client = DhanClient()
        if trade_day == datetime.now(IST).date():
            rows = client.get_trade_book()
        else:
            rows = client.get_trade_history(day, day)
    except DhanAuthError as exc:
        login_url = getattr(exc, "login_url", None) or _dhan_browser_login_url()
        raise TradeLogBrokerSyncError(
            str(exc) or "Dhan access token was rejected.",
            login_url=login_url,
        ) from exc
    except DhanApiError as exc:
        raise TradeLogBrokerSyncError(str(exc) or "Dhan trade request failed.") from exc
    fills = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        fill = row_to_fill(row, trade_day)
        if fill:
            fills.append(fill)
    return fills


def _load_existing(db, trade_day: date) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT id, session_date, symbol, contract, direction,
                   entry_time, entry_price, exit_time, exit_price
            FROM trade_log
            WHERE session_date = CAST(:d AS date)
            """
        ),
        {"d": trade_day.isoformat()},
    ).mappings().all()
    return [dict(row) for row in rows]


def _master_symbol(db, symbol: str) -> str:
    try:
        from backend.services.trade_log_journal import lookup_master

        master = lookup_master(db, symbol)
    except Exception:
        logger.info("trade_log broker sync: master lookup skipped for %s", symbol)
        return symbol
    if master and master.get("stock"):
        return str(master["stock"]).strip().upper()
    return symbol


def _sync_db(trade_day: date, broker: str, plans: Sequence[PlannedTrade], fill_count: int) -> Dict[str, Any]:
    from backend.database import SessionLocal
    from backend.services.rule27_trade_log import ensure_trade_log_table, update_trade_fields, upsert_trade
    from backend.services.trade_log_journal import refresh_session_log

    db = SessionLocal()
    try:
        ensure_trade_log_table()
        existing = _load_existing(db, trade_day)

        def insert_fn(payload: Dict[str, Any]) -> None:
            stored = dict(payload)
            stored["symbol"] = _master_symbol(db, str(stored.get("symbol") or ""))
            upsert_trade(db, stored)

        def update_fn(row: Dict[str, Any], plan: PlannedTrade) -> None:
            update_trade_fields(
                db,
                int(row["id"]),
                {"exit_time": plan.exit_time, "exit_price": plan.exit_price},
            )

        applied = apply_plans(plans, existing, trade_day, broker, insert_fn, update_fn)
        if applied["inserted"] or applied["updated"]:
            refresh_session_log(db, trade_day.isoformat(), None, f"broker_{broker}")
        db.commit()
        logger.info(
            "trade_log broker sync %s %s fills=%s inserted=%s updated=%s skipped=%s",
            broker,
            trade_day.isoformat(),
            fill_count,
            applied["inserted"],
            applied["updated"],
            applied["skipped"],
        )
        return _summary(broker, trade_day, fill_count, applied)
    except TradeLogBrokerSyncError:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise TradeLogBrokerSyncError(str(exc) or "Could not store broker trades.") from exc
    finally:
        db.close()


def sync_from_broker(
    trade_date: Any,
    broker: Any,
    *,
    fills: Optional[Sequence[Fill]] = None,
    existing: Optional[Sequence[Dict[str, Any]]] = None,
    insert_fn: Optional[Callable[[Dict[str, Any]], None]] = None,
    update_fn: Optional[Callable[[Dict[str, Any], PlannedTrade], None]] = None,
) -> Dict[str, Any]:
    """Fetch one IST day's futures and insert trade_log rows.

    Pass ``fills`` to skip the broker call. Pass ``insert_fn`` to skip the database.
    """
    day = parse_trade_date(trade_date)
    name = normalize_broker(broker)
    if fills is None:
        loaded = _fetch_upstox(day) if name == "upstox" else _fetch_dhan(day)
    else:
        loaded = list(fills)
    loaded = _unique_fills(loaded)
    plans = plans_from_fills(loaded)
    if insert_fn is None and existing is None:
        return _sync_db(day, name, plans, len(loaded))
    applied = apply_plans(
        plans,
        list(existing or []),
        day,
        name,
        insert_fn or (lambda _payload: None),
        update_fn,
    )
    return _summary(name, day, len(loaded), applied)

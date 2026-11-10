"""Sync multi-leg option fills from Upstox, Dhan, or Delta Exchange India.

Upstox index options (NIFTY, BANKNIFTY, SENSEX) still go through
``sync_from_upstox``. Dhan, Delta, and the remaining Upstox options (NSE and
BSE stock options, other index options, MCX commodity options) are parsed into
the same leg shape and stored with ``_persist_plan``: 5-minute grouping,
orphan rule, and an editable journal row.

Dhan sets ``exchangeTradeId`` to 0 on every fill. That value is never used as
the stored id. A fill is identified by its option contract, side, execution
time, and price, so two different option fills both numbered 0 stay distinct
and a later sync of the same fill does not insert another leg.

No orders are placed. Secrets are not logged.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy import text

from backend.services.multi_leg_options import (
    IST,
    MultiLegValidationError,
    _parse_date,
    _parse_dt,
    constructed_option_key,
    ensure_multi_leg_tables,
    is_mcx_instrument,
    last_weekday_of_month,
)
from backend.services.multi_leg_upstox_sync import (
    INDEX_UNDERLYINGS,
    _load_existing,
    _persist_plan,
    _refresh_subscriptions,
    _spot_prices,
    _strike_token,
    _token_error,
    canonical_underlying,
    classify_leg_count,
    group_new_legs,
    sync_from_upstox,
)

logger = logging.getLogger(__name__)

BROKERS = ("upstox", "dhan", "delta")
_USELESS_IDS = {"", "NA", "NONE", "NULL"}
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "SEPT": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_CURRENCY_ROOTS = ("USDINR", "EURINR", "GBPINR", "JPYINR", "EURUSD", "GBPUSD")
_EQUITY_SEGMENTS = {"NSE_EQ", "BSE_EQ", "EQ"}
_CURRENCY_SEGMENTS = {"NSE_CURRENCY", "BSE_CURRENCY", "CDS", "CD", "CURRENCY", "NCD_FO", "BCD_FO"}
_OPTION_SEGMENTS = {"NSE_FNO", "NSE_FO", "BSE_FNO", "BSE_FO", "MCX_COMM", "MCX_FO", "COM", "FO", "FNO"}
_DELTA_NAMES = {"BTC": "BITCOIN", "XBT": "BITCOIN", "ETH": "ETHEREUM"}
_DELTA_SKIP_TYPES = {
    "perpetual_futures",
    "perpetual",
    "futures",
    "future",
    "inverse_perpetual_futures",
    "move_options",
}
_DASHED_OPT = re.compile(
    r"^(?P<root>[A-Z][A-Z0-9&]*)-(?:(?P<day>\d{1,2}))?(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)"
    r"(?P<year>\d{4})-(?P<strike>\d+(?:\.\d+)?)-(?P<right>CE|PE|CALL|PUT)$",
    re.IGNORECASE,
)
_COMPACT_OPT = re.compile(
    r"^(?P<root>[A-Z][A-Z0-9&]*?)(?P<yy>\d{2})(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)"
    r"(?P<strike>\d+(?:\.\d+)?)(?P<right>CE|PE)$",
    re.IGNORECASE,
)
_SPACED_OPT = re.compile(
    r"^(?P<root>[A-Z][A-Z0-9& ]*?)\s+(?:(?P<day>\d{1,2})\s+)?(?P<mon>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|SEPT|OCT|NOV|DEC)"
    r"\s+(?P<year>\d{2,4})\s+(?P<strike>\d+(?:\.\d+)?)\s+(?P<right>CE|PE|CALL|PUT)$",
    re.IGNORECASE,
)
_DELTA_OPT = re.compile(r"^(C|P)-([A-Z0-9]+)-(\d+(?:\.\d+)?)-(\d{6})$", re.IGNORECASE)


def canonical_broker(raw: Any) -> str:
    key = " ".join(str(raw or "").strip().lower().replace("_", " ").replace("-", " ").split())
    if key in {"", "upstox"}:
        return "upstox"
    if key == "dhan":
        return "dhan"
    if key in {"delta", "delta exchange", "delta exchange india", "delta india"}:
        return "delta"
    raise MultiLegValidationError("broker must be Upstox, Dhan, or Delta Exchange India")


def parse_sync_date(raw: Any) -> date:
    """YYYY-MM-DD or DD/MM/YYYY in IST. Empty means today."""
    text_raw = str(raw or "").strip()
    if not text_raw:
        return datetime.now(IST).date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text_raw):
        try:
            return date.fromisoformat(text_raw)
        except ValueError as exc:
            raise MultiLegValidationError("trade_date must be YYYY-MM-DD or DD/MM/YYYY") from exc
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", text_raw)
    if match:
        day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return date(year, month, day)
        except ValueError as exc:
            raise MultiLegValidationError("trade_date must be YYYY-MM-DD or DD/MM/YYYY") from exc
    raise MultiLegValidationError("trade_date must be YYYY-MM-DD or DD/MM/YYYY")


def _first(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _upper(raw: Any) -> str:
    return str(raw or "").strip().upper()


def _num(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    text_raw = str(raw).strip()
    if not text_raw or text_raw.upper() in _USELESS_IDS:
        return None
    try:
        value = float(text_raw)
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return value


def _useless_trade_id(raw: Any) -> bool:
    text_raw = str(raw or "").strip()
    if text_raw.upper() in _USELESS_IDS:
        return True
    try:
        return float(text_raw) == 0.0
    except (TypeError, ValueError):
        return False


def leg_fingerprint(leg: Dict[str, Any]) -> str:
    """Contract + side + execution second + price.

    The contract (underlying, expiry, strike, right) is the instrument, so a
    call and a put at the same premium stay separate. Re-syncing the same
    contract at the same time and price hits this key.
    """
    when = leg["entry_time"]
    if not isinstance(when, datetime):
        parsed = _parse_dt(when)
        if parsed is None:
            raise MultiLegValidationError("leg entry_time is required")
        when = parsed
    when = when.astimezone(IST).replace(microsecond=0)
    expiry = leg["expiry"]
    if isinstance(expiry, datetime):
        expiry_text = expiry.date().isoformat()
    elif isinstance(expiry, date):
        expiry_text = expiry.isoformat()
    else:
        parsed_day = _parse_date(expiry)
        expiry_text = parsed_day.isoformat() if parsed_day else str(expiry or "")[:10]
    price = float(leg["entry_price"])
    return "|".join([
        _upper(leg.get("instrument")),
        expiry_text,
        _strike_token(leg.get("strike_price")),
        _upper(leg.get("option_type")),
        _upper(leg.get("side")),
        when.strftime("%Y-%m-%d %H:%M:%S"),
        f"{price:.4f}",
    ])


def order_id_for(broker: str, leg: Dict[str, Any], raw_id: Any = None) -> str:
    """Stable leg id. Never the Dhan placeholder 0."""
    fingerprint = leg_fingerprint(leg)
    if broker == "upstox" and not _useless_trade_id(raw_id):
        return str(raw_id).strip()
    return f"{broker}|{fingerprint}"


def _right_from_text(raw: Any) -> Optional[str]:
    token = _upper(raw)
    if token in {"CALL", "CE"}:
        return "CE"
    if token in {"PUT", "PE"}:
        return "PE"
    return None


def _symbol_text(row: Dict[str, Any]) -> str:
    return str(
        _first(
            row,
            "trading_symbol",
            "tradingsymbol",
            "tradingSymbol",
            "customSymbol",
            "product_symbol",
            "symbol",
            "scrip_name",
        )
        or ""
    ).strip()


def _option_right(row: Dict[str, Any], symbol: str) -> Optional[str]:
    for key in ("drvOptionType", "option_type", "optionType"):
        right = _right_from_text(row.get(key))
        if right:
            return right
    compact = re.sub(r"[^A-Z0-9]", "", symbol.upper())
    if compact.endswith("CE"):
        return "CE"
    if compact.endswith("PE"):
        return "PE"
    parts = symbol.upper().replace("-", " ").split()
    if parts:
        return _right_from_text(parts[-1])
    return None


def _venue_blob(row: Dict[str, Any]) -> Tuple[str, str, str]:
    exchange = _upper(_first(row, "exchange", "exchangeSegment", "exchange_segment"))
    segment = _upper(_first(row, "segment", "exchange_segment") or row.get("_segment_hint"))
    # Dhan puts the venue in exchangeSegment and leaves segment empty.
    if not segment and exchange in _OPTION_SEGMENTS | _EQUITY_SEGMENTS | _CURRENCY_SEGMENTS:
        segment = exchange
    key = _upper(_first(row, "instrument_token", "instrument_key", "instrumentToken"))
    return exchange, segment, key


def _currency_symbol(symbol: str) -> bool:
    compact = re.sub(r"[^A-Z0-9]", "", symbol.upper())
    return compact.startswith(_CURRENCY_ROOTS)


def _venue_allows_option(row: Dict[str, Any], symbol: str) -> bool:
    """NSE, BSE, and MCX option venues. Equity cash and currency are rejected."""
    if _currency_symbol(symbol):
        return False
    exchange, segment, key = _venue_blob(row)
    blob = f"{exchange} {segment} {key}"
    if any(token in blob for token in ("CURRENCY", "NSE_CURRENCY", "BSE_CURRENCY", "NCD_FO", "BCD_FO")):
        return False
    if exchange in _CURRENCY_SEGMENTS or segment in _CURRENCY_SEGMENTS:
        return False
    if exchange in _EQUITY_SEGMENTS or segment in _EQUITY_SEGMENTS:
        return False
    if key.startswith("NSE_EQ") or key.startswith("BSE_EQ"):
        return False
    if exchange in _OPTION_SEGMENTS or segment in _OPTION_SEGMENTS:
        return True
    if key.startswith(("NSE_FO", "NSE_FNO", "BSE_FO", "BSE_FNO", "MCX_FO", "MCX_COMM")):
        return True
    if exchange in {"NSE", "BSE"} and segment in {"FO", "FNO", "NSE_FNO", "BSE_FNO"}:
        return True
    if exchange == "MCX" or segment in {"COM", "MCX"} or "MCX" in blob:
        return True
    hint = _upper(row.get("_segment_hint"))
    if hint in {"FO", "FNO", "COM", "BSE_FO"}:
        return True
    return False


def _parse_expiry_value(raw: Any) -> Optional[date]:
    if raw is None or str(raw).strip() == "" or _upper(raw) in _USELESS_IDS:
        return None
    if isinstance(raw, datetime):
        return raw.astimezone(IST).date() if raw.tzinfo else raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, (int, float)):
        seconds = float(raw)
        if seconds > 1e14:
            seconds /= 1_000_000.0
        elif seconds > 1e11:
            seconds /= 1000.0
        if seconds <= 0:
            return None
        return datetime.fromtimestamp(seconds, IST).date()
    parsed = _parse_date(raw)
    if parsed is not None and not str(raw).strip().isdigit():
        return parsed
    text_raw = str(raw).strip()
    for fmt, width in (
        ("%d-%m-%Y", 10),
        ("%d/%m/%Y", 10),
        ("%d-%b-%Y", 11),
        ("%d%b%Y", 9),
        ("%d-%b-%y", 9),
        ("%Y%m%d", 8),
    ):
        try:
            return datetime.strptime(text_raw[:width], fmt).date()
        except ValueError:
            continue
    return None


def _month_expiry(year: int, month: int, day: Optional[int], instrument: str, venue_segment: str) -> Optional[date]:
    if day:
        try:
            return date(year, month, int(day))
        except ValueError:
            return None
    mcx = is_mcx_instrument(instrument) or "MCX" in venue_segment or venue_segment in {"COM", "MCX_COMM", "MCX_FO"}
    weekday = 3 if mcx else 1
    return last_weekday_of_month(year, month, weekday)


def _parts_from_symbol(symbol: str) -> Dict[str, Any]:
    text_raw = " ".join(symbol.upper().split())
    match = _DASHED_OPT.match(text_raw.replace(" ", ""))
    if not match:
        match = _COMPACT_OPT.match(re.sub(r"[^A-Z0-9]", "", text_raw))
        if match:
            year = 2000 + int(match.group("yy"))
            return {
                "root": match.group("root"),
                "day": None,
                "month": _MONTHS.get(match.group("mon").upper()),
                "year": year,
                "strike": float(match.group("strike")),
                "right": _right_from_text(match.group("right")),
            }
    if not match:
        match = _SPACED_OPT.match(text_raw)
    if not match:
        plain = re.sub(r"[^A-Z0-9&]", "", text_raw)
        if plain and plain.isalnum():
            return {"root": plain}
        return {}
    year_raw = match.group("year")
    year = int(year_raw)
    if year < 100:
        year += 2000
    day_raw = match.groupdict().get("day")
    return {
        "root": re.sub(r"\s+", "", match.group("root") or ""),
        "day": int(day_raw) if day_raw else None,
        "month": _MONTHS.get(match.group("mon").upper()),
        "year": year,
        "strike": float(match.group("strike")),
        "right": _right_from_text(match.group("right")),
    }


def _fill_time(row: Dict[str, Any], trade_date: date) -> Optional[datetime]:
    for key in (
        "exchangeTime",
        "order_timestamp",
        "exchange_timestamp",
        "trade_timestamp",
        "updateTime",
        "createTime",
        "created_at",
        "timestamp",
        "trade_time",
        "time",
    ):
        parsed = _parse_dt(row.get(key))
        if parsed is not None:
            return parsed.astimezone(IST)
    row_day = _parse_expiry_value(_first(row, "trade_date"))
    if row_day == trade_date:
        return IST.localize(datetime(trade_date.year, trade_date.month, trade_date.day, 15, 30, 0))
    return None


def _instrument_name(root: str) -> str:
    name = canonical_underlying(root) or _upper(root)
    return name


def _listed_leg(row: Dict[str, Any], trade_date: date, broker: str) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    symbol = _symbol_text(row)
    right = _option_right(row, symbol)
    if right not in {"CE", "PE"}:
        return None
    if not _venue_allows_option(row, symbol):
        return None
    status = _upper(_first(row, "status", "order_status", "orderStatus")).lower()
    if any(bad in status for bad in ("cancel", "reject")):
        return None
    if status in {"open", "pending", "trigger pending", "trigger_pending", "modified"}:
        return None
    qty = _num(_first(row, "filled_quantity", "tradedQuantity", "quantity", "qty", "size"))
    price = _num(_first(row, "tradedPrice", "average_price", "price", "trade_price", "averagePrice"))
    if qty is None or qty <= 0 or price is None or price <= 0:
        return None
    if status and "complete" not in status and status not in {"filled", "traded", ""} and qty <= 0:
        return None
    parts = _parts_from_symbol(symbol)
    strike = _num(_first(row, "drvStrikePrice", "strike_price", "strikePrice", "strike"))
    if strike is None or strike <= 0:
        strike = parts.get("strike")
    if strike is None or float(strike) <= 0:
        return None
    expiry = _parse_expiry_value(_first(row, "drvExpiryDate", "expiry", "expiry_date", "expiryDate"))
    exchange, segment, key = _venue_blob(row)
    root = _instrument_name(str(parts.get("root") or ""))
    if expiry is None and parts.get("month") and parts.get("year"):
        expiry = _month_expiry(int(parts["year"]), int(parts["month"]), parts.get("day"), root, f"{exchange} {segment}")
    if expiry is None or not root:
        return None
    side = _upper(_first(row, "transaction_type", "transactionType", "side"))
    if side not in {"BUY", "SELL"}:
        return None
    stamp = _fill_time(row, trade_date)
    if stamp is None or stamp.astimezone(IST).date() != trade_date:
        return None
    row_day = str(_first(row, "trade_date") or "").strip()[:10]
    if row_day and row_day != trade_date.isoformat():
        return None
    lot = int(round(float(qty)))
    if lot <= 0:
        return None
    token = str(_first(row, "instrument_token", "instrument_key", "instrumentToken") or "").strip()
    if "|" in token:
        instrument_key = token
    else:
        instrument_key = constructed_option_key(root, expiry, float(strike), right)
    leg = {
        "instrument": root,
        "expiry": expiry,
        "strike_price": float(strike),
        "option_type": right,
        "side": side,
        "entry_price": float(price),
        "entry_time": stamp.astimezone(IST),
        "lot_size": lot,
        "instrument_key": instrument_key,
    }
    raw_id = _first(row, "order_id", "trade_id", "exchange_order_id", "orderId")
    leg["upstox_order_id"] = order_id_for(broker, leg, raw_id)
    return leg


def _delta_underlying_name(token: str) -> str:
    key = _upper(token)
    return _DELTA_NAMES.get(key, key)


def _delta_leg(row: Dict[str, Any], trade_date: date) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    contract_type = _upper(_first(row, "contract_type", "contractType")).lower()
    if contract_type in _DELTA_SKIP_TYPES:
        return None
    symbol = _symbol_text(row)
    match = _DELTA_OPT.match(symbol.strip())
    if not match:
        return None
    if contract_type and contract_type not in {"call_options", "put_options", "options", "option", ""}:
        return None
    right = "CE" if match.group(1).upper() == "C" else "PE"
    try:
        expiry = datetime.strptime(match.group(4), "%d%m%y").date()
        strike = float(match.group(3))
    except ValueError:
        return None
    side_raw = _upper(_first(row, "side", "transaction_type", "transactionType"))
    if side_raw in {"BUY", "B"}:
        side = "BUY"
    elif side_raw in {"SELL", "S"}:
        side = "SELL"
    else:
        return None
    price = _num(_first(row, "price", "average_price", "tradedPrice"))
    size = _num(_first(row, "size", "quantity", "filled_quantity", "tradedQuantity"))
    if price is None or price <= 0 or size is None or size <= 0:
        return None
    lot = int(round(float(size)))
    if lot <= 0:
        return None
    stamp = _fill_time(row, trade_date)
    if stamp is None or stamp.astimezone(IST).date() != trade_date:
        return None
    instrument = _delta_underlying_name(match.group(2))
    leg = {
        "instrument": instrument,
        "expiry": expiry,
        "strike_price": strike,
        "option_type": right,
        "side": side,
        "entry_price": float(price),
        "entry_time": stamp.astimezone(IST),
        "lot_size": lot,
        "instrument_key": match.group(0).upper(),
    }
    leg["upstox_order_id"] = order_id_for("delta", leg, None)
    return leg


def parse_broker_rows(rows: Sequence[Dict[str, Any]], trade_date: date, broker: str) -> Dict[str, Any]:
    """Turn broker rows into journal legs. Futures, equity, and perpetuals drop out."""
    name = canonical_broker(broker)
    day = trade_date if isinstance(trade_date, date) else parse_sync_date(trade_date)
    legs: List[Dict[str, Any]] = []
    unmapped = 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        if name == "delta":
            leg = _delta_leg(row, day)
            looked_like_option = bool(_DELTA_OPT.match(_symbol_text(row).strip()))
        else:
            symbol = _symbol_text(row)
            looked_like_option = _option_right(row, symbol) in {"CE", "PE"} and _venue_allows_option(row, symbol)
            leg = _listed_leg(row, day, name) if looked_like_option else None
        if leg is None:
            if looked_like_option:
                unmapped += 1
            continue
        legs.append(leg)
    return {"legs": legs, "skipped_unmapped": unmapped}


def build_option_plan(
    legs: Sequence[Dict[str, Any]],
    *,
    existing_order_ids: Iterable[str] = (),
    existing_fingerprints: Iterable[str] = (),
    existing_trades: Iterable[Tuple[str, date]] = (),
    ignore_order_ids: Iterable[str] = (),
    ignore_fingerprints: Iterable[str] = (),
) -> Dict[str, Any]:
    """Same grouping and orphan rule as the Upstox index sync."""
    known_ids = {str(item).strip() for item in existing_order_ids if str(item or "").strip()}
    known_fp = {str(item) for item in existing_fingerprints if item}
    ignore_ids = {str(item).strip() for item in ignore_order_ids if str(item or "").strip()}
    ignore_fp = {str(item) for item in ignore_fingerprints if item}
    known_trades = set()
    for item in existing_trades or []:
        if not item or not item[0] or not item[1]:
            continue
        known_trades.add((str(item[0]).strip().upper(), item[1]))
    duplicates = 0
    fresh: List[Dict[str, Any]] = []
    for leg in legs or []:
        oid = str(leg.get("upstox_order_id") or "").strip()
        fingerprint = leg_fingerprint(leg)
        if not oid or oid in known_ids or fingerprint in known_fp:
            if oid not in ignore_ids and fingerprint not in ignore_fp:
                duplicates += 1
            continue
        known_ids.add(oid)
        known_fp.add(fingerprint)
        fresh.append(leg)

    creates: List[Dict[str, Any]] = []
    orphan_legs: List[Dict[str, Any]] = []
    for group in group_new_legs(fresh):
        key = (group[0]["instrument"], group[0]["expiry"])
        if key in known_trades:
            orphan_legs.extend(group)
            continue
        entry = min(leg["entry_time"] for leg in group).astimezone(IST).date()
        creates.append({
            "trade_type": classify_leg_count(len(group)),
            "instrument": key[0],
            "expiry": key[1],
            "entry_date": entry,
            "legs": group,
        })
        known_trades.add(key)
    return {
        "new_legs": sum(len(item["legs"]) for item in creates),
        "new_trades": len(creates),
        "orphans": len(orphan_legs),
        "duplicates_skipped": duplicates,
        "skipped_no_lot": 0,
        "skipped_unmapped": 0,
        "creates": creates,
        "orphan_legs": orphan_legs,
    }


def _summary(plan: Dict[str, Any], trade_date: date, broker: str) -> Dict[str, Any]:
    return {
        "ok": True,
        "broker": broker,
        "trade_date": trade_date.isoformat(),
        "new_legs": int(plan.get("new_legs") or 0),
        "new_trades": int(plan.get("new_trades") or 0),
        "orphans": int(plan.get("orphans") or 0),
        "duplicates_skipped": int(plan.get("duplicates_skipped") or 0),
        "skipped_no_lot": int(plan.get("skipped_no_lot") or 0),
        "skipped_unmapped": int(plan.get("skipped_unmapped") or 0),
    }


def _merge_summary(base: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key in ("new_legs", "new_trades", "orphans", "duplicates_skipped", "skipped_no_lot", "skipped_unmapped"):
        merged[key] = int(base.get(key) or 0) + int(extra.get(key) or 0)
    merged["broker"] = extra.get("broker") or base.get("broker")
    merged["trade_date"] = extra.get("trade_date") or base.get("trade_date")
    merged["ok"] = True
    return merged


def _load_fingerprints(db) -> Set[str]:
    rows = db.execute(text(
        """
        SELECT l.side, l.option_type, l.strike_price, l.leg_expiry_date,
               l.entry_price, l.entry_time,
               UPPER(TRIM(COALESCE(t.instrument, ''))) AS instrument
        FROM multi_leg_trade_legs l
        LEFT JOIN multi_leg_trades t ON t.id = l.trade_id
        WHERE l.entry_time IS NOT NULL
          AND l.entry_price IS NOT NULL
          AND l.strike_price IS NOT NULL
        """
    )).mappings().all()
    found: Set[str] = set()
    for row in rows:
        instrument = _upper(row.get("instrument"))
        expiry = _parse_date(row.get("leg_expiry_date"))
        when = _parse_dt(row.get("entry_time"))
        price = _num(row.get("entry_price"))
        strike = _num(row.get("strike_price"))
        right = _upper(row.get("option_type"))
        side = _upper(row.get("side"))
        if not instrument or expiry is None or when is None or price is None or strike is None:
            continue
        if right not in {"CE", "PE"} or side not in {"BUY", "SELL"}:
            continue
        found.add(leg_fingerprint({
            "instrument": instrument,
            "expiry": expiry,
            "strike_price": strike,
            "option_type": right,
            "side": side,
            "entry_price": price,
            "entry_time": when,
        }))
    return found


def _enrich_spots(spots: Dict[str, Optional[float]], instruments: Iterable[str]) -> None:
    wanted = [name for name in instruments if name and not (spots.get(name) and spots.get(name) > 0)]
    crypto = [name for name in wanted if name in {"BITCOIN", "ETHEREUM"}]
    if not crypto:
        return
    try:
        from backend.services.multi_leg_crypto import crypto_spot_ltps

        quotes = crypto_spot_ltps(crypto)
    except Exception:
        logger.info("multi_leg broker sync: crypto spot lookup failed")
        return
    for name, price in (quotes or {}).items():
        if price and price > 0:
            spots[name] = float(price)


def _store_plan(plan: Dict[str, Any]) -> None:
    if not plan.get("creates") and not plan.get("orphan_legs"):
        return
    names = [item["instrument"] for item in plan.get("creates") or []]
    spots = _spot_prices(names)
    _enrich_spots(spots, names)
    _persist_plan(plan, spots)
    _refresh_subscriptions()


def import_parsed_legs(
    parsed: Dict[str, Any],
    trade_date: date,
    broker: str,
    *,
    ignore_order_ids: Iterable[str] = (),
    ignore_fingerprints: Iterable[str] = (),
) -> Dict[str, Any]:
    """Dedupe against stored legs and write with the Upstox persister."""
    from backend.database import SessionLocal

    ensure_multi_leg_tables()
    legs = list(parsed.get("legs") or [])
    db = SessionLocal()
    try:
        existing_ids, existing_trades = _load_existing(db)
        fingerprints = _load_fingerprints(db)
    finally:
        db.close()
    plan = build_option_plan(
        legs,
        existing_order_ids=existing_ids,
        existing_fingerprints=fingerprints,
        existing_trades=existing_trades,
        ignore_order_ids=ignore_order_ids,
        ignore_fingerprints=ignore_fingerprints,
    )
    plan["skipped_unmapped"] = int(parsed.get("skipped_unmapped") or 0)
    _store_plan(plan)
    summary = _summary(plan, trade_date, broker)
    logger.info("multi_leg broker sync %s %s: %s", broker, trade_date.isoformat(), summary)
    return summary


def _upstox_history_segment(ux, trade_date: date, segment: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    page = 1
    total_pages = 1
    while page <= total_pages and page <= 40:
        response = ux.make_api_request(
            url="https://api.upstox.com/v2/charges/historical-trades",
            method="GET",
            params={
                "segment": segment,
                "start_date": trade_date.isoformat(),
                "end_date": trade_date.isoformat(),
                "page_number": page,
                "page_size": 500,
            },
            timeout=20,
        )
        if not isinstance(response, dict) or response.get("status") != "success":
            err = response.get("message") if isinstance(response, dict) else "Upstox trade history failed"
            errors = response.get("errors") if isinstance(response, dict) else None
            if isinstance(errors, list) and errors:
                first = errors[0]
                if isinstance(first, dict) and first.get("message"):
                    err = first.get("message")
            raise _token_error(str(err or f"Upstox {segment} trade history failed"))
        data = response.get("data") or []
        if not isinstance(data, list):
            data = []
        for item in data:
            if isinstance(item, dict):
                copied = dict(item)
                copied["_segment_hint"] = segment
                rows.append(copied)
        meta = response.get("metadata") if isinstance(response.get("metadata"), dict) else {}
        page_meta = meta.get("page") if isinstance(meta.get("page"), dict) else {}
        try:
            total_pages = int(page_meta.get("total_pages") or 1)
        except (TypeError, ValueError):
            total_pages = 1
        page += 1
    return rows


def _fetch_upstox_rows(trade_date: date) -> List[Dict[str, Any]]:
    from backend.services.multi_leg_options import _upstox

    ux = _upstox()
    if not getattr(ux, "access_token", None):
        raise MultiLegValidationError("Upstox access token is missing or expired")
    if trade_date == datetime.now(IST).date():
        book = ux.get_order_book_today()
        if not isinstance(book, dict) or not book.get("success"):
            err = book.get("error") if isinstance(book, dict) else "Upstox order book failed"
            raise _token_error(str(err or "Upstox order book failed"))
        orders = book.get("orders") or []
        return [item for item in orders if isinstance(item, dict)] if isinstance(orders, list) else []
    rows: List[Dict[str, Any]] = []
    for segment in ("FO", "BSE_FO", "COM"):
        try:
            rows.extend(_upstox_history_segment(ux, trade_date, segment))
        except MultiLegValidationError as exc:
            if segment == "FO":
                raise
            logger.info("multi_leg broker sync: upstox segment %s skipped", segment)
            if "token" in str(exc).lower() or "expired" in str(exc).lower():
                raise
    return rows


def _fetch_dhan_rows(trade_date: date) -> List[Dict[str, Any]]:
    from backend.dhan.auth import DhanAuthError, redact_secrets
    from backend.dhan.client import DhanApiError, DhanClient

    day = trade_date.isoformat()
    try:
        client = DhanClient()
        if trade_date == datetime.now(IST).date():
            rows = client.get_trade_book()
        else:
            rows = client.get_trade_history(day, day)
    except DhanAuthError as exc:
        raise MultiLegValidationError(redact_secrets(exc) or "Dhan access token was rejected.") from exc
    except DhanApiError as exc:
        raise MultiLegValidationError(redact_secrets(exc) or "Dhan trade request failed.") from exc
    return [item for item in rows or [] if isinstance(item, dict)]


def _delta_error(page: Dict[str, Any]) -> str:
    if page.get("missing_env"):
        names = ", ".join(str(item) for item in page.get("missing_env") or [])
        return f"Delta Exchange India credentials are not configured ({names})."
    code = str(page.get("error") or "request_failed")
    status = page.get("http_status")
    if status:
        return f"Delta Exchange India rejected the fills request ({code}, HTTP {status})."
    return f"Delta Exchange India rejected the fills request ({code})."


def _fetch_delta_rows(trade_date: date) -> List[Dict[str, Any]]:
    from backend.services.crypto_options_report.delta_client import fetch_dated_option_fills

    start = IST.localize(datetime(trade_date.year, trade_date.month, trade_date.day, 0, 0, 0))
    end = start + timedelta(days=1)
    page = fetch_dated_option_fills(
        start_time_us=int(start.timestamp() * 1_000_000),
        end_time_us=int(end.timestamp() * 1_000_000),
    )
    if not page.get("ok"):
        raise MultiLegValidationError(_delta_error(page))
    fills = page.get("fills") or []
    return [item for item in fills if isinstance(item, dict)]


def sync_multi_leg_from_broker(trade_date: Any = None, broker: Any = None) -> Dict[str, Any]:
    """Import one IST day's option fills for the chosen broker."""
    name = canonical_broker(broker)
    day = parse_sync_date(trade_date)
    if name == "upstox":
        from backend.database import SessionLocal

        ensure_multi_leg_tables()
        db = SessionLocal()
        try:
            before_ids, _before_trades = _load_existing(db)
        finally:
            db.close()
        summary = sync_from_upstox(day.isoformat())
        summary["broker"] = "upstox"
        db = SessionLocal()
        try:
            after_ids, _after_trades = _load_existing(db)
            after_fp = _load_fingerprints(db)
        finally:
            db.close()
        parsed = parse_broker_rows(_fetch_upstox_rows(day), day, "upstox")
        kept = []
        for leg in parsed["legs"]:
            oid = str(leg.get("upstox_order_id") or "").strip()
            # Index fills sync_from_upstox already stored (or that were already
            # in the journal) stay on that path. A fill it skipped is kept.
            if leg.get("instrument") in INDEX_UNDERLYINGS and (oid in after_ids or leg_fingerprint(leg) in after_fp):
                continue
            if oid and oid in before_ids and leg.get("instrument") in INDEX_UNDERLYINGS:
                continue
            kept.append(leg)
        extra = import_parsed_legs(
            {"legs": kept, "skipped_unmapped": parsed.get("skipped_unmapped") or 0},
            day,
            "upstox",
        )
        merged = _merge_summary(summary, extra)
        logger.info("multi_leg broker sync upstox merged: new_legs=%s duplicates=%s", merged.get("new_legs"), merged.get("duplicates_skipped"))
        return merged
    if name == "dhan":
        rows = _fetch_dhan_rows(day)
    else:
        rows = _fetch_delta_rows(day)
    return import_parsed_legs(parse_broker_rows(rows, day, name), day, name)

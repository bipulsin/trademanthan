"""Import executed NIFTY, BANKNIFTY, and SENSEX option fills for one IST day.

Today uses the Upstox order book (``get_order_book_today``). Any other date
uses FO trade history for that calendar day only. Both paths keep the same
dedupe, 5-minute grouping, and orphan rules. Manual New Trade still goes
through ``create_trade``. This module never places orders.

Orphan rule: a new fill whose underlying and expiry already belong to any
trade (Active or Closed, manual or synced) is stored with trade_id null.
Matching does not attach the leg to that trade.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from backend.database import SessionLocal
from backend.services.multi_leg_options import (
    IST,
    MultiLegNotFound,
    MultiLegValidationError,
    _fetch_ltps,
    _iso_dt,
    _option_index,
    _parse_date,
    _parse_dt,
    _upstox,
    allocate_trade_no,
    ensure_multi_leg_tables,
)
from backend.services.stock_option_signals import expiry_date_from_instrument

logger = logging.getLogger(__name__)

GROUP_WINDOW = timedelta(minutes=5)
INDEX_UNDERLYINGS = ("NIFTY", "BANKNIFTY", "SENSEX")

_UNDERLYING_ALIAS = {
    "NIFTY": "NIFTY",
    "NIFTY50": "NIFTY",
    "NIFTY 50": "NIFTY",
    "BANKNIFTY": "BANKNIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "BANK NIFTY": "BANKNIFTY",
    "SENSEX": "SENSEX",
}

TradeKey = Tuple[str, date]


class ContractIndex:
    """instrument_key (and unique suffix) → option contract fields."""

    def __init__(self) -> None:
        self.by_key: Dict[str, Dict[str, Any]] = {}
        self.by_suffix: Dict[str, List[Dict[str, Any]]] = {}
        self.by_spec: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}

    def __bool__(self) -> bool:
        return bool(self.by_key)


def canonical_underlying(raw: Any) -> Optional[str]:
    key = " ".join(str(raw or "").strip().upper().split())
    if not key:
        return None
    return _UNDERLYING_ALIAS.get(key)


def classify_leg_count(n: int) -> str:
    """Trade type for one new group of legs.

    Iron Condor detection will be revisited later. Four legs are stored as
    IRON_FLY for now so the user can correct the type on the trade.
    """
    count = int(n)
    if count == 2:
        return "STRADDLE"
    if count == 4:
        return "IRON_FLY"
    return "UNCLASSIFIED"


def _normalize_key(raw: Any) -> str:
    return str(raw or "").strip().upper().replace("/", "|").replace(":", "|")


def _row_expiry(row: Dict[str, Any]) -> Optional[date]:
    exp = row.get("expiry")
    if isinstance(exp, datetime):
        return exp.date()
    if isinstance(exp, date):
        return exp
    parsed = _parse_date(exp)
    if parsed is not None and not isinstance(exp, (int, float)):
        # ISO dates only. Millisecond epochs go through the master helper.
        text_exp = str(exp or "").strip()
        if text_exp and not text_exp.isdigit():
            return parsed
    return expiry_date_from_instrument(row)


def _row_strike(row: Dict[str, Any]) -> Optional[float]:
    raw = row.get("strike_price")
    if raw is None:
        raw = row.get("strike")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value


def _row_lot(row: Dict[str, Any]) -> Optional[int]:
    raw = row.get("lot_size")
    if raw is None:
        raw = row.get("lotSize")
    try:
        lot = int(float(raw))
    except (TypeError, ValueError):
        return None
    if lot <= 0:
        return None
    return lot


def build_contract_index(rows: Sequence[Dict[str, Any]]) -> ContractIndex:
    """Index CE/PE rows for NIFTY, BANKNIFTY, and SENSEX from the instrument master."""
    index = ContractIndex()
    lot_fallback: Dict[Tuple[str, str], int] = {}
    staged: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        right = str(row.get("instrument_type") or "").strip().upper()
        if right not in ("CE", "PE"):
            continue
        instrument = canonical_underlying(row.get("underlying_symbol") or row.get("name"))
        if instrument not in INDEX_UNDERLYINGS:
            continue
        expiry = _row_expiry(row)
        strike = _row_strike(row)
        key = str(row.get("instrument_key") or "").strip()
        if expiry is None or strike is None or not key:
            continue
        lot = _row_lot(row)
        if lot and (instrument, right) not in lot_fallback:
            lot_fallback[(instrument, right)] = lot
        staged.append({
            "instrument": instrument,
            "expiry": expiry,
            "strike_price": strike,
            "option_type": right,
            "lot_size": lot,
            "instrument_key": key,
        })
    for contract in staged:
        if not contract["lot_size"]:
            contract["lot_size"] = lot_fallback.get((contract["instrument"], contract["option_type"]))
        norm = _normalize_key(contract["instrument_key"])
        index.by_key[norm] = contract
        suffix = norm.split("|")[-1]
        index.by_suffix.setdefault(suffix, []).append(contract)
        spec = (
            contract["instrument"],
            contract["expiry"].isoformat(),
            _strike_token(contract["strike_price"]),
            contract["option_type"],
        )
        index.by_spec[spec] = contract
    return index


def _strike_token(raw: Any) -> str:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return ""
    if abs(value - round(value)) < 1e-6:
        return str(int(round(value)))
    return str(value)


def contract_for_spec(
    index: ContractIndex,
    instrument: Any,
    expiry: Any,
    strike: Any,
    option_type: Any,
) -> Optional[Dict[str, Any]]:
    exp = expiry if isinstance(expiry, date) and not isinstance(expiry, datetime) else _parse_date(expiry)
    name = canonical_underlying(instrument) or str(instrument or "").strip().upper()
    right = str(option_type or "").strip().upper()
    token = _strike_token(strike)
    if exp is None or not name or not token or right not in ("CE", "PE"):
        return None
    return index.by_spec.get((name, exp.isoformat(), token, right))


def lookup_contract(index: ContractIndex, token: Any) -> Optional[Dict[str, Any]]:
    norm = _normalize_key(token)
    if not norm:
        return None
    hit = index.by_key.get(norm)
    if hit:
        return hit
    suffix = norm.split("|")[-1]
    candidates = index.by_suffix.get(suffix) or []
    if len(candidates) == 1:
        return candidates[0]
    return None


def journal_contract_index() -> ContractIndex:
    rows: List[Dict[str, Any]] = []
    for items in (_option_index() or {}).values():
        rows.extend(items)
    return build_contract_index(rows)


def _order_id(order: Dict[str, Any]) -> str:
    return str(order.get("order_id") or order.get("exchange_order_id") or "").strip()


def _order_token(order: Dict[str, Any]) -> str:
    return str(order.get("instrument_token") or order.get("instrument_key") or "").strip()


def _order_symbol(order: Dict[str, Any]) -> str:
    return str(order.get("trading_symbol") or order.get("tradingsymbol") or "").upper().replace(" ", "")


def _looks_like_index_option(order: Dict[str, Any]) -> bool:
    sym = _order_symbol(order)
    if not (sym.endswith("CE") or sym.endswith("PE")):
        return False
    for name in ("BANKNIFTY", "SENSEX", "NIFTY"):
        if sym.startswith(name):
            return True
    return False


def _is_executed(order: Dict[str, Any]) -> bool:
    status = str(order.get("status") or "").lower().strip()
    if not status or any(bad in status for bad in ("cancel", "reject")):
        return False
    if "complete" not in status and status not in ("filled", "traded"):
        return False
    try:
        qty = float(order.get("filled_quantity") or 0)
    except (TypeError, ValueError):
        return False
    return qty > 0


def _fill_price(order: Dict[str, Any]) -> Optional[float]:
    for key in ("average_price", "average_traded_price"):
        raw = order.get(key)
        if raw is None or str(raw).strip() == "":
            continue
        try:
            price = float(raw)
        except (TypeError, ValueError):
            continue
        if price >= 0:
            return price
    return None


def _fill_time(order: Dict[str, Any]) -> Optional[datetime]:
    for key in ("order_timestamp", "exchange_timestamp", "order_created"):
        parsed = _parse_dt(order.get(key))
        if parsed is not None:
            return parsed
    return None


def group_by_entry_window(
    legs: Sequence[Dict[str, Any]],
    window: timedelta = GROUP_WINDOW,
) -> List[List[Dict[str, Any]]]:
    """Split one underlying+expiry bucket by a 5-minute window from the first fill.

    A later fill stays in the group while ``fill_time - group_start <= window``.
    A fill more than 5 minutes after that start opens the next group.
    """
    ordered = sorted(
        legs,
        key=lambda leg: (leg["entry_time"], str(leg.get("upstox_order_id") or "")),
    )
    groups: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    start: Optional[datetime] = None
    for leg in ordered:
        stamp = leg["entry_time"]
        if not current or start is None:
            current = [leg]
            start = stamp
            continue
        if stamp - start <= window:
            current.append(leg)
            continue
        groups.append(current)
        current = [leg]
        start = stamp
    if current:
        groups.append(current)
    return groups


def group_new_legs(
    legs: Sequence[Dict[str, Any]],
    window: timedelta = GROUP_WINDOW,
) -> List[List[Dict[str, Any]]]:
    """Group new legs by underlying + expiry, then by the 5-minute entry window."""
    buckets: Dict[TradeKey, List[Dict[str, Any]]] = {}
    for leg in legs:
        buckets.setdefault((leg["instrument"], leg["expiry"]), []).append(leg)
    groups: List[List[Dict[str, Any]]] = []
    for key in sorted(buckets, key=lambda item: (item[0], item[1].isoformat())):
        groups.extend(group_by_entry_window(buckets[key], window))
    groups.sort(key=lambda group: (group[0]["entry_time"], group[0]["instrument"]))
    return groups


def _trade_key(instrument: Any, expiry: Any) -> Optional[TradeKey]:
    name = str(instrument or "").strip().upper()
    exp = expiry if isinstance(expiry, date) and not isinstance(expiry, datetime) else _parse_date(expiry)
    if not name or exp is None:
        return None
    return name, exp


def build_sync_plan(
    orders: Sequence[Dict[str, Any]],
    contracts: ContractIndex,
    *,
    existing_order_ids: Iterable[str],
    existing_trades: Iterable[TradeKey],
    today: Optional[date] = None,
) -> Dict[str, Any]:
    """Decide creates vs orphans. Does not write.

    An order id that is already on a leg is skipped. Groups of new legs that
    share an underlying and expiry with any trade become orphans instead of a
    new trade. A trade created earlier in this plan counts, so a later group
    on the same underlying+expiry is also orphaned.
    """
    as_of = today or datetime.now(IST).date()
    known_ids = {str(item).strip() for item in existing_order_ids if str(item or "").strip()}
    known_trades = {key for key in existing_trades if key and key[0] and key[1]}
    duplicates = 0
    skipped_no_lot = 0
    skipped_unmapped = 0
    fresh: List[Dict[str, Any]] = []

    for order in orders or []:
        if not isinstance(order, dict) or not _is_executed(order):
            continue
        oid = _order_id(order)
        if not oid:
            if _looks_like_index_option(order):
                skipped_unmapped += 1
            continue
        if oid in known_ids:
            duplicates += 1
            continue
        contract = lookup_contract(contracts, _order_token(order))
        index_option = _looks_like_index_option(order) or (
            contract is not None and contract.get("instrument") in INDEX_UNDERLYINGS
        )
        if not index_option:
            continue
        if contract is None or not contract.get("lot_size"):
            skipped_no_lot += 1
            continue
        side = str(order.get("transaction_type") or order.get("side") or "").strip().upper()
        price = _fill_price(order)
        stamp = _fill_time(order)
        if side not in ("BUY", "SELL") or price is None or stamp is None:
            skipped_unmapped += 1
            continue
        if stamp.astimezone(IST).date() != as_of:
            continue
        known_ids.add(oid)
        fresh.append({
            "upstox_order_id": oid,
            "instrument": contract["instrument"],
            "expiry": contract["expiry"],
            "strike_price": float(contract["strike_price"]),
            "option_type": contract["option_type"],
            "side": side,
            "entry_price": price,
            "entry_time": stamp.astimezone(IST),
            "lot_size": int(contract["lot_size"]),
            "instrument_key": contract["instrument_key"],
        })

    creates: List[Dict[str, Any]] = []
    orphan_legs: List[Dict[str, Any]] = []
    for group in group_new_legs(fresh):
        key = (group[0]["instrument"], group[0]["expiry"])
        # Same underlying and expiry as a journal trade (including one created
        # by an earlier group in this sync) stays an orphan. Do not attach.
        if key in known_trades:
            orphan_legs.extend(group)
            continue
        kind = classify_leg_count(len(group))
        entry = min(leg["entry_time"] for leg in group).astimezone(IST).date()
        creates.append({
            "trade_type": kind,
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
        "skipped_no_lot": skipped_no_lot,
        "skipped_unmapped": skipped_unmapped,
        "creates": creates,
        "orphan_legs": orphan_legs,
    }


def _load_existing(db) -> Tuple[set, set]:
    id_rows = db.execute(text(
        """
        SELECT upstox_order_id
        FROM multi_leg_trade_legs
        WHERE upstox_order_id IS NOT NULL AND TRIM(upstox_order_id) <> ''
        """
    )).fetchall()
    trade_rows = db.execute(text(
        """
        SELECT UPPER(TRIM(instrument)) AS instrument, expiry_date
        FROM multi_leg_trades
        """
    )).mappings().all()
    ids = {str(row[0]).strip() for row in id_rows if row and row[0]}
    # Active and Closed, manual and synced. Any match blocks a new trade.
    keys = set()
    for row in trade_rows:
        key = _trade_key(row.get("instrument"), row.get("expiry_date"))
        if key:
            keys.add(key)
    return ids, keys


def _spot_prices(instruments: Iterable[str]) -> Dict[str, Optional[float]]:
    from backend.services.upstox_service import UpstoxService

    wanted = [name for name in instruments if name]
    if not wanted:
        return {}
    key_by_name = {
        "NIFTY": UpstoxService.NIFTY50_KEY,
        "BANKNIFTY": UpstoxService.BANKNIFTY_KEY,
        "SENSEX": "BSE_INDEX|SENSEX",
    }
    keys = [key_by_name[name] for name in wanted if name in key_by_name]
    quotes = _fetch_ltps(keys) if keys else {}
    out: Dict[str, Optional[float]] = {}
    for name in wanted:
        ik = key_by_name.get(name)
        out[name] = quotes.get(ik) if ik else None
    return out


def _fallback_spot(legs: Sequence[Dict[str, Any]]) -> float:
    strikes = sorted(float(leg["strike_price"]) for leg in legs)
    return float(strikes[len(strikes) // 2])


def _insert_synced_leg(db, trade_id: Optional[str], leg: Dict[str, Any], sort_order: int) -> None:
    db.execute(
        text(
            """
            INSERT INTO multi_leg_trade_legs (
                id, trade_id, side, option_type, strike_price, leg_expiry_date,
                entry_price, entry_time, exit_price, exit_time, ltp, delta,
                lot_size, instrument_key, leg_pnl, upstox_order_id, sort_order
            ) VALUES (
                CAST(:id AS uuid),
                CASE WHEN :trade_id IS NULL THEN NULL ELSE CAST(:trade_id AS uuid) END,
                :side, :option_type, :strike_price, :leg_expiry_date,
                :entry_price, :entry_time, NULL, NULL, NULL, NULL,
                :lot_size, :instrument_key, NULL, :upstox_order_id, :sort_order
            )
            """
        ),
        {
            "id": str(uuid.uuid4()),
            "trade_id": trade_id,
            "side": leg["side"],
            "option_type": leg["option_type"],
            "strike_price": leg["strike_price"],
            "leg_expiry_date": leg["expiry"],
            "entry_price": leg["entry_price"],
            "entry_time": leg["entry_time"],
            "lot_size": leg["lot_size"],
            "instrument_key": leg["instrument_key"],
            "upstox_order_id": leg["upstox_order_id"],
            "sort_order": sort_order,
        },
    )


def _persist_plan(plan: Dict[str, Any], spots: Dict[str, Optional[float]]) -> None:
    db = SessionLocal()
    try:
        for created in plan.get("creates") or []:
            spot = spots.get(created["instrument"])
            if spot is None or spot <= 0:
                spot = _fallback_spot(created["legs"])
                logger.info(
                    "multi_leg upstox sync: index spot missing for %s, stored strike %s",
                    created["instrument"],
                    spot,
                )
            trade_id = str(uuid.uuid4())
            trade_no = allocate_trade_no(db)
            db.execute(
                text(
                    """
                    INSERT INTO multi_leg_trades (
                        id, trade_type, instrument, spot_price_entry, entry_date, expiry_date,
                        status, total_pnl, trade_no
                    ) VALUES (
                        CAST(:id AS uuid), :trade_type, :instrument, :spot, :entry_date, :expiry_date,
                        'ACTIVE', NULL, :trade_no
                    )
                    """
                ),
                {
                    "id": trade_id,
                    "trade_type": created["trade_type"],
                    "instrument": created["instrument"],
                    "spot": spot,
                    "entry_date": created["entry_date"],
                    "expiry_date": created["expiry"],
                    "trade_no": trade_no,
                },
            )
            for index, leg in enumerate(created["legs"]):
                _insert_synced_leg(db, trade_id, leg, index)
        for index, leg in enumerate(plan.get("orphan_legs") or []):
            _insert_synced_leg(db, None, leg, index)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise MultiLegValidationError(
            "Sync conflicted with another import. Run sync again; duplicates will be skipped."
        ) from exc
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _refresh_subscriptions() -> None:
    try:
        from backend.services.multi_leg_ws_ltp import sync_subscriptions

        sync_subscriptions()
    except Exception:
        logger.info("multi_leg upstox sync: quote subscription refresh failed")


def parse_trade_date(raw: Optional[str]) -> date:
    """YYYY-MM-DD IST calendar day. Empty means today in IST."""
    text_raw = str(raw or "").strip()
    if not text_raw:
        return datetime.now(IST).date()
    if len(text_raw) != 10:
        raise MultiLegValidationError("trade_date must be YYYY-MM-DD")
    try:
        return date.fromisoformat(text_raw)
    except ValueError as exc:
        raise MultiLegValidationError("trade_date must be YYYY-MM-DD") from exc


def _historical_stamp(row: Dict[str, Any], trade_date: date) -> str:
    """Keep a real fill time when the history row has one on this IST day."""
    for key in ("order_timestamp", "exchange_timestamp", "trade_timestamp", "trade_time"):
        parsed = _parse_dt(row.get(key))
        if parsed is not None and parsed.astimezone(IST).date() == trade_date:
            return parsed.astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")
    return trade_date.isoformat() + " 15:10:00"


def historical_row_to_order(
    row: Dict[str, Any],
    trade_date: date,
    contracts: ContractIndex,
) -> Optional[Dict[str, Any]]:
    """Shape one FO history row like an executed order, or skip it.

    Rows whose trade_date is not the requested IST day are dropped here so a
    wider history payload cannot leak into the sync.
    """
    if not isinstance(row, dict):
        return None
    row_day = str(row.get("trade_date") or "").strip()[:10]
    if row_day and row_day != trade_date.isoformat():
        return None
    right = str(row.get("option_type") or "").strip().upper()
    if right not in ("CE", "PE"):
        return None
    instrument = canonical_underlying(row.get("symbol") or row.get("scrip_name"))
    if instrument not in INDEX_UNDERLYINGS:
        return None
    expiry = _parse_date(row.get("expiry"))
    strike = _row_strike(row)
    trade_id = str(row.get("trade_id") or row.get("order_id") or "").strip()
    if expiry is None or strike is None or not trade_id:
        return None
    contract = contract_for_spec(contracts, instrument, expiry, strike, right)
    token = str(row.get("instrument_token") or "").strip()
    if not token and contract:
        token = str(contract.get("instrument_key") or "").strip()
    if not token:
        return None
    try:
        qty = float(row.get("quantity") or row.get("filled_quantity") or 0)
    except (TypeError, ValueError):
        return None
    if qty <= 0:
        return None
    price = row.get("price")
    if price is None:
        price = row.get("average_price")
    return {
        "order_id": trade_id,
        "instrument_token": token,
        "transaction_type": row.get("transaction_type"),
        "average_price": price,
        "filled_quantity": qty,
        "status": "complete",
        "order_timestamp": _historical_stamp(row, trade_date),
        "trading_symbol": f"{instrument}{right}",
    }


def _historical_orders(ux, trade_date: date, contracts: ContractIndex) -> List[Dict[str, Any]]:
    """FO fills whose trade_date equals the requested day. No other dates."""
    orders: List[Dict[str, Any]] = []
    page = 1
    total_pages = 1
    while page <= total_pages and page <= 40:
        response = ux.make_api_request(
            url="https://api.upstox.com/v2/charges/historical-trades",
            method="GET",
            params={
                "segment": "FO",
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
            raise _token_error(str(err or "Upstox trade history failed"))
        data = response.get("data") or []
        if not isinstance(data, list):
            data = []
        for row in data:
            mapped = historical_row_to_order(row, trade_date, contracts)
            if mapped:
                orders.append(mapped)
        meta = response.get("metadata") if isinstance(response.get("metadata"), dict) else {}
        page_meta = meta.get("page") if isinstance(meta.get("page"), dict) else {}
        try:
            total_pages = int(page_meta.get("total_pages") or 1)
        except (TypeError, ValueError):
            total_pages = 1
        page += 1
    return orders


def _orders_for_trade_date(ux, trade_date: date, contracts: ContractIndex) -> List[Dict[str, Any]]:
    """Today comes from the order book. Any other day comes only from that day's history."""
    if trade_date == datetime.now(IST).date():
        book = ux.get_order_book_today()
        if not isinstance(book, dict) or not book.get("success"):
            err = book.get("error") if isinstance(book, dict) else "Upstox order book failed"
            raise _token_error(str(err or "Upstox order book failed"))
        orders = book.get("orders") or []
        return orders if isinstance(orders, list) else []
    return _historical_orders(ux, trade_date, contracts)


def _token_error(message: str) -> MultiLegValidationError:
    text_msg = str(message or "Upstox order book failed").strip()
    low = text_msg.lower()
    if any(part in low for part in ("token", "unauthor", "udapi", "expired", "missing access")):
        return MultiLegValidationError("Upstox access token is missing or expired. " + text_msg)
    return MultiLegValidationError(text_msg)


def sync_from_upstox(trade_date: Optional[str] = None) -> Dict[str, Any]:
    """Pull executed index-option orders for one IST day and write new legs once."""
    as_of = parse_trade_date(trade_date)
    ensure_multi_leg_tables()
    ux = _upstox()
    if not getattr(ux, "access_token", None):
        raise MultiLegValidationError("Upstox access token is missing or expired")
    contracts = journal_contract_index()
    orders = _orders_for_trade_date(ux, as_of, contracts)
    db = SessionLocal()
    try:
        existing_ids, existing_trades = _load_existing(db)
    finally:
        db.close()
    plan = build_sync_plan(
        orders,
        contracts,
        existing_order_ids=existing_ids,
        existing_trades=existing_trades,
        today=as_of,
    )
    spots = _spot_prices(item["instrument"] for item in plan["creates"])
    if plan["creates"] or plan["orphan_legs"]:
        _persist_plan(plan, spots)
        _refresh_subscriptions()
    summary = {
        "ok": True,
        "new_legs": plan["new_legs"],
        "new_trades": plan["new_trades"],
        "orphans": plan["orphans"],
        "duplicates_skipped": plan["duplicates_skipped"],
        "skipped_no_lot": plan["skipped_no_lot"],
        "skipped_unmapped": plan["skipped_unmapped"],
        "master_empty": not contracts,
        "trade_date": as_of.isoformat(),
    }
    logger.info("multi_leg upstox sync: %s", summary)
    return summary


def list_orphans() -> List[Dict[str, Any]]:
    ensure_multi_leg_tables()
    db = SessionLocal()
    try:
        rows = db.execute(text(
            """
            SELECT CAST(id AS text) AS id, side, option_type, strike_price, leg_expiry_date,
                   entry_price, entry_time, lot_size, instrument_key, upstox_order_id
            FROM multi_leg_trade_legs
            WHERE trade_id IS NULL
            ORDER BY entry_time ASC, created_at ASC
            """
        )).mappings().all()
    finally:
        db.close()
    contracts = journal_contract_index() if rows else ContractIndex()
    out: List[Dict[str, Any]] = []
    for row in rows:
        contract = lookup_contract(contracts, row.get("instrument_key"))
        expiry = _parse_date(row.get("leg_expiry_date"))
        strike = row.get("strike_price")
        try:
            strike_f = float(strike) if strike is not None else None
        except (TypeError, ValueError):
            strike_f = None
        out.append({
            "id": str(row.get("id")),
            "instrument": (contract or {}).get("instrument"),
            "expiry_date": expiry.isoformat() if expiry else None,
            "strike_price": strike_f,
            "side": row.get("side"),
            "option_type": row.get("option_type"),
            "entry_price": float(row["entry_price"]) if row.get("entry_price") is not None else None,
            "entry_time": _iso_dt(row.get("entry_time")),
            "lot_size": row.get("lot_size"),
            "instrument_key": row.get("instrument_key"),
            "upstox_order_id": row.get("upstox_order_id"),
        })
    return out


def list_assignable_trades() -> List[Dict[str, Any]]:
    """Every journal trade, so an orphan can be attached by id or underlying+expiry."""
    ensure_multi_leg_tables()
    db = SessionLocal()
    try:
        rows = db.execute(text(
            """
            SELECT CAST(id AS text) AS id, trade_no, trade_type, instrument,
                   expiry_date, status, entry_date
            FROM multi_leg_trades
            ORDER BY entry_date DESC, created_at DESC
            """
        )).mappings().all()
    finally:
        db.close()
    out = []
    for row in rows:
        expiry = _parse_date(row.get("expiry_date"))
        entry = _parse_date(row.get("entry_date"))
        out.append({
            "id": str(row.get("id")),
            "trade_no": int(row["trade_no"]) if row.get("trade_no") is not None else None,
            "trade_type": row.get("trade_type"),
            "instrument": row.get("instrument"),
            "expiry_date": expiry.isoformat() if expiry else None,
            "status": row.get("status"),
            "entry_date": entry.isoformat() if entry else None,
        })
    return out


def journal_orphan_state() -> Dict[str, Any]:
    return {
        "orphans": list_orphans(),
        "assignable_trades": list_assignable_trades(),
    }


def assign_orphan(leg_id: str, trade_id: str) -> Dict[str, Any]:
    """Set trade_id on an orphan so it renders under that trade and joins its PnL."""
    ensure_multi_leg_tables()
    try:
        leg = str(uuid.UUID(str(leg_id).strip()))
        trade = str(uuid.UUID(str(trade_id).strip()))
    except (ValueError, AttributeError) as exc:
        raise MultiLegValidationError("leg id and trade id must be uuids") from exc
    db = SessionLocal()
    try:
        found = db.execute(
            text("SELECT 1 FROM multi_leg_trades WHERE id = CAST(:id AS uuid)"),
            {"id": trade},
        ).fetchone()
        if not found:
            raise MultiLegNotFound("trade not found")
        result = db.execute(
            text(
                """
                UPDATE multi_leg_trade_legs
                SET trade_id = CAST(:trade_id AS uuid),
                    sort_order = COALESCE((
                        SELECT MAX(sort_order) + 1
                        FROM multi_leg_trade_legs
                        WHERE trade_id = CAST(:trade_id AS uuid)
                    ), 0),
                    updated_at = NOW()
                WHERE id = CAST(:leg_id AS uuid)
                  AND trade_id IS NULL
                """
            ),
            {"trade_id": trade, "leg_id": leg},
        )
        if getattr(result, "rowcount", 0) == 0:
            raise MultiLegNotFound("orphan leg not found")
        db.commit()
    except (MultiLegNotFound, MultiLegValidationError):
        db.rollback()
        raise
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    _refresh_subscriptions()
    return {"ok": True, "leg_id": leg, "trade_id": trade}

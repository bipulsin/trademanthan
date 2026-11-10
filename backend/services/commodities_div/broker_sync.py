"""Sync one day's MCX broker fills into Commodities Div In-Trade / History.

Fills are grouped by instrument. A round trip (entry side fully matched by the
opposite side) becomes a History row. Quantity that is still open becomes an
In-Trade row. Re-syncing the same broker and date updates those rows instead
of inserting duplicates.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence

import pytz
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from backend.database import SessionLocal
from backend.services.commodities_div.actions import (
    create_manual_history,
    delete_signal,
    exit_submit,
    update_signal,
)
from backend.services.commodities_div.mapping import (
    parse_free_text_commodity,
    parse_tv_symbol,
)
from backend.services.commodities_div.schema import ensure_commodities_div_tables

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_COMPLETE = {"complete", "completed", "traded", "filled", "success"}
_SKIP_STATUS = {"rejected", "cancelled", "canceled", "open", "pending", "trigger pending"}


class BrokerSyncError(Exception):
    def __init__(self, message: str, *, login_url: Optional[str] = None):
        super().__init__(message)
        self.login_url = login_url


@dataclass
class Fill:
    instrument: str
    symbol_raw: str
    side: str
    qty: int
    price: float
    ts: datetime
    fill_id: str
    instrument_key: Optional[str] = None
    option: bool = False


@dataclass
class Plan:
    sync_key: str
    symbol_raw: str
    symbol_mapped: str
    contract: Optional[str]
    instrument_key: Optional[str]
    direction: str
    open_trade: bool
    entry_price: float
    entry_at: str
    exit_price: Optional[float] = None
    exit_at: Optional[str] = None
    qty: int = 1
    mapped_override: Optional[str] = None
    fill_ids: List[str] = field(default_factory=list)


_DMY = re.compile(r"^(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{4})")


def parse_trade_date(raw: Any) -> date:
    """IST calendar day. YYYY-MM-DD, or DD/MM/YYYY so 09/11/2026 is 9 Nov."""
    text_raw = str(raw or "").strip()
    if not text_raw:
        raise BrokerSyncError("Choose a date to sync.")
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
                raise BrokerSyncError("Choose a date as YYYY-MM-DD or DD/MM/YYYY.") from exc
    if parsed is None:
        raise BrokerSyncError("Choose a date as YYYY-MM-DD or DD/MM/YYYY.")
    today = datetime.now(IST).date()
    if parsed > today:
        raise BrokerSyncError("Choose a date that is not in the future.")
    return parsed


def normalize_broker(raw: Any) -> str:
    name = str(raw or "").strip().lower()
    if name in ("upstox", "dhan"):
        return name
    raise BrokerSyncError("Choose Upstox or Dhan.")


def _stamp(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def _parse_ts(raw: Any, trade_day: date) -> datetime:
    text_raw = str(raw or "").strip()
    if text_raw and text_raw.upper() != "NA":
        cleaned = text_raw.replace("T", " ")[:19]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
            try:
                return datetime.strptime(cleaned[:19] if " " in cleaned else cleaned, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(text_raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            pass
    return datetime(trade_day.year, trade_day.month, trade_day.day, 15, 10, 0)


def _num(raw: Any) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _first(row: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _is_mcx(row: Dict[str, Any], *, segment_hint: str = "") -> bool:
    exchange = str(_first(row, "exchange", "exchangeSegment", "exchange_segment") or "").upper()
    segment = str(_first(row, "segment", "exchange_segment") or segment_hint or "").upper()
    token = str(_first(row, "instrument_token", "instrument_key", "instrumentToken") or "")
    if "MCX" in exchange or exchange in ("COM",):
        return True
    if "MCX" in segment or segment in ("COM", "MCX_COMM"):
        if "NSE" not in exchange and "BSE" not in exchange:
            return True
    if token.upper().startswith("MCX"):
        return True
    return False


def _is_option_symbol(symbol: str, option_type: str = "") -> bool:
    kind = str(option_type or "").strip().upper()
    if kind in ("CE", "PE", "CALL", "PUT"):
        return True
    compact = re.sub(r"[^A-Z0-9]", "", symbol.upper())
    return compact.endswith("CE") or compact.endswith("PE")


def _underlying_for(symbol: str, *, option: bool) -> str:
    if option:
        compact = re.sub(r"[^A-Z0-9]", "", symbol.upper())
        return compact or "MCX"
    parsed = parse_free_text_commodity(symbol)
    underlying = parsed.get("underlying")
    if not underlying:
        underlying = parse_tv_symbol(symbol).get("underlying")
    if underlying:
        return str(underlying).strip().upper()
    compact = re.sub(r"[^A-Z0-9]", "", symbol.upper())
    return compact or "MCX"


def scale_traded_qty(raw_qty: int, exchange_lot: Optional[int]) -> int:
    """Broker qty is sometimes lots, sometimes exchange units."""
    qty = int(raw_qty or 0)
    if qty <= 0:
        return 0
    try:
        lot = int(exchange_lot or 0)
    except (TypeError, ValueError):
        lot = 0
    if lot > 1 and qty < lot:
        return qty * lot
    return qty


def _vwap(chunks: Sequence[tuple]) -> float:
    qty = sum(int(c[0]) for c in chunks)
    if qty <= 0:
        return 0.0
    cost = sum(int(c[0]) * float(c[1]) for c in chunks)
    return round(cost / qty, 4)


@dataclass
class _Lot:
    qty: int
    price: float
    ts: datetime
    fill_id: str


def _pair_instrument(fills: Sequence[Fill], *, broker: str, trade_day: date) -> List[Plan]:
    ordered = sorted(fills, key=lambda f: (f.ts, f.fill_id, f.side))
    if not ordered:
        return []
    sample = ordered[0]
    entry_lots: List[_Lot] = []
    consumed: List[tuple] = []
    exits: List[tuple] = []
    side: Optional[str] = None
    plans: List[Plan] = []
    seq = 0

    def _emit_closed() -> None:
        nonlocal seq, consumed, exits
        if not consumed or not exits:
            consumed = []
            exits = []
            return
        qty = sum(int(c[0]) for c in consumed)
        if qty <= 0:
            consumed = []
            exits = []
            return
        entry_side = side or "BUY"
        direction = "BULL" if entry_side == "BUY" else "BEAR"
        entry_ids = sorted({str(c[3]) for c in consumed})
        exit_ids = sorted({str(c[3]) for c in exits})
        seq += 1
        key = f"{broker}|{trade_day.isoformat()}|{sample.instrument}|h|{seq}|{','.join(entry_ids)}|{','.join(exit_ids)}"
        mapped = _underlying_for(sample.symbol_raw, option=sample.option)
        plans.append(
            Plan(
                sync_key=key,
                symbol_raw=sample.symbol_raw,
                symbol_mapped=mapped,
                contract=None if sample.option else sample.symbol_raw,
                instrument_key=sample.instrument_key,
                direction=direction,
                open_trade=False,
                entry_price=_vwap(consumed),
                entry_at=_stamp(min(c[2] for c in consumed)),
                exit_price=_vwap(exits),
                exit_at=_stamp(max(c[2] for c in exits)),
                qty=qty,
                mapped_override=mapped if sample.option else None,
                fill_ids=entry_ids + exit_ids,
            )
        )
        consumed = []
        exits = []

    for fill in ordered:
        if side is None:
            side = fill.side
            entry_lots.append(_Lot(fill.qty, fill.price, fill.ts, fill.fill_id))
            continue
        if fill.side == side:
            entry_lots.append(_Lot(fill.qty, fill.price, fill.ts, fill.fill_id))
            continue
        remaining = fill.qty
        while remaining > 0 and entry_lots:
            lot = entry_lots[0]
            take = min(lot.qty, remaining)
            consumed.append((take, lot.price, lot.ts, lot.fill_id))
            exits.append((take, fill.price, fill.ts, fill.fill_id))
            lot.qty -= take
            remaining -= take
            if lot.qty <= 0:
                entry_lots.pop(0)
        if not entry_lots:
            _emit_closed()
            side = None
            if remaining > 0:
                side = fill.side
                entry_lots.append(_Lot(remaining, fill.price, fill.ts, fill.fill_id))
    if entry_lots and side:
        if consumed and exits:
            _emit_closed()
        qty = sum(lot.qty for lot in entry_lots)
        if qty > 0:
            direction = "BULL" if side == "BUY" else "BEAR"
            ids = sorted({lot.fill_id for lot in entry_lots})
            key = f"{broker}|{trade_day.isoformat()}|{sample.instrument}|o|{','.join(ids)}"
            mapped = _underlying_for(sample.symbol_raw, option=sample.option)
            plans.append(
                Plan(
                    sync_key=key,
                    symbol_raw=sample.symbol_raw,
                    symbol_mapped=mapped,
                    contract=None if sample.option else sample.symbol_raw,
                    instrument_key=sample.instrument_key,
                    direction=direction,
                    open_trade=True,
                    entry_price=_vwap([(lot.qty, lot.price) for lot in entry_lots]),
                    entry_at=_stamp(min(lot.ts for lot in entry_lots)),
                    qty=qty,
                    mapped_override=mapped if sample.option else None,
                    fill_ids=ids,
                )
            )
    elif consumed and exits:
        _emit_closed()
    return plans


def plans_from_fills(fills: Sequence[Fill], *, broker: str, trade_day: date) -> List[Plan]:
    grouped: Dict[str, List[Fill]] = {}
    for fill in fills:
        if fill.qty <= 0 or fill.price <= 0 or fill.side not in ("BUY", "SELL"):
            continue
        grouped.setdefault(fill.instrument, []).append(fill)
    plans: List[Plan] = []
    for instrument in sorted(grouped):
        plans.extend(_pair_instrument(grouped[instrument], broker=broker, trade_day=trade_day))
    return plans


def _day_of(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, datetime):
        return raw.strftime("%Y-%m-%d")
    if isinstance(raw, date):
        return raw.isoformat()
    return str(raw)[:10]


def _near(a: Any, b: Any) -> bool:
    try:
        left = float(a)
        right = float(b)
    except (TypeError, ValueError):
        return False
    tol = max(0.05, abs(right) * 0.001)
    return abs(left - right) <= tol


def _contracts_compatible(row: Dict[str, Any], plan: Plan) -> bool:
    left = str(row.get("contract") or row.get("symbol_raw") or "").strip().upper()
    right = str(plan.contract or plan.symbol_raw or "").strip().upper()
    if not left or not right or left == right:
        return True
    shorter, longer = (left, right) if len(left) <= len(right) else (right, left)
    if len(shorter) < 4 or not longer.startswith(shorter):
        return False
    if len(longer) == len(shorter):
        return True
    return not longer[len(shorter)].isalpha()


def _same_economics(row: Dict[str, Any], plan: Plan) -> bool:
    if str(row.get("direction") or "").upper() != plan.direction:
        return False
    if not _near(row.get("entry_price"), plan.entry_price):
        return False
    try:
        row_qty = int(row.get("lot_size") or 0)
    except (TypeError, ValueError):
        row_qty = 0
    if row_qty != int(plan.qty):
        return False
    status = str(row.get("status") or "")
    if plan.open_trade:
        return status == "In-Trade" and row.get("exit_price") in (None, "", 0)
    if status != "History":
        return False
    return _near(row.get("exit_price"), plan.exit_price)


def reconcile(plans: Sequence[Plan], existing: Sequence[Dict[str, Any]], *, broker: str, trade_day: date) -> Dict[str, Any]:
    """Match planned rows to workspace rows. Pure: no database writes."""
    prefix = f"{broker}|{trade_day.isoformat()}|"
    by_key = {
        str(row.get("broker_sync_key")): row
        for row in existing
        if row.get("broker_sync_key")
    }
    used = set()
    inserts: List[Plan] = []
    updates: List[Dict[str, Any]] = []
    skips: List[Dict[str, Any]] = []

    def _usable(row: Dict[str, Any]) -> bool:
        if row.get("id") in used:
            return False
        key = str(row.get("broker_sync_key") or "")
        if key and not key.startswith(prefix):
            return False
        return str(row.get("symbol_mapped") or "").strip().upper() == plan_symbol

    for plan in plans:
        plan_symbol = plan.symbol_mapped.strip().upper()
        row = by_key.get(plan.sync_key)
        if row is None:
            candidates = []
            for item in existing:
                if not _usable(item):
                    continue
                if str(item.get("broker_sync_key") or "") not in ("", plan.sync_key):
                    continue
                if not _contracts_compatible(item, plan):
                    continue
                status = str(item.get("status") or "")
                direction = str(item.get("direction") or "").upper()
                if plan.open_trade:
                    if status != "In-Trade":
                        continue
                else:
                    entry_day = _day_of(item.get("trade_taken_at"))
                    if status == "History":
                        if direction != plan.direction:
                            continue
                        if entry_day and entry_day != plan.entry_at[:10]:
                            continue
                    elif status == "In-Trade":
                        if direction != plan.direction:
                            continue
                    else:
                        continue
                candidates.append(item)
            if plan.open_trade:
                price_hits = [item for item in candidates if _near(item.get("entry_price"), plan.entry_price)]
                pool = price_hits or candidates
            else:
                history_hits = [
                    item
                    for item in candidates
                    if str(item.get("status") or "") == "History"
                    and _near(item.get("entry_price"), plan.entry_price)
                ]
                open_hits = [item for item in candidates if str(item.get("status") or "") == "In-Trade"]
                pool = history_hits or open_hits
            if pool:
                pool.sort(
                    key=lambda item: (
                        0 if str(item.get("status")) == ("In-Trade" if plan.open_trade else "History") else 1,
                        item.get("id") or 0,
                    )
                )
                row = pool[0]
        if row is not None:
            used.add(row.get("id"))
            if _same_economics(row, plan) and str(row.get("broker_sync_key") or "") == plan.sync_key:
                skips.append({"id": row.get("id"), "plan": plan, "stamp": False})
            elif _same_economics(row, plan):
                skips.append({"id": row.get("id"), "plan": plan, "stamp": True})
            else:
                close = (not plan.open_trade) and str(row.get("status") or "") != "History"
                updates.append({"id": row.get("id"), "plan": plan, "close": close})
        else:
            inserts.append(plan)

    deletes = []
    for row in existing:
        key = str(row.get("broker_sync_key") or "")
        if key.startswith(prefix) and row.get("id") not in used:
            status = str(row.get("status") or "")
            if status in ("In-Trade", "History"):
                deletes.append(row.get("id"))
    return {"inserts": inserts, "updates": updates, "skips": skips, "deletes": deletes}


def _load_existing() -> List[Dict[str, Any]]:
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, symbol_raw, symbol_mapped, contract, instrument_key, direction,
                       status, entry_price, exit_price, trade_taken_at, exit_at, lot_size,
                       trade_mode, broker_sync_key
                FROM commodities_div_signals
                WHERE status <> 'Rejected'
                """
            )
        ).mappings().all()
        return [dict(row) for row in rows]
    finally:
        db.close()


def _stamp_key(signal_id: int, plan: Plan) -> None:
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE commodities_div_signals
                SET broker_sync_key = :key,
                    contract = COALESCE(:contract, contract),
                    instrument_key = COALESCE(:ik, instrument_key),
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {
                "key": plan.sync_key,
                "contract": plan.contract,
                "ik": plan.instrument_key,
                "id": int(signal_id),
            },
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _apply(result: Dict[str, Any]) -> Dict[str, Any]:
    created = 0
    updated = 0
    skipped = 0
    removed = 0
    warnings: List[str] = []
    in_trade = 0
    history = 0

    def _count(plan: Plan) -> None:
        nonlocal in_trade, history
        if plan.open_trade:
            in_trade += 1
        else:
            history += 1

    for signal_id in result["deletes"]:
        try:
            delete_signal(signal_id=int(signal_id))
            removed += 1
        except Exception as exc:
            warnings.append(f"Could not refresh an older sync row: {exc}")

    for item in result["updates"]:
        plan: Plan = item["plan"]
        try:
            if item["close"]:
                exit_submit(
                    signal_id=int(item["id"]),
                    exit_price=float(plan.exit_price or 0),
                    exit_at=plan.exit_at,
                    entry_price=plan.entry_price,
                    trade_taken_at=plan.entry_at,
                    direction=plan.direction,
                    trade_mode="LIVE",
                    qty=plan.qty,
                )
            else:
                update_signal(
                    signal_id=int(item["id"]),
                    entry_price=plan.entry_price,
                    trade_taken_at=plan.entry_at,
                    direction=plan.direction,
                    exit_price=None if plan.open_trade else plan.exit_price,
                    exit_at=None if plan.open_trade else plan.exit_at,
                    trade_mode="LIVE",
                    qty=plan.qty,
                )
            _stamp_key(int(item["id"]), plan)
            updated += 1
            _count(plan)
        except Exception as exc:
            warnings.append(f"{plan.symbol_raw}: {exc}")

    for item in result["skips"]:
        plan = item["plan"]
        if item.get("stamp"):
            try:
                _stamp_key(int(item["id"]), plan)
            except Exception as exc:
                warnings.append(f"{plan.symbol_raw}: {exc}")
                continue
        skipped += 1
        _count(plan)

    for plan in result["inserts"]:
        try:
            row = create_manual_history(
                commodity=plan.symbol_raw,
                direction=plan.direction,
                entry_price=plan.entry_price,
                trade_taken_at=plan.entry_at,
                exit_price=None if plan.open_trade else plan.exit_price,
                exit_at=None if plan.open_trade else plan.exit_at,
                trade_mode="LIVE",
                qty=plan.qty,
                mapped_symbol=plan.mapped_override,
            )
            _stamp_key(int(row["id"]), plan)
            created += 1
            _count(plan)
        except IntegrityError:
            warnings.append(
                f"{plan.symbol_mapped} already has an open Commodities Div signal. "
                "Close or reject it, then sync again."
            )
        except Exception as exc:
            warnings.append(f"{plan.symbol_raw}: {exc}")

    return {
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "removed": removed,
        "in_trade": in_trade,
        "history": history,
        "warnings": warnings,
    }


def _row_to_fill(row: Dict[str, Any], trade_day: date, *, segment_hint: str = "") -> Optional[Fill]:
    if not isinstance(row, dict) or not _is_mcx(row, segment_hint=segment_hint):
        return None
    status = str(_first(row, "status", "order_status", "orderStatus") or "").strip().lower()
    if status in _SKIP_STATUS:
        return None
    traded_qty = _num(_first(row, "filled_quantity", "tradedQuantity"))
    if status and status not in _COMPLETE and "complete" not in status and traded_qty <= 0:
        return None
    side = str(_first(row, "transaction_type", "transactionType", "side") or "").strip().upper()
    if side not in ("BUY", "SELL"):
        return None
    qty_raw = _num(_first(row, "filled_quantity", "tradedQuantity", "quantity", "qty"))
    qty = int(round(qty_raw))
    price = _num(
        _first(row, "average_price", "tradedPrice", "price", "trade_price", "averagePrice")
    )
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
        security = str(_first(row, "securityId", "security_id") or "").strip()
        symbol = f"MCX:{security}" if security else ""
    if not symbol:
        return None
    option_type = str(_first(row, "option_type", "drvOptionType") or "")
    option = _is_option_symbol(symbol, option_type)
    token = str(_first(row, "instrument_token", "instrument_key", "instrumentToken") or "").strip()
    if token.isdigit():
        token = f"MCX_FO|{token}"
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
    if stamp.date() != trade_day and str(_first(row, "trade_date") or "")[:10] not in ("", trade_day.isoformat()):
        row_day = str(_first(row, "trade_date") or "")[:10]
        if row_day and row_day != trade_day.isoformat():
            return None
        if stamp.date() != trade_day:
            return None
    fill_id = str(
        _first(row, "trade_id", "exchangeTradeId", "order_id", "orderId")
        or f"{stamp.isoformat()}|{side}|{qty}|{price}"
    ).strip()
    instrument = re.sub(r"\s+", "", symbol).upper()
    return Fill(
        instrument=instrument,
        symbol_raw=symbol,
        side=side,
        qty=qty,
        price=price,
        ts=stamp,
        fill_id=fill_id,
        instrument_key=token or None,
        option=option,
    )


def _apply_lot_size(fills: List[Fill]) -> None:
    if not fills:
        return
    try:
        from backend.services.commodities_div.mapping import attach_instrument_fields
    except Exception:
        return
    cache: Dict[str, Optional[int]] = {}
    for fill in fills:
        if fill.option:
            continue
        key = fill.symbol_raw
        if key not in cache:
            inst = None
            lot = 0
            try:
                inst = attach_instrument_fields(key, resolve_contract=True)
                lot = int(inst.get("lot_size") or 0) if inst else 0
            except Exception:
                inst = None
                lot = 0
            cache[key] = lot or None
            if isinstance(inst, dict) and inst.get("instrument_key") and not fill.instrument_key:
                fill.instrument_key = str(inst.get("instrument_key"))
        fill.qty = scale_traded_qty(fill.qty, cache.get(key))


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


def _fills_from_rows(rows: Sequence[Dict[str, Any]], trade_day: date, *, segment_hint: str = "") -> List[Fill]:
    fills: List[Fill] = []
    for row in rows:
        fill = _row_to_fill(row, trade_day, segment_hint=segment_hint)
        if fill:
            fills.append(fill)
    return fills


def _merge_fills(primary: Sequence[Fill], extra: Sequence[Fill]) -> List[Fill]:
    """Keep trade-book fills, then order-book fills that are not the same minute."""
    merged = list(primary)
    covered = {(fill.instrument, fill.side, fill.ts.strftime("%Y-%m-%d %H:%M")) for fill in primary}
    for fill in extra:
        key = (fill.instrument, fill.side, fill.ts.strftime("%Y-%m-%d %H:%M"))
        if key in covered:
            continue
        covered.add(key)
        merged.append(fill)
    return merged


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
    return text_msg[:300]


def _fetch_upstox(trade_day: date) -> List[Fill]:
    from backend.services.upstox_service import upstox_service

    ux = upstox_service
    if not getattr(ux, "access_token", None):
        try:
            ux.reload_token_from_storage()
        except Exception:
            logger.info("commodities_div broker sync: upstox token reload failed")
    if not getattr(ux, "access_token", None):
        raise BrokerSyncError(
            "Upstox access token is missing or expired. Sign in to Upstox and try again."
        )
    raw_rows: List[Dict[str, Any]] = []
    segment_hint = ""
    if trade_day == datetime.now(IST).date():
        response = ux.make_api_request(
            url="https://api.upstox.com/v2/order/trades/get-trades-for-day",
            method="GET",
            timeout=20,
        )
        trade_rows: List[Dict[str, Any]] = []
        trades_ok = isinstance(response, dict) and response.get("status") == "success"
        if trades_ok:
            trade_rows = _payload_rows(response)
        book = ux.get_order_book_today()
        book_ok = isinstance(book, dict) and book.get("success")
        order_rows = book.get("orders") if book_ok and isinstance(book.get("orders"), list) else []
        if not trades_ok and not book_ok:
            err = book.get("error") if isinstance(book, dict) else None
            raise BrokerSyncError(
                _upstox_message(response if isinstance(response, dict) else {"message": err or "Upstox order book failed"})
            )
        fills = _merge_fills(
            _fills_from_rows(trade_rows, trade_day),
            _fills_from_rows(order_rows, trade_day),
        )
        _apply_lot_size(fills)
        return fills
    else:
        segment_hint = "COM"
        page = 1
        total_pages = 1
        while page <= total_pages and page <= 40:
            response = ux.make_api_request(
                url="https://api.upstox.com/v2/charges/historical-trades",
                method="GET",
                params={
                    "segment": "COM",
                    "start_date": trade_day.isoformat(),
                    "end_date": trade_day.isoformat(),
                    "page_number": page,
                    "page_size": 500,
                },
                timeout=20,
            )
            if not isinstance(response, dict) or response.get("status") != "success":
                if page == 1 and not raw_rows:
                    raise BrokerSyncError(_upstox_message(response))
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
        fill = _row_to_fill(row, trade_day, segment_hint=segment_hint)
        if fill:
            fills.append(fill)
    _apply_lot_size(fills)
    return fills


def _fetch_dhan(trade_day: date) -> List[Fill]:
    from backend.services.dhan_service import DhanApiError, DhanAuthError, dhan_service

    try:
        rows = dhan_service.get_trades_for_date(trade_day.isoformat())
    except DhanAuthError as exc:
        raise BrokerSyncError(str(exc), login_url=getattr(exc, "login_url", None)) from exc
    except DhanApiError as exc:
        raise BrokerSyncError(str(exc) or "Dhan trade request failed.") from exc
    fills = []
    for row in rows:
        fill = _row_to_fill(row, trade_day, segment_hint="")
        if fill:
            fills.append(fill)
    _apply_lot_size(fills)
    return fills


def _summary(broker: str, trade_day: date, mcx_count: int, applied: Dict[str, Any]) -> Dict[str, Any]:
    label = "Upstox" if broker == "upstox" else "Dhan"
    warnings = list(applied.get("warnings") or [])
    placed = int(applied["in_trade"]) + int(applied["history"])
    if mcx_count == 0:
        message = f"No MCX trades on {trade_day.isoformat()} for {label}."
    else:
        message = (
            f"Synced {placed} MCX position(s) from {label}: "
            f"{applied['in_trade']} in trade, {applied['history']} history."
        )
        if applied["skipped"]:
            message += f" {applied['skipped']} already matched."
        if applied["created"] or applied["updated"]:
            message += f" {applied['created']} added, {applied['updated']} updated."
    if warnings:
        message = message + " " + " ".join(warnings)
    return {
        "ok": True,
        "broker": broker,
        "date": trade_day.isoformat(),
        "mcx_trades": mcx_count,
        "created": applied["created"],
        "updated": applied["updated"],
        "skipped": applied["skipped"],
        "removed": applied["removed"],
        "in_trade": applied["in_trade"],
        "history": applied["history"],
        "warnings": warnings,
        "message": message.strip(),
    }


def sync_from_broker(trade_date: Any, broker: Any, *, fills: Optional[Sequence[Fill]] = None) -> Dict[str, Any]:
    """Fetch MCX fills for one day and write In-Trade / History rows."""
    day = parse_trade_date(trade_date)
    name = normalize_broker(broker)
    if fills is None:
        loaded = _fetch_upstox(day) if name == "upstox" else _fetch_dhan(day)
    else:
        loaded = list(fills)
    if not loaded:
        return _summary(
            name,
            day,
            0,
            {
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "removed": 0,
                "in_trade": 0,
                "history": 0,
                "warnings": [],
            },
        )
    plans = plans_from_fills(loaded, broker=name, trade_day=day)
    if not plans:
        return _summary(
            name,
            day,
            len(loaded),
            {
                "created": 0,
                "updated": 0,
                "skipped": 0,
                "removed": 0,
                "in_trade": 0,
                "history": 0,
                "warnings": ["MCX trades were found but none had a price and quantity."],
            },
        )
    existing = _load_existing()
    plan_result = reconcile(plans, existing, broker=name, trade_day=day)
    applied = _apply(plan_result)
    logger.info(
        "commodities_div broker sync %s %s fills=%s created=%s updated=%s skipped=%s",
        name,
        day.isoformat(),
        len(loaded),
        applied["created"],
        applied["updated"],
        applied["skipped"],
    )
    return _summary(name, day, len(loaded), applied)

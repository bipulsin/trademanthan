"""Pair Delta India BTC option fills into one short straddle per evening.

Automation id (``meta_data.se_id``) is reused every day, so it is not a trade id.
A trade is one ATM straddle:

- CE sell + PE sell of the same strike and expiry, clustered around 18:00 IST
- CE buy + PE buy of those same contracts after the sells and before 17:30 IST
  the next calendar day

Sells outside the evening window are ignored so a later session is not merged
into the previous one.
"""
from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pytz

IST = pytz.timezone("Asia/Kolkata")

# Evening entry cluster around the 18:00 IST automation (observed 18:00–18:30).
SELL_WINDOW_START = time(17, 0)
SELL_WINDOW_END = time(20, 0)
# Buyback must be strictly before 17:30 IST the next calendar day.
BUY_CUTOFF = time(17, 30)

_SYMBOL_RE = re.compile(r"^(C|P)-BTC-(\d+(?:\.\d+)?)-(\d{6})$", re.IGNORECASE)

def _as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_timestamp(value: Any) -> Optional[datetime]:
    """UTC datetime from an ISO string or epoch seconds/ms/us."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e14:
            v = v / 1_000_000.0
        elif v > 1e11:
            v = v / 1_000.0
        return datetime.fromtimestamp(v, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return parse_timestamp(int(text))
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_ist(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)


def parse_btc_option_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    match = _SYMBOL_RE.match((symbol or "").strip())
    if not match:
        return None
    right = "CE" if match.group(1).upper() == "C" else "PE"
    try:
        expiry = datetime.strptime(match.group(3), "%d%m%y").date()
    except ValueError:
        return None
    return {
        "symbol": f"{match.group(1).upper()}-BTC-{match.group(2)}-{match.group(3)}",
        "right": right,
        "strike": float(match.group(2)),
        "expiry": expiry,
    }


def _automation_id(raw: Dict[str, Any]) -> str:
    """Stable automation tag. Reused across days — never a trade key by itself."""
    meta = raw.get("meta_data") if isinstance(raw.get("meta_data"), dict) else {}
    if not meta and isinstance(raw.get("metadata"), dict):
        meta = raw["metadata"]
    for key in ("se_id", "automation_id", "automationId", "algo_id", "strategy_id"):
        val = raw.get(key)
        if val is None:
            val = meta.get(key)
        if val is not None and str(val).strip() and str(val).strip().lower() != "none":
            return str(val).strip()
    return ""


def _contract_value(raw: Dict[str, Any]) -> Optional[float]:
    direct = _as_float(raw.get("contract_value"))
    if direct is not None and direct > 0:
        return direct
    product = raw.get("product") if isinstance(raw.get("product"), dict) else {}
    cv = _as_float(product.get("contract_value"))
    if cv is not None and cv > 0:
        return cv
    return None


def _quote_currency(raw: Dict[str, Any]) -> Optional[str]:
    for key in ("settling_asset_symbol", "quoting_asset_symbol", "quote_currency"):
        val = raw.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip().upper()
    product = raw.get("product") if isinstance(raw.get("product"), dict) else {}
    for key in ("settling_asset", "quoting_asset"):
        asset = product.get(key)
        if isinstance(asset, dict):
            sym = asset.get("symbol")
            if isinstance(sym, str) and sym.strip():
                return sym.strip().upper()
        elif isinstance(asset, str) and asset.strip():
            return asset.strip().upper()
    return None


def normalize_fill(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Return a BTC option fill, or None when the row is not one."""
    if not isinstance(raw, dict):
        return None
    symbol = str(raw.get("product_symbol") or raw.get("symbol") or "").strip()
    parsed = parse_btc_option_symbol(symbol)
    if not parsed:
        return None
    side = str(raw.get("side") or "").strip().lower()
    if side not in ("buy", "sell"):
        return None
    when = parse_timestamp(raw.get("created_at") or raw.get("timestamp") or raw.get("time"))
    price = _as_float(raw.get("price"))
    size = _as_float(raw.get("size"))
    if when is None or price is None or size is None or size <= 0:
        return None
    fill_id = raw.get("id")
    return {
        "id": "" if fill_id is None else str(fill_id),
        "symbol": parsed["symbol"],
        "right": parsed["right"],
        "strike": parsed["strike"],
        "expiry": parsed["expiry"],
        "side": side,
        "price": price,
        "size": size,
        "created_at": when,
        "ist": to_ist(when),
        "automation_id": _automation_id(raw),
        "contract_value": _contract_value(raw),
        "quote_currency": _quote_currency(raw),
        "underlying": "BTC",
    }


def in_sell_window(ist_dt: datetime) -> bool:
    clock = ist_dt.time().replace(tzinfo=None)
    return SELL_WINDOW_START <= clock <= SELL_WINDOW_END


def buy_deadline(session_date: date) -> datetime:
    """17:30 IST on the calendar day after the sell session. Exclusive."""
    nxt = session_date + timedelta(days=1)
    return IST.localize(datetime.combine(nxt, BUY_CUTOFF))


def _vwap(fills: Sequence[Dict[str, Any]]) -> Tuple[float, float]:
    size = sum(float(f["size"]) for f in fills)
    if size <= 0:
        return 0.0, 0.0
    price = sum(float(f["price"]) * float(f["size"]) for f in fills) / size
    return price, size


def _fill_ids(fills: Sequence[Dict[str, Any]]) -> str:
    ids = [str(f["id"]) for f in fills if f.get("id")]
    return ",".join(ids)


def _cash(price: Optional[float], size: Optional[float], contract_value: Optional[float]) -> Optional[float]:
    if price is None or size is None or contract_value is None:
        return None
    return float(price) * float(size) * float(contract_value)


def _leg_exit_cash(trade: Dict[str, Any], leg: str) -> Optional[float]:
    """Cash debit to close one short leg. Open residual is marked; a full buy uses the fill."""
    cv = trade.get("contract_value")
    sell_size = _as_float(trade.get(f"{leg}_size"))
    if cv is None or sell_size is None:
        return None
    buy_price = _as_float(trade.get(f"{leg}_buy_price"))
    buy_size = _as_float(trade.get(f"{leg}_buy_size")) or 0.0
    mark = _as_float(trade.get(f"{leg}_mark"))
    if trade.get("status") == "CLOSED":
        if buy_price is None:
            return None
        return buy_price * sell_size * float(cv)
    covered = min(sell_size, buy_size) if buy_price is not None else 0.0
    residual = max(sell_size - covered, 0.0)
    cash = 0.0
    if covered:
        cash += buy_price * covered * float(cv)
    if residual:
        if mark is None:
            return None
        cash += mark * residual * float(cv)
    return cash


def recompute_premiums(trade: Dict[str, Any]) -> Dict[str, Any]:
    """Credit straddle: profit when the buyback (or mark) is below the sell premium."""
    cv = trade.get("contract_value")
    ce_entry = _cash(trade.get("ce_sell_price"), trade.get("ce_size"), cv)
    pe_entry = _cash(trade.get("pe_sell_price"), trade.get("pe_size"), cv)
    if ce_entry is None or pe_entry is None:
        trade["entry_premium"] = None
    else:
        trade["entry_premium"] = ce_entry + pe_entry

    ce_exit = _leg_exit_cash(trade, "ce")
    pe_exit = _leg_exit_cash(trade, "pe")
    if ce_exit is None or pe_exit is None or trade.get("entry_premium") is None:
        trade["exit_premium"] = None
        trade["pnl"] = None
    else:
        trade["exit_premium"] = ce_exit + pe_exit
        trade["pnl"] = trade["entry_premium"] - trade["exit_premium"]
    return trade


def apply_open_marks(trades: Iterable[Dict[str, Any]], marks: Dict[str, float]) -> List[Dict[str, Any]]:
    """Stamp live marks on open short legs and refresh PnL. Closed rows keep fill prices."""
    out: List[Dict[str, Any]] = []
    for trade in trades:
        row = dict(trade)
        if row.get("status") != "CLOSED":
            ce = marks.get(row.get("ce_symbol") or "")
            pe = marks.get(row.get("pe_symbol") or "")
            if ce is not None:
                row["ce_mark"] = float(ce)
            if pe is not None:
                row["pe_mark"] = float(pe)
        recompute_premiums(row)
        out.append(row)
    return out


def _group_sells(sells: Sequence[Dict[str, Any]]) -> Dict[Tuple[date, float, date, str, str], List[Dict[str, Any]]]:
    """Group evening sells by session, strike, expiry, right, automation id."""
    groups: Dict[Tuple[date, float, date, str, str], List[Dict[str, Any]]] = {}
    for fill in sells:
        if not in_sell_window(fill["ist"]):
            continue
        key = (
            fill["ist"].date(),
            float(fill["strike"]),
            fill["expiry"],
            fill["right"],
            fill.get("automation_id") or "",
        )
        groups.setdefault(key, []).append(fill)
    return groups


def _buy_matches(buy: Dict[str, Any], *, symbol: str, automation_id: str, entry: datetime, deadline: datetime) -> bool:
    if buy["symbol"] != symbol or buy["side"] != "buy":
        return False
    when = buy["created_at"]
    if when <= entry or when >= deadline:
        return False
    buy_auto = buy.get("automation_id") or ""
    if buy_auto and automation_id and buy_auto != automation_id:
        return False
    return True


def pair_straddle_fills(raw_fills: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Pair fills into one row per straddle. Incomplete buybacks stay OPEN with a null exit."""
    normalized: List[Dict[str, Any]] = []
    for raw in raw_fills:
        row = normalize_fill(raw) if isinstance(raw, dict) else None
        if row:
            normalized.append(row)

    sells = [f for f in normalized if f["side"] == "sell"]
    buys = sorted((f for f in normalized if f["side"] == "buy"), key=lambda f: f["created_at"])
    groups = _group_sells(sells)

    pair_keys = []
    seen = set()
    for (session, strike, expiry, right, auto) in groups:
        if right != "CE":
            continue
        pe_key = (session, strike, expiry, "PE", auto)
        if pe_key not in groups:
            continue
        ident = (session, strike, expiry, auto)
        if ident in seen:
            continue
        seen.add(ident)
        pair_keys.append(ident)

    pair_keys.sort(key=lambda k: (k[0], k[3], k[1]))
    used_buy_ids: set = set()
    trades: List[Dict[str, Any]] = []

    for session, strike, expiry, auto in pair_keys:
        ce_fills = sorted(groups[(session, strike, expiry, "CE", auto)], key=lambda f: f["created_at"])
        pe_fills = sorted(groups[(session, strike, expiry, "PE", auto)], key=lambda f: f["created_at"])
        entry = min(ce_fills[0]["created_at"], pe_fills[0]["created_at"])
        deadline = buy_deadline(session)
        ce_symbol = ce_fills[0]["symbol"]
        pe_symbol = pe_fills[0]["symbol"]

        def take(symbol: str) -> List[Dict[str, Any]]:
            chosen = []
            for buy in buys:
                bid = buy.get("id") or id(buy)
                if bid in used_buy_ids:
                    continue
                if _buy_matches(buy, symbol=symbol, automation_id=auto, entry=entry, deadline=deadline):
                    chosen.append(buy)
                    used_buy_ids.add(bid)
            return chosen

        ce_buys = take(ce_symbol)
        pe_buys = take(pe_symbol)
        ce_sell_px, ce_size = _vwap(ce_fills)
        pe_sell_px, pe_size = _vwap(pe_fills)
        ce_buy_px = pe_buy_px = None
        ce_buy_sz = pe_buy_sz = 0.0
        if ce_buys:
            ce_buy_px, ce_buy_sz = _vwap(ce_buys)
        if pe_buys:
            pe_buy_px, pe_buy_sz = _vwap(pe_buys)

        closed = (
            ce_buy_px is not None
            and pe_buy_px is not None
            and ce_buy_sz + 1e-9 >= ce_size
            and pe_buy_sz + 1e-9 >= pe_size
        )
        exit_time = max((f["created_at"] for f in ce_buys + pe_buys), default=None) if closed else None

        currencies = [f.get("quote_currency") for f in ce_fills + pe_fills if f.get("quote_currency")]
        cvs = [f.get("contract_value") for f in ce_fills + pe_fills if f.get("contract_value")]
        trade = {
            "underlying": "BTC",
            "session_date": session,
            "expiry": expiry,
            "strike": strike,
            "ce_symbol": ce_symbol,
            "pe_symbol": pe_symbol,
            "automation_id": auto,
            "quote_currency": currencies[0] if currencies else None,
            "contract_value": cvs[0] if cvs else None,
            "ce_sell_price": ce_sell_px,
            "pe_sell_price": pe_sell_px,
            "ce_buy_price": ce_buy_px if closed or ce_buys else None,
            "pe_buy_price": pe_buy_px if closed or pe_buys else None,
            "ce_mark": None,
            "pe_mark": None,
            "ce_size": ce_size,
            "pe_size": pe_size,
            "ce_buy_size": ce_buy_sz if ce_buys else None,
            "pe_buy_size": pe_buy_sz if pe_buys else None,
            "entry_time": entry,
            "exit_time": exit_time,
            "status": "CLOSED" if closed else "OPEN",
            "ce_sell_fill_id": _fill_ids(ce_fills),
            "pe_sell_fill_id": _fill_ids(pe_fills),
            "ce_buy_fill_id": _fill_ids(ce_buys),
            "pe_buy_fill_id": _fill_ids(pe_buys),
        }
        recompute_premiums(trade)
        trades.append(trade)

    trades.sort(key=lambda t: (t["session_date"], t["entry_time"]), reverse=True)
    return trades

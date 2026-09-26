"""Manual multi-leg options journal (Straddle, Iron Fly, Iron Condor).

PnL sign
--------
direction_sign is +1 for BUY and -1 for SELL.

    leg_pnl = (exit_or_ltp - entry_price) * direction_sign * lot_size

A long option profits when the premium rises: (ltp - entry) * lot.
A short option profits when the premium falls: (entry - ltp) * lot,
which is the same formula with direction_sign = -1.

Trade PnL is the sum of leg PnLs. A leg with both exit_price and exit_time
is marked at the exit price. Other legs stay marked to LTP. Saving a partial
exit leaves the trade ACTIVE. Close requires every leg to be exited.
"""
from __future__ import annotations

import calendar
import logging
import re
import threading
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.upstox_service import NSE_KNOWN_HOLIDAYS

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

TRADE_TYPES = ("STRADDLE", "IRON_FLY", "IRON_CONDOR")
# UNCLASSIFIED is written by Upstox sync (and kept on edit). Manual create still
# goes through assert_min_legs, which only accepts TRADE_TYPES.
ACCEPTED_TRADE_TYPES = TRADE_TYPES + ("UNCLASSIFIED",)
MIN_LEGS = {"STRADDLE": 2, "IRON_FLY": 4, "IRON_CONDOR": 4}
SIDES = ("BUY", "SELL")
OPTION_TYPES = ("CE", "PE")
STATUSES = ("ACTIVE", "CLOSED")

# Same weekday as UpstoxService.get_monthly_expiry (last Tuesday, 2025+ NSE monthly).
_NSE_EXPIRY_WEEKDAY = 1  # Tuesday
# No MCX monthly-option calendar function exists in this repo. When the
# instrument master has no listed expiries, fall back to last Thursday.
_MCX_EXPIRY_WEEKDAY = 3  # Thursday

INDEX_INSTRUMENTS = ("NIFTY", "BANKNIFTY", "SENSEX")
_BSE_INDEXES = {"SENSEX", "BANKEX"}

_MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "add_multi_leg_trades.sql"
_ENSURED = False
_MASTER_LOCK = threading.Lock()
_MASTER_CACHE: Dict[str, Any] = {"rows_by_und": {}, "loaded_at": 0.0}
_MASTER_TTL = 600.0


class MultiLegValidationError(ValueError):
    """User-facing journal validation failure (HTTP 400)."""


class MultiLegNotFound(MultiLegValidationError):
    """Missing trade (HTTP 404)."""


def direction_sign(side: str) -> int:
    """BUY = +1, SELL = -1. See module docstring."""
    s = str(side or "").strip().upper()
    if s == "BUY":
        return 1
    if s == "SELL":
        return -1
    raise MultiLegValidationError("side must be BUY or SELL")


def leg_pnl(
    side: str,
    entry_price: Any,
    mark: Any,
    lot_size: Any,
) -> Optional[float]:
    """(mark - entry) × direction_sign × lot. None when the mark is missing."""
    entry = _as_float(entry_price)
    px = _as_float(mark)
    lot = _as_int(lot_size)
    if entry is None or px is None or lot is None or lot <= 0:
        return None
    return (px - entry) * direction_sign(side) * lot


def trade_pnl(leg_pnls: Iterable[Optional[float]]) -> Optional[float]:
    vals = [float(p) for p in leg_pnls if p is not None]
    if not vals:
        return None
    return float(sum(vals))


def assert_min_legs(trade_type: str, leg_count: int) -> None:
    kind = str(trade_type or "").strip().upper()
    if kind not in MIN_LEGS:
        raise MultiLegValidationError(
            "trade_type must be STRADDLE, IRON_FLY, or IRON_CONDOR"
        )
    need = MIN_LEGS[kind]
    n = int(leg_count)
    if n < need:
        raise MultiLegValidationError(
            f"{kind} needs at least {need} legs (got {n})"
        )


def leg_is_exited(leg: Dict[str, Any]) -> bool:
    """Closed for PnL only when both exit price and exit time are present."""
    return _as_float(leg.get("exit_price")) is not None and _parse_dt(leg.get("exit_time")) is not None


def close_block_reason(legs: Sequence[Dict[str, Any]]) -> Optional[str]:
    if not legs:
        return "trade has no legs"
    missing = [i for i, leg in enumerate(legs, start=1) if not leg_is_exited(leg)]
    if missing:
        return (
            "close requires every leg to have exit_price and exit_time; "
            f"missing on leg(s) {', '.join(str(i) for i in missing)}"
        )
    return None


def assert_ready_to_close(legs: Sequence[Dict[str, Any]]) -> None:
    reason = close_block_reason(legs)
    if reason:
        raise MultiLegValidationError(reason)


def status_after_save(*, closing: bool, legs: Sequence[Dict[str, Any]]) -> str:
    """Edit / partial exit stays ACTIVE. Only an explicit close flips to CLOSED."""
    if closing:
        assert_ready_to_close(legs)
        return "CLOSED"
    return "ACTIVE"


def mark_price(leg: Dict[str, Any]) -> Optional[float]:
    if leg_is_exited(leg):
        return _as_float(leg.get("exit_price"))
    return _as_float(leg.get("ltp"))


def leg_pnl_from_row(leg: Dict[str, Any]) -> Optional[float]:
    return leg_pnl(leg.get("side"), leg.get("entry_price"), mark_price(leg), leg.get("lot_size"))


def _as_float(raw: Any) -> Optional[float]:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _uuid_or_none(raw: Any) -> Optional[str]:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return str(uuid.UUID(str(raw).strip()))
    except (ValueError, AttributeError):
        return None


def _as_int(raw: Any) -> Optional[int]:
    f = _as_float(raw)
    if f is None:
        return None
    return int(f)


def _parse_date(raw: Any) -> Optional[date]:
    if raw is None or str(raw).strip() == "":
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is not None:
            return raw.astimezone(IST).date()
        return raw.date()
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


_CLOCK_AMPM = re.compile(
    r"^(?P<h>\d{1,2}):(?P<m>\d{2})\s*(?P<ap>AM|PM)$",
    re.IGNORECASE,
)
_DATE_CLOCK_AMPM = re.compile(
    r"^(?P<d>\d{4}-\d{2}-\d{2})[T ](?P<clock>\d{1,2}:\d{2}\s*[AaPp][Mm])$"
)


def parse_clock_ampm(text: str) -> Optional[Tuple[int, int]]:
    """Parse ``hh:mm AM/PM`` into 24-hour ``(hour, minute)``. ``03:10 PM`` is 15:10."""
    match = _CLOCK_AMPM.match(str(text or "").strip())
    if not match:
        return None
    hour = int(match.group("h"))
    minute = int(match.group("m"))
    if hour < 1 or hour > 12 or minute > 59:
        return None
    if match.group("ap").upper() == "AM":
        hour24 = 0 if hour == 12 else hour
    else:
        hour24 = 12 if hour == 12 else hour + 12
    return hour24, minute


def _parse_dt(raw: Any) -> Optional[datetime]:
    if raw is None or str(raw).strip() == "":
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return IST.localize(raw)
        return raw.astimezone(IST)
    s = str(raw).strip().replace("Z", "+00:00")
    ampm = _DATE_CLOCK_AMPM.match(s)
    if ampm:
        clock = parse_clock_ampm(ampm.group("clock"))
        day = _parse_date(ampm.group("d"))
        if clock is None or day is None:
            return None
        return IST.localize(datetime(day.year, day.month, day.day, clock[0], clock[1]))
    try:
        if "T" not in s and " " in s:
            s = s.replace(" ", "T", 1)
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _iso_dt(raw: Any) -> Optional[str]:
    dt = _parse_dt(raw)
    if dt is None:
        return None
    return dt.isoformat()


def _next_month(year: int, month: int) -> Tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Last occurrence of weekday (Mon=0) in the calendar month."""
    last_day = calendar.monthrange(year, month)[1]
    cursor = date(year, month, last_day)
    while cursor.weekday() != weekday:
        cursor -= timedelta(days=1)
    return cursor


def _holiday_set(year: int) -> set:
    return set(NSE_KNOWN_HOLIDAYS.get(year) or [])


def _shift_off_nse_holiday(expiry: date) -> date:
    """Same previous-session walk as UpstoxService.get_monthly_expiry, offline.

    Uses NSE_KNOWN_HOLIDAYS (the helper's fallback list) plus weekends so tests
    do not call the holiday API.
    """
    for _ in range(10):
        if expiry.weekday() < 5 and expiry.isoformat() not in _holiday_set(expiry.year):
            return expiry
        expiry -= timedelta(days=1)
    return expiry


def is_mcx_instrument(instrument: str) -> bool:
    return str(instrument or "").strip().upper() in set(mcx_underlyings())


def next_monthly_option_expiry(entry: date, instrument: str) -> date:
    """Next monthly option expiry on or after ``entry``.

    NSE index/stock and SENSEX: last Tuesday of the month, shifted to the
    previous session when that Tuesday is in ``NSE_KNOWN_HOLIDAYS`` (same rule
    as ``UpstoxService.get_monthly_expiry``). The month itself is NOT chosen
    with that helper's day<=18 cutoff. If ``entry`` is after this month's
    expiry, the next month is used.

    SENSEX has no separate expiry helper in the repo, so it follows the same
    last-Tuesday rule.

    MCX: no monthly option calendar exists in the repo. This calendar fallback
    is last Thursday, same roll-forward rule. The API default prefers a listed
    master expiry when one is available (see ``suggested_expiry``).
    """
    if not isinstance(entry, date):
        raise MultiLegValidationError("entry date is required")
    sym = str(instrument or "").strip().upper()
    weekday = _MCX_EXPIRY_WEEKDAY if is_mcx_instrument(sym) else _NSE_EXPIRY_WEEKDAY
    adjust = weekday == _NSE_EXPIRY_WEEKDAY
    expiry = last_weekday_of_month(entry.year, entry.month, weekday)
    if adjust:
        expiry = _shift_off_nse_holiday(expiry)
    if entry > expiry:
        year, month = _next_month(entry.year, entry.month)
        expiry = last_weekday_of_month(year, month, weekday)
        if adjust:
            expiry = _shift_off_nse_holiday(expiry)
    return expiry


def mcx_underlyings() -> List[str]:
    """Desk + Upstox commodity names already enumerated in the repo."""
    from backend.services.commodities_div.mapping import (
        ALLOWED_UNDERLYINGS,
        UPSTOX_RESOLVE_ALIASES,
    )
    from backend.services.tarang.adapters.upstox_mcx import _ALL_ENERGY

    names = set(ALLOWED_UNDERLYINGS)
    names.update(_ALL_ENERGY)
    for aliases in UPSTOX_RESOLVE_ALIASES.values():
        names.update(str(a).strip().upper() for a in aliases if a)
    return sorted(n for n in names if n)


def underlying_candidates(instrument: str) -> List[str]:
    from backend.services.commodities_div.mapping import UPSTOX_RESOLVE_ALIASES

    sym = str(instrument or "").strip().upper()
    if not sym:
        return []
    out: List[str] = [sym]
    out.extend(str(a).strip().upper() for a in UPSTOX_RESOLVE_ALIASES.get(sym, []))
    for canon, aliases in UPSTOX_RESOLVE_ALIASES.items():
        alias_u = [str(a).strip().upper() for a in aliases]
        if sym == canon or sym in alias_u:
            out.append(canon)
            out.extend(alias_u)
    seen = set()
    uniq: List[str] = []
    for name in out:
        if name and name not in seen:
            seen.add(name)
            uniq.append(name)
    return uniq


def segment_prefix(instrument: str) -> str:
    sym = str(instrument or "").strip().upper()
    if is_mcx_instrument(sym):
        return "MCX_FO"
    if sym in _BSE_INDEXES:
        return "BSE_FO"
    return "NSE_FO"


def constructed_option_key(
    instrument: str,
    expiry: date,
    strike: float,
    option_type: str,
) -> str:
    """Encoded Upstox option key. Numeric master tokens are preferred when present."""
    sym = str(instrument or "").strip().upper()
    right = str(option_type or "").strip().upper()
    strike_i = int(round(float(strike)))
    month = expiry.strftime("%b").upper()
    year = expiry.strftime("%y")
    return f"{segment_prefix(sym)}|{sym}{year}{month}{strike_i}{right}"


def _option_index() -> Dict[str, List[Dict[str, Any]]]:
    """underlying → CE/PE rows from the Upstox complete master. Cached 10 minutes."""
    now = time.monotonic()
    with _MASTER_LOCK:
        if _MASTER_CACHE["rows_by_und"] and (now - float(_MASTER_CACHE["loaded_at"])) < _MASTER_TTL:
            return _MASTER_CACHE["rows_by_und"]
    try:
        from backend.services.divtest.instruments import ensure_instrument_master
        from backend.services.stock_option_signals import expiry_date_from_instrument

        rows = ensure_instrument_master()
    except Exception as exc:
        logger.info("multi_leg option master unavailable: %s", exc)
        return {}
    out: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        kind = str(row.get("instrument_type") or "").strip().upper()
        if kind not in OPTION_TYPES:
            continue
        und = str(row.get("underlying_symbol") or "").strip().upper()
        if not und:
            continue
        if expiry_date_from_instrument(row) is None:
            continue
        out.setdefault(und, []).append(row)
    with _MASTER_LOCK:
        _MASTER_CACHE["rows_by_und"] = out
        _MASTER_CACHE["loaded_at"] = time.monotonic()
    return out


def _rows_for_instrument(instrument: str) -> List[Dict[str, Any]]:
    index = _option_index()
    rows: List[Dict[str, Any]] = []
    seen = set()
    for name in underlying_candidates(instrument):
        for row in index.get(name, []):
            ik = str(row.get("instrument_key") or "")
            if ik in seen:
                continue
            seen.add(ik)
            rows.append(row)
    return rows


def listed_option_expiries(instrument: str, on_or_after: Optional[date] = None) -> List[date]:
    from backend.services.stock_option_signals import expiry_date_from_instrument

    dates = set()
    for row in _rows_for_instrument(instrument):
        exp = expiry_date_from_instrument(row)
        if exp is None:
            continue
        if on_or_after is not None and exp < on_or_after:
            continue
        dates.add(exp)
    return sorted(dates)


def suggested_expiry(entry: date, instrument: str) -> date:
    """UI default. MCX prefers the next listed master expiry; otherwise the calendar rule."""
    cal = next_monthly_option_expiry(entry, instrument)
    if not is_mcx_instrument(instrument):
        return cal
    try:
        listed = listed_option_expiries(instrument, on_or_after=entry)
    except Exception:
        logger.info("multi_leg MCX expiry master lookup failed for %s", instrument)
        return cal
    if not listed:
        return cal
    return listed[0]


def resolve_option_contract(
    instrument: str,
    strike: Any,
    option_type: str,
    expiry: date,
) -> Dict[str, Any]:
    """instrument_key + lot_size from the instrument master for one leg."""
    from backend.services.stock_option_signals import expiry_date_from_instrument

    sym = str(instrument or "").strip().upper()
    right = str(option_type or "").strip().upper()
    strike_f = _as_float(strike)
    exp = expiry if isinstance(expiry, date) and not isinstance(expiry, datetime) else _parse_date(expiry)
    if not sym or right not in OPTION_TYPES or strike_f is None or exp is None:
        raise MultiLegValidationError("leg needs instrument, strike, CE/PE, and expiry")
    want_prefix = segment_prefix(sym)
    matches: List[Dict[str, Any]] = []
    lot_fallback: Optional[int] = None
    for row in _rows_for_instrument(sym):
        kind = str(row.get("instrument_type") or "").strip().upper()
        lot = _as_int(row.get("lot_size") or row.get("lotSize"))
        if kind == right and lot and lot > 0 and lot_fallback is None:
            lot_fallback = lot
        st = _as_float(row.get("strike_price") if row.get("strike_price") is not None else row.get("strike"))
        if kind != right or st is None or abs(st - strike_f) > 1e-4:
            continue
        row_exp = expiry_date_from_instrument(row)
        if row_exp != exp:
            continue
        ik = str(row.get("instrument_key") or "").strip()
        if not ik:
            continue
        seg = str(row.get("segment") or "").upper()
        prefer = 0 if want_prefix.split("_")[0] in seg or want_prefix in seg else 1
        matches.append({"instrument_key": ik, "lot_size": lot, "prefer": prefer})
    if matches:
        matches.sort(key=lambda r: (r["prefer"], 0 if r["lot_size"] else 1))
        best = matches[0]
        lot = best["lot_size"] or lot_fallback
        if not lot or lot <= 0:
            raise MultiLegValidationError(
                f"lot size missing in instrument master for {sym} {strike_f:g} {right} {exp.isoformat()}"
            )
        return {
            "instrument": sym,
            "strike": strike_f,
            "option_type": right,
            "expiry": exp,
            "instrument_key": best["instrument_key"],
            "lot_size": int(lot),
            "resolved_from": "instrument_master",
        }
    key = constructed_option_key(sym, exp, strike_f, right)
    if not lot_fallback or lot_fallback <= 0:
        raise MultiLegValidationError(
            f"could not resolve lot size for {sym} {strike_f:g} {right} {exp.isoformat()}"
        )
    return {
        "instrument": sym,
        "strike": strike_f,
        "option_type": right,
        "expiry": exp,
        "instrument_key": key,
        "lot_size": int(lot_fallback),
        "resolved_from": "constructed_key",
    }


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    try:
        ux.reload_token_from_storage()
    except Exception:
        logger.info("multi_leg upstox token reload failed")
    return ux


def _fetch_deltas(keys: Sequence[str]) -> Dict[str, Optional[float]]:
    """Upstox v3 option-greek. Missing delta stays None (no second model)."""
    from urllib.parse import quote

    from backend.services.tarang.adapters.upstox_mcx import GREEK_URL
    from backend.services.upstox_service import match_upstox_batch_quote

    clean = [k for k in keys if (k or "").strip()]
    out: Dict[str, Optional[float]] = {k: None for k in clean}
    if not clean:
        return out
    try:
        ux = _upstox()
    except Exception as exc:
        logger.info("multi_leg greeks: upstox client unavailable: %s", exc)
        return out
    for i in range(0, len(clean), 50):
        chunk = clean[i : i + 50]
        url = f"{GREEK_URL}?instrument_key={quote(','.join(chunk), safe=',')}"
        try:
            data = ux.make_api_request(url, method="GET", timeout=20, max_retries=1)
        except Exception as exc:
            logger.info("multi_leg option-greek request failed: %s", exc)
            continue
        raw = (data or {}).get("data") if isinstance(data, dict) else None
        if not isinstance(raw, dict):
            continue
        for ik in chunk:
            gd = match_upstox_batch_quote(raw, ik)
            if not isinstance(gd, dict) or gd.get("delta") is None:
                continue
            try:
                out[ik] = float(gd["delta"])
            except (TypeError, ValueError):
                out[ik] = None
    return out


def _fetch_ltps(keys: Sequence[str]) -> Dict[str, Optional[float]]:
    clean = [k for k in keys if (k or "").strip()]
    out: Dict[str, Optional[float]] = {k: None for k in clean}
    if not clean:
        return out
    try:
        ux = _upstox()
        batch = ux.get_market_quotes_batch_by_keys(list(clean)) or {}
    except Exception as exc:
        logger.info("multi_leg quote batch failed: %s", exc)
        return out
    for ik in clean:
        px = batch.get(ik)
        if px is None:
            px = batch.get(ik.replace("|", ":")) or batch.get(ik.replace(":", "|"))
        try:
            val = float(px) if px is not None else None
        except (TypeError, ValueError):
            val = None
        if val is not None and val > 0:
            out[ik] = val
    return out


def get_quote(
    instrument: str,
    strike: Any,
    option_type: str,
    expiry: Any,
    *,
    instrument_key: Optional[str] = None,
    lot_size: Optional[int] = None,
) -> Dict[str, Any]:
    """Live LTP + delta for one option via the existing Upstox quote and greek pipeline."""
    exp = _parse_date(expiry)
    resolved_from = "stored"
    key = (instrument_key or "").strip() or None
    lot = _as_int(lot_size)
    err = None
    if not key or not lot:
        try:
            resolved = resolve_option_contract(instrument, strike, option_type, exp)
            key = key or resolved["instrument_key"]
            lot = lot or resolved["lot_size"]
            resolved_from = resolved["resolved_from"]
        except MultiLegValidationError as exc:
            err = str(exc)
    ltp = None
    delta = None
    if key:
        ltp = _fetch_ltps([key]).get(key)
        delta = _fetch_deltas([key]).get(key)
    return {
        "instrument": str(instrument or "").strip().upper(),
        "strike": _as_float(strike),
        "option_type": str(option_type or "").strip().upper(),
        "expiry": exp.isoformat() if exp else None,
        "instrument_key": key,
        "lot_size": lot,
        "ltp": ltp,
        "delta": delta,
        "resolved_from": resolved_from,
        "error": err,
    }


def next_trade_number(current_max: Optional[int]) -> int:
    """Next stable Trade No. Existing numbers stay put; delete does not reuse a gap."""
    if current_max is None:
        return 1
    return int(current_max) + 1


def assign_missing_trade_numbers(rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    """Keep stored numbers. Rows without one get the next integers by created_at.

    Oldest created_at is numbered first. A later delete does not renumber the rest.
    """
    def sort_key(row: Dict[str, Any]) -> Tuple[str, str]:
        raw = row.get("created_at")
        stamp = raw.isoformat() if isinstance(raw, datetime) else str(raw or "")
        return stamp, str(row.get("id") or "")

    kept: Dict[str, int] = {}
    highest = 0
    missing: List[Dict[str, Any]] = []
    for row in rows or []:
        tid = str(row.get("id") or "")
        if not tid:
            continue
        current = _as_int(row.get("trade_no"))
        if current is None:
            missing.append(row)
            continue
        kept[tid] = current
        if current > highest:
            highest = current
    assigned = dict(kept)
    cursor = highest
    for row in sorted(missing, key=sort_key):
        cursor = next_trade_number(cursor if cursor else None)
        assigned[str(row.get("id"))] = cursor
    return assigned


def allocate_trade_no(conn) -> int:
    """Lock, then take max(trade_no)+1. Call inside the insert transaction."""
    conn.execute(text("SELECT pg_advisory_xact_lock(845221937)"))
    row = conn.execute(text("SELECT MAX(trade_no) AS n FROM multi_leg_trades")).fetchone()
    current = int(row[0]) if row and row[0] is not None else None
    return next_trade_number(current)


def dte_days(expiry: Any, as_of: Optional[date] = None) -> Optional[int]:
    exp = _parse_date(expiry)
    if exp is None:
        return None
    base = as_of or datetime.now(IST).date()
    return (exp - base).days


def _ensure_upstox_sync_schema(conn) -> None:
    """Additive columns for databases created before Upstox sync.

    trade_id null = orphan leg. upstox_order_id is the dedupe key (manual legs
    stay null). trade_type also accepts UNCLASSIFIED.
    """
    conn.execute(text("ALTER TABLE multi_leg_trade_legs ALTER COLUMN trade_id DROP NOT NULL"))
    conn.execute(text(
        "ALTER TABLE multi_leg_trade_legs ADD COLUMN IF NOT EXISTS upstox_order_id TEXT"
    ))
    conn.execute(text(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ix_multi_leg_trade_legs_upstox_order
        ON multi_leg_trade_legs (upstox_order_id)
        WHERE upstox_order_id IS NOT NULL
        """
    ))
    names = conn.execute(text(
        """
        SELECT con.conname AS name
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        WHERE rel.relname = 'multi_leg_trades'
          AND con.contype = 'c'
          AND pg_get_constraintdef(con.oid) ILIKE '%trade_type%'
        """
    )).mappings().all()
    for row in names:
        name = str(row.get("name") or "")
        if not name.replace("_", "").isalnum():
            continue
        conn.execute(text(f'ALTER TABLE multi_leg_trades DROP CONSTRAINT "{name}"'))
    conn.execute(text(
        """
        ALTER TABLE multi_leg_trades
        ADD CONSTRAINT multi_leg_trades_trade_type_check
        CHECK (trade_type IN ('STRADDLE', 'IRON_FLY', 'IRON_CONDOR', 'UNCLASSIFIED'))
        """
    ))


def _ensure_trade_numbers(conn) -> None:
    """Add trade_no and backfill 1..n by created_at. Stored numbers are never rewritten."""
    conn.execute(text("ALTER TABLE multi_leg_trades ADD COLUMN IF NOT EXISTS trade_no INTEGER"))
    conn.execute(text("SELECT pg_advisory_xact_lock(845221937)"))
    rows = conn.execute(text(
        """
        SELECT CAST(id AS text) AS id, trade_no, created_at
        FROM multi_leg_trades
        """
    )).mappings().all()
    assigned = assign_missing_trade_numbers([dict(row) for row in rows])
    for row in rows:
        if _as_int(row.get("trade_no")) is not None:
            continue
        tid = str(row.get("id") or "")
        number = assigned.get(tid)
        if number is None:
            continue
        conn.execute(
            text(
                """
                UPDATE multi_leg_trades
                SET trade_no = :n
                WHERE id = CAST(:id AS uuid) AND trade_no IS NULL
                """
            ),
            {"id": tid, "n": number},
        )
    conn.execute(text(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ix_multi_leg_trades_trade_no
        ON multi_leg_trades (trade_no)
        """
    ))


def ensure_multi_leg_tables() -> None:
    global _ENSURED
    if _ENSURED:
        return
    if not _MIGRATION.is_file():
        raise FileNotFoundError(f"migration missing: {_MIGRATION}")
    sql = _MIGRATION.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
        _ensure_upstox_sync_schema(conn)
        _ensure_trade_numbers(conn)
    _ENSURED = True
    logger.info("multi_leg tables ensured")


def list_instruments() -> Dict[str, Any]:
    equities: List[str] = []
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT UPPER(TRIM(stock)) AS stock
                FROM arbitrage_master
                WHERE stock IS NOT NULL AND TRIM(stock) <> ''
                ORDER BY 1
                """
            )
        ).fetchall()
        equities = [str(r[0]).strip().upper() for r in rows if r and r[0]]
    except Exception:
        logger.exception("multi_leg instrument equity list failed")
    finally:
        db.close()
    commodities = mcx_underlyings()
    skip = set(INDEX_INSTRUMENTS) | set(commodities)
    equities = [s for s in equities if s not in skip]
    return {
        "indices": list(INDEX_INSTRUMENTS),
        "equities": equities,
        "commodities": commodities,
    }


def _normalize_leg_input(raw: Dict[str, Any], *, instrument: str, header_expiry: date) -> Dict[str, Any]:
    side = str(raw.get("side") or "").strip().upper()
    right = str(raw.get("option_type") or "").strip().upper()
    if side not in SIDES:
        raise MultiLegValidationError("leg side must be BUY or SELL")
    if right not in OPTION_TYPES:
        raise MultiLegValidationError("leg option_type must be CE or PE")
    strike = _as_float(raw.get("strike_price") if raw.get("strike_price") is not None else raw.get("strike"))
    if strike is None or strike <= 0:
        raise MultiLegValidationError("leg strike_price must be > 0")
    entry_px = _as_float(raw.get("entry_price"))
    if entry_px is None or entry_px < 0:
        raise MultiLegValidationError("leg entry_price is required")
    entry_time = _parse_dt(raw.get("entry_time"))
    if entry_time is None:
        raise MultiLegValidationError("leg entry_time is required")
    leg_exp = _parse_date(raw.get("leg_expiry_date") or raw.get("expiry") or header_expiry)
    if leg_exp is None:
        raise MultiLegValidationError("leg expiry is required")
    exit_px = _as_float(raw.get("exit_price"))
    exit_time = _parse_dt(raw.get("exit_time"))
    if (exit_px is None) ^ (exit_time is None):
        raise MultiLegValidationError("exit price and exit time must both be set to close a leg")
    if exit_px is not None and exit_px < 0:
        raise MultiLegValidationError("exit_price must be >= 0")
    try:
        resolved = resolve_option_contract(instrument, strike, right, leg_exp)
        lot = int(resolved["lot_size"])
        key = resolved["instrument_key"]
    except MultiLegValidationError:
        lot = _as_int(raw.get("lot_size"))
        key = str(raw.get("instrument_key") or "").strip() or None
        if not lot or lot <= 0 or not key:
            raise
    ltp = _as_float(raw.get("ltp"))
    delta = _as_float(raw.get("delta"))
    order_id = str(raw.get("upstox_order_id") or "").strip() or None
    leg = {
        "id": _uuid_or_none(raw.get("id")),
        "side": side,
        "option_type": right,
        "strike_price": strike,
        "leg_expiry_date": leg_exp,
        "entry_price": entry_px,
        "entry_time": entry_time,
        "exit_price": exit_px,
        "exit_time": exit_time,
        "ltp": ltp,
        "delta": delta,
        "lot_size": int(lot),
        "instrument_key": key,
        "upstox_order_id": order_id,
    }
    leg["leg_pnl"] = leg_pnl_from_row(leg)
    return leg


def _header_from_body(body: Dict[str, Any]) -> Dict[str, Any]:
    kind = str(body.get("trade_type") or "").strip().upper()
    if kind not in ACCEPTED_TRADE_TYPES:
        raise MultiLegValidationError("trade_type must be STRADDLE, IRON_FLY, or IRON_CONDOR")
    instrument = str(body.get("instrument") or "").strip().upper()
    if not instrument:
        raise MultiLegValidationError("instrument is required")
    spot = _as_float(body.get("spot_price_entry") if body.get("spot_price_entry") is not None else body.get("spot"))
    if spot is None or spot <= 0:
        raise MultiLegValidationError("spot_price_entry must be > 0")
    entry = _parse_date(body.get("entry_date")) or datetime.now(IST).date()
    expiry = _parse_date(body.get("expiry_date") or body.get("expiry"))
    if expiry is None:
        expiry = suggested_expiry(entry, instrument)
    return {
        "trade_type": kind,
        "instrument": instrument,
        "spot_price_entry": spot,
        "entry_date": entry,
        "expiry_date": expiry,
    }


def _load_trade_rows(db, trade_id: Optional[str] = None, where_sql: str = "", params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    clauses = []
    bind = dict(params or {})
    if trade_id:
        clauses.append("t.id = CAST(:id AS uuid)")
        bind["id"] = trade_id
    if where_sql:
        clauses.append(where_sql)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = db.execute(
        text(
            f"""
            SELECT t.id AS trade_id, t.trade_type, t.instrument, t.spot_price_entry,
                   t.entry_date, t.expiry_date, t.status, t.exit_date, t.total_pnl,
                   t.trade_no, t.created_at, t.updated_at,
                   l.id AS leg_id, l.side, l.option_type, l.strike_price, l.leg_expiry_date,
                   l.entry_price, l.entry_time, l.exit_price, l.exit_time, l.ltp, l.delta,
                   l.lot_size, l.instrument_key, l.leg_pnl, l.upstox_order_id, l.sort_order
            FROM multi_leg_trades t
            LEFT JOIN multi_leg_trade_legs l ON l.trade_id = t.id
            {where}
            ORDER BY t.entry_date DESC, t.created_at DESC, l.sort_order ASC, l.created_at ASC
            """
        ),
        bind,
    ).mappings().all()
    grouped: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for row in rows:
        tid = str(row["trade_id"])
        if tid not in grouped:
            grouped[tid] = {"header": dict(row), "legs": []}
            order.append(tid)
        if row.get("leg_id") is not None:
            grouped[tid]["legs"].append(dict(row))
    return [_public_trade(grouped[tid]["header"], grouped[tid]["legs"]) for tid in order]


def _public_leg(row: Dict[str, Any]) -> Dict[str, Any]:
    leg = {
        "id": str(row.get("leg_id") or row.get("id")),
        "side": row.get("side"),
        "option_type": row.get("option_type"),
        "strike_price": _as_float(row.get("strike_price")),
        "leg_expiry_date": _parse_date(row.get("leg_expiry_date")).isoformat() if _parse_date(row.get("leg_expiry_date")) else None,
        "entry_price": _as_float(row.get("entry_price")),
        "entry_time": _iso_dt(row.get("entry_time")),
        "exit_price": _as_float(row.get("exit_price")),
        "exit_time": _iso_dt(row.get("exit_time")),
        "ltp": _as_float(row.get("ltp")),
        "delta": _as_float(row.get("delta")),
        "lot_size": _as_int(row.get("lot_size")),
        "instrument_key": row.get("instrument_key"),
        "upstox_order_id": (str(row.get("upstox_order_id")).strip() if row.get("upstox_order_id") else None) or None,
    }
    pnl = leg_pnl_from_row(leg)
    leg["leg_pnl"] = pnl
    leg["exited"] = leg_is_exited(leg)
    leg["direction_sign"] = direction_sign(leg["side"]) if leg.get("side") in SIDES else None
    return leg


def _public_trade(header: Dict[str, Any], legs: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    public_legs = [_public_leg(l) for l in legs]
    total = trade_pnl(l.get("leg_pnl") for l in public_legs)
    expiry = _parse_date(header.get("expiry_date"))
    entry = _parse_date(header.get("entry_date"))
    return {
        "id": str(header.get("trade_id") or header.get("id")),
        "trade_no": _as_int(header.get("trade_no")),
        "trade_type": header.get("trade_type"),
        "instrument": header.get("instrument"),
        "spot_price_entry": _as_float(header.get("spot_price_entry")),
        "entry_date": entry.isoformat() if entry else None,
        "expiry_date": expiry.isoformat() if expiry else None,
        "status": header.get("status"),
        "exit_date": _iso_dt(header.get("exit_date")),
        "dte": dte_days(expiry),
        "total_pnl": total,
        "created_at": _iso_dt(header.get("created_at")),
        "updated_at": _iso_dt(header.get("updated_at")),
        "legs": public_legs,
    }


def _insert_leg(conn, trade_id: str, leg: Dict[str, Any], sort_order: int) -> None:
    conn.execute(
        text(
            """
            INSERT INTO multi_leg_trade_legs (
                id, trade_id, side, option_type, strike_price, leg_expiry_date,
                entry_price, entry_time, exit_price, exit_time, ltp, delta,
                lot_size, instrument_key, leg_pnl, upstox_order_id, sort_order
            ) VALUES (
                CAST(:id AS uuid), CAST(:trade_id AS uuid), :side, :option_type, :strike_price, :leg_expiry_date,
                :entry_price, :entry_time, :exit_price, :exit_time, :ltp, :delta,
                :lot_size, :instrument_key, :leg_pnl, :upstox_order_id, :sort_order
            )
            """
        ),
        {
            "id": leg.get("id") or str(uuid.uuid4()),
            "trade_id": trade_id,
            "side": leg["side"],
            "option_type": leg["option_type"],
            "strike_price": leg["strike_price"],
            "leg_expiry_date": leg["leg_expiry_date"],
            "entry_price": leg["entry_price"],
            "entry_time": leg["entry_time"],
            "exit_price": leg["exit_price"],
            "exit_time": leg["exit_time"],
            "ltp": leg.get("ltp"),
            "delta": leg.get("delta"),
            "lot_size": leg["lot_size"],
            "instrument_key": leg["instrument_key"],
            "leg_pnl": leg.get("leg_pnl"),
            "upstox_order_id": (str(leg.get("upstox_order_id")).strip() if leg.get("upstox_order_id") else None) or None,
            "sort_order": sort_order,
        },
    )


def _store_total(conn, trade_id: str, total: Optional[float]) -> None:
    conn.execute(
        text(
            """
            UPDATE multi_leg_trades
            SET total_pnl = :pnl, updated_at = NOW()
            WHERE id = CAST(:id AS uuid)
            """
        ),
        {"id": trade_id, "pnl": total},
    )


def create_trade(body: Dict[str, Any]) -> Dict[str, Any]:
    ensure_multi_leg_tables()
    header = _header_from_body(body)
    raw_legs = body.get("legs") or []
    if not isinstance(raw_legs, list):
        raise MultiLegValidationError("legs must be a list")
    assert_min_legs(header["trade_type"], len(raw_legs))
    legs = [
        _normalize_leg_input(raw, instrument=header["instrument"], header_expiry=header["expiry_date"])
        for raw in raw_legs
    ]
    status = status_after_save(closing=False, legs=legs)
    trade_id = str(uuid.uuid4())
    total = trade_pnl(l.get("leg_pnl") for l in legs)
    db = SessionLocal()
    try:
        trade_no = allocate_trade_no(db)
        db.execute(
            text(
                """
                INSERT INTO multi_leg_trades (
                    id, trade_type, instrument, spot_price_entry, entry_date, expiry_date,
                    status, total_pnl, trade_no
                ) VALUES (
                    CAST(:id AS uuid), :trade_type, :instrument, :spot, :entry_date, :expiry_date,
                    :status, :pnl, :trade_no
                )
                """
            ),
            {
                "id": trade_id,
                "trade_type": header["trade_type"],
                "instrument": header["instrument"],
                "spot": header["spot_price_entry"],
                "entry_date": header["entry_date"],
                "expiry_date": header["expiry_date"],
                "status": status,
                "pnl": total,
                "trade_no": trade_no,
            },
        )
        for i, leg in enumerate(legs):
            _insert_leg(db, trade_id, leg, i)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    try:
        from backend.services.multi_leg_ws_ltp import sync_subscriptions

        sync_subscriptions()
    except Exception:
        logger.info("multi_leg ws sync after create failed")
    found = get_trade(trade_id)
    if not found:
        raise MultiLegValidationError("trade was saved but could not be reloaded")
    return found


def get_trade(trade_id: str) -> Optional[Dict[str, Any]]:
    ensure_multi_leg_tables()
    db = SessionLocal()
    try:
        rows = _load_trade_rows(db, trade_id=trade_id)
    finally:
        db.close()
    return rows[0] if rows else None


def list_active() -> List[Dict[str, Any]]:
    ensure_multi_leg_tables()
    db = SessionLocal()
    try:
        return _load_trade_rows(db, where_sql="t.status = 'ACTIVE'")
    finally:
        db.close()


def list_report(
    *,
    status: Optional[str] = None,
    instrument: Optional[str] = None,
    trade_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    ensure_multi_leg_tables()
    clauses: List[str] = []
    params: Dict[str, Any] = {}
    st = str(status or "").strip().upper()
    if st and st != "ALL":
        if st not in STATUSES:
            raise MultiLegValidationError("status filter must be ACTIVE, CLOSED, or ALL")
        clauses.append("t.status = :status")
        params["status"] = st
    inst = str(instrument or "").strip().upper()
    if inst:
        clauses.append("UPPER(t.instrument) = :instrument")
        params["instrument"] = inst
    kind = str(trade_type or "").strip().upper()
    if kind:
        if kind not in ACCEPTED_TRADE_TYPES:
            raise MultiLegValidationError("trade_type filter is invalid")
        clauses.append("t.trade_type = :trade_type")
        params["trade_type"] = kind
    start = _parse_date(date_from)
    end = _parse_date(date_to)
    if start:
        clauses.append("t.entry_date >= :date_from")
        params["date_from"] = start
    if end:
        clauses.append("t.entry_date <= :date_to")
        params["date_to"] = end
    where = " AND ".join(clauses)
    db = SessionLocal()
    try:
        return _load_trade_rows(db, where_sql=where, params=params)
    finally:
        db.close()


def update_trade(trade_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
    """Edit header and legs. Partial exits stay ACTIVE (close is a separate call)."""
    ensure_multi_leg_tables()
    current = get_trade(trade_id)
    if not current:
        raise MultiLegNotFound("trade not found")
    if current.get("status") != "ACTIVE":
        raise MultiLegValidationError("only ACTIVE trades can be edited")
    merged = {
        "trade_type": body.get("trade_type") or current.get("trade_type"),
        "instrument": body.get("instrument") or current.get("instrument"),
        "spot_price_entry": body.get("spot_price_entry")
        if body.get("spot_price_entry") is not None
        else current.get("spot_price_entry"),
        "entry_date": body.get("entry_date") or current.get("entry_date"),
        "expiry_date": body.get("expiry_date") or current.get("expiry_date"),
    }
    header = _header_from_body(merged)
    raw_legs = body.get("legs")
    if raw_legs is None:
        legs_in = []
        for existing in current.get("legs") or []:
            legs_in.append(existing)
        raw_legs = legs_in
    if not isinstance(raw_legs, list):
        raise MultiLegValidationError("legs must be a list")
    # Keep stored LTP/delta when the client omits them so a field edit does not blank quotes.
    # upstox_order_id stays on the leg so a later sync still dedupes after Edit.
    by_id = {str(l.get("id")): l for l in (current.get("legs") or [])}
    prepared: List[Dict[str, Any]] = []
    for raw in raw_legs:
        filled = dict(raw)
        prev = by_id.get(str(raw.get("id") or ""))
        if prev:
            if filled.get("ltp") is None:
                filled["ltp"] = prev.get("ltp")
            if filled.get("delta") is None:
                filled["delta"] = prev.get("delta")
            if not str(filled.get("upstox_order_id") or "").strip():
                filled["upstox_order_id"] = prev.get("upstox_order_id")
        prepared.append(
            _normalize_leg_input(filled, instrument=header["instrument"], header_expiry=header["expiry_date"])
        )
    # Manual min-leg rules stay for the three typed structures. Sync may save
    # UNCLASSIFIED with any leg count; Edit keeps that type until the user corrects it.
    if header["trade_type"] == "UNCLASSIFIED":
        if not prepared:
            raise MultiLegValidationError("UNCLASSIFIED trade needs at least one leg")
    else:
        assert_min_legs(header["trade_type"], len(prepared))
    status = status_after_save(closing=False, legs=prepared)
    total = trade_pnl(l.get("leg_pnl") for l in prepared)
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE multi_leg_trades SET
                    trade_type = :trade_type,
                    instrument = :instrument,
                    spot_price_entry = :spot,
                    entry_date = :entry_date,
                    expiry_date = :expiry_date,
                    status = :status,
                    total_pnl = :pnl,
                    updated_at = NOW()
                WHERE id = CAST(:id AS uuid) AND status = 'ACTIVE'
                """
            ),
            {
                "id": trade_id,
                "trade_type": header["trade_type"],
                "instrument": header["instrument"],
                "spot": header["spot_price_entry"],
                "entry_date": header["entry_date"],
                "expiry_date": header["expiry_date"],
                "status": status,
                "pnl": total,
            },
        )
        db.execute(
            text("DELETE FROM multi_leg_trade_legs WHERE trade_id = CAST(:id AS uuid)"),
            {"id": trade_id},
        )
        for i, leg in enumerate(prepared):
            _insert_leg(db, trade_id, leg, i)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    try:
        from backend.services.multi_leg_ws_ltp import sync_subscriptions

        sync_subscriptions()
    except Exception:
        logger.info("multi_leg ws sync after update failed")
    found = get_trade(trade_id)
    if not found:
        raise MultiLegValidationError("trade could not be reloaded")
    return found


def close_trade(trade_id: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    ensure_multi_leg_tables()
    current = get_trade(trade_id)
    if not current:
        raise MultiLegNotFound("trade not found")
    if current.get("status") != "ACTIVE":
        raise MultiLegValidationError("trade is already closed")
    payload = dict(body or {})
    if payload.get("legs"):
        current = update_trade(trade_id, {**current, **payload, "legs": payload["legs"]})
    legs = current.get("legs") or []
    assert_ready_to_close(legs)
    exit_at = datetime.now(IST)
    total = trade_pnl(l.get("leg_pnl") for l in legs)
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE multi_leg_trades SET
                    status = 'CLOSED',
                    exit_date = :exit_date,
                    total_pnl = :pnl,
                    updated_at = NOW()
                WHERE id = CAST(:id AS uuid) AND status = 'ACTIVE'
                """
            ),
            {"id": trade_id, "exit_date": exit_at, "pnl": total},
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    try:
        from backend.services.multi_leg_ws_ltp import sync_subscriptions

        sync_subscriptions()
    except Exception:
        logger.info("multi_leg ws sync after close failed")
    found = get_trade(trade_id)
    if not found:
        raise MultiLegValidationError("trade could not be reloaded")
    return found


def delete_trade(trade_id: str) -> None:
    ensure_multi_leg_tables()
    db = SessionLocal()
    try:
        result = db.execute(
            text("DELETE FROM multi_leg_trades WHERE id = CAST(:id AS uuid)"),
            {"id": trade_id},
        )
        db.commit()
        if getattr(result, "rowcount", 0) == 0:
            raise MultiLegNotFound("trade not found")
    except MultiLegNotFound:
        db.rollback()
        raise
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    try:
        from backend.services.multi_leg_ws_ltp import sync_subscriptions

        sync_subscriptions()
    except Exception:
        logger.info("multi_leg ws sync after delete failed")


def refresh_active_quotes() -> Dict[str, Any]:
    """Batch LTP + option-greek for open legs, persist, and refresh WS subscriptions."""
    ensure_multi_leg_tables()
    trades = list_active()
    open_legs: List[Dict[str, Any]] = []
    for trade in trades:
        for leg in trade.get("legs") or []:
            if leg.get("exited"):
                continue
            if not leg.get("instrument_key"):
                try:
                    resolved = resolve_option_contract(
                        trade.get("instrument"),
                        leg.get("strike_price"),
                        leg.get("option_type"),
                        _parse_date(leg.get("leg_expiry_date")),
                    )
                    leg["instrument_key"] = resolved["instrument_key"]
                    if not leg.get("lot_size"):
                        leg["lot_size"] = resolved["lot_size"]
                except MultiLegValidationError:
                    continue
            if leg.get("instrument_key"):
                open_legs.append(leg)
    keys = []
    seen = set()
    for leg in open_legs:
        ik = str(leg.get("instrument_key") or "").strip()
        if ik and ik not in seen:
            seen.add(ik)
            keys.append(ik)
    quote_error = None
    ltps: Dict[str, Optional[float]] = {}
    deltas: Dict[str, Optional[float]] = {}
    try:
        ltps = _fetch_ltps(keys)
        deltas = _fetch_deltas(keys)
    except Exception as exc:
        quote_error = str(exc)[:200]
        logger.info("multi_leg quote refresh failed: %s", exc)
    db = SessionLocal()
    try:
        for leg in open_legs:
            ik = str(leg.get("instrument_key") or "")
            ltp = ltps.get(ik)
            delta = deltas.get(ik)
            if ltp is None and delta is None and quote_error:
                continue
            # Keep the last persisted LTP when the live quote is missing.
            use_ltp = ltp if ltp is not None else _as_float(leg.get("ltp"))
            use_delta = delta if delta is not None else _as_float(leg.get("delta"))
            pnl = leg_pnl(leg.get("side"), leg.get("entry_price"), use_ltp, leg.get("lot_size"))
            db.execute(
                text(
                    """
                    UPDATE multi_leg_trade_legs SET
                        instrument_key = COALESCE(:ik, instrument_key),
                        ltp = :ltp,
                        delta = :delta,
                        leg_pnl = :pnl,
                        updated_at = NOW()
                    WHERE id = CAST(:id AS uuid)
                      AND exit_time IS NULL
                    """
                ),
                {
                    "id": leg["id"],
                    "ik": ik or None,
                    "ltp": use_ltp,
                    "delta": use_delta,
                    "pnl": pnl,
                },
            )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("multi_leg quote persist failed")
    finally:
        db.close()
    # Recompute trade totals from stored legs.
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                UPDATE multi_leg_trades t SET
                    total_pnl = s.pnl,
                    updated_at = NOW()
                FROM (
                    SELECT trade_id, SUM(leg_pnl) AS pnl
                    FROM multi_leg_trade_legs
                    WHERE trade_id IS NOT NULL
                    GROUP BY trade_id
                ) s
                WHERE t.id = s.trade_id AND t.status = 'ACTIVE'
                """
            )
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("multi_leg total pnl refresh failed")
    finally:
        db.close()
    try:
        from backend.services.multi_leg_ws_ltp import sync_subscriptions

        sync_subscriptions()
    except Exception as exc:
        logger.info("multi_leg ws sync during quote refresh failed: %s", exc)
    return {
        "ok": quote_error is None,
        "quote_error": quote_error,
        "keys": len(keys),
        "trades": list_active(),
    }


def active_open_leg_keys() -> List[Dict[str, Any]]:
    """Open (not exited) active legs for the websocket sidecar."""
    ensure_multi_leg_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT l.id AS leg_id, l.instrument_key, l.side, l.entry_price, l.lot_size
                FROM multi_leg_trade_legs l
                JOIN multi_leg_trades t ON t.id = l.trade_id
                WHERE t.status = 'ACTIVE'
                  AND l.exit_time IS NULL
                  AND l.instrument_key IS NOT NULL
                  AND TRIM(l.instrument_key) <> ''
                """
            )
        ).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()

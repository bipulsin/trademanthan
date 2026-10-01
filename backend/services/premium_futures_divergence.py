"""Second Premium Futures TradingView format: commodity divergence alerts.

The existing JSON webhook (ticker / action / side / message) is untouched.
This module claims a payload only when it matches the divergence sentence,
the equivalent strategy.order fields, or a JSON body whose ``flag`` is one of
BULL-DIV, BEAR-DIV, BULL-GO, BEAR-GO, BULL-EXIT, BEAR-EXIT. Anything else
returns no parse so the caller keeps the JSON handler.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.services.premium_futures_tv_webhook import normalize_tv_ticker

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
_MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "add_premium_futures_divergence.sql"
_ENSURED = False

HANDLER_JSON = "json"
HANDLER_DIVERGENCE = "divergence"

DIVERGENCE_ACTIONS = frozenset(
    {"BULL-DIV", "BEAR-DIV", "BULL-GO", "BEAR-GO", "BULL-EXIT", "BEAR-EXIT"}
)
SOURCE = "commodity_divergence"

_SENTENCE = re.compile(
    r"TWCTO\s+Commodity\s+Divergence\s*:\s*order\s+"
    r"(BULL-DIV|BEAR-DIV|BULL-GO|BEAR-GO|BULL-EXIT|BEAR-EXIT)"
    r"\s*@\s*(\d+)\s+filled\s+on\s+([A-Za-z0-9&_:-]+)\s*\.?",
    re.IGNORECASE,
)
_MESSAGE_KEYS = ("message", "text", "comment", "alert_message", "description")

LtpFn = Callable[[str], Optional[float]]
Resolver = Callable[[str], Optional[Dict[str, str]]]


def select_premium_futures_handler(parsed: Any, raw_text: str = "") -> str:
    """``divergence`` only when the new alert matches; otherwise the JSON handler."""
    if parse_divergence_alert(parsed, raw_text) is None:
        return HANDLER_JSON
    return HANDLER_DIVERGENCE


def parse_divergence_alert(parsed: Any, raw_text: str = "") -> Optional[Dict[str, Any]]:
    """Return action, contracts, ticker, symbol — or None when this is not the new format."""
    for blob in _text_candidates(parsed, raw_text):
        hit = _parse_sentence(blob)
        if hit is not None:
            return hit
    if isinstance(parsed, dict):
        return _parse_structured(parsed)
    return None


def _text_candidates(parsed: Any, raw_text: str) -> List[str]:
    out: List[str] = []
    if isinstance(parsed, str) and parsed.strip():
        out.append(parsed)
    if isinstance(parsed, dict):
        for key in _MESSAGE_KEYS:
            val = parsed.get(key)
            if isinstance(val, str) and val.strip():
                out.append(val)
    if raw_text and raw_text.strip():
        out.append(raw_text)
    return out


def _parse_sentence(blob: str) -> Optional[Dict[str, Any]]:
    m = _SENTENCE.search(blob or "")
    if not m:
        return None
    action = m.group(1).upper()
    if action not in DIVERGENCE_ACTIONS:
        return None
    symbol = normalize_tv_ticker(m.group(3))
    if not symbol:
        return None
    try:
        contracts = int(m.group(2))
    except (TypeError, ValueError):
        contracts = None
    return {
        "action": action,
        "contracts": contracts,
        "ticker_raw": m.group(3).strip(),
        "symbol": symbol,
    }


def _first_symbol(d: Dict[str, Any]) -> Optional[str]:
    for key in ("symbol", "ticker", "tickerid", "sym"):
        val = d.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def parse_event_time(value: Any) -> Optional[datetime]:
    """Epoch milliseconds, epoch seconds, or an ISO string, as aware IST. Invalid → None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, (int, float)):
        return _epoch_to_ist(float(value))
    if isinstance(value, str):
        s = value.strip()
        if not s or "{{" in s:
            return None
        if re.fullmatch(r"-?\d+(?:\.\d+)?", s):
            try:
                return _epoch_to_ist(float(s))
            except (TypeError, ValueError, OverflowError):
                return None
        return _iso_to_ist(s)
    return None


def _epoch_to_ist(n: float) -> Optional[datetime]:
    if n != n or n in (float("inf"), float("-inf")):
        return None
    # 13-digit TradingView times are unix milliseconds; 10-digit values are seconds.
    seconds = n / 1000.0 if abs(n) >= 100_000_000_000 else n
    if seconds < 946684800 or seconds > 4102444800:
        return None
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc).astimezone(IST)
    except (OSError, OverflowError, ValueError):
        return None


def _iso_to_ist(s: str) -> Optional[datetime]:
    text = s.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _with_event_time(alert: Dict[str, Any], d: Dict[str, Any]) -> Dict[str, Any]:
    if "time" not in d:
        return alert
    event_at = parse_event_time(d.get("time"))
    if event_at is not None:
        alert["event_at"] = event_at
    return alert


def _parse_structured(d: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Flag JSON, or strategy.order.action (nested or dotted) plus a symbol.

    Top-level ``action`` stays on the JSON handler. A ``flag`` that is not one
    of the divergence actions is ignored so older JSON keeps its handler.
    """
    flag_u = str(d.get("flag") or "").strip().upper()
    if flag_u in DIVERGENCE_ACTIONS:
        ticker_raw = _first_symbol(d)
        if not ticker_raw:
            return None
        symbol = normalize_tv_ticker(ticker_raw)
        if not symbol:
            return None
        return _with_event_time(
            {
                "action": flag_u,
                "contracts": _as_contracts(d.get("contracts")),
                "ticker_raw": ticker_raw,
                "symbol": symbol,
            },
            d,
        )

    action = None
    contracts = None
    nested = d.get("strategy")
    if isinstance(nested, dict):
        order = nested.get("order")
        if isinstance(order, dict):
            action = order.get("action")
            contracts = order.get("contracts")
    if action is None and "strategy.order.action" in d:
        action = d.get("strategy.order.action")
        contracts = d.get("strategy.order.contracts", contracts)
    action_u = str(action or "").strip().upper()
    if action_u not in DIVERGENCE_ACTIONS:
        return None
    ticker_raw = _first_symbol(d)
    if not ticker_raw:
        return None
    symbol = normalize_tv_ticker(ticker_raw)
    if not symbol:
        return None
    return _with_event_time(
        {
            "action": action_u,
            "contracts": _as_contracts(contracts),
            "ticker_raw": ticker_raw,
            "symbol": symbol,
        },
        d,
    )


def _as_contracts(v: Any) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s or "{{" in s:
            return None
        v = s
    try:
        n = int(float(v))
    except (TypeError, ValueError):
        return None
    if n < 0:
        return None
    return n


def _side_of(action: str) -> str:
    return "bearish" if str(action).upper().startswith("BEAR") else "bullish"


def _kind_of(action: str) -> str:
    return str(action).upper().split("-", 1)[-1]


def _direction(side: str) -> str:
    return "SHORT" if side == "bearish" else "LONG"


def _aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _naive(dt: datetime) -> datetime:
    aware = _aware(dt)
    assert aware is not None
    return aware.replace(tzinfo=None, microsecond=0)


def _hm(dt: Optional[datetime]) -> Optional[str]:
    aware = _aware(dt)
    if aware is None:
        return None
    return aware.strftime("%H:%M")


def _iso(dt: Optional[datetime]) -> Optional[str]:
    aware = _aware(dt)
    if aware is None:
        return None
    return aware.isoformat()


def _session_date(dt: datetime) -> date:
    aware = _aware(dt)
    assert aware is not None
    return aware.date()


def _safe_ltp(ltp_fn: LtpFn, instrument_key: str) -> Tuple[Optional[float], bool]:
    """Return (price, missing). A missing quote still continues the state change."""
    try:
        raw = ltp_fn(instrument_key)
    except Exception as e:
        logger.warning("premium_futures divergence LTP failed ik=%s: %s", instrument_key, e)
        return None, True
    if raw is None:
        logger.info("premium_futures divergence LTP missing ik=%s", instrument_key)
        return None, True
    try:
        price = float(raw)
    except (TypeError, ValueError):
        logger.info("premium_futures divergence LTP unusable ik=%s value=%s", instrument_key, raw)
        return None, True
    if price <= 0:
        logger.info("premium_futures divergence LTP non-positive ik=%s value=%s", instrument_key, price)
        return None, True
    return round(price, 4), False


ENTRY_LAG = timedelta(minutes=5)
SESSION_FLAT = (15, 15)


def could_have_pnl_rupees(
    direction: Any,
    entry_ltp: Any,
    mark_ltp: Any,
    qty: Any,
) -> Optional[float]:
    """Long: (mark − entry) × 1-lot qty. Short: (entry − mark) × 1-lot qty."""
    try:
        entry = float(entry_ltp)
        mark = float(mark_ltp)
        lots = float(qty)
    except (TypeError, ValueError):
        return None
    if lots == 0:
        return None
    side = str(direction or "").strip().upper()
    if side in {"SHORT", "BEAR", "BEARISH"}:
        return round((entry - mark) * lots, 2)
    return round((mark - entry) * lots, 2)


def resolve_entry_ltp(ltp_at_entry_time: Any, scan_ltp: Any) -> Optional[float]:
    """Quote at 1st scan + 5 minutes, else the LTP captured at that scan / GO."""
    for raw in (ltp_at_entry_time, scan_ltp):
        try:
            price = float(raw)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return round(price, 4)
    return None


def one_lot_qty(instrument_key: str, lot_fn: Optional[Callable[[str], Any]] = None) -> Optional[int]:
    """Futures lot size for one current-month contract, from the instrument master."""
    raw = None
    if lot_fn is not None:
        try:
            raw = lot_fn(instrument_key)
        except Exception:
            logger.debug("premium_futures lot lookup failed ik=%s", instrument_key, exc_info=True)
            raw = None
    else:
        raw = _instrument_lot_qty(instrument_key)
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _instrument_lot_qty(instrument_key: str) -> Optional[int]:
    ik = str(instrument_key or "").strip()
    if not ik:
        return None
    try:
        from backend.services.daily_futures_service import fut_lot_for_key

        n = fut_lot_for_key(ik)
        if n:
            return int(n)
    except Exception:
        logger.debug("premium_futures fut_lot_for_key skipped ik=%s", ik, exc_info=True)
    try:
        from backend.services.smart_futures_picker.position_sizing import (
            get_futures_lot_size_by_instrument_key,
        )

        n = get_futures_lot_size_by_instrument_key(ik)
        if n:
            return int(n)
    except Exception:
        logger.debug("premium_futures instrument-file lot skipped ik=%s", ik, exc_info=True)
    return None


def entry_at_from_first_scan(first_scan: datetime) -> datetime:
    base = _aware(first_scan)
    assert base is not None
    return base + ENTRY_LAG


def display_entry_at(first_scan: Any, stored_entry: Any) -> Optional[datetime]:
    """Entry display is always the 1st scan + 5 minutes.

    Rows saved before that rule stored the scan clock itself in ``entry_at``.
    """
    first = _aware(first_scan) if not isinstance(first_scan, str) else _iso_to_ist(str(first_scan))
    stored = _aware(stored_entry) if isinstance(stored_entry, datetime) else None
    if stored is None and isinstance(stored_entry, str) and stored_entry.strip():
        stored = _iso_to_ist(stored_entry)
    if first is None:
        return stored
    shifted = first + ENTRY_LAG
    if stored is None:
        return shifted
    if abs((stored - first).total_seconds()) < 90:
        return shifted
    return stored


def session_exit_deadline(trade_date: date) -> datetime:
    return IST.localize(datetime(trade_date.year, trade_date.month, trade_date.day, 15, 15))


def _as_trade_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        aware = _aware(value)
        return aware.date() if aware else None
    if isinstance(value, date):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def session_exit_due(trade_date: date, now: datetime) -> bool:
    clock = _aware(now)
    if clock is None:
        return False
    return clock >= session_exit_deadline(trade_date)


def apply_session_flat(row: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    """If no EXIT has arrived by 15:15 IST, exit at 15:15 and freeze the last mark."""
    out = dict(row)
    if out.get("exit_at") is not None:
        return out
    trade_date = _as_trade_date(out.get("trade_date"))
    if trade_date is None or not session_exit_due(trade_date, now):
        return out
    mark = out.get("current_ltp")
    if mark is None:
        mark = out.get("scan_ltp")
    if mark is None:
        mark = out.get("entry_ltp")
    out["exit_at"] = _naive(session_exit_deadline(trade_date))
    out["exit_ltp"] = mark
    out["current_ltp"] = mark
    out["ltp_missing_exit"] = mark is None
    return out


def apply_mark_refresh(row: Dict[str, Any], new_ltp: Any) -> Dict[str, Any]:
    """Update Current LTP and PnL only while Exit time is still empty."""
    out = dict(row)
    if out.get("exit_at") is not None:
        return out
    price = resolve_entry_ltp(new_ltp, None)
    if price is None:
        return out
    out["current_ltp"] = price
    qty = out.get("lot_qty") if out.get("lot_qty") is not None else out.get("qty")
    direction = out.get("direction_type") or out.get("side")
    out["pnl_scan_rupees"] = could_have_pnl_rupees(direction, out.get("entry_ltp"), price, qty)
    return out


def premium_futures_ltp(instrument_key: str) -> Optional[float]:
    """Same quote path Today's pick uses, for one current-month future."""
    ik = str(instrument_key or "").strip()
    if not ik:
        return None
    from backend.services.daily_futures_service import _apply_live_ltps_to_picks_and_running

    row: Dict[str, Any] = {"instrument_key": ik}
    _apply_live_ltps_to_picks_and_running([row], [], [], persist_screening=False)
    return row.get("ltp")


class MemoryDivergenceStore:
    """In-process state for tests and for one handler call. Not process-wide."""

    def __init__(self) -> None:
        self.picks: Dict[Tuple[date, str, str], Dict[str, Any]] = {}
        self.could: List[Dict[str, Any]] = []
        self._seq = 0

    def get_pick(self, trade_date: date, underlying: str, side: str) -> Optional[Dict[str, Any]]:
        row = self.picks.get((trade_date, underlying, side))
        return dict(row) if row else None

    def save_pick(self, row: Dict[str, Any]) -> Dict[str, Any]:
        key = (row["trade_date"], row["underlying"], row["side"])
        stored = dict(row)
        self.picks[key] = stored
        return dict(stored)

    def active_picks(self, trade_date: date) -> List[Dict[str, Any]]:
        return [
            dict(r)
            for r in self.picks.values()
            if r.get("trade_date") == trade_date and r.get("active")
        ]

    def deactivate_symbol(self, trade_date: date, underlying: str) -> None:
        for side in ("bullish", "bearish"):
            row = self.picks.get((trade_date, underlying, side))
            if not row:
                continue
            row["active"] = False
            row["enter_enabled"] = False
            row["ticker_eligible"] = False

    def open_could(self, trade_date: date, underlying: str, side: str) -> Optional[Dict[str, Any]]:
        for row in reversed(self.could):
            if (
                row.get("trade_date") == trade_date
                and row.get("underlying") == underlying
                and row.get("side") == side
                and row.get("exit_at") is None
            ):
                return dict(row)
        return None

    def add_could(self, row: Dict[str, Any]) -> Dict[str, Any]:
        existing = self.open_could(row["trade_date"], row["underlying"], row["side"])
        if existing:
            return existing
        self._seq += 1
        stored = dict(row)
        stored["id"] = self._seq
        self.could.append(stored)
        return dict(stored)

    def stamp_exit(
        self,
        could_id: int,
        exit_ltp: Optional[float],
        exit_at: datetime,
        *,
        ltp_missing: bool,
    ) -> Optional[Dict[str, Any]]:
        for row in self.could:
            if int(row.get("id") or 0) != int(could_id):
                continue
            row["exit_ltp"] = exit_ltp
            row["exit_at"] = _naive(exit_at)
            row["ltp_missing_exit"] = bool(ltp_missing)
            frozen = exit_ltp
            if frozen is None:
                frozen = row.get("current_ltp")
            if frozen is None:
                frozen = row.get("scan_ltp")
            if frozen is None:
                frozen = row.get("entry_ltp")
            row["current_ltp"] = frozen
            return dict(row)
        return None

    def set_current_ltp(self, could_id: int, price: float) -> Optional[Dict[str, Any]]:
        for row in self.could:
            if int(row.get("id") or 0) != int(could_id) or row.get("exit_at") is not None:
                continue
            row["current_ltp"] = price
            return dict(row)
        return None

    def set_entry_ltp(self, could_id: int, price: float) -> Optional[Dict[str, Any]]:
        for row in self.could:
            if int(row.get("id") or 0) != int(could_id) or row.get("exit_at") is not None:
                continue
            row["entry_ltp"] = price
            row["entry_ltp_from_history"] = True
            return dict(row)
        return None

    def could_rows(self, trade_date: date) -> List[Dict[str, Any]]:
        return [dict(r) for r in self.could if r.get("trade_date") == trade_date]


class DbDivergenceStore:
    """Session-date picks and could-have rows. Screening sync is best-effort."""

    def ensure(self) -> None:
        global _ENSURED
        if _ENSURED:
            return
        from backend.database import engine

        if not _MIGRATION.is_file():
            raise FileNotFoundError(f"migration missing: {_MIGRATION}")
        sql = _MIGRATION.read_text(encoding="utf-8")
        with engine.begin() as conn:
            conn.execute(text(sql))
            for stmt in (
                "ALTER TABLE premium_futures_divergence_could ADD COLUMN IF NOT EXISTS scan_ltp DOUBLE PRECISION",
                "ALTER TABLE premium_futures_divergence_could ADD COLUMN IF NOT EXISTS current_ltp DOUBLE PRECISION",
                "ALTER TABLE premium_futures_divergence_could ADD COLUMN IF NOT EXISTS lot_qty INTEGER",
                "ALTER TABLE premium_futures_divergence_could ADD COLUMN IF NOT EXISTS entry_ltp_from_history BOOLEAN NOT NULL DEFAULT FALSE",
            ):
                conn.execute(text(stmt))
        _ENSURED = True
        logger.info("premium_futures divergence tables ensured")

    def get_pick(self, trade_date: date, underlying: str, side: str) -> Optional[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            row = db.execute(
                text(
                    """
                    SELECT trade_date, underlying, side, fut_symbol, instrument_key, contracts,
                           active, enter_enabled, ticker_eligible, screening_id, action, div_at, updated_at
                    FROM premium_futures_divergence_pick
                    WHERE trade_date = CAST(:d AS DATE) AND underlying = :u AND side = :s
                    """
                ),
                {"d": trade_date.isoformat(), "u": underlying, "s": side},
            ).mappings().first()
            return dict(row) if row else None
        finally:
            db.close()

    def save_pick(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            db.execute(
                text(
                    """
                    INSERT INTO premium_futures_divergence_pick (
                        trade_date, underlying, side, fut_symbol, instrument_key, contracts,
                        active, enter_enabled, ticker_eligible, screening_id, action, div_at, updated_at
                    ) VALUES (
                        CAST(:d AS DATE), :u, :side, :fs, :ik, :contracts,
                        :active, :enter, :ticker, :sid, :action, :div_at, :updated
                    )
                    ON CONFLICT (trade_date, underlying, side) DO UPDATE SET
                        fut_symbol = EXCLUDED.fut_symbol,
                        instrument_key = EXCLUDED.instrument_key,
                        contracts = EXCLUDED.contracts,
                        active = EXCLUDED.active,
                        enter_enabled = EXCLUDED.enter_enabled,
                        ticker_eligible = EXCLUDED.ticker_eligible,
                        action = EXCLUDED.action,
                        div_at = COALESCE(premium_futures_divergence_pick.div_at, EXCLUDED.div_at),
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    "d": row["trade_date"].isoformat(),
                    "u": row["underlying"],
                    "side": row["side"],
                    "fs": row.get("fut_symbol"),
                    "ik": row.get("instrument_key"),
                    "contracts": row.get("contracts"),
                    "active": bool(row.get("active")),
                    "enter": bool(row.get("enter_enabled")),
                    "ticker": bool(row.get("ticker_eligible")),
                    "sid": row.get("screening_id"),
                    "action": row.get("action"),
                    "div_at": row.get("div_at"),
                    "updated": row.get("updated_at"),
                },
            )
            if row.get("active"):
                sid = _sync_screening(db, row)
                row["screening_id"] = sid
                db.execute(
                    text(
                        """
                        UPDATE premium_futures_divergence_pick
                        SET screening_id = :sid
                        WHERE trade_date = CAST(:d AS DATE) AND underlying = :u AND side = :side
                        """
                    ),
                    {
                        "sid": sid,
                        "d": row["trade_date"].isoformat(),
                        "u": row["underlying"],
                        "side": row["side"],
                    },
                )
            db.commit()
            return dict(row)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def active_picks(self, trade_date: date) -> List[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT trade_date, underlying, side, fut_symbol, instrument_key, contracts,
                           active, enter_enabled, ticker_eligible, screening_id, action, div_at, updated_at
                    FROM premium_futures_divergence_pick
                    WHERE trade_date = CAST(:d AS DATE) AND active = TRUE
                    ORDER BY updated_at ASC
                    """
                ),
                {"d": trade_date.isoformat()},
            ).mappings().all()
            return [dict(r) for r in rows]
        finally:
            db.close()

    def deactivate_symbol(self, trade_date: date, underlying: str) -> None:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            db.execute(
                text(
                    """
                    UPDATE premium_futures_divergence_pick
                    SET active = FALSE, enter_enabled = FALSE, ticker_eligible = FALSE,
                        updated_at = :ts
                    WHERE trade_date = CAST(:d AS DATE) AND underlying = :u
                    """
                ),
                {"d": trade_date.isoformat(), "u": underlying, "ts": _naive(datetime.now(IST))},
            )
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def open_could(self, trade_date: date, underlying: str, side: str) -> Optional[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            row = db.execute(
                text(
                    """
                    SELECT id, trade_date, underlying, side, fut_symbol, instrument_key, contracts,
                           direction_type, entry_ltp, entry_at, exit_ltp, exit_at, first_scan_at,
                           ltp_missing_entry, ltp_missing_exit, scan_ltp, current_ltp, lot_qty,
                           entry_ltp_from_history
                    FROM premium_futures_divergence_could
                    WHERE trade_date = CAST(:d AS DATE) AND underlying = :u AND side = :s
                      AND exit_at IS NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ),
                {"d": trade_date.isoformat(), "u": underlying, "s": side},
            ).mappings().first()
            return dict(row) if row else None
        finally:
            db.close()

    def add_could(self, row: Dict[str, Any]) -> Dict[str, Any]:
        self.ensure()
        existing = self.open_could(row["trade_date"], row["underlying"], row["side"])
        if existing:
            return existing
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            rid = db.execute(
                text(
                    """
                    INSERT INTO premium_futures_divergence_could (
                        trade_date, underlying, side, fut_symbol, instrument_key, contracts,
                        direction_type, entry_ltp, entry_at, first_scan_at, ltp_missing_entry,
                        scan_ltp, current_ltp, lot_qty, entry_ltp_from_history
                    ) VALUES (
                        CAST(:d AS DATE), :u, :side, :fs, :ik, :contracts,
                        :dt, :entry_ltp, :entry_at, :first_scan_at, :missing,
                        :scan_ltp, :current_ltp, :lot_qty, :from_history
                    )
                    RETURNING id
                    """
                ),
                {
                    "d": row["trade_date"].isoformat(),
                    "u": row["underlying"],
                    "side": row["side"],
                    "fs": row.get("fut_symbol"),
                    "ik": row.get("instrument_key"),
                    "contracts": row.get("contracts"),
                    "dt": row.get("direction_type"),
                    "entry_ltp": row.get("entry_ltp"),
                    "entry_at": row.get("entry_at"),
                    "first_scan_at": row.get("first_scan_at"),
                    "missing": bool(row.get("ltp_missing_entry")),
                    "scan_ltp": row.get("scan_ltp"),
                    "current_ltp": row.get("current_ltp"),
                    "lot_qty": row.get("lot_qty"),
                    "from_history": bool(row.get("entry_ltp_from_history")),
                },
            ).scalar()
            db.commit()
            stored = dict(row)
            stored["id"] = int(rid) if rid is not None else None
            return stored
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def stamp_exit(
        self,
        could_id: int,
        exit_ltp: Optional[float],
        exit_at: datetime,
        *,
        ltp_missing: bool,
    ) -> Optional[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            row = db.execute(
                text(
                    """
                    UPDATE premium_futures_divergence_could
                    SET exit_ltp = :px, exit_at = :ts, ltp_missing_exit = :missing,
                        current_ltp = COALESCE(:px, current_ltp, scan_ltp, entry_ltp)
                    WHERE id = :id AND exit_at IS NULL
                    RETURNING id, trade_date, underlying, side, entry_ltp, entry_at, exit_ltp, exit_at,
                              first_scan_at, ltp_missing_entry, ltp_missing_exit, direction_type,
                              fut_symbol, instrument_key, contracts, scan_ltp, current_ltp, lot_qty,
                              entry_ltp_from_history
                    """
                ),
                {
                    "px": exit_ltp,
                    "ts": _naive(exit_at),
                    "missing": bool(ltp_missing),
                    "id": int(could_id),
                },
            ).mappings().first()
            db.commit()
            return dict(row) if row else None
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def set_current_ltp(self, could_id: int, price: float) -> Optional[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            row = db.execute(
                text(
                    """
                    UPDATE premium_futures_divergence_could
                    SET current_ltp = :px
                    WHERE id = :id AND exit_at IS NULL
                    RETURNING id
                    """
                ),
                {"px": price, "id": int(could_id)},
            ).first()
            db.commit()
            return {"id": int(could_id), "current_ltp": price} if row else None
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def set_entry_ltp(self, could_id: int, price: float) -> Optional[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            row = db.execute(
                text(
                    """
                    UPDATE premium_futures_divergence_could
                    SET entry_ltp = :px, entry_ltp_from_history = TRUE
                    WHERE id = :id AND exit_at IS NULL
                      AND COALESCE(entry_ltp_from_history, FALSE) = FALSE
                    RETURNING id
                    """
                ),
                {"px": price, "id": int(could_id)},
            ).first()
            db.commit()
            return {"id": int(could_id), "entry_ltp": price} if row else None
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def open_could_rows(self) -> List[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT id, trade_date, underlying, side, fut_symbol, instrument_key, contracts,
                           direction_type, entry_ltp, entry_at, exit_ltp, exit_at, first_scan_at,
                           ltp_missing_entry, ltp_missing_exit, scan_ltp, current_ltp, lot_qty,
                           entry_ltp_from_history
                    FROM premium_futures_divergence_could
                    WHERE exit_at IS NULL
                    ORDER BY id ASC
                    """
                )
            ).mappings().all()
            return [dict(r) for r in rows]
        finally:
            db.close()

    def could_rows(self, trade_date: date) -> List[Dict[str, Any]]:
        self.ensure()
        from backend.database import SessionLocal

        db = SessionLocal()
        try:
            rows = db.execute(
                text(
                    """
                    SELECT id, trade_date, underlying, side, fut_symbol, instrument_key, contracts,
                           direction_type, entry_ltp, entry_at, exit_ltp, exit_at, first_scan_at,
                           ltp_missing_entry, ltp_missing_exit, scan_ltp, current_ltp, lot_qty,
                           entry_ltp_from_history
                    FROM premium_futures_divergence_could
                    WHERE trade_date = CAST(:d AS DATE)
                    ORDER BY id ASC
                    """
                ),
                {"d": trade_date.isoformat()},
            ).mappings().all()
            return [dict(r) for r in rows]
        finally:
            db.close()


def _sync_screening(db: Any, pick: Dict[str, Any]) -> Optional[int]:
    """Own a daily_futures_screening row so Enter can confirm. Never rewrite another source."""
    underlying = str(pick.get("underlying") or "").strip().upper()
    ik = str(pick.get("instrument_key") or "").strip()
    if not underlying or not ik:
        return pick.get("screening_id")
    direction = _direction(str(pick.get("side") or "bullish"))
    when = _aware(pick.get("div_at") or pick.get("updated_at") or datetime.now(IST))
    breakdown = json.dumps(
        {
            "source": SOURCE,
            "action": pick.get("action"),
            "contracts": pick.get("contracts"),
        }
    )
    existing = db.execute(
        text(
            """
            SELECT id, direction_type, conviction_breakdown_json
            FROM daily_futures_screening
            WHERE trade_date = CAST(:d AS DATE) AND UPPER(TRIM(underlying)) = :u
            LIMIT 1
            """
        ),
        {"d": pick["trade_date"].isoformat(), "u": underlying},
    ).mappings().first()
    if existing:
        src = _breakdown_source(existing.get("conviction_breakdown_json"))
        if src != SOURCE:
            logger.info(
                "premium_futures divergence screening left untouched und=%s existing_source=%s",
                underlying,
                src or "other",
            )
            return None
        db.execute(
            text(
                """
                UPDATE daily_futures_screening SET
                  direction_type = :dt,
                  future_symbol = :fs,
                  instrument_key = :ik,
                  first_hit_at = COALESCE(first_hit_at, :fh),
                  last_hit_at = :lh,
                  conviction_breakdown_json = CAST(:cbj AS JSONB),
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
                """
            ),
            {
                "dt": direction,
                "fs": pick.get("fut_symbol") or underlying,
                "ik": ik,
                "fh": when,
                "lh": when,
                "cbj": breakdown,
                "id": int(existing["id"]),
            },
        )
        return int(existing["id"])
    created = db.execute(
        text(
            """
            INSERT INTO daily_futures_screening (
              trade_date, underlying, direction_type, future_symbol, instrument_key,
              scan_count, first_hit_at, last_hit_at, conviction_score, conviction_breakdown_json
            ) VALUES (
              CAST(:d AS DATE), :u, :dt, :fs, :ik,
              1, :fh, :lh, 0, CAST(:cbj AS JSONB)
            )
            RETURNING id
            """
        ),
        {
            "d": pick["trade_date"].isoformat(),
            "u": underlying,
            "dt": direction,
            "fs": pick.get("fut_symbol") or underlying,
            "ik": ik,
            "fh": when,
            "lh": when,
            "cbj": breakdown,
        },
    ).scalar()
    return int(created) if created is not None else None


def _breakdown_source(raw: Any) -> str:
    if isinstance(raw, dict):
        return str(raw.get("source") or "")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return SOURCE if SOURCE in raw else ""
        if isinstance(parsed, dict):
            return str(parsed.get("source") or "")
    return ""


def _base_result(alert: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "format": HANDLER_DIVERGENCE,
        "action": alert.get("action"),
        "contracts": alert.get("contracts"),
        "symbol": alert.get("symbol"),
        "applied": False,
        "ignored": False,
        "dropped": False,
        "reason": None,
        "enter_enabled": False,
        "ticker_eligible": False,
        "entry_ltp": None,
        "exit_ltp": None,
        "ltp_missing": False,
    }


def _event_clock(alert: Dict[str, Any], now: datetime) -> datetime:
    """Alert ``time`` when it parsed; otherwise the webhook receive time."""
    event_at = alert.get("event_at")
    if isinstance(event_at, datetime):
        aware = _aware(event_at)
        if aware is not None:
            return aware
    return now


def apply_divergence_event(
    alert: Dict[str, Any],
    *,
    store: Any,
    resolver: Resolver,
    ltp_fn: LtpFn,
    now: datetime,
    lot_fn: Optional[Callable[[str], Any]] = None,
) -> Dict[str, Any]:
    """Apply one parsed divergence alert. Unknown master symbols are dropped."""
    result = _base_result(alert)
    symbol = str(alert.get("symbol") or "").strip().upper()
    action = str(alert.get("action") or "").strip().upper()
    side = _side_of(action)
    kind = _kind_of(action)
    result["side"] = side
    fut = resolver(symbol) if symbol else None
    ikey = str((fut or {}).get("fut_instrument_key") or "").strip()
    if not fut or not ikey:
        result["dropped"] = True
        result["reason"] = "no arbitrage_master row or empty currmth FUT"
        logger.info("premium_futures divergence dropped symbol=%s action=%s", symbol, action)
        return result

    underlying = str(fut.get("underlying") or symbol).strip().upper()
    fut_symbol = str(fut.get("fut_symbol") or underlying).strip()
    result["underlying"] = underlying
    result["resolved_fut"] = fut_symbol
    clock = _event_clock(alert, now)
    trade_date = _session_date(clock)
    stamp = _naive(clock)

    if kind == "DIV":
        existing = store.get_pick(trade_date, underlying, side)
        keep_go = bool(existing and existing.get("active") and existing.get("enter_enabled"))
        saved = store.save_pick(
            {
                "trade_date": trade_date,
                "underlying": underlying,
                "side": side,
                "fut_symbol": fut_symbol,
                "instrument_key": ikey,
                "contracts": alert.get("contracts") if alert.get("contracts") is not None else (existing or {}).get("contracts"),
                "active": True,
                "enter_enabled": keep_go,
                "ticker_eligible": keep_go,
                "screening_id": (existing or {}).get("screening_id"),
                "action": action,
                "div_at": (existing or {}).get("div_at") or stamp,
                "updated_at": stamp,
            }
        )
        result["applied"] = True
        result["enter_enabled"] = bool(saved.get("enter_enabled"))
        result["ticker_eligible"] = bool(saved.get("ticker_eligible"))
        logger.info(
            "premium_futures divergence DIV und=%s side=%s fut=%s enter=%s",
            underlying,
            side,
            fut_symbol,
            result["enter_enabled"],
        )
        return result

    if kind == "GO":
        # Matching Today's pick (same currmonth contract, same side) is updated in place.
        # A GO with no active pick creates that pick from arbitrage_master, then follows through.
        existing = store.get_pick(trade_date, underlying, side)
        in_pick = bool(existing and existing.get("active"))
        saved = store.save_pick(
            {
                **(existing if in_pick else {}),
                "trade_date": trade_date,
                "underlying": underlying,
                "side": side,
                "fut_symbol": fut_symbol,
                "instrument_key": ikey,
                "contracts": alert.get("contracts")
                if alert.get("contracts") is not None
                else (existing.get("contracts") if in_pick and existing else None),
                "active": True,
                "enter_enabled": True,
                "ticker_eligible": True,
                "screening_id": existing.get("screening_id") if in_pick and existing else None,
                "action": action,
                "div_at": (existing.get("div_at") if in_pick and existing else None) or stamp,
                "updated_at": stamp,
            }
        )
        price, missing = _safe_ltp(ltp_fn, ikey)
        first_scan = _aware(saved.get("div_at") or stamp) or _aware(stamp)
        assert first_scan is not None
        entry_clock = entry_at_from_first_scan(first_scan)
        # +5 min quote is not available at the GO instant; keep the scan LTP so Entry LTP is not blank.
        entry_px = resolve_entry_ltp(None, price)
        lot = one_lot_qty(ikey, lot_fn)
        store.add_could(
            {
                "trade_date": trade_date,
                "underlying": underlying,
                "side": side,
                "fut_symbol": fut_symbol,
                "instrument_key": ikey,
                "contracts": saved.get("contracts"),
                "direction_type": _direction(side),
                "entry_ltp": entry_px,
                "entry_at": _naive(entry_clock),
                "exit_ltp": None,
                "exit_at": None,
                "first_scan_at": _naive(first_scan),
                "scan_ltp": price,
                "current_ltp": price,
                "lot_qty": lot,
                "entry_ltp_from_history": False,
                "ltp_missing_entry": missing or entry_px is None,
                "ltp_missing_exit": False,
            }
        )
        result["applied"] = True
        result["enter_enabled"] = True
        result["ticker_eligible"] = True
        result["entry_ltp"] = entry_px
        result["ltp_missing"] = missing
        if missing:
            result["reason"] = "LTP missing; could-have entry stored with null price"
        logger.info(
            "premium_futures divergence GO und=%s side=%s entry_ltp=%s missing=%s",
            underlying,
            side,
            price,
            missing,
        )
        return result

    # EXIT: remove from both Today's pick sections. Stamp the matching could-have row when one exists.
    open_row = store.open_could(trade_date, underlying, side)
    if open_row:
        price, missing = _safe_ltp(ltp_fn, ikey)
        store.stamp_exit(int(open_row["id"]), price, clock, ltp_missing=missing)
        result["exit_ltp"] = price
        result["ltp_missing"] = missing
        if missing:
            result["reason"] = "LTP missing; exit stored with null price"
    store.deactivate_symbol(trade_date, underlying)
    result["applied"] = True
    result["enter_enabled"] = False
    result["ticker_eligible"] = False
    logger.info(
        "premium_futures divergence EXIT und=%s side=%s exit_ltp=%s",
        underlying,
        side,
        result.get("exit_ltp"),
    )
    return result


def divergence_workspace_picks(
    store: Any, trade_date: date
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    bull: List[Dict[str, Any]] = []
    bear: List[Dict[str, Any]] = []
    for pick in store.active_picks(trade_date):
        side = str(pick.get("side") or "bullish")
        enter = bool(pick.get("enter_enabled"))
        row = {
            "screening_id": pick.get("screening_id"),
            "underlying": pick.get("underlying"),
            "direction_type": _direction(side),
            "future_symbol": pick.get("fut_symbol") or pick.get("underlying"),
            "instrument_key": pick.get("instrument_key"),
            "lot_size": None,
            "scan_count": 1,
            "first_hit_at": _iso(pick.get("div_at") or pick.get("updated_at")),
            "last_hit_at": _iso(pick.get("updated_at")),
            "order_eligible": enter,
            "order_block_reason": None if enter else "Waiting for GO alert",
            "source": SOURCE,
            "divergence_action": pick.get("action"),
            "contracts": pick.get("contracts"),
            "conviction_score": None,
            "ltp": None,
        }
        if side == "bearish":
            bear.append(row)
        else:
            bull.append(row)
    return bull, bear


def divergence_ticker_signals(store: Any, trade_date: date) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for pick in store.active_picks(trade_date):
        if not pick.get("ticker_eligible"):
            continue
        side = str(pick.get("side") or "bullish")
        out.append(
            {
                "algo": "premium_futures",
                "symbol": pick.get("fut_symbol") or pick.get("underlying"),
                "label": "Bearish" if side == "bearish" else "Bullish",
            }
        )
    return out


def _public_could_row(c: Dict[str, Any]) -> Dict[str, Any]:
    direction = c.get("direction_type") or _direction(str(c.get("side") or "bullish"))
    exit_at = c.get("exit_at")
    exit_ltp = c.get("exit_ltp")
    current = c.get("current_ltp")
    if exit_at is not None:
        if exit_ltp is not None:
            current = exit_ltp
        elif current is None:
            current = c.get("scan_ltp") if c.get("scan_ltp") is not None else c.get("entry_ltp")
    entry_ltp = c.get("entry_ltp")
    if entry_ltp is None:
        entry_ltp = resolve_entry_ltp(None, c.get("scan_ltp"))
    qty = c.get("lot_qty")
    if qty is None:
        qty = one_lot_qty(str(c.get("instrument_key") or ""))
    try:
        qty_out = int(qty) if qty is not None else None
    except (TypeError, ValueError):
        qty_out = None
    if qty_out is not None and qty_out <= 0:
        qty_out = None
    entry_display = display_entry_at(c.get("first_scan_at"), c.get("entry_at"))
    return {
        "underlying": c.get("underlying"),
        "future_symbol": c.get("fut_symbol"),
        "direction_type": direction,
        "instrument_key": c.get("instrument_key"),
        "qty": qty_out,
        "first_scan_time": _hm(c.get("first_scan_at")),
        "entry_time": _hm(entry_display),
        "entry_ltp": entry_ltp,
        "entry_price": entry_ltp,
        "exit_time": _hm(exit_at),
        "exit_ltp": exit_ltp,
        "exit_price": exit_ltp,
        "exit_scan_time": _hm(exit_at),
        "exit_scan_ltp": exit_ltp if exit_at is not None else None,
        "current_ltp": current,
        "pnl_scan_rupees": could_have_pnl_rupees(direction, entry_ltp, current, qty_out),
        "source": SOURCE,
        "ltp_missing_entry": bool(c.get("ltp_missing_entry")),
        "ltp_missing_exit": bool(c.get("ltp_missing_exit")),
    }


def divergence_could_have_rows(
    store: Any,
    trade_date: date,
    now: Optional[datetime] = None,
    persist_flat: bool = False,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for c in store.could_rows(trade_date):
        row = dict(c)
        if now is not None and row.get("exit_at") is None:
            flat = apply_session_flat(row, now)
            if flat.get("exit_at") is not None:
                if persist_flat and row.get("id") is not None:
                    store.stamp_exit(
                        int(row["id"]),
                        flat.get("exit_ltp"),
                        flat["exit_at"],
                        ltp_missing=flat.get("exit_ltp") is None,
                    )
                row = flat
        rows.append(_public_could_row(row))
    return rows


def load_divergence_sections(
    trade_date: date,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    try:
        store = DbDivergenceStore()
        bull, bear = divergence_workspace_picks(store, trade_date)
        # Frozen rows stay on the prior session until 09:00 IST (through 08:59). Not cleared at midnight.
        return bull, bear, divergence_could_have_rows(
            store, trade_date, now=datetime.now(IST), persist_flat=True
        )
    except Exception:
        logger.warning("premium_futures divergence workspace read failed", exc_info=True)
        return [], [], []


def load_divergence_ticker_signals() -> List[Dict[str, Any]]:
    try:
        store = DbDivergenceStore()
        return divergence_ticker_signals(store, datetime.now(IST).date())
    except Exception:
        logger.warning("premium_futures divergence ticker read failed", exc_info=True)
        return []


def divergence_enter_allowed(underlying: str, direction_type: str, screening_source: str) -> bool:
    """Enter confirm bypass only for a divergence-owned screening whose GO is on."""
    if str(screening_source or "") != SOURCE:
        return False
    side = "bearish" if str(direction_type or "").strip().upper() == "SHORT" else "bullish"
    und = str(underlying or "").strip().upper()
    if not und:
        return False
    try:
        pick = DbDivergenceStore().get_pick(datetime.now(IST).date(), und, side)
    except Exception:
        logger.warning("premium_futures divergence enter check failed", exc_info=True)
        return False
    return bool(pick and pick.get("active") and pick.get("enter_enabled"))


def _currmth_ltp_by_underlying() -> Dict[str, float]:
    """Curr-month future LTP already written by the Kavach 10-minute market-data job."""
    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT UPPER(TRIM(stock)) AS stock, currmth_future_ltp
                FROM arbitrage_master
                WHERE currmth_future_ltp IS NOT NULL AND currmth_future_ltp > 0
                """
            )
        ).fetchall()
    finally:
        db.close()
    out: Dict[str, float] = {}
    for stock, ltp in rows:
        try:
            price = float(ltp)
        except (TypeError, ValueError):
            continue
        if stock and price > 0:
            out[str(stock).strip().upper()] = price
    return out


def _cached_5m_ltp_at(instrument_key: str, target: datetime) -> Optional[float]:
    """LTP at a clock from 5m bars the 10-minute warm already cached. No extra fetch."""
    try:
        from backend.services.daily_futures_service import _ltp_asof_ist
        from backend.services.market_data.candle_cache import get_recent
    except Exception:
        logger.debug("premium_futures could-have candle cache unavailable", exc_info=True)
        return None
    candles = get_recent(str(instrument_key or "").strip(), "minutes/5", max_age_sec=20 * 60)
    if not candles:
        return None
    target_aware = _aware(target)
    if target_aware is None:
        return None
    return _ltp_asof_ist(candles, target_aware)


def refresh_could_have_on_scan(now: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Update open Trade-if-could rows from the curr-month LTP the 10-minute scan just stored.

    Rows with an Exit time are left frozen. At/after 15:15 IST with no EXIT, exit becomes 15:15.
    """
    clock = _aware(now or datetime.now(IST))
    assert clock is not None
    summary = {"updated": 0, "flattened": 0, "entry_upgraded": 0, "skipped_frozen": 0}
    try:
        quotes = _currmth_ltp_by_underlying()
    except Exception:
        logger.warning("premium_futures could-have quote read failed", exc_info=True)
        quotes = {}
    try:
        store = DbDivergenceStore()
        open_rows = store.open_could_rows()
    except Exception:
        logger.warning("premium_futures could-have open rows failed", exc_info=True)
        return summary
    for row in open_rows:
        if row.get("exit_at") is not None:
            summary["skipped_frozen"] += 1
            continue
        trade_date = _as_trade_date(row.get("trade_date"))
        if trade_date is not None and session_exit_due(trade_date, clock):
            flat = apply_session_flat(row, clock)
            try:
                store.stamp_exit(
                    int(row["id"]),
                    flat.get("exit_ltp"),
                    flat["exit_at"],
                    ltp_missing=flat.get("exit_ltp") is None,
                )
                summary["flattened"] += 1
            except Exception:
                logger.warning(
                    "premium_futures could-have 15:15 stamp failed id=%s", row.get("id"), exc_info=True
                )
            continue
        if not row.get("entry_ltp_from_history"):
            entry_clock = display_entry_at(row.get("first_scan_at"), row.get("entry_at"))
            if entry_clock is not None and clock >= entry_clock:
                hist = _cached_5m_ltp_at(str(row.get("instrument_key") or ""), entry_clock)
                upgraded = resolve_entry_ltp(hist, None)
                if upgraded is not None:
                    try:
                        if store.set_entry_ltp(int(row["id"]), upgraded):
                            summary["entry_upgraded"] += 1
                    except Exception:
                        logger.debug(
                            "premium_futures could-have entry LTP upgrade skipped id=%s",
                            row.get("id"),
                            exc_info=True,
                        )
        und = str(row.get("underlying") or "").strip().upper()
        price = quotes.get(und)
        if price is None:
            continue
        try:
            if store.set_current_ltp(int(row["id"]), price):
                summary["updated"] += 1
        except Exception:
            logger.warning(
                "premium_futures could-have LTP update failed id=%s", row.get("id"), exc_info=True
            )
    logger.info("premium_futures could-have scan refresh %s", summary)
    return summary


def ingest_divergence_webhook(parsed: Any, raw_body: str, received_at: datetime) -> Optional[Dict[str, Any]]:
    """Run the new flow when the body matches. None means the JSON handler should run."""
    if select_premium_futures_handler(parsed, raw_body) != HANDLER_DIVERGENCE:
        return None
    alert = parse_divergence_alert(parsed, raw_body)
    if alert is None:
        return None
    from backend.services.premium_futures_tv_webhook import resolve_currmth_future

    return apply_divergence_event(
        alert,
        store=DbDivergenceStore(),
        resolver=resolve_currmth_future,
        ltp_fn=premium_futures_ltp,
        now=received_at,
    )

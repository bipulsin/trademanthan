"""Trap-CE Live Chartink webhook ingest — log only, no trade/SL/P&L."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.ist_datetime import naive_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
_MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "add_trap_ce_live_webhook_log.sql"
_ENSURED = False

PARSE_SUCCESS = "success"
PARSE_PARTIAL = "partial"
PARSE_FAILED = "failed"


def now_ist_second() -> datetime:
    return naive_ist(datetime.now(IST))


def ensure_trap_ce_live_webhook_table() -> None:
    global _ENSURED
    if _ENSURED:
        return
    if not _MIGRATION.is_file():
        raise FileNotFoundError(f"migration missing: {_MIGRATION}")
    sql = _MIGRATION.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
    _ENSURED = True
    logger.info("trap_ce_live_webhook_log table ensured")


def decode_raw_payload(body: bytes) -> Tuple[Any, Dict[str, Any]]:
    """Return (parsed_object_or_none, raw_payload_for_jsonb). Never drops the request."""
    text_body = body.decode("utf-8", errors="replace") if body else ""
    if not text_body.strip():
        return None, {}
    try:
        parsed = json.loads(text_body)
        if isinstance(parsed, dict):
            return parsed, parsed
        return parsed, {"_json": parsed}
    except (json.JSONDecodeError, ValueError):
        return None, {"_raw": text_body}


def _split_csv(value: Any) -> Optional[List[str]]:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    s = str(value).strip()
    if not s:
        return []
    return [p.strip() for p in s.split(",") if p.strip()]


def _parse_price(raw: Optional[str]) -> Optional[float]:
    if raw is None or raw == "":
        return None
    try:
        return float(str(raw).strip())
    except (TypeError, ValueError):
        return None


def parse_trap_ce_webhook(parsed: Any) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Pair stocks and trigger_prices by position.
    Unequal length → partial. Missing/unusable → failed (caller inserts one null-symbol row).
    """
    if not isinstance(parsed, dict):
        return PARSE_FAILED, []

    stocks = _split_csv(parsed.get("stocks")) if "stocks" in parsed else None
    prices = _split_csv(parsed.get("trigger_prices")) if "trigger_prices" in parsed else None

    if stocks is None or not stocks:
        return PARSE_FAILED, []

    price_list = prices if prices is not None else []
    n = len(stocks)
    n_prices = len(price_list)
    rows: List[Dict[str, Any]] = []
    prices_ok = True
    for i, symbol in enumerate(stocks):
        price_raw = price_list[i] if i < n_prices else None
        price = _parse_price(price_raw) if price_raw is not None else None
        if price_raw is not None and price is None:
            prices_ok = False
        rows.append({"symbol": symbol, "trigger_price": price})

    equal = n == n_prices and prices is not None
    if equal and prices_ok and all(r["trigger_price"] is not None for r in rows):
        return PARSE_SUCCESS, rows
    return PARSE_PARTIAL, rows


def _meta(parsed: Any) -> Dict[str, Optional[str]]:
    if not isinstance(parsed, dict):
        return {"triggered_at_raw": None, "scan_name": None, "alert_name": None}

    def _s(key: str) -> Optional[str]:
        if key not in parsed or parsed[key] is None:
            return None
        s = str(parsed[key]).strip()
        return s or None

    return {
        "triggered_at_raw": _s("triggered_at"),
        "scan_name": _s("scan_name"),
        "alert_name": _s("alert_name"),
    }


def insert_webhook_rows(
    *,
    received_at: datetime,
    source_ip: Optional[str],
    parsed: Any,
    raw_payload: Dict[str, Any],
) -> Tuple[str, int]:
    ensure_trap_ce_live_webhook_table()
    status, pairs = parse_trap_ce_webhook(parsed)
    meta = _meta(parsed)
    payload_json = json.dumps(raw_payload)
    db = SessionLocal()
    try:
        if not pairs:
            db.execute(
                text(
                    """
                    INSERT INTO trap_ce_live_webhook_log (
                        received_at, source_ip, symbol, trigger_price,
                        triggered_at_raw, scan_name, alert_name, raw_payload, parse_status
                    ) VALUES (
                        :received_at, :source_ip, NULL, NULL,
                        :triggered_at_raw, :scan_name, :alert_name,
                        CAST(:raw_payload AS jsonb), :parse_status
                    )
                    """
                ),
                {
                    "received_at": received_at,
                    "source_ip": source_ip,
                    "triggered_at_raw": meta["triggered_at_raw"],
                    "scan_name": meta["scan_name"],
                    "alert_name": meta["alert_name"],
                    "raw_payload": payload_json,
                    "parse_status": PARSE_FAILED,
                },
            )
            db.commit()
            return PARSE_FAILED, 1

        for pair in pairs:
            db.execute(
                text(
                    """
                    INSERT INTO trap_ce_live_webhook_log (
                        received_at, source_ip, symbol, trigger_price,
                        triggered_at_raw, scan_name, alert_name, raw_payload, parse_status
                    ) VALUES (
                        :received_at, :source_ip, :symbol, :trigger_price,
                        :triggered_at_raw, :scan_name, :alert_name,
                        CAST(:raw_payload AS jsonb), :parse_status
                    )
                    """
                ),
                {
                    "received_at": received_at,
                    "source_ip": source_ip,
                    "symbol": pair["symbol"],
                    "trigger_price": pair["trigger_price"],
                    "triggered_at_raw": meta["triggered_at_raw"],
                    "scan_name": meta["scan_name"],
                    "alert_name": meta["alert_name"],
                    "raw_payload": payload_json,
                    "parse_status": status,
                },
            )
        db.commit()
        return status, len(pairs)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _fmt_received(dt: Any) -> Tuple[str, str]:
    if dt is None:
        return "", ""
    if isinstance(dt, datetime):
        naive = naive_ist(dt) if dt.tzinfo else dt.replace(microsecond=0)
        return naive.strftime("%H:%M:%S"), naive.strftime("%Y-%m-%d %H:%M:%S")
    s = str(dt)
    if len(s) >= 19:
        return s[11:19], s[:19]
    return s, s


def fetch_live_webhook_rows(session_date: date) -> Dict[str, Any]:
    ensure_trap_ce_live_webhook_table()
    db = SessionLocal()
    try:
        days = [
            str(r[0])
            for r in db.execute(
                text(
                    """
                    SELECT DISTINCT CAST(received_at AS date) AS d
                    FROM trap_ce_live_webhook_log
                    ORDER BY d DESC
                    LIMIT 30
                    """
                )
            ).fetchall()
        ]
        rows_raw = db.execute(
            text(
                """
                SELECT id, received_at, source_ip, symbol, trigger_price,
                       triggered_at_raw, scan_name, alert_name, raw_payload, parse_status
                FROM trap_ce_live_webhook_log
                WHERE CAST(received_at AS date) = CAST(:d AS date)
                ORDER BY received_at DESC, id DESC
                """
            ),
            {"d": session_date.isoformat()},
        ).mappings().all()
        rows = []
        for r in rows_raw:
            hhmmss, full = _fmt_received(r["received_at"])
            payload = r["raw_payload"]
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {"_raw": payload}
            triggered_raw = r["triggered_at_raw"]
            if not triggered_raw and isinstance(payload, dict):
                triggered_raw = payload.get("triggered_at")
                if triggered_raw is not None:
                    triggered_raw = str(triggered_raw).strip() or None
            rows.append(
                {
                    "id": r["id"],
                    "received_at": full,
                    "received_at_hms": hhmmss,
                    "source_ip": r["source_ip"],
                    "symbol": r["symbol"],
                    "trigger_price": r["trigger_price"],
                    "triggered_at": triggered_raw,
                    "triggered_at_raw": triggered_raw,
                    "scan_name": r["scan_name"],
                    "alert_name": r["alert_name"],
                    "raw_payload": payload,
                    "parse_status": r["parse_status"],
                }
            )
        today = datetime.now(IST).date().isoformat()
        if today not in days:
            days = [today] + days
        return {
            "ok": True,
            "date": session_date.isoformat(),
            "days": days,
            "count": len(rows),
            "rows": rows,
        }
    finally:
        db.close()

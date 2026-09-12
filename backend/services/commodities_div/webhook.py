"""Commodities Div webhook ingest + Section 4a state machine."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.mapping import attach_instrument_fields, normalize_tv_ticker
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.ist_datetime import naive_ist

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

VALID_FLAGS = {
    "BULL-DIV",
    "BEAR-DIV",
    "BULL-GO",
    "BEAR-GO",
    "BULL-EXIT",
    "BEAR-EXIT",
}

STATUS_DIVERGENCE = "Divergence"
STATUS_ACTIVATED = "Activated"
STATUS_IN_TRADE = "In-Trade"
STATUS_EXIT_TRADE = "Exit Trade"
STATUS_HISTORY = "History"

ACTIVE_PRE_TRADE = {STATUS_DIVERGENCE, STATUS_ACTIVATED}
BLOCKING_STATUSES = {STATUS_IN_TRADE, STATUS_EXIT_TRADE}


def now_ist_second() -> datetime:
    return naive_ist(datetime.now(IST))


def decode_raw_payload(body: bytes) -> Tuple[Any, Dict[str, Any]]:
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


def _ms_to_ist(ms: Any) -> Optional[datetime]:
    try:
        n = int(ms)
    except (TypeError, ValueError):
        return None
    if n > 10_000_000_000:  # ms
        ts = n / 1000.0
    else:
        ts = float(n)
    try:
        utc = datetime.fromtimestamp(ts, tz=timezone.utc)
        return naive_ist(utc.astimezone(IST))
    except (OverflowError, OSError, ValueError):
        return None


def parse_flag_fields(parsed: Any) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Return (parse_status, fields_dict). fields None on hard fail."""
    if not isinstance(parsed, dict):
        return "failed", None
    flag = str(parsed.get("flag") or "").strip().upper()
    symbol = str(parsed.get("symbol") or "").strip()
    time_raw = parsed.get("time")
    if flag not in VALID_FLAGS or not symbol:
        return "failed", None
    try:
        tv_ms = int(time_raw) if time_raw is not None else None
    except (TypeError, ValueError):
        tv_ms = None
        return "partial", {
            "flag": flag,
            "symbol_raw": symbol,
            "direction": "BULL" if flag.startswith("BULL") else "BEAR",
            "signal_kind": flag.split("-", 1)[-1],
            "tv_time_ms": None,
            "tv_time_ist": None,
        }
    kind = flag.split("-", 1)[-1]  # DIV | GO | EXIT
    direction = "BULL" if flag.startswith("BULL") else "BEAR"
    return "success", {
        "flag": flag,
        "symbol_raw": symbol,
        "direction": direction,
        "signal_kind": kind,
        "tv_time_ms": tv_ms,
        "tv_time_ist": _ms_to_ist(tv_ms) if tv_ms is not None else None,
    }


def _get_active(db) -> Optional[Dict[str, Any]]:
    row = db.execute(
        text(
            """
            SELECT * FROM commodities_div_signals
            WHERE status <> 'History'
            ORDER BY id DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    return dict(row) if row else None


def _insert_log(
    db,
    *,
    received_at: datetime,
    source_ip: Optional[str],
    fields: Optional[Dict[str, Any]],
    raw_payload: Dict[str, Any],
    parse_status: str,
    disposition: Optional[str],
    active_signal_id: Optional[int] = None,
    symbol_mapped: Optional[str] = None,
) -> int:
    rid = db.execute(
        text(
            """
            INSERT INTO commodities_div_webhook_log (
                received_at, source_ip, flag, symbol_raw, symbol_mapped,
                direction, signal_kind, tv_time_ms, tv_time_ist,
                raw_payload, parse_status, disposition, active_signal_id
            ) VALUES (
                :received_at, :source_ip, :flag, :symbol_raw, :symbol_mapped,
                :direction, :signal_kind, :tv_time_ms, :tv_time_ist,
                CAST(:raw_payload AS jsonb), :parse_status, :disposition, :active_signal_id
            )
            RETURNING id
            """
        ),
        {
            "received_at": received_at,
            "source_ip": source_ip,
            "flag": (fields or {}).get("flag"),
            "symbol_raw": (fields or {}).get("symbol_raw"),
            "symbol_mapped": symbol_mapped,
            "direction": (fields or {}).get("direction"),
            "signal_kind": (fields or {}).get("signal_kind"),
            "tv_time_ms": (fields or {}).get("tv_time_ms"),
            "tv_time_ist": (fields or {}).get("tv_time_ist"),
            "raw_payload": json.dumps(raw_payload or {}),
            "parse_status": parse_status,
            "disposition": disposition,
            "active_signal_id": active_signal_id,
        },
    ).scalar()
    return int(rid)


def _delete_active_pre_trade(db) -> Optional[int]:
    """Delete Divergence/Activated active row. Returns deleted id."""
    row = db.execute(
        text(
            """
            DELETE FROM commodities_div_signals
            WHERE status IN ('Divergence', 'Activated')
            RETURNING id
            """
        )
    ).first()
    return int(row[0]) if row else None


def _create_divergence(
    db,
    *,
    fields: Dict[str, Any],
    received_at: datetime,
    log_id: int,
    inst: Dict[str, Any],
) -> int:
    rid = db.execute(
        text(
            """
            INSERT INTO commodities_div_signals (
                symbol_raw, symbol_mapped, direction, status,
                div_received_at, div_tv_time_ist, div_webhook_log_id,
                instrument_key, contract, lot_size, updated_at
            ) VALUES (
                :symbol_raw, :symbol_mapped, :direction, 'Divergence',
                :div_received_at, :div_tv_time_ist, :div_webhook_log_id,
                :instrument_key, :contract, :lot_size, NOW()
            )
            RETURNING id
            """
        ),
        {
            "symbol_raw": fields["symbol_raw"],
            "symbol_mapped": inst["symbol_mapped"],
            "direction": fields["direction"],
            "div_received_at": received_at,
            "div_tv_time_ist": fields.get("tv_time_ist"),
            "div_webhook_log_id": log_id,
            "instrument_key": inst.get("instrument_key"),
            "contract": inst.get("contract"),
            "lot_size": inst.get("lot_size"),
        },
    ).scalar()
    return int(rid)


def process_webhook(
    *,
    received_at: datetime,
    source_ip: Optional[str],
    body: bytes,
) -> Dict[str, Any]:
    ensure_commodities_div_tables()
    parsed, raw_payload = decode_raw_payload(body)
    parse_status, fields = parse_flag_fields(parsed)

    db = SessionLocal()
    try:
        if fields is None:
            log_id = _insert_log(
                db,
                received_at=received_at,
                source_ip=source_ip,
                fields=None,
                raw_payload=raw_payload,
                parse_status="failed",
                disposition="parse_failed",
            )
            db.commit()
            return {
                "ok": True,
                "stored": True,
                "parse_status": "failed",
                "disposition": "parse_failed",
                "log_id": log_id,
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
            }

        inst = attach_instrument_fields(fields["symbol_raw"])
        symbol_mapped = inst["symbol_mapped"]
        active = _get_active(db)
        kind = fields["signal_kind"]
        direction = fields["direction"]

        # Unknown underlying (not one of the five) — log only, never create / mutate active.
        if not inst.get("underlying_matched"):
            log_id = _insert_log(
                db,
                received_at=received_at,
                source_ip=source_ip,
                fields=fields,
                raw_payload=raw_payload,
                parse_status="unmatched",
                disposition="unmatched",
                active_signal_id=int(active["id"]) if active else None,
                symbol_mapped=symbol_mapped or None,
            )
            db.commit()
            return {
                "ok": True,
                "stored": True,
                "parse_status": "unmatched",
                "disposition": "unmatched",
                "log_id": log_id,
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": "unknown_underlying",
                "allowed": [
                    "CRUDEOIL",
                    "NATURALGAS",
                    "COPPER",
                    "GOLDPETAL",
                    "SILVERMINI",
                ],
            }

        # --- DIV ---
        if kind == "DIV":
            if active and active["status"] in BLOCKING_STATUSES:
                log_id = _insert_log(
                    db,
                    received_at=received_at,
                    source_ip=source_ip,
                    fields=fields,
                    raw_payload=raw_payload,
                    parse_status=parse_status if parse_status != "failed" else "unmatched",
                    disposition="ignored_in_trade_block",
                    active_signal_id=int(active["id"]),
                    symbol_mapped=symbol_mapped,
                )
                db.commit()
                return {
                    "ok": True,
                    "stored": True,
                    "parse_status": "unmatched",
                    "disposition": "ignored_in_trade_block",
                    "log_id": log_id,
                    "active_id": int(active["id"]),
                    "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "underlying_matched": True,
                    "instrument_key": inst.get("instrument_key"),
                }

            disposition = "applied"
            if active and active["status"] in ACTIVE_PRE_TRADE:
                _delete_active_pre_trade(db)
                disposition = "replaced_prior"

            # Insert log first without signal id, then update
            log_id = _insert_log(
                db,
                received_at=received_at,
                source_ip=source_ip,
                fields=fields,
                raw_payload=raw_payload,
                parse_status=parse_status,
                disposition=disposition,
                symbol_mapped=symbol_mapped,
            )
            sig_id = _create_divergence(
                db, fields=fields, received_at=received_at, log_id=log_id, inst=inst
            )
            db.execute(
                text(
                    "UPDATE commodities_div_webhook_log SET active_signal_id = :sid WHERE id = :lid"
                ),
                {"sid": sig_id, "lid": log_id},
            )
            db.commit()
            return {
                "ok": True,
                "stored": True,
                "parse_status": parse_status,
                "disposition": disposition,
                "log_id": log_id,
                "signal_id": sig_id,
                "status": STATUS_DIVERGENCE,
                "symbol_mapped": symbol_mapped,
                "underlying_matched": True,
                "instrument_key": inst.get("instrument_key"),
                "contract": inst.get("contract"),
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
            }

        # --- GO ---
        if kind == "GO":
            match = (
                active
                and str(active.get("direction")) == direction
                and _symbols_match(active, fields["symbol_raw"], symbol_mapped)
                and active["status"] == STATUS_DIVERGENCE
            )
            if not match:
                # Also allow GO if already Activated same symbol (idempotent) — treat as unmatched duplicate
                log_id = _insert_log(
                    db,
                    received_at=received_at,
                    source_ip=source_ip,
                    fields=fields,
                    raw_payload=raw_payload,
                    parse_status="unmatched",
                    disposition="unmatched",
                    active_signal_id=int(active["id"]) if active else None,
                    symbol_mapped=symbol_mapped,
                )
                db.commit()
                return {
                    "ok": True,
                    "stored": True,
                    "parse_status": "unmatched",
                    "disposition": "unmatched",
                    "log_id": log_id,
                    "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "reason": "no_matching_divergence",
                }

            log_id = _insert_log(
                db,
                received_at=received_at,
                source_ip=source_ip,
                fields=fields,
                raw_payload=raw_payload,
                parse_status=parse_status,
                disposition="applied",
                active_signal_id=int(active["id"]),
                symbol_mapped=symbol_mapped,
            )
            # Refresh instrument if mapping now available
            db.execute(
                text(
                    """
                    UPDATE commodities_div_signals SET
                        status = 'Activated',
                        go_received_at = :go_at,
                        go_tv_time_ist = :go_tv,
                        go_webhook_log_id = :log_id,
                        instrument_key = COALESCE(:ik, instrument_key),
                        contract = COALESCE(:contract, contract),
                        lot_size = COALESCE(:lot, lot_size),
                        updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {
                    "go_at": received_at,
                    "go_tv": fields.get("tv_time_ist"),
                    "log_id": log_id,
                    "ik": inst.get("instrument_key"),
                    "contract": inst.get("contract"),
                    "lot": inst.get("lot_size"),
                    "id": int(active["id"]),
                },
            )
            db.commit()
            return {
                "ok": True,
                "stored": True,
                "parse_status": parse_status,
                "disposition": "applied",
                "log_id": log_id,
                "signal_id": int(active["id"]),
                "status": STATUS_ACTIVATED,
                "underlying_matched": True,
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
            }

        # --- EXIT ---
        if kind == "EXIT":
            match = (
                active
                and str(active.get("direction")) == direction
                and _symbols_match(active, fields["symbol_raw"], symbol_mapped)
                and active["status"] == STATUS_IN_TRADE
            )
            if not match:
                log_id = _insert_log(
                    db,
                    received_at=received_at,
                    source_ip=source_ip,
                    fields=fields,
                    raw_payload=raw_payload,
                    parse_status="unmatched",
                    disposition="unmatched",
                    active_signal_id=int(active["id"]) if active else None,
                    symbol_mapped=symbol_mapped,
                )
                db.commit()
                return {
                    "ok": True,
                    "stored": True,
                    "parse_status": "unmatched",
                    "disposition": "unmatched",
                    "log_id": log_id,
                    "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "reason": "no_matching_in_trade",
                }

            log_id = _insert_log(
                db,
                received_at=received_at,
                source_ip=source_ip,
                fields=fields,
                raw_payload=raw_payload,
                parse_status=parse_status,
                disposition="applied",
                active_signal_id=int(active["id"]),
                symbol_mapped=symbol_mapped,
            )
            db.execute(
                text(
                    """
                    UPDATE commodities_div_signals SET
                        status = 'Exit Trade',
                        exit_signal_received_at = :ex_at,
                        exit_tv_time_ist = :ex_tv,
                        exit_webhook_log_id = :log_id,
                        updated_at = NOW()
                    WHERE id = :id
                    """
                ),
                {
                    "ex_at": received_at,
                    "ex_tv": fields.get("tv_time_ist"),
                    "log_id": log_id,
                    "id": int(active["id"]),
                },
            )
            db.commit()
            return {
                "ok": True,
                "stored": True,
                "parse_status": parse_status,
                "disposition": "applied",
                "log_id": log_id,
                "signal_id": int(active["id"]),
                "status": STATUS_EXIT_TRADE,
                "play_exit_audio": True,
                "received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
            }

        # unreachable
        log_id = _insert_log(
            db,
            received_at=received_at,
            source_ip=source_ip,
            fields=fields,
            raw_payload=raw_payload,
            parse_status="failed",
            disposition="parse_failed",
            symbol_mapped=symbol_mapped,
        )
        db.commit()
        return {"ok": True, "stored": True, "parse_status": "failed", "log_id": log_id}
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _symbols_match(active: Dict[str, Any], symbol_raw: str, symbol_mapped: str) -> bool:
    a_raw = str(active.get("symbol_raw") or "").strip().upper()
    a_map = str(active.get("symbol_mapped") or "").strip().upper()
    raw_u = (symbol_raw or "").strip().upper()
    map_u = (symbol_mapped or "").strip().upper()
    norm_in = normalize_tv_ticker(raw_u) or raw_u
    norm_a = normalize_tv_ticker(a_raw) or a_raw
    if raw_u == a_raw or map_u == a_map:
        return True
    if norm_in and norm_in == norm_a:
        return True
    if map_u and map_u == a_map:
        return True
    return False

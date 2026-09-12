#!/usr/bin/env python3
"""Simulate Commodities Div lifecycle against the configured DATABASE_URL.

Usage (on paperclip app container or any host with DB access):
  ENVIRONMENT=test COMM_DIV_WEBHOOK_TOKEN=x python3 scripts/commodities_div_lifecycle_sample.py
"""
from __future__ import annotations

import json
import sys

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.commodities_div.actions import (
    exit_submit,
    get_active_signal,
    list_history,
    take_trade,
)
from backend.services.commodities_div.schema import ensure_commodities_div_tables
from backend.services.commodities_div.webhook import now_ist_second, process_webhook


def body(flag: str, sym: str = "CDTEST1!", t: int = 1757675460000) -> bytes:
    return json.dumps({"flag": flag, "symbol": sym, "time": t}).encode()


def main() -> int:
    ensure_commodities_div_tables()
    db = SessionLocal()
    try:
        db.execute(
            text(
                "DELETE FROM commodities_div_signals "
                "WHERE symbol_mapped LIKE 'CDTEST%' OR symbol_raw LIKE 'CDTEST%'"
            )
        )
        db.execute(
            text(
                "DELETE FROM commodities_div_webhook_log "
                "WHERE symbol_raw LIKE 'CDTEST%' OR symbol_raw = 'OTHER1!' "
                "OR symbol_mapped LIKE 'CDTEST%'"
            )
        )
        db.commit()
    except Exception as e:
        db.rollback()
        print("cleanup:", e)
    finally:
        db.close()

    print("=== DIV1 ===")
    print(process_webhook(received_at=now_ist_second(), source_ip="127.0.0.1", body=body("BULL-DIV")))
    print("=== DIV2 replace ===")
    print(
        process_webhook(
            received_at=now_ist_second(),
            source_ip="127.0.0.1",
            body=body("BULL-DIV", "CDTEST2!", t=1757675560000),
        )
    )
    active = get_active_signal()
    print("active", active)
    print("=== GO ===")
    print(
        process_webhook(
            received_at=now_ist_second(),
            source_ip="127.0.0.1",
            body=body("BULL-GO", "CDTEST2!"),
        )
    )
    active = get_active_signal()
    assert active and active["status"] == "Activated", active
    print("=== TAKE ===")
    print(take_trade(signal_id=int(active["id"]), entry_price=1234.5))
    print("=== blocked DIV while In-Trade ===")
    print(
        process_webhook(
            received_at=now_ist_second(),
            source_ip="127.0.0.1",
            body=body("BEAR-DIV", "OTHER1!"),
        )
    )
    print("=== EXIT webhook ===")
    print(
        process_webhook(
            received_at=now_ist_second(),
            source_ip="127.0.0.1",
            body=body("BULL-EXIT", "CDTEST2!"),
        )
    )
    active = get_active_signal()
    assert active and active["status"] == "Exit Trade", active
    print("=== EXIT modal ===")
    print(
        exit_submit(
            signal_id=int(active["id"]),
            exit_price=1240.0,
            exit_at=now_ist_second().strftime("%Y-%m-%d %H:%M:%S"),
        )
    )
    print("active_after", get_active_signal())
    print("history", list_history(3))

    db = SessionLocal()
    try:
        logs = db.execute(
            text(
                """
                SELECT id, received_at, flag, symbol_raw, disposition, parse_status
                FROM commodities_div_webhook_log
                WHERE symbol_raw LIKE 'CDTEST%' OR symbol_raw = 'OTHER1!'
                ORDER BY id
                """
            )
        ).mappings().all()
        print("RAW_LOGS", [dict(r) for r in logs])
        sigs = db.execute(
            text(
                """
                SELECT id, symbol_raw, symbol_mapped, status, entry_price, exit_price, trade_log_id
                FROM commodities_div_signals
                WHERE symbol_raw LIKE 'CDTEST%' OR symbol_mapped LIKE 'CDTEST%'
                ORDER BY id
                """
            )
        ).mappings().all()
        print("SIGNALS", [dict(r) for r in sigs])
    finally:
        db.close()
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Heartbeat watchdog: Telegram if ExitEngine or scheduler silent > 3 minutes.

Diagnosis (2026-09): scheduler beat only ran on Delta snapshots (15m) while
SILENCE_SEC was 180 — so "silent 240s" fired every few minutes. Scheduler now
beats at least once a minute. Telegram fires once per incident until it clears.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

SILENCE_SEC = 180


def beat(key: str, detail: Dict[str, Any] | None = None) -> None:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                INSERT INTO tarang_heartbeat (key, last_beat_at, detail)
                VALUES (:k, NOW(), CAST(:d AS jsonb))
                ON CONFLICT (key) DO UPDATE SET last_beat_at = NOW(), detail = CAST(:d AS jsonb)
                """
            ),
            {"k": key, "d": json.dumps(detail or {})},
        )
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("heartbeat beat failed")
    finally:
        db.close()


def _incident_open(db, key: str) -> bool:
    row = db.execute(
        text("SELECT last_message FROM tarang_alert_dedupe WHERE dedupe_key = :k"),
        {"k": f"heartbeat:{key}:open"},
    ).mappings().first()
    return bool(row)


def _set_incident(db, key: str, open_: bool, message: str = "") -> None:
    dk = f"heartbeat:{key}:open"
    if open_:
        db.execute(
            text(
                """
                INSERT INTO tarang_alert_dedupe (dedupe_key, last_sent_at, last_message)
                VALUES (:k, NOW(), :msg)
                ON CONFLICT (dedupe_key) DO UPDATE
                SET last_message = EXCLUDED.last_message
                """
            ),
            {"k": dk, "msg": message[:500]},
        )
    else:
        db.execute(text("DELETE FROM tarang_alert_dedupe WHERE dedupe_key = :k"), {"k": dk})


def check_watchdog() -> Dict[str, Any]:
    ensure_tarang_tables()
    now = datetime.now(timezone.utc)
    fired = []
    cleared = []
    db = SessionLocal()
    try:
        rows = db.execute(text("SELECT key, last_beat_at FROM tarang_heartbeat")).mappings().all()
        by = {r["key"]: r["last_beat_at"] for r in rows}
        from backend.services.tarang.alerts_telegram import notify_critical

        for key in ("exit_engine", "scheduler"):
            ts = by.get(key)
            if ts is None:
                continue
            if getattr(ts, "tzinfo", None) is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (now - ts).total_seconds()
            failing = age > SILENCE_SEC
            open_inc = _incident_open(db, key)
            if failing and not open_inc:
                msg = f"Watchdog: {key} silent {age:.0f}s (>{SILENCE_SEC}s)"
                notify_critical(
                    db,
                    kind="heartbeat",
                    message=msg,
                    dedupe_key=f"heartbeat:{key}",
                    throttle_sec=10**9,
                )
                _set_incident(db, key, True, msg)
                fired.append(key)
            elif failing and open_inc:
                # Still failing — do not Telegram again.
                pass
            elif (not failing) and open_inc:
                _set_incident(db, key, False)
                cleared.append(key)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("watchdog failed")
    finally:
        db.close()
    return {"ok": True, "fired": fired, "cleared": cleared}


def recon_job() -> Dict[str, Any]:
    """Pause auto on mismatch — no-op without live flag (never sends)."""
    from backend.services.tarang.live_control import tarang_live_enabled

    if not tarang_live_enabled():
        return {"ok": True, "skipped": "live_disabled", "paused_auto": False}
    return {"ok": True, "skipped": "phase4_locked", "paused_auto": False}

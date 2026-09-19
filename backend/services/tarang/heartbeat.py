"""Heartbeat watchdog: Telegram if ExitEngine or scheduler silent > 3 minutes."""
from __future__ import annotations

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
        import json

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


def check_watchdog() -> Dict[str, Any]:
    ensure_tarang_tables()
    now = datetime.now(timezone.utc)
    fired = []
    db = SessionLocal()
    try:
        rows = db.execute(text("SELECT key, last_beat_at FROM tarang_heartbeat")).mappings().all()
        by = {r["key"]: r["last_beat_at"] for r in rows}
        for key in ("exit_engine", "scheduler"):
            ts = by.get(key)
            if ts is None:
                continue
            if getattr(ts, "tzinfo", None) is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age = (now - ts).total_seconds()
            if age > SILENCE_SEC:
                from backend.services.tarang.alerts_telegram import notify_critical

                notify_critical(
                    db,
                    kind="heartbeat",
                    message=f"Watchdog: {key} silent {age:.0f}s (>{SILENCE_SEC}s)",
                    dedupe_key=f"heartbeat:{key}",
                )
                fired.append(key)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("watchdog failed")
    finally:
        db.close()
    return {"ok": True, "fired": fired}


def recon_job() -> Dict[str, Any]:
    """Pause auto on mismatch — no-op without live flag (never sends)."""
    from backend.services.tarang.live_control import tarang_live_enabled

    if not tarang_live_enabled():
        return {"ok": True, "skipped": "live_disabled", "paused_auto": False}
    return {"ok": True, "skipped": "phase4_locked", "paused_auto": False}

"""Critical Tarang alerts → Telegram (separate chat/topic, dedupe + throttle)."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests
from sqlalchemy import text

from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

DEFAULT_THROTTLE_SEC = 900  # 15 minutes
EXIT_THROTTLE_SEC = 300
CRITICAL_KINDS = {
    "exit_trigger",
    "hard_exit_warning",
    "stale_feed",
    "upstox_token_expired",
    "kill_switch",
}


def _telegram_dest() -> Dict[str, Optional[str]]:
    return {
        "bot_token": (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_NOTIFY_BOT_TOKEN") or "").strip()
        or None,
        "chat_id": (
            os.getenv("TARANG_TELEGRAM_CHAT_ID")
            or os.getenv("TELEGRAM_TARANG_CHAT_ID")
            or ""
        ).strip()
        or None,
        "thread_id": (os.getenv("TARANG_TELEGRAM_THREAD_ID") or "").strip() or None,
    }


def _send_telegram(text_msg: str) -> bool:
    dest = _telegram_dest()
    if not dest["bot_token"] or not dest["chat_id"]:
        logger.info("tarang telegram skipped (set TARANG_TELEGRAM_CHAT_ID + TELEGRAM_BOT_TOKEN)")
        return False
    payload: Dict[str, Any] = {
        "chat_id": dest["chat_id"],
        "text": text_msg[:3900],
        "disable_web_page_preview": True,
    }
    if dest["thread_id"]:
        try:
            payload["message_thread_id"] = int(dest["thread_id"])
        except ValueError:
            payload["message_thread_id"] = dest["thread_id"]
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{dest['bot_token']}/sendMessage",
            json=payload,
            timeout=15,
        )
        if r.status_code != 200:
            logger.warning("tarang telegram HTTP %s %s", r.status_code, (r.text or "")[:200])
            return False
        return True
    except Exception as e:
        logger.warning("tarang telegram send failed: %s", e)
        return False


def _throttle_sec(kind: str) -> int:
    if kind == "exit_trigger":
        return EXIT_THROTTLE_SEC
    if kind == "kill_switch":
        return 1800
    return DEFAULT_THROTTLE_SEC


def notify_critical(
    db,
    *,
    kind: str,
    message: str,
    trade_id: Optional[int] = None,
    dedupe_key: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    throttle_sec: Optional[int] = None,
) -> Dict[str, Any]:
    """Insert in-app alert and fan-out to the Tarang Telegram channel with dedupe."""
    ensure_tarang_tables()
    kind_n = (kind or "info").strip()
    key = dedupe_key or f"{kind_n}:{trade_id or 'sys'}:{message[:80]}"
    wait = int(throttle_sec if throttle_sec is not None else _throttle_sec(kind_n))
    now = datetime.now(timezone.utc)
    suppressed = False
    try:
        prev = db.execute(
            text("SELECT last_sent_at FROM tarang_alert_dedupe WHERE dedupe_key = :k"),
            {"k": key},
        ).mappings().first()
        if prev and prev.get("last_sent_at"):
            last = prev["last_sent_at"]
            if getattr(last, "tzinfo", None) is None:
                last = last.replace(tzinfo=timezone.utc)
            if (now - last) < timedelta(seconds=wait):
                suppressed = True
    except Exception:
        logger.exception("tarang alert dedupe read failed")

    from backend.services.tarang.events import create_alert

    aid = create_alert(
        db,
        message,
        level="critical" if kind_n in CRITICAL_KINDS else "warn",
        trade_id=trade_id,
        meta={"kind": kind_n, "dedupe_key": key, "telegram_suppressed": suppressed, **(meta or {})},
    )
    sent = False
    if not suppressed:
        sent = _send_telegram(f"Kosmic Tarang [{kind_n}]\n{message}")
        try:
            db.execute(
                text(
                    """
                    INSERT INTO tarang_alert_dedupe (dedupe_key, last_sent_at, last_message)
                    VALUES (:k, :ts, :msg)
                    ON CONFLICT (dedupe_key) DO UPDATE
                    SET last_sent_at = EXCLUDED.last_sent_at, last_message = EXCLUDED.last_message
                    """
                ),
                {"k": key, "ts": now, "msg": message[:500]},
            )
        except Exception:
            logger.exception("tarang alert dedupe write failed")
    return {"alert_id": aid, "sent": sent, "suppressed": suppressed, "kind": kind_n}

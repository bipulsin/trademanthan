"""Rewrite Tarang Telegram routing: ops private-only; signals gated public."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import text

from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

DEFAULT_THROTTLE_SEC = 900
EXIT_THROTTLE_SEC = 300
BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME") or "Tradewithcto"

OPS_KINDS = {
    "test_alert",
    "exit_trigger",
    "hard_exit_warning",
    "stale_feed",
    "upstox_token_expired",
    "kill_switch",
    "data_gap",
    "heartbeat",
    "recon_failure",
    "job_failed",
    "archive_fetch_failed",
    "mcx_expiry_download_reminder",
}
CRITICAL_KINDS = set(OPS_KINDS)


def _bot_token() -> Optional[str]:
    return (os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_NOTIFY_BOT_TOKEN") or "").strip() or None


def _setting(db, key: str, default=None):
    try:
        row = db.execute(text("SELECT value FROM tarang_settings WHERE key = :k"), {"k": key}).mappings().first()
        if not row:
            return default
        v = row["value"]
        if isinstance(v, str):
            try:
                v = json.loads(v)
            except Exception:
                return v
        return v
    except Exception:
        return default


def private_chat_id(db=None) -> Optional[str]:
    env = (os.getenv("TARANG_TELEGRAM_CHAT_ID") or os.getenv("TELEGRAM_TARANG_CHAT_ID") or "").strip()
    if env:
        return env
    if db is None:
        from backend.database import SessionLocal

        s = SessionLocal()
        try:
            v = _setting(s, "telegram_private_chat_id")
        finally:
            s.close()
    else:
        v = _setting(db, "telegram_private_chat_id")
    if v is None:
        return None
    if isinstance(v, dict):
        v = v.get("id") or v.get("chat_id")
    if v is None:
        return None
    s = str(v).strip().strip('"')
    if not s or s.lower() in ("none", "null"):
        return None
    return s


def public_signals_enabled(db=None) -> bool:
    if db is None:
        from backend.database import SessionLocal

        s = SessionLocal()
        try:
            v = _setting(s, "telegram_public_signals", False)
        finally:
            s.close()
    else:
        v = _setting(db, "telegram_public_signals", False)
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes")
    return bool(v)


def public_chat_id() -> str:
    return (os.getenv("TELEGRAM_TRADEWITHCTO_CHAT_ID") or "@Tradewithcto").strip()


def start_link() -> str:
    return f"https://t.me/{BOT_USERNAME.lstrip('@')}?start=tarang"


def save_private_chat_id(db, chat_id: str, *, username: Optional[str] = None) -> None:
    payload = {"id": str(chat_id), "username": username}
    db.execute(
        text(
            """
            INSERT INTO tarang_settings (key, value) VALUES ('telegram_private_chat_id', CAST(:v AS jsonb))
            ON CONFLICT (key) DO UPDATE SET value = CAST(:v AS jsonb), updated_at = NOW()
            """
        ),
        {"v": json.dumps(payload)},
    )


def _send(chat_id: str, text_msg: str, thread_id: Optional[str] = None) -> bool:
    token = _bot_token()
    if not token or not chat_id:
        logger.info("tarang telegram skipped (no bot token or chat_id)")
        return False
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text_msg[:3900],
        "disable_web_page_preview": True,
    }
    if thread_id:
        try:
            payload["message_thread_id"] = int(thread_id)
        except ValueError:
            payload["message_thread_id"] = thread_id
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload, timeout=15)
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


def _destinations(db, kind: str) -> List[str]:
    """Ops → private only. Signals/digest → private; public only if setting on."""
    dests: List[str] = []
    priv = private_chat_id(db)
    if priv:
        dests.append(priv)
    if kind in ("forward_test_signal", "weekly_digest") and public_signals_enabled(db):
        dests.append(public_chat_id())
    return dests


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
    dests = _destinations(db, kind_n) if not suppressed else []
    if not dests and not suppressed:
        logger.info("tarang telegram no destination (link chat via Start); in-app alert stored")
    thread = (os.getenv("TARANG_TELEGRAM_THREAD_ID") or "").strip() or None
    for chat in dests:
        sent = _send(chat, f"Kosmic Tarang [{kind_n}]\n{message}", thread if chat == private_chat_id(db) else None) or sent
    if not suppressed:
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
    return {"alert_id": aid, "sent": sent, "suppressed": suppressed, "kind": kind_n, "destinations": dests}


def notify_ops(db, **kwargs) -> Dict[str, Any]:
    return notify_critical(db, **kwargs)


def notify_signal(db, message: str, *, dedupe_key: Optional[str] = None) -> Dict[str, Any]:
    return notify_critical(db, kind="forward_test_signal", message=message, dedupe_key=dedupe_key, throttle_sec=60)


def handle_telegram_update(update: Dict[str, Any]) -> Dict[str, Any]:
    msg = update.get("message") or update.get("edited_message") or {}
    text_msg = str(msg.get("text") or "")
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    if chat_id is None:
        return {"ok": False, "error": "no_chat"}
    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        ensure_tarang_tables()
        if text_msg.startswith("/start"):
            save_private_chat_id(db, str(chat_id), username=chat.get("username") or msg.get("from", {}).get("username"))
            db.commit()
            _send(str(chat_id), "Kosmic Tarang linked this chat for private ops alerts.")
            return {"ok": True, "linked": str(chat_id)}
        return {"ok": True, "ignored": True}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": str(e)[:200]}
    finally:
        db.close()


def send_test_alert() -> Dict[str, Any]:
    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        ensure_tarang_tables()
        cid = private_chat_id(db)
        if not cid:
            return {
                "ok": False,
                "sent": False,
                "linked": False,
                "error": "private_chat_not_linked",
                "how_to": "Open Link my chat, press Start, then Poll.",
            }
        out = notify_critical(
            db,
            kind="test_alert",
            message="Kosmic Tarang test alert — private chat is linked.",
            dedupe_key="telegram_test_alert",
            throttle_sec=0,
        )
        db.commit()
        return {"ok": True, "linked": True, "chat_id_suffix": str(cid)[-4:], "telegram": out}
    finally:
        db.close()


def poll_and_link() -> Dict[str, Any]:
    token = _bot_token()
    if not token:
        return {"ok": False, "error": "no_bot_token"}
    try:
        r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", params={"timeout": 0, "limit": 20}, timeout=20)
        body = r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}
    linked = []
    for upd in body.get("result") or []:
        if not isinstance(upd, dict):
            continue
        out = handle_telegram_update(upd)
        if out.get("linked"):
            linked.append(out["linked"])
    return {"ok": True, "linked": linked, "n_updates": len(body.get("result") or [])}

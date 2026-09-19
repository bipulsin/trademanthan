"""08:45 / 09:00 / 09:05 IST Upstox token check. Never alerts at overnight JWT expiry (03:30)."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
from backend.services.tarang.alerts_telegram import notify_ops, start_link
from backend.services.tarang.calendar import to_ist
from backend.services.tarang.data_gaps import record_data_gap
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

GREEK_URL = "https://api.upstox.com/v3/market-quote/option-greek"
IST = ZoneInfo("Asia/Kolkata")


def _decode_jwt_claims(token: str) -> Dict[str, Any]:
    import base64
    import json as json_lib

    parts = token.split(".")
    if len(parts) < 2:
        return {}
    pad = "=" * ((4 - len(parts[1]) % 4) % 4)
    try:
        return json_lib.loads(base64.urlsafe_b64decode(parts[1] + pad))
    except Exception:
        return {}


def should_alert_token(now: Optional[datetime] = None) -> bool:
    """Suppress overnight JWT-expiry noise. Alerts only in the 08:45–09:10 IST window (or explicit escalate jobs)."""
    ist = to_ist(now)
    mins = ist.hour * 60 + ist.minute
    return (8 * 60 + 40) <= mins <= (9 * 60 + 15)


def check_upstox_token(*, now: Optional[datetime] = None, escalate: str = "0845") -> Dict[str, Any]:
    ensure_tarang_tables()
    adapter = UpstoxMcxAdapter()
    h = adapter.health()
    expired = bool(h.get("token") in ("missing", "expired") or h.get("token_expired"))
    out: Dict[str, Any] = {
        "checked_at": (now or datetime.now(timezone.utc)).isoformat(),
        "trading_token": h,
        "expired": expired,
        "escalate": escalate,
        "analytics": _probe_analytics_token(),
        "alert_window": should_alert_token(now),
    }
    db = SessionLocal()
    try:
        db.execute(
            text(
                """
                INSERT INTO tarang_settings (key, value) VALUES ('upstox_analytics_probe', CAST(:v AS jsonb))
                ON CONFLICT (key) DO UPDATE SET value = CAST(:v AS jsonb)
                """
            ),
            {"v": json.dumps(out["analytics"], default=str)},
        )
        if expired and should_alert_token(now):
            keys = {
                "0845": "upstox_token_expired_premarket_0845",
                "0900": "upstox_token_expired_premarket_0900",
                "0905": "upstox_token_expired_premarket_0905",
            }
            label = {"0845": "08:45", "0900": "09:00 escalate", "0905": "09:05 escalate"}.get(escalate, escalate)
            notify_ops(
                db,
                kind="upstox_token_expired",
                message=(
                    f"Kosmic Tarang: Upstox token missing/expired at {label} IST. "
                    f"Renew before 09:00 IST. Re-auth: platform Upstox connect. "
                    f"Private Telegram link: {start_link()}"
                ),
                dedupe_key=keys.get(escalate, f"upstox_token_expired_{escalate}"),
                throttle_sec=0,
            )
            record_data_gap(venue="upstox_mcx", reason="upstox_token_invalid", detail={"escalate": escalate})
        db.commit()
    finally:
        db.close()
    return out


def _probe_analytics_token() -> Dict[str, Any]:
    raw = (
        os.getenv("UPSTOX_ANALYTICS_TOKEN")
        or os.getenv("UPSTOX_READ_ONLY_TOKEN")
        or os.getenv("UPSTOX_READONLY_TOKEN")
        or ""
    ).strip()
    if not raw:
        return {
            "configured": False,
            "works_for_mcx": None,
            "note": "No UPSTOX_ANALYTICS_TOKEN / UPSTOX_READ_ONLY_TOKEN in env. Not proven.",
        }
    claims = _decode_jwt_claims(raw)
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {raw}",
        "User-Agent": "TradeManthan-KosmicTarang/token-check",
    }
    url = f"{GREEK_URL}?instrument_key={quote('MCX_FO|CRUDEOILM', safe='')}"
    try:
        r = requests.get(url, headers=headers, timeout=20)
        body = {}
        try:
            body = r.json()
        except Exception:
            body = {"raw": (r.text or "")[:240]}
        codes = []
        if isinstance(body, dict):
            for e in body.get("errors") or []:
                if isinstance(e, dict):
                    codes.append(e.get("errorCode") or e.get("code"))
        works = r.status_code == 200 and (body.get("status") == "success" if isinstance(body, dict) else False)
        return {
            "configured": True,
            "http_status": r.status_code,
            "error_codes": codes,
            "works_for_mcx": works,
            "is_plus": claims.get("isPlusPlan"),
            "jwt_exp": claims.get("exp"),
            "body_excerpt": str(body)[:400],
        }
    except requests.RequestException as e:
        return {"configured": True, "works_for_mcx": False, "error": str(e)[:200]}

"""08:45 IST Upstox token check + optional read-only analytics token probe."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import quote

import requests

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.adapters.upstox_mcx import UpstoxMcxAdapter
from backend.services.tarang.alerts_telegram import notify_critical
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

GREEK_URL = "https://api.upstox.com/v3/market-quote/option-greek"


def _decode_jwt_claims(token: str) -> Dict[str, Any]:
    import base64
    import json

    parts = token.split(".")
    if len(parts) < 2:
        return {}
    pad = "=" * ((4 - len(parts[1]) % 4) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(parts[1] + pad))
    except Exception:
        return {}


def check_upstox_token(*, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Premarket health: trading token + optional analytics/read-only token for MCX."""
    ensure_tarang_tables()
    adapter = UpstoxMcxAdapter()
    h = adapter.health()
    expired = bool(h.get("token") in ("missing", "expired") or h.get("token_expired"))
    out: Dict[str, Any] = {
        "checked_at": (now or datetime.now(timezone.utc)).isoformat(),
        "trading_token": h,
        "expired": expired,
        "analytics": _probe_analytics_token(),
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
        if expired:
            notify_critical(
                db,
                kind="upstox_token_expired",
                message="Kosmic Tarang: Upstox token missing/expired at 08:45 IST pre-market check. MCX snapshots will gap.",
                dedupe_key="upstox_token_expired_premarket",
            )
        db.commit()
    finally:
        db.close()
    return out


def _probe_analytics_token() -> Dict[str, Any]:
    """
    Report only what an env token + a test call prove.

    Looks for UPSTOX_ANALYTICS_TOKEN / UPSTOX_READ_ONLY_TOKEN. If absent, cannot
    claim the analytics token works. If present, hits Option Greek on an MCX key.
    """
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
    # Public master-derived crude mini key is unknown here; use a symbol-style probe
    # that Option Greek rejects cleanly if unauthorized.
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

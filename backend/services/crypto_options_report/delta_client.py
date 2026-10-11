"""Read-only Delta Exchange India client for BTC option straddle fills and marks.

Credentials come from the environment already used by Tarang:
``DELTA_INDIA_API_KEY`` / ``DELTA_INDIA_API_SECRET`` / ``DELTA_INDIA_API_URL``.
This module never logs or returns the key or secret.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)

DEFAULT_BASE = "https://api.india.delta.exchange"
_REQUIRED_ENV = ("DELTA_INDIA_API_KEY", "DELTA_INDIA_API_SECRET")


def missing_credential_env() -> List[str]:
    """Env var names that are absent. Values are never returned."""
    return [name for name in _REQUIRED_ENV if not (os.getenv(name) or "").strip()]


def _host() -> str:
    base = (os.getenv("DELTA_INDIA_API_URL") or DEFAULT_BASE).strip().rstrip("/")
    if base.endswith("/v2"):
        base = base[:-3]
    return base


def _auth_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    missing = missing_credential_env()
    if missing:
        return {"ok": False, "missing_env": missing, "result": [], "error": "missing_credentials"}
    key = os.environ["DELTA_INDIA_API_KEY"].strip()
    secret = os.environ["DELTA_INDIA_API_SECRET"].strip()
    query = ""
    if params:
        query = "?" + urlencode(params)
    sign_path = path if path.startswith("/v2") else f"/v2{path}"
    ts = str(int(time.time()))
    message = "GET" + ts + sign_path + query
    signature = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    headers = {
        "api-key": key,
        "timestamp": ts,
        "signature": signature,
        "Accept": "application/json",
        "User-Agent": "TradeManthan-CryptoOptionsReport/1.0",
    }
    url = f"{_host()}{sign_path}{query}"
    try:
        response = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        logger.warning("delta india fills request failed: %s", exc.__class__.__name__)
        return {"ok": False, "result": [], "error": "request_failed", "missing_env": []}
    try:
        body = response.json()
    except ValueError:
        body = {}
    if response.status_code != 200 or not isinstance(body, dict) or body.get("success") is not True:
        err = body.get("error") if isinstance(body, dict) else None
        code = err.get("code") if isinstance(err, dict) else err
        logger.warning("delta india fills http %s code %s", response.status_code, code)
        return {
            "ok": False,
            "result": [],
            "error": str(code or "http_error"),
            "http_status": response.status_code,
            "missing_env": [],
        }
    result = body.get("result")
    meta = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    return {
        "ok": True,
        "result": result if isinstance(result, list) else [],
        "after": meta.get("after"),
        "missing_env": [],
    }


def _public_get(path: str) -> Dict[str, Any]:
    url = f"{_host()}{path if path.startswith('/v2') else '/v2' + path}"
    try:
        response = requests.get(url, timeout=20, headers={"Accept": "application/json", "User-Agent": "TradeManthan-CryptoOptionsReport/1.0"})
        body = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("delta india ticker failed for %s: %s", path, exc.__class__.__name__)
        return {}
    if not isinstance(body, dict) or body.get("success") is not True:
        return {}
    result = body.get("result")
    return result if isinstance(result, dict) else {}


def fetch_btc_option_fills(*, lookback_days: int = 40, page_size: int = 100, max_pages: int = 10) -> Dict[str, Any]:
    """Executed option fills. Caller filters to BTC straddles; the query is options-only."""
    missing = missing_credential_env()
    if missing:
        return {"ok": False, "missing_env": missing, "fills": [], "truncated": False}
    start = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))
    start_us = int(start.timestamp() * 1_000_000)
    fills: List[Dict[str, Any]] = []
    after = None
    truncated = False
    for _page in range(max_pages):
        params: Dict[str, Any] = {
            "contract_types": "call_options,put_options",
            "page_size": page_size,
            "start_time": start_us,
        }
        if after:
            params["after"] = after
        page = _auth_get("/v2/fills", params)
        if not page.get("ok"):
            return {
                "ok": False,
                "missing_env": page.get("missing_env") or [],
                "fills": [],
                "truncated": False,
                "error": page.get("error"),
            }
        batch = page.get("result") or []
        fills.extend(batch)
        after = page.get("after")
        if not after or not batch:
            break
    else:
        truncated = True
    return {"ok": True, "missing_env": [], "fills": fills, "truncated": truncated, "window_start": start}


def fetch_option_marks(symbols: List[str]) -> Dict[str, float]:
    """Mark price for the specific open short symbols only."""
    marks: Dict[str, float] = {}
    for symbol in symbols:
        sym = (symbol or "").strip()
        if not sym or sym in marks:
            continue
        ticker = _public_get(f"/v2/tickers/{sym}")
        raw = ticker.get("mark_price")
        if raw is None:
            raw = ticker.get("close")
        try:
            if raw is not None:
                marks[sym] = float(raw)
        except (TypeError, ValueError):
            continue
    return marks

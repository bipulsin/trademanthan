"""Thin DhanHQ client: valid token, timeouts, backoff, and one auth retry."""
from __future__ import annotations

import logging
import time
from datetime import date, datetime
from typing import Any, Callable, List, Optional

import requests

from backend.config import settings
from backend.dhan.auth import DhanAuthError, force_refresh, get_valid_token, redact_secrets

logger = logging.getLogger(__name__)

_HTTP_TIMEOUT = 20
_MAX_PAGES = 40
_BACKOFF_START = 0.5
_BACKOFF_ATTEMPTS = 4


class DhanApiError(Exception):
    """Dhan returned a non-auth failure."""


class DhanClient:
    """dhanhq wrapper. Constructed with get_valid_token() unless a token is passed."""

    def __init__(self, access_token: Optional[str] = None):
        supplied = str(access_token or "").strip()
        self._token = supplied or get_valid_token()

    def get_trade_book(self) -> List[dict]:
        """Trades executed today."""
        payload = self._sdk_call("get_trade_book")
        rows = _rows_from_payload(payload)
        logger.info("Dhan trade book rows=%s", len(rows))
        return rows

    def get_trade_history(self, from_date: Any, to_date: Any) -> List[dict]:
        """Pages from 0 until an empty page."""
        start = _as_date_str(from_date)
        end = _as_date_str(to_date)
        rows: List[dict] = []
        for page in range(0, _MAX_PAGES):
            try:
                payload = self._sdk_call(
                    "get_trade_history",
                    start,
                    end,
                    page_number=page,
                )
                batch = _rows_from_payload(payload)
            except DhanApiError:
                if page == 0:
                    raise
                logger.info("Dhan trade history stopped at page=%s", page)
                break
            logger.info("Dhan trade history page=%s rows=%s", page, len(batch))
            if not batch:
                break
            rows.extend(batch)
        else:
            logger.warning("Dhan trade history stopped at page cap=%s", _MAX_PAGES)
        return rows

    def get_ledger(self, from_date: Any, to_date: Any) -> Any:
        """GET /v2/ledger. dhanhq's statement helper is not required."""
        start = _as_date_str(from_date)
        end = _as_date_str(to_date)
        auth_used = False
        while True:
            try:
                body = self._ledger_once(start, end)
                count = len(body) if isinstance(body, list) else "ok"
                logger.info("Dhan ledger from=%s to=%s result=%s", start, end, count)
                return body
            except DhanAuthError:
                if auth_used:
                    raise
                auth_used = True
                logger.info("Dhan ledger auth failure; refreshing token once")
                self._rotate_token()

    def _sdk_call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        auth_used = False
        while True:
            try:
                sdk = self._build_sdk()
                method = getattr(sdk, method_name)
                payload = _invoke_backoff(method, *args, **kwargs)
            except DhanAuthError:
                if auth_used:
                    raise
                auth_used = True
                logger.info("Dhan %s auth failure; refreshing token once", method_name)
                self._rotate_token()
                continue
            if _is_auth_payload(payload):
                if auth_used:
                    raise DhanAuthError("Dhan access token was rejected.")
                auth_used = True
                logger.info("Dhan %s auth failure; refreshing token once", method_name)
                self._rotate_token()
                continue
            return payload

    def _build_sdk(self) -> Any:
        try:
            from dhanhq import DhanContext, dhanhq
        except ImportError as exc:
            raise DhanApiError("Dhan client library is not installed (dhanhq).") from exc
        client_id = str(settings.DHAN_CLIENT_ID or "").strip()
        token = str(self._token or "").strip()
        if not client_id:
            raise DhanAuthError("DHAN_CLIENT_ID is not configured.")
        if not token:
            raise DhanAuthError("Dhan access token is missing.")
        ctx = DhanContext(client_id, token)
        sdk = dhanhq(ctx)
        http = getattr(sdk, "dhan_http", None)
        if http is not None:
            try:
                http.timeout = _HTTP_TIMEOUT
            except Exception:
                pass
            header = getattr(http, "header", None)
            if isinstance(header, dict):
                header["access-token"] = token
                header["client-id"] = client_id
                header["dhanClientId"] = client_id
        return sdk

    def _rotate_token(self) -> str:
        token = force_refresh()
        self._token = token
        return token

    def _ledger_once(self, from_date: str, to_date: str) -> Any:
        client_id = str(settings.DHAN_CLIENT_ID or "").strip()
        token = str(self._token or "").strip()
        if not client_id:
            raise DhanAuthError("DHAN_CLIENT_ID is not configured.")
        if not token:
            raise DhanAuthError("Dhan access token is missing.")
        headers = {
            "Accept": "application/json",
            "access-token": token,
            "dhanClientId": client_id,
        }
        params = {"from-date": from_date, "to-date": to_date}
        delay = _BACKOFF_START
        last_exc: Optional[BaseException] = None
        for attempt in range(_BACKOFF_ATTEMPTS):
            try:
                response = requests.get(
                    "https://api.dhan.co/v2/ledger",
                    headers=headers,
                    params=params,
                    timeout=_HTTP_TIMEOUT,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt + 1 < _BACKOFF_ATTEMPTS:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise DhanApiError(redact_secrets(exc) or "Could not reach Dhan.") from None
            if response.status_code in (401, 403):
                raise DhanAuthError("Dhan access token was rejected.")
            if response.status_code == 429 or response.status_code >= 500:
                if attempt + 1 < _BACKOFF_ATTEMPTS:
                    logger.info(
                        "Dhan ledger HTTP %s; backoff %.1fs",
                        response.status_code,
                        delay,
                    )
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise DhanApiError(f"Dhan ledger request failed ({response.status_code}).")
            try:
                body = response.json()
            except ValueError as exc:
                raise DhanApiError("Dhan returned an unreadable ledger response.") from exc
            if isinstance(body, dict) and _is_auth_payload(body):
                raise DhanAuthError("Dhan access token was rejected.")
            if response.status_code >= 400:
                message = _error_blob(body) if isinstance(body, dict) else ""
                raise DhanApiError(redact_secrets(message) or "Dhan ledger request failed.")
            return body
        raise DhanApiError(redact_secrets(last_exc) or "Dhan ledger request failed.")


def _as_date_str(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text_raw = str(value or "").strip()
    if not text_raw:
        raise DhanApiError("Dhan date range is missing.")
    return text_raw[:10]


def _invoke_backoff(method: Callable, *args: Any, **kwargs: Any) -> Any:
    delay = _BACKOFF_START
    last_exc: Optional[BaseException] = None
    for attempt in range(_BACKOFF_ATTEMPTS):
        try:
            payload = method(*args, **kwargs)
        except DhanAuthError:
            raise
        except Exception as exc:
            if _is_auth_exception(exc):
                raise DhanAuthError(redact_secrets(exc) or "Dhan access token was rejected.") from None
            last_exc = exc
            if attempt + 1 < _BACKOFF_ATTEMPTS and _is_retryable_exception(exc):
                time.sleep(delay)
                delay *= 2
                continue
            raise DhanApiError(redact_secrets(exc) or "Dhan request failed.") from None
        if _is_retryable_payload(payload) and attempt + 1 < _BACKOFF_ATTEMPTS:
            logger.info(
                "Dhan %s retryable response; backoff %.1fs",
                getattr(method, "__name__", "call"),
                delay,
            )
            time.sleep(delay)
            delay *= 2
            continue
        return payload
    raise DhanApiError(redact_secrets(last_exc) or "Dhan request failed.")


def _rows_from_payload(payload: Any) -> List[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        raise DhanApiError("Dhan returned an unreadable response.")
    if _is_auth_payload(payload):
        raise DhanAuthError("Dhan access token was rejected.")
    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ("trades", "tradebook", "data"):
            nested = data.get(key)
            if isinstance(nested, list):
                return [row for row in nested if isinstance(row, dict)]
        return []
    blob = _error_blob(payload).lower()
    status = str(payload.get("status") or "").lower()
    if data in ("", None) and status in ("", "success"):
        return []
    if any(part in blob for part in ("no trade", "no data", "no record", "not found", "empty")):
        return []
    if status == "failure":
        raise DhanApiError(redact_secrets(_error_blob(payload)) or "Dhan request failed.")
    return []


def _error_blob(payload: Any) -> str:
    if not isinstance(payload, dict):
        return str(payload or "")
    parts = []
    remarks = payload.get("remarks")
    if isinstance(remarks, dict):
        parts.extend(str(value) for value in remarks.values() if value)
    elif remarks:
        parts.append(str(remarks))
    for key in ("errorMessage", "errorCode", "errorType", "message"):
        if payload.get(key):
            parts.append(str(payload.get(key)))
    return " ".join(parts)


def _is_auth_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    code = payload.get("status_code") or payload.get("http_status")
    if code in (401, 403):
        return True
    blob = _error_blob(payload).lower()
    if not blob:
        return False
    hints = (
        "invalid token",
        "invalid access",
        "access token",
        "unauthor",
        "expired token",
        "token expired",
        "dh-901",
        "dh-902",
        "authenticate",
    )
    if any(hint in blob for hint in hints):
        return True
    status = str(payload.get("status") or "").lower()
    return status in ("failure", "error") and "token" in blob


def _is_retryable_payload(payload: Any) -> bool:
    if not isinstance(payload, dict) or _is_auth_payload(payload):
        return False
    blob = _error_blob(payload).lower()
    hints = (
        "429",
        "rate",
        "too many",
        "timeout",
        "timed out",
        "502",
        "503",
        "504",
        "500",
        "internal server",
        "connection",
        "temporarily",
        "unavailable",
    )
    return any(hint in blob for hint in hints)


def _is_auth_exception(exc: BaseException) -> bool:
    blob = redact_secrets(exc).lower()
    return any(part in blob for part in ("401", "403", "invalid token", "unauthor", "access token"))


def _is_retryable_exception(exc: BaseException) -> bool:
    if _is_auth_exception(exc):
        return False
    blob = redact_secrets(exc).lower()
    return any(
        part in blob
        for part in ("429", "500", "502", "503", "504", "timeout", "timed out", "connection", "unavailable")
    )

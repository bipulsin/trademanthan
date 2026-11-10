"""Encrypted Dhan access tokens: TOTP generate, renew, and reuse across workers.

Secrets (PIN, TOTP, encryption key, access token) are never written to logs.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Tuple

import pytz
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import text

from backend.config import settings
from backend.database import engine

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

BROKER = "dhan"
MIN_REMAINING = timedelta(hours=2)
# Session advisory lock so only one worker refreshes at a time.
_LOCK_KEY = 845221941
_thread_lock = threading.Lock()
_table_ready = False
_shape_logged = False

_MIGRATION = Path(__file__).resolve().parents[1] / "migrations" / "add_broker_tokens.sql"

_TOKEN_KEYS = ("accessToken", "access_token", "access-token", "token")
_EXPIRY_KEYS = (
    "expiryTime",
    "expiry_time",
    "expiry",
    "expires_at",
    "expiresAt",
    "tokenExpiry",
)
_SENSITIVE_KEYS = {
    "accesstoken",
    "access_token",
    "access-token",
    "token",
    "pin",
    "totp",
    "secret",
    "password",
    "app_secret",
    "api_secret",
    "token_encryption_key",
}

_JWT_RE = re.compile(r"eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}")
_QUERY_SECRET_RE = re.compile(r"(?i)\b(pin|totp|access_token|secret)=([^&\s\"']+)")


class DhanAuthError(Exception):
    """PIN/TOTP login or token renew could not produce a usable access token."""

    def __init__(self, message: str, *, login_url: Optional[str] = None):
        super().__init__(message)
        self.login_url = login_url


def redact_secrets(raw: Any) -> str:
    """Strip tokens, PIN, and TOTP from a message before it is logged."""
    text_raw = _JWT_RE.sub("[redacted]", str(raw or ""))
    text_raw = _QUERY_SECRET_RE.sub(lambda m: f"{m.group(1)}=[redacted]", text_raw)
    return text_raw[:300]


def ensure_broker_tokens_table() -> None:
    """Create broker_tokens if it is missing. Safe to call on every worker."""
    global _table_ready
    if _table_ready:
        return
    if engine is None:
        raise DhanAuthError("Database is not available to store the Dhan token.")
    if not _MIGRATION.is_file():
        raise FileNotFoundError(f"migration missing: {_MIGRATION}")
    sql = _MIGRATION.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
    _table_ready = True
    logger.info("broker_tokens table ensured")


def parse_token_response(response: Any) -> Tuple[str, datetime]:
    """Pull an access token and expiry from a dict or object without assuming one key."""
    if isinstance(response, str) and response.strip() and not response.strip().startswith("{"):
        return response.strip(), _default_expiry()
    mapping = _find_token_mapping(response)
    token = _extract_token(mapping)
    if not token:
        raise DhanAuthError("Dhan did not return an access token.")
    return token, _parse_expiry(_extract_expiry(mapping))


def generate_token() -> Tuple[str, datetime]:
    """PIN + current TOTP code. Retries once if the 30-second window rolled."""
    client_id, pin, secret = _require_totp_credentials()
    try:
        import pyotp
        from dhanhq import DhanLogin
    except ImportError as exc:
        raise DhanAuthError("Dhan login libraries are not installed (dhanhq, pyotp).") from exc

    login = DhanLogin(client_id)
    response = _generate_with_totp_retry(login, pin, secret, pyotp)
    _log_generate_shape_once(response)
    token, expiry = parse_token_response(response)
    _persist(token, expiry)
    logger.info("Dhan token generated; expires %s", expiry.isoformat())
    return token, expiry


def renew_token(access_token: Optional[str] = None) -> Tuple[str, datetime]:
    """Renew the active token. Dhan expires the current token and returns a new 24h token."""
    client_id = str(settings.DHAN_CLIENT_ID or "").strip()
    if not client_id:
        raise DhanAuthError("DHAN_CLIENT_ID is not configured.")
    current = str(access_token or "").strip()
    if not current:
        stored = _read_stored()
        current = stored[0] if stored else ""
    if not current:
        raise DhanAuthError("No Dhan access token is available to renew.")
    try:
        from dhanhq import DhanLogin
    except ImportError as exc:
        raise DhanAuthError("Dhan login library is not installed (dhanhq).") from exc

    login = DhanLogin(client_id)
    try:
        response = login.renew_token(current)
    except Exception as exc:
        raise DhanAuthError(redact_secrets(exc) or "Dhan token renew failed.") from None
    token, expiry = parse_token_response(response)
    _persist(token, expiry)
    logger.info("Dhan token renewed; expires %s", expiry.isoformat())
    return token, expiry


def get_valid_token() -> str:
    """Stored token when more than 2 hours remain; otherwise renew, then TOTP."""
    with _refresh_lock():
        return _get_valid_token_locked()


def force_refresh() -> str:
    """Renew or regenerate even if the stored token still has time left."""
    with _refresh_lock():
        return _force_refresh_locked()


def proactive_refresh() -> datetime:
    """Weekday morning refresh. Returns the new expiry. Does not raise on a short Dhan outage when called from the scheduler wrapper."""
    with _refresh_lock():
        stored = _read_stored()
        if stored:
            try:
                _token, expiry = renew_token(stored[0])
                return expiry
            except Exception as exc:
                logger.warning("Dhan proactive renew failed: %s", redact_secrets(exc))
        _token, expiry = generate_token()
        return expiry


def _get_valid_token_locked() -> str:
    stored = _read_stored()
    if stored and _remaining(stored[1]) > MIN_REMAINING:
        return stored[0]
    if stored:
        try:
            token, _expiry = renew_token(stored[0])
            return token
        except Exception as exc:
            logger.warning("Dhan renew failed; generating a new token (%s)", redact_secrets(exc))
    token, _expiry = generate_token()
    return token


def _force_refresh_locked() -> str:
    stored = _read_stored()
    if stored:
        try:
            token, _expiry = renew_token(stored[0])
            return token
        except Exception as exc:
            logger.warning("Dhan renew failed; generating a new token (%s)", redact_secrets(exc))
    token, _expiry = generate_token()
    return token


def _require_totp_credentials() -> Tuple[str, str, str]:
    client_id = str(settings.DHAN_CLIENT_ID or "").strip()
    pin = str(settings.DHAN_PIN or "").strip()
    secret = str(settings.DHAN_TOTP_SECRET or "").strip()
    missing = []
    if not client_id:
        missing.append("DHAN_CLIENT_ID")
    if not pin:
        missing.append("DHAN_PIN")
    if not secret:
        missing.append("DHAN_TOTP_SECRET")
    if missing:
        raise DhanAuthError(
            "Dhan TOTP login is not configured. Set " + ", ".join(missing) + " in the environment."
        )
    return client_id, pin, secret


def _generate_with_totp_retry(login: Any, pin: str, secret: str, pyotp: Any) -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in (1, 2):
        try:
            code = pyotp.TOTP(secret).now()
        except Exception:
            raise DhanAuthError("DHAN_TOTP_SECRET is not a valid TOTP secret.") from None
        try:
            return login.generate_token(pin, code)
        except Exception as exc:
            last_exc = exc
            if attempt == 1 and _looks_like_rolling_totp(exc):
                logger.info("Dhan TOTP generate missed the 30s window; retrying once")
                time.sleep(5)
                continue
            break
    raise DhanAuthError(redact_secrets(last_exc) or "Dhan TOTP token generation failed.") from None


def _looks_like_rolling_totp(exc: BaseException) -> bool:
    text_raw = redact_secrets(exc).lower()
    if any(part in text_raw for part in ("timed out", "timeout", "connection", "name or service")):
        return False
    return any(part in text_raw for part in ("totp", "otp", "invalid pin", "pin"))


def _log_generate_shape_once(response: Any) -> None:
    """Inspect the generate response once. Access token values are redacted."""
    global _shape_logged
    if _shape_logged:
        return
    _shape_logged = True
    mapping = _as_mapping(response)
    nested = mapping.get("data")
    nested_keys = sorted(str(k) for k in _as_mapping(nested).keys()) if nested is not None else []
    keys = sorted(str(k) for k in mapping.keys())
    expiry = _extract_expiry(mapping) or (_extract_expiry(_as_mapping(nested)) if nested is not None else None)
    token_present = bool(_extract_token(mapping) or _extract_token(_as_mapping(nested)))
    expiry_text = "" if expiry is None else redact_secrets(expiry)
    logger.info(
        "Dhan generate_token response shape keys=%s data_keys=%s expiry=%s token_present=%s",
        keys,
        nested_keys,
        expiry_text,
        token_present,
    )
    logger.debug("Dhan generate_token redacted shape=%s", _redacted_shape(response))


def _redacted_shape(obj: Any, depth: int = 0) -> Any:
    if depth > 4:
        return "..."
    if isinstance(obj, dict):
        out = {}
        for key, value in obj.items():
            if str(key).lower().replace("-", "_") in _SENSITIVE_KEYS or str(key).lower() in _SENSITIVE_KEYS:
                out[str(key)] = f"<redacted len={len(str(value))}>"
            else:
                out[str(key)] = _redacted_shape(value, depth + 1)
        return out
    if isinstance(obj, list):
        return [_redacted_shape(item, depth + 1) for item in obj[:3]]
    if isinstance(obj, str):
        if obj.startswith("eyJ") or len(obj) > 80:
            return f"<redacted len={len(obj)}>"
        return obj
    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj
    return _redacted_shape(_as_mapping(obj), depth + 1)


def _find_token_mapping(response: Any) -> dict:
    mapping = _as_mapping(response)
    if _extract_token(mapping):
        return mapping
    nested = mapping.get("data")
    nested_map = _as_mapping(nested)
    if _extract_token(nested_map):
        return nested_map
    if isinstance(response, dict):
        for value in response.values():
            found = _as_mapping(value)
            if _extract_token(found):
                return found
    return mapping or nested_map


def _as_mapping(obj: Any) -> dict:
    if isinstance(obj, dict):
        return obj
    if obj is None or isinstance(obj, (str, int, float, bool, list)):
        return {}
    data = {}
    for key in _TOKEN_KEYS + _EXPIRY_KEYS + ("data",):
        if hasattr(obj, key):
            data[key] = getattr(obj, key)
    raw = getattr(obj, "__dict__", None)
    if isinstance(raw, dict):
        for key, value in raw.items():
            if not str(key).startswith("_"):
                data.setdefault(key, value)
    return data


def _extract_token(mapping: dict) -> str:
    if not isinstance(mapping, dict):
        return ""
    for key in _TOKEN_KEYS:
        value = mapping.get(key)
        text_raw = str(value or "").strip()
        if text_raw and text_raw.lower() not in ("none", "null"):
            return text_raw
    return ""


def _extract_expiry(mapping: dict) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in _EXPIRY_KEYS:
        if mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return None


def _default_expiry() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=24)


def _parse_expiry(raw: Any) -> datetime:
    if raw is None or raw == "":
        return _default_expiry()
    if isinstance(raw, datetime):
        dt = raw
    elif isinstance(raw, (int, float)):
        val = float(raw)
        if val > 10_000_000_000:
            val /= 1000.0
        return datetime.fromtimestamp(val, tz=timezone.utc)
    else:
        text_raw = str(raw).strip()
        if text_raw.endswith("Z"):
            text_raw = text_raw[:-1] + "+00:00"
        dt = None
        try:
            dt = datetime.fromisoformat(text_raw)
        except ValueError:
            head = text_raw.split(".")[0].replace("T", " ")
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S"):
                try:
                    dt = datetime.strptime(head[:19], fmt)
                    break
                except ValueError:
                    continue
        if dt is None:
            return _default_expiry()
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    return dt.astimezone(timezone.utc)


def _remaining(expires_at: datetime) -> timedelta:
    exp = expires_at
    if exp.tzinfo is None:
        exp = IST.localize(exp)
    return exp.astimezone(timezone.utc) - datetime.now(timezone.utc)


def _fernet() -> Fernet:
    raw = str(settings.TOKEN_ENCRYPTION_KEY or "").strip().strip('"').strip("'")
    if not raw:
        raise DhanAuthError("TOKEN_ENCRYPTION_KEY is not configured.")
    try:
        return Fernet(raw.encode("utf-8"))
    except Exception:
        raise DhanAuthError("TOKEN_ENCRYPTION_KEY is not a valid Fernet key.") from None


def _read_stored() -> Optional[Tuple[str, datetime]]:
    ensure_broker_tokens_table()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT token_encrypted, expires_at
                FROM broker_tokens
                WHERE broker = :broker
                """
            ),
            {"broker": BROKER},
        ).mappings().first()
    if not row:
        return None
    blob = str(row.get("token_encrypted") or "")
    expires_at = row.get("expires_at")
    if not blob or expires_at is None:
        return None
    if not isinstance(expires_at, datetime):
        expires_at = _parse_expiry(expires_at)
    try:
        token = _fernet().decrypt(blob.encode("utf-8")).decode("utf-8").strip()
    except InvalidToken:
        logger.warning("Stored Dhan token could not be decrypted; a new token will be generated")
        return None
    except DhanAuthError:
        raise
    except Exception:
        logger.warning("Stored Dhan token could not be decrypted; a new token will be generated")
        return None
    if not token:
        return None
    return token, expires_at


def _persist(token: str, expires_at: datetime) -> None:
    cleaned = str(token or "").strip()
    if not cleaned:
        raise DhanAuthError("Dhan did not return an access token.")
    encrypted = _fernet().encrypt(cleaned.encode("utf-8")).decode("utf-8")
    exp = expires_at if expires_at.tzinfo else IST.localize(expires_at)
    ensure_broker_tokens_table()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO broker_tokens (broker, token_encrypted, created_at, expires_at)
                VALUES (:broker, :token_encrypted, NOW(), :expires_at)
                ON CONFLICT (broker) DO UPDATE
                SET token_encrypted = EXCLUDED.token_encrypted,
                    created_at = NOW(),
                    expires_at = EXCLUDED.expires_at
                """
            ),
            {"broker": BROKER, "token_encrypted": encrypted, "expires_at": exp},
        )


@contextmanager
def _refresh_lock() -> Iterator[None]:
    """Postgres advisory lock, with a thread lock when Postgres is not the engine."""
    url = str(getattr(engine, "url", "") or "") if engine is not None else ""
    if engine is None or "postgres" not in url.lower():
        with _thread_lock:
            yield
        return
    conn = engine.connect()
    locked = False
    try:
        conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _LOCK_KEY})
        conn.commit()
        locked = True
        yield
    finally:
        if locked:
            try:
                conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _LOCK_KEY})
                conn.commit()
            except Exception:
                logger.warning("Dhan token lock release failed")
        conn.close()

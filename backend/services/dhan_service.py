"""
Dhan API Service for fetching market data and, later, orders.
Credentials are the app-wide DhanHQ app (settings.DHAN_*), shared by every algo.
"""
import base64
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from backend.config import settings

logger = logging.getLogger(__name__)


class DhanAuthError(Exception):
    """Missing or rejected Dhan access token. Optional consent login URL."""

    def __init__(self, message: str, *, login_url: Optional[str] = None):
        super().__init__(message)
        self.login_url = login_url


class DhanApiError(Exception):
    """Dhan trade/order read failed."""


def _dhan_token_path() -> Path:
    if Path("/home/ubuntu").is_dir():
        return Path("/home/ubuntu/trademanthan/data/dhan_token.json")
    return Path(__file__).resolve().parents[2] / "data" / "dhan_token.json"


def _parse_expiry(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text or text.upper() == "NA":
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(text[:19], fmt)
        except ValueError:
            continue
    return None


def _jwt_expiry(token: str) -> Optional[datetime]:
    try:
        parts = str(token or "").split(".")
        if len(parts) < 2:
            return None
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        exp = data.get("exp")
        if exp is None:
            return None
        return datetime.fromtimestamp(int(exp))
    except Exception:
        return None


def _token_still_valid(token: str, expiry: Any = None) -> bool:
    text = str(token or "").strip()
    if not text:
        return False
    exp = _parse_expiry(expiry) or _jwt_expiry(text)
    if exp is None:
        return True
    return datetime.now() < exp


class DhanService:
    """Service to interact with Dhan API (Primary) and Upstox API (Fallback)"""
    
    def __init__(
        self,
        dhan_client_id: Optional[str] = None,
        dhan_access_token: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        self.app_name = settings.DHAN_APP_NAME
        self.api_key = api_key if api_key is not None else settings.DHAN_API_KEY
        self.api_secret = api_secret if api_secret is not None else settings.DHAN_API_SECRET
        self.dhan_client_id = dhan_client_id if dhan_client_id is not None else settings.DHAN_CLIENT_ID
        self.dhan_access_token = (
            dhan_access_token if dhan_access_token is not None else settings.DHAN_ACCESS_TOKEN
        )
        self.redirect_url = settings.DHAN_REDIRECT_URL
        self.postback_url = settings.DHAN_POSTBACK_URL
        self.base_url = "https://api.dhan.co"
        self._override_token: Optional[str] = None
        self._override_expiry: Any = None

    def resolved_access_token(self) -> str:
        """Token renewed this process, else env, else the consent file."""
        if _token_still_valid(self._override_token or "", self._override_expiry):
            return str(self._override_token).strip()
        env = str(settings.DHAN_ACCESS_TOKEN or "").strip()
        if _token_still_valid(env):
            return env
        file_token, file_exp = self._read_token_file()
        if _token_still_valid(file_token, file_exp):
            return file_token
        if _token_still_valid(self.dhan_access_token or ""):
            return str(self.dhan_access_token).strip()
        return ""

    def get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication"""
        client_id = str(self.dhan_client_id or settings.DHAN_CLIENT_ID or "").strip()
        return {
            "Accept": "application/json",
            "access-token": self.resolved_access_token(),
            "dhanClientId": client_id,
            "Content-Type": "application/json",
        }

    def _read_token_file(self) -> tuple:
        path = _dhan_token_path()
        try:
            if not path.is_file():
                return "", None
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return "", None
            return str(data.get("access_token") or "").strip(), data.get("expiry")
        except Exception:
            logger.warning("Dhan token file could not be read")
            return "", None

    def save_access_token(self, access_token: str, expiry: Any = None) -> None:
        token = str(access_token or "").strip()
        if not token:
            raise DhanAuthError("Dhan did not return an access token.")
        self._override_token = token
        self._override_expiry = expiry
        self.dhan_access_token = token
        path = _dhan_token_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "access_token": token,
                        "expiry": str(expiry) if expiry else None,
                        "updated_at": datetime.now().isoformat(timespec="seconds"),
                    }
                ),
                encoding="utf-8",
            )
            os.chmod(path, 0o600)
        except Exception:
            logger.warning("Dhan access token could not be saved to disk")
        logger.info("Dhan access token saved")

    def _app_headers(self) -> Dict[str, str]:
        api_key = str(self.api_key or settings.DHAN_API_KEY or "").strip()
        api_secret = str(self.api_secret or settings.DHAN_API_SECRET or "").strip()
        if not api_key or not api_secret:
            raise DhanAuthError("Dhan API key is not configured.")
        return {"Accept": "application/json", "app_id": api_key, "app_secret": api_secret}

    def start_consent_login(self) -> str:
        """API key consent step 1. Returns the browser login URL (no secrets)."""
        client_id = str(self.dhan_client_id or settings.DHAN_CLIENT_ID or "").strip()
        if not client_id:
            raise DhanAuthError("Dhan client id is not configured.")
        url = f"https://auth.dhan.co/app/generate-consent?client_id={quote(client_id)}"
        try:
            response = requests.post(url, headers=self._app_headers(), timeout=20)
        except requests.RequestException as exc:
            raise DhanApiError("Could not reach Dhan to start login.") from exc
        try:
            data = response.json()
        except ValueError:
            data = {}
        if response.status_code >= 400 or not isinstance(data, dict):
            raise DhanAuthError("Dhan login could not be started. Check the API key setup.")
        consent_id = str(data.get("consentAppId") or "").strip()
        if not consent_id:
            raise DhanAuthError("Dhan login could not be started. Check the API key setup.")
        return f"https://auth.dhan.co/login/consentApp-login?consentAppId={quote(consent_id)}"

    def consume_consent(self, token_id: str) -> None:
        """API key consent step 3. Stores the access token; does not log it."""
        token_id = str(token_id or "").strip()
        if not token_id:
            raise DhanAuthError("Dhan login did not return a token. Try again from Sync frm Broker.")
        url = f"https://auth.dhan.co/app/consumeApp-consent?tokenId={quote(token_id)}"
        try:
            response = requests.get(url, headers=self._app_headers(), timeout=20)
        except requests.RequestException as exc:
            raise DhanApiError("Could not reach Dhan to finish login.") from exc
        try:
            data = response.json()
        except ValueError:
            data = {}
        if response.status_code >= 400 or not isinstance(data, dict):
            raise DhanAuthError("Dhan login did not finish. Open Sync frm Broker and sign in again.")
        access = str(data.get("accessToken") or data.get("access_token") or "").strip()
        if not access:
            raise DhanAuthError("Dhan login did not finish. Open Sync frm Broker and sign in again.")
        self.save_access_token(access, data.get("expiryTime") or data.get("expiry"))

    def renew_access_token(self) -> bool:
        """Renew a still-active token. Expired tokens must use the consent login."""
        current = self.resolved_access_token()
        client_id = str(self.dhan_client_id or settings.DHAN_CLIENT_ID or "").strip()
        if not current or not client_id:
            return False
        try:
            response = requests.post(
                f"{self.base_url}/v2/RenewToken",
                headers={
                    "Accept": "application/json",
                    "access-token": current,
                    "dhanClientId": client_id,
                },
                timeout=20,
            )
        except requests.RequestException:
            logger.warning("Dhan token renew request failed")
            return False
        if response.status_code >= 400:
            return False
        try:
            data = response.json()
        except ValueError:
            return False
        if not isinstance(data, dict):
            return False
        access = str(data.get("accessToken") or data.get("access_token") or "").strip()
        if not access:
            return False
        self.save_access_token(access, data.get("expiryTime") or data.get("expiry"))
        return True

    def _trade_rows_from_response(self, response: requests.Response) -> List[Dict[str, Any]]:
        try:
            data = response.json()
        except ValueError as exc:
            raise DhanApiError("Dhan returned an unreadable trade response.") from exc
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            raw_message = data.get("errorMessage") or data.get("message") or ""
            if not raw_message and isinstance(data.get("remarks"), str):
                raw_message = data.get("remarks")
            message = str(raw_message or "").strip()
            low = message.lower()
            nested = data.get("data")
            if isinstance(nested, list):
                return [row for row in nested if isinstance(row, dict)]
            if response.status_code in (401, 403) or "token" in low or "unauthor" in low:
                raise DhanAuthError("Dhan access token is missing or expired. It lasts 24 hours.")
            if any(part in low for part in ("no trade", "no data", "no record", "not found", "empty")):
                return []
            if message and response.status_code >= 400:
                raise DhanApiError(message[:300] or "Dhan trade request failed.")
            return []
        return []

    def _auth_error(self) -> DhanAuthError:
        login_url = None
        try:
            login_url = self.start_consent_login()
        except Exception:
            login_url = None
        return DhanAuthError(
            "Dhan access token is missing or expired. It lasts 24 hours.",
            login_url=login_url,
        )

    def _get_trades(self, url: str, *, allow_renew: bool = True, empty_on_error: bool = False) -> List[Dict[str, Any]]:
        token = self.resolved_access_token()
        if not token:
            raise self._auth_error()
        headers = self.get_headers()
        try:
            response = requests.get(url, headers=headers, timeout=25)
        except requests.RequestException as exc:
            raise DhanApiError("Could not reach Dhan to fetch trades.") from exc
        if response.status_code in (401, 403):
            if allow_renew and self.renew_access_token():
                return self._get_trades(url, allow_renew=False, empty_on_error=empty_on_error)
            raise self._auth_error()
        if response.status_code >= 500:
            raise DhanApiError("Dhan trade request failed. Try again in a moment.")
        if response.status_code >= 400 and empty_on_error:
            return []
        return self._trade_rows_from_response(response)

    def get_trades_for_date(self, trade_date: str) -> List[Dict[str, Any]]:
        """
        Trades for one calendar day.

        History is paginated. Today's trade book is merged in so a same-day
        sync still sees fills that are not in history yet.
        """
        day = str(trade_date or "").strip()
        rows: List[Dict[str, Any]] = []
        seen = set()

        def _take(batch: List[Dict[str, Any]]) -> int:
            fresh = 0
            for row in batch:
                stamp = str(row.get("exchangeTime") or row.get("updateTime") or row.get("createTime") or "")
                key = "|".join(
                    (
                        str(row.get("exchangeTradeId") or ""),
                        str(row.get("orderId") or ""),
                        stamp,
                        str(row.get("transactionType") or ""),
                        str(row.get("tradedQuantity") or ""),
                        str(row.get("tradedPrice") or ""),
                    )
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
                fresh += 1
            return fresh

        for page in range(0, 40):
            batch = self._get_trades(
                f"{self.base_url}/v2/trades/{day}/{day}/{page}",
                empty_on_error=page > 0,
            )
            if not batch or _take(batch) == 0:
                break
        try:
            import pytz

            today = datetime.now(pytz.timezone("Asia/Kolkata")).date().isoformat()
        except Exception:
            today = datetime.now().date().isoformat()
        if day == today:
            book = self._get_trades(f"{self.base_url}/v2/trades")
            _take(book)
        return rows
    
    def get_stock_ltp(self, stock_name: str, fallback_service=None) -> Optional[float]:
        """
        Fetch LTP for a stock using Dhan API with Upstox fallback
        
        Args:
            stock_name: Stock symbol (e.g., 'RELIANCE', 'TATASTEEL')
            fallback_service: Upstox service instance for fallback
            
        Returns:
            LTP price as float, or None if not found
        """
        try:
            # Try Dhan API first
            # Dhan API expects security IDs, not symbols
            # We'll need to look up the security ID first or use Upstox directly
            # For now, fallback to Upstox until we have Dhan security mapping
            
            logger.info(f"Dhan API: Using Upstox fallback for {stock_name}")
            if fallback_service:
                return fallback_service.get_stock_ltp_from_market_quote(stock_name)
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching LTP for {stock_name}: {str(e)}")
            return None
    
    def get_nifty_data(self, fallback_service=None) -> Optional[Dict]:
        """
        Get NIFTY50 index data using Dhan API with Upstox fallback
        
        Returns:
            Dict with ltp, open, close, trend info
        """
        try:
            # Try Dhan API - use Upstox format for now
            # Dhan uses different format, we'll need proper mapping
            logger.info("Dhan API: Getting NIFTY50 data via Upstox fallback")
            return self._get_nifty_from_upstox(fallback_service)
            
        except Exception as e:
            logger.error(f"Error fetching NIFTY50 data: {str(e)}")
            return None
    
    def get_banknifty_data(self, fallback_service=None) -> Optional[Dict]:
        """
        Get BANKNIFTY index data using Dhan API with Upstox fallback
        
        Returns:
            Dict with ltp, open, close, trend info
        """
        try:
            # Try Dhan API - use Upstox format for now
            logger.info("Dhan API: Getting BANKNIFTY data via Upstox fallback")
            return self._get_banknifty_from_upstox(fallback_service)
            
        except Exception as e:
            logger.error(f"Error fetching BANKNIFTY data: {str(e)}")
            return None
    
    def _get_nifty_from_upstox(self, upstox_service) -> Optional[Dict]:
        """Fallback method to get NIFTY50 data from Upstox"""
        try:
            quote = upstox_service.get_market_quote_by_key(upstox_service.NIFTY50_KEY)
            if quote:
                return {
                    'ltp': quote.get('last_price', 0),
                    'open': quote.get('open', 0),
                    'close': quote.get('close', 0),
                    'source': 'upstox'
                }
        except Exception as e:
            logger.error(f"Upstox fallback failed for NIFTY50: {str(e)}")
        return None
    
    def _get_banknifty_from_upstox(self, upstox_service) -> Optional[Dict]:
        """Fallback method to get BANKNIFTY data from Upstox"""
        try:
            quote = upstox_service.get_market_quote_by_key(upstox_service.BANKNIFTY_KEY)
            if quote:
                return {
                    'ltp': quote.get('last_price', 0),
                    'open': quote.get('open', 0),
                    'close': quote.get('close', 0),
                    'source': 'upstox'
                }
        except Exception as e:
            logger.error(f"Upstox fallback failed for BANKNIFTY: {str(e)}")
        return None


dhan_service = DhanService()

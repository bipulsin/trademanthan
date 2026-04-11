"""
Thin MarketAux REST client (news/all) with connection reuse and small batching.
https://www.marketaux.com/documentation
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.marketaux.com/v1/news/all"
DEFAULT_TIMEOUT = 22


def _published_after_str(dt: datetime) -> str:
    """UTC ISO without sub-second (MarketAux-friendly)."""
    import pytz

    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    else:
        dt = dt.astimezone(pytz.UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


class MarketauxClient:
    def __init__(self, api_token: str, session: Optional[requests.Session] = None):
        self.api_token = (api_token or "").strip()
        self.session = session or requests.Session()

    def is_configured(self) -> bool:
        return bool(self.api_token)

    def fetch_news_for_symbols(
        self,
        symbols: List[str],
        published_after: datetime,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        One GET for a comma-separated symbol batch. Returns `data` article list (may be empty).
        """
        if not self.api_token or not symbols:
            return []
        sym = ",".join(sorted({s.strip().upper() for s in symbols if s and s.strip()}))
        if not sym:
            return []
        params = {
            "api_token": self.api_token,
            "symbols": sym,
            "published_after": _published_after_str(published_after),
            "language": "en",
            "limit": min(int(limit), 100),
        }
        try:
            r = self.session.get(BASE_URL, params=params, timeout=DEFAULT_TIMEOUT)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            logger.warning("MarketAux request failed symbols=%s: %s", sym[:80], e)
            return []
        if not isinstance(payload, dict):
            return []
        if payload.get("error"):
            logger.warning("MarketAux API error: %s", payload.get("error"))
            return []
        data = payload.get("data")
        if not isinstance(data, list):
            return []
        return [a for a in data if isinstance(a, dict)]

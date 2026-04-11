"""
NSE India corporate announcements (browser-like session).

NSE often rejects bare API calls; prime cookies with a normal page hit first,
then call https://www.nseindia.com/api/corporate-announcements with Referer.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")
API_URL = "https://www.nseindia.com/api/corporate-announcements"
# Pages that typically return Set-Cookie / allow API follow-up
PRIME_URLS = (
    "https://www.nseindia.com/option-chain",
    "https://www.nseindia.com/get-quotes/equity?symbol=RELIANCE",
)
DEFAULT_TIMEOUT = 28
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _ist_date_strings(*, lookback_days: int) -> Tuple[str, str]:
    """NSE expects DD-MM-YYYY; use IST calendar dates. lookback_days=0 → single day (today)."""
    today_ist = datetime.now(IST).date()
    start = today_ist - timedelta(days=max(0, int(lookback_days)))
    return start.strftime("%d-%m-%Y"), today_ist.strftime("%d-%m-%Y")


class NseCorporateAnnouncementsClient:
    """Thread-safe session with cookie priming and one retry on 403/401."""

    def __init__(self, *, lookback_calendar_days: int = 0) -> None:
        self._lookback = lookback_calendar_days
        self._lock = threading.Lock()
        self._session: Optional[requests.Session] = None
        self._primed = False

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": UA,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
            }
        )
        return s

    def _prime(self, s: requests.Session) -> bool:
        """Load NSE HTML endpoints so Akamai / session cookies attach."""
        html_headers = {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        ok_any = False
        for url in PRIME_URLS:
            try:
                r = s.get(url, headers=html_headers, timeout=DEFAULT_TIMEOUT)
                st = r.status_code
                if 200 <= st < 400:
                    ok_any = True
                logger.info(
                    "[fin_sentiment][nse][prime] url=%s http_status=%s cookies=%s",
                    url,
                    st,
                    len(s.cookies or {}),
                )
            except Exception as e:
                logger.info("[fin_sentiment][nse][prime] url=%s FAILED: %s", url, e)
        s.headers["Accept"] = "application/json, text/plain, */*"
        s.headers["Referer"] = "https://www.nseindia.com/companies-listing/corporate-filings-announcements"
        return ok_any

    def fetch_equity_announcements(self) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        (ok, rows). ok=False means transport/auth failure — caller should not advance watermark.
        ok=True and empty list means no rows in the requested window.
        """
        with self._lock:
            if self._session is None:
                self._session = self._build_session()
                self._primed = False
            s = self._session

            from_s, to_s = _ist_date_strings(lookback_days=self._lookback)
            params = {"index": "equities", "from_date": from_s, "to_date": to_s}
            logger.info(
                "[fin_sentiment][nse][request_plan] from_date=%s to_date=%s lookback_calendar_days=%s",
                from_s,
                to_s,
                self._lookback,
            )

            def _call() -> requests.Response:
                if not self._primed:
                    self._prime(s)
                    self._primed = True
                return s.get(API_URL, params=params, timeout=DEFAULT_TIMEOUT)

            for attempt in range(2):
                try:
                    logger.info("[fin_sentiment][nse][http] attempt=%s GET corporate-announcements", attempt + 1)
                    r = _call()
                    logger.info(
                        "[fin_sentiment][nse][http] attempt=%s http_status=%s bytes=%s",
                        attempt + 1,
                        r.status_code,
                        len(r.content or b""),
                    )
                    if r.status_code in (401, 403):
                        logger.warning(
                            "[fin_sentiment][nse][http] status=%s re-priming session (attempt %s)",
                            r.status_code,
                            attempt + 1,
                        )
                        self._primed = False
                        self._session = self._build_session()
                        s = self._session
                        time.sleep(0.4)
                        continue
                    r.raise_for_status()
                    data = r.json()
                    if not isinstance(data, list):
                        logger.warning(
                            "[fin_sentiment][nse][parse] expected JSON list, got %s",
                            type(data),
                        )
                        return False, []
                    rows = [x for x in data if isinstance(x, dict)]
                    syms = {str(x.get("symbol") or "").strip().upper() for x in rows}
                    syms.discard("")
                    logger.info(
                        "[fin_sentiment][nse][parse] ok dict_rows=%s distinct_symbols=%s",
                        len(rows),
                        len(syms),
                    )
                    return True, rows
                except Exception as e:
                    logger.warning("[fin_sentiment][nse][http] request failed attempt=%s: %s", attempt + 1, e)
                    self._primed = False
                    if attempt == 0:
                        self._session = self._build_session()
                        s = self._session
                        time.sleep(0.5)
                        continue
                    return False, []
            return False, []

"""
Iron Condor: earnings / results window hints via NSE corporate announcements (best-effort).
Un-scraped upstream failures fall back to user-declared dates in the checklist POST.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.nse_corporate_client import NseCorporateAnnouncementsClient

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

_RESULT_TERMS = re.compile(
    r"(financial\s+results|earning|result declaration|results for the|results of the|board meeting|bm outcome|results today)",
    re.I,
)
_DATE_PATTERNS = [
    # 30 April 2026
    re.compile(r"\b(\d{1,2})[\s\-](Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*[\s\-](\d{4})\b", re.I),
    re.compile(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*[\s\-](\d{1,2})[\s\,]+(\d{4})\b", re.I),
    re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b"),
]


def _mon_to_num(mon: str) -> int:
    m = mon.strip().lower()[:3]
    lookup = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    return lookup.get(m, 0)


def extract_future_dates_from_text(text: str, *, today: date) -> List[date]:
    if not text:
        return []
    out: List[date] = []
    horizon = today + timedelta(days=400)

    for rx in (_DATE_PATTERNS[0], _DATE_PATTERNS[1]):
        for m in rx.finditer(text):
            try:
                if rx is _DATE_PATTERNS[0]:
                    dd, mm, yy = int(m.group(1)), _mon_to_num(m.group(2)), int(m.group(3))
                    d = date(yy, mm, dd)
                else:
                    mmn, dd, yy = _mon_to_num(m.group(1)), int(m.group(2)), int(m.group(3))
                    if mmn <= 0:
                        continue
                    d = date(yy, mmn, dd)
                if today < d <= horizon:
                    out.append(d)
            except Exception:
                continue

    for m in _DATE_PATTERNS[2].finditer(text):
        try:
            yy, md, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            d = date(yy, md, dd)
            if today < d <= horizon:
                out.append(d)
        except Exception:
            continue
    return out


def _row_text(row: Dict[str, Any]) -> str:
    parts = [
        str(row.get("desc") or ""),
        str(row.get("sm_name") or ""),
        str(row.get("an_dt") or ""),
        str(row.get("sort_date") or ""),
    ]
    return " ".join(parts)


def fetch_nse_results_hint(symbol: str) -> Tuple[Optional[date], str, Dict[str, Any]]:
    """
    Return (nearest_future_event_date_optional, narrative, diagnostics).
    When NSE is blocked or irrelevant rows missing, returns (None, reason, {...}).
    """
    diag: Dict[str, Any] = {"source": "NSE_CA", "ok": False}
    sym = (symbol or "").strip().upper()
    today = datetime.now(IST).date()
    start_d = today - timedelta(days=30)
    end_d = today + timedelta(days=120)
    # NSE DD-MM-YYYY
    fr = start_d.strftime("%d-%m-%Y")
    to = end_d.strftime("%d-%m-%Y")

    try:
        client = NseCorporateAnnouncementsClient(lookback_calendar_days=0)
        ok, rows = client.fetch_equity_announcements(from_date=fr, to_date=to)
    except Exception as e:
        diag["error"] = str(e)
        return None, "NSECorporateClient error", diag

    if not ok:
        diag["transport"] = "nse_blocked_or_http"
        return None, "Could not retrieve NSE corporate feed (network / cookies).", diag

    sym_rows: List[Dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        s = str(r.get("symbol") or "").strip().upper().split("-")[0]
        if not sym:
            continue
        if not (s.startswith(sym) and len(s) <= len(sym) + 3):
            continue
        txt = _row_text(r)
        if _RESULT_TERMS.search(txt):
            sym_rows.append(r)
    diag["matched_rows"] = len(sym_rows)
    if not sym_rows:
        diag["symbol"] = sym
        return None, "No results/board-meeting filings for symbol in NSE CA window.", diag

    cand_dates: List[date] = []
    for r in sym_rows:
        cand_dates.extend(extract_future_dates_from_text(_row_text(r), today=today))
    cand_dates.sort()
    uniq: List[date] = []
    for d in cand_dates:
        if not uniq or d != uniq[-1]:
            uniq.append(d)

    diag["parsed_future_dates"] = [str(x) for x in uniq[:6]]
    if not uniq:
        diag["ok"] = True
        diag["note"] = "Relevant filings but no parseable forward date in text."
        return None, "NSE filings found for symbol but no dependable future calendar date extracted.", diag

    nxt = uniq[0]
    diag["ok"] = True
    diag["picked"] = str(nxt)
    return nxt, "Next parsed event-heavy date from recent NSE corporate text.", diag

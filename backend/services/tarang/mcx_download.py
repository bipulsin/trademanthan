"""MCX Bhavcopy downloader — public MCX backpage endpoints only. No Upstox token."""
from __future__ import annotations

import csv
import io
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import requests
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.bhavcopy import DATEWISE_DIR, RAW_DIR, checksum_from_html, import_bhavcopy_text
from backend.services.tarang.calendar import mcx_is_holiday_or_weekend, to_ist
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

BHAVCOPY_URL = "https://www.mcxindia.com/backpage.aspx/GetDateWiseBhavCopy"
DATEWISE_URL = "https://www.mcxindia.com/backpage.aspx/GetHistoricalDataDetails"
ROBOTS_URL = "https://www.mcxindia.com/robots.txt"
PAGE_URL = "https://www.mcxindia.com/market-data/bhavcopy"

UA = "TradeManthan-KosmicTarang/1.0 (operator archival; +https://www.tradewithcto.com)"
MINI_SYMBOLS = ("CRUDEOILM", "NATGASMINI")
FULL_SYMBOLS = ("CRUDEOIL", "NATURALGAS")
INSTRUMENTS = ("OPTFUT", "FUTCOM")
SLEEP_SEC = 4.0  # 3–5s if automation is ever re-enabled with written consent
# robots.txt User-agent: * Disallow: /  AND Googlebot Disallow: /backpage.aspx
# Terms §10 forbid systematic automated data collection without written consent.
AUTOMATION_ALLOWED = False
AUTOMATION_STOP_REASON = (
    "MCX robots.txt (https://www.mcxindia.com/robots.txt) has 'User-agent: *' / 'Disallow: /' "
    "and Googlebot 'Disallow: /backpage.aspx'. Terms of use clause 10 "
    "(https://www.mcxindia.com/terms-and-conditions-of-usage-for-website) states: "
    "'User may not conduct any systematic or automated data collection activities "
    "(including scraping, data mining, data extraction and data harvesting) on or in "
    "relation to the website with MCX’s written consent.' Automated bhavcopy download is STOPPED. "
    "Use admin drag-and-drop only."
)


def _headers() -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json; charset=utf-8",
        "Origin": "https://www.mcxindia.com",
        "Referer": PAGE_URL,
        "X-Requested-With": "XMLHttpRequest",
    }


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_headers())
    return s


def symbols_in_scope(*, include_full: bool = False) -> List[str]:
    out = list(MINI_SYMBOLS)
    if include_full:
        out.extend(FULL_SYMBOLS)
    return out


def include_full_from_config() -> bool:
    try:
        from backend.services.tarang.config import get_profiles

        for p in (get_profiles().get("profiles") or {}).values():
            if str(p.get("contractFamily") or "").lower() == "full":
                return True
    except Exception:
        pass
    return False


def _expiry_to_iso(raw: Any) -> Optional[str]:
    s = str(raw or "").strip().upper().replace(" ", "")
    if not s:
        return None
    for fmt in ("%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d%b%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def json_rows_to_csv(rows: Sequence[Dict[str, Any]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "Date",
            "Instrument Name",
            "Symbol",
            "Expiry Date",
            "Option Type",
            "Strike Price",
            "Open",
            "High",
            "Low",
            "Close",
            "Previous Close",
            "Volume(Lots)",
            "Value(Lacs)",
            "Open Interest(Lots)",
        ]
    )
    for r in rows:
        d = r.get("DateDisplay") or r.get("Date") or ""
        if isinstance(d, str) and "/Date(" in d:
            d = ""
        exp = str(r.get("ExpiryDate") or "")
        w.writerow(
            [
                d if d and not str(d).startswith("0") else str(r.get("DateDisplay") or ""),
                r.get("InstrumentName") or "",
                str(r.get("Symbol") or "").strip(),
                exp,
                r.get("OptionType") or "",
                r.get("StrikePrice") or 0,
                r.get("Open"),
                r.get("High"),
                r.get("Low"),
                r.get("Close"),
                r.get("PreviousClose"),
                r.get("Volume"),
                r.get("Value"),
                r.get("OpenInterest"),
            ]
        )
    return buf.getvalue()


def _extract_rows(body: Any) -> List[Dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    d = body.get("d") or body.get("Data") or body
    if isinstance(d, dict):
        data = d.get("Data") or d.get("data") or []
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
    if isinstance(d, list):
        return [x for x in d if isinstance(x, dict)]
    return []


def probe_endpoints() -> Dict[str, Any]:
    """One polite probe. If blocked, callers must stop automation."""
    s = _session()
    out: Dict[str, Any] = {"ok": False, "blocked": False, "robots": None, "bhavcopy": None}
    try:
        rob = s.get(ROBOTS_URL, timeout=20)
        out["robots"] = {"http": rob.status_code, "excerpt": (rob.text or "")[:400]}
    except requests.RequestException as e:
        out["robots"] = {"error": str(e)[:200]}
    try:
        yesterday = (to_ist().date() - timedelta(days=1)).strftime("%Y%m%d")
        r = s.post(
            BHAVCOPY_URL,
            data=json.dumps({"Date": yesterday, "InstrumentName": "FUTCOM"}),
            timeout=30,
        )
        blocked = r.status_code in (401, 403, 429) or "captcha" in (r.text or "").lower()
        out["bhavcopy"] = {"http": r.status_code, "blocked": blocked, "bytes": len(r.content or b"")}
        out["blocked"] = blocked or r.status_code >= 400
        out["ok"] = r.status_code == 200 and not blocked
        if r.status_code == 200:
            try:
                rows = _extract_rows(r.json())
                out["bhavcopy"]["n_rows"] = len(rows)
                out["ok"] = True
                out["blocked"] = False
            except Exception:
                out["ok"] = False
                out["blocked"] = True
                out["bhavcopy"]["parse"] = "unparseable"
    except requests.RequestException as e:
        out["bhavcopy"] = {"error": str(e)[:200]}
        out["blocked"] = True
    out["date_range_bhavcopy"] = False
    out["date_range_note"] = (
        "GetDateWiseBhavCopy is one trade date at a time. Admin backfill loops calendar days. "
        "GetHistoricalDataDetails is a date-range (max 365d) of aggregated Date Wise volumes, not per-expiry OHLC."
    )
    return out


def fetch_bhavcopy(trade_d: date, instrument: str, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    s = session or _session()
    payload = json.dumps({"Date": trade_d.strftime("%Y%m%d"), "InstrumentName": instrument})
    r = s.post(BHAVCOPY_URL, data=payload, timeout=45)
    if r.status_code != 200:
        return {"ok": False, "http": r.status_code, "blocked": r.status_code in (401, 403, 429), "text": (r.text or "")[:300]}
    try:
        body = r.json()
    except Exception:
        return {"ok": False, "http": r.status_code, "blocked": True, "error": "not_json"}
    rows = _extract_rows(body)
    return {"ok": True, "http": r.status_code, "rows": rows}


def fetch_datewise(start: date, end: date, session: Optional[requests.Session] = None) -> Dict[str, Any]:
    s = session or _session()
    payload = json.dumps(
        {
            "GroupBy": "D",
            "Segment": "ALL",
            "CommodityHead": "ALL",
            "Commodity": "ALL",
            "Startdate": start.strftime("%Y%m%d"),
            "EndDate": end.strftime("%Y%m%d"),
            "InstrumentName": "ALL",
        }
    )
    r = s.post(DATEWISE_URL, data=payload, timeout=45)
    if r.status_code != 200:
        return {"ok": False, "http": r.status_code, "blocked": r.status_code in (401, 403, 429)}
    try:
        rows = _extract_rows(r.json())
    except Exception:
        return {"ok": False, "error": "not_json"}
    DATEWISE_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATEWISE_DIR / f"datewise_{start.isoformat()}_{end.isoformat()}.json"
    dest.write_text(json.dumps(rows, default=str)[:2_000_000], encoding="utf-8")
    return {"ok": True, "n": len(rows), "path": str(dest)}


def _filter_symbols(rows: List[Dict[str, Any]], wanted: Sequence[str]) -> List[Dict[str, Any]]:
    want = {w.upper() for w in wanted}
    out = []
    for r in rows:
        sym = str(r.get("Symbol") or "").strip().upper()
        if sym in want:
            out.append(r)
    return out


def _imported_dates(symbols: Sequence[str]) -> set:
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT trade_date FROM tarang_hist_eod
                WHERE symbol = ANY(:syms)
                """
            ),
            {"syms": list(symbols)},
        ).fetchall()
        return {r[0] if not hasattr(r[0], "isoformat") else r[0] for r in rows}
    except Exception:
        return set()
    finally:
        db.close()


def _iter_session_days(start: date, end: date) -> List[date]:
    days = []
    d = start
    while d <= end:
        dt = datetime(d.year, d.month, d.day, 12, 0, tzinfo=timezone.utc)
        if not mcx_is_holiday_or_weekend(dt):
            days.append(d)
        d += timedelta(days=1)
    return days


def run_mcx_bhavcopy_download(
    *,
    force_probe: bool = True,
    backfill_start: Optional[date] = None,
    include_full: Optional[bool] = None,
) -> Dict[str, Any]:
    """Scheduled download. Independent of Upstox. Stopped: robots.txt + terms forbid scraping."""
    assert "upstox" not in BHAVCOPY_URL.lower()
    if not AUTOMATION_ALLOWED:
        return {
            "ok": False,
            "blocked": True,
            "stopped_by_robots": True,
            "upstox_used": False,
            "reason": AUTOMATION_STOP_REASON,
            "fallback": "Admin drag-and-drop CSV/XLS on Tarang Backtest.",
        }
    probe = probe_endpoints() if force_probe else {"ok": True, "blocked": False}
    if probe.get("blocked") or not probe.get("ok"):
        logger.warning("MCX bhavcopy automation blocked/unclear: %s", probe)
        return {
            "ok": False,
            "blocked": True,
            "probe": probe,
            "fallback": "Use admin drag-and-drop on Tarang Backtest / Data health.",
            "upstox_used": False,
        }
    ensure_tarang_tables()
    full = include_full_from_config() if include_full is None else include_full
    wanted = symbols_in_scope(include_full=full)
    ist = to_ist()
    today = ist.date()
    if backfill_start:
        start = backfill_start
    else:
        start = today - timedelta(days=14)
    end = today - timedelta(days=1) if ist.hour < 12 else today
    have = _imported_dates(wanted)
    missing = [d for d in _iter_session_days(start, end) if d not in have]
    session = _session()
    imported = []
    errors = []
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for d in missing:
        for inst in INSTRUMENTS:
            time.sleep(SLEEP_SEC)
            try:
                fetched = fetch_bhavcopy(d, inst, session)
            except requests.RequestException as e:
                errors.append({"date": d.isoformat(), "instrument": inst, "error": str(e)[:160]})
                continue
            if fetched.get("blocked"):
                return {"ok": False, "blocked": True, "at": d.isoformat(), "probe": fetched, "upstox_used": False}
            if not fetched.get("ok"):
                errors.append({"date": d.isoformat(), "instrument": inst, "http": fetched.get("http")})
                continue
            rows = _filter_symbols(fetched.get("rows") or [], wanted)
            if not rows:
                continue
            csv_text = json_rows_to_csv(rows)
            fn = f"mcx_{inst}_{d.isoformat()}.csv"
            (RAW_DIR / fn).write_text(csv_text, encoding="utf-8")
            imp = import_bhavcopy_text(csv_text, filename=fn, copy_to_raw=False)
            imported.append({"date": d.isoformat(), "instrument": inst, "import": imp})
        try:
            time.sleep(SLEEP_SEC)
            dw = fetch_datewise(d, d, session)
            imported.append({"datewise": dw})
        except Exception as e:
            errors.append({"datewise": str(e)[:160]})
    still_missing = []
    have2 = _imported_dates(wanted)
    cutoff = today - timedelta(days=1)
    for d in _iter_session_days(today - timedelta(days=10), cutoff):
        if d not in have2 and (today - d).days >= 1:
            still_missing.append(d.isoformat())
    if still_missing:
        from backend.services.tarang.alerts_telegram import notify_ops
        from backend.services.tarang.data_gaps import record_data_gap

        record_data_gap(venue="upstox_mcx", reason="bhavcopy_day_missing_24h", detail={"dates": still_missing})
        db = SessionLocal()
        try:
            notify_ops(
                db,
                kind="data_gap",
                message=f"MCX Bhavcopy still missing after 24h: {', '.join(still_missing[:8])}",
                dedupe_key=f"bhavcopy_missing:{still_missing[0]}",
            )
            db.commit()
        finally:
            db.close()
    return {
        "ok": True,
        "blocked": False,
        "upstox_used": False,
        "endpoint": BHAVCOPY_URL,
        "datewise_endpoint": DATEWISE_URL,
        "date_range_bhavcopy": False,
        "symbols": wanted,
        "imported": imported,
        "errors": errors,
        "still_missing": still_missing,
        "probe": {k: probe.get(k) for k in ("ok", "blocked", "date_range_note")},
    }

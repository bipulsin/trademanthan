"""Private Telegram reminder after MCX expiry: drag-drop OPTFUT + FUTCOM date range.

MCX HTTP scraping stays off. Does not call mcxindia.com.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from backend.database import SessionLocal
from backend.services.tarang.adapters.upstox_mcx import _expiry_iso, _expiry_ms, _is_mcx_fo
from backend.services.tarang.calendar import mcx_is_holiday_or_weekend, to_ist
from backend.services.tarang.schema import ensure_tarang_tables
from backend.services.divtest.instruments import ensure_instrument_master

IST = ZoneInfo("Asia/Kolkata")
SYMBOLS = ("CRUDEOILM", "NATGASMINI")
INSTRUMENTS = ("OPTFUT", "FUTCOM")
LOOKBACK_DAYS = 90


def download_span(expiry: date) -> tuple[date, date]:
    return expiry - timedelta(days=LOOKBACK_DAYS), expiry


def option_expiries_on(day: date, symbol: str) -> List[date]:
    rows = ensure_instrument_master()
    us = symbol.upper()
    found = set()
    for r in rows:
        if not _is_mcx_fo(r):
            continue
        if str(r.get("underlying_symbol") or "").upper() != us:
            continue
        itype = str(r.get("instrument_type") or "").upper()
        if itype not in ("CE", "PE"):
            continue
        ems = _expiry_ms(r)
        if ems is None:
            continue
        exp = date.fromisoformat(_expiry_iso(ems))
        if exp == day:
            found.add(exp)
    return sorted(found)


def reminder_lines(day: Optional[date] = None) -> List[Dict[str, Any]]:
    d = day or to_ist().date()
    out = []
    for sym in SYMBOLS:
        for exp in option_expiries_on(d, sym):
            lo, hi = download_span(exp)
            out.append(
                {
                    "symbol": sym,
                    "expiry": exp.isoformat(),
                    "instruments": list(INSTRUMENTS),
                    "from_date": lo.isoformat(),
                    "to_date": hi.isoformat(),
                    "filenames_hint": [
                        f"{sym}_{inst}_{exp.strftime('%d%b%Y').upper()}_{lo.isoformat()}_{hi.isoformat()}.csv"
                        for inst in INSTRUMENTS
                    ],
                }
            )
    return out


def run_mcx_expiry_reminder(*, force_date: Optional[str] = None) -> Dict[str, Any]:
    ensure_tarang_tables()
    if force_date:
        day = date.fromisoformat(force_date)
    else:
        if mcx_is_holiday_or_weekend():
            return {"ok": True, "skipped": "holiday_or_weekend"}
        day = to_ist().date()
    rows = reminder_lines(day)
    if not rows:
        return {"ok": True, "n": 0, "day": day.isoformat(), "note": "no MCX option expiry today"}
    lines = [
        f"MCX expiry {day.isoformat()} — scrape stays OFF. Drag-drop contract-wise CSVs:",
    ]
    for r in rows:
        lines.append(
            f"- {r['symbol']} expiry {r['expiry']}: download OPTFUT and FUTCOM for "
            f"{r['from_date']} → {r['to_date']} (lookback {LOOKBACK_DAYS}d)."
        )
    msg = "\n".join(lines)
    from backend.services.tarang.alerts_telegram import notify_ops

    db = SessionLocal()
    try:
        tg = notify_ops(
            db,
            kind="mcx_expiry_download_reminder",
            message=msg,
            dedupe_key=f"mcx_expiry_dl:{day.isoformat()}",
            throttle_sec=0,
        )
        db.commit()
    finally:
        db.close()
    return {"ok": True, "n": len(rows), "day": day.isoformat(), "rows": rows, "telegram": tg}

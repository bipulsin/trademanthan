"""Parse journal paste + enrich trade_log rows from arbitrage_master."""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, time
from functools import lru_cache
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import get_instruments_file_path
from backend.services.rule27_session_log import (
    ensure_trade_session_log_table,
    net_pnl_from_trade_log,
    upsert_session,
)
from backend.services.rule27_trade_log import (
    _as_date,
    _as_time,
    _bars_held,
    _f,
    _points,
    normalize_exit_trigger_type,
)
from backend.services.smart_futures_picker.position_sizing import (
    get_futures_lot_size_by_instrument_key,
)

logger = logging.getLogger(__name__)

_MONTHS = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)


def _num(s: str) -> Optional[float]:
    try:
        return float(str(s).replace(",", "").replace("₹", "").replace("INR", "").strip())
    except (TypeError, ValueError):
        return None


def _int_qty(s: str) -> Optional[int]:
    v = _num(s)
    if v is None:
        return None
    return int(round(v))


def normalize_underlying(raw: str) -> str:
    s = (raw or "").upper().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\bFUT(?:URES?)?\b", " ", s)
    s = re.sub(rf"\d{{1,2}}\s+(?:{'|'.join(_MONTHS)})\s+\d{{2,4}}", " ", s)
    s = re.sub(r"\b[QNM]\d{4}\b", " ", s)
    s = re.sub(r"^([A-Z0-9]+?)[QNM]\d{4}$", r"\1", s.strip())
    return re.sub(r"[^A-Z0-9]", "", s)


def _kv_map(text_in: str) -> Dict[str, str]:
    """Parse ``key : value`` / ``key=value`` lines (spaces around separators OK)."""
    out: Dict[str, str] = {}
    for line in (text_in or "").splitlines():
        m = re.match(r"^\s*([A-Za-z][A-Za-z0-9_/ ]{0,48}?)\s*[:=]\s*(.+?)\s*$", line)
        if not m:
            continue
        key = re.sub(r"[\s/]+", "_", m.group(1).strip().lower())
        out[key] = m.group(2).strip()
    return out


def _clock_from_val(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = str(val).strip()
    m = re.search(r"(?:20\d{2}-\d{2}-\d{2}[ T])?(\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?)", s)
    if not m:
        return None
    clock = m.group(1).split(".", 1)[0]
    parts = clock.split(":")
    if len(parts) == 2:
        return f"{int(parts[0]):02d}:{int(parts[1]):02d}:00"
    return f"{int(parts[0]):02d}:{int(parts[1]):02d}:{int(parts[2]):02d}"


def parse_journal_text(text_in: str) -> Dict[str, Any]:
    """Extract journal fields from a pasted trade note. Missing keys are None."""
    raw = text_in or ""
    t = raw.replace("\u2013", "-").replace("\u2014", "-")
    kv = _kv_map(t)
    out: Dict[str, Any] = {
        "session_date": None,
        "symbol": None,
        "direction": None,
        "entry_time": None,
        "entry_price": None,
        "exit_time": None,
        "exit_price": None,
        "qty": None,
        "slippage_pts": None,
        "exit_price_intended": None,
        "exit_trigger_type": None,
        "exit_trigger": None,
        "notes": raw.strip(),
        "parse_warnings": [],
    }

    # Prefer explicit key/value lines (Cursor journal / structured paste).
    for date_key in ("trade_date", "session_date", "date"):
        if kv.get(date_key):
            dm = re.search(r"(20\d{2}-\d{2}-\d{2})", kv[date_key])
            if dm:
                out["session_date"] = dm.group(1)
                break
    for sym_key in ("symbol", "symbol_instrument", "instrument", "underlying", "ticker"):
        if kv.get(sym_key):
            out["symbol"] = kv[sym_key].strip().upper()
            break
    for dir_key in ("side", "direction", "dir"):
        if kv.get(dir_key):
            dm = re.search(r"\b(LONG|SHORT)\b", kv[dir_key], re.I)
            if dm:
                out["direction"] = dm.group(1).upper()
                break
    if kv.get("entry_time") or kv.get("entrytime"):
        out["entry_time"] = _clock_from_val(kv.get("entry_time") or kv.get("entrytime"))
    if kv.get("exit_time") or kv.get("exittime"):
        out["exit_time"] = _clock_from_val(kv.get("exit_time") or kv.get("exittime"))
    if kv.get("entry_price") or kv.get("entryprice"):
        out["entry_price"] = _num(kv.get("entry_price") or kv.get("entryprice") or "")
    if kv.get("exit_price") or kv.get("exitprice"):
        out["exit_price"] = _num(kv.get("exit_price") or kv.get("exitprice") or "")
    for qty_key in ("qty", "quantity", "size", "lot", "lots"):
        if kv.get(qty_key):
            out["qty"] = _int_qty(kv[qty_key])
            break
    if kv.get("slippage_pts") or kv.get("slippage"):
        out["slippage_pts"] = _num(kv.get("slippage_pts") or kv.get("slippage") or "")
    if kv.get("exit_price_intended") or kv.get("intended_exit") or kv.get("target"):
        out["exit_price_intended"] = _num(
            kv.get("exit_price_intended") or kv.get("intended_exit") or kv.get("target") or ""
        )
    if kv.get("exit_trigger_type"):
        out["exit_trigger_type"] = normalize_exit_trigger_type(kv["exit_trigger_type"])
    if kv.get("exit_trigger") or kv.get("exit_rule") or kv.get("exitrule"):
        out["exit_trigger"] = (kv.get("exit_trigger") or kv.get("exit_rule") or kv.get("exitrule") or "")[
            :400
        ]

    dm = re.search(r"(20\d{2}-\d{2}-\d{2})", t)
    if not out["session_date"] and dm:
        out["session_date"] = dm.group(1)

    if not out["direction"]:
        dir_m = re.search(r"\b(LONG|SHORT)\b", t, re.I)
        if dir_m:
            out["direction"] = dir_m.group(1).upper()

    if not out["symbol"]:
        for pat in (
            r"Symbol\s*/\s*Instrument\s*[:|]?\s*([A-Z0-9][A-Z0-9 ./-]{1,40})",
            r"Symbol\s*[:|]\s*([A-Z0-9][A-Z0-9 ./-]{1,40})",
            r"\b(?:LONG|SHORT)\s+([A-Z]{2,20})(?:\s+FUT)?\b",
            r"\b([A-Z]{2,20})\s+(?:LONG|SHORT)\b",
            r"\b([A-Z]{2,20})\s+FUT\b",
        ):
            m = re.search(pat, t, re.I)
            if m:
                out["symbol"] = m.group(1).strip().upper()
                break

    if not out["entry_time"]:
        em = re.search(
            r"Entry(?:_?Time| \(time\)| time)?\s*[:|]?\s*(?:20\d{2}-\d{2}-\d{2}\s+)?(\d{1,2}:\d{2}(?::\d{2})?)",
            t,
            re.I,
        )
        if em:
            out["entry_time"] = _clock_from_val(em.group(1))
    if not out["exit_time"]:
        xm = re.search(
            r"Exit(?:_?Time| \(time\)| time)?\s*[:|]?\s*(?:20\d{2}-\d{2}-\d{2}\s+)?(\d{1,2}:\d{2}(?::\d{2})?)",
            t,
            re.I,
        )
        if xm:
            out["exit_time"] = _clock_from_val(xm.group(1))
    if not out["entry_time"] or not out["exit_time"]:
        span = re.search(
            r"(\d{1,2}:\d{2}(?::\d{2})?)\s*(?:to|-|→)\s*(\d{1,2}:\d{2}(?::\d{2})?)",
            t,
            re.I,
        )
        if span:
            out["entry_time"] = out["entry_time"] or _clock_from_val(span.group(1))
            out["exit_time"] = out["exit_time"] or _clock_from_val(span.group(2))

    if out["entry_price"] is None:
        ep = re.search(
            r"Entry(?:_?Price| \(price\)| price)\s*[:|]?\s*([0-9,]+\.?[0-9]*)",
            t,
            re.I,
        )
        if ep:
            out["entry_price"] = _num(ep.group(1))
    if out["exit_price"] is None:
        xp = re.search(
            r"Exit(?:_?Price| \(price\)| price)\s*[:|]?\s*([0-9,]+\.?[0-9]*)",
            t,
            re.I,
        )
        if xp:
            out["exit_price"] = _num(xp.group(1))
    if out["entry_price"] is None or out["exit_price"] is None:
        arrow = re.search(
            r"\(?\s*([0-9,]+\.?[0-9]*)\s*(?:→|->)\s*([0-9,]+\.?[0-9]*)\s*\)?",
            t,
        )
        if arrow:
            out["entry_price"] = out["entry_price"] or _num(arrow.group(1))
            out["exit_price"] = out["exit_price"] or _num(arrow.group(2))

    if out["qty"] is None:
        qm = re.search(
            r"(?:Size|Executed Shares\s*/\s*Lot|qty|quantity)\s*[:|]?\s*([0-9,]+)",
            t,
            re.I,
        )
        if qm:
            out["qty"] = _int_qty(qm.group(1))

    if out["slippage_pts"] is None:
        sm = None
        for m in re.finditer(r"([0-9]+\.?[0-9]*)\s*pts\b", t, re.I):
            sm = m
        if sm:
            out["slippage_pts"] = _num(sm.group(1))
    if out["exit_price_intended"] is None:
        intended = re.search(
            r"(?:intended|target)\s*(?:exit|fill)?[^0-9]{0,20}([0-9]+\.?[0-9]*)",
            t,
            re.I,
        )
        if intended:
            out["exit_price_intended"] = _num(intended.group(1))

    if not out["exit_trigger_type"]:
        etype = re.search(r"exit_trigger_type\s*[=:]\s*([a-z_]+)", t, re.I)
        if etype:
            out["exit_trigger_type"] = normalize_exit_trigger_type(etype.group(1))
        elif re.search(r"discretionary", t, re.I) and not re.search(
            r"rule_compliant|2-candle|F&O pause|pre-F&O", t, re.I
        ):
            out["exit_trigger_type"] = "discretionary"
        elif re.search(r"rule_compliant|2-candle fail|F&O pause|pre-F&O pause", t, re.I):
            out["exit_trigger_type"] = "rule_compliant"

    if not out["exit_trigger"]:
        trig = re.search(r"ExitRule\s*[:|]?\s*(.+)", t, re.I)
        if trig:
            out["exit_trigger"] = trig.group(1).strip()[:400]
        elif out["exit_trigger_type"]:
            out["exit_trigger"] = out["exit_trigger_type"]

    # Exit is optional for open / in-progress journals.
    missing = [
        k
        for k in ("symbol", "direction", "session_date", "entry_time", "entry_price")
        if not out.get(k)
    ]
    if missing:
        out["parse_warnings"].append("missing: " + ", ".join(missing))
    return out


def lookup_master(db: Session, symbol_raw: str) -> Optional[Dict[str, Any]]:
    token = (symbol_raw or "").strip()
    if not token:
        return None
    under = normalize_underlying(token)
    row = db.execute(
        text(
            """
            SELECT TRIM(stock) AS stock,
                   NULLIF(TRIM(currmth_future_symbol), '') AS future_symbol,
                   NULLIF(TRIM(currmth_future_instrument_key), '') AS instrument_key
            FROM arbitrage_master
            WHERE UPPER(TRIM(stock)) = :u
               OR UPPER(TRIM(currmth_future_symbol)) = :raw
               OR UPPER(TRIM(currmth_future_symbol)) LIKE :pfx
            ORDER BY CASE WHEN UPPER(TRIM(stock)) = :u THEN 0 ELSE 1 END
            LIMIT 1
            """
        ),
        {
            "u": under,
            "raw": token.upper(),
            "pfx": under + " FUT%",
        },
    ).mappings().first()
    return dict(row) if row else None


@lru_cache(maxsize=1)
def _trading_symbol_index() -> Dict[str, str]:
    path = get_instruments_file_path()
    out: Dict[str, str] = {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return out
    for inst in data if isinstance(data, list) else []:
        if not isinstance(inst, dict):
            continue
        key = str(inst.get("instrument_key") or "").strip()
        ts = str(inst.get("trading_symbol") or "").strip()
        if key and ts:
            out[key] = ts
    return out


def enrich_from_master(db: Session, parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Fill contract, qty (1 lot if omitted), points, gross PnL from master + prices."""
    out = dict(parsed)
    warnings = list(parsed.get("parse_warnings") or [])
    master = lookup_master(db, str(parsed.get("symbol") or ""))
    if not master:
        warnings.append("symbol not found in arbitrage_master")
        out["parse_warnings"] = warnings
        out["master_ok"] = False
        return out
    out["master_ok"] = True
    out["symbol"] = str(master["stock"]).upper()
    ikey = master.get("instrument_key") or ""
    lot = get_futures_lot_size_by_instrument_key(ikey) if ikey else 0
    out["lot_size"] = lot or None
    compact = _trading_symbol_index().get(ikey) if ikey else None
    out["contract"] = compact or master.get("future_symbol")
    out["future_symbol"] = master.get("future_symbol")
    if out.get("qty") is None:
        if lot and lot > 0:
            out["qty"] = int(lot)
        else:
            warnings.append("lot size unavailable; qty not set")
    direction = str(out.get("direction") or "").upper()
    entry = _f(out.get("entry_price"))
    exit_px = _f(out.get("exit_price"))
    qty = int(out["qty"]) if out.get("qty") is not None else None
    points = _points(direction, entry, exit_px) if entry is not None else None
    out["points_captured"] = points
    out["gross_pnl_inr"] = (
        round(float(points) * int(qty), 2) if points is not None and qty is not None else None
    )
    slip = _f(out.get("slippage_pts"))
    if slip is not None and qty is not None:
        out["slippage_inr"] = round(float(slip) * int(qty), 2)
    intended = _f(out.get("exit_price_intended"))
    if intended is None and slip is not None and exit_px is not None:
        if direction == "SHORT":
            intended = round(float(exit_px) - float(slip), 4)
        else:
            intended = round(float(exit_px) + float(slip), 4)
        out["exit_price_intended"] = intended
    et = _as_time(out.get("entry_time"))
    xt = _as_time(out.get("exit_time"))
    if et and xt:
        out["bars_held_10m"] = _bars_held(et, xt)
        out["entry_time"] = et.strftime("%H:%M:%S")
        out["exit_time"] = xt.strftime("%H:%M:%S")
    sd = _as_date(out.get("session_date"))
    if sd:
        out["session_date"] = str(sd)
    out["parse_warnings"] = warnings
    return out


def payload_from_enriched(enriched: Dict[str, Any], *, source: str) -> Dict[str, Any]:
    required = ("symbol", "direction", "session_date", "entry_time", "entry_price")
    missing = [k for k in required if not enriched.get(k)]
    if missing:
        raise ValueError("missing required fields: " + ", ".join(missing))
    if not enriched.get("master_ok"):
        raise ValueError("symbol not found in arbitrage_master")
    if enriched.get("qty") is None:
        raise ValueError("qty missing and lot size unavailable")
    notes = enriched.get("notes") or ""
    return {
        "session_date": enriched["session_date"],
        "symbol": enriched["symbol"],
        "contract": enriched.get("contract"),
        "direction": str(enriched["direction"]).upper(),
        "qty": int(enriched["qty"]),
        "entry_time": enriched["entry_time"],
        "entry_price": float(enriched["entry_price"]),
        "exit_time": enriched.get("exit_time"),
        "exit_price": _f(enriched.get("exit_price")),
        "exit_price_intended": _f(enriched.get("exit_price_intended")),
        "slippage_pts": _f(enriched.get("slippage_pts")),
        "points_captured": _f(enriched.get("points_captured")),
        "bars_held_10m": enriched.get("bars_held_10m"),
        "exit_trigger_type": enriched.get("exit_trigger_type"),
        "exit_trigger": enriched.get("exit_trigger"),
        "notes": notes,
        "source": source,
    }


def refresh_session_log(db: Session, session_date: str, last_exit: Optional[str], source: str) -> None:
    ensure_trade_session_log_table()
    n = db.execute(
        text(
            """
            SELECT COUNT(*) FROM trade_log
            WHERE session_date = CAST(:d AS date) AND exit_price IS NOT NULL
            """
        ),
        {"d": session_date},
    ).scalar()
    pnl = net_pnl_from_trade_log(db, session_date)
    upsert_session(
        db,
        {
            "session_date": session_date,
            "trades_taken_count": int(n or 0),
            "last_exit_time": last_exit,
            "net_pnl_at_session_end": pnl,
            "notes": f"tradelog UI refresh; {int(n or 0)} closed trades; net ₹{pnl:.2f}",
            "source": source,
        },
    )

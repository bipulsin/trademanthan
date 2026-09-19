"""MCX Bhavcopy (OPTFUT/FUTCOM) parser, importer, and Date Wise checksum."""
from __future__ import annotations

import csv
import io
import json
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.schema import ensure_tarang_tables

PLACEHOLDER_CLOSE = 0.05
FROM_TO_RE = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})")
MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
TRADE_DATE_RE = re.compile(
    r"^\s*(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})\s*$"
)
EXPIRY_RE = re.compile(r"^(\d{1,2})([A-Za-z]{3})(\d{4})$")
NUM_PREFIX_RE = re.compile(r"[-+]?\d*\.?\d+")

RAW_DIR = Path(__file__).resolve().parents[3] / "data" / "mcx_bhavcopy" / "raw"


def strip_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().strip('"').strip()


def parse_trade_date(raw: str) -> Optional[date]:
    s = strip_cell(raw)
    m = TRADE_DATE_RE.match(s)
    if not m:
        return None
    day = int(m.group(1))
    mon = MONTHS.get(m.group(2).upper()[:3])
    year = int(m.group(3))
    if not mon:
        return None
    return date(year, mon, day)


def parse_expiry(raw: str) -> Optional[date]:
    s = strip_cell(raw).upper().replace(" ", "")
    m = EXPIRY_RE.match(s)
    if not m:
        return None
    day = int(m.group(1))
    mon = MONTHS.get(m.group(2)[:3])
    year = int(m.group(3))
    if not mon:
        return None
    return date(year, mon, day)


def parse_float(raw: str) -> Optional[float]:
    s = strip_cell(raw)
    if s == "":
        return None
    m = NUM_PREFIX_RE.search(s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def parse_int_lots(raw: str) -> Optional[int]:
    v = parse_float(raw)
    if v is None:
        return None
    return int(v)


def normalize_option_type(instrument: str, option_type: str) -> str:
    inst = strip_cell(instrument).upper()
    ot = strip_cell(option_type).upper()
    if inst == "FUTCOM" or ot in ("", "XX", "FUT", "F"):
        return "FUT"
    if ot in ("CE", "C", "CALL"):
        return "CE"
    if ot in ("PE", "P", "PUT"):
        return "PE"
    return ot or "FUT"


def is_traded(volume_lots: Optional[int], close: Optional[float]) -> bool:
    if not volume_lots or volume_lots <= 0:
        return False
    if close is None:
        return False
    if abs(float(close) - PLACEHOLDER_CLOSE) < 1e-9:
        return False
    return True


def filename_from_to(filename: str) -> Tuple[Optional[date], Optional[date]]:
    m = FROM_TO_RE.search(filename or "")
    if not m:
        return None, None
    try:
        return date.fromisoformat(m.group(1)), date.fromisoformat(m.group(2))
    except ValueError:
        return None, None


def parse_bhavcopy_csv(text: str, *, source_filename: str = "") -> List[Dict[str, Any]]:
    """Parse MCX Bhavcopy CSV. Dates come from rows, never the filename."""
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    rows: List[Dict[str, Any]] = []
    for raw in reader:
        mapped = {strip_cell(k): strip_cell(v) for k, v in (raw or {}).items()}
        inst = mapped.get("Instrument Name") or mapped.get("Instrument") or ""
        symbol = strip_cell(mapped.get("Symbol") or "").upper()
        trade_d = parse_trade_date(mapped.get("Date") or "")
        expiry_d = parse_expiry(mapped.get("Expiry Date") or "")
        if not symbol or trade_d is None or expiry_d is None:
            continue
        ot = normalize_option_type(inst, mapped.get("Option Type") or "")
        strike = parse_float(mapped.get("Strike Price") or "") or 0.0
        if ot == "FUT":
            strike = 0.0
        close = parse_float(mapped.get("Close") or "")
        vol = parse_int_lots(mapped.get("Volume(Lots)") or "")
        row = {
            "trade_date": trade_d,
            "instrument_name": strip_cell(inst).upper() or None,
            "symbol": symbol,
            "expiry_date": expiry_d,
            "option_type": ot,
            "strike": float(strike),
            "open": parse_float(mapped.get("Open") or ""),
            "high": parse_float(mapped.get("High") or ""),
            "low": parse_float(mapped.get("Low") or ""),
            "close": close,
            "prev_close": parse_float(mapped.get("Previous Close") or ""),
            "volume_lots": vol if vol is not None else 0,
            "value_lacs": parse_float(mapped.get("Value(Lacs)") or ""),
            "oi_lots": parse_int_lots(mapped.get("Open Interest(Lots)") or ""),
            "source_filename": source_filename,
            "traded": is_traded(vol, close),
        }
        rows.append(row)
    return rows


def import_report_from_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    filename: str,
    inserted: int,
    duplicates_skipped: int,
) -> Dict[str, Any]:
    dates = sorted({r["trade_date"] for r in rows if r.get("trade_date")})
    first_d = dates[0] if dates else None
    last_d = dates[-1] if dates else None
    from_d, to_d = filename_from_to(filename)
    per_date: Dict[str, int] = defaultdict(int)
    for r in rows:
        if r.get("traded") and r.get("trade_date"):
            per_date[r["trade_date"].isoformat()] += 1
    early = bool(from_d and first_d and first_d == from_d)
    return {
        "filename": filename,
        "first_trade_date": first_d.isoformat() if first_d else None,
        "last_trade_date": last_d.isoformat() if last_d else None,
        "filename_from_date": from_d.isoformat() if from_d else None,
        "filename_to_date": to_d.isoformat() if to_d else None,
        "rows_total": len(rows),
        "inserted": inserted,
        "duplicates_skipped": duplicates_skipped,
        "traded_rows": sum(1 for r in rows if r.get("traded")),
        "traded_row_counts_per_date": dict(sorted(per_date.items())),
        "early_life_may_be_missing": early,
        "early_life_flag_note": (
            "first trade_date equals filename From Date — contract's early life may be missing"
            if early
            else None
        ),
    }


def _row_key(r: Dict[str, Any]) -> Tuple:
    return (
        r["symbol"],
        r["expiry_date"].isoformat() if isinstance(r["expiry_date"], date) else str(r["expiry_date"]),
        r["option_type"],
        float(r["strike"]),
        r["trade_date"].isoformat() if isinstance(r["trade_date"], date) else str(r["trade_date"]),
    )


def import_bhavcopy_text(
    csv_text: str,
    *,
    filename: str,
    copy_to_raw: bool = True,
) -> Dict[str, Any]:
    ensure_tarang_tables()
    rows = parse_bhavcopy_csv(csv_text, source_filename=filename)
    if copy_to_raw and filename:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        dest = RAW_DIR / Path(filename).name
        if not dest.exists():
            dest.write_text(csv_text, encoding="utf-8")
    started = datetime.now(timezone.utc)
    db = SessionLocal()
    inserted = 0
    dupes = 0
    import_id = None
    try:
        ins = db.execute(
            text(
                """
                INSERT INTO tarang_hist_imports (filename, started_at, report)
                VALUES (:fn, :st, '{}'::jsonb)
                RETURNING id
                """
            ),
            {"fn": filename, "st": started},
        )
        import_id = ins.scalar()
        upsert = text(
            """
            INSERT INTO tarang_hist_eod (
                symbol, expiry_date, option_type, strike, trade_date,
                open, high, low, close, prev_close, volume_lots, value_lacs, oi_lots,
                instrument_name, traded, source_filename, import_id
            ) VALUES (
                :symbol, :expiry_date, :option_type, :strike, :trade_date,
                :open, :high, :low, :close, :prev_close, :volume_lots, :value_lacs, :oi_lots,
                :instrument_name, :traded, :source_filename, :import_id
            )
            ON CONFLICT (symbol, expiry_date, option_type, strike, trade_date)
            DO NOTHING
            """
        )
        seen: set = set()
        for r in rows:
            k = _row_key(r)
            if k in seen:
                dupes += 1
                continue
            seen.add(k)
            params = {
                **r,
                "import_id": import_id,
                "expiry_date": r["expiry_date"],
                "trade_date": r["trade_date"],
                "traded": bool(r["traded"]),
            }
            res = db.execute(upsert, params)
            n = res.rowcount if res.rowcount is not None else 0
            if n:
                inserted += 1
            else:
                dupes += 1
        report = import_report_from_rows(rows, filename=filename, inserted=inserted, duplicates_skipped=dupes)
        report["import_id"] = import_id
        finished = datetime.now(timezone.utc)
        db.execute(
            text(
                """
                UPDATE tarang_hist_imports
                SET finished_at = :fin, report = CAST(:rep AS jsonb)
                WHERE id = :id
                """
            ),
            {"fin": finished, "rep": json.dumps(report), "id": import_id},
        )
        db.commit()
        return report
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def import_bhavcopy_path(path: str | Path, *, copy_to_raw: bool = True) -> Dict[str, Any]:
    p = Path(path)
    text = p.read_text(encoding="utf-8", errors="replace")
    return import_bhavcopy_text(text, filename=p.name, copy_to_raw=copy_to_raw)


class _DateWiseParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_td = False
        self.in_th = False
        self.cell = ""
        self.rows: List[List[str]] = []
        self.cur: List[str] = []

    def handle_starttag(self, tag: str, attrs):
        if tag in ("td", "th"):
            self.in_td = tag == "td"
            self.in_th = tag == "th"
            self.cell = ""
        if tag == "tr":
            self.cur = []

    def handle_endtag(self, tag: str):
        if tag in ("td", "th"):
            self.cur.append(strip_cell(self.cell))
            self.in_td = self.in_th = False
        if tag == "tr" and self.cur:
            self.rows.append(self.cur)
            self.cur = []

    def handle_data(self, data: str):
        if self.in_td or self.in_th:
            self.cell += data


def parse_date_wise_html(html: str) -> List[Dict[str, Any]]:
    """Parse MCX Date Wise HTML (often saved as .xls). One row per date+commodity."""
    p = _DateWiseParser()
    p.feed(html)
    if not p.rows:
        return []
    header = [h.lower() for h in p.rows[0]]
    def col(*names: str) -> Optional[int]:
        for i, h in enumerate(header):
            for n in names:
                if n in h:
                    return i
        return None

    i_date = col("date")
    i_sym = col("commodity", "symbol", "contract")
    i_lots = col("traded contract", "traded contracts", "lots")
    out: List[Dict[str, Any]] = []
    for row in p.rows[1:]:
        if i_date is None or i_date >= len(row):
            continue
        d = parse_trade_date(row[i_date]) or _try_iso(row[i_date])
        if d is None:
            continue
        symbol = strip_cell(row[i_sym]).upper() if i_sym is not None and i_sym < len(row) else ""
        lots = parse_float(row[i_lots]) if i_lots is not None and i_lots < len(row) else None
        out.append({"trade_date": d, "symbol": symbol, "traded_contract_lots": lots})
    return out


def _try_iso(raw: str) -> Optional[date]:
    s = strip_cell(raw)
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def checksum_bhavcopy_vs_datewise(
    datewise_rows: Sequence[Dict[str, Any]],
    *,
    threshold_pct: float = 1.0,
    db_volume_by_key: Optional[Dict[Tuple[str, date], float]] = None,
) -> Dict[str, Any]:
    """Compare Date Wise Traded Contract (Lots) vs SUM(volume_lots) for symbol+date across expiries."""
    if db_volume_by_key is None:
        db_volume_by_key = _sum_volume_from_db()
    flags: List[Dict[str, Any]] = []
    compared = 0
    for r in datewise_rows:
        d = r["trade_date"]
        if isinstance(d, str):
            d = date.fromisoformat(d[:10])
        sym = strip_cell(r.get("symbol") or "").upper()
        reported = r.get("traded_contract_lots")
        if reported is None:
            continue
        imported = float(db_volume_by_key.get((sym, d), 0.0))
        compared += 1
        if reported == 0 and imported == 0:
            continue
        base = max(abs(float(reported)), 1e-9)
        pct = abs(imported - float(reported)) / base * 100.0
        if pct > threshold_pct:
            flags.append(
                {
                    "symbol": sym,
                    "trade_date": d.isoformat(),
                    "datewise_lots": float(reported),
                    "bhavcopy_sum_volume_lots": imported,
                    "abs_pct_diff": round(pct, 4),
                }
            )
    return {
        "compared": compared,
        "threshold_pct": threshold_pct,
        "flags": flags,
        "ok": not flags,
    }


def _sum_volume_from_db() -> Dict[Tuple[str, date], float]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, trade_date, SUM(volume_lots)::float AS vol
                FROM tarang_hist_eod
                GROUP BY symbol, trade_date
                """
            )
        ).mappings().all()
        out: Dict[Tuple[str, date], float] = {}
        for r in rows:
            d = r["trade_date"]
            if hasattr(d, "isoformat") and not isinstance(d, date):
                d = d
            out[(str(r["symbol"]).upper(), d)] = float(r["vol"] or 0)
        return out
    finally:
        db.close()


def checksum_from_html(html: str, *, filename: str = "") -> Dict[str, Any]:
    parsed = parse_date_wise_html(html)
    cmp_ = checksum_bhavcopy_vs_datewise(parsed)
    cmp_["filename"] = filename
    cmp_["datewise_rows"] = len(parsed)
    return cmp_

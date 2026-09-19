"""Row-diff MCX contract-wise CSV vs tarang_hist_eod (no live MCX HTTP)."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from backend.services.tarang.bhavcopy import parse_bhavcopy_csv


def _key(r: Dict[str, Any]) -> Tuple:
    exp = r.get("expiry_date")
    td = r.get("trade_date")
    return (
        str(r.get("symbol") or "").strip().upper(),
        exp.isoformat() if hasattr(exp, "isoformat") else str(exp),
        str(r.get("option_type") or "").upper(),
        round(float(r.get("strike") or 0), 4),
        td.isoformat() if hasattr(td, "isoformat") else str(td),
    )


def _num(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


FIELDS = ("open", "high", "low", "close", "prev_close", "volume_lots", "oi_lots")


def compare_csv_to_rows(csv_text: str, db_rows: List[Dict[str, Any]], *, filename: str = "") -> Dict[str, Any]:
    parsed = parse_bhavcopy_csv(csv_text, source_filename=filename)
    file_map = {_key(r): r for r in parsed}
    db_map = {_key(r): r for r in db_rows}
    only_file = [k for k in file_map if k not in db_map]
    only_db = [k for k in db_map if k not in file_map]
    mismatches = []
    matched = 0
    for k, fr in file_map.items():
        dr = db_map.get(k)
        if not dr:
            continue
        diffs = {}
        for f in FIELDS:
            a, b = _num(fr.get(f)), _num(dr.get(f))
            if a is None and b is None:
                continue
            if a is None or b is None or abs(a - b) > 1e-6:
                diffs[f] = {"file": a, "db": b}
        if diffs:
            mismatches.append({"key": k, "diffs": diffs})
        else:
            matched += 1
    return {
        "file_rows": len(parsed),
        "db_rows": len(db_rows),
        "matched_all_fields": matched,
        "only_in_file": len(only_file),
        "only_in_db": len(only_db),
        "field_mismatches": len(mismatches),
        "sample_only_file": only_file[:5],
        "sample_only_db": only_db[:5],
        "sample_mismatches": mismatches[:8],
        "csv_fields": [
            "Date", "Instrument Name", "Symbol", "Expiry Date", "Option Type", "Strike Price",
            "Open", "High", "Low", "Close", "Previous Close", "Volume(Lots)", "Open Interest(Lots)",
        ],
        "datewise_api_note": (
            "GetHistoricalDataDetails is aggregated Date Wise volume — no strikes. "
            "GetDateWiseBhavCopy is contract-wise (has strikes/OHLC/OI) but automation is stopped (robots/terms)."
        ),
    }


def load_manual_csv(path: Optional[Path] = None) -> str:
    candidates = [
        path,
        Path("/Users/bipulsahay/.cursor/projects/Users-bipulsahay-TradeManthan/attachments/e5fa1eac-cc42-4fef-9f7b-3b378136d926/CRUDEOILM_OPTFUT_17SEP2026_2026-06-17_2026-09-17.csv"),
        Path(__file__).resolve().parents[3] / "data/mcx_bhavcopy/raw/CRUDEOILM_OPTFUT_17SEP2026_2026-06-17_2026-09-17.csv",
    ]
    for p in candidates:
        if p and p.is_file():
            return p.read_text(encoding="utf-8", errors="replace")
    raise FileNotFoundError("manual CRUDEOILM contract-wise CSV not found")

"""Append-only MTF Swing CSVs. A written row is never rewritten."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set

from backend.services.mtf_swing.config import (
    DISCARD_FIELDS,
    DISCARD_FIELDS_V2,
    REGIME_LOG_FIELDS,
    TRADE_FIELDS,
    TRADE_FIELDS_V2,
    VARIANT_SUMMARY_FIELDS,
)


def trade_key(row: Dict[str, str]) -> str:
    return str(row.get("trade_id") or "")


def discard_key(row: Dict[str, str]) -> str:
    return str(row.get("discard_id") or "")


def read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def existing_keys(path: Path, field: str) -> Set[str]:
    return {str(r.get(field) or "") for r in read_rows(path) if r.get(field)}


def append_rows(path: Path, fields: Sequence[str], rows: Iterable[Dict[str, str]], key_field: str) -> int:
    """Append rows whose key is not already in the file. Returns the number written.

    The file is opened in append mode. Existing bytes, including the header, are not touched.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fresh = not path.is_file() or path.stat().st_size == 0
    seen = set() if fresh else existing_keys(path, key_field)
    wrote = 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fields), extrasaction="ignore")
        if fresh:
            writer.writeheader()
        for row in rows:
            key = str(row.get(key_field) or "")
            if not key or key in seen:
                continue
            writer.writerow({k: row.get(k, "") for k in fields})
            seen.add(key)
            wrote += 1
    return wrote


def append_trades(path: Path, rows: Iterable[Dict[str, str]]) -> int:
    return append_rows(path, TRADE_FIELDS, rows, "trade_id")


def append_discards(path: Path, rows: Iterable[Dict[str, str]]) -> int:
    return append_rows(path, DISCARD_FIELDS, rows, "discard_id")


def append_trades_v2(path: Path, rows: Iterable[Dict[str, str]]) -> int:
    return append_rows(path, TRADE_FIELDS_V2, rows, "trade_id")


def append_discards_v2(path: Path, rows: Iterable[Dict[str, str]]) -> int:
    return append_rows(path, DISCARD_FIELDS_V2, rows, "discard_id")


def append_regime_log(path: Path, rows: Iterable[Dict[str, str]]) -> int:
    return append_rows(path, REGIME_LOG_FIELDS, rows, "screen_date")


def write_variant_summary(path: Path, rows: Iterable[Dict[str, str]]) -> int:
    """Overwrite variant_summary.csv (ablation roll-up, not append-only)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    material = list(rows)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(VARIANT_SUMMARY_FIELDS), extrasaction="ignore")
        writer.writeheader()
        for row in material:
            writer.writerow({k: row.get(k, "") for k in VARIANT_SUMMARY_FIELDS})
    return len(material)

"""CSV store for stock-day rows. The page and the analysis both read this."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from backend.services.big_move.features import FEATURE_META, THRESHOLDS

STRINGS = {
    "date",
    "symbol",
    "contract",
    "instrument_key",
    "oi_state",
    "oi_basis",
    "gap_location",
}

LABELS = ["max_up_atr", "max_down_atr", "close_move_atr"]
for _threshold in THRESHOLDS:
    _tag = str(_threshold)
    LABELS.extend(
        [
            f"big_up_{_tag}",
            f"big_down_{_tag}",
            f"big_either_{_tag}",
            f"clean_up_{_tag}",
            f"clean_down_{_tag}",
        ]
    )

COLUMNS = (
    ["date", "symbol", "contract", "instrument_key", "reference", "atr", "prev_close"]
    + [name for name, _title, _group, _kind in FEATURE_META]
    + ["oi_basis"]
    + LABELS
)
SKIP_COLUMNS = ["date", "symbol", "contract", "reason"]


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _parse(key: str, value: str) -> Any:
    if value == "":
        return "" if key in STRINGS else None
    if key in STRINGS:
        return value
    try:
        return float(value)
    except ValueError:
        return value


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], columns: Optional[List[str]] = None) -> None:
    columns = columns or COLUMNS
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _cell(row.get(key)) for key in columns})
    tmp.replace(path)


def read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.is_file():
        return []
    out: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if not raw:
                continue
            out.append({key: _parse(key, (raw.get(key) or "").strip()) for key in raw})
    return out

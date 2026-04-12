"""
Stock → Upstox sector index instrument_key from ``backend/fno_sector_mapping.csv``.

Used for ``arbitrage_master.sector_index`` when a row exists in the CSV; otherwise the
static ``equity_sector_index_instrument_key`` mapping applies.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Optional

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV_PATH = _REPO_ROOT / "backend" / "fno_sector_mapping.csv"


def load_fno_sector_index_map(csv_path: Optional[Path] = None) -> Dict[str, str]:
    """
    Returns uppercased NSE symbol → sector_index string (e.g. ``NSE_INDEX|Nifty IT``).
    Empty dict if file is missing.
    """
    path = csv_path or DEFAULT_CSV_PATH
    if not path.is_file():
        return {}
    out: Dict[str, str] = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.reader(f):
            if not row:
                continue
            sym = str(row[0] or "").strip().upper()
            if not sym:
                continue
            if len(row) < 2:
                continue
            idx = str(row[1] or "").strip()
            if idx:
                out[sym] = idx
    return out

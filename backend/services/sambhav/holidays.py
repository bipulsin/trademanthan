"""NSE holiday dates used by Sambhav session classification (V1).

These dates are EXCLUDED from the Sambhav V1 ML dataset. They are not
treated as missing candles. Candles are never fabricated for holidays.
"""

from __future__ import annotations

from datetime import date
from typing import Dict, Iterable, List, Set

# Source: NSE holiday circulars + dates observed with zero NIFTY 10m bars
# during the 2022-01-03 → 2026-08-12 Upstox import (not weekends).
SAMBHAV_NSE_HOLIDAYS: Dict[int, List[str]] = {
    2022: [
        "2022-01-26",
        "2022-03-01",
        "2022-03-18",
        "2022-04-14",
        "2022-04-15",
        "2022-05-03",
        "2022-08-09",
        "2022-08-15",
        "2022-08-31",
        "2022-10-05",
        "2022-10-24",
        "2022-10-26",
        "2022-11-08",
    ],
    2023: [
        "2023-01-26",
        "2023-03-07",
        "2023-03-30",
        "2023-04-04",
        "2023-04-07",
        "2023-04-14",
        "2023-05-01",
        "2023-06-29",
        "2023-08-15",
        "2023-09-19",
        "2023-10-02",
        "2023-10-24",
        "2023-11-14",
        "2023-11-27",
        "2023-12-25",
    ],
    2024: [
        "2024-01-22",
        "2024-01-26",
        "2024-03-08",
        "2024-03-25",
        "2024-03-29",
        "2024-04-11",
        "2024-04-17",
        "2024-05-01",
        "2024-05-20",
        "2024-06-17",
        "2024-07-17",
        "2024-08-15",
        "2024-10-02",
        "2024-11-01",
        "2024-11-15",
        "2024-11-20",
        "2024-12-25",
    ],
    2025: [
        "2025-01-26",
        "2025-02-26",
        "2025-03-14",
        "2025-03-26",
        "2025-03-31",
        "2025-04-10",
        "2025-04-14",
        "2025-04-18",
        "2025-04-21",
        "2025-05-01",
        "2025-06-06",
        "2025-08-15",
        "2025-08-27",
        "2025-10-02",
        "2025-10-20",
        "2025-10-22",
        "2025-11-01",
        "2025-11-04",
        "2025-11-05",
        "2025-11-14",
        "2025-12-25",
    ],
    2026: [
        "2026-01-15",
        "2026-01-26",
        "2026-03-03",
        "2026-03-26",
        "2026-03-31",
        "2026-04-03",
        "2026-04-14",
        "2026-05-01",
        "2026-05-28",
        "2026-06-26",
        "2026-09-14",
        "2026-10-02",
        "2026-10-20",
        "2026-11-10",
        "2026-11-24",
        "2026-12-25",
    ],
}


def sambhav_holiday_dates(from_date: date, to_date: date) -> Set[date]:
    """Return Sambhav holiday dates in [from_date, to_date] inclusive."""
    out: Set[date] = set()
    for year in range(from_date.year, to_date.year + 1):
        for s in SAMBHAV_NSE_HOLIDAYS.get(year, []):
            try:
                hd = date.fromisoformat(s)
            except ValueError:
                continue
            if from_date <= hd <= to_date:
                out.add(hd)
    return out


def merge_holiday_sets(*sets: Iterable[date]) -> Set[date]:
    out: Set[date] = set()
    for s in sets:
        out |= set(s)
    return out

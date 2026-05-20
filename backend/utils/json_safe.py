"""JSON-serializable conversions for DB row dicts."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any


def json_safe_value(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def json_safe_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: json_safe_value(v) for k, v in row.items()}

"""Single mapping from internal Tarang enums to user-visible labels.

Internal values may remain PAPER / LIVE / AUTO. Display strings never include PAPER.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

RECORD_FORWARD_TEST = "FORWARD_TEST"
RECORD_LIVE = "LIVE"
FILL_SIMULATED = "SIMULATED"
FILL_USER = "USER_ENTERED"
FILL_BROKER = "BROKER_VERIFIED"

RECORD_TYPE_LABELS = {
    RECORD_FORWARD_TEST: "Forward test",
    RECORD_LIVE: "Live",
    "PAPER": "Forward test",
}

FILL_SOURCE_LABELS = {
    FILL_SIMULATED: "Simulated",
    FILL_USER: "User entered",
    FILL_BROKER: "Broker verified",
}

MODE_BADGE = {
    "PAPER": "Forward test",
    "LIVE": "Live",
    RECORD_FORWARD_TEST: "Forward test",
    RECORD_LIVE: "Live",
}

AUTO_LOCK_LABEL = "Auto orders: locked"
CSV_RECORD_TYPE = "Record type"
CSV_FILL_SOURCE = "Fill source"
CSV_MODE = "Book"


def record_type_label(value: Optional[str]) -> str:
    key = str(value or RECORD_FORWARD_TEST).upper()
    return RECORD_TYPE_LABELS.get(key, "Forward test")


def fill_source_label(value: Optional[str]) -> str:
    key = str(value or FILL_SIMULATED).upper()
    return FILL_SOURCE_LABELS.get(key, key.replace("_", " ").title())


def mode_badge(mode: Optional[str] = None, record_type: Optional[str] = None) -> str:
    if record_type:
        return record_type_label(record_type)
    key = str(mode or "PAPER").upper()
    return MODE_BADGE.get(key, "Forward test")


def auto_lock_label() -> str:
    return AUTO_LOCK_LABEL


def decorate_trade(row: Dict[str, Any]) -> Dict[str, Any]:
    """Add display_* fields. Never put PAPER in display strings."""
    out = dict(row)
    rt = out.get("record_type") or (
        RECORD_LIVE if str(out.get("mode") or "").upper() == "LIVE" else RECORD_FORWARD_TEST
    )
    fs = out.get("fill_source") or FILL_SIMULATED
    out["record_type"] = rt
    out["fill_source"] = fs
    out["display_record_type"] = record_type_label(rt)
    out["display_fill_source"] = fill_source_label(fs)
    out["display_mode"] = mode_badge(out.get("mode"), rt)
    out["display_auto"] = auto_lock_label()
    if out.get("broker_verified"):
        out["display_verified"] = "Verified"
    return out


def assert_no_paper_in_display(s: str) -> None:
    if "PAPER" in str(s).upper() and "newspaper" not in str(s).lower():
        raise AssertionError(f"user-facing string contains PAPER: {s!r}")

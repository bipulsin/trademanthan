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

FRIENDLY_SYMBOLS = {
    "CL": {"name": "Crude Oil Mini", "contract": "CRUDEOILM", "code": "CL"},
    "NG": {"name": "Natural Gas Mini", "contract": "NATGASMINI", "code": "NG"},
    "BTC": {"name": "Bitcoin", "contract": "BTC", "code": "BTC"},
    "ETH": {"name": "Ether", "contract": "ETH", "code": "ETH"},
    "CRUDEOILM": {"name": "Crude Oil Mini", "contract": "CRUDEOILM", "code": "CL"},
    "NATGASMINI": {"name": "Natural Gas Mini", "contract": "NATGASMINI", "code": "NG"},
}

GATE_PLAIN = {
    "iv_percentile": "IV percentile is still warming up or below the bar",
    "iv_vs_rv": "IV is below realized volatility",
    "stale_data": "Quotes are stale",
    "chain": "Market data is missing",
    "two_sided_quotes": "No two-sided quotes",
    "expiry_dte": "Expiry is outside the allowed window",
    "event_blackout": "Event blackout is on",
    "credit_fraction": "Credit is too small versus the width",
    "sizing": "Size does not fit the risk budget",
    "portfolio_limit": "Portfolio risk cap would be exceeded",
    "liquidity": "Liquidity is too thin",
    "short_delta": "Short-option delta is outside the band",
    "fee_gate": "Fees would eat too much of the credit",
    "market_closed": "Market closed",
}

STRUCTURE_PLAIN = {
    "put_credit_spread": "Put credit spread",
    "call_credit_spread": "Call credit spread",
    "iron_condor": "Iron condor",
    "credit_spread": "Credit spread",
}


def friendly_symbol(profile_or_und: Optional[str]) -> Dict[str, str]:
    key = str(profile_or_und or "").upper()
    row = FRIENDLY_SYMBOLS.get(key) or {"name": key or "—", "contract": key, "code": key}
    code = row.get("code") or key
    contract = row.get("contract") or key
    name = row["name"]
    if name in ("Bitcoin", "Ether"):
        display = f"{code}"
    else:
        display = f"{name} ({contract}) / {code}"
    return {
        "profile_id": key,
        "display_name": display,
        "short_name": name,
        "contract": contract,
        "code": code,
    }


def gate_plain(name: Optional[str], detail: Optional[str] = None) -> str:
    key = str(name or "").strip()
    mapped = GATE_PLAIN.get(key)
    if mapped:
        return mapped
    if detail:
        return str(detail)
    return key.replace("_", " ") if key else "Watching"


def structure_plain(name: Optional[str]) -> str:
    key = str(name or "").strip().lower()
    return STRUCTURE_PLAIN.get(key, (name or "—").replace("_", " "))


def exit_reason_plain(name: Optional[str]) -> str:
    key = str(name or "").upper()
    return {
        "PROFIT_TARGET": "Target",
        "CREDIT_STOP": "Stop",
        "DELTA_STOP": "Stop",
        "BUDGET_STOP": "Stop",
        "IV_STOP": "Stop",
        "HARD_EXIT": "Time stop",
        "TIME_STOP": "Time stop",
        "EVENT_STOP": "Stop",
        "MANUAL": "Manual",
        "VOIDED": "Cancelled",
        "OTHER": "Other",
    }.get(key, (name or "—").replace("_", " ").title())


def format_inr(n: Any) -> str:
    try:
        v = int(round(float(n)))
    except (TypeError, ValueError):
        return "—"
    sign = "-" if v < 0 else ""
    s = str(abs(v))
    if len(s) <= 3:
        body = s
    else:
        last3 = s[-3:]
        rest = s[:-3]
        parts = []
        while len(rest) > 2:
            parts.append(rest[-2:])
            rest = rest[:-2]
        if rest:
            parts.append(rest)
        body = ",".join(reversed(parts)) + "," + last3
    return f"{sign}₹{body}"


def format_opt_px(n: Any) -> str:
    try:
        return f"{float(n):.2f}"
    except (TypeError, ValueError):
        return "—"


def format_usd(n: Any) -> str:
    try:
        return f"${float(n):.2f}"
    except (TypeError, ValueError):
        return "—"


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
    from backend.services.tarang.calendar import parse_to_ist_str

    out["entry_at_ist"] = parse_to_ist_str(out.get("entry_at"))
    out["exit_at_ist"] = parse_to_ist_str(out.get("exit_at"))
    out["created_at_ist"] = parse_to_ist_str(out.get("created_at"))
    fs = friendly_symbol(out.get("profile_id") or (out.get("meta") or {}).get("underlying"))
    out["display_symbol"] = fs["display_name"]
    out["display_structure"] = structure_plain(out.get("structure"))
    out["fills_confirmed"] = bool(out.get("fills_confirmed")) if out.get("fills_confirmed") is not None else (
        str(out.get("record_type") or "").upper() != RECORD_LIVE
    )
    if out.get("voided"):
        out["display_status"] = "Cancelled"
    return out


def assert_no_paper_in_display(s: str) -> None:
    if "PAPER" in str(s).upper() and "newspaper" not in str(s).lower():
        raise AssertionError(f"user-facing string contains PAPER: {s!r}")

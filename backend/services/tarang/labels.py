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
    "structure": "Could not build the option structure",
}


def _fmt_num(n: Any, *, pct: bool = False, digits: int = 2) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return "—"
    if pct:
        return f"{v * 100:.1f}%"
    if abs(v) >= 100:
        return f"{v:.0f}"
    return f"{v:.{digits}f}"


def _fmt_threshold_band(threshold: Any) -> str:
    if isinstance(threshold, dict):
        lo = threshold.get("min")
        hi = threshold.get("max")
        if lo is not None and hi is not None:
            return f"{lo}–{hi}"
        if lo is not None:
            return f"≥ {lo}"
        if hi is not None:
            return f"≤ {hi}"
        if "min_percentile" in threshold:
            return f"≥ {_fmt_num(threshold.get('min_percentile'), digits=0)}"
        if "max_spread_pct_of_mid" in threshold:
            oi = threshold.get("min_oi")
            return f"spread ≤ {_fmt_num(threshold.get('max_spread_pct_of_mid'), digits=0)}% of mid · OI ≥ {_fmt_num(oi, digits=0)}"
        if "hours_before" in threshold:
            return f"clear of events within {_fmt_num(threshold.get('hours_before'), digits=1)}h"
        if "open" in threshold or "projected" in threshold:
            return f"≤ {format_inr(threshold.get('threshold') or threshold.get('cap'))}"
    if isinstance(threshold, (int, float)):
        # Relative mins often 0.10 / 0.20
        if 0 < float(threshold) < 1:
            return f"≥ {_fmt_num(threshold, pct=True)}"
        return f"≥ {_fmt_num(threshold)}"
    if threshold is None:
        return "—"
    return str(threshold)


def format_gate_observed_accepted(gate: Dict[str, Any]) -> tuple[str, str]:
    """Return (observed, accepted) display strings for a gate payload dict."""
    name = str(gate.get("name") or "").strip()
    actual = gate.get("actual")
    threshold = gate.get("threshold")
    detail = str(gate.get("detail") or "").strip()

    if name == "iv_vs_rv":
        if isinstance(actual, dict):
            rel = actual.get("relative")
            atm = actual.get("atm_iv")
            rv = actual.get("rv_20d")
            if rel is not None:
                obs = f"IV vs RV {_fmt_num(rel, pct=True)} (IV {_fmt_num(atm)} · RV {_fmt_num(rv)})"
            else:
                obs = f"IV {_fmt_num(atm)} · RV {_fmt_num(rv)}"
        else:
            obs = detail or "—"
        acc = f"≥ {_fmt_num(threshold, pct=True)}" if threshold is not None else "≥ +10%"
        return obs, acc

    if name == "iv_percentile":
        if isinstance(actual, dict):
            p = actual.get("percentile")
            snaps = actual.get("snapshots")
            obs = f"percentile {_fmt_num(p, digits=1)}" + (f" · {snaps} snapshots" if snaps is not None else "")
        else:
            obs = _fmt_num(actual, digits=1) if actual is not None else (detail or "—")
        if isinstance(threshold, dict):
            acc = f"≥ {_fmt_num(threshold.get('min_percentile'), digits=0)}"
        else:
            acc = _fmt_threshold_band(threshold)
        return obs, acc

    if name == "expiry_dte":
        obs = f"DTE {_fmt_num(actual, digits=0)}" if actual is not None else (detail or "DTE unknown")
        acc = f"allowed {_fmt_threshold_band(threshold)}" if threshold is not None else "—"
        return obs, acc

    if name == "credit_fraction":
        obs = _fmt_num(actual, pct=True) if actual is not None else (detail or "—")
        acc = f"≥ {_fmt_num(threshold, pct=True)}" if threshold is not None else "≥ 20%"
        return obs, acc

    if name == "short_delta":
        try:
            ad = abs(float(actual)) if actual is not None else None
        except (TypeError, ValueError):
            ad = None
        obs = f"|δ| {_fmt_num(ad, digits=3)}" if ad is not None else (detail or "—")
        acc = f"band {_fmt_threshold_band(threshold)}" if threshold is not None else "—"
        return obs, acc

    if name == "liquidity":
        if isinstance(actual, dict) and actual.get("fails"):
            fails = actual.get("fails") or []
            obs = "; ".join(str(x) for x in fails[:2]) or "thin"
        else:
            obs = "ok" if actual == "ok" else (detail or str(actual or "—"))
        acc = _fmt_threshold_band(threshold) if threshold is not None else "tight spreads · enough OI"
        return obs, acc

    if name == "two_sided_quotes":
        if isinstance(actual, dict) and actual.get("fails"):
            obs = "; ".join(str(x) for x in (actual.get("fails") or [])[:2])
        else:
            obs = "ok" if actual == "ok" else (detail or "—")
        return obs, "live bid and ask on every leg"

    if name == "sizing":
        if isinstance(actual, dict):
            units = actual.get("units")
            ml = actual.get("max_loss_per_unit")
            total = actual.get("total")
            obs = f"{units or 0} lot(s) · max loss {format_inr(total if total is not None else ml)}"
        else:
            obs = detail or "—"
        acc = f"budget ≤ {format_inr(threshold)}" if threshold is not None else "within risk budget"
        return obs, acc

    if name == "portfolio_limit":
        if isinstance(actual, dict):
            obs = f"projected {format_inr(actual.get('projected'))}"
        else:
            obs = detail or "—"
        acc = f"cap ≤ {format_inr(threshold)}" if threshold is not None else "within portfolio cap"
        return obs, acc

    if name == "stale_data":
        obs = f"{_fmt_num(actual, digits=0)}s old" if isinstance(actual, (int, float)) else (detail or str(actual or "—"))
        acc = f"≤ {_fmt_num(threshold, digits=0)}s" if threshold is not None else "fresh quotes"
        return obs, acc

    if name == "structure":
        obs = str(actual or detail or "build failed")
        return obs, "valid credit structure on chain"

    if name == "event_blackout":
        obs = detail if detail and detail != "no blackout" else (str(actual) if actual not in (None, "clear") else "clear")
        acc = _fmt_threshold_band(threshold) if threshold is not None else "outside event blackout"
        return obs, acc

    if name == "fee_gate":
        if isinstance(actual, dict):
            frac = actual.get("frac")
            net = actual.get("net_per_contract_inr")
            parts = []
            if frac is not None:
                parts.append(f"fees {_fmt_num(frac, pct=True)} of credit")
            if net is not None:
                parts.append(f"net {format_inr(net)}/contract")
            obs = detail or (" · ".join(parts) if parts else "—")
        else:
            obs = detail or str(actual or "—")
        if isinstance(threshold, dict):
            max_frac = threshold.get("max_fees_frac_of_credit")
            min_net = threshold.get("min_net_credit_per_contract_inr")
            bits = []
            if max_frac is not None:
                bits.append(f"fees ≤ {_fmt_num(max_frac, pct=True)} of credit")
            if min_net is not None:
                bits.append(f"net ≥ {format_inr(min_net)}/contract")
            acc = " · ".join(bits) if bits else "fees within credit"
        else:
            acc = "fees within credit"
        return obs, acc

    # Generic fallback
    if isinstance(actual, dict):
        obs = detail or ", ".join(f"{k}={v}" for k, v in list(actual.items())[:3])
    elif actual is None:
        obs = detail or "—"
    else:
        obs = str(actual)
    acc = _fmt_threshold_band(threshold)
    return obs, acc


def gate_check_row(gate: Dict[str, Any]) -> Dict[str, Any]:
    """Structured Why? row: label + observed/accepted + pass/fail/neutral."""
    name = str(gate.get("name") or "").strip()
    evaluability = str(gate.get("evaluability") or "").lower()
    status_hint = str(gate.get("status_hint") or "").upper()
    if evaluability == "not_evaluable" or status_hint == "NOT_EVALUABLE":
        outcome = "neutral"
    elif bool(gate.get("passed")) and evaluability != "failed":
        outcome = "pass"
    else:
        outcome = "fail"

    if name in GATE_PLAIN:
        label = GATE_PLAIN[name]
        if name == "structure" and gate.get("detail"):
            label = str(gate.get("detail"))
        elif name == "iv_percentile" and status_hint == "WARMING_UP":
            label = "IV percentile is still warming up"
    else:
        label = gate_plain(name, gate.get("detail"))

    observed, accepted = format_gate_observed_accepted(gate)
    return {
        "name": name,
        "label": label,
        "passed": outcome == "pass",
        "outcome": outcome,
        "observed": observed,
        "accepted": accepted,
        "text": f"{label} · observed {observed} · need {accepted}",
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

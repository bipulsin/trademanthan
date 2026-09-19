"""Tradable-expiry table: DTE vs entry minimum, two-sided quotes required."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from backend.services.tarang.chain_builder import ChainBuilder
from backend.services.tarang.config import get_profiles

IST = ZoneInfo("Asia/Kolkata")


def _dte(expiry_iso: str, today: date) -> Optional[int]:
    try:
        return (date.fromisoformat(str(expiry_iso)[:10]) - today).days
    except ValueError:
        return None


def expiry_eligibility(*, today: Optional[date] = None, builder: Optional[ChainBuilder] = None) -> Dict[str, Any]:
    """Per-underlying listed expiries vs the 7-day (or profile) entry rule."""
    today = today or datetime.now(timezone.utc).astimezone(IST).date()
    builder = builder or ChainBuilder()
    profiles = get_profiles().get("profiles") or {}
    rows: List[Dict[str, Any]] = []
    for pid, prof in profiles.items():
        if not prof.get("enabled"):
            continue
        min_dte = prof.get("expiry_min_dte") if prof.get("expiry_min_dte") is not None else prof.get("expiry_dte_min")
        min_dte_i = int(min_dte) if min_dte is not None else None
        max_dte = prof.get("expiry_dte_max")
        max_dte_i = int(max_dte) if max_dte is not None else None
        time_stop = int(prof.get("time_stop_dte") or (5 if prof.get("venue") == "upstox_mcx" else 1))
        try:
            listed = builder.snapshot_expiries(pid)
        except Exception as e:
            rows.append(
                {
                    "profile_id": pid,
                    "underlying": prof.get("underlying_symbol"),
                    "error": str(e)[:160],
                    "tradable": False,
                }
            )
            continue
        for exp in listed:
            dte = _dte(exp, today)
            reasons: List[str] = []
            tradable = True
            if dte is None:
                tradable = False
                reasons.append("bad_expiry")
            else:
                if dte < 0:
                    tradable = False
                    reasons.append("expired")
                if min_dte_i is not None and dte < min_dte_i:
                    tradable = False
                    reasons.append(f"dte_{dte}_below_min_{min_dte_i}")
                if max_dte_i is not None and dte > max_dte_i:
                    tradable = False
                    reasons.append(f"dte_{dte}_above_max_{max_dte_i}")
            note = None
            if str(prof.get("underlying_symbol") or "").upper() in ("NATGASMINI", "NATURALGAS") and exp[:10] == "2026-09-23":
                note = "NG 23 Sep is inside the 7-day rule exclusion (DTE < 7)."
            rows.append(
                {
                    "profile_id": pid,
                    "underlying": prof.get("underlying_symbol"),
                    "venue": prof.get("venue"),
                    "expiry": exp,
                    "dte": dte,
                    "min_dte": min_dte_i,
                    "max_dte": max_dte_i,
                    "time_stop_dte": time_stop,
                    "tradable": tradable,
                    "reasons": reasons,
                    "note": note,
                    "requires_two_sided_quotes": True,
                    "model_prices_not_tradable": True,
                }
            )
    return {
        "asof": today.isoformat(),
        "rule": "Never trade an expiry with DTE below profile min (MCX 7). Never trade without two-sided quotes. Black-76 fills Greeks only, never prices.",
        "rows": rows,
    }

"""Kosmic Tarang config loader (profiles / risk / events)."""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

_LOCK = threading.Lock()
_CACHE: Dict[str, Any] = {}

_CONFIG_DIR = Path(__file__).resolve().parent / "config_data"


def _load_json(name: str) -> Dict[str, Any]:
    path = _CONFIG_DIR / name
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def get_profiles() -> Dict[str, Any]:
    with _LOCK:
        if "profiles" not in _CACHE:
            _CACHE["profiles"] = _load_json("profiles.default.json")
        return _CACHE["profiles"]


def get_risk() -> Dict[str, Any]:
    with _LOCK:
        if "risk" not in _CACHE:
            _CACHE["risk"] = _load_json("risk.default.json")
        return _CACHE["risk"]


def get_events() -> Dict[str, Any]:
    with _LOCK:
        if "events" not in _CACHE:
            _CACHE["events"] = _load_json("events.default.json")
        return _CACHE["events"]


def energy_budget() -> Dict[str, Any]:
    return dict((get_risk().get("buckets") or {}).get("ENERGY") or {})


def crypto_budget() -> Dict[str, Any]:
    return dict((get_risk().get("buckets") or {}).get("CRYPTO") or {})


def contract_family() -> str:
    fam = str((get_profiles().get("contractFamily") or "mini")).strip().lower()
    return fam if fam in ("mini", "full") else "mini"


def resolve_energy_underlying(profile_id: str, profile: Optional[Dict[str, Any]] = None) -> str:
    """Map CL/NG to mini or full underlying; honour explicit underlying_symbol."""
    pid = (profile_id or "").upper()
    prof = profile if profile is not None else (get_profiles().get("profiles") or {}).get(pid) or {}
    explicit = str(prof.get("underlying_symbol") or "").strip().upper()
    fam = str(prof.get("contract_family") or contract_family()).strip().lower()
    maps = get_profiles()
    table = (maps.get("mini_underlyings") if fam != "full" else maps.get("full_underlyings")) or {}
    mapped = str(table.get(pid) or "").strip().upper()
    if fam == "full":
        return mapped or explicit or pid
    return explicit or mapped or pid


def validate_profiles() -> List[str]:
    """Entry min DTE must exceed time-stop DTE by at least 2."""
    errors: List[str] = []
    profiles = (get_profiles().get("profiles") or {})
    for pid, p in profiles.items():
        if not p.get("enabled"):
            continue
        min_dte = p.get("expiry_min_dte") if p.get("expiry_min_dte") is not None else p.get("expiry_dte_min")
        ts = p.get("time_stop_dte")
        if min_dte is None or ts is None:
            errors.append(f"{pid}: missing expiry_min_dte/time_stop_dte")
            continue
        if int(min_dte) - int(ts) < 2:
            errors.append(
                f"{pid}: expiry min DTE {min_dte} must exceed time_stop_dte {ts} by at least 2"
            )
    return errors

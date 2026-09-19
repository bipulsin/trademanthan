"""Kosmic Tarang config loader (profiles / risk / events)."""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict

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


def reload_config() -> None:
    with _LOCK:
        _CACHE.clear()

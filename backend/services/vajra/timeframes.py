"""Vajra scan / HTF timeframe definitions and validation."""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

# UI token -> config
SCAN_TF_IDS = ("5m", "15m", "30m", "1hr", "1d")
HTF_IDS = ("1hr", "1d", "1w")

DEFAULT_SCAN_TF = "30m"
DEFAULT_HTF = "1d"

_TF_MINUTES: Dict[str, int] = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1hr": 60,
    "1d": 1440,
    "1w": 10080,
}

# Upstox interval + history depth for stable indicators (~80+ bars)
_TF_FETCH: Dict[str, Dict[str, object]] = {
    "5m": {"interval": "minutes/5", "days_back": 8},
    "15m": {"interval": "minutes/15", "days_back": 12},
    "30m": {"interval": "minutes/30", "days_back": 20},
    "1hr": {"interval": "hours/1", "days_back": 92},
    "1d": {"interval": "days/1", "days_back": 120},
    "1w": {"interval": "weeks/1", "days_back": 730},
}

MIN_SCAN_BARS = 60


def tf_minutes(tf_id: str) -> int:
    key = (tf_id or "").strip().lower()
    if key not in _TF_MINUTES:
        raise ValueError(f"Unknown timeframe: {tf_id}")
    return _TF_MINUTES[key]


def normalize_scan_tf(value: Optional[str]) -> str:
    v = (value or DEFAULT_SCAN_TF).strip().lower()
    if v in _TF_MINUTES and v in SCAN_TF_IDS:
        return v
    raise ValueError(f"Invalid scan_tf: {value}")


def normalize_htf(value: Optional[str]) -> str:
    v = (value or DEFAULT_HTF).strip().lower()
    if v in _TF_MINUTES and v in HTF_IDS:
        return v
    raise ValueError(f"Invalid htf: {value}")


def valid_htf_for_scan(scan_tf: str) -> List[str]:
    sm = tf_minutes(scan_tf)
    return [h for h in HTF_IDS if tf_minutes(h) > sm]


def validate_tf_pair(scan_tf: str, htf: str) -> Tuple[str, str]:
    s = normalize_scan_tf(scan_tf)
    h = normalize_htf(htf)
    if tf_minutes(h) <= tf_minutes(s):
        raise ValueError(f"HTF ({h}) must be higher than Scan TF ({s})")
    if h not in valid_htf_for_scan(s):
        raise ValueError(f"HTF {h} is not allowed for scan_tf {s}")
    return s, h


def fetch_config(tf_id: str) -> Dict[str, object]:
    v = (tf_id or "").strip().lower()
    if v not in _TF_FETCH:
        raise ValueError(f"Unknown timeframe: {tf_id}")
    return dict(_TF_FETCH[v])

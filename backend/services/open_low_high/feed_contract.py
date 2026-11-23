"""Upstox WS feed-type contract: classify, count unknowns, alert on subscribed misses.

Handled feed unions (MarketDataFeedV3 ``Feed`` oneof + ``FullFeed`` oneof):
  - fullFeed.marketFF  → NSE_EQ / NSE_FO (MarketFullFeed)
  - fullFeed.indexFF   → NSE_INDEX (IndexFullFeed) — fixed 2026-11-23
  - ltpc               → LTPC-only mode
  - firstLevelWithGreeks → options greeks mode

Anything else is ``unknown`` and is counted per IST minute.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz

from backend.services.open_low_high.clock import olh_now
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

HANDLED_FEED_TYPES = frozenset(
    {
        "fullFeed.marketFF",
        "fullFeed.indexFF",
        "ltpc",
        "firstLevelWithGreeks",
    }
)

FIXTURES_PATH = Path(__file__).resolve().parent / "fixtures" / "ws_frames_schema.json"

_lock = threading.RLock()
# minute_key "HH:MM" -> {feed_type: count}
_unknown_per_minute: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
_handled_per_minute: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
# Subscribed keys that produced an unknown/empty parse this minute
_unknown_subscribed: Dict[str, Set[str]] = defaultdict(set)
_last_alert_minute: Optional[str] = None


def classify_feed_type(feed_val: Any) -> str:
    """Return canonical feed-type label for one Feed message dict."""
    if not isinstance(feed_val, dict):
        return "invalid"
    if "fullFeed" in feed_val or "full_feed" in feed_val:
        ff = feed_val.get("fullFeed") or feed_val.get("full_feed") or {}
        if not isinstance(ff, dict):
            return "fullFeed.invalid"
        if ff.get("marketFF") or ff.get("marketFf") or ff.get("market_ff"):
            return "fullFeed.marketFF"
        if ff.get("indexFF") or ff.get("indexFf") or ff.get("index_ff"):
            return "fullFeed.indexFF"
        # Unknown FullFeed union member
        keys = sorted(k for k in ff.keys() if k)
        return f"fullFeed.unknown:{','.join(keys) or 'empty'}"
    if "ltpc" in feed_val:
        return "ltpc"
    if "firstLevelWithGreeks" in feed_val or "first_level_with_greeks" in feed_val:
        return "firstLevelWithGreeks"
    keys = sorted(k for k in feed_val.keys() if k)
    if not keys:
        return "empty_feed"
    return f"unknown:{','.join(keys)}"


def is_handled(feed_type: str) -> bool:
    return feed_type in HANDLED_FEED_TYPES


def load_schema_fixtures() -> Dict[str, Any]:
    with FIXTURES_PATH.open() as f:
        return json.load(f)


def handled_feed_types_report() -> Dict[str, Any]:
    fixtures = load_schema_fixtures()
    meta = fixtures.get("_meta") or {}
    frames = fixtures.get("frames") or []
    by_type: Dict[str, List[str]] = defaultdict(list)
    for fr in frames:
        ft = classify_feed_type(fr.get("feed"))
        by_type[ft].append(str(fr.get("label") or ""))
    return {
        "handled": sorted(HANDLED_FEED_TYPES),
        "meta_handled": meta.get("handled_feed_types"),
        "raw_frames_recorded": bool(meta.get("raw_frames_recorded")),
        "fixture_labels_by_type": dict(by_type),
        "note": meta.get("note"),
    }


def note_feed_observation(
    instrument_key: str,
    feed_val: Any,
    *,
    subscribed_keys: Optional[Set[str]] = None,
    now: Optional[datetime] = None,
) -> str:
    """Classify + count. Alert once/minute if unknown for a subscribed key."""
    ft = classify_feed_type(feed_val)
    t = now or olh_now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    minute_key = t.strftime("%H:%M")
    ik = str(instrument_key or "").strip().replace(":", "|")
    with _lock:
        if is_handled(ft):
            _handled_per_minute[minute_key][ft] += 1
        else:
            _unknown_per_minute[minute_key][ft] += 1
            sub = subscribed_keys or set()
            if ik and (not sub or ik in sub):
                _unknown_subscribed[minute_key].add(ik)
                _maybe_alert_unknown(minute_key, ik, ft)
    return ft


def _maybe_alert_unknown(minute_key: str, ik: str, ft: str) -> None:
    global _last_alert_minute
    if _last_alert_minute == minute_key:
        return
    _last_alert_minute = minute_key
    detail = f"unknown_feed minute={minute_key} ik={ik} type={ft}"
    logger.warning("olh feed_contract: %s", detail)
    ohl_log("failed", action="unknown_feed_type", symbol=ik, source="ws", detail=detail[:200])
    try:
        from backend.services.open_low_high.alerts import send_olh_alert

        send_olh_alert(
            f"Unknown WS feed type {minute_key}",
            f"instrument={ik} feed_type={ft}",
            meta={"minute": minute_key, "feed_type": ft},
        )
    except Exception:
        logger.debug("unknown feed alert failed", exc_info=True)


def unknown_counts_snapshot(minute_key: Optional[str] = None) -> Dict[str, Any]:
    with _lock:
        if minute_key:
            return {
                "minute": minute_key,
                "unknown": dict(_unknown_per_minute.get(minute_key) or {}),
                "handled": dict(_handled_per_minute.get(minute_key) or {}),
                "unknown_subscribed_keys": sorted(_unknown_subscribed.get(minute_key) or []),
            }
        return {
            "unknown_by_minute": {m: dict(v) for m, v in _unknown_per_minute.items()},
            "handled_by_minute": {m: dict(v) for m, v in _handled_per_minute.items()},
            "unknown_subscribed_by_minute": {
                m: sorted(s) for m, s in _unknown_subscribed.items()
            },
        }


def reset_feed_contract_for_tests() -> None:
    global _last_alert_minute
    with _lock:
        _unknown_per_minute.clear()
        _handled_per_minute.clear()
        _unknown_subscribed.clear()
        _last_alert_minute = None


def parse_fixture_to_ohlc(
    feed_val: Any,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[int], Optional[int]]:
    """Extract (o,h,l,c, oi, volume) from a fixture feed for contract persistence tests.

    Prefers I1 bar when present; else LTP as all four OHLC legs. OI/volume nullable.
    """
    from backend.services.upstox_market_feed import (
        _extract_feed_fields,
        _extract_oi_ltp_from_feed_value,
    )

    fields = _extract_feed_fields(feed_val)
    oi = fields.get("oi")
    ltp = fields.get("ltp")
    i1 = (fields.get("i1_bars") or [None])[0]
    if i1:
        return (
            float(i1["open"]),
            float(i1["high"]),
            float(i1["low"]),
            float(i1["close"]),
            oi if oi is not None else None,
            int(i1["volume"]) if i1.get("volume") is not None else None,
        )
    if ltp is None:
        oi2, ltp2 = _extract_oi_ltp_from_feed_value(feed_val)
        oi = oi if oi is not None else oi2
        ltp = ltp2
    if ltp is None:
        return None, None, None, None, oi, None
    v = float(ltp)
    return v, v, v, v, oi, None

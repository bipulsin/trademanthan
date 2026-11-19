"""Open Low High Upstox WS subscribe (equity + index + future) and 09:15–09:25 lock.

Lock behavior (IST Mon–Fri window 09:15–09:25):
  - OLH registers a named feed provider ``open_low_high`` with NSE_EQ + NSE_INDEX
    (Nifty 50) + front-month FUT keys via ``set_feed_provider_keys``.
  - While locked, EQ/INDEX keys snapshotted at engage are **always** re-merged into
    the provider set — FO-only re-subscribe cannot drop them from this provider.
  - ``subscribe_olh_ws`` refuses to clear or replace without locked EQ/INDEX keys.
  - Lock releases at 09:25 (subscriptions may remain for Phase B; unlock only lifts
    the force-merge guard).
"""
from __future__ import annotations

import logging
import threading
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from sqlalchemy import text

from backend.services.open_high_low.config import NIFTY_INDEX_KEY
from backend.services.open_low_high.ohl_log import ohl_log

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

PROVIDER_NAME = "open_low_high"
LOCK_START = time(9, 15, 0)
LOCK_END = time(9, 25, 0)

_lock = threading.RLock()
_locked_eq_index: Set[str] = set()
_lock_engaged: bool = False
_last_subscribe: Dict[str, Any] = {}


def _now() -> datetime:
    return datetime.now(IST)


def _normalize_ik(raw: str) -> str:
    return str(raw or "").strip().replace(":", "|")


def in_ws_lock_window(now: Optional[datetime] = None) -> bool:
    t = now or _now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    return LOCK_START <= tt < LOCK_END


def is_ws_lock_engaged() -> bool:
    with _lock:
        return bool(_lock_engaged)


def collect_olh_instrument_keys(
    *,
    session: Optional[date] = None,
) -> Dict[str, List[str]]:
    """Return ``{equity, index, future, all}`` instrument key lists for OLH universe."""
    from backend.services.open_high_low.data import load_universe
    from backend.services.open_low_high.screener import _resolve_equity_key, _upstox

    _ = session  # reserved for future session-scoped universe
    upstox = _upstox()
    universe = load_universe()
    equity: List[str] = []
    future: List[str] = []
    for row in universe:
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        eq = _normalize_ik(_resolve_equity_key(upstox, row, sym))
        if eq.upper().startswith("NSE_EQ|"):
            equity.append(eq)
        fut = _normalize_ik(str(row.get("currmth_key") or ""))
        if fut.upper().startswith("NSE_FO|") or fut:
            if fut:
                future.append(fut)
    index = [_normalize_ik(NIFTY_INDEX_KEY)]
    # Preserve order, drop dupes.
    def _uniq(keys: List[str]) -> List[str]:
        return list(dict.fromkeys(k for k in keys if k))

    equity_u = _uniq(equity)
    index_u = _uniq(index)
    future_u = _uniq(future)
    all_keys = _uniq(equity_u + index_u + future_u)
    return {
        "equity": equity_u,
        "index": index_u,
        "future": future_u,
        "all": all_keys,
    }


def engage_ws_lock(*, keys: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
    """Snapshot EQ+INDEX keys and engage the 09:15–09:25 drop-protection lock."""
    global _lock_engaged, _locked_eq_index
    bundles = keys or collect_olh_instrument_keys()
    eq_idx = set(bundles.get("equity") or []) | set(bundles.get("index") or [])
    with _lock:
        _locked_eq_index = set(eq_idx)
        _lock_engaged = True
    ohl_log(
        "success",
        action="ws_lock_engage",
        detail=f"eq_index={len(eq_idx)} lock={LOCK_START.isoformat()}-{LOCK_END.isoformat()}",
    )
    return {
        "ok": True,
        "engaged": True,
        "locked_eq_index": len(eq_idx),
        "window": f"{LOCK_START.strftime('%H:%M')}-{LOCK_END.strftime('%H:%M')} IST",
    }


def release_ws_lock() -> Dict[str, Any]:
    global _lock_engaged
    with _lock:
        _lock_engaged = False
        locked_n = len(_locked_eq_index)
    ohl_log("success", action="ws_lock_release", detail=f"was_locked_eq_index={locked_n}")
    return {"ok": True, "engaged": False, "was_locked_eq_index": locked_n}


def _provider_keys_with_lock(desired: List[str]) -> List[str]:
    with _lock:
        locked = set(_locked_eq_index) if _lock_engaged else set()
    merged = list(dict.fromkeys([_normalize_ik(k) for k in desired if k] + sorted(locked)))
    return merged


def subscribe_olh_ws(
    *,
    include_future: bool = True,
    engage_lock: Optional[bool] = None,
) -> Dict[str, Any]:
    """Subscribe Upstox WS for Equity + Index (+ Future). Honors WS lock merge."""
    from backend.services.upstox_market_feed import set_feed_provider_keys

    bundles = collect_olh_instrument_keys()
    keys = list(bundles["equity"]) + list(bundles["index"])
    if include_future:
        keys.extend(bundles["future"])
    if engage_lock is True or (engage_lock is None and in_ws_lock_window()):
        engage_ws_lock(keys=bundles)
    final = _provider_keys_with_lock(keys)
    try:
        set_feed_provider_keys(PROVIDER_NAME, final)
        outcome = "success"
        detail = (
            f"equity={len(bundles['equity'])} index={len(bundles['index'])} "
            f"future={len(bundles['future']) if include_future else 0} "
            f"subscribed={len(final)} lock={is_ws_lock_engaged()}"
        )
    except Exception as exc:
        outcome = "failed"
        detail = str(exc)[:200]
        ohl_log(outcome, action="ws_subscribe", source="ws", detail=detail)
        raise
    payload = {
        "ok": outcome == "success",
        "status": "subscribed",
        "equity": len(bundles["equity"]),
        "index": len(bundles["index"]),
        "future": len(bundles["future"]) if include_future else 0,
        "subscribed": len(final),
        "lock_engaged": is_ws_lock_engaged(),
        "provider": PROVIDER_NAME,
    }
    global _last_subscribe
    _last_subscribe = dict(payload)
    _last_subscribe["at"] = _now().isoformat()
    ohl_log(outcome, action="ws_subscribe", source="ws", detail=detail)
    return payload


def _ws_1m_counts(instrument_keys: List[str], session: date) -> Dict[str, int]:
    """How many 1m WS rows exist per key for the session morning."""
    from backend.database import SessionLocal

    if not instrument_keys:
        return {}
    start = IST.localize(datetime(session.year, session.month, session.day, 9, 0, 0))
    end = start + timedelta(hours=1)
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT instrument_key, COUNT(*) AS n
                FROM upstox_ws_intraday_1m
                WHERE instrument_key = ANY(:keys)
                  AND candle_time >= :start
                  AND candle_time < :end
                GROUP BY instrument_key
                """
            ),
            {"keys": list(instrument_keys), "start": start, "end": end},
        ).fetchall()
        return {str(r[0]): int(r[1] or 0) for r in rows}
    except Exception:
        # SQLite / non-ARRAY fallback: count individually (small index set).
        out: Dict[str, int] = {}
        for ik in instrument_keys:
            try:
                n = db.execute(
                    text(
                        """
                        SELECT COUNT(*) FROM upstox_ws_intraday_1m
                        WHERE instrument_key = :ik
                          AND candle_time >= :start
                          AND candle_time < :end
                        """
                    ),
                    {"ik": ik, "start": start, "end": end},
                ).scalar()
                out[ik] = int(n or 0)
            except Exception:
                out[ik] = 0
        return out
    finally:
        db.close()


def check_equity_index_ws_warm(
    *,
    session: Optional[date] = None,
    min_equity_share: float = 0.15,
) -> Dict[str, Any]:
    """Verify EQ/INDEX appear on the OLH provider and have some WS 1m rows.

    ``warm`` is True when Nifty is subscribed and a non-trivial share of equity
    keys already have ≥1 WS 1m candle (or feed thread is up with keys registered
    pre-open when the session has not printed bars yet).
    """
    from backend.services.upstox_market_feed import feed_status

    trade_date = session or _now().date()
    bundles = collect_olh_instrument_keys(session=trade_date)
    equity = bundles["equity"]
    index = bundles["index"]
    status = {}
    try:
        status = feed_status() or {}
    except Exception:
        status = {}
    provider_n = int((status.get("provider_keys") or {}).get(PROVIDER_NAME) or 0)
    feed_alive = bool(status.get("thread_alive") or provider_n > 0)

    sample_eq = equity[: min(80, len(equity))]
    counts = _ws_1m_counts(sample_eq + index, trade_date)
    eq_with = sum(1 for ik in sample_eq if counts.get(ik, 0) > 0)
    nifty_n = sum(counts.get(ik, 0) for ik in index)
    sample_n = max(len(sample_eq), 1)
    eq_share = float(eq_with) / float(sample_n)
    # Pre-09:15: bars may be empty — treat provider registration + alive feed as warm.
    tt = _now().time()
    if tt < time(9, 15):
        warm = feed_alive and provider_n > 0 and len(equity) > 0 and len(index) > 0
        reason = "preopen_provider"
    else:
        warm = (
            feed_alive
            and provider_n > 0
            and len(index) > 0
            and (nifty_n > 0 or eq_share >= float(min_equity_share))
        )
        reason = "bars" if warm else "insufficient_ws_bars"

    payload = {
        "ok": True,
        "warm": bool(warm),
        "reason": reason,
        "equity_keys": len(equity),
        "index_keys": len(index),
        "provider_keys": provider_n,
        "feed_alive": feed_alive,
        "equity_sample": len(sample_eq),
        "equity_with_bars": eq_with,
        "equity_share": round(eq_share, 4),
        "nifty_1m_bars": nifty_n,
        "lock_engaged": is_ws_lock_engaged(),
    }
    ohl_log(
        "success" if warm else "failed",
        action="ws_warm_check",
        source="ws",
        detail=(
            f"warm={warm} reason={reason} eq_share={eq_share:.2%} "
            f"nifty_bars={nifty_n} provider={provider_n}"
        ),
    )
    return payload


def prewarm_ws(*, engage_lock: bool = False) -> Dict[str, Any]:
    """09:05 (and 09:15 re-warm): subscribe Equity / Index / Future on Upstox WS."""
    sub = subscribe_olh_ws(include_future=True, engage_lock=engage_lock)
    warm = check_equity_index_ws_warm()
    payload = {
        "ok": bool(sub.get("ok")),
        "status": "prewarmed_ws",
        "subscribe": sub,
        "warm": warm,
        "message": (
            f"WS pre-warm equity={sub.get('equity')} index={sub.get('index')} "
            f"future={sub.get('future')} warm={warm.get('warm')}"
        ),
    }
    ohl_log(
        "success" if payload["ok"] else "failed",
        action="ws_prewarm",
        source="ws",
        detail=payload["message"],
    )
    return payload


def ensure_equity_index_warm_or_rewarm(*, session: Optional[date] = None) -> Dict[str, Any]:
    """09:15 check: engage lock, verify warm, re-subscribe if not."""
    engage_ws_lock()
    subscribe_olh_ws(include_future=True, engage_lock=True)
    warm = check_equity_index_ws_warm(session=session)
    rewarmed = False
    if not warm.get("warm"):
        ohl_log("failed", action="ws_warm_check", source="ws", detail="rewarming")
        prewarm_ws(engage_lock=True)
        warm = check_equity_index_ws_warm(session=session)
        rewarmed = True
    return {
        "ok": True,
        "rewarmed": rewarmed,
        "warm": warm,
        "lock_engaged": is_ws_lock_engaged(),
        "status": "warm" if warm.get("warm") else "not_warm",
    }


def get_last_subscribe() -> Dict[str, Any]:
    return dict(_last_subscribe)

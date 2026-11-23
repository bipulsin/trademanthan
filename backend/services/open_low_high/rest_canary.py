"""Timed REST 5m canary for OLH (3 equities + Nifty) — every trading day.

Fires at configured IST clocks (Mon–Fri; holiday-aware when NSE calendar is
available), logs status / headers / body[:300] / raw count before+after
``_drop_unclosed_bars`` / latest candle ts. Uses the dedicated **canary**
candle budget (never Phase A/B). Fail-fast: 09:20:05 empty/fail for Nifty
alerts immediately.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# Legacy one-day pin (kept for docs / migrations); canaries now run every trading day.
CANARY_DATE = date(2026, 11, 20)
CANARY_TIMES: Tuple[time, ...] = (
    time(9, 20, 5),
    time(9, 20, 30),
    time(9, 21, 0),
    time(9, 25, 5),
    time(9, 25, 30),
)
CANARY_CLOCKS = CANARY_TIMES
DEFAULT_SYMBOLS = ("RELIANCE", "TCS", "INFY")


def canary_should_run_today(now: Optional[datetime] = None) -> bool:
    """True on NSE trading days (Mon–Fri minus known holidays)."""
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    try:
        from backend.services.open_high_low.data import is_trading_day

        return bool(is_trading_day(t.date()))
    except Exception:
        return t.weekday() < 5


def run_rest_canary(
    *,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    session: Optional[date] = None,
    label: str = "",
) -> Dict[str, Any]:
    """Fetch REST 5m for equities + Nifty; log results. Safe to call any time."""
    from backend.services.open_high_low.config import NIFTY_INDEX_KEY
    from backend.services.open_high_low.data import load_universe
    from backend.services.open_low_high.candle_budget import olh_candle_pool
    from backend.services.open_low_high.ohl_log import ohl_log
    from backend.services.open_low_high.screener import (
        _bars_from_rows,
        _drop_unclosed_bars,
        _resolve_equity_key,
        _token_tail,
        _today,
        _upstox,
    )

    trade_date = session or _today()
    now = datetime.now(IST)
    tag = label or now.strftime("%H:%M:%S")
    ux = _upstox()
    token_last4 = _token_tail(getattr(ux, "access_token", None))

    rows_by_sym: Dict[str, Dict[str, Any]] = {}
    try:
        for u in load_universe():
            rows_by_sym[str(u.get("symbol") or "").upper()] = u
    except Exception as exc:
        logger.warning("olh rest canary universe load failed: %s", exc)

    targets: List[Tuple[str, str]] = []
    for sym in symbols:
        sym_u = str(sym).strip().upper()
        row = rows_by_sym.get(sym_u) or {"underlying_key": ""}
        ik = _resolve_equity_key(ux, row, sym_u)
        targets.append((sym_u, ik or ""))
    targets.append(("NIFTY", NIFTY_INDEX_KEY))

    results: List[Dict[str, Any]] = []
    with olh_candle_pool("canary"):
        for sym, ik in targets:
            entry: Dict[str, Any] = {
                "symbol": sym,
                "instrument_key": ik,
                "candle_count": 0,
                "latest_ts": None,
                "http_status": "unknown",
                "headers": {},
                "body_snip": None,
                "raw_count_before_drop": None,
                "raw_count_after_drop": None,
                "delay_sec_after_bar_close": None,
            }
            if not ik:
                entry["http_status"] = "skip_no_key"
                ohl_log(
                    "skipped",
                    action="rest_canary",
                    symbol=sym,
                    source=tag,
                    detail="no_instrument_key",
                )
                results.append(entry)
                continue
            try:
                raw = None
                raw_exc = None
                http_meta: Dict[str, Any] = {}
                try:
                    # Prefer instrumented fetch when available.
                    fetch = getattr(ux, "_fetch_intraday_candles_v3", None)
                    if fetch is None:
                        raise RuntimeError("no_intraday_v3")
                    raw = fetch(ik, "minutes/5")
                except Exception as exc:
                    raw_exc = exc

                # Capture last HTTP diagnostics if UpstoxService stashed them.
                try:
                    http_meta = dict(getattr(ux, "_last_candle_http_meta", None) or {})
                except Exception:
                    http_meta = {}
                entry["headers"] = http_meta.get("headers") or {}
                entry["body_snip"] = (http_meta.get("body_snip") or "")[:300] or None
                if http_meta.get("status") is not None:
                    entry["http_status"] = str(http_meta.get("status"))

                raw_n = len(raw) if isinstance(raw, list) else None
                entry["raw_count_before_drop"] = raw_n
                entry["raw_candle_count"] = raw_n
                bars = (
                    _drop_unclosed_bars(
                        _bars_from_rows(list(raw or []), ik),
                        log_label=f"rest_canary:{sym}",
                    )
                    if isinstance(raw, list)
                    else []
                )
                entry["raw_count_after_drop"] = len(bars)
                day_bars = [b for b in bars if b.session_date == trade_date]
                entry["candle_count"] = len(day_bars)
                if day_bars:
                    latest = day_bars[-1].ts
                    entry["latest_ts"] = latest.isoformat()
                    if entry["http_status"] in ("unknown",):
                        entry["http_status"] = "200_ok"
                    elif entry["http_status"] == "200":
                        entry["http_status"] = "200_ok"
                    bar_close = latest + timedelta(minutes=5)
                    entry["delay_sec_after_bar_close"] = round(
                        (now - bar_close).total_seconds(), 1
                    )
                elif raw_exc is not None:
                    entry["http_status"] = f"exception:{type(raw_exc).__name__}"
                elif raw is None:
                    entry["http_status"] = "none_timeout_429_or_budget_denied"
                elif isinstance(raw, list) and len(raw) == 0:
                    entry["http_status"] = "200_empty_candles"
                elif raw_n and not day_bars:
                    entry["http_status"] = "200_no_session_bars_after_drop"
                else:
                    entry["http_status"] = "200_empty_or_no_session_bars"

                detail = (
                    f"status={entry['http_status']} "
                    f"headers={entry['headers']} "
                    f"body[:300]={entry['body_snip']!r} "
                    f"raw_before={entry['raw_count_before_drop']} "
                    f"raw_after_drop={entry['raw_count_after_drop']} "
                    f"latest={entry['latest_ts']} "
                    f"delay_s={entry['delay_sec_after_bar_close']} "
                    f"token_last4={token_last4}"
                )
                ohl_log(
                    "success" if day_bars else "failed",
                    action="rest_canary",
                    symbol=sym,
                    source=tag,
                    detail=detail[:900],
                )
                logger.info(
                    "olh rest_canary tag=%s symbol=%s ik=%s status=%s "
                    "raw_before=%s raw_after_drop=%s latest=%s body[:300]=%s",
                    tag,
                    sym,
                    ik,
                    entry["http_status"],
                    entry["raw_count_before_drop"],
                    entry["raw_count_after_drop"],
                    entry["latest_ts"],
                    (entry["body_snip"] or "")[:300],
                )

                # Fail-fast: 09:20:05 empty/fail for Nifty → alert immediately.
                is_092005 = tag.startswith("09:20:05") or (
                    now.hour == 9 and now.minute == 20 and now.second <= 10
                )
                if sym == "NIFTY" and is_092005 and not day_bars:
                    try:
                        from backend.services.open_low_high.alerts import alert_degraded

                        alert_degraded(
                            f"REST canary FAIL-FAST Nifty empty/fail at {tag}",
                            trade_date=trade_date.isoformat(),
                            status=entry["http_status"],
                            raw_before=entry["raw_count_before_drop"],
                        )
                    except Exception:
                        logger.debug("nifty canary fail-fast alert failed", exc_info=True)
                    logger.error(
                        "olh rest_canary FAIL-FAST Nifty empty/fail tag=%s status=%s",
                        tag,
                        entry["http_status"],
                    )
            except Exception as exc:
                entry["http_status"] = f"exception:{type(exc).__name__}"
                ohl_log(
                    "failed",
                    action="rest_canary",
                    symbol=sym,
                    source=tag,
                    detail=(
                        f"exception_type={type(exc).__name__} error={exc} "
                        f"token_last4={token_last4}"
                    ),
                )
            results.append(entry)

    # Explicit fail-fast for probe_at label 09:20:05
    nifty = next((r for r in results if r.get("symbol") == "NIFTY"), None)
    if tag in ("09:20:05", "09:20:05 IST") or label.startswith("09:20:05"):
        if nifty and int(nifty.get("candle_count") or 0) == 0:
            try:
                from backend.services.open_low_high.alerts import alert_degraded

                alert_degraded(
                    "REST canary FAIL-FAST Nifty empty at 09:20:05",
                    trade_date=trade_date.isoformat(),
                    status=nifty.get("http_status"),
                )
            except Exception:
                pass

    payload = {
        "ok": True,
        "probe_at": tag,
        "tag": tag,
        "trade_date": trade_date.isoformat(),
        "now_ist": now.isoformat(),
        "token_last4": token_last4,
        "results": results,
    }
    logger.info(
        "olh rest_canary done probe_at=%s token_last4=%s n=%s",
        tag,
        token_last4,
        len(results),
    )
    return payload

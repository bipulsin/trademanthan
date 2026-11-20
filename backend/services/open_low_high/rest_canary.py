"""Timed REST 5m canary for OLH (3 equities + Nifty).

Fires at configured IST clocks on ``CANARY_DATE``, logs candle count / latest ts /
HTTP outcome to OHL_Log + app logger. Read-only — does not write screen rows.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytz

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# One-day probe window (IST calendar).
CANARY_DATE = date(2026, 11, 20)
CANARY_TIMES: Tuple[time, ...] = (
    time(9, 20, 5),
    time(9, 20, 30),
    time(9, 21, 0),
    time(9, 25, 5),
    time(9, 25, 30),
)
# Alias used by some callers / docs.
CANARY_CLOCKS = CANARY_TIMES
DEFAULT_SYMBOLS = ("RELIANCE", "TCS", "INFY")


def canary_should_run_today(now: Optional[datetime] = None) -> bool:
    t = now or datetime.now(IST)
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    return t.date() == CANARY_DATE


def run_rest_canary(
    *,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    session: Optional[date] = None,
    label: str = "",
) -> Dict[str, Any]:
    """Fetch REST 5m for equities + Nifty; log results. Safe to call any time."""
    from backend.services.open_high_low.config import NIFTY_INDEX_KEY
    from backend.services.open_high_low.data import load_universe
    from backend.services.open_low_high.ohl_log import ohl_log
    from backend.services.open_low_high.screener import (
        _resolve_equity_key,
        _token_tail,
        _upstox,
        _today,
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
    for sym, ik in targets:
        entry: Dict[str, Any] = {
            "symbol": sym,
            "instrument_key": ik,
            "candle_count": 0,
            "latest_ts": None,
            "http_status": "unknown",
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
            from backend.services.open_low_high.screener import (
                _bars_from_rows,
                _drop_unclosed_bars,
            )

            # Single intraday probe — classify None / empty / drop vs usable session bars.
            raw = None
            raw_exc = None
            try:
                raw = ux._fetch_intraday_candles_v3(ik, "minutes/5")
            except Exception as exc:
                raw_exc = exc
            raw_n = len(raw) if isinstance(raw, list) else None
            entry["raw_candle_count"] = raw_n
            bars = (
                _drop_unclosed_bars(
                    _bars_from_rows(list(raw or []), ik),
                    log_label=f"rest_canary:{sym}",
                )
                if isinstance(raw, list)
                else []
            )
            day_bars = [b for b in bars if b.session_date == trade_date]
            entry["candle_count"] = len(day_bars)
            if day_bars:
                latest = day_bars[-1].ts
                entry["latest_ts"] = latest.isoformat()
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
            ohl_log(
                "success" if day_bars else "failed",
                action="rest_canary",
                symbol=sym,
                source=tag,
                detail=(
                    f"count={entry['candle_count']} raw={entry.get('raw_candle_count')} "
                    f"latest={entry['latest_ts']} "
                    f"status={entry['http_status']} "
                    f"delay_s={entry['delay_sec_after_bar_close']} "
                    f"token_last4={token_last4}"
                ),
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
        logger.info(
            "olh rest_canary tag=%s symbol=%s ik=%s count=%s latest=%s status=%s delay_s=%s",
            tag,
            sym,
            ik,
            entry["candle_count"],
            entry["latest_ts"],
            entry["http_status"],
            entry["delay_sec_after_bar_close"],
        )

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

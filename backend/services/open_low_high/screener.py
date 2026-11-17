"""Live Open Low High screener — universe scan, Phase A/B, freeze + sector tags.

Schedule (IST):
  ~09:20:30  Phase A on completed 09:15 bar + Phase B on forming/complete 09:20 bar
             (WS preferred for 09:20; REST intraday fallback). Freeze by 09:22.
  After freeze: card list + S1–S3/W1–W3 tags are immutable for the day.
"""
from __future__ import annotations

import logging
import threading
import time as time_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.open_high_low.bars import Bar, bar_at, first_vwap
from backend.services.open_high_low.data import (
    front_month_expiry,
    load_universe,
    previous_trading_day,
    resolve_contract,
)
from backend.services.open_low_high.persistence import (
    ensure_tables,
    get_day_meta,
    is_day_frozen,
    list_screen_for_date,
    list_trade_log,
    upsert_day_meta,
    upsert_screen_rows,
)
from backend.services.open_low_high.signals import (
    confirm_phase_b,
    evaluate_phase_a,
    freeze_tags_immutable,
)

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

FIRST_CANDLE = time(9, 15)
SECOND_CANDLE = time(9, 20)
SCREEN_AFTER = time(9, 20, 30)
FREEZE_BY = time(9, 22, 0)

_run_lock = threading.Lock()
_last_run: Dict[str, Any] = {}


def _now() -> datetime:
    return datetime.now(IST)


def _today() -> date:
    return _now().date()


def _upstox():
    from backend.config import settings
    from backend.services.upstox_service import UpstoxService

    return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)


def window_status(now: Optional[datetime] = None) -> str:
    """waiting | open | past — relative to the 09:20:30–09:22 screen window."""
    t = now or _now()
    if t.tzinfo is None:
        t = IST.localize(t)
    else:
        t = t.astimezone(IST)
    tt = t.time()
    if tt < SCREEN_AFTER:
        return "waiting"
    if tt <= FREEZE_BY:
        return "open"
    return "past"


def _bars_from_rows(rows: List[Dict[str, Any]], contract: str) -> List[Bar]:
    from backend.services.open_high_low.bars import parse_ts

    out: List[Bar] = []
    for row in rows or []:
        ts = parse_ts(row.get("timestamp") or row.get("candle_time"))
        if ts is None:
            continue
        try:
            out.append(
                Bar(
                    ts=ts,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row.get("volume") or 0),
                    contract=contract,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda b: b.ts)
    return out


def _fetch_ws_5m_bars(instrument_key: str, session: date) -> List[Bar]:
    """Aggregate Upstox WS 1m candles into 5m bars for today's session (preferred for 09:20)."""
    from sqlalchemy import text

    from backend.database import SessionLocal

    db = SessionLocal()
    try:
        start = IST.localize(datetime(session.year, session.month, session.day, 9, 0, 0))
        end = start + timedelta(hours=7)
        rows = db.execute(
            text(
                """
                SELECT candle_time AS timestamp, open, high, low, close, volume
                FROM upstox_ws_intraday_1m
                WHERE instrument_key = :ik
                  AND candle_time >= :start
                  AND candle_time < :end
                ORDER BY candle_time ASC
                """
            ),
            {"ik": instrument_key, "start": start, "end": end},
        ).mappings().all()
    except Exception:
        logger.debug("olh ws 1m fetch miss %s", instrument_key, exc_info=True)
        return []
    finally:
        db.close()

    if not rows:
        return []
    # Aggregate 1m → 5m aligned to :00/:05/:10… IST
    buckets: Dict[datetime, Dict[str, float]] = {}
    for r in rows:
        ts = r["timestamp"]
        if isinstance(ts, str):
            from backend.services.open_high_low.bars import parse_ts

            ts = parse_ts(ts)
        if ts is None:
            continue
        if ts.tzinfo is None:
            ts = IST.localize(ts)
        else:
            ts = ts.astimezone(IST)
        minute = (ts.minute // 5) * 5
        bucket = ts.replace(minute=minute, second=0, microsecond=0)
        o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
        vol = float(r.get("volume") or 0)
        cur = buckets.get(bucket)
        if cur is None:
            buckets[bucket] = {"open": o, "high": h, "low": l, "close": c, "volume": vol}
        else:
            cur["high"] = max(cur["high"], h)
            cur["low"] = min(cur["low"], l)
            cur["close"] = c
            cur["volume"] += vol
    out: List[Bar] = []
    for bucket, ohlc in sorted(buckets.items()):
        out.append(
            Bar(
                ts=bucket,
                open=ohlc["open"],
                high=ohlc["high"],
                low=ohlc["low"],
                close=ohlc["close"],
                volume=ohlc["volume"],
                contract=instrument_key,
            )
        )
    return out


def _fetch_rest_5m(upstox: Any, instrument_key: str, session: date) -> List[Bar]:
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="minutes/5",
            days_back=3,
            range_end_date=session,
        )
    except Exception as exc:
        logger.info("olh rest 5m %s: %s", instrument_key, exc)
        raw = None
    if not raw:
        # Live intraday endpoint as last resort
        try:
            raw = upstox._fetch_intraday_candles_v3(instrument_key, "minutes/5")
        except Exception:
            raw = None
    return _bars_from_rows(list(raw or []), instrument_key)


def _fetch_daily_prev(
    upstox: Any, instrument_key: str, session: date
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return (prev_high, prev_low, prev_close) for previous trading day on this contract."""
    prev = previous_trading_day(session)
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval="days/1",
            days_back=10,
            range_end_date=session,
        )
    except Exception as exc:
        logger.info("olh daily %s: %s", instrument_key, exc)
        return None, None, None
    bars = _bars_from_rows(list(raw or []), instrument_key)
    for b in bars:
        if b.session_date == prev:
            return b.high, b.low, b.close
    # Fallback: last bar strictly before session
    earlier = [b for b in bars if b.session_date < session]
    if earlier:
        b = earlier[-1]
        return b.high, b.low, b.close
    return None, None, None


def _merge_bars(primary: List[Bar], fallback: List[Bar]) -> List[Bar]:
    by_ts: Dict[datetime, Bar] = {b.ts: b for b in fallback}
    for b in primary:
        by_ts[b.ts] = b
    return [by_ts[k] for k in sorted(by_ts)]


def _screen_one(
    upstox: Any,
    row: Dict[str, str],
    session: date,
    expiry: date,
) -> Optional[Dict[str, Any]]:
    symbol = str(row.get("symbol") or "").upper()
    if not symbol:
        return None
    resolved = resolve_contract(
        upstox,
        symbol,
        expiry,
        str(row.get("underlying_key") or ""),
        universe_row=row,
    )
    ik = str(resolved.get("instrument_key") or "").strip()
    if not ik:
        return None

    ws_bars = _fetch_ws_5m_bars(ik, session)
    rest_bars = _fetch_rest_5m(upstox, ik, session)
    bars_5m = _merge_bars(ws_bars, rest_bars)
    if not bars_5m:
        return None

    first = bar_at(bars_5m, session, FIRST_CANDLE)
    second = bar_at(bars_5m, session, SECOND_CANDLE)
    if first is None:
        return None

    vwap = first_vwap(bars_5m, session, FIRST_CANDLE)
    if vwap is None:
        # Single-bar typical as VWAP of first candle
        vwap = first.typical

    prev_high, prev_low, prev_close = _fetch_daily_prev(upstox, ik, session)
    if prev_high is None or prev_low is None:
        return None

    phase_a = evaluate_phase_a(
        first_open=first.open,
        first_high=first.high,
        first_low=first.low,
        first_close=first.close,
        vwap_0915=vwap,
        prev_high=prev_high,
        prev_low=prev_low,
        prev_close=prev_close,
    )
    if phase_a is None:
        return None

    # Phase B: without a second candle we cannot confirm — skip (will not keep).
    # Interpretation: remove cards that FAIL confirmation; missing second = fail.
    if second is None:
        return None
    if not confirm_phase_b(
        phase_a.side,
        first_high=phase_a.first_high,
        first_low=phase_a.first_low,
        second_close=second.close,
    ):
        return None

    contract_sym = (
        str(row.get("currmth_symbol") or "").strip()
        or f"{symbol} FUT {expiry.strftime('%d %b %y').upper()}"
    )
    return {
        "symbol": symbol,
        "side": phase_a.side,
        "contract": contract_sym,
        "expiry": expiry,
        "instrument_key": ik,
        "prev_close": prev_close,
        "prev_high": prev_high,
        "prev_low": prev_low,
        "first_open": phase_a.first_open,
        "first_high": phase_a.first_high,
        "first_low": phase_a.first_low,
        "first_close": phase_a.first_close,
        "second_close": float(second.close),
        "vwap_0915": phase_a.vwap_0915,
    }


def _apply_sector_badges(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        from backend.services.momentx.sector_badges import badges_for_symbols
    except Exception:
        logger.exception("olh sector badges import failed")
        return cards
    by = badges_for_symbols(c.get("symbol") for c in cards)
    out: List[Dict[str, Any]] = []
    for c in cards:
        card = dict(c)
        info = by.get(str(card.get("symbol") or "").upper()) or {}
        tags = freeze_tags_immutable(
            {
                "nifty_sector_label": info.get("nifty_sector_label"),
                "sector_badge": info.get("sector_badge"),
                "sector_star": bool(info.get("sector_star")),
                "sector_badge_display": info.get("sector_badge_display"),
            },
            frozen=True,
        )
        card.update(
            {
                "nifty_sector_label": tags.get("nifty_sector_label"),
                "sector_badge": tags.get("sector_badge"),
                "sector_star": bool(tags.get("sector_star")),
                "sector_badge_display": tags.get("sector_badge_display"),
                "frozen": True,
            }
        )
        out.append(card)
    return out


def run_screen(
    *,
    force: bool = False,
    session: Optional[date] = None,
    max_workers: int = 6,
) -> Dict[str, Any]:
    """Run Phase A+B screen and freeze today's set.

    Outside the 09:20:30–09:22 window, refuses unless ``force=True`` (manual/test).
    If already frozen for the day, returns stored rows without re-scanning.
    """
    ensure_tables()
    trade_date = session or _today()
    now = _now()
    win = window_status(now)

    if is_day_frozen(trade_date) and not force:
        rows = list_screen_for_date(trade_date)
        meta = get_day_meta(trade_date) or {}
        return {
            "ok": True,
            "status": "frozen",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": True,
            "bullish": [r for r in rows if r.get("side") == "BULLISH"],
            "bearish": [r for r in rows if r.get("side") == "BEARISH"],
            "meta": meta,
            "message": "Already frozen for today — cards and sector tags immutable.",
        }

    if win == "waiting" and not force:
        return {
            "ok": True,
            "status": "waiting",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": False,
            "bullish": [],
            "bearish": [],
            "message": "Waiting for 09:20:30 IST screen window.",
        }

    if not _run_lock.acquire(blocking=False):
        return {
            "ok": False,
            "status": "busy",
            "trade_date": trade_date.isoformat(),
            "message": "Screen already running.",
        }

    try:
        upsert_day_meta(trade_date, status="screening", frozen=False)
        upstox = _upstox()
        universe = load_universe()
        expiry = front_month_expiry(trade_date)
        logger.info(
            "olh screen start date=%s expiry=%s symbols=%s force=%s",
            trade_date,
            expiry,
            len(universe),
            force,
        )

        results: List[Dict[str, Any]] = []
        errors = 0

        def _one(urow: Dict[str, str]) -> Optional[Dict[str, Any]]:
            try:
                return _screen_one(upstox, urow, trade_date, expiry)
            except Exception:
                logger.exception("olh screen symbol failed %s", urow.get("symbol"))
                return None

        # Bounded parallelism to respect Upstox rate limits somewhat.
        with ThreadPoolExecutor(max_workers=max(1, min(max_workers, 8))) as pool:
            futs = {pool.submit(_one, u): u for u in universe}
            for fut in as_completed(futs):
                try:
                    got = fut.result()
                except Exception:
                    errors += 1
                    continue
                if got:
                    results.append(got)
                # Soft pacing
                time_mod.sleep(0.02)

        tagged = _apply_sector_badges(results)
        for c in tagged:
            c["frozen"] = True

        upsert_screen_rows(trade_date, tagged, frozen=True)
        bullish = [c for c in tagged if c.get("side") == "BULLISH"]
        bearish = [c for c in tagged if c.get("side") == "BEARISH"]
        phase_ts = _now()
        upsert_day_meta(
            trade_date,
            status="frozen",
            frozen=True,
            bullish_count=len(bullish),
            bearish_count=len(bearish),
            phase_a_at=phase_ts,
            phase_b_at=phase_ts,
            notes=f"screened={len(universe)} kept={len(tagged)} errors={errors} force={force}",
            meta={"window": win, "expiry": expiry.isoformat()},
        )
        payload = {
            "ok": True,
            "status": "frozen",
            "trade_date": trade_date.isoformat(),
            "window": win,
            "frozen": True,
            "bullish": bullish,
            "bearish": bearish,
            "scanned": len(universe),
            "kept": len(tagged),
            "errors": errors,
            "message": "Screen complete — set frozen for the day.",
        }
        global _last_run
        _last_run = dict(payload)
        _last_run["at"] = phase_ts.isoformat()
        logger.info(
            "olh screen done kept=%s bull=%s bear=%s",
            len(tagged),
            len(bullish),
            len(bearish),
        )
        return payload
    except Exception as exc:
        logger.exception("olh run_screen failed")
        upsert_day_meta(trade_date, status="error", frozen=False, notes=str(exc)[:500])
        return {
            "ok": False,
            "status": "error",
            "trade_date": trade_date.isoformat(),
            "message": str(exc),
        }
    finally:
        _run_lock.release()


def get_today_state(*, auto_run: bool = True) -> Dict[str, Any]:
    """API payload for the live page. Optionally triggers screen inside the window."""
    ensure_tables()
    trade_date = _today()
    win = window_status()
    meta = get_day_meta(trade_date)
    frozen = bool(meta and meta.get("frozen")) or is_day_frozen(trade_date)

    if auto_run and not frozen and win == "open":
        return run_screen(force=False, session=trade_date)

    rows = list_screen_for_date(trade_date)
    bullish = [r for r in rows if str(r.get("side") or "").upper() == "BULLISH"]
    bearish = [r for r in rows if str(r.get("side") or "").upper() == "BEARISH"]

    if frozen:
        status = "frozen"
        message = "Frozen for today — cards and sector tags will not change."
    elif win == "waiting":
        status = "waiting"
        message = "Waiting for first candle (09:15) to complete — screen runs ~09:20:30 IST."
    elif win == "past" and not rows:
        status = "empty"
        message = "No screen stored for today. Use Refresh (force) or wait for tomorrow's window."
    elif rows:
        status = "live"
        message = "Showing stored screen rows."
    else:
        status = "waiting"
        message = "No qualifiers yet."

    return {
        "ok": True,
        "status": status,
        "trade_date": trade_date.isoformat(),
        "window": win,
        "frozen": frozen,
        "bullish": bullish,
        "bearish": bearish,
        "meta": meta,
        "server_time": _now().isoformat(),
        "message": message,
        "last_run": _last_run or None,
    }


def get_trade_log_payload(
    *,
    limit_days: int = 60,
    side: Optional[str] = None,
) -> Dict[str, Any]:
    rows = list_trade_log(limit_days=limit_days, side=side)
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        d = str(r.get("trade_date") or "")[:10]
        by_date.setdefault(d, []).append(r)
    sections = [{"date": d, "rows": by_date[d]} for d in sorted(by_date.keys(), reverse=True)]
    return {
        "ok": True,
        "sections": sections,
        "total": len(rows),
        "limit_days": limit_days,
    }

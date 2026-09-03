"""5m candle fetch, disk cache, and session bar helpers."""
from __future__ import annotations

import contextvars
import json
import logging
import time
from datetime import date, datetime, time as dt_time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.breakfast_strategy.config import (
    CANDLE_1M_INTERVAL,
    CANDLE_DAYS_BACK,
    CANDLE_INTERVAL,
    FETCH_THROTTLE_SEC,
    LIVE_1M_DAYS_BACK,
    LIVE_1M_THROTTLE_SEC,
    MONITOR_FROM,
    MONITOR_FROM_AFTER_915,
    SIGNAL_BAR_TIME,
    TIME_EXIT,
)
from backend.services.volume_mismatch.candles import _parse_ts

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")


def default_cache_dir() -> Path:
    ec2 = Path("/home/ubuntu/trademanthan/data/breakfast_strategy_candle_cache")
    if Path("/home/ubuntu/trademanthan/data").is_dir():
        ec2.mkdir(parents=True, exist_ok=True)
        return ec2
    root = Path(__file__).resolve().parents[3]
    p = root / "data" / "breakfast_strategy_candle_cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _sanitize_key(instrument_key: str) -> str:
    return (instrument_key or "").strip().replace("|", "__")


def _cache_path(cache_dir: Path, instrument_key: str) -> Path:
    return cache_dir / f"{_sanitize_key(instrument_key)}_5m.json"


def _cache_path_1m(cache_dir: Path, instrument_key: str) -> Path:
    return cache_dir / f"{_sanitize_key(instrument_key)}_1m.json"


def _merge_candles(existing: List[Dict[str, Any]], fresh: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_ts: Dict[str, Dict[str, Any]] = {}
    for c in list(existing) + list(fresh):
        key = str(c.get("timestamp") or "")
        if key:
            by_ts[key] = c
    return [by_ts[k] for k in sorted(by_ts)]


def load_cached_5m(cache_dir: Path, instrument_key: str) -> List[Dict[str, Any]]:
    p = _cache_path(cache_dir, instrument_key)
    if not p.is_file():
        return []
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        rows = doc.get("candles") if isinstance(doc, dict) else None
        return list(rows) if isinstance(rows, list) else []
    except Exception as e:
        logger.debug("breakfast cache read %s: %s", p, e)
        return []


def save_cached_5m(cache_dir: Path, instrument_key: str, candles: List[Dict[str, Any]]) -> None:
    p = _cache_path(cache_dir, instrument_key)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    payload = {
        "instrument_key": instrument_key,
        "interval": CANDLE_INTERVAL,
        "updated_at": datetime.now(IST).isoformat(),
        "candles": candles,
    }
    tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
    tmp.replace(p)


def load_cached_1m(cache_dir: Path, instrument_key: str) -> List[Dict[str, Any]]:
    p = _cache_path_1m(cache_dir, instrument_key)
    if not p.is_file():
        return []
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        rows = doc.get("candles") if isinstance(doc, dict) else None
        return list(rows) if isinstance(rows, list) else []
    except Exception as e:
        logger.debug("breakfast 1m cache read %s: %s", p, e)
        return []


def save_cached_1m(cache_dir: Path, instrument_key: str, candles: List[Dict[str, Any]]) -> None:
    p = _cache_path_1m(cache_dir, instrument_key)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    payload = {
        "instrument_key": instrument_key,
        "interval": CANDLE_1M_INTERVAL,
        "updated_at": datetime.now(IST).isoformat(),
        "candles": candles,
    }
    tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
    tmp.replace(p)


def fetch_1m_range(
    upstox: Any,
    instrument_key: str,
    *,
    range_end: date,
    days_back: int = LIVE_1M_DAYS_BACK,
    throttle_sec: float = LIVE_1M_THROTTLE_SEC,
) -> List[Dict[str, Any]]:
    if throttle_sec > 0:
        time.sleep(throttle_sec)
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval=CANDLE_1M_INTERVAL,
            days_back=days_back,
            range_end_date=range_end,
        )
        return list(raw or [])
    except Exception as e:
        logger.warning("breakfast 1m fetch %s: %s", instrument_key, e)
        return []


def ensure_1m_cached(
    upstox: Any,
    cache_dir: Path,
    instrument_key: str,
    *,
    range_end: date,
    force: bool = False,
) -> List[Dict[str, Any]]:
    ik = (instrument_key or "").strip()
    if not ik:
        return []
    if not force:
        cached = load_cached_1m(cache_dir, ik)
        if cached:
            return cached
    fresh = fetch_1m_range(upstox, ik, range_end=range_end)
    merged = _merge_candles(load_cached_1m(cache_dir, ik) if not force else [], fresh)
    if merged:
        save_cached_1m(cache_dir, ik, merged)
    return merged


def fetch_1m_parallel(
    upstox: Any,
    cache_dir: Path,
    instrument_keys: List[str],
    *,
    session_date: date,
    max_workers: int = 8,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch/update 1m candles for many keys concurrently (Breakfast live ticks)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    keys = [str(k).strip() for k in instrument_keys if str(k or "").strip()]
    if not keys:
        return {}

    def _one(ik: str) -> tuple[str, List[Dict[str, Any]]]:
        return ik, ensure_1m_cached(upstox, cache_dir, ik, range_end=session_date, force=True)

    out: Dict[str, List[Dict[str, Any]]] = {}
    workers = max(1, min(int(max_workers), len(keys)))
    try:
        from backend.services.breakfast_upstox_gate import breakfast_priority_owner_active

        if breakfast_priority_owner_active():
            logger.info(
                "breakfast_upstox_owner applied on pool workers batch_size=%s interval=1m",
                len(keys),
            )
    except Exception as e:
        logger.exception("breakfast_exclusivity: check_failed error=%s", e)
    ctx = contextvars.copy_context()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(ctx.run, _one, ik) for ik in keys]
        for fut in as_completed(futures):
            try:
                ik, candles = fut.result()
                out[ik] = candles
            except Exception as e:
                logger.warning("breakfast 1m parallel fetch failed: %s", e)
    return out


def fetch_5m_parallel(
    upstox: Any,
    cache_dir: Path,
    instrument_keys: List[str],
    *,
    session_date: date,
    max_workers: int = 8,
    throttle_sec: float = 0.0,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch/update minutes/5 for many keys concurrently (Breakfast freeze)."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    keys = [str(k).strip() for k in instrument_keys if str(k or "").strip()]
    if not keys:
        return {}

    def _one(ik: str) -> tuple[str, List[Dict[str, Any]]]:
        return ik, ensure_5m_cached(
            upstox,
            cache_dir,
            ik,
            range_end=session_date,
            session_dates=[session_date],
            force=True,
            throttle_sec=throttle_sec,
        )

    out: Dict[str, List[Dict[str, Any]]] = {}
    workers = max(1, min(int(max_workers), len(keys)))
    try:
        from backend.services.breakfast_upstox_gate import breakfast_priority_owner_active

        if breakfast_priority_owner_active():
            logger.info(
                "breakfast_upstox_owner applied on pool workers batch_size=%s interval=5m",
                len(keys),
            )
    except Exception as e:
        logger.exception("breakfast_exclusivity: check_failed error=%s", e)
    ctx = contextvars.copy_context()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(ctx.run, _one, ik) for ik in keys]
        for fut in as_completed(futures):
            try:
                ik, candles = fut.result()
                out[ik] = candles
            except Exception as e:
                logger.warning("breakfast 5m parallel fetch failed: %s", e)
    return out


def forming_bar_from_1m_upto(
    bars_1m: List[Dict[str, Any]],
    session_date: date,
    upto_hhmm: Tuple[int, int],
) -> Optional[Dict[str, Any]]:
    """Synthetic 9:15→upto_hhmm bar from 1m closes (minute-close at each tick)."""
    return aggregate_1m_to_session_5m(
        bars_1m,
        session_date,
        from_hhmm=(9, 15),
        to_hhmm=upto_hhmm,
    )


def fetch_5m_range(
    upstox: Any,
    instrument_key: str,
    *,
    range_end: date,
    days_back: int = CANDLE_DAYS_BACK,
    throttle_sec: float = FETCH_THROTTLE_SEC,
) -> List[Dict[str, Any]]:
    if throttle_sec > 0:
        time.sleep(throttle_sec)
    try:
        raw = upstox.get_historical_candles_by_instrument_key(
            instrument_key,
            interval=CANDLE_INTERVAL,
            days_back=days_back,
            range_end_date=range_end,
        )
        return list(raw or [])
    except Exception as e:
        logger.warning("breakfast 5m fetch %s: %s", instrument_key, e)
        return []


def covers_session_dates(
    candles: List[Dict[str, Any]],
    session_dates: List[date],
    *,
    require_signal_bar: bool = True,
) -> bool:
    """True when every session date has a usable 9:15 (or 9:20 fallback) bar."""
    if not session_dates:
        return bool(candles)
    for sd in session_dates:
        if require_signal_bar:
            if signal_bar(candles, sd) is None:
                return False
        elif first_5m_bar(candles, sd) is None:
            return False
    return True


def session_has_stock_bars(candles: List[Dict[str, Any]], session_date: date) -> bool:
    """True when both signal (9:15 stamp) and anchor bars exist for the session."""
    return signal_bar(candles, session_date) is not None and anchor_bar(candles, session_date) is not None


def ensure_5m_cached(
    upstox: Any,
    cache_dir: Path,
    instrument_key: str,
    *,
    range_end: date,
    range_start: Optional[date] = None,
    session_dates: Optional[List[date]] = None,
    force: bool = False,
    throttle_sec: Optional[float] = None,
) -> List[Dict[str, Any]]:
    ik = (instrument_key or "").strip()
    if not ik:
        return []
    check_dates = session_dates or ([range_start] if range_start else [])
    if not force and check_dates:
        cached = load_cached_5m(cache_dir, ik)
        if covers_session_dates(cached, check_dates):
            return cached
    elif not force:
        cached = load_cached_5m(cache_dir, ik)
        if first_5m_bar(cached, range_end) is not None:
            return cached
    days_back = CANDLE_DAYS_BACK
    if range_start is not None:
        days_back = max(days_back, (range_end - range_start).days + 10)
    tsec = FETCH_THROTTLE_SEC if throttle_sec is None else float(throttle_sec)
    existing = load_cached_5m(cache_dir, ik)
    fresh = fetch_5m_range(upstox, ik, range_end=range_end, days_back=days_back, throttle_sec=tsec)
    # Always merge with disk — force=True must refresh, not discard a warm 9:15 bar when REST flakes.
    merged = _merge_candles(existing, fresh)
    if check_dates and not covers_session_dates(merged, check_dates) and covers_session_dates(existing, check_dates):
        return existing
    if not check_dates and force and first_5m_bar(merged, range_end) is None and first_5m_bar(existing, range_end) is not None:
        return existing
    if merged:
        save_cached_5m(cache_dir, ik, merged)
    return merged


def _bar_dt(c: Dict[str, Any]) -> Optional[datetime]:
    return _parse_ts(c.get("timestamp"))


def _ohlcv(c: Dict[str, Any]) -> Tuple[float, float, float, float, float]:
    def _f(k: str) -> float:
        try:
            return float(c.get(k) or 0)
        except (TypeError, ValueError):
            return 0.0

    return _f("open"), _f("high"), _f("low"), _f("close"), _f("volume")


def ist_ts(session_date: date, hh: int, mm: int) -> datetime:
    return IST.localize(datetime.combine(session_date, dt_time(hh, mm)))


def aggregate_1m_to_session_5m(
    bars_1m: List[Dict[str, Any]],
    session_date: date,
    *,
    from_hhmm: Tuple[int, int] = (9, 15),
    to_hhmm: Tuple[int, int] = (9, 20),
) -> Optional[Dict[str, Any]]:
    """Build a synthetic 9:15–9:20 5m bar from 1-minute OHLCV rows."""
    fh, fm = from_hhmm
    th, tm = to_hhmm
    start_m = fh * 60 + fm
    end_m = th * 60 + tm
    window: List[Dict[str, Any]] = []
    for c in bars_1m or []:
        dt = _bar_dt(c)
        if dt is None:
            continue
        t = dt.astimezone(IST)
        if t.date() != session_date:
            continue
        mins = t.hour * 60 + t.minute
        if start_m <= mins < end_m:
            window.append(c)
    if not window:
        return None
    window.sort(key=lambda x: _bar_dt(x) or datetime.min.replace(tzinfo=IST))
    o, _, _, _, _ = _ohlcv(window[0])
    _, _, _, cl, _ = _ohlcv(window[-1])
    hi = max(_ohlcv(c)[1] for c in window)
    lo = min(_ohlcv(c)[2] for c in window)
    vol = sum(_ohlcv(c)[4] for c in window)
    if o <= 0 or cl <= 0:
        return None
    return {
        "timestamp": ist_ts(session_date, to_hhmm[0], to_hhmm[1]).isoformat(),
        "open": o,
        "high": hi,
        "low": lo,
        "close": cl,
        "volume": vol,
    }


def bars_ohlc_close_match(
    a: Optional[Dict[str, Any]],
    b: Optional[Dict[str, Any]],
    *,
    abs_tol: float = 0.05,
    rel_tol: float = 0.0005,
) -> bool:
    """True when OHLC close values agree within tolerance."""
    if not a or not b:
        return False

    def _close(c: Dict[str, Any]) -> float:
        return float(_ohlcv(c)[3])

    for c in (_close(a), _close(b)):
        if c <= 0:
            return False
    fields = ("open", "high", "low", "close")
    for field in fields:
        av = float(a.get(field) or 0)
        bv = float(b.get(field) or 0)
        if av <= 0 or bv <= 0:
            return False
        tol = max(abs_tol, rel_tol * max(abs(av), abs(bv)))
        if abs(av - bv) > tol:
            return False
    return True


def move_pct_vs_prev_close(close_px: float, prev_close: float) -> Optional[float]:
    if prev_close <= 0 or close_px <= 0:
        return None
    return (close_px - prev_close) / prev_close * 100.0


def _synthetic_5m_bar(session_date: date, open_px: float, close_px: float) -> Dict[str, Any]:
    hi = max(open_px, close_px)
    lo = min(open_px, close_px)
    return {
        "timestamp": ist_ts(session_date, 9, 20).isoformat(),
        "open": open_px,
        "high": hi,
        "low": lo,
        "close": close_px,
        "volume": 0.0,
    }


def first_5m_bar_from_quote(upstox: Any, instrument_key: str, session_date: date) -> Optional[Dict[str, Any]]:
    """Build 9:15–9:20 proxy from live Upstox market-quote (session open vs LTP)."""
    try:
        q = upstox.get_market_quote_by_key(instrument_key)
    except Exception as e:
        logger.warning("breakfast quote fallback %s: %s", instrument_key, e)
        return None
    if not q:
        return None
    ohlc = q.get("ohlc") if isinstance(q.get("ohlc"), dict) else {}
    open_px = float(ohlc.get("open") or 0)
    close_px = float(q.get("last_price") or ohlc.get("close") or 0)
    if open_px <= 0 or close_px <= 0:
        return None
    return _synthetic_5m_bar(session_date, open_px, close_px)


def _index_price_snapshots(session_date: date, index_name: str = "NIFTY50") -> List[Tuple[datetime, float, Optional[float]]]:
    """Rows (price_time, ltp, day_open) for morning snapshots on session_date."""
    start = IST.localize(datetime.combine(session_date, dt_time(9, 10)))
    end = IST.localize(datetime.combine(session_date, dt_time(9, 25)))
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                "SELECT price_time, ltp, day_open FROM index_prices "
                "WHERE index_name = :name AND price_time >= :start AND price_time <= :end "
                "ORDER BY price_time"
            ),
            {"name": index_name, "start": start.replace(tzinfo=None), "end": end.replace(tzinfo=None)},
        ).fetchall()
    except Exception as e:
        logger.debug("breakfast index_prices %s %s: %s", index_name, session_date, e)
        return []
    finally:
        db.close()
    out: List[Tuple[datetime, float, Optional[float]]] = []
    for r in rows or []:
        pt = r.price_time
        if pt is None:
            continue
        if pt.tzinfo is None:
            pt = IST.localize(pt)
        else:
            pt = pt.astimezone(IST)
        ltp = float(r.ltp or 0)
        if ltp <= 0:
            continue
        day_open = float(r.day_open) if r.day_open else None
        out.append((pt, ltp, day_open))
    return out


def _pick_snapshot(
    snaps: List[Tuple[datetime, float, Optional[float]]],
    target: dt_time,
    *,
    tolerance_min: int = 3,
) -> Optional[Tuple[datetime, float, Optional[float]]]:
    if not snaps:
        return None
    target_dt = datetime.combine(date.today(), target)
    best: Optional[Tuple[datetime, float, Optional[float], float]] = None
    for pt, ltp, day_open in snaps:
        delta = abs((datetime.combine(pt.date(), pt.time()) - target_dt).total_seconds())
        if delta > tolerance_min * 60:
            continue
        if best is None or delta < best[3]:
            best = (pt, ltp, day_open, delta)
    if best is None:
        return None
    return best[0], best[1], best[2]


def first_5m_bar_from_index_prices(session_date: date, index_name: str = "NIFTY50") -> Optional[Dict[str, Any]]:
    """9:15–9:20 proxy from index_prices (populated from live quote during the session)."""
    snaps = _index_price_snapshots(session_date, index_name=index_name)
    open_row = _pick_snapshot(snaps, dt_time(9, 15))
    close_row = _pick_snapshot(snaps, dt_time(9, 20))
    if not open_row or not close_row:
        return None
    _t_o, ltp_open, _ = open_row
    _t_c, ltp_close, _ = close_row
    if ltp_open <= 0 or ltp_close <= 0:
        return None
    return _synthetic_5m_bar(session_date, ltp_open, ltp_close)


def resolve_nifty_first_5m_bar(
    candles: List[Dict[str, Any]],
    session_date: date,
    upstox: Any,
    *,
    instrument_key: str,
) -> Optional[Dict[str, Any]]:
    """Historical 5m first; then index_prices; then live quote when session is today."""
    bar = first_5m_bar(candles, session_date)
    if bar:
        return bar
    bar = first_5m_bar_from_index_prices(session_date, index_name="NIFTY50")
    if bar:
        logger.info("breakfast NIFTY 5m from index_prices for %s", session_date)
        return bar
    today_ist = datetime.now(IST).date()
    if session_date == today_ist:
        bar = first_5m_bar_from_quote(upstox, instrument_key, session_date)
        if bar:
            logger.info("breakfast NIFTY 5m from live quote for %s", session_date)
            return bar
    return None


def bar_at_session_time(
    candles: List[Dict[str, Any]],
    session_date: date,
    target: Tuple[int, int],
    *,
    tolerance_min: int = 0,
) -> Optional[Dict[str, Any]]:
    """Return the session bar whose timestamp is nearest to target HH:MM."""
    th, tm = target
    target_mins = th * 60 + tm
    best: Optional[Tuple[float, Dict[str, Any]]] = None
    for c in candles or []:
        dt = _bar_dt(c)
        if dt is None:
            continue
        t = dt.astimezone(IST)
        if t.date() != session_date:
            continue
        delta = abs((t.hour * 60 + t.minute) - target_mins)
        if delta > tolerance_min:
            continue
        if best is None or delta < best[0]:
            best = (delta, c)
    return best[1] if best else None


def anchor_bar(candles: List[Dict[str, Any]], session_date: date) -> Optional[Dict[str, Any]]:
    """9:15-stamp bar for anchor/entry/TP/SL; fallback to 9:20 stamp if missing."""
    from backend.services.breakfast_strategy.config import ANCHOR_BAR_TIME

    return bar_at_session_time(candles, session_date, ANCHOR_BAR_TIME) or bar_at_session_time(
        candles, session_date, (9, 20)
    )


def signal_bar(candles: List[Dict[str, Any]], session_date: date) -> Optional[Dict[str, Any]]:
    """9:15-stamp bar (9:15–9:20 opening candle, Upstox start-labeled) for ranking."""
    return bar_at_session_time(candles, session_date, SIGNAL_BAR_TIME) or bar_at_session_time(
        candles, session_date, (9, 20)
    )


def monitor_from_after_anchor(anchor: Dict[str, Any]) -> Tuple[int, int]:
    """Monitor from 9:20 when entry is 9:15 close; else 9:25."""
    dt = _bar_dt(anchor)
    if dt is None:
        return MONITOR_FROM
    t = dt.astimezone(IST)
    if t.time() == dt_time(9, 15):
        return MONITOR_FROM_AFTER_915
    return MONITOR_FROM


def first_5m_bar(candles: List[Dict[str, Any]], session_date: date) -> Optional[Dict[str, Any]]:
    """Session signal bar (9:15 stamp = 9:15–9:20 opening window) for NIFTY/sector first-5m."""
    bar = signal_bar(candles, session_date)
    if bar:
        return bar
    # Legacy fallback: prefer 9:15 over 9:20 if signal_bar missed both exact lookups
    hits: List[Tuple[datetime, Dict[str, Any]]] = []
    for c in candles or []:
        dt = _bar_dt(c)
        if dt is None:
            continue
        t = dt.astimezone(IST)
        if t.date() != session_date:
            continue
        if t.time() in (dt_time(9, 15), dt_time(9, 20)):
            hits.append((t, c))
    if not hits:
        for c in candles or []:
            dt = _bar_dt(c)
            if dt is None:
                continue
            t = dt.astimezone(IST)
            if t.date() == session_date and dt_time(9, 15) <= t.time() <= dt_time(9, 20):
                return c
        return None
    hits.sort(key=lambda x: (0 if x[0].time() == dt_time(9, 15) else 1, x[0]))
    return hits[0][1]


def session_5m_bars_after_entry(
    candles: List[Dict[str, Any]],
    session_date: date,
    *,
    from_hhmm: Tuple[int, int] = (9, 25),
    to_hhmm: Tuple[int, int] = (15, 15),
) -> List[Tuple[datetime, Dict[str, Any]]]:
    out: List[Tuple[datetime, Dict[str, Any]]] = []
    for c in candles or []:
        dt = _bar_dt(c)
        if dt is None:
            continue
        t = dt.astimezone(IST)
        if t.date() != session_date:
            continue
        hm = (t.hour, t.minute)
        if hm < from_hhmm or hm > to_hhmm:
            continue
        out.append((t, c))
    out.sort(key=lambda x: x[0])
    return out


def bar_move_pct(c: Dict[str, Any]) -> Optional[float]:
    o, _, _, cl, _ = _ohlcv(c)
    if o <= 0 or cl <= 0:
        return None
    return (cl - o) / o * 100.0


def bar_volume(c: Dict[str, Any]) -> float:
    return _ohlcv(c)[4]


def candle_ohlcv(c: Dict[str, Any]) -> Tuple[float, float, float, float, float]:
    return _ohlcv(c)


def first_5m_ohlc_payload(bar: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """OHLC + bar open timestamp for persist/UI. Empty dict if bar is missing."""
    if not bar:
        return {}
    o, h, l, c, _ = _ohlcv(bar)
    ts = bar.get("timestamp")
    if hasattr(ts, "isoformat"):
        ts = ts.isoformat()
    return {
        "first_5m_open": o,
        "first_5m_high": h,
        "first_5m_low": l,
        "first_5m_close": c,
        "first_5m_ts": ts,
    }


def prev_session_close(
    candles: List[Dict[str, Any]],
    session_date: date,
) -> Optional[float]:
    """Last 5m bar close on the most recent session date before session_date."""
    prev_date: Optional[date] = None
    for c in candles or []:
        dt = _bar_dt(c)
        if dt is None:
            continue
        d = dt.astimezone(IST).date()
        if d >= session_date:
            continue
        if prev_date is None or d > prev_date:
            prev_date = d
    if prev_date is None:
        return None
    last_close: Optional[float] = None
    last_ts: Optional[datetime] = None
    for c in candles or []:
        dt = _bar_dt(c)
        if dt is None:
            continue
        t = dt.astimezone(IST)
        if t.date() != prev_date:
            continue
        cl = _ohlcv(c)[3]
        if cl <= 0:
            continue
        if last_ts is None or t > last_ts:
            last_ts = t
            last_close = cl
    return last_close


def setup_bar_vs_prev_close(
    candles: List[Dict[str, Any]],
    session_date: date,
) -> Optional[Tuple[Dict[str, Any], float, float]]:
    """Signal bar (9:15 stamp) close vs previous session close — used for stock ranking."""
    bar = signal_bar(candles, session_date)
    if not bar:
        return None
    prev = prev_session_close(candles, session_date)
    if prev is None or prev <= 0:
        return None
    _, _, _, cl, _ = _ohlcv(bar)
    if cl <= 0:
        return None
    pct = (cl - prev) / prev * 100.0
    return bar, float(prev), float(pct)


def session_vwap_by_bar_time(
    candles: List[Dict[str, Any]],
    session_date: date,
    *,
    from_hhmm: Tuple[int, int] = (9, 15),
    to_hhmm: Tuple[int, int] = TIME_EXIT,
) -> Dict[datetime, float]:
    """Cumulative session VWAP (typical price) keyed by bar timestamp."""
    from backend.services.smart_futures_picker.indicators import session_vwap

    bars = session_5m_bars_after_entry(
        candles, session_date, from_hhmm=from_hhmm, to_hhmm=to_hhmm
    )
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    vols: List[float] = []
    out: Dict[datetime, float] = {}
    for t, c in bars:
        _o, h, lo, cl, v = _ohlcv(c)
        highs.append(h)
        lows.append(lo)
        closes.append(cl)
        vols.append(v)
        out[t] = session_vwap(highs, lows, closes, vols)
    return out

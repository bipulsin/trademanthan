"""
Smart Futures historical backtest engine.

Mirrors picker scoring math and filters (read-only reuse of indicator helpers + ScoredPick shape)
without modifying ``smart_futures_picker/job.py`` or sentiment jobs.
"""
from __future__ import annotations

import time
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.config import settings
from backend.services.smart_futures_backtest.april_2026_universe import (
    APRIL_2026_FUT_SESSION_END,
    load_april_2026_futures_by_underlying,
    use_fixed_april_2026_futures,
)
from backend.services.smart_futures_backtest.log_setup import get_backtest_logger
from backend.services.smart_futures_backtest.retry import call_with_retries
from backend.services.smart_futures_backtest.sector_asof import sector_score_as_of
from backend.services.smart_futures_backtest.sentiment import load_sentiment_map_for_session_date
from backend.services.smart_futures_picker.indicators import (
    adx_14_value,
    compute_cms_core,
    compute_obv_slope_daily,
    divergence_bundle,
    ha_trend_score,
    renko_momentum_score,
    session_vwap,
    volume_surge_ratio,
    wilder_atr,
    wilder_atr_14,
)
from backend.services.smart_futures_picker.job import (
    ScoredPick,
    _ist_date_from_ts,
    _session_elapsed_fraction,
    _sort_candles,
)
from backend.services.upstox_service import UpstoxService

IST = pytz.timezone("Asia/Kolkata")
INDIA_VIX_KEY = "NSE_INDEX|India VIX"
SESSION_MINUTES = 375.0

# Earliest calendar session_date allowed for backtests (product rule).
BACKTEST_MIN_SESSION_DATE = date(2026, 2, 1)

# Session dates 2026-04-01+ use arbitrage_master.currmth_future_* at run time.
# Session dates 2026-02-01 .. 2026-03-31 use April-2026 expiry FUT from nse_instruments.json (see april_2026_universe).
FUTURES_UNIVERSE_CURRMTH = "arbitrage_master_currmth"
FUTURES_UNIVERSE_APRIL2026 = "nse_instruments_april2026_expiry"


def _aggregate_futures_universe_label(d0: date, d1: date) -> str:
    """Human-readable label for a whole date range (slots may differ if range spans Apr 2026)."""
    if d0 > APRIL_2026_FUT_SESSION_END:
        return FUTURES_UNIVERSE_CURRMTH
    if d1 <= APRIL_2026_FUT_SESSION_END:
        return FUTURES_UNIVERSE_APRIL2026
    return "mixed_april2026_expiry_then_currmth"


def validate_backtest_date_bounds(d0: date, d1: date) -> Optional[str]:
    """Return an error message if the inclusive range is not allowed; else None."""
    if d0 < BACKTEST_MIN_SESSION_DATE:
        return f"from_date must be on or after {BACKTEST_MIN_SESSION_DATE.isoformat()}"
    if d1 < BACKTEST_MIN_SESSION_DATE:
        return f"to_date must be on or after {BACKTEST_MIN_SESSION_DATE.isoformat()}"
    return None


def _parse_bar_end_ist(ts: str) -> Optional[datetime]:
    if not ts or len(ts) < 19:
        return None
    try:
        return datetime.strptime(ts[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=IST)
    except ValueError:
        return None


def _daily_last_n_upto(daily_raw: Optional[List[dict]], session_date: date, n: int = 10) -> List[dict]:
    s = _sort_candles(daily_raw)
    acc: List[dict] = []
    for x in s:
        ts = str(x.get("timestamp") or "")[:10]
        try:
            d = datetime.strptime(ts, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d <= session_date:
            acc.append(x)
    if len(acc) < n:
        return []
    return acc[-n:]


def _m5_session_upto(m5_sorted: List[dict], session_date: date, cutoff_ist: datetime) -> List[dict]:
    out: List[dict] = []
    for b in m5_sorted:
        ts = str(b.get("timestamp") or "")
        if _ist_date_from_ts(ts) != session_date:
            continue
        t_end = _parse_bar_end_ist(ts)
        if t_end is None or t_end > cutoff_ist:
            continue
        out.append(b)
    return out


def _entry_price_at_cutoff(upstox: UpstoxService, fut_key: str, cutoff_ist: datetime) -> Optional[float]:
    try:
        c = upstox.get_historical_candles_by_instrument_key(fut_key, interval="minutes/1", days_back=120)
        s = _sort_candles(c)
        if not s:
            return None
        for bar in reversed(s):
            ts = str(bar.get("timestamp") or "")
            t_end = _parse_bar_end_ist(ts)
            if t_end is None:
                continue
            if t_end <= cutoff_ist:
                cl = float(bar.get("close") or 0)
                return cl if cl > 0 else None
        cl = float(s[-1].get("close") or 0)
        return cl if cl > 0 else None
    except Exception:
        return None


def _vix_at_cutoff(upstox: UpstoxService, session_date: date, cutoff_ist: datetime) -> Optional[float]:
    try:
        c = upstox.get_historical_candles_by_instrument_key(
            INDIA_VIX_KEY, interval="minutes/5", days_back=120
        )
        s = _sort_candles(c)
        last: Optional[float] = None
        for bar in s:
            ts = str(bar.get("timestamp") or "")
            if _ist_date_from_ts(ts) != session_date:
                continue
            t_end = _parse_bar_end_ist(ts)
            if t_end is None or t_end > cutoff_ist:
                continue
            lp = float(bar.get("close") or 0)
            if lp > 0:
                last = lp
        return last
    except Exception:
        return None


def score_symbol_backtest(
    upstox: UpstoxService,
    stock: str,
    fut_sym: str,
    fut_key: str,
    session_date: date,
    cutoff_ist: datetime,
    sentiment_map: Dict[str, float],
    sector_index: Optional[str] = None,
) -> Optional[ScoredPick]:
    daily_raw = upstox.get_historical_candles_by_instrument_key(
        fut_key, interval="days/1", days_back=120
    )
    daily = _daily_last_n_upto(daily_raw, session_date, 10)
    if len(daily) < 10:
        return None
    closes_d = [float(x["close"]) for x in daily]
    vols_d = [float(x.get("volume") or 0) for x in daily]
    avg_daily_vol = sum(vols_d) / 10.0
    obv_slope = compute_obv_slope_daily(closes_d, vols_d)

    m5_raw = upstox.get_historical_candles_by_instrument_key(
        fut_key, interval="minutes/5", days_back=120
    )
    m5 = _sort_candles(m5_raw)
    m5_today = _m5_session_upto(m5, session_date, cutoff_ist)
    if len(m5_today) < 15:
        return None

    highs = [float(b["high"]) for b in m5_today]
    lows = [float(b["low"]) for b in m5_today]
    opens = [float(b["open"]) for b in m5_today]
    closes = [float(b["close"]) for b in m5_today]
    vols = [float(b.get("volume") or 0) for b in m5_today]

    atr = wilder_atr_14(highs, lows, closes)
    if atr is None or atr <= 0:
        return None
    atr5 = wilder_atr(highs, lows, closes, 5)
    if atr5 is None or atr5 <= 0:
        return None
    atr5_14_ratio = float(atr5) / float(atr)
    adx = adx_14_value(highs, lows, closes)
    if adx is None:
        adx = 0.0

    vwap = session_vwap(highs, lows, closes, vols)
    last_close = closes[-1]
    cvatr = (last_close - vwap) / atr
    cvatr = max(-4.0, min(4.0, cvatr))

    frac = _session_elapsed_fraction(session_date, m5_today)
    vs = volume_surge_ratio(vols, avg_daily_vol, frac)
    if vs < 1.5:
        return None

    brick = max(atr * 0.1, last_close * 0.0005)
    rm = renko_momentum_score(closes, brick)
    ha = ha_trend_score(opens, highs, lows, closes)
    md, rd, sd = divergence_bundle(highs, lows, closes)

    cms = compute_cms_core(obv_slope, vs, adx, cvatr, rm, ha, md, rd, sd)

    sector_score = sector_score_as_of(upstox, stock, sector_index, session_date)
    if sector_score < -0.6:
        return None

    raw_sent = sentiment_map.get(stock.upper())
    if raw_sent is not None:
        try:
            comb = float(raw_sent)
        except (TypeError, ValueError):
            comb = 0.0
        if -0.4 <= comb <= 0.3:
            return None
    else:
        comb = 0.0

    final_cms = cms * (1.0 + 0.5 * sector_score) * (1.0 + comb)
    if abs(final_cms) <= 2.5:
        return None

    side = "LONG" if final_cms > 0 else "SHORT"
    return ScoredPick(
        stock=stock,
        fut_symbol=fut_sym or "",
        fut_instrument_key=fut_key,
        side=side,
        obv_slope=obv_slope,
        volume_surge=vs,
        adx_14=float(adx),
        atr_14=float(atr),
        atr5_14_ratio=atr5_14_ratio,
        renko_momentum=rm,
        ha_trend=ha,
        macd_div=md,
        rsi_div=rd,
        stoch_div=sd,
        cms=float(cms),
        final_cms=float(final_cms),
        sector_score=float(sector_score),
        combined_sentiment=comb,
    )


def _persist_backtest_rows(
    db: Session,
    session_date: date,
    simulated_asof: datetime,
    scan_time_label: str,
    vix: Optional[float],
    sentiment_source: str,
    sentiment_run_at_match_count: int,
    picks: List[Tuple[ScoredPick, float]],
) -> int:
    db.execute(
        text(
            """
            DELETE FROM backtest_smart_future
            WHERE session_date = :sd AND scan_time_label = :lbl
            """
        ),
        {"sd": session_date, "lbl": scan_time_label},
    )
    if not picks:
        db.commit()
        return 0
    n = 0
    for pick, entry_price in picks:
        atr = pick.atr_14
        hold_type = "positional" if abs(pick.final_cms) > 4.0 else "intraday"
        if pick.side == "LONG":
            sl = entry_price - atr * 1.5
            tgt = entry_price + atr * 3.0
        else:
            sl = entry_price + atr * 1.5
            tgt = entry_price - atr * 3.0
        db.execute(
            text(
                """
                INSERT INTO backtest_smart_future (
                    session_date, simulated_asof, scan_time_label,
                    stock, fut_symbol, fut_instrument_key, side,
                    obv_slope, volume_surge, adx_14, atr_14, atr5_14_ratio,
                    renko_momentum, ha_trend, macd_div, rsi_div, stoch_div,
                    cms, final_cms, sector_score, combined_sentiment,
                    entry_price, sl_price, target_price, hold_type,
                    trend_continuation, scan_trigger, vix_at_scan,
                    sentiment_source, sentiment_run_at_match_count
                ) VALUES (
                    :session_date, :simulated_asof, :scan_time_label,
                    :stock, :fut_symbol, :fut_instrument_key, :side,
                    :obv_slope, :volume_surge, :adx_14, :atr_14, :atr5_14_ratio,
                    :renko_momentum, :ha_trend, :macd_div, :rsi_div, :stoch_div,
                    :cms, :final_cms, :sector_score, :combined_sentiment,
                    :entry_price, :sl_price, :target_price, :hold_type,
                    NULL, :scan_trigger, :vix_at_scan,
                    :sentiment_source, :sentiment_run_at_match_count
                )
                """
            ),
            {
                "session_date": session_date,
                "simulated_asof": simulated_asof,
                "scan_time_label": scan_time_label,
                "stock": pick.stock,
                "fut_symbol": pick.fut_symbol,
                "fut_instrument_key": pick.fut_instrument_key,
                "side": pick.side,
                "obv_slope": pick.obv_slope,
                "volume_surge": pick.volume_surge,
                "adx_14": pick.adx_14,
                "atr_14": pick.atr_14,
                "atr5_14_ratio": pick.atr5_14_ratio,
                "renko_momentum": pick.renko_momentum,
                "ha_trend": pick.ha_trend,
                "macd_div": pick.macd_div,
                "rsi_div": pick.rsi_div,
                "stoch_div": pick.stoch_div,
                "cms": pick.cms,
                "final_cms": pick.final_cms,
                "sector_score": pick.sector_score,
                "combined_sentiment": pick.combined_sentiment,
                "entry_price": entry_price,
                "sl_price": sl,
                "target_price": tgt,
                "hold_type": hold_type,
                "scan_trigger": scan_time_label,
                "vix_at_scan": vix,
                "sentiment_source": sentiment_source[:2000]
                if sentiment_source
                else None,
                "sentiment_run_at_match_count": sentiment_run_at_match_count,
            },
        )
        n += 1
    db.commit()
    return n


def run_backtest_cutoff(
    db: Session,
    session_date: date,
    scan_time_label: str,
    cutoff_ist: datetime,
    *,
    throttle_sec: float = 0.04,
) -> Dict[str, Any]:
    """
    Run one backtest snapshot.

    Futures resolution:
    - Session dates **2026-02-01 .. 2026-03-31**: April-2026-expiry FUT from ``nse_instruments.json``
      per underlying (not ``arbitrage_master.currmth``).
    - Session dates **2026-04-01 onwards**: ``arbitrage_master.currmth_future_*`` at run time.

    Stock list and ``sector_index`` still come from ``arbitrage_master``.

    ``cutoff_ist`` must be timezone-aware (Asia/Kolkata recommended).
    """
    log = get_backtest_logger()
    if session_date < BACKTEST_MIN_SESSION_DATE:
        msg = f"session_date must be on or after {BACKTEST_MIN_SESSION_DATE.isoformat()}"
        log.warning("backtest rejected: %s (got %s)", msg, session_date)
        return {
            "error": msg,
            "session_date": str(session_date),
            "scan_time_label": scan_time_label,
            "futures_universe": FUTURES_UNIVERSE_CURRMTH,
        }
    futures_universe = (
        FUTURES_UNIVERSE_APRIL2026 if use_fixed_april_2026_futures(session_date) else FUTURES_UNIVERSE_CURRMTH
    )
    if cutoff_ist.tzinfo is None:
        cutoff_ist = IST.localize(cutoff_ist)
    else:
        cutoff_ist = cutoff_ist.astimezone(IST)
    simulated_asof = cutoff_ist

    sentiment_map, sent_note, sent_match = load_sentiment_map_for_session_date(db, session_date)

    def _init_upstox() -> UpstoxService:
        return UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

    upstox, uerr = call_with_retries(log, "upstox_init", _init_upstox, max_tries=2)
    if upstox is None:
        log.error("run_backtest_cutoff: no Upstox client: %s", uerr)
        return {
            "error": uerr or "upstox_init_failed",
            "session_date": str(session_date),
            "scan_time_label": scan_time_label,
            "futures_universe": futures_universe,
        }

    vix, _ = call_with_retries(
        log,
        f"vix_{session_date}_{scan_time_label}",
        lambda: _vix_at_cutoff(upstox, session_date, cutoff_ist),
        max_tries=2,
    )
    skip_long_vix = vix is not None and vix > 22.0

    rows = db.execute(
        text(
            """
            SELECT stock, currmth_future_symbol, currmth_future_instrument_key, sector_index
            FROM arbitrage_master
            WHERE currmth_future_instrument_key IS NOT NULL
              AND TRIM(currmth_future_instrument_key) <> ''
            ORDER BY stock
            """
        )
    ).fetchall()
    if not rows:
        log.warning("backtest: empty arbitrage_master universe")
        return {
            "skipped": "no_universe",
            "session_date": str(session_date),
            "scan_time_label": scan_time_label,
            "futures_universe": futures_universe,
        }

    apr_map: Optional[Dict[str, Tuple[str, str]]] = None
    if use_fixed_april_2026_futures(session_date):
        apr_map = load_april_2026_futures_by_underlying()
        if not apr_map:
            log.error("backtest: April 2026 futures map empty (instruments file)")
            return {
                "error": "No April 2026 equity futures found in nse_instruments.json; refresh instruments.",
                "session_date": str(session_date),
                "scan_time_label": scan_time_label,
                "futures_universe": futures_universe,
            }

    longs: List[ScoredPick] = []
    shorts: List[ScoredPick] = []

    for stock, fut_sym_am, fut_key_am, sector_index in rows:
        st = str(stock).strip().upper()
        if apr_map is not None:
            pair = apr_map.get(st)
            if not pair:
                continue
            fut_sym, fk = pair[0], pair[1]
        else:
            if not fut_key_am:
                continue
            fut_sym, fk = str(fut_sym_am or ""), str(fut_key_am).strip()

        def _do_score() -> Optional[ScoredPick]:
            return score_symbol_backtest(
                upstox,
                st,
                str(fut_sym or ""),
                fk,
                session_date,
                cutoff_ist,
                sentiment_map,
                str(sector_index).strip() if sector_index else None,
            )

        sc, err = call_with_retries(
            log,
            f"score_{st}_{session_date}_{scan_time_label}",
            _do_score,
            max_tries=2,
        )
        if throttle_sec > 0:
            time.sleep(throttle_sec)
        if sc is None:
            if err:
                log.debug("backtest skip %s: %s", st, err)
            continue
        if sc.side == "LONG":
            if skip_long_vix:
                continue
            longs.append(sc)
        else:
            shorts.append(sc)

    longs.sort(key=lambda x: x.final_cms, reverse=True)
    shorts.sort(key=lambda x: x.final_cms)
    best_long = longs[0] if longs else None
    best_short = shorts[0] if shorts else None

    picks_to_save: List[Tuple[ScoredPick, float]] = []

    for pick in (best_long, best_short):
        if not pick:
            continue

        def _do_entry() -> Optional[float]:
            # Historical replay: use 1m candles only (no live quote; avoids mixing current LTP into past dates).
            return _entry_price_at_cutoff(upstox, pick.fut_instrument_key, cutoff_ist)

        entry, ent_err = call_with_retries(
            log,
            f"entry_{pick.stock}_{session_date}_{scan_time_label}",
            _do_entry,
            max_tries=2,
        )
        if entry is None or entry <= 0:
            log.warning("backtest: no entry for %s (%s)", pick.stock, ent_err)
            continue
        picks_to_save.append((pick, float(entry)))

    saved = _persist_backtest_rows(
        db,
        session_date,
        simulated_asof,
        scan_time_label,
        vix,
        sent_note,
        sent_match,
        picks_to_save,
    )
    log.info(
        "backtest done session=%s label=%s saved=%s long=%s short=%s vix=%s",
        session_date,
        scan_time_label,
        saved,
        best_long.stock if best_long else None,
        best_short.stock if best_short else None,
        vix,
    )
    return {
        "session_date": str(session_date),
        "scan_time_label": scan_time_label,
        "saved": saved,
        "vix": vix,
        "best_long": best_long.stock if best_long else None,
        "best_short": best_short.stock if best_short else None,
        "skip_long_vix": skip_long_vix,
        "futures_universe": futures_universe,
    }


def _iter_trading_days(d0: date, d1: date) -> List[date]:
    out: List[date] = []
    d = d0
    while d <= d1:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def _combine_ist(session_date: date, hhmm: str) -> datetime:
    parts = hhmm.strip().split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    return IST.localize(datetime.combine(session_date, dtime(h, m)))


def run_backtest_date_range(
    db: Session,
    from_date: date,
    to_date: date,
    scan_time_labels: Tuple[str, ...] = ("09:30", "10:30"),
    *,
    throttle_sec: float = 0.04,
) -> Dict[str, Any]:
    """
    Run ``run_backtest_cutoff`` for each weekday in range and each scan label (IST wall-clock).

    Dates must be on or after ``BACKTEST_MIN_SESSION_DATE``. Futures keys always come from
    ``arbitrage_master.currmth_future_*`` at run time.
    """
    log = get_backtest_logger()
    ve = validate_backtest_date_bounds(from_date, to_date)
    if ve:
        log.warning("backtest range rejected: %s", ve)
        return {
            "error": ve,
            "results": [],
            "ok_slots": 0,
            "total_slots": 0,
            "futures_universe": FUTURES_UNIVERSE_CURRMTH,
        }
    summary: List[Dict[str, Any]] = []
    for d in _iter_trading_days(from_date, to_date):
        for lbl in scan_time_labels:
            try:
                co = _combine_ist(d, lbl)
                r = run_backtest_cutoff(db, d, lbl, co, throttle_sec=throttle_sec)
                r["ok"] = not r.get("error") and not r.get("skipped")
                summary.append(r)
            except Exception as e:
                log.exception("backtest fatal %s %s: %s", d, lbl, e)
                summary.append(
                    {
                        "session_date": str(d),
                        "scan_time_label": lbl,
                        "error": str(e),
                        "ok": False,
                    }
                )
    ok_n = sum(1 for x in summary if x.get("ok"))
    log.info("backtest range complete days=%s slots=%s ok_slots=%s", len(_iter_trading_days(from_date, to_date)), len(summary), ok_n)
    return {
        "results": summary,
        "ok_slots": ok_n,
        "total_slots": len(summary),
        "futures_universe": _aggregate_futures_universe_label(from_date, to_date),
    }

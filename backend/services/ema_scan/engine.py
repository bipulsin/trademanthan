"""Signal rules and outcome measurement for the EMA 30 reclaim scan.

Indicators are causal. The forward window starts at the next candle and is
never used to decide the signal.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services.ema_scan.calendar import (
    candle_start,
    holidays,
    in_cash_session,
    previous_trading_day,
    session_open_dt,
    trading_days,
)
from backend.services.ema_scan.indicators import adx_di_series, ema_series, wilder_atr_series
from backend.services.mtf_swing.bars import ensure_ist, parse_ts

HIT_LEVELS = (0.25, 0.5, 0.75, 1.0, 1.5, 2.0)
ADX_BUCKETS = ("25–30", "30–40", "40+")
DISTANCE_BUCKETS = ("0–0.1%", "0.1–0.25%", "0.25–0.5%")
DIST_BINS = (
    ("< 0", None, 0.0),
    ("0–0.25", 0.0, 0.25),
    ("0.25–0.5", 0.25, 0.5),
    ("0.5–0.75", 0.5, 0.75),
    ("0.75–1", 0.75, 1.0),
    ("1–1.5", 1.0, 1.5),
    ("1.5–2", 1.5, 2.0),
    ("2–3", 2.0, 3.0),
    ("3+", 3.0, None),
)


def default_params() -> Dict[str, Any]:
    return {
        "date_from": "2026-10-20",
        "date_to": "2026-11-13",
        "ema_period": 30,
        "adx_period": 14,
        "adx_min": 25.0,
        "max_above_ema": 0.005,
        "screen_candles": 6,
        "first_only": True,
        "window_minutes": 120,
        "bar_minutes": 5,
        "atr_period": 14,
        "atr_basis": "daily",
        "ema_stable_bars": 90,
    }


def candle_qualifies(
    open_: float,
    close: float,
    ema: Optional[float],
    adx: Optional[float],
    plus_di: Optional[float],
    minus_di: Optional[float],
    *,
    adx_min: float = 25.0,
    max_above_ema: float = 0.005,
) -> bool:
    if ema is None or adx is None or plus_di is None or minus_di is None:
        return False
    if ema <= 0:
        return False
    if not (open_ < ema):
        return False
    if not (close > ema):
        return False
    if not (close <= ema * (1.0 + float(max_above_ema))):
        return False
    if not (adx > float(adx_min)):
        return False
    if not (plus_di > minus_di):
        return False
    return True


def adx_bucket(adx: float) -> str:
    if adx < 30.0:
        return "25–30"
    if adx < 40.0:
        return "30–40"
    return "40+"


def distance_bucket(pct_above: float) -> str:
    if pct_above < 0.1:
        return "0–0.1%"
    if pct_above < 0.25:
        return "0.1–0.25%"
    return "0.25–0.5%"


def _minute_key(ts: datetime) -> str:
    local = ensure_ist(ts)
    return local.strftime("%Y-%m-%dT%H:%M")


def _bar_ohlc(bar: Dict[str, Any]) -> Tuple[float, float, float, float]:
    return (
        float(bar["open"]),
        float(bar["high"]),
        float(bar["low"]),
        float(bar["close"]),
    )


def first_touch_label(bars: Sequence[Dict[str, Any]], entry: float, atr: float) -> str:
    """Which 1 ATR side is touched first. Same-bar ties use the OHLC path.

    A candle that closes at or above its open is treated as open → low → high → close.
    A candle that closes below its open is treated as open → high → low → close.
    """
    up = entry + atr
    down = entry - atr
    for bar in bars:
        _open, high, low, close = _bar_ohlc(bar)
        hit_up = high >= up
        hit_down = low <= down
        if not hit_up and not hit_down:
            continue
        if hit_up and not hit_down:
            return "+1 ATR"
        if hit_down and not hit_up:
            return "-1 ATR"
        if close >= _open:
            return "-1 ATR"
        return "+1 ATR"
    return "neither"


def measure_window(
    window: Sequence[Dict[str, Any]],
    entry: float,
    atr: float,
    *,
    bar_minutes: int,
) -> Dict[str, Any]:
    highs = [float(b["high"]) for b in window]
    lows = [float(b["low"]) for b in window]
    max_high = highs[0]
    max_i = 0
    for i, high in enumerate(highs):
        if high > max_high:
            max_high = high
            max_i = i
    min_low = min(lows)
    mfe = (max_high - entry) / atr
    mae = (entry - min_low) / atr
    close_2h = (float(window[-1]["close"]) - entry) / atr
    hits = {f"{level:g}": bool(mfe + 1e-12 >= level) for level in HIT_LEVELS}
    return {
        "max_favorable_atr": mfe,
        "max_adverse_atr": mae,
        "close_at_2h_atr": close_2h,
        "time_to_max_favorable": int(max_i * bar_minutes),
        "first_touch": first_touch_label(window, entry, atr),
        "hits": hits,
    }


def _prepare_bars(rows: Sequence[Dict[str, Any]], *, bar_minutes: int, hols) -> List[Dict[str, Any]]:
    by_key: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        ts = parse_ts(row.get("timestamp"))
        if ts is None:
            continue
        ts = ensure_ist(ts)
        if not in_cash_session(ts, bar_minutes):
            continue
        if ts.date().weekday() >= 5 or ts.date() in hols:
            continue
        try:
            bar = {
                "timestamp": ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
        except (KeyError, TypeError, ValueError):
            continue
        by_key[_minute_key(ts)] = bar
    return [by_key[k] for k in sorted(by_key)]


def _prepare_daily(rows: Sequence[Dict[str, Any]]) -> Dict[date, Dict[str, Any]]:
    by_day: Dict[date, Dict[str, Any]] = {}
    for row in rows or []:
        ts = parse_ts(row.get("timestamp"))
        if ts is None:
            raw = str(row.get("timestamp") or "")[:10]
            try:
                day = date.fromisoformat(raw)
            except ValueError:
                continue
        else:
            day = ensure_ist(ts).date()
        try:
            bar = {
                "date": day,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
        except (KeyError, TypeError, ValueError):
            continue
        by_day[day] = bar
    return by_day


def _round(value: Optional[float], places: int) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), places)


def scan_contract(
    *,
    symbol: str,
    contract: str,
    instrument_key: str,
    bars_5m: Sequence[Dict[str, Any]],
    bars_daily: Sequence[Dict[str, Any]],
    days: Sequence[date],
    params: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Scan one contract over the sessions where it is the front month.

    ``days`` must already be limited to sessions that use this contract.
    """
    hols = holidays()
    bar_minutes = int(params["bar_minutes"])
    screen_n = int(params["screen_candles"])
    window_bars = int(params["window_minutes"]) // bar_minutes
    stable_need = int(params["ema_stable_bars"])
    ema_period = int(params["ema_period"])
    adx_period = int(params["adx_period"])
    atr_period = int(params["atr_period"])
    first_only = bool(params["first_only"])
    atr_basis = str(params.get("atr_basis") or "daily").lower()

    series = _prepare_bars(bars_5m, bar_minutes=bar_minutes, hols=hols)
    daily = _prepare_daily(bars_daily)
    signals: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    def _skip(day: date, reason: str, **extra: Any) -> None:
        row = {
            "date": day.isoformat(),
            "symbol": symbol,
            "contract": contract,
            "reason": reason,
        }
        row.update(extra)
        skipped.append(row)

    if not series:
        for day in days:
            _skip(day, "no 5-minute candles on this contract")
        return signals, skipped

    closes = [b["close"] for b in series]
    highs = [b["high"] for b in series]
    lows = [b["low"] for b in series]
    ema = ema_series(closes, ema_period)
    adx, plus_di, minus_di = adx_di_series(highs, lows, closes, adx_period)
    atr_m5 = wilder_atr_series(highs, lows, closes, atr_period)
    index_by_key = {_minute_key(b["timestamp"]): i for i, b in enumerate(series)}

    daily_days = sorted(daily)
    d_high = [daily[d]["high"] for d in daily_days]
    d_low = [daily[d]["low"] for d in daily_days]
    d_close = [daily[d]["close"] for d in daily_days]
    atr_daily = wilder_atr_series(d_high, d_low, d_close, atr_period)
    daily_index = {d: i for i, d in enumerate(daily_days)}

    for day in days:
        open_slots = [candle_start(day, n, bar_minutes) for n in range(1, screen_n + 1)]
        present = [ts for ts in open_slots if _minute_key(ts) in index_by_key]
        if not present:
            _skip(day, "no 5-minute candles in the 09:15–09:45 window")
            continue

        prior = 0
        session_open = session_open_dt(day)
        for bar in series:
            if bar["timestamp"] < session_open:
                prior += 1
            else:
                break
        first_idx = index_by_key[_minute_key(present[0])]
        if prior < stable_need or adx[first_idx] is None or ema[first_idx] is None:
            _skip(
                day,
                f"EMA {ema_period} / ADX({adx_period}) not stable "
                f"({prior} prior 5-minute bars on this contract, need {stable_need})",
            )
            continue

        atr_value: Optional[float] = None
        if atr_basis == "daily":
            prev = previous_trading_day(day, hols)
            if prev is None or prev not in daily_index:
                _skip(day, "daily ATR(14) not stable — missing the previous session's daily bar")
                continue
            atr_value = atr_daily[daily_index[prev]]
            if atr_value is None or atr_value <= 0:
                _skip(day, "daily ATR(14) not stable as of the previous day's close")
                continue

        for number, slot in enumerate(open_slots, start=1):
            key = _minute_key(slot)
            idx = index_by_key.get(key)
            if idx is None:
                continue
            bar = series[idx]
            if not candle_qualifies(
                bar["open"],
                bar["close"],
                ema[idx],
                adx[idx],
                plus_di[idx],
                minus_di[idx],
                adx_min=float(params["adx_min"]),
                max_above_ema=float(params["max_above_ema"]),
            ):
                continue

            if atr_basis != "daily":
                atr_value = atr_m5[idx]
                if atr_value is None or atr_value <= 0:
                    _skip(
                        day,
                        "5-minute ATR(14) not stable at the signal candle",
                        candle=number,
                        time=slot.strftime("%H:%M"),
                    )
                    if first_only:
                        break
                    continue

            window_keys = [
                _minute_key(slot + timedelta(minutes=bar_minutes * (step + 1)))
                for step in range(window_bars)
            ]
            if any(k not in index_by_key for k in window_keys):
                _skip(
                    day,
                    "forward window has missing 5-minute candles",
                    candle=number,
                    time=slot.strftime("%H:%M"),
                )
                if first_only:
                    break
                continue

            window = [series[index_by_key[k]] for k in window_keys]
            entry = float(window[0]["open"])
            measured = measure_window(window, entry, float(atr_value), bar_minutes=bar_minutes)
            ema_v = float(ema[idx])
            pct = (float(bar["close"]) / ema_v - 1.0) * 100.0
            adx_v = float(adx[idx])
            signals.append(
                {
                    "date": day.isoformat(),
                    "symbol": symbol,
                    "contract": contract,
                    "instrument_key": instrument_key,
                    "candle_number": number,
                    "time": slot.strftime("%H:%M"),
                    "open": _round(bar["open"], 2),
                    "close": _round(bar["close"], 2),
                    "ema30": _round(ema_v, 2),
                    "pct_above_ema": _round(pct, 4),
                    "adx": _round(adx_v, 2),
                    "plus_di": _round(float(plus_di[idx]), 2),
                    "minus_di": _round(float(minus_di[idx]), 2),
                    "atr": _round(float(atr_value), 4),
                    "atr_basis": atr_basis,
                    "entry": _round(entry, 2),
                    "max_favorable_atr": _round(measured["max_favorable_atr"], 4),
                    "max_adverse_atr": _round(measured["max_adverse_atr"], 4),
                    "close_at_2h_atr": _round(measured["close_at_2h_atr"], 4),
                    "time_to_max_favorable": measured["time_to_max_favorable"],
                    "first_touch": measured["first_touch"],
                    "hits": measured["hits"],
                    "adx_bucket": adx_bucket(adx_v),
                    "distance_bucket": distance_bucket(pct),
                }
            )
            if first_only:
                break
    return signals, skipped


def _percentile(values: Sequence[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (float(pct) / 100.0)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    weight = rank - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * weight


def _mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values)) / float(len(values))


def _bucket_rows(signals: Sequence[Dict[str, Any]], key: str, labels: Sequence[str]) -> List[Dict[str, Any]]:
    rows = []
    for label in labels:
        group = [s for s in signals if s.get(key) == label]
        mfes = [float(s["max_favorable_atr"]) for s in group]
        hit1 = sum(1 for s in group if s.get("hits", {}).get("1"))
        rows.append(
            {
                "label": label,
                "count": len(group),
                "avg_mfe": _round(_mean(mfes), 3),
                "median_mfe": _round(_percentile(mfes, 50), 3),
                "pct_hit_1": _round((100.0 * hit1 / len(group)) if group else 0.0, 1),
            }
        )
    return rows


def _distribution(signals: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts = {label: 0 for label, _lo, _hi in DIST_BINS}
    for sig in signals:
        value = float(sig["max_favorable_atr"])
        for label, lo, hi in DIST_BINS:
            if lo is None and value < float(hi):
                counts[label] += 1
                break
            if hi is None and value >= float(lo):
                counts[label] += 1
                break
            if lo is not None and hi is not None and float(lo) <= value < float(hi):
                counts[label] += 1
                break
    return [{"label": label, "count": counts[label]} for label, _lo, _hi in DIST_BINS]


def build_report(
    *,
    signals: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
    params: Dict[str, Any],
    universe: int,
    contract_rule: str,
) -> Dict[str, Any]:
    hols = holidays()
    start = date.fromisoformat(str(params["date_from"]))
    end = date.fromisoformat(str(params["date_to"]))
    days = trading_days(start, end, hols)
    by_day: Dict[str, List[Dict[str, Any]]] = {d.isoformat(): [] for d in days}
    for sig in signals:
        by_day.setdefault(sig["date"], []).append(sig)
    day_rows = []
    for day in days:
        key = day.isoformat()
        items = sorted(by_day.get(key, []), key=lambda s: (s["time"], s["symbol"]))
        day_rows.append(
            {
                "date": key,
                "count": len(items),
                "signals": [
                    {
                        "symbol": s["symbol"],
                        "contract": s["contract"],
                        "candle_number": s["candle_number"],
                        "time": s["time"],
                        "adx": s["adx"],
                        "pct_above_ema": s["pct_above_ema"],
                        "max_favorable_atr": s["max_favorable_atr"],
                    }
                    for s in items
                ],
            }
        )

    mfes = [float(s["max_favorable_atr"]) for s in signals]
    maes = [float(s["max_adverse_atr"]) for s in signals]
    hit_pct = {}
    for level in HIT_LEVELS:
        key = f"{level:g}"
        hits = sum(1 for s in signals if s.get("hits", {}).get(key))
        hit_pct[key] = _round((100.0 * hits / len(signals)) if signals else 0.0, 1)

    skipped_symbols = sorted({row["symbol"] for row in skipped if row.get("symbol")})
    ordered = sorted(signals, key=lambda s: (s["date"], s["time"], s["symbol"], s["candle_number"]))
    return {
        "ok": True,
        "params": params,
        "header": {
            "date_from": start.isoformat(),
            "date_to": end.isoformat(),
            "trading_days": len(days),
            "signals": len(ordered),
            "stocks_skipped": len(skipped_symbols),
            "skip_rows": len(skipped),
            "universe": int(universe),
            "contract_rule": contract_rule,
        },
        "summary": {
            "count": len(ordered),
            "avg_mfe": _round(_mean(mfes), 3),
            "median_mfe": _round(_percentile(mfes, 50), 3),
            "p25_mfe": _round(_percentile(mfes, 25), 3),
            "p75_mfe": _round(_percentile(mfes, 75), 3),
            "p90_mfe": _round(_percentile(mfes, 90), 3),
            "avg_mae": _round(_mean(maes), 3),
            "hit_pct": hit_pct,
        },
        "days": day_rows,
        "breakdowns": {
            "candle": _bucket_rows(
                ordered,
                "candle_number",
                list(range(1, int(params["screen_candles"]) + 1)),
            ),
            "adx": _bucket_rows(ordered, "adx_bucket", ADX_BUCKETS),
            "distance": _bucket_rows(ordered, "distance_bucket", DISTANCE_BUCKETS),
        },
        "distribution": _distribution(ordered),
        "signals": ordered,
        "skipped": skipped,
    }

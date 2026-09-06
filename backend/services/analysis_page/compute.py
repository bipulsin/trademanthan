"""Pure analytics for the Analysis page (no I/O).

Trend (10 bars)
---------------
Split the last 10 candles into prior 5 (older) vs recent 5 (newer).
Compare max(high) and min(low) of each half with a 0.5% equality band
(relative to the prior-half level).

- Bullish: recent high is higher AND recent low is higher (HH and HL).
- Bearish: recent high is lower AND recent low is lower (LH and LL).
- Sideways: highs approx equal AND lows approx equal, or mixed (HH+LL / LH+HL).

Weekly RSI: Wilder RSI(14) on weekly closes. Zones: >65 OverBought, <35 OverSold,
35–65 inclusive Mid.

Comparative RS: ratio = close_stock / close_index on overlapping daily dates.
Label vs SMA(50) of that ratio; |ratio - SMA| / SMA <= 0.1% is Mid.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

EQUAL_TOL = 0.005  # 0.5%
RS_ON_MA_EPS = 0.001  # 0.1%
RSI_PERIOD = 14
RS_MA_PERIOD = 50
TREND_BARS = 10
HAMMER_SHADOW_MULT = 2.0
HAMMER_UPPER_FRAC = 1.0

PATTERN_BULLISH_HARAMI = "Bullish Harami"
PATTERN_BEARISH_HARAMI = "Bearish Harami"
PATTERN_BULLISH_ENGULFING = "Bullish Engulfing"
PATTERN_BEARISH_ENGULFING = "Bearish Engulfing"
PATTERN_PIERCING = "Piercing"
PATTERN_DARK_CLOUD = "Dark Cloud Cover"
PATTERN_HAMMER = "Hammer"
PATTERN_SHOOTING_STAR = "Shooting Star"
PATTERN_INVERTED_HAMMER = "Inverted Hammer"
PATTERN_DOJI = "Doji"

ACTION_NO_TRADE = "--"
ACTION_BUY = "BUY"
ACTION_SELL = "SELL"
ACTION_SELL_EXHAUSTION = "SELL (Exhaustion)"
ACTION_WATCH = "WATCH"

_BUY_PATTERNS = frozenset({PATTERN_BULLISH_ENGULFING, PATTERN_PIERCING, PATTERN_HAMMER})
_SELL_PATTERNS = frozenset({PATTERN_BEARISH_ENGULFING, PATTERN_SHOOTING_STAR})
_WATCH_PATTERNS = frozenset(
    {PATTERN_BULLISH_HARAMI, PATTERN_BEARISH_HARAMI, PATTERN_INVERTED_HAMMER, PATTERN_DOJI}
)


def _f(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:
        return None
    return x


def sort_candles(candles: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(list(candles or []), key=lambda c: str(c.get("timestamp") or ""))


def candle_date(c: Dict[str, Any]) -> Optional[str]:
    ts = c.get("timestamp")
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.date().isoformat()
    s = str(ts).strip()
    if not s:
        return None
    if "T" in s:
        return s.split("T", 1)[0][:10]
    if " " in s:
        return s.split(" ", 1)[0][:10]
    return s[:10]


def _ohlc(c: Dict[str, Any]) -> Optional[Tuple[float, float, float, float]]:
    o, h, l, cl = _f(c.get("open")), _f(c.get("high")), _f(c.get("low")), _f(c.get("close"))
    if o is None or h is None or l is None or cl is None:
        return None
    if h < l:
        return None
    return o, h, l, cl


def _cmp_level(recent: float, prior: float, tol: float = EQUAL_TOL) -> int:
    """1 if recent > prior beyond tol, -1 if lower, 0 if approx equal."""
    base = abs(prior) if prior else 1.0
    if abs(recent - prior) / base <= tol:
        return 0
    return 1 if recent > prior else -1


def classify_trend(
    candles: Sequence[Dict[str, Any]],
    *,
    n: int = TREND_BARS,
    tol: float = EQUAL_TOL,
) -> Optional[str]:
    bars = sort_candles(candles)
    if len(bars) < n:
        return None
    window = bars[-n:]
    prior, recent = window[: n // 2], window[n // 2 :]
    ph = max((_ohlc(c) or (0, 0, 0, 0))[1] for c in prior)
    pl = min((_ohlc(c) or (0, 0, 0, 0))[2] for c in prior)
    rh = max((_ohlc(c) or (0, 0, 0, 0))[1] for c in recent)
    rl = min((_ohlc(c) or (0, 0, 0, 0))[2] for c in recent)
    hi = _cmp_level(rh, ph, tol)
    lo = _cmp_level(rl, pl, tol)
    if hi == 1 and lo == 1:
        return "Bullish"
    if hi == -1 and lo == -1:
        return "Bearish"
    return "Sideways"


def wilder_rsi(closes: Sequence[float], period: int = RSI_PERIOD) -> Optional[float]:
    n = len(closes)
    if n < period + 1:
        return None
    gains = [0.0]
    losses = [0.0]
    for i in range(1, n):
        ch = closes[i] - closes[i - 1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
    avg_g = sum(gains[1 : period + 1]) / float(period)
    avg_l = sum(losses[1 : period + 1]) / float(period)
    ag, al = avg_g, avg_l
    last = 50.0
    rs = ag / al if al > 0 else 99.0
    last = 100.0 - (100.0 / (1.0 + rs))
    for i in range(period + 1, n):
        ag = (ag * (period - 1) + gains[i]) / float(period)
        al = (al * (period - 1) + losses[i]) / float(period)
        rs = ag / al if al > 0 else 99.0
        last = 100.0 - (100.0 / (1.0 + rs))
    return last


def rsi_zone(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    if value > 65:
        return "OverBought"
    if value < 35:
        return "OverSold"
    return "Mid"


def weekly_rsi_zone(weekly_candles: Sequence[Dict[str, Any]], period: int = RSI_PERIOD) -> Tuple[Optional[float], Optional[str]]:
    bars = sort_candles(weekly_candles)
    closes = []
    for c in bars:
        v = _f(c.get("close"))
        if v is not None:
            closes.append(v)
    rsi = wilder_rsi(closes, period)
    return rsi, rsi_zone(rsi)


def align_closes_by_date(
    stock: Sequence[Dict[str, Any]],
    index: Sequence[Dict[str, Any]],
) -> List[Tuple[float, float]]:
    idx_map: Dict[str, float] = {}
    for c in sort_candles(index):
        d = candle_date(c)
        v = _f(c.get("close"))
        if d and v and v > 0:
            idx_map[d] = v
    out: List[Tuple[float, float]] = []
    for c in sort_candles(stock):
        d = candle_date(c)
        sv = _f(c.get("close"))
        if not d or sv is None or sv <= 0:
            continue
        iv = idx_map.get(d)
        if iv:
            out.append((sv, iv))
    return out


def rs_vs_ma50(
    stock: Sequence[Dict[str, Any]],
    index: Sequence[Dict[str, Any]],
    *,
    period: int = RS_MA_PERIOD,
    eps: float = RS_ON_MA_EPS,
) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    pairs = align_closes_by_date(stock, index)
    if len(pairs) < period:
        return None, None, None
    ratios = [s / i for s, i in pairs]
    last = ratios[-1]
    ma = sum(ratios[-period:]) / float(period)
    if ma == 0:
        return last, ma, None
    if abs(last - ma) / abs(ma) <= eps:
        return last, ma, "Mid"
    return last, ma, "Above" if last > ma else "Below"


def _body(o: float, c: float) -> Tuple[float, float, float]:
    lo, hi = (c, o) if c < o else (o, c)
    return lo, hi, abs(c - o)


def is_bullish(o: float, c: float) -> bool:
    return c > o


def is_bearish(o: float, c: float) -> bool:
    return c < o


def detect_bullish_harami(prev: Dict[str, Any], curr: Dict[str, Any]) -> bool:
    a, b = _ohlc(prev), _ohlc(curr)
    if not a or not b:
        return False
    po, ph, pl, pc = a
    co, ch, cl, cc = b
    if not is_bearish(po, pc) or not is_bullish(co, cc):
        return False
    plo, phi, pbody = _body(po, pc)
    clo, chi, cbody = _body(co, cc)
    if pbody <= 0 or cbody >= pbody:
        return False
    return clo >= plo and chi <= phi


def detect_bearish_harami(prev: Dict[str, Any], curr: Dict[str, Any]) -> bool:
    a, b = _ohlc(prev), _ohlc(curr)
    if not a or not b:
        return False
    po, ph, pl, pc = a
    co, ch, cl, cc = b
    if not is_bullish(po, pc) or not is_bearish(co, cc):
        return False
    plo, phi, pbody = _body(po, pc)
    clo, chi, cbody = _body(co, cc)
    if pbody <= 0 or cbody >= pbody:
        return False
    return clo >= plo and chi <= phi


def detect_bullish_engulfing(prev: Dict[str, Any], curr: Dict[str, Any]) -> bool:
    a, b = _ohlc(prev), _ohlc(curr)
    if not a or not b:
        return False
    po, ph, pl, pc = a
    co, ch, cl, cc = b
    if not is_bearish(po, pc) or not is_bullish(co, cc):
        return False
    plo, phi, pbody = _body(po, pc)
    clo, chi, cbody = _body(co, cc)
    if cbody <= pbody:
        return False
    return clo <= plo and chi >= phi


def detect_bearish_engulfing(prev: Dict[str, Any], curr: Dict[str, Any]) -> bool:
    a, b = _ohlc(prev), _ohlc(curr)
    if not a or not b:
        return False
    po, ph, pl, pc = a
    co, ch, cl, cc = b
    if not is_bullish(po, pc) or not is_bearish(co, cc):
        return False
    plo, phi, pbody = _body(po, pc)
    clo, chi, cbody = _body(co, cc)
    if cbody <= pbody:
        return False
    return clo <= plo and chi >= phi


def detect_piercing(prev: Dict[str, Any], curr: Dict[str, Any]) -> bool:
    a, b = _ohlc(prev), _ohlc(curr)
    if not a or not b:
        return False
    po, ph, pl, pc = a
    co, ch, cl, cc = b
    if not is_bearish(po, pc) or not is_bullish(co, cc):
        return False
    mid = (po + pc) / 2.0
    if co >= pl:
        return False
    if cc <= mid:
        return False
    if cc >= po:
        return False
    return True


def detect_dark_cloud_cover(prev: Dict[str, Any], curr: Dict[str, Any]) -> bool:
    a, b = _ohlc(prev), _ohlc(curr)
    if not a or not b:
        return False
    po, ph, pl, pc = a
    co, ch, cl, cc = b
    if not is_bullish(po, pc) or not is_bearish(co, cc):
        return False
    mid = (po + pc) / 2.0
    if co <= ph:
        return False
    if cc >= mid:
        return False
    if cc <= po:
        return False
    return True


def detect_hammer(curr: Dict[str, Any]) -> bool:
    ohlc = _ohlc(curr)
    if not ohlc:
        return False
    o, h, l, c = ohlc
    body = abs(c - o)
    rng = h - l
    if rng <= 0:
        return False
    lower = min(o, c) - l
    upper = h - max(o, c)
    if body <= 0:
        body = rng * 0.01
    if lower < HAMMER_SHADOW_MULT * body:
        return False
    if upper > HAMMER_UPPER_FRAC * body:
        return False
    return True


def detect_shooting_star(curr: Dict[str, Any]) -> bool:
    ohlc = _ohlc(curr)
    if not ohlc:
        return False
    o, h, l, c = ohlc
    body = abs(c - o)
    rng = h - l
    if rng <= 0:
        return False
    lower = min(o, c) - l
    upper = h - max(o, c)
    if body <= 0:
        body = rng * 0.01
    if upper < HAMMER_SHADOW_MULT * body:
        return False
    if lower > HAMMER_UPPER_FRAC * body:
        return False
    return True


def detect_patterns(daily: Sequence[Dict[str, Any]]) -> List[str]:
    bars = sort_candles(daily)
    if not bars:
        return []
    found: List[str] = []
    last = bars[-1]
    if detect_hammer(last):
        found.append(PATTERN_HAMMER)
    if detect_shooting_star(last):
        found.append(PATTERN_SHOOTING_STAR)
    if len(bars) >= 2:
        prev, curr = bars[-2], bars[-1]
        checks = (
            (PATTERN_BULLISH_HARAMI, detect_bullish_harami),
            (PATTERN_BEARISH_HARAMI, detect_bearish_harami),
            (PATTERN_BULLISH_ENGULFING, detect_bullish_engulfing),
            (PATTERN_BEARISH_ENGULFING, detect_bearish_engulfing),
            (PATTERN_PIERCING, detect_piercing),
            (PATTERN_DARK_CLOUD, detect_dark_cloud_cover),
        )
        for name, fn in checks:
            if fn(prev, curr):
                found.append(name)
    return found


def _search(haystack: Optional[str], needle: str) -> bool:
    if haystack is None:
        return False
    return needle.upper() in str(haystack).upper()


def _rs_dir(rs_ma50: Optional[str]) -> Optional[str]:
    if rs_ma50 == "Above":
        return "UP"
    if rs_ma50 == "Below":
        return "DOWN"
    return None


def _pattern_names(patterns: Optional[Sequence[str]]) -> List[str]:
    out: List[str] = []
    for p in patterns or []:
        s = str(p).strip() if p is not None else ""
        if not s or s in ("—", "-", "--", "none", "None"):
            continue
        out.append(s)
    return out


def _any_pattern(names: Sequence[str], wanted: frozenset) -> bool:
    return any(n in wanted for n in names)


def classify_action(
    weekly_trend: Optional[str] = None,
    daily_trend: Optional[str] = None,
    weekly_rsi_zone: Optional[str] = None,
    rs_ma50: Optional[str] = None,
    patterns: Optional[Sequence[str]] = None,
) -> str:
    """Spreadsheet IFS: first match wins. No pattern → always --."""
    names = _pattern_names(patterns)
    if not names:
        return ACTION_NO_TRADE
    rs = _rs_dir(rs_ma50)
    w_bull = _search(weekly_trend, "BULL")
    w_bear = _search(weekly_trend, "BEAR")
    d_bear = _search(daily_trend, "BEAR")
    # Spreadsheet SEARCH("OveBo") is a typo for OverBought; match both.
    rsi_ob = _search(weekly_rsi_zone, "OveBo") or _search(weekly_rsi_zone, "OverBo")
    if w_bull and rs == "UP" and _any_pattern(names, _BUY_PATTERNS):
        return ACTION_BUY
    if w_bear and d_bear and rs == "DOWN" and _any_pattern(names, _SELL_PATTERNS):
        return ACTION_SELL
    if w_bull and rsi_ob and _any_pattern(names, _SELL_PATTERNS):
        return ACTION_SELL_EXHAUSTION
    if _any_pattern(names, _WATCH_PATTERNS) or (w_bull and rs == "DOWN"):
        return ACTION_WATCH
    return ACTION_NO_TRADE


def daily_to_weekly(daily: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate daily OHLC into calendar weeks (Mon–Sun, last close of week)."""
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for c in sort_candles(daily):
        d = candle_date(c)
        if not d:
            continue
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            continue
        iso = dt.isocalendar()
        key = f"{iso[0]}-W{iso[1]:02d}"
        buckets.setdefault(key, []).append(c)
    out: List[Dict[str, Any]] = []
    for key in sorted(buckets):
        bars = buckets[key]
        ohlcs = [_ohlc(c) for c in bars]
        ohlcs = [x for x in ohlcs if x]
        if not ohlcs:
            continue
        out.append(
            {
                "timestamp": candle_date(bars[-1]) + "T00:00:00",
                "open": ohlcs[0][0],
                "high": max(x[1] for x in ohlcs),
                "low": min(x[2] for x in ohlcs),
                "close": ohlcs[-1][3],
            }
        )
    return out

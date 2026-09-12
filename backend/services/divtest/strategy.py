"""MACD regular divergence + histogram flip strategy."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set


def _ema(values: List[Optional[float]], period: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if not values or period <= 0:
        return out
    k = 2 / (period + 1)
    prev: Optional[float] = None
    total = 0.0
    count = 0
    for i, v in enumerate(values):
        if v is None:
            out[i] = prev
            continue
        if prev is None:
            total += float(v)
            count += 1
            if count == period:
                prev = total / period
                out[i] = prev
        else:
            prev = float(v) * k + prev * (1 - k)
            out[i] = prev
    return out


def compute_macd(
    closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9
) -> Dict[str, List[Optional[float]]]:
    ema_fast = _ema(list(closes), fast)
    ema_slow = _ema(list(closes), slow)
    macd_line: List[Optional[float]] = [
        (ema_fast[i] - ema_slow[i])
        if ema_fast[i] is not None and ema_slow[i] is not None
        else None
        for i in range(len(closes))
    ]
    # Signal EMA over macd_line, ignoring leading Nones
    sig: List[Optional[float]] = [None] * len(macd_line)
    k = 2 / (signal + 1)
    prev: Optional[float] = None
    buf: List[float] = []
    for i, v in enumerate(macd_line):
        if v is None:
            continue
        if prev is None:
            buf.append(float(v))
            if len(buf) == signal:
                prev = sum(buf) / signal
                sig[i] = prev
        else:
            prev = float(v) * k + prev * (1 - k)
            sig[i] = prev
    hist: List[Optional[float]] = [
        (macd_line[i] - sig[i]) if macd_line[i] is not None and sig[i] is not None else None
        for i in range(len(closes))
    ]
    return {"macd_line": macd_line, "signal": sig, "histogram": hist}


def compute_atr(candles: List[Dict[str, Any]], period: int = 14) -> List[Optional[float]]:
    trs: List[Optional[float]] = [None] * len(candles)
    for i, c in enumerate(candles):
        if i == 0:
            trs[i] = float(c["high"]) - float(c["low"])
            continue
        prev_close = float(candles[i - 1]["close"])
        trs[i] = max(
            float(c["high"]) - float(c["low"]),
            abs(float(c["high"]) - prev_close),
            abs(float(c["low"]) - prev_close),
        )
    atr: List[Optional[float]] = [None] * len(candles)
    prev: Optional[float] = None
    total = 0.0
    for i, tr in enumerate(trs):
        if tr is None:
            continue
        if prev is None:
            total += tr
            if i == period - 1:
                prev = total / period
                atr[i] = prev
        else:
            prev = (prev * (period - 1) + tr) / period
            atr[i] = prev
    return atr


def find_swing_points(values: List[Optional[float]], order: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    highs: List[Dict[str, Any]] = []
    lows: List[Dict[str, Any]] = []
    for i in range(order, len(values) - order):
        v = values[i]
        if v is None:
            continue
        is_high = True
        is_low = True
        for j in range(1, order + 1):
            left = values[i - j]
            right = values[i + j]
            if left is None or right is None:
                is_high = is_low = False
                break
            if not (v > left and v >= right):
                is_high = False
            if not (v < left and v <= right):
                is_low = False
        if is_high:
            highs.append({"index": i, "value": float(v)})
        if is_low:
            lows.append({"index": i, "value": float(v)})
    return {"highs": highs, "lows": lows}


def _nearest(swings: List[Dict[str, Any]], target: int, max_dist: int) -> Optional[Dict[str, Any]]:
    best = None
    best_d = 10**9
    for s in swings:
        d = abs(int(s["index"]) - target)
        if d <= max_dist and d < best_d:
            best = s
            best_d = d
    return best


def detect_divergences_at(
    price_swings: Dict[str, List[Dict[str, Any]]],
    macd_swings: Dict[str, List[Dict[str, Any]]],
    confirm_index: int,
    lookback: int,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    min_idx = max(0, confirm_index - lookback)
    macd_min = max(0, confirm_index - lookback - 8)

    price_highs = [s for s in price_swings["highs"] if min_idx <= s["index"] <= confirm_index]
    macd_highs = [s for s in macd_swings["highs"] if macd_min <= s["index"] <= confirm_index]
    if len(price_highs) >= 2 and len(macd_highs) >= 2:
        p2, p1 = price_highs[-1], price_highs[-2]
        if p2["index"] >= confirm_index - 2:
            m2 = _nearest(macd_highs, p2["index"], 6)
            prior = [s for s in macd_highs if s["index"] < ((m2["index"] if m2 else p2["index"]) - 2)]
            m1 = _nearest(prior, p1["index"], 10)
            if m1 and m2 and p2["value"] > p1["value"] and m2["value"] < m1["value"]:
                results.append({"type": "bearish", "confirm_index": p2["index"]})

    price_lows = [s for s in price_swings["lows"] if min_idx <= s["index"] <= confirm_index]
    macd_lows = [s for s in macd_swings["lows"] if macd_min <= s["index"] <= confirm_index]
    if len(price_lows) >= 2 and len(macd_lows) >= 2:
        p2, p1 = price_lows[-1], price_lows[-2]
        if p2["index"] >= confirm_index - 2:
            m2 = _nearest(macd_lows, p2["index"], 6)
            prior = [s for s in macd_lows if s["index"] < ((m2["index"] if m2 else p2["index"]) - 2)]
            m1 = _nearest(prior, p1["index"], 10)
            if m1 and m2 and p2["value"] < p1["value"] and m2["value"] > m1["value"]:
                results.append({"type": "bullish", "confirm_index": p2["index"]})
    return results


def histogram_flip_signal(histogram: List[Optional[float]], i: int) -> Optional[str]:
    if i < 2:
        return None
    a, b, c = histogram[i - 2], histogram[i - 1], histogram[i]
    if a is None or b is None or c is None:
        return None
    if b < 0 <= c:
        return "bullish_flip"
    if b > 0 >= c:
        return "bearish_flip"
    if a < 0 and b < 0 and c < 0 and abs(b) > abs(a) and abs(c) < abs(b):
        return "bullish_flip"
    if a > 0 and b > 0 and c > 0 and abs(b) > abs(a) and abs(c) < abs(b):
        return "bearish_flip"
    if b < 0 and c > b and a <= b:
        return "bullish_flip"
    if b > 0 and c < b and a >= b:
        return "bearish_flip"
    return None


def _opposite(side: str, flip: Optional[str]) -> bool:
    if side == "LONG":
        return flip == "bearish_flip"
    if side == "SHORT":
        return flip == "bullish_flip"
    return False


def _r2(n: float) -> float:
    return round(float(n) * 100) / 100


def _finalize(
    open_trade: Dict[str, Any],
    *,
    exit_index: int,
    exit_dt: Any,
    exit_price: float,
    exit_reason: str,
    exit_detail: str,
    opts: Dict[str, Any],
) -> Dict[str, Any]:
    direction = 1 if open_trade["side"] == "LONG" else -1
    raw = (exit_price - open_trade["entry_price"]) * opts["qty_per_lot"] * direction
    fees = (opts["brokerage_per_side"] * 2) if opts["brokerage_enabled"] else 0.0
    return {
        "instrument": opts["instrument"],
        "timeframe": opts["timeframe"],
        "entry_datetime": open_trade["entry_datetime"],
        "entry_price": _r2(open_trade["entry_price"]),
        "side": open_trade["side"],
        "exit_datetime": exit_dt,
        "exit_price": _r2(exit_price),
        "qty_per_lot": opts["qty_per_lot"],
        "pnl_inr": _r2(raw - fees),
        "raw_pnl_inr": _r2(raw),
        "fees_inr": fees,
        "divergence_type": open_trade["divergence_type"],
        "exit_reason": exit_reason,
        "exit_detail": exit_detail or exit_reason,
        "stop": _r2(open_trade["stop"]) if open_trade.get("stop") is not None else None,
        "target": _r2(open_trade["target"]) if open_trade.get("target") is not None else None,
        "entry_index": open_trade["entry_index"],
        "exit_index": exit_index,
        "holding_bars": exit_index - open_trade["entry_index"],
    }


def run_strategy(candles: List[Dict[str, Any]], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    opts = {
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
        "divergence_lookback": 25,
        "histogram_flip_window": 10,
        "entry_mode": "next_open",
        "exit_opposite_flip": True,
        "exit_stop_loss": True,
        "exit_time_based": True,
        "atr_period": 14,
        "atr_stop_multiplier": 1.5,
        "atr_target_multiplier": 2.5,
        "max_holding_bars": 40,
        "swing_order": 3,
        "instrument": "UNKNOWN",
        "timeframe": "15min",
        "qty_per_lot": 1,
        "brokerage_per_side": 20.0,
        "brokerage_enabled": True,
    }
    if options:
        opts.update(options)

    if not candles or len(candles) < opts["macd_slow"] + opts["macd_signal"] + 10:
        return {"trades": [], "meta": {"reason": "insufficient_candles", "count": len(candles or [])}}

    closes = [float(c["close"]) for c in candles]
    macd = compute_macd(closes, opts["macd_fast"], opts["macd_slow"], opts["macd_signal"])
    atr = compute_atr(candles, opts["atr_period"])
    price_swings = find_swing_points(closes, opts["swing_order"])
    macd_swings = find_swing_points(macd["macd_line"], opts["swing_order"])

    trades: List[Dict[str, Any]] = []
    pending = None
    open_trade = None
    seen: Set[str] = set()

    for i in range(len(candles)):
        if open_trade and i > open_trade["entry_index"]:
            bars_held = i - open_trade["entry_index"]
            c = candles[i]
            exit_reason = None
            exit_price = None
            if opts["exit_stop_loss"] and open_trade.get("stop") is not None:
                if open_trade["side"] == "LONG":
                    if float(c["low"]) <= open_trade["stop"]:
                        exit_reason, exit_price = "stop_loss", open_trade["stop"]
                    elif open_trade.get("target") is not None and float(c["high"]) >= open_trade["target"]:
                        exit_reason, exit_price = "stop_loss", open_trade["target"]
                else:
                    if float(c["high"]) >= open_trade["stop"]:
                        exit_reason, exit_price = "stop_loss", open_trade["stop"]
                    elif open_trade.get("target") is not None and float(c["low"]) <= open_trade["target"]:
                        exit_reason, exit_price = "stop_loss", open_trade["target"]
            if not exit_reason and opts["exit_opposite_flip"]:
                flip = histogram_flip_signal(macd["histogram"], i)
                if flip and _opposite(open_trade["side"], flip):
                    exit_reason, exit_price = "opposite_flip", float(c["close"])
            if not exit_reason and opts["exit_time_based"] and bars_held >= opts["max_holding_bars"]:
                exit_reason, exit_price = "time_exit", float(c["close"])
            if exit_reason:
                trades.append(
                    _finalize(
                        open_trade,
                        exit_index=i,
                        exit_dt=c.get("timestamp") or c.get("datetime"),
                        exit_price=float(exit_price),
                        exit_reason=exit_reason,
                        exit_detail=exit_reason,
                        opts=opts,
                    )
                )
                open_trade = None
                continue

        if not open_trade:
            for d in detect_divergences_at(price_swings, macd_swings, i, opts["divergence_lookback"]):
                key = f"{d['type']}:{d['confirm_index']}"
                if key in seen:
                    continue
                seen.add(key)
                pending = {
                    "divergence": d,
                    "armed_index": d["confirm_index"],
                    "expires_at": d["confirm_index"] + opts["histogram_flip_window"],
                }

        if not open_trade and pending and pending["armed_index"] <= i <= pending["expires_at"]:
            flip = histogram_flip_signal(macd["histogram"], i)
            want_bull = pending["divergence"]["type"] == "bullish"
            ok = (want_bull and flip == "bullish_flip") or ((not want_bull) and flip == "bearish_flip")
            if ok:
                side = "LONG" if want_bull else "SHORT"
                if opts["entry_mode"] == "same_close":
                    entry_index = i
                    entry_price = float(candles[i]["close"])
                else:
                    entry_index = i + 1
                    if entry_index >= len(candles):
                        pending = None
                        continue
                    entry_price = float(candles[entry_index]["open"])
                entry_atr = atr[min(i, len(atr) - 1)] or 0.0
                stop_dist = float(entry_atr) * opts["atr_stop_multiplier"]
                tgt_dist = float(entry_atr) * opts["atr_target_multiplier"]
                open_trade = {
                    "side": side,
                    "divergence_type": pending["divergence"]["type"],
                    "entry_index": entry_index,
                    "entry_datetime": candles[entry_index].get("timestamp")
                    or candles[entry_index].get("datetime"),
                    "entry_price": entry_price,
                    "stop": (
                        (entry_price - stop_dist)
                        if (opts["exit_stop_loss"] and entry_atr and side == "LONG")
                        else ((entry_price + stop_dist) if (opts["exit_stop_loss"] and entry_atr) else None)
                    ),
                    "target": (
                        (entry_price + tgt_dist)
                        if (opts["exit_stop_loss"] and entry_atr and side == "LONG")
                        else ((entry_price - tgt_dist) if (opts["exit_stop_loss"] and entry_atr) else None)
                    ),
                }
                pending = None

        if pending and i > pending["expires_at"]:
            pending = None

    if open_trade:
        last = candles[-1]
        trades.append(
            _finalize(
                open_trade,
                exit_index=len(candles) - 1,
                exit_dt=last.get("timestamp") or last.get("datetime"),
                exit_price=float(last["close"]),
                exit_reason="time_exit",
                exit_detail="series_end",
                opts=opts,
            )
        )

    return {
        "trades": trades,
        "meta": {
            "candle_count": len(candles),
            "macd_ready": sum(1 for v in macd["macd_line"] if v is not None),
            "divergence_candidates": len(seen),
        },
    }


def analyze_trades(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    empty = {
        "total_trades": 0,
        "win_rate_pct": 0.0,
        "total_pnl": 0.0,
        "avg_pnl": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "max_drawdown_inr": 0.0,
        "max_drawdown_pct": 0.0,
        "profit_factor": 0.0,
        "largest_win": 0.0,
        "largest_loss": 0.0,
        "avg_holding_bars": 0.0,
        "equity_curve": [],
    }
    if not trades:
        return empty
    sorted_t = sorted(trades, key=lambda t: str(t.get("exit_datetime") or ""))
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    curve = []
    wins = 0
    gross_profit = 0.0
    gross_loss = 0.0
    win_sum = 0.0
    loss_sum = 0.0
    largest_win = float("-inf")
    largest_loss = float("inf")
    hold_sum = 0
    for t in sorted_t:
        pnl = float(t.get("pnl_inr") or 0)
        equity += pnl
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
        curve.append({"t": t.get("exit_datetime"), "equity": _r2(equity), "pnl": pnl})
        hold_sum += int(t.get("holding_bars") or 0)
        if pnl > 0:
            wins += 1
            gross_profit += pnl
            win_sum += pnl
            largest_win = max(largest_win, pnl)
        elif pnl < 0:
            gross_loss += -pnl
            loss_sum += pnl
            largest_loss = min(largest_loss, pnl)
        else:
            largest_win = max(largest_win, pnl)
            largest_loss = min(largest_loss, pnl)
    n = len(sorted_t)
    loss_count = sum(1 for t in sorted_t if float(t.get("pnl_inr") or 0) < 0)
    return {
        "total_trades": n,
        "win_rate_pct": _r2((wins / n) * 100),
        "total_pnl": _r2(equity),
        "avg_pnl": _r2(equity / n),
        "avg_win": _r2(win_sum / wins) if wins else 0.0,
        "avg_loss": _r2(loss_sum / loss_count) if loss_count else 0.0,
        "max_drawdown_inr": _r2(max_dd),
        "max_drawdown_pct": _r2((max_dd / peak) * 100) if peak > 0 else _r2(max_dd),
        "profit_factor": _r2(gross_profit / gross_loss)
        if gross_loss > 0
        else (999.0 if gross_profit > 0 else 0.0),
        "largest_win": 0.0 if largest_win == float("-inf") else _r2(largest_win),
        "largest_loss": 0.0 if largest_loss == float("inf") else _r2(largest_loss),
        "avg_holding_bars": _r2(hold_sum / n),
        "equity_curve": curve,
    }

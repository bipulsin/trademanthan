"""Aggregate metrics for OpenHighLow backtest results."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Sequence


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def summarize(trades: Sequence[Dict[str, Any]], skips: Sequence[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    trades = list(trades or [])
    n = len(trades)
    wins = [t for t in trades if _f(t.get("rupee_pnl")) > 0]
    losses = [t for t in trades if _f(t.get("rupee_pnl")) <= 0]
    net = sum(_f(t.get("rupee_pnl")) for t in trades)
    pts = sum(_f(t.get("points_pnl")) for t in trades)
    avg_r = (sum(_f(t.get("r_multiple")) for t in trades) / n) if n else 0.0

    by_side: Dict[str, Dict[str, Any]] = {}
    for side in ("BULLISH", "BEARISH"):
        sub = [t for t in trades if t.get("side") == side]
        sn = len(sub)
        by_side[side] = {
            "trades": sn,
            "wins": sum(1 for t in sub if _f(t.get("rupee_pnl")) > 0),
            "win_rate": (sum(1 for t in sub if _f(t.get("rupee_pnl")) > 0) / sn * 100) if sn else 0.0,
            "net_pnl": sum(_f(t.get("rupee_pnl")) for t in sub),
        }

    by_exit = Counter(str(t.get("exit_reason") or "") for t in trades)
    by_weekday = Counter()
    daily_pnl: Dict[str, float] = defaultdict(float)
    stock_pnl: Dict[str, float] = defaultdict(float)
    for t in trades:
        d = str(t.get("date") or "")
        daily_pnl[d] += _f(t.get("rupee_pnl"))
        stock_pnl[str(t.get("symbol") or "")] += _f(t.get("rupee_pnl"))
        try:
            from datetime import date as _date

            wd = _date.fromisoformat(d).strftime("%A")
        except Exception:
            wd = "?"
        by_weekday[wd] += 1

    # Equity curve (cumulative rupee PnL in trade order by date+entry).
    ordered = sorted(trades, key=lambda t: (str(t.get("date")), str(t.get("entry_time"))))
    equity: List[Dict[str, Any]] = []
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for t in ordered:
        cum += _f(t.get("rupee_pnl"))
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)
        equity.append({"date": t.get("date"), "symbol": t.get("symbol"), "cum_pnl": cum})

    flag_yes = [t for t in trades if t.get("open_eq_extreme") == "YES"]
    flag_no = [t for t in trades if t.get("open_eq_extreme") != "YES"]

    def _flag_block(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        rn = len(rows)
        return {
            "trades": rn,
            "wins": sum(1 for t in rows if _f(t.get("rupee_pnl")) > 0),
            "win_rate": (sum(1 for t in rows if _f(t.get("rupee_pnl")) > 0) / rn * 100) if rn else 0.0,
            "net_pnl": sum(_f(t.get("rupee_pnl")) for t in rows),
            "avg_r": (sum(_f(t.get("r_multiple")) for t in rows) / rn) if rn else 0.0,
        }

    flag_cmp = {
        "YES": {
            "all": _flag_block(flag_yes),
            "BULLISH": _flag_block([t for t in flag_yes if t.get("side") == "BULLISH"]),
            "BEARISH": _flag_block([t for t in flag_yes if t.get("side") == "BEARISH"]),
        },
        "NO": {
            "all": _flag_block(flag_no),
            "BULLISH": _flag_block([t for t in flag_no if t.get("side") == "BULLISH"]),
            "BEARISH": _flag_block([t for t in flag_no if t.get("side") == "BEARISH"]),
        },
    }

    ranked = sorted(stock_pnl.items(), key=lambda kv: kv[1], reverse=True)
    top10 = [{"symbol": s, "pnl": p} for s, p in ranked[:10]]
    bottom10 = [{"symbol": s, "pnl": p} for s, p in ranked[-10:][::-1]] if ranked else []

    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / n * 100.0) if n else 0.0,
        "net_pnl": net,
        "points_pnl": pts,
        "avg_r": avg_r,
        "max_drawdown": max_dd,
        "by_side": by_side,
        "by_exit": dict(by_exit),
        "by_weekday": dict(by_weekday),
        "daily_pnl": dict(sorted(daily_pnl.items())),
        "equity": equity,
        "top10": top10,
        "bottom10": bottom10,
        "flag_comparison": flag_cmp,
        "skips": list(skips or []),
        "skip_count": len(skips or []),
    }

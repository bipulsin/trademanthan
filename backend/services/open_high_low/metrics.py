"""Aggregate metrics for OpenHighLow backtest results."""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Sequence


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _block(
    trades: Sequence[Dict[str, Any]],
    *,
    pnl_key: str = "rupee_pnl",
) -> Dict[str, Any]:
    trades = list(trades or [])
    n = len(trades)
    wins = [t for t in trades if _f(t.get(pnl_key)) > 0]
    losses = [t for t in trades if _f(t.get(pnl_key)) <= 0]
    gross_win = sum(_f(t.get(pnl_key)) for t in wins)
    gross_loss = abs(sum(_f(t.get(pnl_key)) for t in losses))
    pf = (gross_win / gross_loss) if gross_loss > 1e-12 else (float("inf") if gross_win > 0 else 0.0)
    net = sum(_f(t.get(pnl_key)) for t in trades)
    avg_r = (sum(_f(t.get("r_multiple")) for t in trades) / n) if n else 0.0

    ordered = sorted(trades, key=lambda t: (str(t.get("date")), str(t.get("entry_time"))))
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    equity: List[Dict[str, Any]] = []
    daily_pnl: Dict[str, float] = defaultdict(float)
    days: set = set()
    for t in ordered:
        p = _f(t.get(pnl_key))
        cum += p
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)
        d = str(t.get("date") or "")
        days.add(d)
        daily_pnl[d] += p
        equity.append({"date": t.get("date"), "symbol": t.get("symbol"), "cum_pnl": cum})

    trades_per_day = (n / len(days)) if days else 0.0

    by_side: Dict[str, Dict[str, Any]] = {}
    for side in ("BULLISH", "BEARISH"):
        sub = [t for t in trades if t.get("side") == side]
        sn = len(sub)
        by_side[side] = {
            "trades": sn,
            "wins": sum(1 for t in sub if _f(t.get(pnl_key)) > 0),
            "win_rate": (sum(1 for t in sub if _f(t.get(pnl_key)) > 0) / sn * 100) if sn else 0.0,
            "net_pnl": sum(_f(t.get(pnl_key)) for t in sub),
            "avg_r": (sum(_f(t.get("r_multiple")) for t in sub) / sn) if sn else 0.0,
        }

    by_exit = Counter(str(t.get("exit_reason") or "") for t in trades)

    # Per-month
    by_month: Dict[str, Dict[str, Any]] = {}
    month_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for t in trades:
        d = str(t.get("date") or "")
        month_groups[d[:7] if len(d) >= 7 else d].append(t)
    for month, rows in sorted(month_groups.items()):
        mn = len(rows)
        m_wins = sum(1 for t in rows if _f(t.get(pnl_key)) > 0)
        by_month[month] = {
            "trades": mn,
            "wins": m_wins,
            "win_rate": (m_wins / mn * 100.0) if mn else 0.0,
            "net_pnl": sum(_f(t.get(pnl_key)) for t in rows),
            "avg_r": (sum(_f(t.get("r_multiple")) for t in rows) / mn) if mn else 0.0,
        }

    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / n * 100.0) if n else 0.0,
        "net_pnl": net,
        "profit_factor": pf if pf != float("inf") else None,
        "profit_factor_inf": pf == float("inf"),
        "avg_r": avg_r,
        "max_drawdown": max_dd,
        "trades_per_day": trades_per_day,
        "trading_days_with_trades": len(days),
        "by_side": by_side,
        "by_exit": dict(by_exit),
        "by_month": by_month,
        "daily_pnl": dict(sorted(daily_pnl.items())),
        "equity": equity,
        "pnl_key": pnl_key,
    }


def summarize(
    trades: Sequence[Dict[str, Any]],
    skips: Sequence[Dict[str, Any]] | None = None,
    *,
    pnl_key: str = "rupee_pnl",
) -> Dict[str, Any]:
    base = _block(trades, pnl_key=pnl_key)
    trades_l = list(trades or [])

    by_weekday = Counter()
    stock_pnl: Dict[str, float] = defaultdict(float)
    for t in trades_l:
        stock_pnl[str(t.get("symbol") or "")] += _f(t.get(pnl_key))
        d = str(t.get("date") or "")
        try:
            from datetime import date as _date

            wd = _date.fromisoformat(d).strftime("%A")
        except Exception:
            wd = "?"
        by_weekday[wd] += 1

    flag_yes = [t for t in trades_l if t.get("open_eq_extreme") == "YES"]
    flag_no = [t for t in trades_l if t.get("open_eq_extreme") != "YES"]

    def _flag_block(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        rn = len(rows)
        return {
            "trades": rn,
            "wins": sum(1 for t in rows if _f(t.get(pnl_key)) > 0),
            "win_rate": (sum(1 for t in rows if _f(t.get(pnl_key)) > 0) / rn * 100) if rn else 0.0,
            "net_pnl": sum(_f(t.get(pnl_key)) for t in rows),
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

    base.update(
        {
            "points_pnl": sum(_f(t.get("points_pnl")) for t in trades_l),
            "by_weekday": dict(by_weekday),
            "top10": top10,
            "bottom10": bottom10,
            "flag_comparison": flag_cmp,
            "skips": list(skips or []),
            "skip_count": len(skips or []),
        }
    )
    return base


def summarize_study(
    trades: Sequence[Dict[str, Any]],
    skips: Sequence[Dict[str, Any]] | None = None,
    *,
    selection_study: Optional[Dict[str, Any]] = None,
    sl_modes: Sequence[str] = ("fixed", "candle_trail", "atr_trail"),
) -> Dict[str, Any]:
    """Baseline (all signals) × each SL mode, plus each selection rule × mode.

    Also reports fixed-risk sizing metrics alongside one-lot.
    """
    trades_l = list(trades or [])
    by_mode: Dict[str, Any] = {}
    for mode in sl_modes:
        mode_trades = [t for t in trades_l if str(t.get("sl_mode") or "") == mode]
        one_lot = summarize(mode_trades, skips if mode == sl_modes[0] else [], pnl_key="rupee_pnl")
        fixed_risk = _block(mode_trades, pnl_key="rupee_pnl_fixed_risk")

        rules: Dict[str, Any] = {}
        for rule, flag in (
            ("ema", "selected_ema"),
            ("range", "selected_range"),
            ("composite", "selected_composite"),
        ):
            sel = [t for t in mode_trades if t.get(flag)]
            rules[rule] = {
                "one_lot": _block(sel, pnl_key="rupee_pnl"),
                "fixed_risk": _block(sel, pnl_key="rupee_pnl_fixed_risk"),
                "by_side": {
                    side: _block(
                        [t for t in sel if t.get("side") == side], pnl_key="rupee_pnl"
                    )
                    for side in ("BULLISH", "BEARISH")
                },
            }

        # All filtered (passed move filter) as reference
        filtered = [t for t in mode_trades if t.get("passed_move_filter")]
        by_mode[mode] = {
            "baseline_one_lot": one_lot,
            "baseline_fixed_risk": fixed_risk,
            "filtered_one_lot": _block(filtered, pnl_key="rupee_pnl"),
            "rules": rules,
        }

    # Slim primary summary = atr_trail baseline one-lot (or first available)
    primary_mode = "atr_trail" if "atr_trail" in by_mode else (list(sl_modes)[0] if sl_modes else "fixed")
    primary = by_mode.get(primary_mode, {}).get("baseline_one_lot") or summarize([], skips)

    out = dict(primary)
    out["by_sl_mode"] = by_mode
    out["selection"] = selection_study or {}
    out["skips"] = list(skips or [])
    out["skip_count"] = len(skips or [])
    return out

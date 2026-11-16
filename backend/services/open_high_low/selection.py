"""Post-signal trade selection study: move filter, top-2 rules, random baseline."""
from __future__ import annotations

import random
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from backend.services.open_high_low.config import (
    MOVE_FILTER_PCT,
    RANDOM_BASELINE_SAMPLES,
    SELECTION_TOP_N_PER_SIDE,
)


def passes_move_filter(
    side: str,
    move_pct: Optional[float],
    *,
    limit_pct: float = MOVE_FILTER_PCT,
) -> bool:
    """Signed move toward the trade must not already exceed ``limit_pct``.

    BULLISH: ``move_pct <= limit`` (not already run up >3% into the long).
    BEARISH: ``move_pct >= -limit`` (not already run down >3% into the short).
    Missing move → fail the filter.
    """
    if move_pct is None:
        return False
    try:
        m = float(move_pct)
    except (TypeError, ValueError):
        return False
    lim = float(limit_pct)
    if side == "BULLISH":
        return m <= lim
    if side == "BEARISH":
        return m >= -lim
    return False


def _f(row: Dict[str, Any], key: str) -> Optional[float]:
    v = row.get(key)
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def rank_key_ema(row: Dict[str, Any]) -> Tuple[float, str]:
    """Nearest EMA30: smallest |entry_dist_ema30_atr|; missing → worst."""
    d = _f(row, "entry_dist_ema30_atr")
    if d is None:
        return (float("inf"), str(row.get("symbol") or ""))
    return (abs(d), str(row.get("symbol") or ""))


def rank_key_range(row: Dict[str, Any]) -> Tuple[float, str]:
    """Smallest first_range_atr; missing → worst."""
    r = _f(row, "first_range_atr")
    if r is None:
        return (float("inf"), str(row.get("symbol") or ""))
    return (r, str(row.get("symbol") or ""))


def _dense_ranks(rows: Sequence[Dict[str, Any]], key_fn) -> Dict[str, float]:
    """1-based ranks (ties share average rank). Keyed by symbol within the list."""
    ordered = sorted(enumerate(rows), key=lambda iv: key_fn(iv[1]))
    n = len(ordered)
    ranks: Dict[int, float] = {}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and key_fn(ordered[j + 1][1])[0] == key_fn(ordered[i][1])[0]:
            j += 1
        avg = (i + 1 + j + 1) / 2.0  # 1-based
        for k in range(i, j + 1):
            ranks[ordered[k][0]] = avg
        i = j + 1
    return {str(rows[idx].get("symbol") or ""): ranks[idx] for idx in ranks}


def select_top_per_side(
    candidates: Sequence[Dict[str, Any]],
    *,
    rule: str,
    top_n: int = SELECTION_TOP_N_PER_SIDE,
) -> List[Dict[str, Any]]:
    """Take top ``top_n`` BULLISH + top ``top_n`` BEARISH under ``rule``.

    Rules: ``ema``, ``range``, ``composite`` (average of ema+range ranks).
    """
    by_side: Dict[str, List[Dict[str, Any]]] = {"BULLISH": [], "BEARISH": []}
    for row in candidates:
        side = str(row.get("side") or "")
        if side in by_side:
            by_side[side].append(row)

    picked: List[Dict[str, Any]] = []
    for side, rows in by_side.items():
        if not rows:
            continue
        if rule == "ema":
            ranked = sorted(rows, key=rank_key_ema)
        elif rule == "range":
            ranked = sorted(rows, key=rank_key_range)
        elif rule == "composite":
            r_ema = _dense_ranks(rows, rank_key_ema)
            r_rng = _dense_ranks(rows, rank_key_range)

            def comp_key(r: Dict[str, Any]) -> Tuple[float, str]:
                sym = str(r.get("symbol") or "")
                avg = (r_ema.get(sym, float("inf")) + r_rng.get(sym, float("inf"))) / 2.0
                return (avg, sym)

            ranked = sorted(rows, key=comp_key)
        else:
            raise ValueError(f"unknown selection rule: {rule!r}")
        picked.extend(ranked[: max(0, int(top_n))])
    return picked


def filter_candidates(
    signals: Sequence[Dict[str, Any]],
    *,
    limit_pct: float = MOVE_FILTER_PCT,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in signals:
        ok = passes_move_filter(
            str(row.get("side") or ""),
            _f(row, "move_prev_close_to_entry_pct"),
            limit_pct=limit_pct,
        )
        if ok:
            out.append(row)
    return out


def apply_selection_flags(
    trades: Sequence[Dict[str, Any]],
    *,
    limit_pct: float = MOVE_FILTER_PCT,
    top_n: int = SELECTION_TOP_N_PER_SIDE,
) -> Dict[str, Any]:
    """Annotate trades with passed_move_filter + selected_* flags (in place).

    Selection is by (date, symbol) signal — same flags for all sl_mode rows.
    Returns selection summary structure (without random baseline).
    """
    # Deduplicate to one signal row per date|symbol (prefer first / any mode).
    signals_by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    seen: Set[Tuple[str, str]] = set()
    signal_proto: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for t in trades:
        key = (str(t.get("date") or ""), str(t.get("symbol") or ""))
        if key in seen:
            continue
        seen.add(key)
        signal_proto[key] = t
        signals_by_day[key[0]].append(t)

    selected_keys: Dict[str, Set[Tuple[str, str]]] = {
        "ema": set(),
        "range": set(),
        "composite": set(),
    }
    filter_pass_keys: Set[Tuple[str, str]] = set()

    for day, day_signals in signals_by_day.items():
        for sig in day_signals:
            key = (str(sig.get("date") or ""), str(sig.get("symbol") or ""))
            ok = passes_move_filter(
                str(sig.get("side") or ""),
                _f(sig, "move_prev_close_to_entry_pct"),
                limit_pct=limit_pct,
            )
            if ok:
                filter_pass_keys.add(key)
        filtered = [
            s
            for s in day_signals
            if (str(s.get("date") or ""), str(s.get("symbol") or "")) in filter_pass_keys
        ]
        for rule in ("ema", "range", "composite"):
            picks = select_top_per_side(filtered, rule=rule, top_n=top_n)
            for p in picks:
                selected_keys[rule].add(
                    (str(p.get("date") or ""), str(p.get("symbol") or ""))
                )

    for t in trades:
        key = (str(t.get("date") or ""), str(t.get("symbol") or ""))
        t["passed_move_filter"] = key in filter_pass_keys
        t["selected_ema"] = key in selected_keys["ema"]
        t["selected_range"] = key in selected_keys["range"]
        t["selected_composite"] = key in selected_keys["composite"]

    return {
        "filter": (
            "BULLISH move_prev_close_to_entry_pct <= "
            f"{limit_pct}%; BEARISH move >= -{limit_pct}% "
            "(not already extended >limit in trade direction)"
        ),
        "top_n_per_side": top_n,
        "selected_counts": {r: len(selected_keys[r]) for r in selected_keys},
        "filter_pass_signals": len(filter_pass_keys),
    }


def _signal_pnl_for_mode(
    trades: Sequence[Dict[str, Any]],
    sl_mode: str,
    pnl_key: str = "rupee_pnl",
) -> Dict[Tuple[str, str], float]:
    out: Dict[Tuple[str, str], float] = {}
    for t in trades:
        if str(t.get("sl_mode") or "") != sl_mode:
            continue
        key = (str(t.get("date") or ""), str(t.get("symbol") or ""))
        try:
            out[key] = float(t.get(pnl_key) or 0.0)
        except (TypeError, ValueError):
            out[key] = 0.0
    return out


def random_baseline_percentile(
    day_candidates: Sequence[Dict[str, Any]],
    selected_keys: Set[Tuple[str, str]],
    pnl_map: Dict[Tuple[str, str], float],
    *,
    top_n: int = SELECTION_TOP_N_PER_SIDE,
    n_samples: int = RANDOM_BASELINE_SAMPLES,
    rng: Optional[random.Random] = None,
) -> Optional[float]:
    """Percentile of rule net P&L among ``n_samples`` random 2+2 picks same day pool.

    Returns percentile in [0, 100] (higher = rule beat more random samples),
    or None if not enough candidates.
    """
    rng = rng or random.Random(0)
    bulls = [
        (str(c.get("date") or ""), str(c.get("symbol") or ""))
        for c in day_candidates
        if c.get("side") == "BULLISH"
    ]
    bears = [
        (str(c.get("date") or ""), str(c.get("symbol") or ""))
        for c in day_candidates
        if c.get("side") == "BEARISH"
    ]
    if len(bulls) < top_n and len(bears) < top_n:
        # Still allow partial days when one side is thin
        pass
    need_b = min(top_n, len(bulls))
    need_s = min(top_n, len(bears))
    if need_b + need_s == 0:
        return None

    def net(keys: Iterable[Tuple[str, str]]) -> float:
        return sum(pnl_map.get(k, 0.0) for k in keys)

    rule_pnl = net(selected_keys)
    better_or_eq = 0
    for _ in range(int(n_samples)):
        pick: List[Tuple[str, str]] = []
        if need_b:
            pick.extend(rng.sample(bulls, need_b))
        if need_s:
            pick.extend(rng.sample(bears, need_s))
        if net(pick) <= rule_pnl:
            better_or_eq += 1
    return 100.0 * better_or_eq / float(n_samples)


def run_selection_study(
    trades: Sequence[Dict[str, Any]],
    *,
    sl_modes: Sequence[str],
    limit_pct: float = MOVE_FILTER_PCT,
    top_n: int = SELECTION_TOP_N_PER_SIDE,
    n_samples: int = RANDOM_BASELINE_SAMPLES,
    pnl_key: str = "rupee_pnl",
    seed: int = 42,
) -> Dict[str, Any]:
    """Full selection study: flags + metrics stubs filled by caller metrics helper.

    Returns structure with per-day picks and random percentiles by mode×rule.
    """
    flags_meta = apply_selection_flags(trades, limit_pct=limit_pct, top_n=top_n)

    # Build filtered candidates per day from unique signals.
    signals: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for t in trades:
        key = (str(t.get("date") or ""), str(t.get("symbol") or ""))
        signals.setdefault(key, t)

    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for key, sig in signals.items():
        by_day[key[0]].append(sig)

    picks_by_day: Dict[str, Dict[str, List[Tuple[str, str]]]] = {}
    for day, day_sigs in by_day.items():
        filtered = filter_candidates(day_sigs, limit_pct=limit_pct)
        picks_by_day[day] = {}
        for rule in ("ema", "range", "composite"):
            picks = select_top_per_side(filtered, rule=rule, top_n=top_n)
            picks_by_day[day][rule] = [
                (str(p.get("date") or ""), str(p.get("symbol") or "")) for p in picks
            ]

    rng = random.Random(seed)
    percentiles: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for mode in sl_modes:
        pnl_map = _signal_pnl_for_mode(trades, mode, pnl_key=pnl_key)
        percentiles[mode] = {}
        for rule in ("ema", "range", "composite"):
            day_pcts: List[float] = []
            for day, day_sigs in by_day.items():
                filtered = filter_candidates(day_sigs, limit_pct=limit_pct)
                sel = set(picks_by_day[day][rule])
                # Restrict selected to this day's filtered set
                day_keys = {
                    (str(s.get("date") or ""), str(s.get("symbol") or ""))
                    for s in filtered
                }
                sel_day = {k for k in sel if k in day_keys}
                pct = random_baseline_percentile(
                    filtered,
                    sel_day,
                    pnl_map,
                    top_n=top_n,
                    n_samples=n_samples,
                    rng=rng,
                )
                if pct is not None:
                    day_pcts.append(pct)
            percentiles[mode][rule] = {
                "mean_percentile": (sum(day_pcts) / len(day_pcts)) if day_pcts else None,
                "days_scored": len(day_pcts),
                "n_samples": n_samples,
            }

    return {
        "meta": flags_meta,
        "picks_by_day": {
            d: {r: [f"{a}|{b}" for a, b in keys] for r, keys in rules.items()}
            for d, rules in picks_by_day.items()
        },
        "random_percentiles": percentiles,
    }

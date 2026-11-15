"""Rank features and threshold rules from the stock-day CSV.

Quintile edges and rules are fit on the training window only. The same rules
are then scored on the later window.
"""
from __future__ import annotations

import math
from datetime import datetime
from itertools import combinations
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from backend.services.big_move.features import FEATURE_META, THRESHOLDS

DIRECTIONS = ("up", "down", "either")
MIN_RULE_N = 30
MIN_TEST_N = 20
HOLD_LIFT = 1.15
HOLD_RATIO = 0.6
POOL = 12
Z = 1.96


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or math.isinf(out):
        return None
    return out


def _flag(row: Dict[str, Any], key: str) -> int:
    return 1 if _num(row.get(key)) == 1 else 0


def wilson(positives: int, n: int) -> Tuple[float, float, float]:
    if n <= 0:
        return 0.0, 0.0, 0.0
    p = positives / float(n)
    z2 = Z * Z
    denom = 1.0 + z2 / n
    centre = (p + z2 / (2.0 * n)) / denom
    margin = Z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n)) / denom
    return p, max(0.0, centre - margin), min(1.0, centre + margin)


def roc_auc(scores: Sequence[float], labels: Sequence[int]) -> Optional[float]:
    pairs = sorted(zip(scores, labels), key=lambda item: item[0])
    n_pos = sum(labels)
    n_neg = len(labels) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    rank_sum = 0.0
    i = 0
    while i < len(pairs):
        j = i
        while j < len(pairs) and pairs[j][0] == pairs[i][0]:
            j += 1
        avg_rank = (i + 1 + j) / 2.0
        rank_sum += avg_rank * sum(pairs[k][1] for k in range(i, j))
        i = j
    return (rank_sum - n_pos * (n_pos + 1) / 2.0) / float(n_pos * n_neg)


def _label_key(direction: str, threshold: int) -> str:
    return f"big_{direction}_{threshold}"


def _clean_key(direction: str, threshold: int) -> Optional[str]:
    if direction == "up":
        return f"clean_up_{threshold}"
    if direction == "down":
        return f"clean_down_{threshold}"
    return None


def _split(rows: Sequence[Dict[str, Any]], train_end: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    train = [r for r in rows if str(r.get("date") or "") <= train_end]
    test = [r for r in rows if str(r.get("date") or "") > train_end]
    return train, test


def _rate_block(rows: Sequence[Dict[str, Any]], key: str) -> Dict[str, Any]:
    n = len(rows)
    positives = sum(_flag(r, key) for r in rows)
    rate, lo, hi = wilson(positives, n)
    return {
        "positives": positives,
        "n": n,
        "rate": rate,
        "ci_lo": lo,
        "ci_hi": hi,
        "low_sample": positives < MIN_RULE_N,
    }


def _base_rates(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for threshold in THRESHOLDS:
        tag = str(threshold)
        block = {
            "big_up": _rate_block(rows, f"big_up_{tag}"),
            "big_down": _rate_block(rows, f"big_down_{tag}"),
            "big_either": _rate_block(rows, f"big_either_{tag}"),
            "clean_up": _rate_block(rows, f"clean_up_{tag}"),
            "clean_down": _rate_block(rows, f"clean_down_{tag}"),
        }
        for name, key in (("up", f"big_up_{tag}"), ("down", f"big_down_{tag}"), ("either", f"big_either_{tag}")):
            events = [r for r in rows if _flag(r, key)]
            hit = sum(1 for r in events if _flag(r, "extreme_gap") or _flag(r, "extreme_rvol"))
            block[f"catalyst_share_{name}"] = (hit / len(events)) if events else None
        out[tag] = block
    return out


def _edges(values: Sequence[float]) -> List[float]:
    ordered = sorted(values)
    n = len(ordered)
    if n < 5:
        return []
    return [ordered[min(n - 1, int(n * q / 5.0))] for q in (1, 2, 3, 4)]


def _bucket_of(value: float, edges: Sequence[float]) -> int:
    for i, edge in enumerate(edges):
        if value < edge:
            return i
    return len(edges)


def _fmt_edge(value: float) -> str:
    av = abs(value)
    if av >= 100:
        return f"{value:.0f}"
    if av >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def _bucket_rows(
    indexed: Sequence[Tuple[Dict[str, Any], Optional[float], int]],
    label: str,
    base: float,
) -> List[Dict[str, Any]]:
    grouped: Dict[Any, List[int]] = {}
    for _row, bucket, flag in indexed:
        grouped.setdefault(bucket, []).append(flag)
    out = []
    for bucket in sorted(grouped, key=lambda item: str(item)):
        flags = grouped[bucket]
        n = len(flags)
        positives = sum(flags)
        rate, lo, hi = wilson(positives, n)
        lift = (rate / base) if base > 0 else None
        out.append(
            {
                "bucket": str(bucket),
                "n": n,
                "positives": positives,
                "rate": rate,
                "lift": lift,
                "ci_lo": lo,
                "ci_hi": hi,
            }
        )
    return out


def _feature_report(
    train: Sequence[Dict[str, Any]],
    test: Sequence[Dict[str, Any]],
    direction: str,
    threshold: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    key = _label_key(direction, threshold)
    base_n = len(train)
    base_pos = sum(_flag(r, key) for r in train)
    base = (base_pos / base_n) if base_n else 0.0
    test_base_n = len(test)
    test_base = (sum(_flag(r, key) for r in test) / test_base_n) if test_base_n else 0.0
    ranking = []
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for name, title, group, kind in FEATURE_META:
        train_vals = []
        for row in train:
            if kind == "cat":
                text = str(row.get(name) or "").strip()
                train_vals.append((row, text or None))
            else:
                train_vals.append((row, _num(row.get(name))))
        present = [(r, v) for r, v in train_vals if v is not None]
        if len(present) < MIN_RULE_N:
            continue
        labels = [_flag(r, key) for r, _v in present]
        auc = None
        side = ""
        if kind != "cat":
            raw = roc_auc([v for _r, v in present], labels)
            if raw is not None:
                side = "high" if raw >= 0.5 else "low"
                auc = raw if raw >= 0.5 else 1.0 - raw
        indexed: List[Tuple[Dict[str, Any], Any, int]] = []
        predicate = None
        if kind == "num":
            edges = _edges([v for _r, v in present])
            if len(edges) < 4:
                continue
            names = ["Q1", "Q2", "Q3", "Q4", "Q5"]
            for row, value in present:
                b = _bucket_of(float(value), edges)
                indexed.append((row, names[b], _flag(row, key)))
            q1 = [item for item in indexed if item[1] == "Q1"]
            q5 = [item for item in indexed if item[1] == "Q5"]
            def _lift(items: List[Tuple[Dict[str, Any], Any, int]]) -> Tuple[float, int]:
                if not items or base <= 0:
                    return 0.0, len(items)
                rate = sum(flag for _r, _b, flag in items) / float(len(items))
                return rate / base, len(items)
            l1, n1 = _lift(q1)
            l5, n5 = _lift(q5)
            if n1 >= MIN_RULE_N and (n5 < MIN_RULE_N or l1 >= l5):
                predicate = {"feature": name, "op": "<=", "value": edges[0], "text": f"{title} ≤ {_fmt_edge(edges[0])}"}
                best_name, best_items = "Q1", q1
            elif n5 >= MIN_RULE_N:
                predicate = {"feature": name, "op": ">=", "value": edges[3], "text": f"{title} ≥ {_fmt_edge(edges[3])}"}
                best_name, best_items = "Q5", q5
            else:
                best_name, best_items = ("Q5", q5) if l5 >= l1 else ("Q1", q1)
        elif kind == "bin":
            for row, value in present:
                indexed.append((row, "Yes" if value >= 1 else "No", _flag(row, key)))
            yes = [item for item in indexed if item[1] == "Yes"]
            no = [item for item in indexed if item[1] == "No"]
            def _lift(items: List[Tuple[Dict[str, Any], Any, int]]) -> float:
                if not items or base <= 0:
                    return 0.0
                return (sum(flag for _r, _b, flag in items) / float(len(items))) / base
            if len(yes) >= MIN_RULE_N and _lift(yes) >= _lift(no):
                predicate = {"feature": name, "op": "==", "value": 1, "text": title}
                best_name, best_items = "Yes", yes
            elif len(no) >= MIN_RULE_N:
                predicate = {"feature": name, "op": "==", "value": 0, "text": f"Not {title}"}
                best_name, best_items = "No", no
            else:
                best_name, best_items = "Yes", yes
        else:
            levels = sorted({str(value) for _row, value in present if str(value)})
            best_level = None
            best_lift = -1.0
            best_items = []
            for level in levels:
                items = [(row, level, _flag(row, key)) for row, value in present if str(value) == level]
                if len(items) < MIN_RULE_N or base <= 0:
                    continue
                lift = (sum(flag for _r, _b, flag in items) / float(len(items))) / base
                if lift > best_lift:
                    best_lift = lift
                    best_level = level
                    best_items = items
            indexed = [(row, str(value), _flag(row, key)) for row, value in present if str(value)]
            if best_level:
                predicate = {"feature": name, "op": "==", "value": best_level, "text": f"{title} = {best_level.replace('_', ' ')}"}
                best_name = best_level
            else:
                best_name, best_items = "", []
        chart = _bucket_rows(indexed, key, base)
        buckets[name] = chart
        if not best_items:
            continue
        n = len(best_items)
        positives = sum(flag for _r, _b, flag in best_items)
        rate, lo, hi = wilson(positives, n)
        lift = (rate / base) if base > 0 else None
        test_stats = _score_predicate(test, predicate, key, test_base) if predicate else None
        ranking.append(
            {
                "feature": name,
                "label": title,
                "group": group,
                "kind": kind,
                "auc": auc,
                "side": side,
                "top_bucket": best_name,
                "top_n": n,
                "top_rate": rate,
                "top_lift": lift,
                "ci_lo": lo,
                "ci_hi": hi,
                "test_n": None if not test_stats else test_stats["n"],
                "test_rate": None if not test_stats else test_stats["rate"],
                "test_lift": None if not test_stats else test_stats["lift"],
                "predicate": predicate,
            }
        )
    ranking.sort(key=lambda row: (-(row["top_lift"] or 0), -(row["auc"] or 0), row["label"]))
    return ranking, buckets


def _matches(row: Dict[str, Any], clause: Dict[str, Any]) -> bool:
    raw = row.get(clause["feature"])
    if clause["op"] == "==" and not isinstance(clause["value"], (int, float)):
        return str(raw or "") == str(clause["value"])
    value = _num(raw)
    if value is None:
        return False
    target = float(clause["value"])
    if clause["op"] == ">=":
        return value >= target
    if clause["op"] == "<=":
        return value <= target
    return value == target


def _score_predicate(
    rows: Sequence[Dict[str, Any]],
    clause: Optional[Dict[str, Any]],
    key: str,
    base: float,
) -> Optional[Dict[str, Any]]:
    if not clause:
        return None
    matched = [r for r in rows if _matches(r, clause)]
    return _score_rows(matched, key, base, None)


def _score_rows(
    matched: Sequence[Dict[str, Any]],
    key: str,
    base: float,
    clean_key: Optional[str],
) -> Dict[str, Any]:
    n = len(matched)
    positives = sum(_flag(r, key) for r in matched)
    rate, lo, hi = wilson(positives, n)
    lift = (rate / base) if base > 0 and n else None
    clean = None
    if clean_key and n:
        clean = sum(_flag(r, clean_key) for r in matched) / float(n)
    return {"n": n, "positives": positives, "rate": rate, "lift": lift, "ci_lo": lo, "ci_hi": hi, "clean": clean}


def _mask(rows: Sequence[Dict[str, Any]], clauses: Sequence[Dict[str, Any]]) -> List[int]:
    hits = []
    for i, row in enumerate(rows):
        if all(_matches(row, clause) for clause in clauses):
            hits.append(i)
    return hits


def _rules_for(
    train: Sequence[Dict[str, Any]],
    test: Sequence[Dict[str, Any]],
    ranking: Sequence[Dict[str, Any]],
    direction: str,
    threshold: int,
) -> Dict[str, List[Dict[str, Any]]]:
    key = _label_key(direction, threshold)
    clean = _clean_key(direction, threshold)
    base = (sum(_flag(r, key) for r in train) / len(train)) if train else 0.0
    test_base = (sum(_flag(r, key) for r in test) / len(test)) if test else 0.0
    pool = []
    seen = set()
    for row in ranking:
        pred = row.get("predicate")
        if not pred or pred["feature"] in seen:
            continue
        if (row.get("top_lift") or 0) <= 1.0 or (row.get("top_n") or 0) < MIN_RULE_N:
            continue
        seen.add(pred["feature"])
        pool.append(pred)
        if len(pool) >= POOL:
            break

    def pack(clauses: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        idx = _mask(train, clauses)
        if len(idx) < MIN_RULE_N:
            return None
        matched = [train[i] for i in idx]
        train_stats = _score_rows(matched, key, base, clean)
        if not train_stats["lift"] or train_stats["lift"] <= 1.0:
            return None
        test_matched = [r for r in test if all(_matches(r, c) for c in clauses)]
        test_stats = _score_rows(test_matched, key, test_base, clean)
        holds = bool(
            test_stats["n"] >= MIN_TEST_N
            and test_stats["lift"] is not None
            and test_stats["lift"] >= HOLD_LIFT
            and test_stats["lift"] >= HOLD_RATIO * train_stats["lift"]
        )
        return {
            "text": " AND ".join(c["text"] for c in clauses),
            "clauses": [dict(c) for c in clauses],
            "train_n": train_stats["n"],
            "train_rate": train_stats["rate"],
            "train_lift": train_stats["lift"],
            "train_clean": train_stats["clean"],
            "test_n": test_stats["n"],
            "test_rate": test_stats["rate"],
            "test_lift": test_stats["lift"],
            "test_clean": test_stats["clean"],
            "holds": holds,
        }

    pairs = []
    for a, b in combinations(pool, 2):
        packed = pack((a, b))
        if packed:
            pairs.append(packed)
    pairs.sort(key=lambda row: (-(row["train_lift"] or 0), -row["train_n"]))
    triples = []
    for combo in combinations(pool[:10], 3):
        packed = pack(combo)
        if packed:
            triples.append(packed)
    triples.sort(key=lambda row: (-(row["train_lift"] or 0), -row["train_n"]))
    return {"pairs": pairs[:12], "triples": triples[:12]}


def _screen(
    rows: Sequence[Dict[str, Any]],
    rules: Sequence[Dict[str, Any]],
    train_end: str,
) -> List[Dict[str, Any]]:
    chosen = [r for r in rules if r.get("holds")][:5]
    confirmed = True
    if not chosen:
        chosen = list(rules[:3])
        confirmed = False
    if not chosen:
        return []
    out = []
    for row in rows:
        passed = [rule["text"] for rule in chosen if all(_matches(row, c) for c in rule["clauses"])]
        if not passed:
            continue
        out.append(
            {
                "date": row.get("date"),
                "symbol": row.get("symbol"),
                "contract": row.get("contract"),
                "split": "train" if str(row.get("date") or "") <= train_end else "test",
                "rules": passed,
                "confirmed": confirmed,
                "max_up_atr": _num(row.get("max_up_atr")),
                "max_down_atr": _num(row.get("max_down_atr")),
                "close_move_atr": _num(row.get("close_move_atr")),
            }
        )
    out.sort(key=lambda item: (str(item["date"]), str(item["symbol"])))
    return out


def build_analysis(
    rows: Sequence[Dict[str, Any]],
    *,
    train_end: str,
    params: Dict[str, Any],
    skipped: Optional[Iterable[Dict[str, Any]]] = None,
    contract_rule: str = "",
) -> Dict[str, Any]:
    train, test = _split(rows, train_end)
    skipped_list = list(skipped or [])
    dates = sorted({str(r.get("date")) for r in rows if r.get("date")})
    symbols = sorted({str(r.get("symbol")) for r in rows if r.get("symbol")})
    rankings: Dict[str, Any] = {}
    buckets: Dict[str, Any] = {}
    rules: Dict[str, Any] = {}
    screens: Dict[str, Any] = {}
    for direction in DIRECTIONS:
        rankings[direction] = {}
        buckets[direction] = {}
        rules[direction] = {}
        screens[direction] = {}
        for threshold in THRESHOLDS:
            tag = str(threshold)
            rank, chart = _feature_report(train, test, direction, threshold)
            rankings[direction][tag] = [{k: v for k, v in row.items() if k != "predicate"} | {"rule": (row.get("predicate") or {}).get("text")} for row in rank]
            buckets[direction][tag] = chart
            packed = _rules_for(train, test, rank, direction, threshold)
            rules[direction][tag] = packed
            if direction != "either":
                screens[direction][tag] = _screen(rows, packed["pairs"] + packed["triples"], train_end)
    return {
        "ok": True,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "params": params,
        "contract_rule": contract_rule,
        "header": {
            "stock_days": len(rows),
            "skipped": len(skipped_list),
            "trading_days": len(dates),
            "symbols": len(symbols),
            "date_from": dates[0] if dates else params.get("date_from"),
            "date_to": dates[-1] if dates else params.get("date_to"),
            "train_days": len({r.get("date") for r in train}),
            "test_days": len({r.get("date") for r in test}),
            "train_rows": len(train),
            "test_rows": len(test),
            "train_end": train_end,
        },
        "base_rates": {"all": _base_rates(rows), "train": _base_rates(train), "test": _base_rates(test)},
        "rankings": rankings,
        "buckets": buckets,
        "rules": rules,
        "screens": screens,
        "skip_reasons": _reasons(skipped_list),
    }


def _reasons(skipped: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for row in skipped:
        reason = str(row.get("reason") or "skipped")
        counts[reason] = counts.get(reason, 0) + 1
    return [{"reason": reason, "count": count} for reason, count in sorted(counts.items(), key=lambda item: -item[1])]

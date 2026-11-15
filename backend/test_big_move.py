"""Big Move Study: labels, no look-ahead, and train-only rules. No network."""
from __future__ import annotations

from datetime import date

from backend.services.big_move.analyze import _rate_block, build_analysis, roc_auc, wilson
from backend.services.big_move.features import build_stock_day
from backend.services.ema_scan.calendar import candle_start, trading_days


def _days():
    return trading_days(date(2025, 12, 1), date(2026, 2, 27))


def _daily(prior, last_span=0.05, span=0.5):
    rows = []
    for i, day in enumerate(prior):
        width = last_span if i == len(prior) - 1 else span
        close = 100.0
        rows.append(
            {
                "timestamp": f"{day.isoformat()}T00:00:00+05:30",
                "open": close,
                "high": close + width,
                "low": close - width,
                "close": close,
                "oi": 10000 + i * 10,
            }
        )
    return rows


def _session(day, price=100.0, volume=1000.0, width=0.1, drop=(), mutate=None):
    bars = []
    for n in range(1, 76):
        if n in drop:
            continue
        bar = {
            "timestamp": candle_start(day, n).isoformat(),
            "open": price,
            "high": price + width,
            "low": price - width,
            "close": price,
            "volume": volume,
            "oi": 12000,
        }
        if mutate:
            mutate(n, bar)
        bars.append(bar)
    return bars


def _build(day, prior, bars, daily=None):
    return build_stock_day(
        symbol="RELIANCE",
        contract="RELIANCE 24FEB26",
        instrument_key="NSE_FO|1",
        day=day,
        bars_5m=bars,
        bars_daily=daily if daily is not None else _daily(prior),
    )


def test_skip_when_an_opening_candle_is_missing():
    days = _days()
    target, prior = days[30], days[:30]
    row, reason = _build(target, prior, _session(target, drop=(4,)))
    assert row is None
    assert "first 6" in reason


def test_skip_when_the_afternoon_is_incomplete():
    days = _days()
    target, prior = days[30], days[:30]
    row, reason = _build(target, prior, _session(target, drop=(20,)))
    assert row is None
    assert "after 09:45" in reason


def test_labels_nr7_gap_held_and_no_lookahead():
    days = _days()
    target, prior = days[30], days[:30]
    nxt = days[31]

    def gap(n, bar):
        bar["open"] = 106.0
        bar["close"] = 106.0
        bar["low"] = 105.8
        bar["high"] = 106.4

    bars = _session(target, mutate=gap)
    row, reason = _build(target, prior, bars)
    assert reason is None and row is not None
    assert row["nr7"] == 1
    assert row["gap_location"] == "above_high"
    assert row["gap_held"] == 1
    assert row["extreme_gap"] == 1
    assert row["oi_basis"] == "intraday_0945"
    assert row["oi_state"] == "long_buildup"
    features = {k: row[k] for k in ("nr7", "gap_atr", "prev_range_atr", "atr", "or_break", "close_pos_30", "adx_5m")}

    def later(n, bar):
        gap(n, bar)
        if n == 10:
            bar["high"] = 130.0
            bar["low"] = 70.0
            bar["open"] = 100.0
            bar["close"] = 110.0

    changed, _ = _build(target, prior, _session(target, mutate=later))
    for key, value in features.items():
        assert changed[key] == value
    assert changed["max_up_atr"] > row["max_up_atr"]
    assert changed["big_up_2"] == 1
    assert changed["big_down_2"] == 1
    assert changed["clean_up_2"] == 0

    def clean(n, bar):
        gap(n, bar)
        if n >= 7:
            bar["high"] = 130.0
            bar["low"] = 105.8
            bar["close"] = 120.0

    clean_row, _ = _build(target, prior, _session(target, mutate=clean))
    assert clean_row["big_up_4"] == 1
    assert clean_row["clean_up_4"] == 1
    assert clean_row["clean_down_2"] == 0

    daily = _daily(prior) + [
        {
            "timestamp": f"{nxt.isoformat()}T00:00:00+05:30",
            "open": 100,
            "high": 180,
            "low": 20,
            "close": 100,
            "oi": 1,
        }
    ]
    future, _ = _build(target, prior, bars, daily=daily)
    assert future["prev_range_atr"] == row["prev_range_atr"]
    assert future["atr"] == row["atr"]
    assert future["nr7"] == row["nr7"]


def test_gap_not_held_when_filled_before_0945():
    days = _days()
    target, prior = days[30], days[:30]

    def filled(n, bar):
        if n == 1:
            bar["open"] = 106.0
            bar["high"] = 106.5
        if n == 5:
            bar["low"] = 99.0

    row, reason = _build(target, prior, _session(target, mutate=filled))
    assert reason is None
    assert row["gap_held"] == 0


def test_wilson_auc_and_low_sample():
    rate, lo, hi = wilson(50, 100)
    assert rate == 0.5
    assert 0.3 < lo < 0.5 < hi < 0.7
    scores = list(range(100))
    labels = [1 if s >= 50 else 0 for s in scores]
    assert roc_auc(scores, labels) > 0.9
    small = _rate_block([{"big_up_4": 1 if i < 10 else 0} for i in range(40)], "big_up_4")
    assert small["positives"] == 10
    assert small["low_sample"] is True


def _study_row(day, rvol, gap, up):
    row = {
        "date": day,
        "symbol": "ABC",
        "contract": "ABC",
        "rvol_30": rvol,
        "gap_atr": gap,
        "prev_range_atr": 1.0,
        "extreme_gap": 1 if abs(gap) > 2 else 0,
        "extreme_rvol": 1 if rvol > 3 else 0,
    }
    for threshold in (2, 3, 4):
        flag = 1 if up and threshold == 2 else 0
        row[f"big_up_{threshold}"] = flag
        row[f"big_down_{threshold}"] = 0
        row[f"big_either_{threshold}"] = flag
        row[f"clean_up_{threshold}"] = flag
        row[f"clean_down_{threshold}"] = 0
    return row


def test_rules_are_fit_on_train_and_scored_on_test():
    train = []
    for i in range(200):
        high = i >= 160
        train.append(_study_row(f"2025-11-{(i % 27) + 1:02d}", float(i), float(i) / 20.0, high))
    # Keep every train date inside the training window. Day-of-month collisions are fine.
    test = []
    for i in range(40):
        test.append(_study_row(f"2026-07-{(i % 27) + 1:02d}", 250.0, 12.0, True))
    for i in range(120):
        test.append(_study_row(f"2026-08-{(i % 27) + 1:02d}", 1.0, 0.0, False))
    doc = build_analysis(train + test, train_end="2026-06-30", params={"train_end": "2026-06-30"})
    ranking = doc["rankings"]["up"]["2"]
    top = next(row for row in ranking if row["feature"] == "rvol_30")
    assert top["top_lift"] > 3
    assert top["auc"] > 0.8
    held = [rule for rule in doc["rules"]["up"]["2"]["pairs"] if rule["holds"]]
    assert held
    assert all(rule["test_lift"] >= 1.15 for rule in held)
    assert doc["header"]["train_rows"] == 200
    assert doc["header"]["test_rows"] == 160
    assert doc["base_rates"]["all"]["2"]["big_up"]["positives"] == 80


def test_a_rule_that_fails_later_is_not_marked_as_holding():
    train = [_study_row("2026-03-01", float(i), float(i) / 10.0, i >= 160) for i in range(200)]
    test = [_study_row("2026-09-01", 250.0, 20.0, False) for _ in range(40)]
    doc = build_analysis(train + test, train_end="2026-06-30", params={})
    pairs = doc["rules"]["up"]["2"]["pairs"]
    assert pairs
    assert all(rule["holds"] is False for rule in pairs)

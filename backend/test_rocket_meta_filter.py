"""Offline tests for Rocket ML meta-filter (no network)."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz

from rocket.ml.feature_extractor import FEATURE_COLUMNS, RocketFeatureExtractor
from rocket.ml.meta_filter import MetaModelConfig, RocketMetaFilter
from rocket.ml.pipeline import build_comparison_table, label_simulated_trade, path_label_signal
from rocket.ml.trade_selector import (
    ConfluenceGatesConfig,
    DailyTradeRanker,
    apply_tiered_sizing,
    fractional_kelly,
)


IST = pytz.timezone("Asia/Kolkata")


def _buy_confluence(**extra):
    row = {
        "close": 1000.0,
        "rvol": 1.5,
        "rvol_raw": 1.5,
        "ema_20_15m": 990.0,
        "vwap": 995.0,
        "clv": 0.60,
        "market_breadth": 0.60,
        "timestamp": IST.localize(datetime(2026, 8, 3, 10, 0)),
    }
    row.update(extra)
    return row


def _sell_confluence(**extra):
    row = {
        "close": 1000.0,
        "rvol": 1.5,
        "rvol_raw": 1.5,
        "ema_20_15m": 1015.0,
        "vwap": 1012.0,
        "clv": -0.60,
        "market_breadth": 0.40,
        "timestamp": IST.localize(datetime(2026, 8, 3, 10, 0)),
    }
    row.update(extra)
    return row


def _synth_ohlcv(n: int = 80) -> pd.DataFrame:
    start = IST.localize(datetime(2026, 8, 3, 9, 15))
    rows = []
    px = 1000.0
    for i in range(n):
        ts = start + timedelta(minutes=5 * i)
        if ts.weekday() >= 5:
            continue
        if ts.time().hour >= 15 and ts.time().minute > 30:
            continue
        o = px
        c = px * (1.0 + np.sin(i / 7.0) * 0.002)
        h = max(o, c) * 1.001
        l = min(o, c) * 0.999
        rows.append(
            {
                "timestamp": ts,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": 1000 + i * 10,
                "oi": 10000 + i,
            }
        )
        px = c
    return pd.DataFrame(rows)


def test_feature_extractor_columns():
    df = RocketFeatureExtractor.calculate_indicators(_synth_ohlcv())
    assert "atr_14" in df.columns
    assert "vwap" in df.columns
    assert "ema_10" in df.columns
    assert "ema5_dist_atr" in df.columns
    assert "ema_20_15m" in df.columns
    assert "rvol" in df.columns
    assert "clv" in df.columns
    assert "is_open_drive" in df.columns
    feats = RocketFeatureExtractor.extract_trade_features(df, 40, "BUY")
    for col in FEATURE_COLUMNS:
        assert col in feats
        assert np.isfinite(feats[col])
    assert "ema5_dist_atr" in feats
    assert "raw_rsi_14" in feats
    assert "ema_20_15m" in feats
    assert "rvol_raw" in feats
    assert "clv" in feats


def test_trailing_stop_ratchet():
    from rocket.engine.order_book import Side
    from rocket.engine.portfolio import Position

    pos = Position(
        symbol="ABC",
        instrument_key="NSE_FO|ABC",
        side=Side.BUY,
        quantity=50,
        avg_price=1000.0,
        lot_size=50,
        stop_loss=1000.0 - 1.8 * 10.0,
        take_profit=1000.0 + 3.2 * 10.0,
        atr=10.0,
    )
    # Not enough favorable move yet (<1.2×ATR)
    pos.update_trailing_stop(high=1010.0, low=998.0, activate_at_r=1.2, trail_atr_mult=1.8)
    assert pos.trail_activated is False
    assert abs(pos.stop_loss - 982.0) < 1e-6

    # +1.2×ATR: activate and ratchet to high − 1.8ATR
    pos.update_trailing_stop(high=1012.0, low=1000.0, activate_at_r=1.2, trail_atr_mult=1.8)
    assert pos.trail_activated is True
    assert abs(pos.stop_loss - (1012.0 - 18.0)) < 1e-6

    # Further high ratchets stop up only
    pos.update_trailing_stop(high=1020.0, low=1005.0, activate_at_r=1.2, trail_atr_mult=1.8)
    assert abs(pos.stop_loss - (1020.0 - 18.0)) < 1e-6


def test_breakeven_lock_and_tighter_trail():
    from rocket.engine.order_book import Side
    from rocket.engine.portfolio import Position

    pos = Position(
        symbol="ABC",
        instrument_key="NSE_FO|ABC",
        side=Side.BUY,
        quantity=50,
        avg_price=1000.0,
        lot_size=50,
        stop_loss=1000.0 - 1.4 * 10.0,
        take_profit=1000.0 + 3.0 * 10.0,
        atr=10.0,
    )
    # +1.0×ATR locks breakeven; never give back the full structural SL
    newly = pos.update_breakeven_stop(high=1010.0, low=1000.0, trigger_atr_mult=1.0, buffer=0.05)
    assert newly is True
    assert pos.breakeven_locked is True
    assert abs(pos.stop_loss - 1000.05) < 1e-6
    assert pos.stop_exit_reason() == "breakeven_exit"

    # Post-BE: trail only activates at +1.6×ATR with 1.2×ATR distance
    pos.update_trailing_stop(high=1012.0, low=1000.0, activate_at_r=1.6, trail_atr_mult=1.2)
    assert pos.trail_activated is False
    assert abs(pos.stop_loss - 1000.05) < 1e-6

    pos.update_trailing_stop(high=1016.0, low=1005.0, activate_at_r=1.6, trail_atr_mult=1.2)
    assert pos.trail_activated is True
    assert abs(pos.stop_loss - (1016.0 - 12.0)) < 1e-6
    assert pos.stop_exit_reason() == "trailing_stop"

    # Short side
    short = Position(
        symbol="XYZ",
        instrument_key="NSE_FO|XYZ",
        side=Side.SELL,
        quantity=50,
        avg_price=1000.0,
        lot_size=50,
        stop_loss=1000.0 + 1.4 * 10.0,
        take_profit=1000.0 - 3.0 * 10.0,
        atr=10.0,
    )
    short.update_breakeven_stop(high=1000.0, low=990.0, trigger_atr_mult=1.0, buffer=0.05)
    assert short.breakeven_locked is True
    assert abs(short.stop_loss - 999.95) < 1e-6
    assert short.stop_exit_reason() == "breakeven_exit"


def test_stagnation_time_exit():
    from rocket.engine.order_book import Side
    from rocket.engine.portfolio import Position

    pos = Position(
        symbol="ABC",
        instrument_key="NSE_FO|ABC",
        side=Side.BUY,
        quantity=50,
        avg_price=1000.0,
        lot_size=50,
        atr=10.0,
        peak_favorable_price=1000.0,
        bars_in_trade=0,
        time_exit_armed=True,
    )
    # Weak MFE through 2 bars → exit
    pos.update_mfe(high=1003.0, low=998.0)
    pos.maybe_disarm_time_exit(0.5)
    pos.bars_in_trade = 2
    assert pos.mfe() < 5.0
    assert pos.time_exit_armed is True
    assert pos.should_stagnation_exit(time_exit_bars=2, time_exit_atr_min=0.5) is True

    # Strong MFE disarms timer
    pos2 = Position(
        symbol="ABC",
        instrument_key="NSE_FO|ABC",
        side=Side.SELL,
        quantity=50,
        avg_price=1000.0,
        lot_size=50,
        atr=10.0,
        peak_favorable_price=1000.0,
        bars_in_trade=0,
        time_exit_armed=True,
    )
    pos2.update_mfe(high=1001.0, low=994.0)  # MFE = 6 ≥ 0.5*10
    pos2.maybe_disarm_time_exit(0.5)
    pos2.bars_in_trade = 2
    assert pos2.time_exit_armed is False
    assert pos2.should_stagnation_exit(time_exit_bars=2, time_exit_atr_min=0.5) is False


def test_fractional_kelly_and_tier_sizing():
    f = fractional_kelly(0.80, 2.2, kelly_factor=0.35)
    assert 0.0 < f <= 0.35

    high = apply_tiered_sizing(
        _buy_confluence(
            win_probability=0.55,
            side="BUY",
            entry_price=1000.0,
            atr=10.0,
            safe_atr=10.0,
            ema_5=1001.0,
            ema_20=990.0,
            ema5_dist_atr=0.1,
            ema20_dist_atr=1.0,
            raw_rsi_14=55.0,
            lot_size=50,
        ),
        is_top_rank=True,
    )
    assert high is not None
    assert high["tier"] == 1
    assert high["lots"] == 2
    # Long: struct=min(990,995)=990; SL=min(988, max(990,984))=988
    assert abs(high["stop_loss"] - 988.0) < 1e-6
    assert high["total_risk"] <= 8000.0

    mid = apply_tiered_sizing(
        _sell_confluence(
            win_probability=0.45,
            side="SELL",
            entry_price=1000.0,
            atr=10.0,
            safe_atr=10.0,
            ema_5=999.0,
            ema_20=1015.0,
            ema5_dist_atr=0.1,
            ema20_dist_atr=1.5,
            raw_rsi_14=45.0,
            lot_size=50,
        ),
        is_top_rank=False,
    )
    assert mid is not None
    assert mid["tier"] == 2
    assert mid["lots"] == 1
    # Short: struct=max(1015,1012)=1015; SL=max(1012, min(1015,1016))=1015
    assert abs(mid["stop_loss"] - 1015.0) < 1e-6

    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.42,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is not None
    )
    assert apply_tiered_sizing({"win_probability": 0.37, "entry_price": 1000.0, "atr": 10.0}) is None
    # Below calibrated floor
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.10,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is None
    )
    # Weak CLV rejected (below ±0.15)
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.55,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                clv=0.10,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is None
    )
    # Breadth below long min rejected
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.55,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                market_breadth=0.49,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is None
    )
    # Asymmetric breadth chop zone rejected
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.55,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                market_breadth=0.50,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            ),
            gates=ConfluenceGatesConfig(breadth_long_min=0.52, breadth_short_max=0.48),
        )
        is None
    )
    # Low RVOL rejected
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.55,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                rvol=1.0,
                rvol_raw=1.0,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is None
    )
    # HTF counter-trend rejected
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.55,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                ema_20_15m=1010.0,  # close below HTF EMA
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is None
    )
    # Artifact spike must be excluded
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.95,
                side="BUY",
                entry_price=1000.0,
                atr=10.0,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is None
    )
    # Post-curfew rejected
    assert (
        apply_tiered_sizing(
            _buy_confluence(
                win_probability=0.55,
                side="BUY",
                timestamp=IST.localize(datetime(2026, 8, 3, 13, 0)),
                entry_price=1000.0,
                atr=10.0,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
        is None
    )


def test_gates_and_risk_cap():
    assert (
        apply_tiered_sizing(
            {
                "win_probability": 0.70,
                "side": "SELL",
                "entry_price": 170.95,
                "atr": 1.76,
                "safe_atr": 1.76,
                "ema5_dist_atr": 2.73,
                "ema20_dist_atr": 1.0,
                "raw_rsi_14": 45.0,
                "lot_size": 5000,
                "ema_20": 172.0,
                "vwap": 173.0,
            }
        )
        is None
    )
    assert (
        apply_tiered_sizing(
            {
                "win_probability": 0.70,
                "side": "SELL",
                "entry_price": 1000.0,
                "atr": 10.0,
                "ema5_dist_atr": 0.1,
                "ema20_dist_atr": 0.5,
                "raw_rsi_14": 20.0,
                "lot_size": 50,
                "ema_20": 1010.0,
                "vwap": 1010.0,
            }
        )
        is None
    )
    assert (
        apply_tiered_sizing(
            {
                "win_probability": 0.70,
                "side": "SELL",
                "entry_price": 170.95,
                "atr": 1.76,
                "safe_atr": 1.76,
                "ema5_dist_atr": 0.2,
                "ema20_dist_atr": 0.5,
                "raw_rsi_14": 45.0,
                "lot_size": 5000,
                "ema_20": 174.13,
                "vwap": 175.0,
            }
        )
        is None
    )


def test_ordinal_daily_selection_with_curfew():
    """0.12≤P≤0.85 ordinal 0–3/day with breadth+CLV+HTF+RVOL; empty days stay empty."""
    rows = []
    base = IST.localize(datetime(2026, 8, 3, 10, 0))
    day1_probs = [0.55, 0.48, 0.42, 0.39, 0.35, 0.95, 0.18]
    for i, p in enumerate(day1_probs):
        rows.append(
            _buy_confluence(
                timestamp=base,
                trade_date="2026-08-03",
                symbol=f"W{i}",
                side="BUY",
                win_probability=p,
                strategy_confidence=0.6,
                entry_price=1000.0,
                atr=10.0,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
    rows.append(
        _buy_confluence(
            timestamp=IST.localize(datetime(2026, 8, 3, 14, 45)),
            trade_date="2026-08-03",
            symbol="LATE",
            side="BUY",
            win_probability=0.80,
            strategy_confidence=0.9,
            entry_price=1000.0,
            atr=10.0,
            ema5_dist_atr=0.1,
            ema20_dist_atr=0.5,
            raw_rsi_14=50.0,
            lot_size=50,
            ema_20=990.0,
        )
    )
    for i, p in enumerate([0.95, 0.90, 0.08, 0.05]):
        rows.append(
            _buy_confluence(
                timestamp=base + timedelta(days=1),
                trade_date="2026-08-04",
                symbol=f"A{i}",
                side="BUY",
                win_probability=p,
                strategy_confidence=0.6,
                entry_price=1000.0,
                atr=10.0,
                ema5_dist_atr=0.1,
                ema20_dist_atr=0.5,
                raw_rsi_14=50.0,
                lot_size=50,
                ema_20=990.0,
            )
        )
    selected = DailyTradeRanker(None, max_trades_per_day=3, min_trades_per_day=0).rank_and_select(
        rows
    )
    by_day = {}
    for s in selected:
        by_day.setdefault(s["trade_date"], []).append(s)
    assert "2026-08-03" in by_day
    assert 1 <= len(by_day["2026-08-03"]) <= 3
    assert by_day["2026-08-03"][0]["lots"] == 2
    assert by_day["2026-08-03"][0]["symbol"] == "W0"
    assert all(0.12 <= float(s["win_probability"]) <= 0.85 for s in selected)
    assert all(s["symbol"] != "LATE" for s in selected)
    assert "2026-08-04" not in by_day


def test_open_drive_does_not_outrank_higher_p():
    """Daily rank is pure win_probability — open-drive is not a boost."""
    base = IST.localize(datetime(2026, 8, 3, 10, 0))
    rows = [
        _buy_confluence(
            timestamp=base,
            trade_date="2026-08-03",
            symbol="DRIVE",
            side="BUY",
            win_probability=0.50,
            strategy_confidence=0.55,
            is_open_drive=1,
            entry_price=1000.0,
            atr=10.0,
            ema5_dist_atr=0.1,
            ema20_dist_atr=0.5,
            raw_rsi_14=50.0,
            lot_size=50,
            ema_20=990.0,
        ),
        _buy_confluence(
            timestamp=base,
            trade_date="2026-08-03",
            symbol="MIDDAY",
            side="BUY",
            win_probability=0.55,
            strategy_confidence=0.9,
            is_open_drive=0,
            entry_price=1000.0,
            atr=10.0,
            ema5_dist_atr=0.1,
            ema20_dist_atr=0.5,
            raw_rsi_14=50.0,
            lot_size=50,
            ema_20=990.0,
        ),
    ]
    selected = DailyTradeRanker(None, max_trades_per_day=1, min_trades_per_day=0).rank_and_select(
        rows
    )
    assert len(selected) == 1
    assert selected[0]["symbol"] == "MIDDAY"


def test_expansion_only_labels():
    assert label_simulated_trade(0.2, "take_profit") == 1
    assert label_simulated_trade(1.8, "trailing_stop") == 1
    assert label_simulated_trade(0.05, "breakeven_exit") == 0
    assert label_simulated_trade(-0.1, "time_stagnation_exit") == 0
    assert label_simulated_trade(-1.4, "stop_loss") == 0
    assert label_simulated_trade(0.4, "early_trend_invalidation") == 0


def test_early_trend_invalidation():
    from rocket.engine.order_book import Side
    from rocket.engine.portfolio import Position

    pos = Position(
        symbol="ABC",
        instrument_key="NSE_FO|ABC",
        side=Side.BUY,
        quantity=50,
        avg_price=1000.0,
        lot_size=50,
        atr=10.0,
        bars_in_trade=1,
    )
    assert pos.should_early_invalidate(close=989.0, ema_20=990.0) is True
    pos.bars_in_trade = 3
    assert pos.should_early_invalidate(close=989.0, ema_20=990.0) is False
    short = Position(
        symbol="XYZ",
        instrument_key="NSE_FO|XYZ",
        side=Side.SELL,
        quantity=50,
        avg_price=1000.0,
        lot_size=50,
        atr=10.0,
        bars_in_trade=2,
    )
    assert short.should_early_invalidate(close=1001.0, ema_20=990.0) is True
    assert short.should_early_invalidate(close=980.0, ema_20=990.0) is False


def test_path_label_and_walk_forward_selector():
    df = RocketFeatureExtractor.calculate_indicators(_synth_ohlcv(120))
    rows = []
    for day_offset, side in [(0, "BUY"), (0, "SELL"), (1, "BUY"), (1, "BUY"), (2, "SELL")]:
        idx = 30 + day_offset * 20
        close = float(df.iloc[idx]["close"])
        atr = float(df.iloc[idx]["safe_atr"])
        sl = close - 1.4 * atr if side == "BUY" else close + 1.4 * atr
        tp = close + 3.0 * atr if side == "BUY" else close - 3.0 * atr
        label = path_label_signal(df, idx, side, sl, tp, close)
        feats = RocketFeatureExtractor.extract_trade_features(df, idx, side)
        rows.append(
            {
                "timestamp": df.iloc[idx]["timestamp"],
                "trade_date": pd.Timestamp(df.iloc[idx]["timestamp"]).date(),
                "symbol": f"SYM{day_offset}",
                "side": side,
                "strategy_confidence": 0.7,
                "entry_price": close,
                "close": close,
                "atr": atr,
                **feats,
                **label,
            }
        )
    base = pd.DataFrame(rows)
    big = pd.concat([base] * 15, ignore_index=True)
    big["symbol"] = [f"S{i%30}" for i in range(len(big))]
    big["trade_date"] = pd.to_datetime(big["timestamp"]).dt.date

    meta = RocketMetaFilter(MetaModelConfig(min_train_samples=10, scoring_threshold=0.30))
    scored = meta.score_walk_forward(big)
    assert "win_probability" in scored.columns
    assert scored["win_probability"].notna().all()

    scored = scored.copy()
    rng = np.random.default_rng(42)
    scored["win_probability"] = 0.36 + rng.random(len(scored)) * 0.30
    scored.loc[scored.index[:5], "win_probability"] = 0.95
    scored["timestamp"] = IST.localize(datetime(2026, 8, 3, 10, 30))
    scored["side"] = "BUY"
    scored["close"] = scored["entry_price"]
    scored["rvol"] = 1.5
    scored["rvol_raw"] = 1.5
    scored["ema_20_15m"] = scored["entry_price"] - 10.0
    scored["vwap"] = scored["entry_price"] - 5.0
    scored["clv"] = 0.60
    scored["market_breadth"] = 0.60
    scored["ema5_dist_atr"] = 0.1
    scored["ema20_dist_atr"] = 0.5
    scored["raw_rsi_14"] = 50.0
    scored["lot_size"] = 50
    scored["ema_20"] = scored["entry_price"] - 10.0
    scored["safe_atr"] = scored["atr"]
    selected = DailyTradeRanker(meta, max_trades_per_day=3, min_trades_per_day=0).rank_and_select(
        scored
    )
    assert isinstance(selected, list)
    assert all(int(s.get("lots") or 0) >= 1 for s in selected)
    assert all(s.get("stop_loss") is not None for s in selected)
    assert all(0.12 <= float(s["win_probability"]) <= 0.85 for s in selected)

    from rocket.ml.pipeline import iter_confluence_sweep_grid, pick_best_confluence_config

    grid = iter_confluence_sweep_grid()
    assert len(grid) == 36
    fake_rows = [
        {
            "label": "a",
            "total_trades": 10,
            "profit_factor": 1.5,
            "profit_factor_num": 1.5,
            "in_target_band": False,
        },
        {
            "label": "b",
            "total_trades": 24,
            "profit_factor": 0.9,
            "profit_factor_num": 0.9,
            "in_target_band": True,
        },
        {
            "label": "c",
            "total_trades": 28,
            "profit_factor": 1.2,
            "profit_factor_num": 1.2,
            "in_target_band": True,
        },
    ]
    best = pick_best_confluence_config(fake_rows)
    assert best is not None and best["label"] == "c"

    cmp = build_comparison_table(
        {
            "total_trades": 100,
            "win_rate_pct": 40.0,
            "profit_factor": 0.8,
            "max_drawdown_pct": 5.0,
            "expectancy": -100.0,
            "net_return_pct": -2.0,
            "final_equity": 9_800_000,
            "costs": {"total": 1.0},
            "trades": [],
        },
        {
            "total_trades": 30,
            "win_rate_pct": 55.0,
            "profit_factor": 1.2,
            "max_drawdown_pct": 2.0,
            "expectancy": 50.0,
            "net_return_pct": 1.0,
            "final_equity": 10_100_000,
            "costs": {"total": 0.5},
            "trades": [],
        },
    )
    assert any(r["metric"] == "Total Trades" for r in cmp)

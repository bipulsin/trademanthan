"""Offline tests for Rocket ML meta-filter (no network)."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz

from rocket.ml.feature_extractor import FEATURE_COLUMNS, RocketFeatureExtractor
from rocket.ml.meta_filter import MetaModelConfig, RocketMetaFilter
from rocket.ml.pipeline import build_comparison_table, path_label_signal
from rocket.ml.trade_selector import (
    DailyTradeRanker,
    apply_tiered_sizing,
    fractional_kelly,
)


IST = pytz.timezone("Asia/Kolkata")


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
    assert "is_open_drive" in df.columns
    feats = RocketFeatureExtractor.extract_trade_features(df, 40, "BUY")
    for col in FEATURE_COLUMNS:
        assert col in feats
        assert np.isfinite(feats[col])
    assert "ema5_dist_atr" in feats
    assert "raw_rsi_14" in feats


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
    # Not enough favorable move yet
    pos.update_trailing_stop(high=1005.0, low=998.0, activate_at_r=1.0, trail_atr_mult=2.0)
    assert pos.trail_activated is False
    assert abs(pos.stop_loss - 982.0) < 1e-6

    # +1.0×ATR: activate and ratchet to high − 2ATR
    pos.update_trailing_stop(high=1010.0, low=1000.0, activate_at_r=1.0, trail_atr_mult=2.0)
    assert pos.trail_activated is True
    assert abs(pos.stop_loss - (1010.0 - 20.0)) < 1e-6

    # Further high ratchets stop up only
    pos.update_trailing_stop(high=1020.0, low=1005.0, activate_at_r=1.0, trail_atr_mult=2.0)
    assert abs(pos.stop_loss - (1020.0 - 20.0)) < 1e-6


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
        {
            "win_probability": 0.40,
            "side": "BUY",
            "entry_price": 1000.0,
            "atr": 10.0,
            "safe_atr": 10.0,
            "ema_5": 1001.0,
            "ema_20": 990.0,
            "vwap": 985.0,
            "ema5_dist_atr": 0.1,
            "ema20_dist_atr": 1.0,
            "raw_rsi_14": 55.0,
            "lot_size": 50,
        },
        is_top_rank=True,
    )
    assert high is not None
    assert high["tier"] == 1
    assert high["lots"] == 2
    # Long: struct=min(990,985)=985; SL=min(986, max(985,980))=985
    assert abs(high["stop_loss"] - 985.0) < 1e-6
    assert high["total_risk"] <= 8000.0

    mid = apply_tiered_sizing(
        {
            "win_probability": 0.35,
            "side": "SELL",
            "entry_price": 1000.0,
            "atr": 10.0,
            "safe_atr": 10.0,
            "ema_5": 999.0,
            "ema_20": 1015.0,
            "vwap": 1012.0,
            "ema5_dist_atr": 0.1,
            "ema20_dist_atr": 1.5,
            "raw_rsi_14": 45.0,
            "lot_size": 50,
        },
        is_top_rank=False,
    )
    assert mid is not None
    assert mid["tier"] == 2
    assert mid["lots"] == 1
    # Short: struct=max(1015,1012)=1015; SL=max(1014, min(1015,1020))=1015
    assert abs(mid["stop_loss"] - 1015.0) < 1e-6

    assert apply_tiered_sizing(
        {
            "win_probability": 0.30,
            "side": "BUY",
            "entry_price": 1000.0,
            "atr": 10.0,
            "ema5_dist_atr": 0.1,
            "ema20_dist_atr": 0.5,
            "raw_rsi_14": 50.0,
            "lot_size": 50,
            "ema_20": 990.0,
            "vwap": 990.0,
        }
    ) is not None
    assert apply_tiered_sizing({"win_probability": 0.19, "entry_price": 1000.0, "atr": 10.0}) is None


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


def test_zscore_daily_selection():
    """Cross-sectional z≥1.65 + P≥0.20 → top 2–3/day; skip flat/weak days."""
    rows = []
    base = IST.localize(datetime(2026, 8, 3, 10, 0))
    # Day 1: two clear outliers above a dense pack → z≥1.65
    day1_probs = [0.55, 0.52, 0.48, 0.28, 0.27, 0.26, 0.25, 0.24, 0.23, 0.22]
    for i, p in enumerate(day1_probs):
        rows.append(
            {
                "timestamp": base,
                "trade_date": "2026-08-03",
                "symbol": f"W{i}",
                "side": "BUY",
                "win_probability": p,
                "strategy_confidence": 0.6,
                "entry_price": 1000.0,
                "atr": 10.0,
                "ema5_dist_atr": 0.1,
                "ema20_dist_atr": 0.5,
                "raw_rsi_14": 50.0,
                "lot_size": 50,
                "ema_20": 990.0,
                "vwap": 990.0,
            }
        )
    # Day 2: all near floor — no z≥1.65
    for i, p in enumerate([0.22, 0.21, 0.20, 0.19]):
        rows.append(
            {
                "timestamp": base + timedelta(days=1),
                "trade_date": "2026-08-04",
                "symbol": f"A{i}",
                "side": "BUY",
                "win_probability": p,
                "strategy_confidence": 0.6,
                "entry_price": 1000.0,
                "atr": 10.0,
                "ema5_dist_atr": 0.1,
                "ema20_dist_atr": 0.5,
                "raw_rsi_14": 50.0,
                "lot_size": 50,
                "ema_20": 990.0,
                "vwap": 990.0,
            }
        )
    selected = DailyTradeRanker(None, max_trades_per_day=3, min_trades_per_day=2).rank_and_select(
        rows
    )
    by_day = {}
    for s in selected:
        by_day.setdefault(s["trade_date"], []).append(s)
    assert "2026-08-03" in by_day
    assert 1 <= len(by_day["2026-08-03"]) <= 3
    assert by_day["2026-08-03"][0]["lots"] == 2
    assert all(s["z_score"] is not None and s["z_score"] >= 1.65 for s in by_day["2026-08-03"])
    assert "2026-08-04" not in by_day


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
    # Vary P within each day so z≥1.65 outliers exist
    rng = np.random.default_rng(42)
    scored["win_probability"] = 0.25 + rng.random(len(scored)) * 0.35
    scored["ema5_dist_atr"] = 0.1
    scored["ema20_dist_atr"] = 0.5
    scored["raw_rsi_14"] = 50.0
    scored["lot_size"] = 50
    scored["ema_20"] = scored["entry_price"] - 10.0
    scored["vwap"] = scored["entry_price"] - 10.0
    scored["safe_atr"] = scored["atr"]
    selected = DailyTradeRanker(meta, max_trades_per_day=3).rank_and_select(scored)
    assert isinstance(selected, list)
    assert all(int(s.get("lots") or 0) >= 1 for s in selected)
    assert all(s.get("stop_loss") is not None for s in selected)
    assert all(
        (s.get("z_score") is None) or (float(s["z_score"]) >= 1.65 - 1e-9) for s in selected
    )

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

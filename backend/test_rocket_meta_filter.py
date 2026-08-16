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
    f = fractional_kelly(0.80, 2.5, kelly_factor=0.35)
    assert 0.0 < f <= 0.35

    # Near EMA5, mid RSI, small lot → Tier1 2 lots with structural/fallback SL
    high = apply_tiered_sizing(
        {
            "win_probability": 0.80,
            "side": "BUY",
            "entry_price": 1000.0,
            "atr": 10.0,
            "safe_atr": 10.0,
            "ema_5": 1001.0,
            "ema_10": 990.0,
            "vwap": 985.0,
            "ema5_dist_atr": 0.1,
            "raw_rsi_14": 55.0,
            "lot_size": 50,
        }
    )
    assert high is not None
    assert high["tier"] == 1
    assert high["lots"] == 2
    assert high["stop_loss"] == 990.0  # max(ema10,vwap) below entry
    assert high["total_risk"] <= 3000.0

    mid = apply_tiered_sizing(
        {
            "win_probability": 0.55,
            "side": "SELL",
            "entry_price": 1000.0,
            "atr": 10.0,
            "safe_atr": 10.0,
            "ema_5": 999.0,
            "ema_10": 1010.0,
            "vwap": 1015.0,
            "ema5_dist_atr": 0.1,
            "raw_rsi_14": 45.0,
            "lot_size": 50,
        }
    )
    assert mid is not None
    assert mid["tier"] == 2
    assert mid["lots"] == 1
    assert mid["stop_loss"] == 1010.0  # min(ema10,vwap) above entry

    assert apply_tiered_sizing(
        {
            "win_probability": 0.50,
            "side": "BUY",
            "entry_price": 1000.0,
            "atr": 10.0,
            "ema_5": 1000.0,
            "ema5_dist_atr": 0.1,
            "raw_rsi_14": 50.0,
            "lot_size": 50,
            "ema_10": 990.0,
            "vwap": 990.0,
        }
    ) is not None
    assert apply_tiered_sizing({"win_probability": 0.49, "entry_price": 1000.0, "atr": 10.0}) is None


def test_gates_and_risk_cap():
    # Mid-air chase
    assert (
        apply_tiered_sizing(
            {
                "win_probability": 0.70,
                "side": "SELL",
                "entry_price": 170.95,
                "atr": 1.76,
                "safe_atr": 1.76,
                "ema5_dist_atr": 2.73,
                "raw_rsi_14": 45.0,
                "lot_size": 5000,
                "ema_10": 172.0,
                "vwap": 173.0,
            }
        )
        is None
    )
    # RSI exhaustion short
    assert (
        apply_tiered_sizing(
            {
                "win_probability": 0.70,
                "side": "SELL",
                "entry_price": 1000.0,
                "atr": 10.0,
                "ema5_dist_atr": 0.1,
                "raw_rsi_14": 20.0,
                "lot_size": 50,
                "ema_10": 1010.0,
                "vwap": 1010.0,
            }
        )
        is None
    )
    # ASHOKLEY-like: 1 lot × 1.8ATR risk ≈ ₹15.8k → reject
    assert (
        apply_tiered_sizing(
            {
                "win_probability": 0.70,
                "side": "SELL",
                "entry_price": 170.95,
                "atr": 1.76,
                "safe_atr": 1.76,
                "ema5_dist_atr": 0.2,
                "raw_rsi_14": 45.0,
                "lot_size": 5000,
                "ema_10": 174.13,
                "vwap": 175.0,
            }
        )
        is None
    )


def test_dynamic_soft_fill_hits_min_per_day():
    rows = []
    base = IST.localize(datetime(2026, 8, 3, 10, 0))
    # Day with only weak scores below 0.50 → no soft-fill
    for i, p in enumerate([0.40, 0.35, 0.30, 0.12]):
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
                "raw_rsi_14": 50.0,
                "lot_size": 50,
                "ema_10": 990.0,
                "vwap": 990.0,
            }
        )
    # Day with strong scores → prefer floor band, cap at 3
    for i, p in enumerate([0.80, 0.70, 0.60, 0.58, 0.56]):
        rows.append(
            {
                "timestamp": base + timedelta(days=1),
                "trade_date": "2026-08-04",
                "symbol": f"S{i}",
                "side": "BUY",
                "win_probability": p,
                "strategy_confidence": 0.6,
                "entry_price": 1000.0,
                "atr": 10.0,
                "ema5_dist_atr": 0.1,
                "raw_rsi_14": 50.0,
                "lot_size": 50,
                "ema_10": 990.0,
                "vwap": 990.0,
            }
        )
    selected = DailyTradeRanker(
        None, min_probability_threshold=0.50, max_trades_per_day=3, min_trades_per_day=2
    ).rank_and_select(rows)
    by_day: dict = {}
    for s in selected:
        by_day.setdefault(s["trade_date"], []).append(s)
    assert "2026-08-03" not in by_day  # nothing ≥ 0.50
    assert len(by_day["2026-08-04"]) == 3
    assert all(s["win_probability"] >= 0.50 for s in by_day["2026-08-04"])
    assert by_day["2026-08-04"][0]["tier"] == 1
    assert by_day["2026-08-04"][0]["lots"] == 2


def test_path_label_and_walk_forward_selector():
    df = RocketFeatureExtractor.calculate_indicators(_synth_ohlcv(120))
    rows = []
    for day_offset, side in [(0, "BUY"), (0, "SELL"), (1, "BUY"), (1, "BUY"), (2, "SELL")]:
        idx = 30 + day_offset * 20
        close = float(df.iloc[idx]["close"])
        atr = float(df.iloc[idx]["safe_atr"])
        sl = close - 1.8 * atr if side == "BUY" else close + 1.8 * atr
        tp = close + 3.2 * atr if side == "BUY" else close - 3.2 * atr
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

    meta = RocketMetaFilter(MetaModelConfig(min_train_samples=10, scoring_threshold=0.55))
    scored = meta.score_walk_forward(big)
    assert "win_probability" in scored.columns
    assert scored["win_probability"].notna().all()

    scored = scored.copy()
    scored["win_probability"] = 0.58
    scored["ema5_dist_atr"] = 0.1
    scored["raw_rsi_14"] = 50.0
    scored["lot_size"] = 50
    scored["ema_10"] = scored["entry_price"] - 10.0
    scored["vwap"] = scored["entry_price"] - 10.0
    scored["safe_atr"] = scored["atr"]
    selected = DailyTradeRanker(meta, min_probability_threshold=0.50, max_trades_per_day=3).rank_and_select(
        scored
    )
    assert isinstance(selected, list)
    assert all(int(s.get("lots") or 0) >= 1 for s in selected)
    assert all(s.get("stop_loss") is not None for s in selected)

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

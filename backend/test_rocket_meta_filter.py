"""Offline tests for Rocket ML meta-filter (no network)."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz

from rocket.ml.feature_extractor import FEATURE_COLUMNS, RocketFeatureExtractor
from rocket.ml.meta_filter import MetaModelConfig, RocketMetaFilter
from rocket.ml.pipeline import build_comparison_table, path_label_signal
from rocket.ml.trade_selector import DailyTradeRanker


IST = pytz.timezone("Asia/Kolkata")


def _synth_ohlcv(n: int = 80) -> pd.DataFrame:
    start = IST.localize(datetime(2026, 8, 3, 9, 15))
    rows = []
    px = 1000.0
    for i in range(n):
        ts = start + timedelta(minutes=5 * i)
        if ts.weekday() >= 5:
            continue
        # skip overnight gaps roughly
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
    assert "is_open_drive" in df.columns
    feats = RocketFeatureExtractor.extract_trade_features(df, 40, "BUY")
    for col in FEATURE_COLUMNS:
        assert col in feats
        assert np.isfinite(feats[col])


def test_path_label_and_walk_forward_selector():
    df = RocketFeatureExtractor.calculate_indicators(_synth_ohlcv(120))
    rows = []
    for day_offset, side in [(0, "BUY"), (0, "SELL"), (1, "BUY"), (1, "BUY"), (2, "SELL")]:
        idx = 30 + day_offset * 20
        close = float(df.iloc[idx]["close"])
        atr = float(df.iloc[idx]["safe_atr"])
        sl = close - 1.5 * atr if side == "BUY" else close + 1.5 * atr
        tp = close + 2.5 * atr if side == "BUY" else close - 2.5 * atr
        label = path_label_signal(df, idx, side, sl, tp, close)
        feats = RocketFeatureExtractor.extract_trade_features(df, idx, side)
        rows.append(
            {
                "timestamp": df.iloc[idx]["timestamp"],
                "trade_date": pd.Timestamp(df.iloc[idx]["timestamp"]).date(),
                "symbol": f"SYM{day_offset}",
                "side": side,
                "strategy_confidence": 0.7,
                **feats,
                **label,
            }
        )
    # inflate training set
    base = pd.DataFrame(rows)
    big = pd.concat([base] * 15, ignore_index=True)
    big["symbol"] = [f"S{i%30}" for i in range(len(big))]
    big["trade_date"] = pd.to_datetime(big["timestamp"]).dt.date

    meta = RocketMetaFilter(MetaModelConfig(min_train_samples=10, scoring_threshold=0.55))
    scored = meta.score_walk_forward(big)
    assert "win_probability" in scored.columns
    assert scored["win_probability"].notna().all()

    selected = DailyTradeRanker(meta, min_probability_threshold=0.55, max_trades_per_day=4).rank_and_select(
        scored
    )
    assert isinstance(selected, list)

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

"""ML Institutional Futures Engine hook + default directional model.

Production path: pass a scikit-learn / xgboost estimator (or any object with
``predict_proba(X)`` / ``predict(X)``) via ``model=``. Feature matrix columns are
documented in ``FEATURE_COLUMNS``.

Without a fitted model, a transparent institutional-momentum heuristic is used so
backtests remain runnable and auditable.
"""

from __future__ import annotations

from datetime import datetime, time
from typing import Any, Dict, List, Optional, Sequence

import numpy as np

from rocket.strategies.base_strategy import BaseStrategy, Bias, Signal

FEATURE_COLUMNS: Sequence[str] = (
    "ret_1",
    "ret_5",
    "vwap_dist_pct",
    "vol_z",
    "atr_pct",
    "mom_10",
    "range_pct",
    "oi_chg_pct",
)


class MLInstitutionalStrategy(BaseStrategy):
    name = "ml_institutional"

    def __init__(
        self,
        *,
        model: Any = None,
        min_confidence: float = 0.58,
        max_signals_per_bar: int = 3,
        atr_stop_mult: float = 1.8,
        atr_target_mult: float = 3.2,
        session_start: time = time(9, 30),
        session_end: time = time(14, 45),
    ):
        self.model = model
        self.min_confidence = float(min_confidence)
        self.max_signals_per_bar = int(max_signals_per_bar)
        self.atr_stop_mult = float(atr_stop_mult)
        self.atr_target_mult = float(atr_target_mult)
        self.session_start = session_start
        self.session_end = session_end

    def generate_signals(
        self,
        timestamp: datetime,
        market_snapshot: Dict[str, Dict[str, Any]],
    ) -> List[Signal]:
        t = timestamp.timetz().replace(tzinfo=None) if timestamp.tzinfo else timestamp.time()
        if t < self.session_start or t > self.session_end:
            return []

        scored: List[Signal] = []
        for sym, bar in market_snapshot.items():
            if bar.get("position"):
                continue  # one position per symbol; exits handled by engine stops
            feats = self._features(bar)
            if feats is None:
                continue
            bias, conf = self._predict(feats)
            if bias == Bias.NEUTRAL or conf < self.min_confidence:
                continue
            close = float(bar["close"])
            atr = float(bar.get("safe_atr") or bar.get("atr") or close * 0.005)
            from rocket.engine.backtester import compute_structural_stop_target

            levels = compute_structural_stop_target(
                side="BUY" if bias == Bias.LONG else "SELL",
                entry_price=close,
                ema_20=bar.get("ema_20"),
                ema_10=bar.get("ema_10"),
                vwap=bar.get("vwap"),
                safe_atr=atr,
            )
            sl = float(levels["stop_loss"])
            tp = float(levels["take_profit"])
            scored.append(
                Signal(
                    symbol=sym,
                    instrument_key=str(bar.get("instrument_key") or ""),
                    bias=bias,
                    confidence=conf,
                    target=tp,
                    stop_loss=sl,
                    lots=1,
                    reason="ml_institutional",
                    features=dict(zip(FEATURE_COLUMNS, feats.tolist())),
                    atr=atr,
                )
            )

        scored.sort(key=lambda s: s.confidence, reverse=True)
        return scored[: self.max_signals_per_bar]

    def _predict(self, feats: np.ndarray) -> tuple[Bias, float]:
        if self.model is not None:
            return self._predict_model(feats)
        return self._predict_heuristic(feats)

    def _predict_model(self, feats: np.ndarray) -> tuple[Bias, float]:
        X = feats.reshape(1, -1)
        if hasattr(self.model, "predict_proba"):
            proba = np.asarray(self.model.predict_proba(X)[0], dtype=float)
            # Expect classes ordered [SHORT, NEUTRAL, LONG] or binary [SHORT, LONG]
            if len(proba) == 3:
                idx = int(np.argmax(proba))
                mapping = {0: Bias.SHORT, 1: Bias.NEUTRAL, 2: Bias.LONG}
                return mapping[idx], float(proba[idx])
            if len(proba) == 2:
                p_long = float(proba[1])
                if p_long >= 0.55:
                    return Bias.LONG, p_long
                if p_long <= 0.45:
                    return Bias.SHORT, 1.0 - p_long
                return Bias.NEUTRAL, 0.5
        pred = self.model.predict(X)[0]
        label = str(pred).upper()
        if label in ("1", "LONG", "BUY"):
            return Bias.LONG, 0.66
        if label in ("-1", "SHORT", "SELL"):
            return Bias.SHORT, 0.66
        return Bias.NEUTRAL, 0.5

    @staticmethod
    def _predict_heuristic(feats: np.ndarray) -> tuple[Bias, float]:
        """
        Institutional momentum proxy:
        - Long: positive multi-bar momentum, price above VWAP, elevated volume
        - Short: inverse
        """
        ret_1, ret_5, vwap_dist, vol_z, atr_pct, mom_10, range_pct, oi_chg = feats.tolist()
        long_score = 0.0
        short_score = 0.0
        if mom_10 > 0 and ret_5 > 0 and vwap_dist > 0:
            long_score += 0.35
        if mom_10 < 0 and ret_5 < 0 and vwap_dist < 0:
            short_score += 0.35
        if vol_z > 0.5:
            long_score += 0.15 if mom_10 > 0 else 0.0
            short_score += 0.15 if mom_10 < 0 else 0.0
        if oi_chg > 0 and mom_10 > 0:
            long_score += 0.15  # long buildup proxy
        if oi_chg > 0 and mom_10 < 0:
            short_score += 0.15  # short buildup proxy
        if atr_pct < 0.012:
            long_score *= 0.9
            short_score *= 0.9
        # mild mean-reversion dampener on stretched range
        if range_pct > 0.02:
            long_score *= 0.85
            short_score *= 0.85

        if long_score >= short_score and long_score >= 0.45:
            return Bias.LONG, min(0.95, 0.5 + long_score)
        if short_score > long_score and short_score >= 0.45:
            return Bias.SHORT, min(0.95, 0.5 + short_score)
        return Bias.NEUTRAL, 0.5

    @staticmethod
    def _features(bar: Dict[str, Any]) -> Optional[np.ndarray]:
        try:
            vals = [float(bar.get(c, 0.0) or 0.0) for c in FEATURE_COLUMNS]
            if not np.isfinite(vals).all():
                return None
            return np.asarray(vals, dtype=float)
        except Exception:
            return None

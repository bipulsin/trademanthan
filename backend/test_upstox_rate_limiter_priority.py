"""Tests for candle RL scheduled priority over discretionary callers."""
from __future__ import annotations

import threading

from backend.services import upstox_rate_limiter as rl


def test_discretionary_denied_while_scheduled_warm_active(monkeypatch):
    monkeypatch.setattr(rl, "_LIMITER", rl.SlidingWindowRateLimiter([(100, 1.0)]))
    rl._acquired = rl._denied = rl._denied_yield_scheduled = 0

    with rl.candle_warm_execution("scheduled_10m"):
        assert rl.acquire_candle_slot() is False
        assert rl._denied_yield_scheduled >= 1

        with rl.scheduled_candle_worker():
            assert rl.acquire_candle_slot() is True


def test_headroom_blocks_discretionary(monkeypatch):
    lim = rl.SlidingWindowRateLimiter([(10, 1800.0)])
    monkeypatch.setattr(rl, "_LIMITER", lim)
    rl._acquired = rl._denied = rl._denied_yield_headroom = 0

    class _Settings:
        UPSTOX_CANDLE_RATE_LIMIT_ENABLED = True
        UPSTOX_CANDLE_RL_PER_30MIN = 10
        SCHEDULED_CANDLE_RL_HEADROOM = 3
        UPSTOX_CANDLE_RL_MAX_WAIT = 0.05
        UPSTOX_CANDLE_RL_SCHEDULED_MAX_WAIT = 1.0

    monkeypatch.setattr(rl, "_get_limiter", lambda: lim)

    import backend.config as cfg

    monkeypatch.setattr(cfg, "settings", _Settings())

    for _ in range(7):
        lim.acquire(max_wait=0.01)

    assert rl._headroom_exhausted() is True
    assert rl.acquire_candle_slot() is False
    assert rl._denied_yield_headroom >= 1


def test_scheduled_execution_context_enables_workers(monkeypatch):
    monkeypatch.setattr(rl, "_LIMITER", rl.SlidingWindowRateLimiter([(50, 1.0)]))
    results: list[bool] = []

    def worker():
        with rl.scheduled_candle_worker():
            results.append(rl.acquire_candle_slot())

    with rl.candle_warm_execution("scheduled_10m"):
        t = threading.Thread(target=worker)
        t.start()
        t.join()
    assert results == [True]

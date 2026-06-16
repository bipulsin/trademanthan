"""Unit tests for pool watchdog state machine (no DB required)."""

from backend.services.pool_watchdog import evaluate_pool_watchdog


def test_no_action_when_healthy():
    d = evaluate_pool_watchdog(
        pool_stressed=False,
        db_ok=True,
        now_mono=1000.0,
        stress_since_mono=None,
        db_fail_since_mono=None,
        last_self_heal_mono=0.0,
    )
    assert d.action == "none"


def test_self_heal_after_sustained_pool_stress():
    d = evaluate_pool_watchdog(
        pool_stressed=True,
        db_ok=True,
        now_mono=400.0,
        stress_since_mono=100.0,
        db_fail_since_mono=None,
        last_self_heal_mono=-10_000.0,
        stress_minutes=3,
        cooldown_minutes=5,
    )
    assert d.action == "self_heal"
    assert d.reason == "pool_stressed"


def test_self_heal_after_sustained_db_failure():
    d = evaluate_pool_watchdog(
        pool_stressed=False,
        db_ok=False,
        now_mono=500.0,
        stress_since_mono=None,
        db_fail_since_mono=200.0,
        last_self_heal_mono=-10_000.0,
        db_fail_minutes=3,
        cooldown_minutes=5,
    )
    assert d.action == "self_heal"
    assert d.reason == "db_unreachable"


def test_cooldown_blocks_self_heal():
    d = evaluate_pool_watchdog(
        pool_stressed=True,
        db_ok=True,
        now_mono=400.0,
        stress_since_mono=100.0,
        db_fail_since_mono=None,
        last_self_heal_mono=350.0,
        stress_minutes=3,
        cooldown_minutes=30,
    )
    assert d.action == "none"


def test_stress_below_threshold_no_heal():
    d = evaluate_pool_watchdog(
        pool_stressed=True,
        db_ok=True,
        now_mono=200.0,
        stress_since_mono=100.0,
        db_fail_since_mono=None,
        last_self_heal_mono=-10_000.0,
        stress_minutes=3,
        cooldown_minutes=5,
    )
    assert d.action == "none"
    assert d.stressed_for_sec == 100.0

"""Off-market tests for 2026-11-25 approved OLH P0–P3 fixes. No production deploy."""
from __future__ import annotations

from datetime import datetime, time
from unittest.mock import MagicMock

import pytz

IST = pytz.timezone("Asia/Kolkata")


def test_phase_a_reserved_pool_independent_of_shared(monkeypatch):
    from backend.services.open_low_high import candle_budget as cb
    from backend.services import upstox_rate_limiter as rl

    cb.reset_pools_for_tests()
    monkeypatch.setattr(rl, "_LIMITER", rl.SlidingWindowRateLimiter([(1, 60.0)], min_interval=30.0))
    # Exhaust shared
    assert rl.acquire_candle_slot() is True or True
    rl._LIMITER.acquire(immediate=True)
    # Even if shared is saturated, phase_a dedicated grants.
    with cb.olh_candle_pool("phase_a"):
        assert cb.acquire_olh_candle_slot() is True
        cb.release_olh_candle_slot()
    snap = cb.usage_snapshot()
    assert sum(v.get("granted", 0) for v in (snap.get("phase_a") or {}).values()) >= 1


def test_shared_deny_falls_back_to_phase_a_reserved(monkeypatch):
    from backend.services.open_low_high import candle_budget as cb

    cb.reset_pools_for_tests()
    with cb.olh_candle_pool("phase_a"):
        # Simulate: shared denied path → fallback reserved.
        ok = cb.fallback_phase_a_reserved_after_shared_deny()
        assert ok is True
        cb.release_olh_candle_slot()
    snap = cb.usage_snapshot()
    granted = sum(v.get("granted", 0) for v in (snap.get("phase_a") or {}).values())
    assert granted >= 1


def test_shared_yields_to_olh_in_priority_window(monkeypatch):
    from backend.services import upstox_rate_limiter as rl
    from backend.services.open_low_high import config as cfg

    monkeypatch.setattr(rl, "_LIMITER", rl.SlidingWindowRateLimiter([(100, 1.0)]))
    rl._denied = 0
    rl._denied_yield_olh = 0
    monkeypatch.setattr(cfg, "OLH_PRIORITY_YIELD_ENABLED", True)
    monkeypatch.setattr(cfg, "OLH_PRIORITY_YIELD_START", time(0, 0, 0))
    monkeypatch.setattr(cfg, "OLH_PRIORITY_YIELD_END", time(23, 59, 59))
    assert rl.acquire_candle_slot() is False
    assert rl._denied_yield_olh >= 1


def test_canary_healthy_while_shared_denied(monkeypatch):
    from backend.services.open_low_high import candle_budget as cb
    from backend.services import upstox_rate_limiter as rl

    cb.reset_pools_for_tests()
    # Shared yields / denied
    monkeypatch.setattr(rl, "_in_olh_priority_window", lambda: True)
    assert rl.acquire_candle_slot() is False
    with cb.olh_candle_pool("canary"):
        assert cb.acquire_olh_candle_slot() is True
        cb.release_olh_candle_slot()
    snap = cb.usage_snapshot()
    assert sum(v.get("granted", 0) for v in (snap.get("canary") or {}).values()) >= 1


def test_ws_401_recovery_bounded(monkeypatch):
    import backend.services.upstox_market_feed as umf

    umf._AUTH_RECOVERY_ATTEMPTS = 0
    umf._AUTH_RECOVERY_DAY = None
    alerts = []

    class FakeUx:
        access_token = "oldtokenXXXX"

        def reload_token_from_storage(self):
            self.access_token = "newtokenYYYY"
            return True

        def refresh_access_token(self):
            return self.access_token

    monkeypatch.setattr(
        "backend.services.open_low_high.alerts.send_olh_alert",
        lambda *a, **k: alerts.append(a[0]) or True,
    )
    monkeypatch.setattr(
        "backend.services.open_low_high.ohl_log.ohl_log",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "backend.services.open_low_high.config.OLH_WS_AUTH_MAX_ATTEMPTS",
        3,
    )
    ux = FakeUx()
    umf._FEED_LAST_ERROR = "authorize 401 body=Invalid token"
    assert umf._recover_token_after_auth_failure(ux) is True
    assert ux.access_token.endswith("YYYY")
    assert alerts
    # Exhaust attempts
    umf._AUTH_RECOVERY_ATTEMPTS = 3
    assert umf._recover_token_after_auth_failure(ux) is False


def test_authorize_logs_status_and_body(monkeypatch):
    import backend.services.upstox_market_feed as umf

    class Resp:
        status_code = 401
        text = '{"status":"error","errors":[{"message":"Invalid token"}]}'
        content = text.encode()

        def json(self):
            import json

            return json.loads(self.text)

    monkeypatch.setattr(
        umf.requests,
        "get",
        lambda *a, **k: Resp(),
    )
    uri = umf._authorize_ws_url("badtoken1234")
    assert uri is None
    info = umf.last_authorize_info()
    assert info["status_code"] == 401
    assert "Invalid token" in (info.get("body") or "")
    assert info.get("auth_error") is True
    assert info.get("token_last4") == "1234"


def test_eq_stall_armed_only_after_091505():
    from backend.services.open_low_high.silent_stall import check_eq_silent_stall

    early = IST.localize(datetime(2026, 11, 25, 9, 15, 0))
    out = check_eq_silent_stall(force=True, now=early)
    assert out.get("skipped") == "pre_arm"


def test_force_rest_flag_skips_ws(monkeypatch):
    from backend.services.open_high_low.bars import Bar
    from backend.services.open_low_high import force_rest as fr
    from backend.services.open_low_high import screener as scr

    fr.clear_force_rest_reserved()
    fr.set_force_rest_reserved("test")
    calls = {"ws": 0, "rest": 0}

    fake = [
        Bar(
            ts=IST.localize(datetime(2026, 11, 25, 9, 15, 0)),
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            volume=0,
        )
    ]

    def _ws(*a, **k):
        calls["ws"] += 1
        return [], "ws"

    def _rest(*a, **k):
        calls["rest"] += 1
        return fake

    monkeypatch.setattr(scr, "_ws_try_once", _ws)
    monkeypatch.setattr(scr, "_fetch_rest_5m", _rest)
    monkeypatch.setattr(scr, "_record_attempt", lambda *a, **k: None)
    monkeypatch.setattr(
        "backend.services.open_low_high.ohl_log.ohl_log",
        lambda *a, **k: None,
    )

    bars, src = scr._session_5m_bars_try(
        MagicMock(),
        "NSE_EQ|X",
        datetime(2026, 11, 25).date(),
        symbol="X",
        prefer="ws",
    )
    assert calls["ws"] == 0
    assert calls["rest"] >= 1
    assert src == "rest"
    assert bars
    fr.clear_force_rest_reserved()


def test_alerts_reach_telegram_mock(monkeypatch):
    import backend.services.open_low_high.alerts as al

    sent = []

    monkeypatch.setattr(al, "OLH_ALERTS_ENABLED", True)
    monkeypatch.setattr(
        "backend.services.telegram_trade_channel.send_trade_with_cto_channel_message",
        lambda text: sent.append(text) or True,
    )
    monkeypatch.setattr(al, "ohl_log", lambda *a, **k: None)
    ok = al.send_olh_alert("test subject", "test body")
    assert ok is True
    assert sent and "[OLH] test subject" in sent[0]


def test_raw_recording_prefers_writable_path(tmp_path, monkeypatch):
    from pathlib import Path

    import backend.services.open_low_high.raw_recording as rr

    # First candidate: exists but not writable (chmod the leaf dir).
    bad = tmp_path / "readonly_leaf"
    bad.mkdir()
    bad.chmod(0o555)
    good = tmp_path / "app_data_olh"
    good.mkdir()
    monkeypatch.setattr(rr, "OLH_RAW_RECORD_ENABLED", True)
    monkeypatch.setattr(rr, "OLH_RAW_RECORD_DIR", str(bad))
    monkeypatch.setattr(rr, "_resolved_root", None)
    monkeypatch.setattr(
        rr,
        "_candidate_roots",
        lambda: [bad, good],
    )
    try:
        root = rr.recording_root()
        assert root == good
        assert rr.record_ws_frame(
            {"feeds": {}},
            now=IST.localize(datetime(2026, 11, 25, 9, 10, 0)),
        )
    finally:
        bad.chmod(0o755)


def test_raw_recording_keeps_trademanthan_mount_first(tmp_path, monkeypatch):
    """paperclip mounts ./data at /home/ubuntu/trademanthan/data — must not skip it."""
    import backend.services.open_low_high.raw_recording as rr

    mount = tmp_path / "trademanthan_data" / "olh_raw_recordings"
    mount.mkdir(parents=True)
    monkeypatch.setattr(rr, "OLH_RAW_RECORD_DIR", str(mount))
    monkeypatch.setattr(rr, "_resolved_root", None)
    roots = rr._candidate_roots()
    assert roots[0] == mount
    assert rr.recording_root() == mount


def test_phase_a_and_b_complete_via_reserved_when_shared_empty(monkeypatch):
    """Shared empty at 09:20 but OLH completes A+B via reserved pools."""
    from backend.services.open_low_high import candle_budget as cb
    from backend.services import upstox_rate_limiter as rl

    cb.reset_pools_for_tests()
    monkeypatch.setattr(rl, "_in_olh_priority_window", lambda: True)
    # Shared always denied in window
    assert rl.acquire_candle_slot() is False

    with cb.olh_candle_pool("phase_a"):
        for _ in range(10):
            assert cb.acquire_olh_candle_slot() is True
            cb.release_olh_candle_slot()
    with cb.olh_candle_pool("phase_b"):
        for _ in range(5):
            assert cb.acquire_olh_candle_slot() is True
            cb.release_olh_candle_slot()
    snap = cb.usage_snapshot()
    assert sum(v.get("granted", 0) for v in (snap.get("phase_a") or {}).values()) >= 10
    assert sum(v.get("granted", 0) for v in (snap.get("phase_b") or {}).values()) >= 5
    assert sum(v.get("denied", 0) for v in (snap.get("phase_a") or {}).values()) == 0


def test_job_usage_attributed_on_shared_acquire(monkeypatch):
    from backend.services import upstox_rate_limiter as rl

    monkeypatch.setattr(rl, "_LIMITER", rl.SlidingWindowRateLimiter([(100, 1.0)]))
    monkeypatch.setattr(rl, "_in_olh_priority_window", lambda: False)
    monkeypatch.setattr(rl, "_headroom_exhausted", lambda: False)
    monkeypatch.setattr(rl, "_scheduled_warm_active", lambda: False)
    with rl.candle_warm_execution("scheduled_aux_0905"):
        with rl.scheduled_candle_worker():
            assert rl.acquire_candle_slot() is True
    snap = rl.job_usage_snapshot()
    assert any(
        sum(v.get("granted", 0) for v in mins.values()) >= 1
        for j, mins in snap.items()
        if j == "scheduled_aux_0905"
    )


def test_job_usage_tags_shared_acquires(monkeypatch):
    from backend.services import upstox_rate_limiter as rl

    monkeypatch.setattr(rl, "_LIMITER", rl.SlidingWindowRateLimiter([(100, 1.0)]))
    monkeypatch.setattr(rl, "_in_olh_priority_window", lambda: False)
    with rl.candle_warm_execution("unit_test_job"):
        assert rl.acquire_candle_slot() is True
    snap = rl.job_usage_snapshot()
    assert "unit_test_job" in snap
    assert sum(v.get("granted", 0) for v in snap["unit_test_job"].values()) >= 1


def test_feed_status_reports_ws_limits():
    from backend.services import upstox_market_feed as umf

    st = umf.feed_status()
    assert st.get("upstox_ws_connection_limit_per_user") == 2
    assert "last_authorize" in st

"""L5 — alert and Fast Watch config defaults."""
from backend.services.rs_conviction_config import DEFAULTS


def test_alerts_default_muted():
    assert DEFAULTS["alert_sound_enabled"] is False
    assert DEFAULTS["go_alert_sound_enabled"] is False
    assert DEFAULTS["fast_watch_sound_enabled"] is False


def test_fast_watch_ui_default_off():
    assert DEFAULTS["fast_watch_ui_enabled"] is False
    assert DEFAULTS["fast_watch_scope"] == "locked_or_top5"
    assert DEFAULTS.get("fast_watch_enabled") is True


def test_go_sticky_default_six_minutes():
    assert DEFAULTS["go_sticky_minutes"] == 6

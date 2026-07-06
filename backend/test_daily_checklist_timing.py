"""L2/L3 — GO timestamps, sticky window, staleness cap."""
from datetime import datetime, timedelta

import pytz

from backend.services.daily_checklist import D_GO, D_WATCH, SEC_GO, SEC_OUT, SEC_WATCH
from backend.services.daily_checklist_timing import apply_go_timing, apply_staleness_cap

IST = pytz.timezone("Asia/Kolkata")


def _go_derived():
    return {
        "section": SEC_GO,
        "decision": D_GO,
        "gate_score": 9,
        "state_ok": True,
        "time_ok": True,
    }


def test_staleness_cap_blocks_go():
    out = apply_staleness_cap(_go_derived(), stale=True)
    assert out["section"] == SEC_WATCH
    assert out["decision"] == D_WATCH
    assert out["indicator_stale"] is True
    assert "stale" in (out.get("eligibility_note") or "").lower()


def test_go_timestamp_on_fresh_edge():
    now = IST.localize(datetime(2026, 7, 6, 11, 0, 0))
    prev = {"section": SEC_WATCH, "go_enter_first_at": None, "go_sticky_until": None}
    out = apply_go_timing(_go_derived(), prev, stale=False, now=now)
    assert out["go_enter_first_at"] is not None
    assert out["go_sticky_until"] is not None
    sticky = datetime.fromisoformat(out["go_sticky_until"])
    first = datetime.fromisoformat(out["go_enter_first_at"])
    assert (sticky - first).total_seconds() == 6 * 60


def test_no_go_timestamp_when_stale():
    now = IST.localize(datetime(2026, 7, 6, 11, 0, 0))
    prev = {"section": SEC_WATCH}
    capped = apply_staleness_cap(_go_derived(), stale=True)
    out = apply_go_timing(capped, prev, stale=True, now=now)
    assert out.get("go_enter_first_at") is None


def test_sticky_holds_go_on_flicker():
    now = IST.localize(datetime(2026, 7, 6, 11, 2, 0))
    sticky_until = now + timedelta(minutes=4)
    prev = {
        "section": SEC_GO,
        "go_enter_first_at": (now - timedelta(minutes=2)).isoformat(),
        "go_sticky_until": sticky_until.isoformat(),
    }
    flicker = {
        "section": SEC_WATCH,
        "decision": D_WATCH,
        "gate_score": 7,
        "state_ok": True,
        "time_ok": True,
    }
    out = apply_go_timing(flicker, prev, stale=False, now=now)
    assert out["section"] == SEC_GO
    assert out.get("go_sticky_active") is True


def test_hard_fail_clears_sticky():
    now = IST.localize(datetime(2026, 7, 6, 11, 2, 0))
    prev = {
        "section": SEC_GO,
        "go_enter_first_at": (now - timedelta(minutes=2)).isoformat(),
        "go_sticky_until": (now + timedelta(minutes=4)).isoformat(),
    }
    hard = {
        "section": SEC_OUT,
        "decision": "🔴 NO TRADE",
        "state_ok": False,
        "time_ok": True,
        "gate_score": 3,
    }
    out = apply_go_timing(hard, prev, stale=False, now=now)
    assert out.get("go_sticky_until") is None


def test_sticky_expired_live_eval_wins():
    now = IST.localize(datetime(2026, 7, 6, 11, 10, 0))
    prev = {
        "section": SEC_GO,
        "go_enter_first_at": (now - timedelta(minutes=10)).isoformat(),
        "go_sticky_until": (now - timedelta(minutes=1)).isoformat(),
    }
    watch = {"section": SEC_WATCH, "decision": D_WATCH, "gate_score": 6, "state_ok": True, "time_ok": True}
    out = apply_go_timing(watch, prev, stale=False, now=now)
    assert out["section"] == SEC_WATCH

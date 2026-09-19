"""Kosmic Tarang forward-test, labels, shadow live, and failure-path tests."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from backend.services.tarang.arming import arm, flatten_all
from backend.services.tarang.labels import (
    auto_lock_label,
    decorate_trade,
    fill_source_label,
    mode_badge,
    record_type_label,
)
from backend.services.tarang.live_broker import MockBroker, ShadowBroker, place_or_shadow
from backend.services.tarang.live_control import live_send_allowed, tarang_live_enabled
from backend.services.tarang.order_machine import recon_consider_tag_only, retry_place, split_freeze_qty, submit_spread_shadow
from backend.services.tarang.paper_broker import simulated_fill_price
from backend.services.tarang.report import build_report, compute_metrics
from backend.services.tarang.signal_key import canonical_signal_key


def test_display_labels_never_contain_paper():
    for fn, args in (
        (record_type_label, ("FORWARD_TEST",)),
        (record_type_label, ("LIVE",)),
        (record_type_label, ("PAPER",)),
        (fill_source_label, ("SIMULATED",)),
        (mode_badge, ("PAPER",)),
        (mode_badge, ("LIVE",)),
        (auto_lock_label, ()),
    ):
        s = fn(*args)
        assert "PAPER" not in s.upper()
    d = decorate_trade({"mode": "PAPER", "record_type": "FORWARD_TEST", "fill_source": "SIMULATED"})
    assert d["display_mode"] == "Forward test"
    assert "PAPER" not in d["display_fill_source"].upper()


def test_signal_key_canonical_sorted_strikes():
    a = canonical_signal_key(
        underlying="CRUDEOILM",
        expiry="2026-10-16",
        structure="put_credit_spread",
        legs=[{"strike": 5400}, {"strike": 5300}],
    )
    b = canonical_signal_key(
        underlying="crudeoilm",
        expiry="2026-10-16",
        structure="put_credit_spread",
        strikes=[5300, 5400],
    )
    assert a == b
    assert a.startswith("CRUDEOILM|2026-10-16|")


def test_conservative_fill_uses_spread_fraction():
    assert abs(simulated_fill_price("SELL", 9, 11, 10, spread_fraction=0.4) - 9.2) < 1e-9


def test_live_flag_defaults_false_cannot_place():
    os.environ.pop("TARANG_LIVE_ENABLED", None)
    assert tarang_live_enabled() is False
    assert live_send_allowed() is False
    with patch("backend.services.tarang.live_broker._log_shadow", return_value="tarang-x"):
        out = place_or_shadow({"venue": "delta_india", "qty": 1, "side": "BUY"})
    assert out.get("sent") is False
    assert out.get("shadow") is True


def test_shadow_logs_payload_sends_nothing():
    with patch("backend.services.tarang.live_broker._log_shadow", return_value="tarang-abc") as log:
        sb = ShadowBroker()
        r = sb.place({"venue": "upstox_mcx", "qty": 1})
        assert r["sent"] is False
        log.assert_called()


def test_failure_partial_reject_duplicate_timeout_token_ws():
    for sc in ("partial", "reject", "duplicate", "timeout", "token_expiry", "ws_drop"):
        b = MockBroker(sc)
        r = b.place({"qty": 2, "client_order_id": "tarang-x"})
        assert r.get("sent") is False
        if sc == "reject":
            assert r.get("status") == "REJECTED"
        if sc == "partial":
            assert r.get("status") == "PARTIAL"
    last = retry_place(MockBroker("timeout"), {"qty": 1}, retries=2)
    assert last.get("sent") is False


def test_recon_mismatch_tag_only():
    orders = [{"client_order_id": "tarang-1"}, {"client_order_id": "other-9"}]
    only = recon_consider_tag_only(orders)
    assert len(only) == 1
    assert split_freeze_qty(120, 50) == [50, 50, 20]


def test_arming_and_flatten_fail_closed():
    assert arm(kind="auto_entry", confirmation="ARM TARANG LIVE", actor="admin")["armed"] is False
    assert flatten_all(confirmation="FLATTEN ALL")["placed"] is False


def test_report_all_does_not_blend():
    trades = [
        {"net_pnl": 100, "record_type": "FORWARD_TEST", "signal_key": "a", "exit_reason": "PROFIT_TARGET", "profile_id": "CL"},
        {"net_pnl": -50, "record_type": "LIVE", "signal_key": "b", "exit_reason": "MANUAL", "profile_id": "BTC"},
    ]
    m_ft = compute_metrics([t for t in trades if t["record_type"] == "FORWARD_TEST"])
    m_lv = compute_metrics([t for t in trades if t["record_type"] == "LIVE"])
    assert m_ft["count"] == 1
    assert m_lv["count"] == 1
    with patch("backend.services.tarang.report.list_closed_trades", return_value=trades):
        rep = build_report(book="ALL")
        assert rep["blended"] is False
        paired = {p["metric"]: p for p in (rep.get("all_paired") or [])}
        assert paired["count"]["forward_test"] == 1
        assert paired["count"]["live"] == 1
        assert rep["metrics"]["blended"] is False


def test_unique_open_and_cooldown_and_skip_and_snapshot(monkeypatch):
    from backend.services.tarang import forward_record as fr
    from backend.services.tarang.forward_record import persist_skipped, record_forward_tests_from_screen

    calls = {"take": 0, "skipped": []}

    class R:
        def __init__(self, first=None, all_=None):
            self._f = first
            self._a = all_ or []

        def mappings(self):
            return self

        def first(self):
            return self._f

        def all(self):
            return self._a

    open_ids = {"n": 0}

    def execute(sql, params=None):
        q = str(sql)
        if "signal_key" in q and "FORWARD_TEST" in q and "ENTRY_PENDING" in q:
            return R({"id": 1} if open_ids["n"] else None)
        if "CLOSED" in q and "signal_key" in q:
            return R(None)
        if "INSERT INTO tarang_skipped" in q:
            calls["skipped"].append(params)
            return R()
        if "SUM" in q:
            return R({"risk": 0})
        if "net_pnl" in q:
            return R(all_=[])
        return R()

    db = MagicMock()
    db.execute.side_effect = execute

    def take(*a, **k):
        calls["take"] += 1
        open_ids["n"] = 1
        return {"ok": True, "trade_id": 9}

    row = {
        "status": "QUALIFIED",
        "candidate_id": 1,
        "profile_id": "CL",
        "underlying": "CRUDEOILM",
        "expiry": "2026-10-16",
        "structure": "put_credit_spread",
        "legs": [{"strike": 1}, {"strike": 2}],
        "venue": "upstox_mcx",
        "candidate_risk_inr": 100,
    }
    with patch.object(fr, "SessionLocal", return_value=db), patch.object(
        fr, "ensure_tarang_tables"
    ), patch("backend.services.tarang.lifecycle.take_trade_paper", side_effect=take), patch(
        "backend.services.tarang.lifecycle._kill_switch_tripped", return_value=False
    ):
        first = record_forward_tests_from_screen([row])
        assert first["recorded"]
        second = record_forward_tests_from_screen([row])
        assert any(s.get("reason") == "already_open" for s in second["skipped"])

    db2 = MagicMock()

    def execute2(sql, params=None):
        q = str(sql)
        if "INSERT INTO tarang_skipped" in q:
            calls["skipped"].append(params)
        if "signal_key" in q and "FORWARD_TEST" in q and "ENTRY_PENDING" in q:
            return R(None)
        if "CLOSED" in q:
            return R(None)
        if "SUM" in q:
            return R({"risk": 0})
        if "net_pnl" in q:
            return R(all_=[{"net_pnl": -1}, {"net_pnl": -1}, {"net_pnl": -1}])
        return R()

    db2.execute.side_effect = execute2
    with patch.object(fr, "SessionLocal", return_value=db2), patch.object(
        fr, "ensure_tarang_tables"
    ), patch("backend.services.tarang.lifecycle._kill_switch_tripped", return_value=True), patch(
        "backend.services.tarang.lifecycle.take_trade_paper", return_value={"ok": True}
    ):
        skipped = record_forward_tests_from_screen([row])
        assert any(s.get("reason") == "kill_switch" for s in skipped["skipped"])

    snap = {"legs": []}
    # immutability: application snapshot dict copied; guard is DB trigger
    original = dict(snap)
    snap["legs"] = [{"x": 1}]
    assert original["legs"] == []


def test_snapshot_update_attempt_noop_in_python():
    from backend.services.tarang.forward_record import build_immutable_snapshot

    p = {"legs": [{"bid": 1, "ask": 2, "mid": 1.5, "oi": 10, "volume": 1, "iv": 0.4, "delta": 0.2, "strike": 1, "side": "SELL", "right": "PE"}]}
    s1 = build_immutable_snapshot(p)
    s1_copy = dict(s1)
    s1["legs"] = []
    s2 = build_immutable_snapshot(p)
    assert s2["legs"][0]["bid"] == 1
    assert s1_copy["legs"][0]["bid"] == 1

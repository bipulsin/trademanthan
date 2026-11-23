"""OLH reliability program tests (A–I) — offline / staging; no production deploy."""
from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path

import pytz

IST = pytz.timezone("Asia/Kolkata")


def test_a_feed_contract_handles_market_index_fut_ltpc():
    from backend.services.open_low_high.feed_contract import (
        HANDLED_FEED_TYPES,
        classify_feed_type,
        handled_feed_types_report,
        load_schema_fixtures,
        note_feed_observation,
        parse_fixture_to_ohlc,
        reset_feed_contract_for_tests,
    )

    reset_feed_contract_for_tests()
    fixtures = load_schema_fixtures()
    assert fixtures["_meta"]["raw_frames_recorded"] is False
    report = handled_feed_types_report()
    assert "fullFeed.marketFF" in report["handled"]
    assert "fullFeed.indexFF" in report["handled"]

    seen = set()
    rows = []
    for fr in fixtures["frames"]:
        ft = classify_feed_type(fr["feed"])
        seen.add(ft)
        note_feed_observation(
            fr["instrument_key"],
            fr["feed"],
            subscribed_keys={fr["instrument_key"]},
            now=IST.localize(datetime(2026, 11, 23, 9, 16, 0)),
        )
        if fr.get("expect_unknown"):
            assert ft not in HANDLED_FEED_TYPES
            continue
        o, h, l, c, oi, vol = parse_fixture_to_ohlc(fr["feed"])
        assert o is not None and h is not None and l is not None and c is not None
        # OI/volume nullable — EQ/INDEX may be None
        rows.append({"seg": fr["segment"], "oi": oi, "vol": vol, "ohlc": (o, h, l, c)})

    assert "fullFeed.marketFF" in seen
    assert "fullFeed.indexFF" in seen
    assert any(r["seg"] == "EQ" and r["oi"] is None for r in rows)
    assert any(r["seg"] == "FUT" and r["oi"] is not None for r in rows)
    assert any(r["seg"] == "INDEX" for r in rows)


def test_a_unknown_feed_alert_for_subscribed_key(monkeypatch):
    from backend.services.open_low_high import feed_contract as fc
    import backend.services.open_low_high.alerts as al

    fc.reset_feed_contract_for_tests()
    alerts = []
    monkeypatch.setattr(al, "send_olh_alert", lambda *a, **k: alerts.append(a[0]) or True)
    ft = fc.note_feed_observation(
        "NSE_EQ|X",
        {"mysteryFeed": {}},
        subscribed_keys={"NSE_EQ|X"},
        now=IST.localize(datetime(2026, 11, 23, 9, 20, 0)),
    )
    assert not fc.is_handled(ft)
    snap = fc.unknown_counts_snapshot("09:20")
    assert snap["unknown"]
    assert "NSE_EQ|X" in snap["unknown_subscribed_keys"]
    assert alerts


def test_b_raw_recording_size_estimate_and_window(tmp_path, monkeypatch):
    import backend.services.open_low_high.raw_recording as rr

    monkeypatch.setattr(rr, "OLH_RAW_RECORD_ENABLED", True)
    monkeypatch.setattr(rr, "OLH_RAW_RECORD_DIR", str(tmp_path))
    monkeypatch.setattr(rr, "recording_root", lambda: Path(tmp_path))

    est = rr.estimate_daily_bytes()
    assert est["volume_mb_est"] > 0
    assert est["retention_days"] == 14

    now = IST.localize(datetime(2026, 11, 23, 9, 10, 0))
    assert rr.in_record_window(now)
    assert not rr.in_record_window(IST.localize(datetime(2026, 11, 23, 10, 0, 0)))

    ok = rr.record_ws_frame({"feeds": {"NSE_EQ|1": {"ltpc": {"ltp": 1}}}}, now=now)
    assert ok
    ok2 = rr.record_rest_response(
        url="https://api.upstox.com/v3/historical-candle/intraday/x",
        status_code=200,
        headers={"content-type": "application/json"},
        body={"data": {"candles": []}},
        label="canary",
        now=now,
    )
    assert ok2
    day_dir = Path(tmp_path) / "2026-11-23"
    assert (day_dir / "ws_frames.jsonl.gz").exists()
    assert (day_dir / "rest_responses.jsonl").exists()


def test_c_dress_rehearsal_synthetic():
    from backend.services.open_low_high.rehearsal import run_dress_rehearsal

    report = run_dress_rehearsal(
        source_session=date(2026, 11, 23),
        mode="synthetic",
        run_faults=True,
    )
    assert report["session_source"] == "2026-11-23"
    assert report["persisted_1m_count"] >= 3
    assert "fullFeed.marketFF" in report["handled_feed_types"]
    assert "fullFeed.indexFF" in report["handled_feed_types"]
    assert report["raw_frames_available"] is False
    assert len(report["fault_table"]) >= 5
    # Required invariants present in fault table
    names = {r["scenario"] for r in report["fault_table"]}
    assert "eq_silent_stall_0920_0923" in names
    assert "partial_freeze_repairable" in names
    assert "holiday_20261124_skip" in names


def test_d_fault_injection_table_no_hang():
    from backend.services.open_low_high.fault_injection import run_all_fault_scenarios

    out = run_all_fault_scenarios()
    assert out["total"] >= 6
    for row in out["table"]:
        assert "expected" in row and "actual" in row and "pass" in row
        # no scenario should hang (>8s bounded)
        assert float(row.get("elapsed_sec") or 0) < 8.0
    # Core scenarios must pass
    by = {r["scenario"]: r for r in out["table"]}
    assert by["ws_disconnect_partial"]["pass"]
    assert by["partial_freeze_repairable"]["pass"]
    assert by["unknown_feed_subscribed_alert"]["pass"]
    assert by["eq_silent_stall_0920_0923"]["pass"]
    assert by["holiday_20261124_skip"]["pass"]


def test_e_readiness_holiday_skip():
    from backend.services.open_low_high.readiness import run_readiness_0850
    import backend.services.market_holiday as mh

    orig = mh.should_skip_scheduled_market_jobs_ist
    mh.should_skip_scheduled_market_jobs_ist = lambda now=None: True
    try:
        r = run_readiness_0850(
            now=IST.localize(datetime(2026, 11, 24, 8, 50, 0)),
            send_alert=False,
        )
    finally:
        mh.should_skip_scheduled_market_jobs_ist = orig
    assert r.get("skipped") == "holiday_or_weekend"
    assert r.get("red") is False


def test_e_watchdog_slots():
    from backend.services.open_low_high.watchdog import WATCHDOG_TIMES, nearest_watchdog_slot

    assert time(9, 21) in WATCHDOG_TIMES
    assert time(9, 26) in WATCHDOG_TIMES
    assert time(9, 30) in WATCHDOG_TIMES
    slot = nearest_watchdog_slot(IST.localize(datetime(2026, 11, 25, 9, 27, 0)))
    assert slot == time(9, 26)


def test_f_daily_reconcile_holiday_skip(monkeypatch):
    from backend.services.open_low_high import daily_reconcile as dr
    import backend.services.market_holiday as mh

    monkeypatch.setattr(mh, "should_skip_scheduled_market_jobs_ist", lambda now=None: True)
    r = dr.run_daily_reconcile(session=date(2026, 11, 24), send_alert=False)
    assert r.get("skipped") == "holiday_or_weekend"


def test_g_holiday_20261124_and_jobs_for_25():
    from backend.services.open_low_high.rest_canary import canary_should_run_today
    from backend.services.upstox_service import NSE_KNOWN_HOLIDAYS

    assert "2026-11-24" in NSE_KNOWN_HOLIDAYS[2026]

    hol = IST.localize(datetime(2026, 11, 24, 9, 20, 5))
    nxt = IST.localize(datetime(2026, 11, 25, 9, 20, 5))
    assert canary_should_run_today(hol) is False
    assert canary_should_run_today(nxt) is True

    # Scheduler still lists Mon–Fri cron (gate skips on holiday)
    from backend.services.open_low_high import scheduler as sch
    import inspect

    src_sch = inspect.getsource(sch.start_open_low_high_scheduler)
    assert "_run_readiness_0850" in src_sch
    assert "_run_daily_reconcile_1545" in src_sch
    assert "_run_eq_silent_stall_pulse" in src_sch


def test_i_eq_silent_stall_timeline_documents_no_raw_frames():
    from backend.services.open_low_high.silent_stall import eq_timeline_20261123

    doc = eq_timeline_20261123()
    assert doc["raw_ws_frames_recorded"] is False
    assert doc["connection_topology"]["mode"] == "full"
    assert doc["watchdog"]["stall_sec"] == 20


def test_i_candle_builder_persist_shape_nullable_oi_vol():
    """Each segment fixture → 1m OHLC with nullable OI/volume (contract)."""
    from backend.services.open_low_high.candle_builder import MinuteCandleBuilder, TickEvent
    from backend.services.open_low_high.feed_contract import load_schema_fixtures, parse_fixture_to_ohlc

    fixtures = load_schema_fixtures()
    now = IST.localize(datetime(2026, 11, 23, 9, 16, 5))
    b = MinuteCandleBuilder(now_fn=lambda: IST.localize(datetime(2026, 11, 23, 9, 16, 30)))
    for fr in fixtures["frames"]:
        if fr.get("expect_unknown"):
            continue
        o, h, l, c, oi, vol = parse_fixture_to_ohlc(fr["feed"])
        if o is None:
            continue
        tick = TickEvent(
            instrument_key=fr["instrument_key"],
            ltp=float(c),
            exchange_ts=now,
            oi=oi,
            volume=vol,
        )
        candle = b.ingest_tick(tick)
        assert candle is not None
        d = candle.to_persist_dict()
        assert d["open"] > 0 and d["high"] >= d["low"]
        # nullable fields must not gate
        assert "oi_open" in d and "volume" in d

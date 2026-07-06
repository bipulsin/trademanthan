"""8-step Kavach composite integration (fixture/mocks, no production DB)."""
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytz

from backend.services.daily_checklist import (
    D_GO,
    SEC_GO,
    SEC_OUT,
    SEC_WATCH,
    _derive_with_timing,
    evaluate,
)
from backend.services.daily_checklist_timing import apply_go_timing, apply_staleness_cap

IST = pytz.timezone("Asia/Kolkata")


def _passing_long_row():
    return {
        "direction": "LONG",
        "news_clean": True,
        "entry_time": "11:30",
        "kavach_score_entry": 87,
        "confidence": "A",
        "trading_state": "BUY",
        "ema_vs_vwap": "Above",
        "supertrend": "Bullish",
        "macd": "Bullish",
        "adx_entry": 34.5,
        "di_alignment": "DI+>DI-",
        "volume": "High",
        "server_eval": True,
    }


def test_chain_stale_rs_capped_to_watch():
    """Step 1-2: stale indicator never yields GO."""
    row = _passing_long_row()
    derived = evaluate(row)
    capped = apply_staleness_cap(derived, stale=True)
    assert capped["section"] != SEC_GO


def test_chain_fresh_go_sets_timestamp():
    """Step 3: fresh GO sets go_enter_first_at."""
    now = IST.localize(datetime(2026, 7, 6, 11, 5, 0))
    row = _passing_long_row()
    derived = evaluate(row)
    derived = apply_staleness_cap(derived, stale=False)
    out = apply_go_timing(derived, {"section": SEC_WATCH}, stale=False, now=now)
    assert out["section"] == SEC_GO
    assert out["go_enter_first_at"] is not None


def test_chain_fast_watch_does_not_promote_go():
    """Step 4: Fast Watch flip alone does not change checklist section."""
    from backend.services.rs_fast_watch import _flip_state

    row = _passing_long_row()
    row["trading_state"] = "WATCH"
    derived = evaluate(row)
    assert derived["section"] != SEC_GO
    assert _flip_state("BUY", "LONG") is True


def test_chain_sticky_then_hard_fail():
    """Steps 5-6: sticky holds, then hard-fail clears."""
    now = IST.localize(datetime(2026, 7, 6, 11, 6, 0))
    prev = {"section": SEC_WATCH}
    go = evaluate(_passing_long_row())
    go = apply_staleness_cap(go, stale=False)
    first = apply_go_timing(go, prev, stale=False, now=now)
    assert first["section"] == SEC_GO

    flicker = dict(go)
    flicker["section"] = SEC_WATCH
    flicker["decision"] = "🟡 WATCH — WAIT"
    flicker["gate_score"] = 7
    sticky = apply_go_timing(flicker, first, stale=False, now=now + timedelta(minutes=1))
    assert sticky["section"] == SEC_GO

    hard = dict(go)
    hard["section"] = SEC_OUT
    hard["state_ok"] = False
    hard["time_ok"] = True
    cleared = apply_go_timing(hard, sticky, stale=False, now=now + timedelta(minutes=2))
    assert cleared.get("go_sticky_until") is None


def test_chain_sticky_expiry():
    """Step 7: after sticky window, live evaluate wins."""
    now = IST.localize(datetime(2026, 7, 6, 11, 20, 0))
    prev = {
        "section": SEC_GO,
        "go_enter_first_at": (now - timedelta(minutes=10)).isoformat(),
        "go_sticky_until": (now - timedelta(minutes=1)).isoformat(),
    }
    watch = evaluate(_passing_long_row())
    watch["trading_state"] = "WATCH"
    watch["section"] = SEC_WATCH
    out = apply_go_timing(watch, prev, stale=False, now=now)
    assert out["section"] == SEC_WATCH


@patch("backend.services.daily_checklist_live.is_indicator_stale", return_value=False)
@patch("backend.services.daily_checklist_live.recompute_locked_symbol")
def test_chain_live_recompute_wired(mock_live, _mock_stale):
    """Step 2: locked symbol refresh uses live recompute when available."""
    mock_live.return_value = {
        "fields": {"kavach_score_entry": 90, "trading_state": "BUY", "confidence": "A"},
        "indicator_as_of": datetime.now(IST),
        "source": "live_recompute",
    }
    db = MagicMock()
    raw = _passing_long_row()
    raw["indicator_as_of"] = datetime.now(IST).isoformat()
    raw["section"] = SEC_WATCH
    with patch("backend.services.daily_checklist._latest_rs_scan_time", return_value=datetime.now(IST)):
        out = _derive_with_timing(db, raw, prev=raw)
    assert out["section"] in (SEC_GO, SEC_WATCH, SEC_OUT)


def test_chain_board_cycle_not_touched_on_refresh():
    """Step 8: refresh path does not invoke conviction board cycle."""
    from backend.services import daily_checklist as svc

    row = SimpleNamespace(
        symbol="DLF",
        ranking_type="BULLISH",
        relative_strength=1.0,
        trade_score=80,
        volume_ratio=1.2,
        volume_label="High",
        vwap_purity_pct=90,
        market_regime="TREND",
        confidence_grade="A",
        kavach_state="BUY",
        ema5=100,
        vwap=99,
        supertrend=1.0,
        macd=1,
        macd_signal=0.5,
        macd_histogram=0.5,
        adx=30,
        scan_time=datetime.now(IST),
        rank_position=1,
        maturity_tag="FRESH",
        consecutive_days_on_list=1,
        range_vs_atr_ratio=1.0,
    )
    mock_db = MagicMock()
    mock_db.execute.return_value.fetchall.side_effect = [
        [row],
        [],
        [],
    ]
    mock_db.execute.return_value.fetchone.return_value = None

    with patch.object(svc, "get_locked_symbols", return_value=["DLF"]), \
         patch.object(svc, "is_snapshot_locked", return_value=True), \
         patch.object(svc, "locked_direction_map", return_value={"DLF": "LONG"}), \
         patch.object(svc, "get_state", return_value={"ok": True}), \
         patch.object(svc, "at_or_after_lock_time", return_value=True), \
         patch.object(svc, "anchor_overlap_at_0925", return_value={}), \
         patch.object(svc, "_sector_badges_for_top5", return_value={}), \
         patch.object(svc, "_live_direction_map", return_value={}), \
         patch.object(svc, "_load_raw", return_value=None), \
         patch.object(svc, "_upsert_stock"), \
         patch.object(svc, "audit_checklist_lock_coverage", return_value=[]), \
         patch.object(svc, "SessionLocal") as mock_sl, \
         patch("backend.services.rs_conviction_board.run_conviction_board_cycle") as mock_board:
        mock_sl.return_value.__enter__ = lambda s: mock_db
        mock_sl.return_value.__exit__ = lambda s, *a: None
        mock_sl.return_value = mock_db
        svc._refresh_checklist_from_rs(full_populate=False)
        mock_board.assert_not_called()

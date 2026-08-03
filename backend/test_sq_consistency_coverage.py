"""SQ consistency-log coverage for SQ-only promotions."""
from backend.services.structural_quality_ready import ensure_sq_consistency_rows


def test_ensure_sq_consistency_rows_appends_sq_only():
    rows = [
        {
            "symbol": "FORTIS",
            "session_date": "2026-08-03",
            "pre_gate_state": "READY",
        }
    ]
    stocks = [
        {
            "symbol": "FORTIS",
            "sq_promoted_this_cycle": False,
            "promoted_via_structural_score": True,
            "trade_state": "READY",
        },
        {
            "symbol": "ASHOKLEY",
            "direction": "LONG",
            "sq_promoted_this_cycle": True,
            "sq_pre_state": "WAIT FOR PULLBACK",
            "trade_state": "READY",
            "trade_entry": 175.28,
            "trade_sl": 174.48,
            "confidence": "D!",
            "in_lock": True,
            "lock_rank": 3,
            "vwap_quality": {"steep_ok": True, "quality_pass": True},
        },
    ]
    n = ensure_sq_consistency_rows(rows, stocks, session_date="2026-08-03")
    assert n == 1
    assert len(rows) == 2
    ash = rows[1]
    assert ash["symbol"] == "ASHOKLEY"
    assert ash["pre_gate_state"] == "WAIT FOR PULLBACK"
    assert ash["rendered_state"] == "READY"
    assert ash["inputs"]["sq_appended_post_promote"] is True
    assert ash["inputs"]["trade_entry"] == 175.28


def test_ensure_sq_consistency_rows_skips_when_already_queued():
    rows = [{"symbol": "ASHOKLEY", "session_date": "2026-08-03"}]
    stocks = [
        {
            "symbol": "ASHOKLEY",
            "sq_promoted_this_cycle": True,
            "trade_state": "READY",
            "sq_pre_state": "WAIT FOR PULLBACK",
        }
    ]
    assert ensure_sq_consistency_rows(rows, stocks, session_date="2026-08-03") == 0
    assert len(rows) == 1

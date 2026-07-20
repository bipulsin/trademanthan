"""Tests for RS scan exclusion audit + rank-depth logging (no DB required)."""
from backend.services.kavach_engine import STATE_BUY, STATE_NEUTRAL, STATE_SELL
from backend.services.relative_strength_scanner import PERSIST_TOP_N, TOP_N, _rank
from backend.services.rs_exclusion_audit import (
    REASON_BEYOND_PERSIST,
    REASON_NEUTRAL,
    exclusion_row,
)


def _row(sym: str, rs: float, state: str = STATE_BUY) -> dict:
    return {
        "symbol": sym,
        "instrument_key": f"NSE_FO|{sym}",
        "kavach_state": state,
        "relative_strength": rs,
        "volume_ratio": 1.2,
        "adx": 28.0,
        "current_price": 100.0,
        "vwap": 99.0,
        "ema10": 98.5,
        "volume_label": "High",
        "vwap_purity_pct": 75.0,
        "market_regime": "TREND",
    }


def test_rank_logs_neutral_and_beyond_persist():
    rows = []
    # 12 bullish so 2 are beyond persist (10)
    for i in range(12):
        rows.append(_row(f"BULL{i}", rs=10.0 - i * 0.1, state=STATE_BUY))
    # 1 neutral
    rows.append(_row("NEUT1", rs=1.0, state=STATE_NEUTRAL))
    # a few bearish
    for i in range(3):
        rows.append(_row(f"BEAR{i}", rs=-1.0 - i * 0.1, state=STATE_SELL))

    bullish, bearish, exclusions = _rank(rows)
    assert len(bullish) == PERSIST_TOP_N
    assert len(bearish) == 3
    assert bullish[0]["rank_position"] == 1
    assert bullish[-1]["rank_position"] == PERSIST_TOP_N

    reasons = {e["exclusion_reason"] for e in exclusions}
    assert REASON_NEUTRAL in reasons
    assert REASON_BEYOND_PERSIST in reasons

    neutrals = [e for e in exclusions if e["exclusion_reason"] == REASON_NEUTRAL]
    assert len(neutrals) == 1
    assert neutrals[0]["symbol"] == "NEUT1"

    beyond = [e for e in exclusions if e["exclusion_reason"] == REASON_BEYOND_PERSIST]
    assert len(beyond) == 2
    assert all(e["ranking_side"] == "BULLISH" for e in beyond)
    assert all(e["would_be_rank"] > PERSIST_TOP_N for e in beyond)
    assert all(e["rank_cutoff"] == PERSIST_TOP_N for e in beyond)
    assert all(e["top_n_cutoff"] == TOP_N for e in beyond)
    # Cutoff RS should be the RS of the last persisted bullish name
    assert beyond[0]["cutoff_rs_persist"] == bullish[-1]["relative_strength"]


def test_exclusion_row_helper():
    e = exclusion_row(
        symbol="unionbank",
        exclusion_reason=REASON_BEYOND_PERSIST,
        would_be_rank=15,
        ranking_side="BULLISH",
    )
    assert e["symbol"] == "UNIONBANK"
    assert e["would_be_rank"] == 15

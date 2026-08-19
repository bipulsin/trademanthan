"""Parse journal paste for trade_log UI (no DB)."""
from backend.services.trade_log_journal import normalize_underlying, parse_journal_text


ITC_NOTE = """
Trade Log — SHORT ITC (266.20 → 266.60) — 14:25:13 to 15:14:47
Symbol: ITC FUT
Setup: Pullback #6 (momentum short)
EntryTime: 2026-08-19 14:25:13
EntryPrice: 266.20
ExitTime: 2026-08-19 15:14:47
ExitPrice: 266.60
Size: 1,725
exit_trigger_type=rule_compliant
Slippage: Market exit; target 266.40 but filled at 266.60 (0.20 pts)
"""


def test_normalize_underlying_strips_fut_and_expiry():
    assert normalize_underlying("ITC FUT 25 AUG 26") == "ITC"
    assert normalize_underlying("ITCQ2026") == "ITC"
    assert normalize_underlying("COFORGE") == "COFORGE"


def test_parse_itc_journal_prompt():
    p = parse_journal_text(ITC_NOTE)
    assert p["direction"] == "SHORT"
    assert "ITC" in p["symbol"]
    assert p["session_date"] == "2026-08-19"
    assert p["entry_time"].startswith("14:25")
    assert p["exit_time"].startswith("15:14")
    assert p["entry_price"] == 266.20
    assert p["exit_price"] == 266.60
    assert p["qty"] == 1725
    assert p["slippage_pts"] == 0.20
    assert p["exit_trigger_type"] == "rule_compliant"
    assert not any(str(w).startswith("missing:") for w in p["parse_warnings"])


def test_parse_long_arrow_line():
    p = parse_journal_text(
        "COFORGE LONG 1807.40 → 1820.00 entry 10:26:29 exit 10:48:16 on 2026-08-19 qty 475"
    )
    assert p["direction"] == "LONG"
    assert p["session_date"] == "2026-08-19"
    assert p["entry_price"] == 1807.40
    assert p["exit_price"] == 1820.00

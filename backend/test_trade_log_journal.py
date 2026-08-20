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


PFC_NOTE = """
trade_date : 2026-08-20
symbol : PFCQ2026
contract_month : Aug 2026
side : SHORT
entry_time : 10:25:00
entry_price : 365.80
entry_candle_type : RED
entry_trigger_type : Rule 15 - EMA5 Pullback
entry_grade : A
pullback_number : 1
exit_time : 11:45:00
exit_price : 364.20
qty : 1300
"""


def test_parse_snake_case_key_value_journal():
    p = parse_journal_text(PFC_NOTE)
    assert p["session_date"] == "2026-08-20"
    assert p["symbol"] == "PFCQ2026"
    assert normalize_underlying(p["symbol"]) == "PFC"
    assert p["direction"] == "SHORT"
    assert p["entry_time"] == "10:25:00"
    assert p["entry_price"] == 365.80
    assert p["exit_time"] == "11:45:00"
    assert p["exit_price"] == 364.20
    assert p["qty"] == 1300
    assert not any(str(w).startswith("missing:") for w in p["parse_warnings"])


def test_parse_open_trade_does_not_require_exit():
    p = parse_journal_text(
        "trade_date : 2026-08-20\nsymbol : ITC\nside : LONG\nentry_time : 10:25:00\nentry_price : 400.5\n"
    )
    assert p["symbol"] == "ITC"
    assert p["entry_time"] == "10:25:00"
    assert p["entry_price"] == 400.5
    assert p["exit_time"] is None
    assert not any(str(w).startswith("missing:") for w in p["parse_warnings"])


AXIS_NOTE = """
Session Date: 20/08/2026
Symbol: AXISBANK
direction: LONG
Entry Price: 1254.30
Entry Time: 10:17:35 IST
Entry Logic: Chased above red bar (R2 pivot resistance)
Exit Price: 1253.70
Exit Time: 10:41:53 IST
Exit Trigger: Discretionary (resistance rejection)
Confidence Grade: C (below minimum-B gate)
"""


def test_parse_dd_mm_yyyy_session_date():
    p = parse_journal_text(AXIS_NOTE)
    assert p["session_date"] == "2026-08-20"
    assert p["symbol"] == "AXISBANK"
    assert p["direction"] == "LONG"
    assert p["entry_time"] == "10:17:35"
    assert p["entry_price"] == 1254.30
    assert p["exit_time"] == "10:41:53"
    assert p["exit_price"] == 1253.70
    assert p["exit_trigger_type"] == "discretionary"
    assert not any(str(w).startswith("missing:") for w in p["parse_warnings"])


def test_date_from_val_formats():
    from backend.services.trade_log_journal import _date_from_val

    assert _date_from_val("20/08/2026") == "2026-08-20"
    assert _date_from_val("2026-08-20") == "2026-08-20"
    assert _date_from_val("20-08-2026") == "2026-08-20"
    assert _date_from_val("08/20/2026") == "2026-08-20"  # month/day when day>12


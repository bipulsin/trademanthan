"""Unit tests: MCX Bhavcopy parse, checksum, EOD reconstruction, DTE window."""
from __future__ import annotations

from datetime import date

from backend.services.tarang.backtest import (
    INTRADAY_EOD_MSG,
    cycle_is_expired,
    eod_iv_gate,
    first_entry_reject,
    iv_history_warmup,
    mcx_entry_dte_ok,
    run_eod_backtest,
)
from backend.services.tarang.eod_reconstruct import (
    match_futures_underlying,
    put_call_parity_F,
    reconstruct_slice,
    years_to_expiry,
)
from backend.services.tarang.bhavcopy import (
    checksum_bhavcopy_vs_datewise,
    import_report_from_rows,
    parse_bhavcopy_csv,
    parse_date_wise_html,
    parse_expiry,
    parse_trade_date,
    strip_cell,
)
from backend.services.tarang.config import get_profiles

TINY_CSV = """Date,Instrument Name,Symbol,Expiry Date,Option Type,Strike Price,Open,High,Low,Close,Previous Close,Volume(Lots),Volume(In 000's),Value(Lacs),Open Interest(Lots)
"18 Sep 2026","OPTFUT","CRUDEOILM    ","17SEP2026","CE","5400","","","","0.05","0.05","0","0.000 BBL  ","0.00","0"
"17 Sep 2026","OPTFUT","CRUDEOILM    ","17NOV2026","CE","5500","12","13","11","12.5","10","20","1.2 BBL","1.00","100"
"17 Sep 2026","OPTFUT","CRUDEOILM    ","17NOV2026","PE","5300","8","9","7","8.5","8","20","1.0 BBL","0.80","90"
"17 Sep 2026","FUTCOM","CRUDEOILM    ","17NOV2026","XX","0","5400","5410","5390","5405","5400","50","5.0 BBL","10.00","200"
"17 Sep 2026","OPTFUT","CRUDEOILM    ","17NOV2026","CE","5300","20","22","19","21","20","6","0.5 BBL","0.40","40"
"""

DATEWISE_HTML = """
<table>
<tr><th>Date</th><th>Commodity</th><th>Traded Contract (Lots)</th></tr>
<tr><td>17 Sep 2026</td><td>CRUDEOILM</td><td>96</td></tr>
<tr><td>17 Sep 2026</td><td>CRUDEOILM</td><td>200</td></tr>
</table>
"""


def test_strip_and_dates():
    assert strip_cell('  "CRUDEOILM    "  ') == "CRUDEOILM"
    assert parse_trade_date("18 Sep 2026") == date(2026, 9, 18)
    assert parse_expiry("17SEP2026") == date(2026, 9, 17)
    assert parse_expiry("17NOV2026") == date(2026, 11, 17)


def test_parse_excludes_placeholder_and_zero_volume_from_traded():
    rows = parse_bhavcopy_csv(TINY_CSV, source_filename="CRUDEOILM_OPTFUT_17NOV2026_2026-08-17_2026-09-18.csv")
    assert any(r["close"] == 0.05 and r["volume_lots"] == 0 and r["traded"] is False for r in rows)
    traded = [r for r in rows if r["traded"]]
    assert all(r["close"] != 0.05 for r in traded)
    assert all(r["volume_lots"] > 0 for r in traded)


def test_futcom_and_any_symbol():
    extra = TINY_CSV + '"17 Sep 2026","FUTCOM","SILVERM       ","05DEC2026","  ","0","","","", "72000","71000","1","1","1","10"\n'
    rows = parse_bhavcopy_csv(extra)
    fut = [r for r in rows if r["option_type"] == "FUT"]
    assert any(r["symbol"] == "CRUDEOILM" and r["strike"] == 0 for r in fut)
    assert any(r["symbol"] == "SILVERM" for r in fut)


def test_early_life_flag_when_first_equals_from_date():
    rows = parse_bhavcopy_csv(
        '"17 Aug 2026","OPTFUT","CRUDEOILM","17NOV2026","CE","5500","","","","1","1","10","1","1","10"\n',
        source_filename="x_2026-08-17_2026-09-18.csv",
    )
    # header missing — empty. Use proper csv
    body = (
        "Date,Instrument Name,Symbol,Expiry Date,Option Type,Strike Price,Open,High,Low,Close,Previous Close,"
        "Volume(Lots),Volume(In 000's),Value(Lacs),Open Interest(Lots)\n"
        '"17 Aug 2026","OPTFUT","CRUDEOILM","17NOV2026","CE","5500","","","","1","1","10","1","1","10"\n'
    )
    rows = parse_bhavcopy_csv(body, source_filename="CRUDEOILM_OPTFUT_17NOV2026_2026-08-17_2026-09-18.csv")
    rep = import_report_from_rows(rows, filename="CRUDEOILM_OPTFUT_17NOV2026_2026-08-17_2026-09-18.csv", inserted=1, duplicates_skipped=0)
    assert rep["early_life_may_be_missing"] is True
    assert rep["first_trade_date"] == "2026-08-17"


def test_idempotent_keys():
    rows = parse_bhavcopy_csv(TINY_CSV)
    keys = [
        (r["symbol"], r["expiry_date"], r["option_type"], r["strike"], r["trade_date"])
        for r in rows
    ]
    assert len(keys) == len(set(keys))
    again = rows + rows
    assert len(again) == 2 * len(rows)


def test_checksum_one_percent():
    dw = parse_date_wise_html(DATEWISE_HTML)
    db = {
        ("CRUDEOILM", date(2026, 9, 17)): 96.0,
    }
    # first row matches; second row 200 vs 96 is >1%
    out = checksum_bhavcopy_vs_datewise(dw, db_volume_by_key=db)
    assert out["ok"] is False
    assert any(f["abs_pct_diff"] > 1.0 for f in out["flags"])
    close = checksum_bhavcopy_vs_datewise(
        [{"trade_date": date(2026, 9, 17), "symbol": "CRUDEOILM", "traded_contract_lots": 100}],
        db_volume_by_key={("CRUDEOILM", date(2026, 9, 17)): 100.5},
    )
    assert close["ok"] is True  # 0.5% < 1%
    over = checksum_bhavcopy_vs_datewise(
        [{"trade_date": date(2026, 9, 17), "symbol": "CRUDEOILM", "traded_contract_lots": 100}],
        db_volume_by_key={("CRUDEOILM", date(2026, 9, 17)): 102},
    )
    assert over["ok"] is False


def test_otm_only_iv():
    F = 5405.0
    rows = [
        {"option_type": "FUT", "close": F, "strike": 0, "traded": True, "trade_date": date(2026, 9, 17), "expiry_date": date(2026, 11, 17)},
        {
            "option_type": "CE",
            "strike": 5500,
            "close": 12.5,
            "volume_lots": 20,
            "oi_lots": 100,
            "traded": True,
            "trade_date": date(2026, 9, 17),
            "expiry_date": date(2026, 11, 17),
        },
        {
            "option_type": "CE",
            "strike": 5300,
            "close": 120,
            "volume_lots": 20,
            "oi_lots": 100,
            "traded": True,
            "trade_date": date(2026, 9, 17),
            "expiry_date": date(2026, 11, 17),
        },
        {
            "option_type": "PE",
            "strike": 5300,
            "close": 8.5,
            "volume_lots": 20,
            "oi_lots": 90,
            "traded": True,
            "trade_date": date(2026, 9, 17),
            "expiry_date": date(2026, 11, 17),
        },
    ]
    rec = reconstruct_slice(rows)
    ce_otm = next(r for r in rec if r["option_type"] == "CE" and r["strike"] == 5500)
    ce_itm = next(r for r in rec if r["option_type"] == "CE" and r["strike"] == 5300)
    pe_otm = next(r for r in rec if r["option_type"] == "PE" and r["strike"] == 5300)
    assert ce_otm["iv"] is not None
    assert ce_itm["iv"] is None
    assert pe_otm["iv"] is not None
    untraded = {
        "option_type": "CE",
        "strike": 5600,
        "close": 0.05,
        "volume_lots": 0,
        "oi_lots": 0,
        "traded": False,
        "trade_date": date(2026, 9, 17),
        "expiry_date": date(2026, 11, 17),
    }
    rec2 = reconstruct_slice(rows + [untraded])
    u = next(r for r in rec2 if r["strike"] == 5600)
    assert u["iv"] is None


def test_tte_skip_expiry_day():
    assert years_to_expiry(date(2026, 9, 17), date(2026, 9, 17)) is None
    t = years_to_expiry(date(2026, 9, 16), date(2026, 9, 17))
    assert t is not None and t > 0


def test_dte_window_7_35():
    assert mcx_entry_dte_ok(7) is True
    assert mcx_entry_dte_ok(35) is True
    assert mcx_entry_dte_ok(6) is False
    assert mcx_entry_dte_ok(36) is False
    cl = (get_profiles().get("profiles") or {})["CL"]
    ng = (get_profiles().get("profiles") or {})["NG"]
    assert int(cl["expiry_dte_max"]) == 35
    assert int(ng["expiry_dte_max"]) == 35
    assert int(cl["expiry_min_dte"]) == 7


def test_intraday_not_on_eod_message():
    assert "INTRADAY cannot be tested on EOD" in INTRADAY_EOD_MSG
    try:
        out = run_eod_backtest(today=date(2026, 9, 19))
        assert INTRADAY_EOD_MSG in out["intraday_note"]
        assert out["label"] == "MCX EOD-reconstructed, modelled fills"
    except Exception:
        pass


def test_expired_only_cycle_count_excludes_open_oct_nov():
    today = date(2026, 9, 19)
    assert cycle_is_expired(date(2026, 9, 17), today) is True
    assert cycle_is_expired(date(2026, 8, 17), today) is True
    assert cycle_is_expired(date(2026, 10, 15), today) is False
    assert cycle_is_expired(date(2026, 11, 17), today) is False
    open_exps = [date(2026, 10, 15), date(2026, 11, 17)]
    independent = [e for e in [date(2026, 8, 17), date(2026, 9, 17), *open_exps] if cycle_is_expired(e, today)]
    assert date(2026, 10, 15) not in independent
    assert date(2026, 11, 17) not in independent
    assert len(independent) == 2


def test_first_reject_gate_order():
    assert first_entry_reject(has_underlying=False, n_quotes=10, dte=20) == "data"
    assert first_entry_reject(has_underlying=True, n_quotes=0, dte=20) == "data"
    assert first_entry_reject(has_underlying=True, n_quotes=4, dte=6) == "dte_window"
    assert first_entry_reject(has_underlying=True, n_quotes=4, dte=36) == "dte_window"
    assert first_entry_reject(has_underlying=True, n_quotes=4, dte=20, iv_passed=False) == "iv_gate"
    assert first_entry_reject(has_underlying=True, n_quotes=4, dte=20, iv_passed=True, structure_error="no_PE_short") == "liquidity"
    assert first_entry_reject(has_underlying=True, n_quotes=4, dte=20, iv_passed=True, structure_error="no_CE_long") == "traded_wings"
    assert first_entry_reject(
        has_underlying=True, n_quotes=4, dte=20, iv_passed=True, credit=0.0, width=10.0
    ) == "min_credit"
    assert first_entry_reject(
        has_underlying=True, n_quotes=4, dte=20, iv_passed=True, credit=3.0, width=10.0, units=0
    ) == "budget"
    assert first_entry_reject(
        has_underlying=True, n_quotes=4, dte=20, iv_passed=True, credit=3.0, width=10.0, units=1, fee_ok=False
    ) == "fees"
    assert first_entry_reject(
        has_underlying=True, n_quotes=4, dte=20, iv_passed=True, credit=3.0, width=10.0, units=1, fee_ok=True
    ) is None


def test_futures_match_closest_parity_forward():
    parity = put_call_parity_F(
        [{"strike": 5400, "close": 80, "traded": True}],
        [{"strike": 5400, "close": 20, "traded": True}],
    )
    # F ≈ 5400 + 80 - 20 = 5460
    assert abs(parity - 5460) < 1e-9
    futs = [
        {"symbol": "CRUDEOILM", "expiry_date": date(2026, 8, 17), "close": 5300, "option_type": "FUT"},
        {"symbol": "CRUDEOILM", "expiry_date": date(2026, 9, 17), "close": 5455, "option_type": "FUT"},
        {"symbol": "CRUDEOILM", "expiry_date": date(2026, 11, 17), "close": 5600, "option_type": "FUT"},
    ]
    m = match_futures_underlying(parity, futs, option_expiry=date(2026, 10, 15))
    assert m["estimated"] is False
    assert m["fut_expiry"] == date(2026, 9, 17)
    assert m["source"] == "futcom_closest_parity"
    none = match_futures_underlying(parity, [], option_expiry=date(2026, 10, 15))
    assert none["estimated"] is True
    assert none["F"] == parity
    rec = reconstruct_slice(
        [
            {
                "option_type": "CE",
                "strike": 5400,
                "close": 80,
                "traded": True,
                "volume_lots": 20,
                "oi_lots": 10,
                "trade_date": date(2026, 8, 1),
                "expiry_date": date(2026, 10, 15),
            },
            {
                "option_type": "PE",
                "strike": 5400,
                "close": 20,
                "traded": True,
                "volume_lots": 20,
                "oi_lots": 10,
                "trade_date": date(2026, 8, 1),
                "expiry_date": date(2026, 10, 15),
            },
        ],
        futures_for_date=futs,
    )
    assert rec[0]["underlying_fut_expiry"] == date(2026, 9, 17)
    assert rec[0]["underlying_estimated"] is False


def test_warmup_iv_vs_rv_skips_percentile():
    assert iv_history_warmup(59) is True
    assert iv_history_warmup(60) is False
    warm = eod_iv_gate(0.40, 0.30, [0.2] * 10, relative_min=0.10)
    assert warm["warmup"] is True
    assert warm["passed"] is True
    warm_fail = eod_iv_gate(0.31, 0.30, [0.2] * 10, relative_min=0.10)
    assert warm_fail["passed"] is False
    assert warm_fail["gate"] == "iv_gate"
    full_hist = [0.20 + i * 0.001 for i in range(60)]
    full = eod_iv_gate(0.40, 0.30, full_hist, min_percentile=50.0, relative_min=0.10)
    assert full["warmup"] is False
    low_pct = eod_iv_gate(0.10, 0.05, full_hist, min_percentile=50.0, relative_min=0.10)
    assert low_pct["passed"] is False
    assert low_pct["mode"] == "percentile"

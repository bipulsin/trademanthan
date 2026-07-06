"""Unit tests for BTST CSV-fed backtest."""
from datetime import date

from backend.services.btst_backtest.csv_import import parse_btst_csv
from backend.services.btst_backtest.gates import (
    check_hull_gate,
    check_liquidity_gate,
    check_supertrend_gate,
)
from backend.services.btst_backtest.indicators import compute_cpr
from backend.services.btst_backtest.row_processor import (
    compute_change_pct,
    compute_pnl,
    direction_from_change_pct,
    is_within_premium_history_window,
)


def test_cpr_formula():
    pivot, tc, bc = compute_cpr(100, 80, 90)
    assert pivot == (100 + 80 + 90) / 3
    assert bc == 90
    assert tc == pivot + (pivot - bc)


def test_parse_btst_csv_minimal():
    csv = "trade_date,stock_symbol,sector\n2026-06-02,RELIANCE,Banks\n"
    rows, warnings = parse_btst_csv(csv)
    assert len(rows) == 1
    assert rows[0]["trade_date"] == date(2026, 6, 2)
    assert rows[0]["stock_symbol"] == "RELIANCE"
    assert rows[0]["sector"] == "Banks"
    assert not warnings


def test_parse_btst_csv_date_time_header():
    csv = "Date Time,Name,sector\n02-06-2026,TCS,IT\n"
    rows, _ = parse_btst_csv(csv)
    assert rows[0]["stock_symbol"] == "TCS"


def test_change_pct_and_direction():
    assert abs(compute_change_pct(100, 103) - 3.0) < 0.001
    assert direction_from_change_pct(2.5) == "CE"
    assert direction_from_change_pct(-1.0) == "PE"


def test_compute_pnl():
    p = compute_pnl(10.0, 12.0, 9.0, 500)
    assert p["buy_cost"] == 5000.0
    assert p["exit_a_pnl"] == 1000.0
    assert p["exit_b_pnl"] == -500.0


def test_premium_history_window():
    recent = date(2026, 7, 1)
    assert is_within_premium_history_window(recent, window_days=24) is True


def _m5_bar(trade_date: date, hh: int, mm: int, close: float, vol: float = 1000) -> dict:
    return {
        "timestamp": f"{trade_date.isoformat()}T{hh:02d}:{mm:02d}:00+05:30",
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": vol,
    }


def test_liquidity_gate():
    assert check_liquidity_gate(600_000, 500_000) is True
    assert check_liquidity_gate(400_000, 500_000) is False


def test_supertrend_and_hull_smoke():
    d = date(2026, 7, 1)
    candles = []
    p = 50.0
    for i in range(40):
        mm = (15 + i * 5) % 60
        hh = 9 + (15 + i * 5) // 60
        p += 0.2
        candles.append(_m5_bar(d, hh, mm, p))
    st_pass, _ = check_supertrend_gate(candles, d, "15:15", period=10, multiplier=3.0)
    hull_pass, _, rising = check_hull_gate(candles, d, "15:15", length=32)
    assert isinstance(st_pass, bool)
    assert isinstance(hull_pass, bool)
    assert isinstance(rising, bool)

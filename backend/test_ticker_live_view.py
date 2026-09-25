"""Ticker snapshot: open trades only, no desk prefixes, signals inside IST windows."""
from datetime import datetime
from pathlib import Path

import backend.services.ticker_live as tl

IST = tl.IST
ROOT = Path(__file__).resolve().parents[1]


class _DB:
    def close(self):
        pass


def _settings(db, uid):
    return {"enabled": True, "algos": {k: True for k in tl.ALGOS}}


def _clear():
    tl._CACHE.clear()
    tl._INFLIGHT.clear()


def test_refresh_interval_is_120_seconds():
    assert tl.TICKER_REFRESH_SEC == 120.0
    src = (ROOT / "macos/TradeWithCTOTicker/main.swift").read_text(encoding="utf-8")
    assert "let pollSeconds: TimeInterval = 120" in src
    assert "algo_label" not in src
    assert "withTimeInterval: 1.0" not in src


def test_commdiv_in_trade_keeps_pnl_and_drops_blanks(monkeypatch):
    monkeypatch.setattr(
        "backend.services.commodities_div.actions.list_in_trade_signals",
        lambda: [
            {"display_symbol": "COPPER FUT 30 OCT 26", "pnl": -5125, "status": "In-Trade"},
            {"display_symbol": "GOLD", "pnl": None, "status": "In-Trade"},
            {"display_symbol": "—", "pnl": 10, "status": "In-Trade"},
            {"display_symbol": "", "pnl": 20, "status": "In-Trade"},
        ],
    )
    rows = tl._commdiv_trades()
    assert [r["symbol"] for r in rows] == ["COPPER FUT 30 OCT 26"]
    assert rows[0]["pnl"] == -5125.0
    assert rows[0]["label"] == "In-Trade"


def test_stock_options_only_open_live_with_rupee_pnl(monkeypatch):
    monkeypatch.setattr(
        "backend.services.stock_option_signals.list_workspace",
        lambda: {
            "radar": [{"symbol": "TCS"}],
            "active": [{"symbol": "RELIANCE"}, {"symbol": "—"}, {"symbol": ""}],
            "executed": [
                {"symbol": "BHARTIARTL", "trade_mode": "PAPER", "combined_pnl_inr": None},
                {"symbol": "SAGILITY", "trade_mode": "PAPER", "combined_pnl_inr": 40},
                {"symbol": "VEDL", "trade_mode": "LIVE", "combined_pnl_inr": None, "combined_pnl": 1.5},
                {"symbol": "IDFCFIRSTB", "trade_mode": "LIVE", "combined_pnl_inr": 93},
                {"symbol": "—", "trade_mode": "LIVE", "combined_pnl_inr": 10},
            ],
            "completed": [{"symbol": "INFY", "trade_mode": "LIVE", "combined_pnl_inr": 50}],
        },
    )
    trades, signals = tl._stock_options()
    assert [t["symbol"] for t in trades] == ["IDFCFIRSTB"]
    assert trades[0]["pnl"] == 93.0
    assert [s["symbol"] for s in signals] == ["RELIANCE"]


def test_signal_windows_ist_and_closed_days(monkeypatch):
    monkeypatch.setattr(tl, "_nse_closed_day", lambda now: False)
    monkeypatch.setattr(tl, "_mcx_closed_day", lambda now: False)

    def at(h, m):
        return IST.localize(datetime(2026, 9, 25, h, m))

    assert tl.signal_visible("stock_options", at(9, 14)) is False
    assert tl.signal_visible("premium_futures", at(9, 14)) is False
    assert tl.signal_visible("commdiv", at(8, 59)) is False
    assert tl.signal_visible("commdiv", at(9, 0)) is True
    assert tl.signal_visible("stock_options", at(9, 15)) is True
    assert tl.signal_visible("premium_futures", at(15, 30)) is True
    assert tl.signal_visible("stock_options", at(15, 31)) is False
    assert tl.signal_visible("kavach", at(15, 31)) is False
    assert tl.signal_visible("commdiv", at(18, 0)) is True
    assert tl.signal_visible("commdiv", at(23, 30)) is True
    assert tl.signal_visible("commdiv", at(23, 31)) is False

    monkeypatch.setattr(tl, "_nse_closed_day", lambda now: True)
    monkeypatch.setattr(tl, "_mcx_closed_day", lambda now: True)
    assert tl.signal_visible("stock_options", at(10, 0)) is False
    assert tl.signal_visible("premium_futures", at(10, 0)) is False
    assert tl.signal_visible("commdiv", at(10, 0)) is False


def test_snapshot_open_trades_all_day_signals_follow_window(monkeypatch):
    _clear()
    monkeypatch.setattr(tl, "get_settings", _settings)
    monkeypatch.setattr(tl, "_nse_closed_day", lambda now: False)
    monkeypatch.setattr(tl, "_mcx_closed_day", lambda now: False)
    monkeypatch.setattr(
        tl,
        "_commdiv_trades",
        lambda: [tl._row("commdiv", "COPPER FUT 30 OCT 26", pnl=-5125, label="In-Trade")],
    )
    monkeypatch.setattr(
        tl,
        "_commdiv_signals",
        lambda: [tl._row("commdiv", "CRUDEOIL", label="Activated")],
    )
    monkeypatch.setattr(
        tl,
        "_stock_options",
        lambda: (
            [
                tl._row("stock_options", "BHARTIARTL", pnl=None, label="Executed"),
                tl._row("stock_options", "IDFCFIRSTB", pnl=93, label="LIVE"),
            ],
            [tl._row("stock_options", "RELIANCE", label="Active")],
        ),
    )
    monkeypatch.setattr(tl, "_kavach_trades", lambda: [tl._row("kavach", "TCS", pnl=None, label="OPEN")])
    monkeypatch.setattr(tl, "_kavach_ready", lambda: [])
    monkeypatch.setattr(tl, "_breakfast_signals", lambda: [])
    monkeypatch.setattr(
        tl,
        "_premium",
        lambda uid: (
            [
                tl._row("premium_futures", "NIFTY FUT", pnl=1200, label="LONG"),
                tl._row("premium_futures", "BLANK FUT", pnl=None, label="LONG"),
            ],
            [tl._row("premium_futures", "BANKNIFTY FUT", label="Pick")],
        ),
    )
    monkeypatch.setattr(tl, "_tarang_trades", lambda: [])

    monkeypatch.setattr(tl, "_now", lambda: IST.localize(datetime(2026, 9, 25, 10, 0)))
    snap = tl.build_snapshot(_DB(), 4)
    symbols = [r["symbol"] for r in snap["trades"]]
    assert symbols == ["COPPER FUT 30 OCT 26", "IDFCFIRSTB", "NIFTY FUT"]
    assert "BHARTIARTL" not in symbols
    assert "BLANK FUT" not in symbols
    assert "TCS" not in symbols
    blob = " ".join(r["symbol"] + r["algo_label"] for r in snap["trades"] + snap["signals"])
    assert "CommDiv" not in blob
    assert "Stock Options" not in blob
    assert "Premium Futures" not in blob
    assert all(r["algo_label"] == "" for r in snap["trades"] + snap["signals"])
    sigs = {r["symbol"] for r in snap["signals"]}
    assert sigs == {"CRUDEOIL", "RELIANCE", "BANKNIFTY FUT"}

    # Cached desk reads must still be dropped once the equity window is over.
    monkeypatch.setattr(tl, "_now", lambda: IST.localize(datetime(2026, 9, 25, 18, 0)))
    evening = tl.build_snapshot(_DB(), 4)
    assert [r["symbol"] for r in evening["trades"]] == symbols
    assert {r["symbol"] for r in evening["signals"]} == {"CRUDEOIL"}

    monkeypatch.setattr(tl, "_now", lambda: IST.localize(datetime(2026, 9, 25, 23, 31)))
    late = tl.build_snapshot(_DB(), 4)
    assert [r["symbol"] for r in late["trades"]] == symbols
    assert late["signals"] == []

    monkeypatch.setattr(tl, "_now", lambda: IST.localize(datetime(2026, 9, 25, 11, 0)))
    monkeypatch.setattr(tl, "_nse_closed_day", lambda now: True)
    monkeypatch.setattr(tl, "_mcx_closed_day", lambda now: True)
    holiday = tl.build_snapshot(_DB(), 4)
    assert [r["symbol"] for r in holiday["trades"]] == symbols
    assert holiday["signals"] == []

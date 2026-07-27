"""Unit tests for FO UI display-symbol mapping."""
from backend.services.fo_display_symbol import attach_future_symbols, ui_display_symbol


def test_ui_display_prefers_future():
    assert ui_display_symbol("RELIANCE", "RELIANCE FUT 25 AUG 26") == "RELIANCE FUT 25 AUG 26"


def test_ui_display_falls_back_when_fo_null():
    assert ui_display_symbol("EXIDEIND", None) == "EXIDEIND"
    assert ui_display_symbol("NUVAMA", "") == "NUVAMA"
    assert ui_display_symbol("NUVAMA", "  ") == "NUVAMA"


def test_attach_future_symbols_preserves_underlying():
    items = [{"symbol": "KPITTECH"}, {"symbol": "MISSINGFO"}]
    attach_future_symbols(
        items,
        fmap={"KPITTECH": "KPITTECH FUT 25 AUG 26"},
    )
    assert items[0]["symbol"] == "KPITTECH"
    assert items[0]["future_symbol"] == "KPITTECH FUT 25 AUG 26"
    assert items[0]["display_symbol"] == "KPITTECH FUT 25 AUG 26"
    assert items[1]["symbol"] == "MISSINGFO"
    assert items[1]["future_symbol"] == ""
    assert items[1]["display_symbol"] == "MISSINGFO"


def test_attach_keeps_existing_future_symbol():
    items = [{"symbol": "X", "future_symbol": "X FUT 30 JUN 26"}]
    attach_future_symbols(items, fmap={"X": "X FUT 25 AUG 26"})
    assert items[0]["future_symbol"] == "X FUT 30 JUN 26"
    assert items[0]["display_symbol"] == "X FUT 30 JUN 26"

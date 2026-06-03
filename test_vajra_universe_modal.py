"""More modal — full arbitrage_master universe rows."""
from backend.services.vajra.ranking import build_universe_modal_rows, grade_sort_rank
from backend.services.vajra.qualification_config import STATE_EXECUTABLE, STATE_REJECT


def test_grade_sort_rank_order():
    assert grade_sort_rank("A+") > grade_sort_rank("A")
    assert grade_sort_rank("A") > grade_sort_rank("B+")
    assert grade_sort_rank("B+") > grade_sort_rank("B")
    assert grade_sort_rank("B") > grade_sort_rank("C")


def test_build_universe_modal_rows_includes_all_universe_and_grades():
    universe = [
        {"stock": "AAA", "future_symbol": "AAAFUT", "instrument_key": "k1"},
        {"stock": "BBB", "future_symbol": "BBBFUT", "instrument_key": "k2"},
        {"stock": "CCC", "future_symbol": "CCCFUT", "instrument_key": "k3"},
    ]
    rated = [
        {
            "stock": "AAA",
            "security": "AAAFUT",
            "qualification_state": STATE_EXECUTABLE,
            "quality_grade": "A+",
            "executable_score": 90,
            "conviction_score": 85,
            "setup_quality_score": 80,
        },
        {
            "stock": "BBB",
            "security": "BBBFUT",
            "qualification_state": STATE_EXECUTABLE,
            "quality_grade": "C",
            "executable_score": 20,
            "conviction_score": 40,
            "setup_quality_score": 35,
        },
    ]
    rows = build_universe_modal_rows(rated, universe)
    stocks = [r.get("stock") for r in rows]
    assert stocks == ["AAA", "BBB", "CCC"]
    by_stock = {r["stock"]: r for r in rows}
    assert by_stock["AAA"]["quality_grade"] == "A+"
    assert by_stock["BBB"]["quality_grade"] == "C"
    assert by_stock["CCC"]["qualification_state"] == STATE_REJECT
    assert by_stock["CCC"]["quality_grade"] == "C"

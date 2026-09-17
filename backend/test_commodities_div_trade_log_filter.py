"""CommDiv PAPER must stay out of trade_log lists / reports (no DB)."""
from __future__ import annotations

import json

from backend.services.rule27_trade_log import (
    SOURCE_COMMODITIES_DIV,
    filter_out_commodities_div_paper,
    is_commodities_div_paper_row,
    notes_trade_mode,
    serialize_trade,
)


def test_notes_trade_mode_parses_json_string_and_dict():
    assert notes_trade_mode('{"trade_mode": "live"}') == "LIVE"
    assert notes_trade_mode({"trade_mode": "PAPER"}) == "PAPER"
    assert notes_trade_mode("not-json") is None
    assert notes_trade_mode(None) is None
    assert notes_trade_mode({"other": 1}) is None


def test_is_commodities_div_paper_row_defaults_missing_mode_to_paper():
    assert is_commodities_div_paper_row(
        {"source": SOURCE_COMMODITIES_DIV, "notes": "{}"}
    )
    assert is_commodities_div_paper_row(
        {"source": "commodities_div", "notes": json.dumps({"trade_mode": "PAPER"})}
    )
    assert not is_commodities_div_paper_row(
        {"source": SOURCE_COMMODITIES_DIV, "notes": json.dumps({"trade_mode": "LIVE"})}
    )
    assert not is_commodities_div_paper_row(
        {"source": "kavach", "notes": json.dumps({"trade_mode": "PAPER"})}
    )
    assert not is_commodities_div_paper_row(None)


def test_filter_out_commodities_div_paper_keeps_live_and_other_sources():
    rows = [
        {"id": 1, "source": "commodities_div", "notes": json.dumps({"trade_mode": "PAPER"})},
        {"id": 2, "source": "commodities_div", "notes": json.dumps({"trade_mode": "LIVE"})},
        {"id": 3, "source": "manual", "notes": None},
        None,
    ]
    filtered = filter_out_commodities_div_paper(rows)
    assert [r["id"] for r in filtered] == [2, 3]


def test_serialize_trade_exposes_commdiv_trade_mode():
    out = serialize_trade(
        {
            "qty": 1,
            "points_captured": 10.0,
            "source": SOURCE_COMMODITIES_DIV,
            "notes": json.dumps({"trade_mode": "LIVE"}),
        }
    )
    assert out["trade_mode"] == "LIVE"
    assert out["gross_pnl_inr"] == 10.0

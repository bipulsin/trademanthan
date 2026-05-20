"""Arbitrage API rows must JSON-serialize Decimal and datetime."""
import json
from datetime import datetime
from decimal import Decimal

from backend.utils.json_safe import json_safe_row


def test_json_safe_row_decimal_and_datetime():
    row = {
        "stock_ltp": Decimal("1234.50"),
        "currmth_future_ltp": Decimal("1220.00"),
        "trade_entry_time": datetime(2026, 5, 20, 10, 30, 0),
        "has_open_order": False,
        "stock": "RELIANCE",
    }
    safe = json_safe_row(row)
    json.dumps(safe)
    assert safe["stock_ltp"] == 1234.5
    assert "2026-05-20" in safe["trade_entry_time"]

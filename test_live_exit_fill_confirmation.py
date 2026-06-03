"""Live exit must not return success until SELL order is filled."""
from unittest.mock import MagicMock, patch

from backend.services import live_trading as lt


def test_wait_for_sell_order_fill_complete():
    row = {
        "status": "complete",
        "transaction_type": "SELL",
        "filled_quantity": 100,
        "average_price": 12.5,
    }
    with patch.object(lt.upstox_service, "get_order_details", return_value={"success": True, "data": {"data": row}}):
        out = lt.wait_for_sell_order_fill("OID1", 100, timeout_sec=2.0, poll_interval=0.01)
    assert out.get("filled") is True
    assert out.get("average_price") == 12.5


def test_place_live_exit_does_not_succeed_on_place_only():
    placed = {"success": True, "order_id": "SELL123"}
    unfilled = {"filled": False, "error": "timeout_waiting_for_fill", "order_id": "SELL123"}
    with patch.object(lt, "is_trading_live_enabled", return_value=True), patch.object(
        lt, "is_scan_trade_live_exit_enabled", return_value=True
    ), patch.object(lt.upstox_service, "place_order", return_value=placed), patch.object(
        lt, "wait_for_sell_order_fill", return_value=unfilled
    ):
        out = lt.place_live_upstox_exit(
            instrument_key="NSE_FO|X",
            qty=100,
            stock_name="PNBHOUSING",
            option_contract="PNBHOUSING 900 PE 30 JUN 26",
            buy_order_id="BUY1",
        )
    assert out.get("success") is False
    assert out.get("exit_manually") is True

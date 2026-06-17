from backend.services import live_trading


def test_duplicate_guard_blocks_when_net_long(monkeypatch):
    monkeypatch.setattr(live_trading, "upstox_service", object())
    monkeypatch.setattr(
        live_trading,
        "get_broker_net_long_qty_for_instrument",
        lambda _ik: 125,
    )
    out = live_trading._broker_duplicate_entry_guard("NSE_FO|12345")
    assert out["block"] is True
    assert "open_broker_position_qty" in out["reason"]


def test_duplicate_guard_blocks_on_recent_active_buy(monkeypatch):
    class _Svc:
        def get_order_book_today(self):
            return {
                "orders": [
                    {
                        "order_id": "OID123",
                        "transaction_type": "BUY",
                        "status": "open",
                        "instrument_token": "NSE_FO|99999",
                        "order_timestamp": "2099-01-01 10:00:00",
                    }
                ]
            }

    monkeypatch.setattr(live_trading, "upstox_service", _Svc())
    monkeypatch.setattr(
        live_trading,
        "get_broker_net_long_qty_for_instrument",
        lambda _ik: 0,
    )
    out = live_trading._broker_duplicate_entry_guard("NSE_FO|99999")
    assert out["block"] is True
    assert "recent_or_active_buy_order" in out["reason"]


def test_duplicate_guard_allows_when_no_open_or_recent(monkeypatch):
    class _Svc:
        def get_order_book_today(self):
            return {"orders": []}

    monkeypatch.setattr(live_trading, "upstox_service", _Svc())
    monkeypatch.setattr(
        live_trading,
        "get_broker_net_long_qty_for_instrument",
        lambda _ik: 0,
    )
    out = live_trading._broker_duplicate_entry_guard("NSE_FO|11111")
    assert out["block"] is False


def test_cancel_open_buy_reports_unresolved_on_cancel_failure(monkeypatch):
    class _Svc:
        def get_order_details(self, _order_id):
            return {
                "success": True,
                "data": {"data": {"status": "open", "filled_quantity": 0}},
            }

        def cancel_order(self, _order_id):
            return {"success": False, "error": "broker_timeout"}

    monkeypatch.setattr(live_trading, "upstox_service", _Svc())
    out = live_trading.cancel_open_buy_if_pending("OID-X")
    assert out["resolved"] is False
    assert out["state"] == "cancel_failed"


def test_limit_buy_at_ltp_forwards_validity(monkeypatch):
    class _Svc:
        def __init__(self):
            self.validity = None

        def get_tick_size_by_instrument_key(self, _instrument_key):
            return 0.05

        def place_order(self, **kwargs):
            self.validity = kwargs.get("validity")
            return {"success": True, "order_id": "OID-IOC"}

    svc = _Svc()
    monkeypatch.setattr(live_trading, "upstox_service", svc)
    monkeypatch.setattr(live_trading, "is_trading_live_enabled", lambda: True)
    out = live_trading.place_live_upstox_limit_buy_at_ltp(
        instrument_key="NSE_FO|123",
        qty=25,
        stock_name="X",
        option_contract="X 100 CE",
        buy_price=10.2,
        validity="IOC",
    )
    assert out["success"] is True
    assert svc.validity == "IOC"


def test_limit_buy_at_price_forwards_validity(monkeypatch):
    class _Svc:
        def __init__(self):
            self.validity = None

        def get_tick_size_by_instrument_key(self, _instrument_key):
            return 0.05

        def place_order(self, **kwargs):
            self.validity = kwargs.get("validity")
            return {"success": True, "order_id": "OID-DAY"}

    svc = _Svc()
    monkeypatch.setattr(live_trading, "upstox_service", svc)
    monkeypatch.setattr(live_trading, "is_trading_live_enabled", lambda: True)
    out = live_trading.place_live_upstox_limit_buy_at_price(
        instrument_key="NSE_FO|123",
        qty=25,
        stock_name="X",
        option_contract="X 100 CE",
        limit_price=10.2,
        validity="DAY",
    )
    assert out["success"] is True
    assert svc.validity == "DAY"


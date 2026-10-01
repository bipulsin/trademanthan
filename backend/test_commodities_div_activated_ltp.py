"""Activated LTP is snapshotted once at GO and ignored by later quote ticks."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from backend.services.commodities_div.ltp_sidecar import refresh_in_trade_ltp
from backend.services.commodities_div.webhook import process_webhook
from backend.services.commodities_div.ws_ltp import _persist_ltp

ROOT = Path(__file__).resolve().parents[1]
RECEIVED = datetime(2026, 9, 16, 10, 0, 0)
INSTRUMENT_KEY = "MCX_FO|123"


class _Rows:
    def __init__(self, rows, scalar=None):
        self._rows = rows
        self._scalar = scalar

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar(self):
        return self._scalar


class SignalStore:
    """Enough of SessionLocal to run DIV → GO and a later LTP tick."""

    def __init__(self):
        self.signals = []
        self._id = 1
        self.go_activated_ltp = "unset"

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        params = dict(params or {})
        if "INSERT INTO commodities_div_webhook_log" in sql:
            rid = self._id
            self._id += 1
            return _Rows([], scalar=rid)
        if "UPDATE commodities_div_webhook_log" in sql:
            return _Rows([])
        if "INSERT INTO commodities_div_signals" in sql:
            rid = self._id
            self._id += 1
            self.signals.append(
                {
                    "id": rid,
                    "status": "Divergence",
                    "activated_ltp": None,
                    "ltp": None,
                    "symbol_raw": params.get("symbol_raw"),
                    "symbol_mapped": params.get("symbol_mapped"),
                    "direction": params.get("direction"),
                    "instrument_key": params.get("instrument_key"),
                }
            )
            return _Rows([], scalar=rid)
        if "SELECT * FROM commodities_div_signals" in sql and "status <> 'History'" in sql:
            rows = [r for r in self.signals if r.get("status") != "History"]
            return _Rows(rows)
        if "SELECT" in sql and "status = 'In-Trade'" in sql:
            rows = [r for r in self.signals if r.get("status") == "In-Trade"]
            return _Rows(rows)
        if "activated_ltp = :activated_ltp" in sql:
            assert "activated_ltp" in params
            self.go_activated_ltp = params.get("activated_ltp")
            for row in self.signals:
                if int(row["id"]) == int(params["id"]):
                    row["status"] = "Activated"
                    row["activated_ltp"] = params.get("activated_ltp")
                    row["instrument_key"] = params.get("ik") or row.get("instrument_key")
            return _Rows([])
        if "ltp = :ltp" in sql:
            assert "activated_ltp" not in sql
            for row in self.signals:
                if int(row["id"]) != int(params["id"]):
                    continue
                if "status = 'In-Trade'" in sql and row.get("status") != "In-Trade":
                    continue
                row["ltp"] = params["ltp"]
            return _Rows([])
        raise AssertionError(f"unexpected SQL: {sql[:400]}")

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        return None


def _body(flag: str) -> bytes:
    return (
        '{"flag":"%s","symbol":"CRUDEOIL1!","time":1757675460000}' % flag
    ).encode()


def _inst(_symbol_raw, resolve_contract=True):
    return {
        "symbol_mapped": "CRUDEOIL",
        "underlying_matched": True,
        "instrument_key": INSTRUMENT_KEY,
        "contract": "CRUDEOIL FUT",
        "lot_size": 100,
    }


def _patches(store: SignalStore, quote_fn):
    return (
        patch("backend.services.commodities_div.webhook.ensure_commodities_div_tables"),
        patch("backend.services.commodities_div.webhook.SessionLocal", return_value=store),
        patch(
            "backend.services.commodities_div.webhook.attach_instrument_fields",
            side_effect=_inst,
        ),
        patch(
            "backend.services.commodities_div.ltp_sidecar.quote_futures_ltp",
            side_effect=quote_fn,
        ),
    )


def _run_cycle(store: SignalStore, quote, *, div_flag: str, go_flag: str):
    calls = {"n": 0, "keys": []}

    def _quote(instrument_key):
        calls["n"] += 1
        calls["keys"].append(instrument_key)
        return quote["price"]

    patches = _patches(store, _quote)
    for p in patches:
        p.start()
    try:
        div = process_webhook(received_at=RECEIVED, source_ip="127.0.0.1", body=_body(div_flag))
        after_div = calls["n"]
        div_activated = store.signals[0]["activated_ltp"] if store.signals else "missing"
        go = process_webhook(received_at=RECEIVED, source_ip="127.0.0.1", body=_body(go_flag))
    finally:
        for p in reversed(patches):
            p.stop()
    return div, go, calls, after_div, div_activated


def test_div_does_not_set_activated_ltp_go_sets_once_tick_does_not_replace():
    store = SignalStore()
    quote = {"price": 4321.5}
    div, go, calls, after_div, div_activated = _run_cycle(
        store, quote, div_flag="BULL-DIV", go_flag="BULL-GO"
    )

    assert div["status"] == "Divergence"
    assert after_div == 0
    assert div_activated is None
    assert calls["n"] == 1
    assert calls["keys"] == [INSTRUMENT_KEY]
    row = store.signals[0]
    assert row["activated_ltp"] == 4321.5
    assert go["status"] == "Activated"
    assert store.go_activated_ltp == 4321.5

    quote["price"] = 7777.0
    with patch(
        "backend.services.commodities_div.webhook.ensure_commodities_div_tables"
    ), patch(
        "backend.services.commodities_div.webhook.SessionLocal", return_value=store
    ), patch(
        "backend.services.commodities_div.webhook.attach_instrument_fields", side_effect=_inst
    ), patch(
        "backend.services.commodities_div.ltp_sidecar.quote_futures_ltp",
        side_effect=lambda instrument_key: quote["price"],
    ):
        again = process_webhook(
            received_at=RECEIVED, source_ip="127.0.0.1", body=_body("BULL-GO")
        )
    assert again["disposition"] == "unmatched"
    assert row["activated_ltp"] == 4321.5

    # Take-trade keeps the stored snapshot. Later live ticks rewrite `ltp` only.
    row["status"] = "In-Trade"
    with patch(
        "backend.services.commodities_div.ws_ltp.SessionLocal", return_value=store
    ):
        _persist_ltp(row["id"], 5555.0, RECEIVED)
    assert row["ltp"] == 5555.0
    assert row["activated_ltp"] == 4321.5

    class _Upstox:
        access_token = "token"

        def get_market_quotes_batch_by_keys(self, keys):
            return {keys[0]: 8888.0}

    with patch(
        "backend.services.commodities_div.ltp_sidecar.ensure_commodities_div_tables"
    ), patch(
        "backend.services.commodities_div.ltp_sidecar.SessionLocal", return_value=store
    ), patch(
        "backend.services.upstox_service.UpstoxService", return_value=_Upstox()
    ):
        refresh_in_trade_ltp()
    assert row["ltp"] == 8888.0
    assert row["activated_ltp"] == 4321.5


def test_missing_quote_at_go_stays_null_when_a_later_tick_arrives():
    store = SignalStore()
    quote = {"price": None}
    div, go, calls, after_div, div_activated = _run_cycle(
        store, quote, div_flag="BEAR-DIV", go_flag="BEAR-GO"
    )
    assert after_div == 0
    assert div_activated is None

    assert div["disposition"] == "applied"
    assert go["status"] == "Activated"
    assert calls["n"] == 1
    row = store.signals[0]
    assert row["activated_ltp"] is None
    assert store.go_activated_ltp is None

    row["status"] = "In-Trade"
    with patch(
        "backend.services.commodities_div.ws_ltp.SessionLocal", return_value=store
    ):
        _persist_ltp(row["id"], 5555.0, RECEIVED)
    assert row["ltp"] == 5555.0
    assert row["activated_ltp"] is None


def test_active_tab_column_sits_before_entry_only():
    js = (ROOT / "frontend" / "public" / "commDiv.js").read_text(encoding="utf-8")
    active = js.split("function renderActive", 1)[1].split("function renderInTrade", 1)[0]
    assert "<th>Activated LTP</th>" in active
    assert active.index("<th>Activated LTP</th>") < active.index("<th>Entry</th>")
    assert active.index("row.activated_ltp") < active.index("row.entry_price")
    outside = js.replace(active, "", 1)
    assert "Activated LTP" not in outside
    assert "activated_ltp" not in outside

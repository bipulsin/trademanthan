"""Ticker app tokens must be minted on the request session, not a second connection."""
import threading

import backend.services.ticker_live as tl


class _Session:
    def __init__(self):
        self.executed = 0
        self.commits = 0
        self.params = []

    def execute(self, _stmt, params=None):
        self.executed += 1
        if params:
            self.params.append(params)
        return self

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    @property
    def rowcount(self):
        return 1


def test_ensure_ticker_tables_uses_caller_session_once(monkeypatch):
    monkeypatch.setattr(
        "backend.database.SessionLocal",
        lambda: (_ for _ in ()).throw(AssertionError("opened a second pooled session")),
    )
    previous = tl._tables_ready
    tl._tables_ready = False
    db = _Session()
    try:
        tl.ensure_ticker_tables(db)
        tl.ensure_ticker_tables(db)
        assert db.executed == 2
        assert db.commits == 1
        assert tl._tables_ready is True
    finally:
        tl._tables_ready = previous


def test_issue_token_returns_app_token(monkeypatch):
    monkeypatch.setattr(tl, "ensure_ticker_tables", lambda db=None: None)
    db = _Session()
    issued = tl.issue_token(db, 42)
    assert issued["token"].startswith(tl.TOKEN_PREFIX)
    assert issued["hint"] == issued["token"][-4:]
    assert db.params[0]["u"] == 42
    assert db.params[0]["h"] == tl._hash_token(issued["token"])
    assert db.params[0]["hint"] == issued["hint"]


class _Mappings:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _Exec:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _Mappings(self._rows)


class _ReadyDB:
    def execute(self, stmt, params=None):
        sql = str(stmt)
        if "kavach_ready_consistency_log" in sql:
            return _Exec([{"symbol": "TCS", "rendered_state": "READY"}])
        return self

    def close(self):
        pass


def test_kavach_ready_reads_log_not_checklist(monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("checklist get_state must not run on the ticker path")

    monkeypatch.setattr("backend.services.daily_checklist.get_state", boom, raising=False)
    monkeypatch.setattr("backend.database.SessionLocal", lambda: _ReadyDB())
    rows = tl._kavach_ready()
    assert rows == [
        {
            "algo": "kavach",
            "algo_label": "Kavach",
            "symbol": "TCS",
            "pnl": None,
            "label": "READY",
        }
    ]


def test_snapshot_keeps_commdiv_when_another_desk_stalls(monkeypatch):
    tl._CACHE.clear()
    tl._INFLIGHT.clear()
    monkeypatch.setattr(tl, "SNAPSHOT_BUDGET_SEC", 0.4)
    monkeypatch.setattr(
        tl,
        "get_settings",
        lambda db, uid: {"enabled": True, "algos": {k: True for k in tl.ALGOS}},
    )
    gate = threading.Event()

    def stall():
        gate.wait(5)
        return []

    monkeypatch.setattr(
        tl,
        "_commdiv_trades",
        lambda: [tl._row("commdiv", "COPPER FUT 30 OCT 26", pnl=-4250, label="In-Trade")],
    )
    monkeypatch.setattr(tl, "_commdiv_signals", lambda: [])
    monkeypatch.setattr(tl, "_stock_options", lambda: ([], []))
    monkeypatch.setattr(tl, "_kavach_trades", lambda: [])
    monkeypatch.setattr(tl, "_kavach_ready", stall)
    monkeypatch.setattr(tl, "_breakfast_signals", lambda: [])
    monkeypatch.setattr(tl, "_premium", lambda uid: ([], []))
    monkeypatch.setattr(tl, "_tarang_trades", lambda: [])
    monkeypatch.setattr(tl, "_multi_leg_trades", lambda: [])

    class _DB:
        def close(self):
            pass

    try:
        snap = tl.build_snapshot(_DB(), 4)
    finally:
        gate.set()
    symbols = [row["symbol"] for row in snap["trades"]]
    assert "COPPER FUT 30 OCT 26" in symbols
    copper = next(row for row in snap["trades"] if row["symbol"].startswith("COPPER"))
    assert copper["pnl"] == -4250.0
    assert copper["label"] == "In-Trade"
    assert snap["ok"] is True

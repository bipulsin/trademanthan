"""Ticker app tokens must be minted on the request session, not a second connection."""
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

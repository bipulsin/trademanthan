"""API smoke tests for Sambhav router (auth overridden)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import sambhav as sambhav_router


class _Admin:
    email = "admin@test.com"
    is_admin = "Yes"


@pytest.fixture()
def client(monkeypatch):
    app = FastAPI()
    app.include_router(sambhav_router.router, prefix="/api/sambhav")

    def _admin():
        return _Admin()

    app.dependency_overrides[sambhav_router._require_user] = _admin
    app.dependency_overrides[sambhav_router._require_admin] = _admin

    monkeypatch.setattr(sambhav_router, "ensure_sambhav_tables", lambda: None)

    mock_db = MagicMock()
    mock_db.execute.return_value.scalar.return_value = 0
    mock_db.execute.return_value.fetchone.return_value = None
    mock_db.execute.return_value.fetchall.return_value = []

    def _get_db():
        yield mock_db

    from backend.database import get_db

    app.dependency_overrides[get_db] = _get_db

    # Patch symbols used via late import inside handlers
    import backend.services.sambhav.importer as importer
    import backend.services.sambhav.train as train

    monkeypatch.setattr(importer, "get_import_state", lambda db, instrument_key=None: {"status": "idle"})
    monkeypatch.setattr(train, "get_active_model_row", lambda db, prefer_status="LIVE": None)

    return TestClient(app)


def test_status_smoke(client):
    r = client.get("/api/sambhav/status")
    assert r.status_code == 200
    body = r.json()
    assert body["module"] == "TWCTO Sambhav"
    assert "disclaimer" in body


def test_tradingview_stub(client):
    r = client.get("/api/sambhav/tradingview-stub")
    assert r.status_code == 200
    assert r.json()["status"] == "DEFERRED"


def test_current_insufficient(client):
    r = client.get("/api/sambhav/current")
    assert r.status_code == 200
    assert r.json()["status"] in ("INSUFFICIENT DATA",)


def test_history_empty(client):
    r = client.get("/api/sambhav/history")
    assert r.status_code == 200
    assert r.json()["n"] == 0

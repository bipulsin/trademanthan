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
    monkeypatch.setattr("backend.services.sambhav.data_status.ensure_sambhav_tables", lambda: None)
    monkeypatch.setattr("backend.services.sambhav.data_status.get_import_state", lambda db, instrument_key=None: {"status": "idle"})
    monkeypatch.setattr("backend.services.sambhav.importer.ensure_sambhav_tables", lambda: None)

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


def test_data_status_not_imported(client):
    r = client.get("/api/sambhav/data-status")
    assert r.status_code == 200
    body = r.json()
    assert body["instrument"] == "NIFTY 50"
    assert body["interval"] == "10m"
    assert body["candle_count"] == 0
    assert body["status"] == "NOT_IMPORTED"
    assert "raw_1m" not in body
    assert body["phase"] == "DATA COLLECTION"


def test_calibration_n_zero_insufficient(client, monkeypatch):
    from backend.routers import sambhav as sambhav_router

    row = type("R", (), {})()
    row.calibration_buckets_json = {"status": "OK", "n": 0, "ece": None, "buckets": []}
    row.metrics_json = {}
    row.model_id = 1
    row.created_at = None

    mock_db = MagicMock()
    mock_db.execute.return_value.fetchone.return_value = row

    def _get_db():
        yield mock_db

    from backend.database import get_db

    sambhav_router.router  # keep import used
    client.app.dependency_overrides[get_db] = _get_db
    r = client.get("/api/sambhav/calibration")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "INSUFFICIENT DATA"
    assert body["n"] == 0
    assert body["ece"] is None

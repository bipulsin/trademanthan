"""Chart indicator selection is stored per user and rejected when malformed."""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from backend.services.chart_indicator_prefs import (
    get_indicator_prefs,
    sanitize_indicator_prefs,
    save_indicator_prefs,
)


def _prefs(**overrides):
    base = {
        "emas": [
            {"enabled": True, "period": 9},
            {"enabled": False, "period": 30},
            {"enabled": False, "period": 100},
        ],
        "vwap": True,
        "volume": True,
        "hm": False,
        "macd": False,
    }
    base.update(overrides)
    return base


def test_sanitize_clamps_periods_and_keeps_selection():
    cleaned = sanitize_indicator_prefs(
        _prefs(
            emas=[
                {"enabled": 1, "period": 1},
                {"enabled": 0, "period": "40"},
                {"enabled": True, "period": 9999},
            ],
            vwap=False,
            hm=True,
            macd=1,
            extra="ignored",
        )
    )
    assert cleaned["emas"] == [
        {"enabled": True, "period": 2},
        {"enabled": False, "period": 40},
        {"enabled": True, "period": 200},
    ]
    assert cleaned["vwap"] is False
    assert cleaned["volume"] is True
    assert cleaned["hm"] is True
    assert cleaned["macd"] is True
    assert "extra" not in cleaned


def test_sanitize_rejects_bad_shape():
    with pytest.raises(ValueError):
        sanitize_indicator_prefs(["ema"])
    with pytest.raises(ValueError):
        sanitize_indicator_prefs({"emas": [{"enabled": True, "period": 9}]})


def test_save_and_reload_per_user():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE chart_indicator_prefs (
                    user_id INTEGER PRIMARY KEY,
                    prefs TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
    db = sessionmaker(bind=engine)()
    try:
        saved = save_indicator_prefs(db, 7, _prefs(macd=True, vwap=False))
        assert get_indicator_prefs(db, 7) == saved
        assert get_indicator_prefs(db, 8) is None
        updated = save_indicator_prefs(db, 7, _prefs(hm=True))
        assert get_indicator_prefs(db, 7)["hm"] is True
        assert get_indicator_prefs(db, 7)["macd"] is False
        assert updated["vwap"] is True
    finally:
        db.close()

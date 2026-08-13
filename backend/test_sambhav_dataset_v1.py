"""Sambhav V1 dataset finalization tests — classification, quality, immutability."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from backend.services.sambhav.candles import validate_ohlc
from backend.services.sambhav.config import (
    DATASET_VERSION_V1,
    EXPECTED_10M_PER_SESSION,
    IST,
    SESSION_TYPE_EXCLUDED_HOLIDAY,
    SESSION_TYPE_EXCLUDED_MUHURAT,
    SESSION_TYPE_EXCLUDED_SPECIAL,
    SESSION_TYPE_REGULAR,
    V1_EXCLUDED_MUHURAT_DATES,
    V1_EXCLUDED_SPECIAL_DATES,
)
from backend.services.sambhav.data_status import calibration_status_payload
from backend.services.sambhav.historical import expected_10m_starts
from backend.services.sambhav.holidays import sambhav_holiday_dates
from backend.services.sambhav.importer import import_historical_1m
from backend.services.sambhav.sessions import classify_session_type, validate_regular_session_bars
from backend.services.sambhav.source_guard import assert_feature_modules_do_not_mutate_source


def test_regular_session_classification():
    st, _ = classify_session_type(
        date(2025, 1, 2),
        candle_count=38,
        grid_ok=True,
        holiday_dates=set(),
    )
    assert st == SESSION_TYPE_REGULAR


def test_holiday_exclusion():
    hol = sambhav_holiday_dates(date(2022, 1, 1), date(2026, 8, 12))
    assert date(2022, 1, 26) in hol
    st, _ = classify_session_type(
        date(2022, 1, 26),
        candle_count=0,
        grid_ok=False,
        holiday_dates=hol,
    )
    assert st == SESSION_TYPE_EXCLUDED_HOLIDAY
    assert date(2025, 10, 22) in hol  # Balipratipada after muhurat


def test_muhurat_exclusion():
    assert date(2025, 10, 21) in V1_EXCLUDED_MUHURAT_DATES
    st, _ = classify_session_type(
        date(2025, 10, 21),
        candle_count=6,
        grid_ok=False,
        holiday_dates=set(),
    )
    assert st == SESSION_TYPE_EXCLUDED_MUHURAT


def test_special_saturday_exclusion():
    assert date(2024, 3, 2) in V1_EXCLUDED_SPECIAL_DATES
    assert date(2024, 5, 18) in V1_EXCLUDED_SPECIAL_DATES
    st, _ = classify_session_type(
        date(2024, 3, 2),
        candle_count=12,
        grid_ok=False,
        holiday_dates=set(),
    )
    assert st == SESSION_TYPE_EXCLUDED_SPECIAL


def test_38_bar_regular_session_validation():
    d = date(2025, 1, 2)
    starts = expected_10m_starts(d)
    assert len(starts) == EXPECTED_10M_PER_SESSION
    assert starts[0].strftime("%H:%M") == "09:15"
    assert starts[-1].strftime("%H:%M") == "15:25"
    ohlc = [(100.0, 101.0, 99.0, 100.5)] * len(starts)
    check = validate_regular_session_bars(d, starts, ohlc)
    assert check["ok"] is True
    assert check["missing"] == 0


def test_zero_missing_when_full_grid():
    d = date(2025, 1, 3)
    starts = expected_10m_starts(d)
    # Drop one bar → genuine missing
    incomplete = starts[:-1]
    check = validate_regular_session_bars(d, incomplete, None)
    assert check["ok"] is False
    assert check["missing"] == 1
    # Full grid → zero missing
    full = validate_regular_session_bars(d, starts, None)
    assert full["missing"] == 0


def test_source_data_immutability_static():
    repo = Path(__file__).resolve().parents[1]
    assert_feature_modules_do_not_mutate_source(repo)


def test_dataset_version_constant():
    assert DATASET_VERSION_V1 == "sambhav_dataset_v1_20260813"


def test_duplicate_protection_unique_contract():
    # Schema contract: tables DDL declares UNIQUE (instrument_key, candle_start)
    from backend.services.sambhav import tables as tables_mod

    assert "UNIQUE (instrument_key, candle_start)" in tables_mod._DDL
    assert "sambhav_sessions" in tables_mod._DDL
    assert "sambhav_dataset_versions" in tables_mod._DDL
    assert "sambhav_features" in tables_mod._DDL


def test_no_accidental_1m_redownload():
    with pytest.raises(RuntimeError, match="does not download 1-minute"):
        import_historical_1m(None, from_date=date(2022, 1, 1))  # type: ignore[arg-type]


def test_incremental_helper_exists():
    from backend.services.sambhav import importer as imp

    assert hasattr(imp, "import_incremental_10m")
    assert hasattr(imp, "last_stored_candle_date")


def test_calibration_insufficient_when_n0():
    payload = calibration_status_payload(buckets={"n": 0, "ece": None})
    assert payload["status"] == "INSUFFICIENT DATA"
    assert payload["n"] == 0
    assert payload["ece"] is None


def test_validate_ohlc_still_strict():
    assert validate_ohlc(100, 101, 99, 100.5)
    assert not validate_ohlc(100, 99, 101, 100)


def test_backup_docs_exist():
    root = Path(__file__).resolve().parents[1]
    assert (root / "docs" / "SAMBHAV_DATASET.md").exists()
    assert (root / "docs" / "SAMBHAV_DATA_BACKUP.md").exists()
    assert (root / "scripts" / "sambhav_backup.sh").exists()
    assert (root / "scripts" / "sambhav_finalize_v1_dataset.py").exists()

"""Arbitrage morning setup: roll window + post-exclusivity 09:25 backstop."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytz

from backend.services.arbitrage_daily_setup_scheduler import (
    ArbitrageDailySetupScheduler,
    _MORNING_ROLL_EXECUTIONS,
    _futures_roll_window_ist,
    _pick_current_next_futures,
)

IST = pytz.timezone("Asia/Kolkata")


def _fut(symbol: str, expiry_ms: int) -> dict:
    return {"trading_symbol": symbol, "instrument_key": f"key:{symbol}", "expiry": expiry_ms}


def test_morning_roll_executions_include_925():
    assert _MORNING_ROLL_EXECUTIONS == frozenset({"morning_910", "morning_920", "morning_925"})


def test_futures_roll_window_after_20th_same_month():
    # SEP 2026 expiry ~25 Sep — after 20th, still in SEP → roll window
    sep_exp = int(IST.localize(datetime(2026, 9, 25, 15, 30)).timestamp() * 1000)
    first = _fut("AUROPHARMA SEP FUT", sep_exp)
    at_23 = IST.localize(datetime(2026, 9, 23, 9, 25))
    assert _futures_roll_window_ist(at_23, first) is True
    at_20 = IST.localize(datetime(2026, 9, 20, 9, 25))
    assert _futures_roll_window_ist(at_20, first) is False


def test_pick_current_next_rolls_to_2nd_3rd_in_window():
    sep = int(IST.localize(datetime(2026, 9, 25, 15, 30)).timestamp() * 1000)
    oct_ = int(IST.localize(datetime(2026, 10, 29, 15, 30)).timestamp() * 1000)
    nov = int(IST.localize(datetime(2026, 11, 26, 15, 30)).timestamp() * 1000)
    contracts = [
        _fut("X SEP FUT", sep),
        _fut("X OCT FUT", oct_),
        _fut("X NOV FUT", nov),
    ]
    fake_now = IST.localize(datetime(2026, 9, 23, 10, 0))
    with patch(
        "backend.services.arbitrage_daily_setup_scheduler.datetime"
    ) as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.fromtimestamp = datetime.fromtimestamp
        cur, nxt = _pick_current_next_futures(contracts, apply_roll_window=True)
    assert cur["trading_symbol"] == "X OCT FUT"
    assert nxt["trading_symbol"] == "X NOV FUT"
    cur2, nxt2 = _pick_current_next_futures(contracts, apply_roll_window=False)
    assert cur2["trading_symbol"] == "X SEP FUT"
    assert nxt2["trading_symbol"] == "X OCT FUT"


@patch("backend.services.arbitrage_daily_setup_scheduler.should_skip_scheduled_market_jobs_ist", return_value=False)
@patch("backend.services.arbitrage_daily_setup_scheduler._morning_910_succeeded_today_ist", return_value=False)
@patch("backend.services.arbitrage_daily_setup_scheduler.run_arbitrage_daily_setup")
@patch("backend.services.arbitrage_daily_setup_scheduler._set_morning_910_state")
def test_925_runs_when_morning_not_ok(mock_set, mock_run, _ok, _skip):
    mock_run.return_value = {"success": True, "execution": "morning_925"}
    sched = ArbitrageDailySetupScheduler()
    sched._run_morning_925()
    mock_run.assert_called_once_with(execution="morning_925")
    mock_set.assert_called_once_with(True)


@patch("backend.services.arbitrage_daily_setup_scheduler.should_skip_scheduled_market_jobs_ist", return_value=False)
@patch("backend.services.arbitrage_daily_setup_scheduler._morning_910_succeeded_today_ist", return_value=True)
@patch("backend.services.arbitrage_daily_setup_scheduler.run_arbitrage_daily_setup")
def test_925_skips_when_morning_already_ok(mock_run, _ok, _skip):
    sched = ArbitrageDailySetupScheduler()
    sched._run_morning_925()
    mock_run.assert_not_called()


@patch("backend.services.arbitrage_daily_setup_scheduler.should_skip_scheduled_market_jobs_ist", return_value=False)
@patch("backend.services.arbitrage_daily_setup_scheduler.run_arbitrage_daily_setup")
@patch("backend.services.arbitrage_daily_setup_scheduler._set_morning_910_state")
def test_910_exclusivity_skip_does_not_mark_failed(mock_set, mock_run, _skip):
    mock_run.return_value = {
        "success": False,
        "skipped": True,
        "reason": "breakfast_exclusivity",
    }
    sched = ArbitrageDailySetupScheduler()
    sched._run_morning_910()
    mock_set.assert_not_called()


def test_scheduler_registers_925_job():
    sched = ArbitrageDailySetupScheduler()
    with patch.object(sched.scheduler, "start"), patch.object(sched.scheduler, "add_job") as add_job:
        sched.start()
    ids = [c.kwargs.get("id") for c in add_job.call_args_list]
    assert "arbitrage_dailySetup_925" in ids
    assert "arbitrage_dailySetup_910" in ids
    assert "arbitrage_dailySetup_920" in ids

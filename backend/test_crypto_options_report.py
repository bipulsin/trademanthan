"""Pairing and schedule for the Crypto Options Report."""
from __future__ import annotations

from datetime import datetime, timezone

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from backend.services.crypto_options_report.pairing import apply_open_marks, pair_straddle_fills
from backend.services.crypto_options_report.scheduler import SCHEDULE_IST, register_crypto_options_jobs

IST = pytz.timezone("Asia/Kolkata")
AUTO = "ZECE5A23"


def _fill(
    *,
    fill_id: str,
    side: str,
    right: str,
    strike: int,
    expiry_ddmmyy: str,
    ist: datetime,
    price: float,
    automation: str = AUTO,
    size: float = 20,
):
    aware = IST.localize(ist) if ist.tzinfo is None else ist.astimezone(IST)
    created = aware.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    code = "C" if right == "CE" else "P"
    return {
        "id": fill_id,
        "product_symbol": f"{code}-BTC-{strike}-{expiry_ddmmyy}",
        "side": side,
        "price": price,
        "size": size,
        "created_at": created,
        "contract_value": 0.001,
        "settling_asset_symbol": "USD",
        "meta_data": {"source": "se_auto", "se_id": automation},
        "product": {
            "contract_value": "0.001",
            "settling_asset": {"symbol": "USD"},
            "quoting_asset": {"symbol": "USD"},
        },
    }


def test_four_fills_one_evening_become_one_closed_trade():
    fills = [
        _fill(fill_id="s-ce", side="sell", right="CE", strike=83000, expiry_ddmmyy="101026", ist=datetime(2026, 10, 9, 18, 0, 1), price=608),
        _fill(fill_id="s-pe", side="sell", right="PE", strike=83000, expiry_ddmmyy="101026", ist=datetime(2026, 10, 9, 18, 0, 1), price=483),
        _fill(fill_id="b-ce", side="buy", right="CE", strike=83000, expiry_ddmmyy="101026", ist=datetime(2026, 10, 9, 23, 11, 41), price=142),
        _fill(fill_id="b-pe", side="buy", right="PE", strike=83000, expiry_ddmmyy="101026", ist=datetime(2026, 10, 9, 23, 11, 41), price=402),
    ]
    trades = pair_straddle_fills(fills)
    assert len(trades) == 1
    trade = trades[0]
    assert trade["status"] == "CLOSED"
    assert trade["exit_time"] is not None
    assert trade["automation_id"] == AUTO
    assert trade["quote_currency"] == "USD"
    assert trade["ce_sell_price"] == 608
    assert trade["pe_sell_price"] == 483
    assert trade["ce_buy_price"] == 142
    assert trade["pe_buy_price"] == 402
    # (608-142 + 483-402) * 0.001 * 20 = 10.94 USD
    assert round(trade["pnl"], 2) == 10.94


def test_same_automation_id_next_day_stays_a_separate_trade():
    day1 = [
        _fill(fill_id="d1-sce", side="sell", right="CE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 8, 18, 0), price=700),
        _fill(fill_id="d1-spe", side="sell", right="PE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 8, 18, 0), price=600),
        _fill(fill_id="d1-bce", side="buy", right="CE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 9, 10, 0), price=200),
        _fill(fill_id="d1-bpe", side="buy", right="PE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 9, 10, 0), price=400),
    ]
    day2 = [
        _fill(fill_id="d2-sce", side="sell", right="CE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 9, 18, 0), price=500),
        _fill(fill_id="d2-spe", side="sell", right="PE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 9, 18, 0), price=450),
        _fill(fill_id="d2-bce", side="buy", right="CE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 9, 23, 0), price=100),
        _fill(fill_id="d2-bpe", side="buy", right="PE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 9, 23, 0), price=300),
    ]
    trades = pair_straddle_fills(day1 + day2)
    assert len(trades) == 2
    by_session = {t["session_date"].isoformat(): t for t in trades}
    assert set(by_session) == {"2026-10-08", "2026-10-09"}
    assert by_session["2026-10-08"]["status"] == "CLOSED"
    assert by_session["2026-10-09"]["status"] == "CLOSED"
    assert by_session["2026-10-08"]["automation_id"] == by_session["2026-10-09"]["automation_id"] == AUTO
    assert by_session["2026-10-08"]["ce_sell_price"] == 700
    assert by_session["2026-10-09"]["ce_sell_price"] == 500
    assert by_session["2026-10-08"]["ce_buy_fill_id"] == "d1-bce"
    assert by_session["2026-10-09"]["ce_buy_fill_id"] == "d2-bce"


def test_incomplete_straddle_stays_open_with_null_exit():
    fills = [
        _fill(fill_id="s-ce", side="sell", right="CE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 10, 18, 0), price=291),
        _fill(fill_id="s-pe", side="sell", right="PE", strike=82800, expiry_ddmmyy="111026", ist=datetime(2026, 10, 10, 18, 0), price=221),
    ]
    trades = pair_straddle_fills(fills)
    assert len(trades) == 1
    trade = trades[0]
    assert trade["status"] == "OPEN"
    assert trade["exit_time"] is None
    assert trade["ce_buy_price"] is None
    assert trade["pe_buy_price"] is None
    assert trade["pnl"] is None

    marked = apply_open_marks(trades, {"C-BTC-82800-111026": 286.0, "P-BTC-82800-111026": 200.0})
    assert marked[0]["status"] == "OPEN"
    assert marked[0]["exit_time"] is None
    assert marked[0]["ce_mark"] == 286.0
    assert round(marked[0]["pnl"], 2) == 0.52


def test_buy_at_or_after_1730_next_day_does_not_close_prior_straddle():
    fills = [
        _fill(fill_id="s-ce", side="sell", right="CE", strike=84000, expiry_ddmmyy="031026", ist=datetime(2026, 10, 2, 18, 30), price=595),
        _fill(fill_id="s-pe", side="sell", right="PE", strike=84000, expiry_ddmmyy="031026", ist=datetime(2026, 10, 2, 18, 30), price=580),
        _fill(fill_id="late-ce", side="buy", right="CE", strike=84000, expiry_ddmmyy="031026", ist=datetime(2026, 10, 3, 17, 30), price=10),
        _fill(fill_id="late-pe", side="buy", right="PE", strike=84000, expiry_ddmmyy="031026", ist=datetime(2026, 10, 3, 17, 30), price=10),
    ]
    trades = pair_straddle_fills(fills)
    assert len(trades) == 1
    assert trades[0]["status"] == "OPEN"
    assert trades[0]["exit_time"] is None
    assert trades[0]["ce_buy_price"] is None


def test_scheduler_times_are_registered_in_ist():
    scheduler = BackgroundScheduler(timezone="Asia/Kolkata")
    register_crypto_options_jobs(scheduler, func=lambda: None)
    start = IST.localize(datetime(2026, 10, 11, 0, 0, 0))
    fired = set()
    for job in scheduler.get_jobs():
        nxt = job.trigger.get_next_fire_time(None, start)
        assert nxt is not None
        assert "Asia/Kolkata" in str(job.trigger.timezone)
        assert nxt.date().isoformat() == "2026-10-11"
        fired.add((nxt.hour, nxt.minute))
    assert fired == set(SCHEDULE_IST)
    assert len(scheduler.get_jobs()) == 7

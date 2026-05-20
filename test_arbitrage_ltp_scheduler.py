"""Arbitrage intraday LTP refresh schedule (formula mirrors scheduler module)."""


def _intraday_ltp_cron_slots():
    return tuple((h, m) for h in range(9, 15) for m in (15, 45)) + ((15, 15), (15, 30))


def test_intraday_ltp_slots_every_30m_915_to_1530():
    slots = [f"{h:02d}:{m:02d}" for h, m in _intraday_ltp_cron_slots()]
    assert slots == [
        "09:15",
        "09:45",
        "10:15",
        "10:45",
        "11:15",
        "11:45",
        "12:15",
        "12:45",
        "13:15",
        "13:45",
        "14:15",
        "14:45",
        "15:15",
        "15:30",
    ]
    assert "15:45" not in slots

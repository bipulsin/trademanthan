"""Unit tests for websocket candle buckets and signed-volume classification."""
from datetime import datetime

import pytz

from backend.services.rocket_ws_live import bucket_bounds, classify_signed_volume

IST = pytz.timezone("Asia/Kolkata")


def _ist(h, m):
    return IST.localize(datetime(2026, 8, 18, h, m, 0))


def test_bucket_10m_aligned_to_915():
    start, end = bucket_bounds(_ist(9, 22), 10)
    assert start.hour == 9 and start.minute == 15
    assert end.hour == 9 and end.minute == 25


def test_bucket_10m_second_bar():
    start, end = bucket_bounds(_ist(9, 25), 10)
    assert start.minute == 25
    assert end.minute == 35


def test_classify_hit_ask_is_buy():
    signed, direction = classify_signed_volume(
        100.05, 50, best_bid=100.0, best_ask=100.05, last_px=100.0, last_dir=0
    )
    assert signed == 50
    assert direction == 1


def test_classify_hit_bid_is_sell():
    signed, direction = classify_signed_volume(
        99.95, 40, best_bid=99.95, best_ask=100.05, last_px=100.0, last_dir=0
    )
    assert signed == -40
    assert direction == -1


def test_tick_rule_uptick_without_quotes():
    signed, direction = classify_signed_volume(
        101.0, 10, best_bid=None, best_ask=None, last_px=100.0, last_dir=0
    )
    assert signed == 10
    assert direction == 1


def test_tick_rule_unchanged_inherits_prior_dir():
    signed, direction = classify_signed_volume(
        100.0, 8, best_bid=None, best_ask=None, last_px=100.0, last_dir=-1
    )
    assert signed == -8
    assert direction == -1

"""OLH WS 1m candle builder + 5m aggregator tests (a–g) — no network."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytz

from backend.services.open_low_high.candle_builder import (
    STATE_CLOSED,
    STATE_IN_PROGRESS,
    STATE_PARTIAL,
    TRUST_PARTIAL,
    TRUST_TRUSTED,
    TRUST_UNTRUSTED,
    Candle1m,
    MinuteCandleBuilder,
    TickEvent,
    aggregate_5m_from_1m,
    bar_end_1m,
    compare_official_open,
    exchange_5m_bucket,
    expected_1m_minutes,
)
from backend.services.open_high_low.bars import Bar
from backend.services.open_low_high.screener import _drop_unclosed_bars

IST = pytz.timezone("Asia/Kolkata")
DAY = datetime(2026, 11, 20)


def _ts(h: int, m: int, s: int = 0, us: int = 0) -> datetime:
    return IST.localize(DAY.replace(hour=h, minute=m, second=s, microsecond=us))


def _tick(ik: str, ts: datetime, ltp: float, **kw) -> TickEvent:
    return TickEvent(instrument_key=ik, ltp=ltp, exchange_ts=ts, **kw)


# ---------------------------------------------------------------------------
# a) Synthetic ticks → correct 1m O/H/L/C
# ---------------------------------------------------------------------------
def test_a_synthetic_ticks_build_1m_ohlc():
    closed: List[Candle1m] = []
    clock = {"now": _ts(9, 15, 50)}

    b = MinuteCandleBuilder(on_closed=closed.append, now_fn=lambda: clock["now"])
    ik = "NSE_EQ|INE002A01018"
    b.ingest_tick(_tick(ik, _ts(9, 15, 1), 100.0))
    b.ingest_tick(_tick(ik, _ts(9, 15, 20), 105.0))
    b.ingest_tick(_tick(ik, _ts(9, 15, 40), 98.0))
    b.ingest_tick(_tick(ik, _ts(9, 15, 55), 102.0))

    c = b.get(ik, _ts(9, 15))
    assert c is not None
    assert c.open == 100.0
    assert c.high == 105.0
    assert c.low == 98.0
    assert c.close == 102.0
    assert c.state == STATE_IN_PROGRESS
    assert c.oi_open is None and c.volume is None

    # Close the minute by advancing clock past bar_end
    clock["now"] = _ts(9, 16, 0)
    flushed = b.flush_closed()
    assert len(flushed) == 1
    assert flushed[0].state == STATE_CLOSED
    assert flushed[0].close == 102.0
    assert closed and closed[-1].close == 102.0


# ---------------------------------------------------------------------------
# b) Late first tick (after :05) → first-minute present but trust soft-fail path
#    Spec completeness: if 09:15 minute exists → not UNTRUSTED from missing first.
# ---------------------------------------------------------------------------
def test_b_late_first_tick_still_builds_0915_minute():
    clock = {"now": _ts(9, 15, 50)}
    b = MinuteCandleBuilder(now_fn=lambda: clock["now"])
    ik = "NSE_EQ|INE467B01029"
    # First tick at 09:15:06 (late vs WS_FIRST_TICK_TRUST_SEC=5)
    b.ingest_tick(_tick(ik, _ts(9, 15, 6), 2500.0))
    b.ingest_tick(_tick(ik, _ts(9, 15, 30), 2505.0))
    c = b.get(ik, _ts(9, 15))
    assert c is not None
    assert c.open == 2500.0
    assert c.close == 2505.0
    # Bucket still has first minute → aggregator will be TRUSTED if all 5 present
    rows = [
        Candle1m(ik, _ts(9, 15), 2500, 2505, 2499, 2505),
        Candle1m(ik, _ts(9, 16), 2505, 2510, 2504, 2508),
        Candle1m(ik, _ts(9, 17), 2508, 2512, 2507, 2511),
        Candle1m(ik, _ts(9, 18), 2511, 2515, 2510, 2514),
        Candle1m(ik, _ts(9, 19), 2514, 2516, 2513, 2515),
    ]
    agg = aggregate_5m_from_1m(rows, bucket_start=_ts(9, 15))
    assert len(agg) == 1
    assert agg[0].state == TRUST_TRUSTED
    assert agg[0].open == 2500.0
    assert agg[0].close == 2515.0


# ---------------------------------------------------------------------------
# c) Missing 09:17 → PARTIAL + missing list
# ---------------------------------------------------------------------------
def test_c_missing_0917_is_partial():
    ik = "NSE_EQ|INE009A01021"
    rows = [
        Candle1m(ik, _ts(9, 15), 100, 101, 99, 100.5),
        Candle1m(ik, _ts(9, 16), 100.5, 102, 100, 101),
        # 09:17 missing
        Candle1m(ik, _ts(9, 18), 101.5, 103, 101, 102),
        Candle1m(ik, _ts(9, 19), 102, 104, 101.5, 103),
    ]
    agg = aggregate_5m_from_1m(rows, bucket_start=_ts(9, 15))
    assert len(agg) == 1
    a = agg[0]
    assert a.state == TRUST_PARTIAL
    assert _ts(9, 17) in a.missing_minutes
    assert a.open == 100.0  # first minute open
    assert a.close == 103.0  # last present close (09:19 present)
    assert a.high == 104.0
    assert a.low == 99.0


# ---------------------------------------------------------------------------
# d) Socket drop → mark PARTIAL, resume
# ---------------------------------------------------------------------------
def test_d_socket_drop_marks_partial_then_resume():
    clock = {"now": _ts(9, 15, 30)}
    b = MinuteCandleBuilder(now_fn=lambda: clock["now"])
    ik = "NSE_INDEX|Nifty 50"
    b.ingest_tick(_tick(ik, _ts(9, 15, 10), 24000.0))  # no volume
    c = b.get(ik, _ts(9, 15))
    assert c is not None and c.state == STATE_IN_PROGRESS
    assert c.volume is None

    touched = b.mark_partial_on_reconnect([ik])
    assert len(touched) == 1
    assert touched[0].state == STATE_PARTIAL

    # Resume with more ticks
    b.ingest_tick(_tick(ik, _ts(9, 15, 40), 24010.0))
    c2 = b.get(ik, _ts(9, 15))
    assert c2 is not None
    assert c2.state == STATE_PARTIAL  # stays PARTIAL
    assert c2.high == 24010.0
    assert c2.close == 24010.0
    assert c2.volume is None  # index: missing volume never blocks


# ---------------------------------------------------------------------------
# e) Out-of-order + duplicate ticks
# ---------------------------------------------------------------------------
def test_e_out_of_order_and_duplicates():
    clock = {"now": _ts(9, 16, 5)}
    b = MinuteCandleBuilder(now_fn=lambda: clock["now"])
    ik = "NSE_EQ|INE001A01036"
    # Deliver ticks out of order within the minute
    b.ingest_tick(_tick(ik, _ts(9, 15, 40), 110.0, tick_id="t40"))
    b.ingest_tick(_tick(ik, _ts(9, 15, 10), 100.0, tick_id="t10"))  # earlier → open
    b.ingest_tick(_tick(ik, _ts(9, 15, 50), 108.0, tick_id="t50"))
    b.ingest_tick(_tick(ik, _ts(9, 15, 50), 108.0, tick_id="t50"))  # duplicate
    c = b.get(ik, _ts(9, 15))
    assert c is not None
    assert c.open == 100.0
    assert c.close == 108.0
    assert c.high == 110.0
    assert c.low == 100.0
    assert c.tick_count == 3  # dupe ignored


# ---------------------------------------------------------------------------
# f) Index with no volume never blocks 1m or 5m
# ---------------------------------------------------------------------------
def test_f_index_no_volume_never_blocks():
    ik = "NSE_INDEX|Nifty 50"
    rows = []
    for i, m in enumerate(range(15, 20)):
        rows.append(
            Candle1m(
                ik,
                _ts(9, m),
                24000 + i,
                24001 + i,
                23999 + i,
                24000.5 + i,
                volume=None,
            )
        )
    agg = aggregate_5m_from_1m(rows, bucket_start=_ts(9, 15))
    assert len(agg) == 1
    assert agg[0].state == TRUST_TRUSTED
    assert agg[0].volume is None
    assert agg[0].open == 24000.0
    assert agg[0].close == 24000.5 + 4


# ---------------------------------------------------------------------------
# g) Drop open 09:20 bar at 09:20:05
# ---------------------------------------------------------------------------
def test_g_drop_unclosed_0920_at_092005():
    b915 = Bar(
        ts=_ts(9, 15),
        open=100,
        high=105,
        low=99,
        close=104,
        volume=0,
        contract="NSE_EQ|X",
    )
    b920 = Bar(
        ts=_ts(9, 20),
        open=104,
        high=106,
        low=103,
        close=105,
        volume=0,
        contract="NSE_EQ|X",
    )
    fetch = _ts(9, 20, 5)
    out = _drop_unclosed_bars([b915, b920], fetch_time=fetch, log_label="test_g")
    assert len(out) == 1
    assert out[0].ts == _ts(9, 15)
    # At exact close 09:25 keep both
    out2 = _drop_unclosed_bars([b915, b920], fetch_time=_ts(9, 25, 0))
    assert len(out2) == 2


# ---------------------------------------------------------------------------
# Extra: first minute missing → UNTRUSTED
# ---------------------------------------------------------------------------
def test_first_minute_missing_untrusted():
    ik = "NSE_EQ|INE"
    rows = [
        Candle1m(ik, _ts(9, 16), 101, 102, 100, 101.5),
        Candle1m(ik, _ts(9, 17), 101.5, 103, 101, 102),
        Candle1m(ik, _ts(9, 18), 102, 104, 101.5, 103),
        Candle1m(ik, _ts(9, 19), 103, 105, 102, 104),
    ]
    agg = aggregate_5m_from_1m(rows, bucket_start=_ts(9, 15))
    assert agg[0].state == TRUST_UNTRUSTED
    assert _ts(9, 15) in agg[0].missing_minutes


def test_exchange_5m_bucket_alignment():
    assert exchange_5m_bucket(_ts(9, 15, 1)) == _ts(9, 15)
    assert exchange_5m_bucket(_ts(9, 19, 59)) == _ts(9, 15)
    assert exchange_5m_bucket(_ts(9, 20, 0)) == _ts(9, 20)
    assert exchange_5m_bucket(_ts(9, 24, 30)) == _ts(9, 20)
    assert exchange_5m_bucket(_ts(9, 25, 0)) == _ts(9, 25)
    assert expected_1m_minutes(_ts(9, 15)) == [
        _ts(9, 15),
        _ts(9, 16),
        _ts(9, 17),
        _ts(9, 18),
        _ts(9, 19),
    ]
    assert bar_end_1m(_ts(9, 15)) == _ts(9, 16)


def test_official_open_mismatch_logged():
    r = compare_official_open(
        {"open": 100.0, "high": 105.0, "low": 99.0},
        {"open": 100.5, "high": 105.0, "low": 99.0},  # 0.5% open diff
        tolerance_pct=0.05,
        label="test",
    )
    assert r["ok"] is False
    assert "open" in r["mismatches"]


def test_oi_absent_futures_present():
    """OI NULL for EQ; futures can store OI — neither gates 1m build."""
    clock = {"now": _ts(9, 15, 30)}
    b = MinuteCandleBuilder(now_fn=lambda: clock["now"])
    eq = "NSE_EQ|INE002A01018"
    fut = "NSE_FO|NIFTY26NOVFUT"
    b.ingest_tick(_tick(eq, _ts(9, 15, 5), 100.0, oi=None, volume=None))
    b.ingest_tick(_tick(fut, _ts(9, 15, 5), 24050.0, oi=1_200_000, volume=50))
    ce = b.get(eq, _ts(9, 15))
    cf = b.get(fut, _ts(9, 15))
    assert ce is not None and ce.oi_close is None
    assert cf is not None and cf.oi_close == 1_200_000 and cf.volume == 50

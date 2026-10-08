"""Commodity-divergence alerts on the Premium Futures webhook. No database."""
import json
from datetime import datetime
from unittest.mock import patch

import pytz

from backend.services.premium_futures_divergence import (
    HANDLER_DIVERGENCE,
    HANDLER_JSON,
    MemoryDivergenceStore,
    apply_divergence_event,
    apply_mark_refresh,
    apply_session_flat,
    could_have_pnl_rupees,
    divergence_could_have_rows,
    divergence_ticker_signals,
    divergence_workspace_picks,
    _candle_close_near,
    entry_at_from_first_scan,
    ingest_divergence_webhook,
    refresh_could_have_on_scan,
    parse_divergence_alert,
    parse_event_time,
    resolve_entry_ltp,
    select_premium_futures_handler,
)
from backend.services.premium_futures_tv_webhook import (
    PARSE_SUCCESS,
    decode_raw_payload,
    parse_tv_alert,
)

IST = pytz.timezone("Asia/Kolkata")
ALERT_MS = 1790654400000  # 2026-09-29 09:30:00 IST
EXIT_MS = 1790655300000  # 2026-09-29 09:45:00 IST

FUT = {
    "underlying": "RELIANCE",
    "fut_symbol": "RELIANCE25SEPFUT",
    "fut_instrument_key": "NSE_FO|reliance",
}
DIV_AT = datetime(2026, 9, 28, 10, 15, 0)
GO_AT = datetime(2026, 9, 28, 10, 22, 0)
EXIT_AT = datetime(2026, 9, 28, 11, 5, 0)
SENTENCE = "TWCTO Commodity Divergence : order BULL-DIV @ 1 filled on RELIANCE."
_NO_QUOTE = object()


def _resolve(symbol):
    if str(symbol).upper() == "RELIANCE":
        return dict(FUT)
    return None


def _apply(store, text, now, ltp=_NO_QUOTE, lot=None):
    alert = parse_divergence_alert(None, text)
    assert alert is not None

    def _ltp(_ik):
        if ltp is _NO_QUOTE:
            raise AssertionError("LTP should not be read")
        return ltp(_ik) if callable(ltp) else ltp

    return apply_divergence_event(
        alert,
        store=store,
        resolver=_resolve,
        ltp_fn=_ltp,
        lot_fn=lambda _ik: lot,
        now=now,
    )


def test_divergence_string_parses_action_contracts_ticker():
    hit = parse_divergence_alert(None, SENTENCE)
    assert hit["action"] == "BULL-DIV"
    assert hit["contracts"] == 1
    assert hit["symbol"] == "RELIANCE"
    loose = parse_divergence_alert(
        None,
        "TWCTO   Commodity  Divergence:  order   bear-div  @   3   filled   on   tcs.",
    )
    assert loose["action"] == "BEAR-DIV"
    assert loose["contracts"] == 3
    assert loose["symbol"] == "TCS"
    no_period = parse_divergence_alert(
        None, "TWCTO Commodity Divergence : order BULL-GO @ 2 filled on RELIANCE"
    )
    assert no_period["action"] == "BULL-GO"
    assert no_period["contracts"] == 2
    as_json = {
        "message": SENTENCE,
        "strategy": {"order": {"action": "BEAR-EXIT", "contracts": "4"}},
        "ticker": "INFY",
    }
    from_message = parse_divergence_alert(as_json, "")
    assert from_message["action"] == "BULL-DIV"
    assert from_message["symbol"] == "RELIANCE"
    structured = parse_divergence_alert(
        {"strategy": {"order": {"action": "BULL-GO", "contracts": "2"}}, "ticker": "NSE:TCS"},
        "",
    )
    assert structured["action"] == "BULL-GO"
    assert structured["contracts"] == 2
    assert structured["symbol"] == "TCS"


def test_unknown_ticker_is_dropped():
    store = MemoryDivergenceStore()
    alert = parse_divergence_alert(
        None, "TWCTO Commodity Divergence : order BULL-DIV @ 1 filled on NOTAREAL."
    )
    result = apply_divergence_event(
        alert,
        store=store,
        resolver=lambda _s: None,
        ltp_fn=lambda _ik: (_ for _ in ()).throw(AssertionError("no quote")),
        now=DIV_AT,
    )
    assert result["dropped"] is True
    assert result["applied"] is False
    go = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on NOTAREAL.",
        GO_AT,
    )
    assert go["dropped"] is True
    assert go["applied"] is False
    bull, bear = divergence_workspace_picks(store, DIV_AT.date())
    assert bull == [] and bear == []
    assert divergence_could_have_rows(store, GO_AT.date()) == []


def test_bull_div_creates_pick_with_enter_disabled_and_not_in_ticker():
    store = MemoryDivergenceStore()
    result = _apply(store, SENTENCE, DIV_AT)
    assert result["applied"] is True
    assert result["enter_enabled"] is False
    assert result["ticker_eligible"] is False
    bull, bear = divergence_workspace_picks(store, DIV_AT.date())
    assert len(bull) == 1 and bear == []
    assert bull[0]["future_symbol"] == "RELIANCE25SEPFUT"
    assert bull[0]["order_eligible"] is False
    assert bull[0]["contracts"] == 1
    assert divergence_ticker_signals(store, DIV_AT.date()) == []


def test_bull_go_without_prior_pick_creates_pick_enter_ticker_and_could_have():
    store = MemoryDivergenceStore()
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=2500.5,
    )
    assert result["applied"] is True
    assert result["ignored"] is False
    assert result["dropped"] is False
    assert result["enter_enabled"] is True
    assert result["ticker_eligible"] is True
    assert result["entry_ltp"] == 2500.5
    assert result["ltp_missing"] is False
    bull, bear = divergence_workspace_picks(store, GO_AT.date())
    assert len(bull) == 1 and bear == []
    assert bull[0]["underlying"] == "RELIANCE"
    assert bull[0]["direction_type"] == "LONG"
    assert bull[0]["future_symbol"] == "RELIANCE25SEPFUT"
    assert bull[0]["instrument_key"] == "NSE_FO|reliance"
    assert bull[0]["order_eligible"] is True
    signals = divergence_ticker_signals(store, GO_AT.date())
    assert signals == [
        {
            "algo": "premium_futures",
            "symbol": "RELIANCE25SEPFUT",
            "label": "Bullish",
        }
    ]
    could = divergence_could_have_rows(store, GO_AT.date())
    assert len(could) == 1
    assert could[0]["entry_ltp"] == 2500.5
    assert could[0]["entry_price"] == 2500.5
    assert could[0]["first_scan_time"] == "10:15"  # nearest 15m of receipt 10:22
    assert could[0]["entry_time"] == "10:27"
    assert could[0]["direction_type"] == "LONG"
    assert could[0]["exit_ltp"] is None
    bull_hits = divergence_workspace_picks(store, GO_AT.date())[0]
    assert bull_hits[0]["first_hit_at"].endswith("10:15:00+05:30") or "T10:15:00" in bull_hits[0]["first_hit_at"]
    assert bull_hits[0]["last_hit_at"].endswith("10:15:00+05:30") or "T10:15:00" in bull_hits[0]["last_hit_at"]

    later = datetime(2026, 9, 28, 10, 30, 0)
    second = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 2 filled on RELIANCE.",
        later,
        ltp=2511.0,
    )
    assert second["applied"] is True
    bull2, bear2 = divergence_workspace_picks(store, GO_AT.date())
    assert len(bull2) == 1 and bear2 == []
    assert bull2[0]["future_symbol"] == "RELIANCE25SEPFUT"
    assert bull2[0]["order_eligible"] is True
    could2 = divergence_could_have_rows(store, GO_AT.date())
    assert len(could2) == 1
    assert could2[0]["entry_ltp"] == 2500.5


def test_bear_go_without_prior_pick_creates_bearish_pick():
    store = MemoryDivergenceStore()
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BEAR-GO @ 3 filled on RELIANCE.",
        GO_AT,
        ltp=2490.0,
        lot=500,
    )
    assert result["applied"] is True
    assert result["enter_enabled"] is True
    assert result["ticker_eligible"] is True
    assert result["entry_ltp"] == 2490.0
    bull, bear = divergence_workspace_picks(store, GO_AT.date())
    assert bull == [] and len(bear) == 1
    assert bear[0]["direction_type"] == "SHORT"
    assert bear[0]["order_eligible"] is True
    assert divergence_ticker_signals(store, GO_AT.date()) == [
        {
            "algo": "premium_futures",
            "symbol": "RELIANCE25SEPFUT",
            "label": "Bearish",
        }
    ]
    could = divergence_could_have_rows(store, GO_AT.date())
    assert len(could) == 1
    assert could[0]["direction_type"] == "SHORT"
    assert could[0]["entry_ltp"] == 2490.0
    assert could[0]["qty"] == 500
    assert bear[0]["contracts"] == 3


def test_bull_go_after_div_enables_enter_ticker_and_could_have_entry():
    store = MemoryDivergenceStore()
    _apply(store, SENTENCE, DIV_AT)
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=2500.5,
        lot=250,
    )
    assert result["applied"] is True
    assert result["enter_enabled"] is True
    assert result["ticker_eligible"] is True
    assert result["entry_ltp"] == 2500.5
    assert result["ltp_missing"] is False
    bull, _bear = divergence_workspace_picks(store, GO_AT.date())
    assert bull[0]["order_eligible"] is True
    signals = divergence_ticker_signals(store, GO_AT.date())
    assert signals == [
        {
            "algo": "premium_futures",
            "symbol": "RELIANCE25SEPFUT",
            "label": "Bullish",
        }
    ]
    could = divergence_could_have_rows(store, GO_AT.date())
    assert len(could) == 1
    assert could[0]["entry_ltp"] == 2500.5
    assert could[0]["first_scan_time"] == "10:15"  # DIV receipt 10:15 (already on quarter)
    assert could[0]["entry_time"] == "10:20"
    assert could[0]["exit_ltp"] is None
    assert could[0]["qty"] == 250


def test_bull_go_records_null_entry_when_ltp_missing():
    store = MemoryDivergenceStore()
    _apply(store, SENTENCE, DIV_AT)
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=None,
    )
    assert result["applied"] is True
    assert result["ltp_missing"] is True
    assert result["entry_ltp"] is None
    bull, _bear = divergence_workspace_picks(store, GO_AT.date())
    assert bull[0]["order_eligible"] is True
    could = divergence_could_have_rows(store, GO_AT.date())
    assert could[0]["entry_ltp"] is None
    assert could[0]["ltp_missing_entry"] is True


def test_bull_exit_removes_pick_and_stamps_exit_ltp():
    store = MemoryDivergenceStore()
    _apply(store, SENTENCE, DIV_AT)
    _apply(
        store,
        "TWCTO Commodity Divergence : order BEAR-DIV @ 1 filled on RELIANCE.",
        DIV_AT,
        ltp=1,
    )
    _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=2500.5,
    )
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-EXIT @ 1 filled on RELIANCE.",
        EXIT_AT,
        ltp=2510.25,
    )
    assert result["applied"] is True
    assert result["exit_ltp"] == 2510.25
    bull, bear = divergence_workspace_picks(store, EXIT_AT.date())
    assert bull == [] and bear == []
    assert divergence_ticker_signals(store, EXIT_AT.date()) == []
    could = divergence_could_have_rows(store, EXIT_AT.date())
    assert could[0]["entry_ltp"] == 2500.5
    assert could[0]["exit_ltp"] == 2510.25
    assert could[0]["exit_time"] == "11:00"


def test_exit_stamps_null_when_ltp_missing():
    store = MemoryDivergenceStore()
    _apply(store, SENTENCE, DIV_AT)
    _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=100.0,
    )
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-EXIT @ 1 filled on RELIANCE.",
        EXIT_AT,
        ltp=lambda _ik: None,
    )
    assert result["applied"] is True
    assert result["ltp_missing"] is True
    bull, bear = divergence_workspace_picks(store, EXIT_AT.date())
    assert bull == [] and bear == []
    could = divergence_could_have_rows(store, EXIT_AT.date())
    assert could[0]["exit_ltp"] is None
    assert could[0]["ltp_missing_exit"] is True


def test_existing_json_is_not_consumed_by_divergence_parser():
    body = {
        "ticker": "NSE:RELIANCE",
        "action": "buy",
        "side": "long",
        "message": "BULLISH",
    }
    raw = json.dumps(body)
    assert parse_divergence_alert(body, raw) is None
    assert select_premium_futures_handler(body, raw) == HANDLER_JSON
    assert ingest_divergence_webhook(body, raw, DIV_AT) is None
    status, _ticker, sym, side = parse_tv_alert(body, raw)
    assert status == PARSE_SUCCESS
    assert sym == "RELIANCE"
    assert side == "bullish"
    parsed, _payload, text = decode_raw_payload(b"NSE:RELIANCE BULLISH")
    assert select_premium_futures_handler(parsed, text) == HANDLER_JSON
    assert select_premium_futures_handler(None, "order BULL-DIV filled on RELIANCE") == HANDLER_JSON
    dotted = {"ticker": "RELIANCE", "strategy.order.action": "BULL-DIV", "strategy.order.contracts": 1}
    assert select_premium_futures_handler(dotted, json.dumps(dotted)) == HANDLER_DIVERGENCE
    top_level = {"ticker": "RELIANCE", "action": "BULL-DIV"}
    assert select_premium_futures_handler(top_level, json.dumps(top_level)) == HANDLER_JSON
    other_flag = {"flag": "LONG", "symbol": "RELIANCE", "time": ALERT_MS, "action": "buy", "side": "long"}
    assert parse_divergence_alert(other_flag, json.dumps(other_flag)) is None
    assert select_premium_futures_handler(other_flag, json.dumps(other_flag)) == HANDLER_JSON


def _ist_stamp(dt):
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    else:
        dt = dt.astimezone(IST)
    return dt.strftime("%Y-%m-%d %H:%M")


def _apply_alert(store, alert, now, ltp=_NO_QUOTE, lot=None):
    def _ltp(_ik):
        if ltp is _NO_QUOTE:
            raise AssertionError("LTP should not be read")
        return ltp(_ik) if callable(ltp) else ltp

    return apply_divergence_event(
        alert,
        store=store,
        resolver=_resolve,
        ltp_fn=_ltp,
        lot_fn=lambda _ik: lot,
        now=now,
    )


def test_flag_json_parses_div_go_exit_and_alert_time():
    expected = _ist_stamp(datetime(2026, 9, 29, 9, 30))
    for flag in ("BULL-DIV", "BEAR-DIV", "BULL-GO", "BEAR-GO", "BULL-EXIT", "BEAR-EXIT"):
        body = {"flag": flag, "symbol": "RELIANCE1!", "time": ALERT_MS}
        raw = json.dumps(body)
        assert select_premium_futures_handler(body, raw) == HANDLER_DIVERGENCE
        hit = parse_divergence_alert(body, raw)
        assert hit["action"] == flag
        assert hit["symbol"] == "RELIANCE"
        assert hit["ticker_raw"] == "RELIANCE1!"
        assert _ist_stamp(hit["event_at"]) == expected
    plain = {"flag": "BULL-DIV", "symbol": "RELIANCE", "time": ALERT_MS}
    assert parse_divergence_alert(plain, json.dumps(plain))["symbol"] == "RELIANCE"
    assert _ist_stamp(parse_event_time(str(ALERT_MS))) == expected
    assert _ist_stamp(parse_event_time(ALERT_MS // 1000)) == expected
    assert _ist_stamp(parse_event_time("2026-09-29T09:30:00+05:30")) == expected
    assert _ist_stamp(parse_event_time("2026-09-29T04:00:00Z")) == expected
    assert parse_event_time("not-a-time") is None
    assert parse_event_time(None) is None


def test_scan_display_clamps_future_candle_to_receipt_quarter():
    from backend.services.premium_futures_divergence import _scan_hm

    now = IST.localize(datetime(2026, 10, 8, 9, 47, 0))
    assert _scan_hm(datetime(2026, 10, 8, 11, 0, 0), now) == "09:45"
    assert _scan_hm(datetime(2026, 10, 8, 9, 47, 12), now) == "09:45"


def test_flag_json_go_and_exit_use_receipt_time_not_candle_open():
    store = MemoryDivergenceStore()
    received = datetime(2026, 9, 29, 9, 45, 38)
    go = parse_divergence_alert(
        {"flag": "BULL-GO", "symbol": "RELIANCE", "time": ALERT_MS},
        "",
    )
    result = _apply_alert(store, go, received, ltp=2500.5)
    assert result["applied"] is True
    assert result["dropped"] is False
    assert result["enter_enabled"] is True
    assert result["ticker_eligible"] is True
    assert result["entry_ltp"] == 2500.5
    bull, bear = divergence_workspace_picks(store, datetime(2026, 9, 29).date())
    assert len(bull) == 1 and bear == []
    assert bull[0]["order_eligible"] is True
    # Receipt 09:45:38 → nearest 15m display 09:45 (not candle open 09:30).
    # Sep 29 is in the past relative to the test runner, so the future-clamp does not move it.
    assert "T09:45:00" in (bull[0]["first_hit_at"] or "")
    assert "T09:45:00" in (bull[0]["last_hit_at"] or "")
    assert divergence_ticker_signals(store, datetime(2026, 9, 29).date()) == [
        {
            "algo": "premium_futures",
            "symbol": "RELIANCE25SEPFUT",
            "label": "Bullish",
        }
    ]
    could = divergence_could_have_rows(store, datetime(2026, 9, 29).date())
    assert could[0]["entry_ltp"] == 2500.5
    assert could[0]["first_scan_time"] == "09:45"
    assert could[0]["entry_time"] == "09:50"
    assert could[0]["exit_ltp"] is None

    exit_body = {"flag": "BULL-EXIT", "symbol": "RELIANCE", "time": EXIT_MS}
    exit_alert = parse_divergence_alert(exit_body, json.dumps(exit_body))
    exit_received = datetime(2026, 9, 29, 9, 52, 10)
    exit_result = _apply_alert(store, exit_alert, exit_received, ltp=2510.25)
    assert exit_result["applied"] is True
    assert exit_result["exit_ltp"] == 2510.25
    assert exit_result["enter_enabled"] is False
    bull2, bear2 = divergence_workspace_picks(store, datetime(2026, 9, 29).date())
    assert bull2 == [] and bear2 == []
    assert divergence_ticker_signals(store, datetime(2026, 9, 29).date()) == []
    closed = divergence_could_have_rows(store, datetime(2026, 9, 29).date())
    assert closed[0]["exit_ltp"] == 2510.25
    # Receipt 09:52:10 → nearest 15m 09:45? 09:52 is closer to 09:45 (7m) than 10:00 (8m)
    assert closed[0]["exit_time"] == "09:45"
    assert closed[0]["exit_scan_time"] == "09:45"


def test_flag_json_div_keeps_enter_off_and_bad_time_uses_receive_time():
    store = MemoryDivergenceStore()
    received = datetime(2026, 9, 29, 9, 45, 38)
    div = parse_divergence_alert(
        {"flag": "bear-div", "symbol": "RELIANCE", "time": "nope"},
        "",
    )
    result = _apply_alert(store, div, received)
    assert result["applied"] is True
    assert result["enter_enabled"] is False
    assert result["ticker_eligible"] is False
    bull, bear = divergence_workspace_picks(store, received.date())
    assert bull == [] and len(bear) == 1
    assert bear[0]["order_eligible"] is False
    assert divergence_ticker_signals(store, received.date()) == []
    go = parse_divergence_alert({"flag": "BEAR-GO", "symbol": "RELIANCE", "time": ""}, "")
    _apply_alert(store, go, received, ltp=2400.0)
    could = divergence_could_have_rows(store, received.date())
    assert could[0]["entry_time"] == "09:50"
    assert could[0]["entry_ltp"] == 2400.0
    assert could[0]["direction_type"] == "SHORT"


def test_flag_json_unknown_symbol_is_dropped():
    store = MemoryDivergenceStore()
    alert = parse_divergence_alert(
        {"flag": "BULL-GO", "symbol": "NOTAREAL1!", "time": ALERT_MS},
        "",
    )
    result = apply_divergence_event(
        alert,
        store=store,
        resolver=lambda _s: None,
        ltp_fn=lambda _ik: (_ for _ in ()).throw(AssertionError("no quote")),
        now=datetime(2026, 9, 29, 9, 45, 38),
    )
    assert result["dropped"] is True
    assert result["applied"] is False
    bull, bear = divergence_workspace_picks(store, datetime(2026, 9, 29).date())
    assert bull == [] and bear == []


def test_entry_is_first_scan_plus_five_and_scan_ltp_fills_entry_when_later_quote_missing():
    assert entry_at_from_first_scan(GO_AT).astimezone(IST).strftime("%H:%M") == "10:27"
    assert resolve_entry_ltp(None, 2500.5) == 2500.5
    assert resolve_entry_ltp(2511.25, 2500.5) == 2511.25
    store = MemoryDivergenceStore()
    _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=2500.5,
        lot=250,
    )
    could = divergence_could_have_rows(store, GO_AT.date())
    assert could[0]["first_scan_time"] == "10:15"  # nearest 15m of GO receipt 10:22
    assert could[0]["entry_time"] == "10:27"
    assert could[0]["entry_ltp"] == 2500.5
    assert could[0]["qty"] == 250
    assert "second_scan_hm" not in could[0]
    assert "pnl_1515_rupees" not in could[0]
    assert "exit_1515_time" not in could[0]


def test_pnl_sign_is_long_versus_short_times_one_lot_qty():
    assert could_have_pnl_rupees("LONG", 100, 110, 250) == 2500
    assert could_have_pnl_rupees("SHORT", 100, 110, 250) == -2500
    assert could_have_pnl_rupees("LONG", 100, 90, 250) == -2500
    assert could_have_pnl_rupees("BEARISH", 100, 90, 250) == 2500


def test_exit_sets_exit_time_and_stops_ltp_refresh():
    store = MemoryDivergenceStore()
    _apply(store, SENTENCE, DIV_AT)
    _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=2500.5,
        lot=250,
    )
    open_row = store.could[0]
    moved = apply_mark_refresh(open_row, 2600)
    assert moved["current_ltp"] == 2600
    assert moved["pnl_scan_rupees"] == round((2600 - 2500.5) * 250, 2)
    _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-EXIT @ 1 filled on RELIANCE.",
        EXIT_AT,
        ltp=2510.25,
    )
    could = divergence_could_have_rows(store, EXIT_AT.date())
    assert could[0]["exit_scan_time"] == "11:00"
    assert could[0]["exit_time"] == "11:00"
    assert could[0]["current_ltp"] == 2510.25
    assert could[0]["pnl_scan_rupees"] == round((2510.25 - 2500.5) * 250, 2)
    frozen = apply_mark_refresh(store.could[0], 9999)
    assert frozen["current_ltp"] == 2510.25
    assert frozen.get("pnl_scan_rupees") != round((9999 - 2500.5) * 250, 2)


def test_past_1515_without_exit_becomes_1515_and_refresh_stops():
    store = MemoryDivergenceStore()
    _apply(
        store,
        "TWCTO Commodity Divergence : order BEAR-GO @ 4 filled on RELIANCE.",
        GO_AT,
        ltp=100.0,
        lot=500,
    )
    before = divergence_could_have_rows(store, GO_AT.date(), now=datetime(2026, 9, 28, 14, 0))
    assert before[0]["exit_scan_time"] is None
    closed = apply_session_flat(store.could[0], datetime(2026, 9, 28, 15, 20))
    assert closed["exit_at"].strftime("%H:%M") == "15:15"
    assert closed["current_ltp"] == 100.0
    assert could_have_pnl_rupees("SHORT", 100.0, 100.0, 500) == 0
    store.could[0] = closed
    rows = divergence_could_have_rows(
        store,
        GO_AT.date(),
        now=datetime(2026, 9, 28, 15, 20),
        persist_flat=True,
    )
    assert rows[0]["exit_scan_time"] == "15:15"
    assert rows[0]["qty"] == 500
    assert rows[0]["pnl_scan_rupees"] == 0
    frozen = apply_mark_refresh(store.could[0], 80)
    assert frozen["current_ltp"] == 100.0
    # A later refresh must not book the short PnL of the new quote.
    assert frozen.get("pnl_scan_rupees") != could_have_pnl_rupees("SHORT", 100.0, 80, 500)


def test_candle_close_near_uses_entry_minute_or_the_next_bar():
    target = IST.localize(datetime(2026, 10, 1, 10, 5, 0))
    candles = [
        {"timestamp": "2026-10-01T09:15:00+05:30", "close": 100.0},
        {"timestamp": "2026-10-01T10:04:00+05:30", "close": 110.0},
        {"timestamp": "2026-10-01T10:05:00+05:30", "close": 111.5},
        {"timestamp": "2026-10-01T10:06:00+05:30", "close": 112.0},
    ]
    assert _candle_close_near(candles, target) == 111.5
    missing_minute = [c for c in candles if "10:05" not in c["timestamp"] and "10:04" not in c["timestamp"]]
    assert _candle_close_near(missing_minute, target) == 112.0
    assert _candle_close_near([candles[0]], target) is None


def test_refresh_stores_historical_entry_and_does_not_copy_the_live_mark():
    store = MemoryDivergenceStore()
    _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        datetime(2026, 10, 1, 10, 0, 0),
        ltp=685.0,
        lot=825,
    )
    open_row = store.could[0]
    assert open_row["entry_ltp"] == 685.0
    assert open_row["current_ltp"] == 685.0

    def _hist(_ik, _target):
        return 690.25

    with patch(
        "backend.services.premium_futures_divergence.DbDivergenceStore",
        return_value=store,
    ), patch(
        "backend.services.premium_futures_divergence._historical_1m_ltp_at",
        side_effect=_hist,
    ), patch(
        "backend.services.premium_futures_divergence._currmth_ltp_by_underlying",
        return_value={"RELIANCE": 700.0},
    ):
        summary = refresh_could_have_on_scan(now=datetime(2026, 10, 1, 12, 0, 0))

    assert summary["entry_upgraded"] == 1
    assert summary["updated"] == 1
    assert summary["flattened"] == 0
    row = store.could[0]
    assert row["entry_ltp"] == 690.25
    assert row["current_ltp"] == 700.0
    assert row["exit_at"] is None
    assert row["entry_ltp_from_history"] is True

    def _missing(_ik, _target):
        return None

    store2 = MemoryDivergenceStore()
    _apply(
        store2,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        datetime(2026, 10, 1, 10, 0, 0),
        ltp=685.0,
        lot=825,
    )
    with patch(
        "backend.services.premium_futures_divergence.DbDivergenceStore",
        return_value=store2,
    ), patch(
        "backend.services.premium_futures_divergence._historical_1m_ltp_at",
        side_effect=_missing,
    ), patch(
        "backend.services.premium_futures_divergence._currmth_ltp_by_underlying",
        return_value={"RELIANCE": 700.0},
    ):
        refresh_could_have_on_scan(now=datetime(2026, 10, 1, 12, 0, 0))
    assert store2.could[0]["entry_ltp"] == 685.0
    assert store2.could[0]["current_ltp"] == 700.0


def test_refresh_upgrades_entry_before_session_flat_and_keeps_frozen_mark():
    store = MemoryDivergenceStore()
    _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        datetime(2026, 10, 1, 10, 0, 0),
        ltp=685.0,
        lot=825,
    )
    with patch(
        "backend.services.premium_futures_divergence.DbDivergenceStore",
        return_value=store,
    ), patch(
        "backend.services.premium_futures_divergence._historical_1m_ltp_at",
        return_value=690.25,
    ), patch(
        "backend.services.premium_futures_divergence._currmth_ltp_by_underlying",
        return_value={"RELIANCE": 999.0},
    ):
        summary = refresh_could_have_on_scan(now=datetime(2026, 10, 1, 15, 20, 0))
    assert summary["entry_upgraded"] == 1
    assert summary["flattened"] == 1
    assert summary["updated"] == 0
    row = store.could[0]
    assert row["entry_ltp"] == 690.25
    assert row["current_ltp"] == 685.0
    assert row["exit_ltp"] == 685.0
    assert row["exit_at"] is not None
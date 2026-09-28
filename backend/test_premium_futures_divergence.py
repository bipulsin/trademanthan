"""Commodity-divergence alerts on the Premium Futures webhook. No database."""
import json
from datetime import datetime

from backend.services.premium_futures_divergence import (
    HANDLER_DIVERGENCE,
    HANDLER_JSON,
    MemoryDivergenceStore,
    apply_divergence_event,
    divergence_could_have_rows,
    divergence_ticker_signals,
    divergence_workspace_picks,
    ingest_divergence_webhook,
    parse_divergence_alert,
    select_premium_futures_handler,
)
from backend.services.premium_futures_tv_webhook import (
    PARSE_SUCCESS,
    decode_raw_payload,
    parse_tv_alert,
)

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


def _apply(store, text, now, ltp=_NO_QUOTE):
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
    bull, bear = divergence_workspace_picks(store, DIV_AT.date())
    assert bull == [] and bear == []


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


def test_bull_go_without_prior_pick_does_nothing():
    store = MemoryDivergenceStore()
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
    )
    assert result["applied"] is False
    assert result["ignored"] is True
    bull, bear = divergence_workspace_picks(store, GO_AT.date())
    assert bull == [] and bear == []
    assert divergence_could_have_rows(store, GO_AT.date()) == []
    assert divergence_ticker_signals(store, GO_AT.date()) == []


def test_bull_go_after_div_enables_enter_ticker_and_could_have_entry():
    store = MemoryDivergenceStore()
    _apply(store, SENTENCE, DIV_AT)
    result = _apply(
        store,
        "TWCTO Commodity Divergence : order BULL-GO @ 1 filled on RELIANCE.",
        GO_AT,
        ltp=2500.5,
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
    assert could[0]["entry_time"] == "10:22"
    assert could[0]["exit_ltp"] is None
    assert could[0]["qty"] == 1


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
    assert could[0]["exit_time"] == "11:05"


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
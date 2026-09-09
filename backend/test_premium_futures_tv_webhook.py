"""Premium Futures TradingView webhook parse tests (no DB)."""
from datetime import date, datetime

import pytz

from backend.services.premium_futures_tv_webhook import (
    PARSE_FAILED,
    PARSE_SUCCESS,
    decode_raw_payload,
    extract_ticker_raw,
    merge_tv_picks_into_workspace,
    normalize_tv_ticker,
    parse_tv_alert,
    parse_tv_side,
)


IST = pytz.timezone("Asia/Kolkata")


def test_normalize_nse_colon_ticker():
    assert normalize_tv_ticker("NSE:RELIANCE") == "RELIANCE"
    assert normalize_tv_ticker("reliance") == "RELIANCE"
    assert normalize_tv_ticker(" NSE:RELIANCE ") == "RELIANCE"


def test_normalize_unexpanded_template_is_none():
    assert normalize_tv_ticker("{{ticker}}") is None


def test_normalize_continuous_and_fut_label():
    assert normalize_tv_ticker("NSE:RELIANCE1!") == "RELIANCE"
    assert normalize_tv_ticker("RELIANCE FUT 30 JUN 26") == "RELIANCE"


def test_side_from_strategy_action_buy():
    assert parse_tv_side({"strategy.order.action": "buy"}) == "bullish"
    assert parse_tv_side({"action": "sell"}) == "bearish"


def test_side_from_message_words():
    assert parse_tv_side({"ticker": "NSE:RELIANCE", "message": "BULLISH"}) == "bullish"
    assert parse_tv_side(None, "NSE:TCS BEARISH") == "bearish"
    assert parse_tv_side({"message": "go LONG"}) == "bullish"
    assert parse_tv_side({"message": "SHORT"}) == "bearish"


def test_parse_json_alert_success_shape():
    status, raw, sym, side = parse_tv_alert(
        {"ticker": "NSE:RELIANCE", "side": "bullish"}
    )
    assert status == PARSE_SUCCESS
    assert sym == "RELIANCE"
    assert side == "bullish"
    assert raw


def test_parse_plain_text():
    parsed, payload, raw = decode_raw_payload(b"NSE:RELIANCE BULLISH")
    assert parsed is None
    assert "_raw" in payload
    status, ticker_raw, sym, side = parse_tv_alert(parsed, raw)
    assert status == PARSE_SUCCESS
    assert extract_ticker_raw(parsed, raw)
    assert sym == "RELIANCE"
    assert side == "bullish"


def test_unknown_symbol_parses_but_is_not_faked():
    """Unknown names still parse; master miss is unmatched at ingest (no fake FUT)."""
    status, _, sym, side = parse_tv_alert({"ticker": "NSE:NOTAREALTICKERXYZ", "side": "LONG"})
    assert status == PARSE_SUCCESS
    assert sym == "NOTAREALTICKERXYZ"
    assert side == "bullish"


def test_missing_side_is_failed():
    status, _, sym, side = parse_tv_alert({"ticker": "NSE:RELIANCE"})
    assert status == PARSE_FAILED
    assert sym == "RELIANCE"
    assert side is None


def test_unknown_symbol_resolve_returns_none(monkeypatch):
    import backend.services.premium_futures_tv_webhook as m

    monkeypatch.setattr(m, "resolve_currmth_future", lambda symbol: None)
    assert m.resolve_currmth_future("NOTAREALTICKERXYZ") is None


def test_extract_tv_ohlc_from_payload():
    from backend.services.premium_futures_tv_webhook import extract_tv_ohlc

    hi, lo = extract_tv_ohlc({"ticker": "NSE:PFC", "high": 412.5, "low": 408.1})
    assert hi == 412.5
    assert lo == 408.1
    hi2, lo2 = extract_tv_ohlc({"high": "{{high}}"})
    assert hi2 is None and lo2 is None


def test_floor_received_to_15m():
    from backend.services.premium_futures_tv_webhook import floor_received_to_15m

    dt = IST.localize(datetime(2026, 9, 9, 11, 14, 22))
    slot = floor_received_to_15m(dt)
    assert slot.hour == 11 and slot.minute == 0


def test_merge_tv_featured_first_and_dedupes_scanner(monkeypatch):
    import backend.services.premium_futures_tv_webhook as m

    monkeypatch.setattr(
        m,
        "session_tv_hit_map",
        lambda d: {
            ("RELIANCE", "bullish"): {
                "scan_count": 1,
                "first_hit_at": None,
                "last_hit_at": None,
                "future_symbol": "RELIANCE FUT",
                "instrument_key": "NSE_FO|x",
                "alert_candle_high": 1400.0,
                "alert_candle_low": 1390.0,
            }
        },
    )
    bull, bear = merge_tv_picks_into_workspace(
        [{"underlying": "TCS"}, {"underlying": "RELIANCE", "direction_type": "LONG"}],
        [{"underlying": "INFY"}],
        date(2026, 9, 9),
    )
    rel = [x for x in bull if x["underlying"] == "RELIANCE"][0]
    assert rel["tv_featured"] is True
    assert rel["tv_webhook"] is True
    assert rel["scan_count"] == 1
    assert rel["target_entry_price"] == 1400.0
    assert [x["underlying"] for x in bull] == ["TCS", "RELIANCE"]
    assert [x["underlying"] for x in bear] == ["INFY"]


def test_repeat_tv_scan_count_from_hit_map(monkeypatch):
    import backend.services.premium_futures_tv_webhook as m

    monkeypatch.setattr(
        m,
        "session_tv_hit_map",
        lambda d: {
            ("PFC", "bullish"): {
                "scan_count": 2,
                "first_hit_at": None,
                "last_hit_at": None,
                "alert_candle_high": 100.0,
                "alert_candle_low": 99.0,
            }
        },
    )
    bull, _ = merge_tv_picks_into_workspace(
        [{"underlying": "PFC", "direction_type": "LONG", "ltp": 1}],
        [],
        date(2026, 9, 9),
    )
    assert bull[0]["scan_count"] == 2
    assert bull[0]["target_entry_price"] == 100.0

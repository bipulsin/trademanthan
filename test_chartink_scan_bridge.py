"""ChartInk Daily Futures → Scan bridge."""
from backend.services.chartink_scan_bridge import chartink_payload_for_scan


def test_chartink_payload_for_scan_from_dict():
    inner = {
        "stocks": "RELIANCE,TCS",
        "trigger_prices": "2500,3800",
        "triggered_at": "10:30 am",
        "scan_name": "Bullish Breakout",
        "alert_name": "Alert for Bullish",
    }
    out = chartink_payload_for_scan(inner, direction="bullish")
    assert out["stocks"] == "RELIANCE,TCS"
    assert "Bullish" in out["scan_name"] or out.get("alert_name")


def test_chartink_payload_csv_string():
    out = chartink_payload_for_scan("HDFCBANK,ICICI", direction="bearish")
    assert out["stocks"] == "HDFCBANK,ICICI"

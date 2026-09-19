#!/usr/bin/env python3
"""
Item 10: Does Upstox offer historical/expired-contract candles for MCX options?

Reports only what documentation or a test call proves.
"""
from __future__ import annotations

import importlib.util
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.parse import quote

import requests

_P0 = Path(__file__).resolve().parent / "tarang_phase0_upstox_mcx_greeks.py"
_spec = importlib.util.spec_from_file_location("tarang_p0_greeks", _P0)
_p0 = importlib.util.module_from_spec(_spec)
assert _spec.loader
_spec.loader.exec_module(_p0)
get_headers = _p0.get_headers
_load_token = _p0._load_token
_token_expiry_meta = _p0._token_expiry_meta

EXPIRIES_URL = "https://api.upstox.com/v2/expired-instruments/expiries"
CONTRACTS_URL = "https://api.upstox.com/v2/expired-instruments/option/contract"
CANDLE_TMPL = (
    "https://api.upstox.com/v2/expired-instruments/historical-candle/"
    "{key}/{interval}/{to_date}/{from_date}"
)
HIST_TMPL = "https://api.upstox.com/v2/historical-candle/{key}/{interval}/{to_date}/{from_date}"

# MCX crude futures-style underlying keys used elsewhere in this repo.
MCX_KEYS = [
    "MCX_FO|CRUDEOILM",
    "MCX_FO|CRUDEOIL",
    "MCX_FO|NATGASMINI",
    "MCX_FO|NATURALGAS",
    "MCX_INDEX|CRUDEOIL",
]
NSE_CONTROL = "NSE_INDEX|Nifty 50"


def _get(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    try:
        r = requests.get(url, headers=headers, timeout=30)
        try:
            body = r.json()
        except Exception:
            body = {"raw": (r.text or "")[:800]}
        return {"http_status": r.status_code, "body": body, "url": url}
    except requests.RequestException as e:
        return {"http_status": None, "error": str(e), "url": url}


def main() -> int:
    token = _load_token()
    headers = get_headers(token)
    token_meta = _token_expiry_meta(token)
    now = datetime.now(timezone.utc)
    out: Dict[str, Any] = {
        "ran_at_utc": now.isoformat(),
        "token_meta": token_meta,
        "docs": {
            "get_expiries": "https://upstox.com/developer/api-documentation/get-expiries/",
            "expired_candles": "https://upstox.com/developer/api-documentation/get-expired-historical-candle-data/",
            "plus_plan": "https://upstox.com/plus/",
            "doc_quotes": [
                "Expiries is currently not available for the MCX. (Get Expiries API)",
                "UDAPI1149: This API is available exclusively with an Upstox Plus plan subscription.",
                "Expired-candle examples in the docs are NSE_FO / NSE_INDEX, not MCX_FO.",
            ],
        },
        "calls": {},
    }

    nse = _get(f"{EXPIRIES_URL}?instrument_key={quote(NSE_CONTROL, safe='')}", headers)
    nse_body = nse.get("body") or {}
    nse_errors = nse_body.get("errors") or nse_body.get("error") or nse_body
    out["calls"]["nse_nifty_expiries"] = {
        "http_status": nse.get("http_status"),
        "status": nse_body.get("status") if isinstance(nse_body, dict) else None,
        "n_expiries": len(nse_body.get("data") or []) if isinstance(nse_body, dict) else None,
        "error_codes": [
            (e.get("errorCode") or e.get("code"))
            for e in (nse_body.get("errors") or [])
            if isinstance(e, dict)
        ]
        if isinstance(nse_body, dict)
        else None,
        "body_excerpt": json.dumps(nse_body, default=str)[:600],
    }

    mcx_calls = []
    for key in MCX_KEYS:
        r = _get(f"{EXPIRIES_URL}?instrument_key={quote(key, safe='')}", headers)
        body = r.get("body") or {}
        codes = []
        if isinstance(body, dict):
            for e in body.get("errors") or []:
                if isinstance(e, dict):
                    codes.append(e.get("errorCode") or e.get("code") or e.get("message"))
        mcx_calls.append(
            {
                "instrument_key": key,
                "http_status": r.get("http_status"),
                "status": body.get("status") if isinstance(body, dict) else None,
                "n_expiries": len(body.get("data") or []) if isinstance(body, dict) else None,
                "error_codes": codes,
                "body_excerpt": json.dumps(body, default=str)[:500],
            }
        )
    out["calls"]["mcx_expiries"] = mcx_calls

    # Also try expired option contracts + a dummy MCX candle key
    sample_contract = _get(
        f"{CONTRACTS_URL}?instrument_key={quote('MCX_FO|CRUDEOILM', safe='')}&expiry_date=2026-08-19",
        headers,
    )
    out["calls"]["mcx_expired_option_contract"] = {
        "http_status": sample_contract.get("http_status"),
        "body_excerpt": json.dumps(sample_contract.get("body"), default=str)[:600],
    }
    sample_candle = _get(
        CANDLE_TMPL.format(
            key=quote("MCX_FO|CRUDEOILM", safe=""),
            interval="day",
            to_date="2026-08-19",
            from_date="2026-08-01",
        ),
        headers,
    )
    out["calls"]["mcx_expired_candle"] = {
        "http_status": sample_candle.get("http_status"),
        "body_excerpt": json.dumps(sample_candle.get("body"), default=str)[:600],
    }

    plus = False
    codes_all = []
    for c in [out["calls"]["nse_nifty_expiries"], *mcx_calls]:
        codes_all.extend(c.get("error_codes") or [])
    plus = "UDAPI1149" in codes_all
    mcx_any_data = any((c.get("n_expiries") or 0) > 0 for c in mcx_calls)
    out["conclusion"] = {
        "plus_plan_required": plus or None,
        "mcx_expiries_returned_data": mcx_any_data,
        "docs_say_mcx_expiries_unavailable": True,
        "nse_control_ok": (out["calls"]["nse_nifty_expiries"].get("http_status") == 200)
        and (out["calls"]["nse_nifty_expiries"].get("n_expiries") or 0) > 0,
    }
    path = Path(os.getenv("TARANG_SPIKE_OUT", "docs/_spike_upstox_expired_mcx.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"conclusion": out["conclusion"], "out": str(path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

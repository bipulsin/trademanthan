#!/usr/bin/env python3
"""
Tarang Phase 0 spike: Delta Exchange India public products/tickers for BTC/ETH options.

Uses https://api.india.delta.exchange/v2 (no API key required for public market data).
Writes a JSON summary suitable for docs/upstox-mcx-findings.md companion docs.

Usage:
  python3 scripts/tarang_phase0_delta_india_proof.py
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

try:
    import requests
except ImportError:  # pragma: no cover
    raise SystemExit("requests required: pip install requests")

BASE = os.getenv("DELTA_INDIA_BASE", "https://api.india.delta.exchange/v2").rstrip("/")
HEADERS = {"Accept": "application/json", "User-Agent": "TarangPhase0/1.0"}


def get(path: str, **params: Any) -> Dict[str, Any]:
    url = f"{BASE}{path}"
    r = requests.get(url, params=params or None, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def product_summary(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": p.get("symbol"),
        "id": p.get("id"),
        "contract_type": p.get("contract_type"),
        "contract_value": p.get("contract_value"),
        "strike_price": p.get("strike_price"),
        "settlement_time": p.get("settlement_time"),
        "trading_status": p.get("trading_status"),
        "state": p.get("state"),
        "tick_size": p.get("tick_size"),
        "lot_size": p.get("lot_size"),
        "notional_type": p.get("notional_type"),
        "underlying_symbol": (p.get("underlying_asset") or {}).get("symbol"),
        "quoting_symbol": (p.get("quoting_asset") or {}).get("symbol"),
        "short_description": p.get("short_description"),
    }


def main() -> int:
    findings: Dict[str, Any] = {
        "ran_at_utc": datetime.now(timezone.utc).isoformat(),
        "base": BASE,
        "products": {},
        "tickers": {},
        "notes": [
            "Public REST only; authenticated order APIs not exercised in Phase 0.",
            "Expiries must come from live products (settlement_time), never hard-coded.",
            "Testnet base (not called here): https://cdn-ind.testnet.deltaex.org",
        ],
    }

    for asset in ("BTC", "ETH"):
        for ctype in ("call_options", "put_options"):
            d = get(
                "/products",
                contract_types=ctype,
                underlying_asset_symbols=asset,
                states="live",
                page_size=50,
            )
            res: List[Dict[str, Any]] = d.get("result") or []
            key = f"{asset}_{ctype}"
            findings["products"][key] = {
                "count_page": len(res),
                "success": d.get("success"),
                "symbol_samples": [x.get("symbol") for x in res[:12]],
                "settlement_times": sorted(
                    {x.get("settlement_time") for x in res if x.get("settlement_time")}
                )[:10],
                "contract_values": sorted({str(x.get("contract_value")) for x in res})[:10],
                "sample": product_summary(res[0]) if res else None,
            }

        d = get(
            "/tickers",
            contract_types="call_options",
            underlying_asset_symbols=asset,
            page_size=5,
        )
        tres: List[Dict[str, Any]] = d.get("result") or []
        entry: Dict[str, Any] = {"count_page": len(tres)}
        if tres:
            t = tres[0]
            entry["ticker_keys"] = sorted(t.keys())
            entry["sample"] = {
                "symbol": t.get("symbol"),
                "product_id": t.get("product_id"),
                "mark_price": t.get("mark_price"),
                "mark_vol": t.get("mark_vol"),
                "spot_price": t.get("spot_price"),
                "strike_price": t.get("strike_price"),
                "contract_type": t.get("contract_type"),
                "greeks": t.get("greeks"),
                "quotes": t.get("quotes"),
                "settlement_time": t.get("settlement_time") or t.get("expiration_time"),
            }
            sym = t.get("symbol")
            if sym:
                one = get(f"/tickers/{sym}")
                result = one.get("result")
                if isinstance(result, dict):
                    entry["single_ticker_keys"] = sorted(result.keys())
                    entry["single_greeks"] = result.get("greeks")
                    entry["single_mark_vol"] = result.get("mark_vol")
        findings["tickers"][asset] = entry

    btc_syms = findings["products"].get("BTC_call_options", {}).get("symbol_samples") or []
    eth_syms = findings["products"].get("ETH_call_options", {}).get("symbol_samples") or []
    findings["symbol_shape_observations"] = {
        "btc_call_examples": btc_syms[:5],
        "eth_call_examples": eth_syms[:5],
        "contract_value_btc": findings["products"]
        .get("BTC_call_options", {})
        .get("contract_values"),
        "contract_value_eth": findings["products"]
        .get("ETH_call_options", {})
        .get("contract_values"),
        "expiry_field": "settlement_time (ISO8601 UTC on products; tickers may echo)",
    }

    out = Path(
        os.getenv(
            "TARANG_DELTA_OUT",
            str(Path(__file__).resolve().parents[1] / "docs" / "_spike_delta_india_raw.json"),
        )
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(findings, indent=2, default=str), encoding="utf-8")
    print(json.dumps(findings, indent=2, default=str)[:6000])
    print(f"\nWrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

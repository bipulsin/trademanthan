#!/usr/bin/env python3
"""
Probe Delta India option-candle history (item 9).

Tries ≥10 expired BTC/ETH option symbols across several expiries.
Reports bar count, earliest date, granularity, and gaps.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

BASE = os.getenv("DELTA_INDIA_BASE", "https://api.india.delta.exchange").rstrip("/")
HEADERS = {"Accept": "application/json", "User-Agent": "TradeManthan-KosmicTarang/probe"}
RESOLUTIONS = ["1m", "5m", "15m", "30m", "1h", "1d", "1D", "60", "15", "5", "1"]


def _host() -> str:
    b = BASE[:-3] if BASE.endswith("/v2") else BASE
    return b


def get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{_host()}{path if path.startswith('/') else '/' + path}"
    if not path.startswith("/v2"):
        url = f"{_host()}/v2{path if path.startswith('/') else '/' + path}"
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        try:
            body = r.json()
        except Exception:
            body = {"raw": (r.text or "")[:500]}
        return {"http_status": r.status_code, "body": body, "url": r.url}
    except requests.RequestException as e:
        return {"http_status": None, "error": str(e)}


def _parse_exp(sym: str) -> Optional[str]:
    parts = str(sym).split("-")
    if len(parts) < 4:
        return None
    try:
        return datetime.strptime(parts[3], "%d%m%y").date().isoformat()
    except ValueError:
        return None


def _fridays_back(n: int = 8) -> List[str]:
    """Past Friday expiry codes DDMMYY (Delta weekly)."""
    d = datetime.now(timezone.utc).date()
    out = []
    while len(out) < n:
        d = d - timedelta(days=1)
        if d.weekday() == 4:
            out.append(d.strftime("%d%m%y"))
    return out


def _gap_count(times: List[datetime], expected_sec: Optional[float]) -> int:
    if len(times) < 2 or not expected_sec:
        return 0
    gaps = 0
    ordered = sorted(times)
    for a, b in zip(ordered, ordered[1:]):
        dt = (b - a).total_seconds()
        if dt > expected_sec * 1.6:
            gaps += 1
    return gaps


def main() -> int:
    now = datetime.now(timezone.utc)
    findings: Dict[str, Any] = {
        "ran_at_utc": now.isoformat(),
        "base": _host(),
        "symbols": [],
        "notes": [],
    }

    expired_btc = get(
        "/products",
        {"contract_types": "call_options,put_options", "underlying_asset_symbols": "BTC", "states": "expired", "page_size": 50},
    )
    expired_eth = get(
        "/products",
        {"contract_types": "call_options,put_options", "underlying_asset_symbols": "ETH", "states": "expired", "page_size": 50},
    )
    findings["expired_products_btc"] = {
        "http": expired_btc.get("http_status"),
        "success": (expired_btc.get("body") or {}).get("success"),
        "n": len((expired_btc.get("body") or {}).get("result") or []) if isinstance((expired_btc.get("body") or {}).get("result"), list) else None,
        "error": (expired_btc.get("body") or {}).get("error"),
    }
    findings["expired_products_eth"] = {
        "http": expired_eth.get("http_status"),
        "success": (expired_eth.get("body") or {}).get("success"),
        "n": len((expired_eth.get("body") or {}).get("result") or []) if isinstance((expired_eth.get("body") or {}).get("result"), list) else None,
    }

    symbols: List[str] = []
    for blob in (expired_btc, expired_eth):
        result = (blob.get("body") or {}).get("result") or []
        if isinstance(result, list):
            for p in result:
                if isinstance(p, dict) and p.get("symbol"):
                    symbols.append(str(p["symbol"]))

    # Construct typical weekly symbols if the expired-products list is thin
    fridays = _fridays_back(6)
    btc_strikes = [95000, 100000, 105000, 110000]
    eth_strikes = [3500, 4000, 4200, 4500]
    constructed = []
    for code in fridays:
        for k in btc_strikes[:2]:
            constructed.append(f"C-BTC-{k}-{code}")
            constructed.append(f"P-BTC-{k}-{code}")
        for k in eth_strikes[:2]:
            constructed.append(f"C-ETH-{k}-{code}")
            constructed.append(f"P-ETH-{k}-{code}")
    # Prefer API-listed expired symbols, then fill with constructed
    seen = set()
    ordered: List[str] = []
    for s in symbols + constructed:
        if s not in seen:
            seen.add(s)
            ordered.append(s)
    probe_list = ordered[:16]
    findings["symbols_considered"] = probe_list

    end = int(now.timestamp())
    start = int((now - timedelta(days=120)).timestamp())
    per_symbol: List[Dict[str, Any]] = []

    for sym in probe_list:
        row: Dict[str, Any] = {"symbol": sym, "expiry": _parse_exp(sym), "resolutions": {}}
        best = None
        for res in ("1d", "30m", "15m", "5m", "1h", "1m"):
            resp = get(
                "/history/candles",
                {"symbol": sym, "resolution": res, "start": start, "end": end},
            )
            body = resp.get("body") or {}
            result = body.get("result")
            n = len(result) if isinstance(result, list) else 0
            entry = {
                "http": resp.get("http_status"),
                "n": n,
                "success": body.get("success"),
                "error": body.get("error") or body.get("message"),
            }
            if isinstance(result, list) and result:
                times = []
                for bar in result:
                    ts = None
                    if isinstance(bar, dict):
                        ts = bar.get("time") or bar.get("timestamp")
                    elif isinstance(bar, (list, tuple)) and bar:
                        ts = bar[0]
                    if ts is None:
                        continue
                    try:
                        ts_i = int(ts)
                        if ts_i > 10_000_000_000:
                            ts_i //= 1000
                        times.append(datetime.fromtimestamp(ts_i, tz=timezone.utc))
                    except (TypeError, ValueError, OSError):
                        continue
                if times:
                    expected = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "1d": 86400}.get(res)
                    entry["earliest"] = min(times).isoformat()
                    entry["latest"] = max(times).isoformat()
                    entry["gaps"] = _gap_count(times, expected)
                    entry["sample_bar"] = result[0]
                    if best is None or n > best.get("n", 0):
                        best = {"resolution": res, **entry}
            row["resolutions"][res] = {k: v for k, v in entry.items() if k != "sample_bar"}
            if n > 0:
                break  # found a working granularity; still record this one
        row["best"] = best
        per_symbol.append(row)
        time.sleep(0.15)

    findings["per_symbol"] = per_symbol
    with_bars = [s for s in per_symbol if (s.get("best") or {}).get("n", 0) > 0]
    findings["symbols_with_bars"] = len(with_bars)
    findings["symbols_tried"] = len(per_symbol)
    if with_bars:
        findings["reconstruct_from_candles"] = "PARTIAL" if len(with_bars) < 10 else "VIABLE_IF_CHAIN_COMPLETE"
        findings["reconstruct_note"] = (
            "Candle history exists for some expired option symbols, but a full chain "
            "(~10 strikes either side, both types, every snapshot) is not guaranteed. "
            "Own full-chain snapshots remain required for the backtest."
        )
    else:
        findings["reconstruct_from_candles"] = "NOT_VIABLE"
        findings["reconstruct_note"] = (
            "No expired option-symbol candles returned. Do not plan to reconstruct crypto "
            "chains from Delta history/candles."
        )

    out = Path(os.getenv("TARANG_SPIKE_OUT", "docs/_spike_delta_option_candles.json"))
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(findings, indent=2, default=str), encoding="utf-8")
    print(
        json.dumps(
            {
                "tried": findings["symbols_tried"],
                "with_bars": findings["symbols_with_bars"],
                "verdict": findings["reconstruct_from_candles"],
                "out": str(out),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

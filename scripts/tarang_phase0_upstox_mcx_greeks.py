#!/usr/bin/env python3
"""
Tarang Phase 0 spike: Upstox Option Greek (v3) for a few MCX CL/NG option keys.

Does NOT hard-code API keys. Token resolution order:
  1. UPSTOX_ACCESS_TOKEN env
  2. UPSTOX_TOKEN_FILE env (JSON with access_token)
  3. Common deploy paths (paperclip / local)

Instrument keys are resolved from the public Upstox complete instrument master
(chain endpoint is unavailable for MCX — same approach Tarang will use).

Usage:
  UPSTOX_ACCESS_TOKEN=... python3 scripts/tarang_phase0_upstox_mcx_greeks.py
  # or on paperclip with token file present:
  python3 scripts/tarang_phase0_upstox_mcx_greeks.py
"""
from __future__ import annotations

import gzip
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:  # pragma: no cover
    raise SystemExit("requests required: pip install requests")

COMPLETE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
GREEK_URL = "https://api.upstox.com/v3/market-quote/option-greek"
CACHE_PATH = Path(os.getenv("UPSTOX_MASTER_CACHE", "/tmp/upstox_complete.json"))
# Cloudflare blocks bare Python-urllib UA; match app-style Accept + a normal UA.
HTTP_HEADERS_BASE = {
    "Accept": "application/json",
    "User-Agent": "TradeManthan-TarangPhase0/1.0",
}

TOKEN_CANDIDATES = [
    os.getenv("UPSTOX_TOKEN_FILE") or "",
    "/home/ubuntu/twcto/data/upstox_token.json",
    "/home/ubuntu/trademanthan/data/upstox_token.json",
    str(Path(__file__).resolve().parents[1] / "data" / "upstox_token.json"),
]


def _load_token() -> str:
    env = (os.getenv("UPSTOX_ACCESS_TOKEN") or "").strip()
    if env:
        return env
    for p in TOKEN_CANDIDATES:
        if not p:
            continue
        path = Path(p)
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            tok = (data.get("access_token") or "").strip()
            if tok:
                return tok
        except Exception as exc:  # noqa: BLE001 — spike: report and continue
            print(f"warn: could not read {path}: {exc}", file=sys.stderr)
    raise SystemExit(
        "No Upstox access token. Set UPSTOX_ACCESS_TOKEN or place upstox_token.json."
    )


def _load_master(force: bool = False) -> List[Dict[str, Any]]:
    if not force and CACHE_PATH.is_file() and CACHE_PATH.stat().st_size > 1000:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    print(f"Downloading instrument master → {CACHE_PATH} …")
    resp = requests.get(COMPLETE_URL, headers=HTTP_HEADERS_BASE, timeout=120)
    resp.raise_for_status()
    raw = resp.content
    if raw[:2] == b"\x1f\x8b":
        text = gzip.decompress(raw).decode("utf-8")
    else:
        text = raw.decode("utf-8")
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(text, encoding="utf-8")
    return json.loads(text)


def _token_expiry_meta(token: str) -> Dict[str, Any]:
    """Best-effort JWT exp check (no secret validation)."""
    import base64
    import time

    meta: Dict[str, Any] = {"token_len": len(token)}
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return meta
        pad = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad))
        exp = payload.get("exp")
        now = int(time.time())
        meta["jwt_exp"] = exp
        meta["now_unix"] = now
        if isinstance(exp, (int, float)):
            meta["token_expired"] = int(exp) < now
    except Exception as exc:  # noqa: BLE001
        meta["jwt_parse_error"] = str(exc)
    return meta


def _underlying_bucket(row: Dict[str, Any]) -> Optional[str]:
    blob = " ".join(
        str(row.get(k) or "")
        for k in ("name", "underlying_symbol", "trading_symbol", "short_name")
    ).upper()
    if "CRUDEOIL" in blob or "CRUDE OIL" in blob:
        return "CL"
    if "NATURALGAS" in blob or "NATURAL GAS" in blob or "NATGAS" in blob:
        return "NG"
    return None


def _pick_mcx_options(rows: List[Dict[str, Any]], per_bucket: int = 3) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {"CL": [], "NG": []}
    for r in rows:
        seg = str(r.get("segment") or "").upper()
        itype = str(r.get("instrument_type") or "").upper()
        if "MCX" not in seg or itype not in ("CE", "PE"):
            continue
        bucket = _underlying_bucket(r)
        if not bucket or len(buckets[bucket]) >= per_bucket:
            continue
        if not r.get("instrument_key"):
            continue
        buckets[bucket].append(r)
        if all(len(v) >= per_bucket for v in buckets.values()):
            break
    out: List[Dict[str, Any]] = []
    for b in ("CL", "NG"):
        out.extend(buckets[b])
    return out


def _fetch_greeks(token: str, keys: List[str]) -> Dict[str, Any]:
    # Up to 50 keys; spike uses a handful.
    headers = {**HTTP_HEADERS_BASE, "Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(
            GREEK_URL,
            params={"instrument_key": ",".join(keys)},
            headers=headers,
            timeout=45,
        )
        try:
            body: Any = resp.json()
        except Exception:  # noqa: BLE001
            body = {"raw": (resp.text or "")[:2000]}
        out: Dict[str, Any] = {"http_status": resp.status_code, "body": body}
        if resp.status_code >= 400:
            out["error"] = f"HTTP {resp.status_code}"
        return out
    except requests.RequestException as e:
        return {"http_status": None, "body": {}, "error": str(e)}


def main() -> int:
    token = _load_token()
    token_meta = _token_expiry_meta(token)
    rows = _load_master()
    picked = _pick_mcx_options(rows, per_bucket=3)
    summary: Dict[str, Any] = {
        "ran_at_utc": datetime.now(timezone.utc).isoformat(),
        "token_meta": token_meta,
        "master_rows": len(rows),
        "picked_count": len(picked),
        "picked": [
            {
                "bucket": _underlying_bucket(r),
                "instrument_key": r.get("instrument_key"),
                "trading_symbol": r.get("trading_symbol"),
                "instrument_type": r.get("instrument_type"),
                "expiry": r.get("expiry"),
                "strike_price": r.get("strike_price"),
                "lot_size": r.get("lot_size"),
                "segment": r.get("segment"),
            }
            for r in picked
        ],
    }
    if not picked:
        summary["verdict"] = "FAIL_NO_MCX_OPTIONS_IN_MASTER"
        print(json.dumps(summary, indent=2, default=str))
        return 2

    keys = [str(r["instrument_key"]) for r in picked]
    greek = _fetch_greeks(token, keys)
    summary["greek_http_status"] = greek.get("http_status")
    body = greek.get("body") or {}
    data = body.get("data") if isinstance(body, dict) else None
    summary["greek_status"] = body.get("status") if isinstance(body, dict) else None
    summary["greek_error"] = greek.get("error")
    if isinstance(body, dict) and "errors" in body:
        summary["greek_errors"] = body.get("errors")

    per_key: List[Dict[str, Any]] = []
    non_null_iv = 0
    non_null_delta = 0
    if isinstance(data, dict):
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            iv = v.get("iv")
            delta = v.get("delta")
            if iv is not None:
                non_null_iv += 1
            if delta is not None:
                non_null_delta += 1
            per_key.append(
                {
                    "response_key": k,
                    "instrument_token": v.get("instrument_token"),
                    "last_price": v.get("last_price"),
                    "iv": iv,
                    "delta": delta,
                    "gamma": v.get("gamma"),
                    "theta": v.get("theta"),
                    "vega": v.get("vega"),
                    "oi": v.get("oi"),
                    "volume": v.get("volume"),
                }
            )
    summary["per_key"] = per_key
    summary["non_null_iv_count"] = non_null_iv
    summary["non_null_delta_count"] = non_null_delta
    summary["iv_greeks_non_null"] = non_null_iv > 0 and non_null_delta > 0
    if summary["iv_greeks_non_null"]:
        summary["verdict"] = "PASS_IV_AND_GREEKS_PRESENT"
    elif token_meta.get("token_expired"):
        summary["verdict"] = "BLOCKED_EXPIRED_UPSTOX_TOKEN"
    elif summary.get("greek_http_status") in (401, 403):
        summary["verdict"] = f"BLOCKED_HTTP_{summary.get('greek_http_status')}"
    elif isinstance(data, dict) and data:
        summary["verdict"] = "PARTIAL_RESPONSE_BUT_NULLS"
    else:
        summary["verdict"] = "FAIL_NO_USABLE_GREEK_DATA"

    # Prefer repo docs/ when script lives under scripts/; else /tmp (e.g. scp'd spike).
    _script = Path(__file__).resolve()
    _default_out = (
        _script.parents[1] / "docs" / "_spike_upstox_mcx_raw.json"
        if _script.parent.name == "scripts"
        else Path("/tmp/_spike_upstox_mcx_raw.json")
    )
    out_path = Path(os.getenv("TARANG_SPIKE_OUT", str(_default_out)))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    print(f"\nWrote {out_path}", file=sys.stderr)
    return 0 if summary.get("iv_greeks_non_null") else 1


if __name__ == "__main__":
    raise SystemExit(main())

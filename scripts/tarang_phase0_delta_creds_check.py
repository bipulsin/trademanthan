#!/usr/bin/env python3
"""
Kosmic Tarang Phase 0 follow-up: read-only Delta India credential check.

Uses existing delta_api signing pattern. Credentials from (in order):
  DELTA_API_KEY / DELTA_API_SECRET / DELTA_API_URL env
  or DELTA_EXCHANGE_API_KEY / DELTA_EXCHANGE_API_SECRET / DELTA_EXCHANGE_API_URL
  or optional --from-algo-defaults (loads constants from backend/routers/algo.py —
    only for this Phase 0 verification; do not invent keys).

Read-only calls only: GET /v2/wallet/balances, GET /v2/positions.
Never places orders.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

try:
    import requests
except ImportError:  # pragma: no cover
    raise SystemExit("requests required")

DEFAULT_BASE = "https://api.india.delta.exchange"


def _load_from_algo_py() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    algo = Path(__file__).resolve().parents[1] / "backend" / "routers" / "algo.py"
    if not algo.is_file():
        return None, None, None
    text = algo.read_text(encoding="utf-8")
    url = key = secret = None
    m = re.search(r"DELTA_EXCHANGE_API_URL\s*=\s*'([^']+)'", text)
    if m:
        url = m.group(1)
    m = re.search(r"DELTA_EXCHANGE_API_KEY\s*=\s*'([^']+)'", text)
    if m:
        key = m.group(1)
    m = re.search(r"DELTA_EXCHANGE_API_SECRET\s*=\s*'([^']+)'", text)
    if m:
        secret = m.group(1)
    return key, secret, url


def _creds() -> Tuple[str, str, str, str]:
    key = (
        os.getenv("DELTA_API_KEY")
        or os.getenv("DELTA_EXCHANGE_API_KEY")
        or ""
    ).strip()
    secret = (
        os.getenv("DELTA_API_SECRET")
        or os.getenv("DELTA_EXCHANGE_API_SECRET")
        or ""
    ).strip()
    url = (
        os.getenv("DELTA_API_URL")
        or os.getenv("DELTA_EXCHANGE_API_URL")
        or DEFAULT_BASE
    ).strip().rstrip("/")
    source = "env"
    if (not key or not secret) and (
        "--from-algo-defaults" in sys.argv or os.getenv("TARANG_DELTA_USE_ALGO_DEFAULTS") == "1"
    ):
        k2, s2, u2 = _load_from_algo_py()
        if k2 and s2:
            key, secret = k2, s2
            if u2:
                url = u2.rstrip("/")
            source = "backend/routers/algo.py constants"
    if not key or not secret:
        raise SystemExit(
            "No Delta credentials. Set DELTA_API_KEY/DELTA_API_SECRET "
            "or re-run with --from-algo-defaults / TARANG_DELTA_USE_ALGO_DEFAULTS=1"
        )
    return key, secret, url, source


def _sign(secret: str, method: str, path: str, body: str = "") -> Dict[str, str]:
    timestamp = str(int(time.time()))
    message = method + timestamp + path + body
    signature = hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()
    return {
        "api-key": "",  # filled by caller
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "KosmicTarang-Phase0/1.0",
    }


def _auth_get(base: str, key: str, secret: str, path: str) -> Dict[str, Any]:
    headers = _sign(secret, "GET", path)
    headers["api-key"] = key
    url = f"{base}{path}"
    try:
        r = requests.get(url, headers=headers, timeout=30)
        try:
            body = r.json()
        except Exception:  # noqa: BLE001
            body = {"raw": (r.text or "")[:1500]}
        return {"http_status": r.status_code, "body": body, "url": url}
    except requests.RequestException as e:
        return {"http_status": None, "body": {}, "error": str(e), "url": url}


def _redact_balances(body: Any) -> Any:
    """Keep structure / asset ids; scrub large numeric detail if needed — keep balances
    (not secrets) for permission proof."""
    return body


def main() -> int:
    key, secret, base, source = _creds()
    report: Dict[str, Any] = {
        "ran_at_utc": datetime.now(timezone.utc).isoformat(),
        "product": "Kosmic Tarang",
        "base": base,
        "credential_source": source,
        "api_key_fingerprint": f"{key[:4]}…{key[-4:]} (len={len(key)})",
        "calls": {},
        "notes": [
            "Read-only: wallet/balances + positions only.",
            "Market data + paper: India prod public endpoints + PaperBroker (Phase 1+).",
            "Testnet only Phase 4. Client order ids will prefix tarang-.",
        ],
    }

    bal = _auth_get(base, key, secret, "/v2/wallet/balances")
    pos = _auth_get(base, key, secret, "/v2/positions")
    # Also try margined positions (sometimes separate permission)
    pos_m = _auth_get(base, key, secret, "/v2/positions/margined")

    report["calls"]["wallet_balances"] = {
        "http_status": bal.get("http_status"),
        "error": bal.get("error"),
        "success": (bal.get("body") or {}).get("success") if isinstance(bal.get("body"), dict) else None,
        "body_summary": _summarize_balances(bal.get("body")),
        "error_message": _err_msg(bal.get("body")),
    }
    report["calls"]["positions"] = {
        "http_status": pos.get("http_status"),
        "error": pos.get("error"),
        "success": (pos.get("body") or {}).get("success") if isinstance(pos.get("body"), dict) else None,
        "body_summary": _summarize_positions(pos.get("body")),
        "error_message": _err_msg(pos.get("body")),
    }
    report["calls"]["positions_margined"] = {
        "http_status": pos_m.get("http_status"),
        "success": (pos_m.get("body") or {}).get("success") if isinstance(pos_m.get("body"), dict) else None,
        "error_message": _err_msg(pos_m.get("body")),
    }

    bal_ok = report["calls"]["wallet_balances"].get("http_status") == 200 and report["calls"][
        "wallet_balances"
    ].get("success")
    pos_ok = report["calls"]["positions"].get("http_status") == 200 and report["calls"][
        "positions"
    ].get("success")

    bal_err = report["calls"]["wallet_balances"].get("error_message") or ""
    pos_err = report["calls"]["positions"].get("error_message") or ""
    ip_blocked = "ip_not_whitelisted" in f"{bal_err} {pos_err}"

    if bal_ok and pos_ok:
        report["verdict"] = "PASS_READ_OK"
        report["permissions_inferred"] = {
            "read_balances": True,
            "read_positions": True,
            "trade": "UNKNOWN_NOT_TESTED (no order calls by design)",
        }
    elif ip_blocked:
        report["verdict"] = "FAIL_IP_NOT_WHITELISTED"
        report["permissions_inferred"] = {
            "read_balances": False,
            "read_positions": False,
            "trade": "UNKNOWN",
            "note": "Keys authenticate enough for Delta to return ip_not_whitelisted_for_api_key; whitelist paperclip 140.245.14.17 (and local if needed) or issue keys without IP lock.",
        }
        report["blocking"] = True
        report["ask_user"] = (
            "Existing Delta India keys reject both local and paperclip IPs "
            "(ip_not_whitelisted_for_api_key, client_ip includes 140.245.14.17). "
            "Please whitelist paperclip-vm IP and/or provide new India API keys "
            "with read balances/positions (trade later for Phase 4)."
        )
    elif bal.get("http_status") in (401, 403) or pos.get("http_status") in (401, 403):
        report["verdict"] = "FAIL_AUTH_OR_PERMISSIONS"
        report["permissions_inferred"] = {
            "read_balances": bool(bal_ok),
            "read_positions": bool(pos_ok),
            "trade": "UNKNOWN",
        }
        report["blocking"] = True
        report["ask_user"] = "Existing Delta keys failed auth/permissions. Please provide new India API keys with at least read balances/positions (trade later for Phase 4)."
    else:
        report["verdict"] = "FAIL_OR_PARTIAL"
        report["permissions_inferred"] = {
            "read_balances": bool(bal_ok),
            "read_positions": bool(pos_ok),
            "trade": "UNKNOWN",
        }
        report["blocking"] = True
        report["ask_user"] = "Delta read-only calls did not fully succeed. Need new/working India keys before Phase 1 live Delta adapter."

    out = Path(
        os.getenv(
            "TARANG_DELTA_CREDS_OUT",
            str(Path(__file__).resolve().parents[1] / "docs" / "_spike_delta_creds_raw.json"),
        )
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, indent=2, default=str))
    print(f"\nWrote {out}", file=sys.stderr)
    return 0 if report["verdict"] == "PASS_READ_OK" else 1


def _err_msg(body: Any) -> Optional[str]:
    if not isinstance(body, dict):
        return None
    err = body.get("error")
    if isinstance(err, dict):
        return str(err.get("message") or err.get("code") or err)
    if body.get("message"):
        return str(body.get("message"))
    return None


def _summarize_balances(body: Any) -> Dict[str, Any]:
    if not isinstance(body, dict):
        return {"ok": False}
    res = body.get("result")
    if not isinstance(res, list):
        return {"ok": False, "result_type": type(res).__name__}
    assets = []
    for b in res[:20]:
        if not isinstance(b, dict):
            continue
        assets.append(
            {
                "asset_id": b.get("asset_id") or b.get("asset_symbol") or b.get("currency"),
                "available_balance_present": b.get("available_balance") is not None,
                "balance_present": b.get("balance") is not None,
            }
        )
    return {"ok": True, "n_assets": len(res), "assets_sample": assets}


def _summarize_positions(body: Any) -> Dict[str, Any]:
    if not isinstance(body, dict):
        return {"ok": False}
    res = body.get("result")
    if res is None:
        return {"ok": True, "n_positions": 0}
    if isinstance(res, list):
        return {"ok": True, "n_positions": len(res)}
    if isinstance(res, dict):
        return {"ok": True, "result_keys": sorted(res.keys())[:20]}
    return {"ok": False, "result_type": type(res).__name__}


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Kosmic Tarang Phase 0 follow-up: Upstox MCX Option Greek spike (full CL/NG only).

Picks ATM CE/PE around futures LTP for the nearest two expiries of exact-name
CRUDEOIL and NATURALGAS (excludes CRUDEOILM / NATGASMINI). Cross-checks native
Greeks vs Black-76, reports 10–16Δ bid-ask % of mid, and estimates min max-loss
per lot for narrowest / profile widths.

Token resolution:
  1. UPSTOX_ACCESS_TOKEN
  2. UPSTOX_TOKEN_FILE / paperclip / local upstox_token.json

Uses UpstoxService.get_headers()-compatible headers (Accept + Bearer). Cloudflare
blocks bare urllib UAs — requests + Accept/UA required.
"""
from __future__ import annotations

import gzip
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

try:
    import requests
except ImportError:  # pragma: no cover
    raise SystemExit("requests required: pip install requests")

COMPLETE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
GREEK_URL = "https://api.upstox.com/v3/market-quote/option-greek"
QUOTES_URL = "https://api.upstox.com/v2/market-quote/quotes"
CACHE_PATH = Path(os.getenv("UPSTOX_MASTER_CACHE", "/tmp/upstox_complete.json"))

# Profile widths (strike steps) from Kosmic Tarang strategy doc §4.
PROFILE_WIDTH_STEPS = {"CL": (5, 8), "NG": (3, 5)}
ATM_STRIKE_PAIRS = 4  # CE+PE pairs nearest ATM per expiry
DELTA_BAND = (0.10, 0.16)
BLACK76_DELTA_ABS_TOL = 0.12  # flag if |native - model| exceeds this
BLACK76_IV_REL_TOL = 0.35  # flag if |native_iv - model_iv| / max(native,1e-6) exceeds

HTTP_HEADERS_BASE = {
    "Accept": "application/json",
    "User-Agent": "TradeManthan-KosmicTarangPhase0/1.1",
}

TOKEN_CANDIDATES = [
    os.getenv("UPSTOX_TOKEN_FILE") or "",
    "/home/ubuntu/twcto/data/upstox_token.json",
    "/home/ubuntu/trademanthan/data/upstox_token.json",
    str(Path(__file__).resolve().parents[1] / "data" / "upstox_token.json"),
]


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _black76_d1(F: float, K: float, T: float, sigma: float) -> Optional[float]:
    if F <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    return (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))


def black76_delta(F: float, K: float, T: float, sigma: float, opt: str) -> Optional[float]:
    """Futures-option Black-76 delta (r≈0 discount on forward measure). Put negative."""
    d1 = _black76_d1(F, K, T, sigma)
    if d1 is None:
        return None
    nd1 = _norm_cdf(d1)
    if opt.upper() == "CE":
        return nd1
    if opt.upper() == "PE":
        return nd1 - 1.0
    return None


def black76_price(F: float, K: float, T: float, sigma: float, opt: str) -> Optional[float]:
    d1 = _black76_d1(F, K, T, sigma)
    if d1 is None:
        return None
    d2 = d1 - sigma * math.sqrt(T)
    if opt.upper() == "CE":
        return F * _norm_cdf(d1) - K * _norm_cdf(d2)
    if opt.upper() == "PE":
        return K * _norm_cdf(-d2) - F * _norm_cdf(-d1)
    return None


def implied_vol_black76(
    price: float, F: float, K: float, T: float, opt: str, tol: float = 1e-5
) -> Optional[float]:
    """Bracket + bisection IV from mid/last price (annualized)."""
    if price is None or price <= 0 or F <= 0 or K <= 0 or T <= 0:
        return None
    lo, hi = 1e-4, 5.0
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        model = black76_price(F, K, T, mid, opt)
        if model is None:
            return None
        if abs(model - price) < tol:
            return mid
        if model > price:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)


def get_headers(token: str) -> Dict[str, str]:
    """Mirror UpstoxService.get_headers() + UA (Cloudflare)."""
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": HTTP_HEADERS_BASE["User-Agent"],
    }


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
        except Exception as exc:  # noqa: BLE001
            print(f"warn: could not read {path}: {exc}", file=sys.stderr)
    raise SystemExit(
        "No Upstox access token. Set UPSTOX_ACCESS_TOKEN or place upstox_token.json."
    )


def _load_master(force: bool = False) -> List[Dict[str, Any]]:
    if not force and CACHE_PATH.is_file() and CACHE_PATH.stat().st_size > 1000:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    print(f"Downloading instrument master → {CACHE_PATH} …", file=sys.stderr)
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


def _exact_family(row: Dict[str, Any]) -> Optional[str]:
    """Exact underlying_symbol on MCX_FO: CRUDEOIL / NATURALGAS (not mini).

    Note: Upstox ``name`` is often ``CRUDE OIL`` (spaced) and NATGASMINI rows
    also carry name NATURALGAS — always key off ``underlying_symbol``.
    """
    us = str(row.get("underlying_symbol") or "").strip().upper()
    if us == "CRUDEOIL":
        return "CL"
    if us == "NATURALGAS":
        return "NG"
    return None


def _is_mcx_fo(row: Dict[str, Any]) -> bool:
    seg = str(row.get("segment") or "").upper()
    return seg == "MCX_FO" or seg.startswith("MCX_FO")


def _expiry_ms(row: Dict[str, Any]) -> Optional[int]:
    exp = row.get("expiry")
    try:
        return int(exp) if exp is not None else None
    except (TypeError, ValueError):
        return None


def _strike_step(strikes: List[float]) -> Optional[float]:
    uniq = sorted({float(s) for s in strikes if s is not None})
    diffs = [round(uniq[i + 1] - uniq[i], 6) for i in range(len(uniq) - 1) if uniq[i + 1] > uniq[i]]
    if not diffs:
        return None
    # modal smallest positive diff
    from collections import Counter

    c = Counter(diffs)
    return c.most_common(1)[0][0]


def _years_to_expiry(expiry_ms: int, now: Optional[datetime] = None) -> float:
    now = now or datetime.now(timezone.utc)
    exp = datetime.fromtimestamp(expiry_ms / 1000.0, tz=timezone.utc)
    return max((exp - now).total_seconds() / (365.25 * 24 * 3600), 1e-6)


def _fetch_json(url: str, headers: Dict[str, str], params: Optional[Dict] = None) -> Dict[str, Any]:
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=45)
        try:
            body: Any = resp.json()
        except Exception:  # noqa: BLE001
            body = {"raw": (resp.text or "")[:2000]}
        return {"http_status": resp.status_code, "body": body}
    except requests.RequestException as e:
        return {"http_status": None, "body": {}, "error": str(e)}


def _fetch_greeks(token: str, keys: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"chunks": [], "merged": {}}
    headers = get_headers(token)
    for i in range(0, len(keys), 50):
        chunk = keys[i : i + 50]
        r = _fetch_json(GREEK_URL, headers, {"instrument_key": ",".join(chunk)})
        out["chunks"].append({"n": len(chunk), "http_status": r.get("http_status"), "error": r.get("error")})
        body = r.get("body") or {}
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(data, dict):
            out["merged"].update(data)
        if isinstance(body, dict) and body.get("errors"):
            out.setdefault("errors", []).extend(body.get("errors") or [])
        if r.get("http_status") and int(r["http_status"]) >= 400:
            out["last_error_body"] = body
    return out


def _extract_bid_ask(qd: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    depth = qd.get("depth") or {}
    buys = depth.get("buy") or []
    sells = depth.get("sell") or []
    bid = ask = None
    if buys and isinstance(buys[0], dict):
        try:
            bid = float(buys[0].get("price"))
        except (TypeError, ValueError):
            bid = None
    if sells and isinstance(sells[0], dict):
        try:
            ask = float(sells[0].get("price"))
        except (TypeError, ValueError):
            ask = None
    if bid is None:
        for k in ("best_bid_price", "bid_price"):
            if qd.get(k) is not None:
                try:
                    bid = float(qd[k])
                    break
                except (TypeError, ValueError):
                    pass
    if ask is None:
        for k in ("best_ask_price", "ask_price"):
            if qd.get(k) is not None:
                try:
                    ask = float(qd[k])
                    break
                except (TypeError, ValueError):
                    pass
    return bid, ask


def _norm_key(k: str) -> str:
    return str(k or "").replace("%7C", "|").replace("%7c", "|").strip().upper()


def _fetch_quotes(token: str, keys: List[str]) -> Dict[str, Dict[str, Any]]:
    headers = get_headers(token)
    result: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(keys), 50):
        chunk = keys[i : i + 50]
        url = f"{QUOTES_URL}?instrument_key={quote(','.join(chunk), safe=',')}"
        r = _fetch_json(url, headers)
        body = r.get("body") or {}
        data = body.get("data") if isinstance(body, dict) else None
        if not isinstance(data, dict):
            continue
        by_norm = {_norm_key(k): v for k, v in data.items() if isinstance(v, dict)}
        for ik in chunk:
            qd = by_norm.get(_norm_key(ik))
            if qd is None:
                # match instrument_token field
                for v in data.values():
                    if not isinstance(v, dict):
                        continue
                    tok = v.get("instrument_token") or v.get("instrument_key") or ""
                    if _norm_key(str(tok)) == _norm_key(ik):
                        qd = v
                        break
            if not isinstance(qd, dict):
                continue
            bid, ask = _extract_bid_ask(qd)
            last = qd.get("last_price")
            try:
                last_f = float(last) if last is not None else None
            except (TypeError, ValueError):
                last_f = None
            mid = None
            if bid is not None and ask is not None and ask > 0 and bid >= 0:
                mid = 0.5 * (bid + ask)
            spread_pct_mid = None
            if mid and mid > 0 and bid is not None and ask is not None:
                spread_pct_mid = (ask - bid) / mid * 100.0
            result[ik] = {
                "last_price": last_f,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_pct_of_mid": spread_pct_mid,
                "oi": qd.get("oi") or (qd.get("open_interest")),
                "volume": qd.get("volume"),
            }
    return result


def _pick_futures_ltp(
    rows: List[Dict[str, Any]], family: str, expiry_ms: int, token: str
) -> Dict[str, Any]:
    """Nearest FUT for same exact underlying_symbol; prefer matching expiry else front."""
    us = "CRUDEOIL" if family == "CL" else "NATURALGAS"
    futs = [
        r
        for r in rows
        if _is_mcx_fo(r)
        and str(r.get("underlying_symbol") or "").strip().upper() == us
        and str(r.get("instrument_type") or "").upper() in ("FUT", "FUTURES")
        and r.get("instrument_key")
    ]
    same_exp = [r for r in futs if _expiry_ms(r) == expiry_ms]
    if same_exp:
        pool = same_exp
    else:
        # Closest futures expiry to the option expiry (not arbitrary front month).
        pool = sorted(
            futs,
            key=lambda r: abs((_expiry_ms(r) or 0) - expiry_ms),
        )
    if not pool:
        return {"error": "no_futures_in_master"}
    fut = pool[0]
    ik = str(fut["instrument_key"])
    quotes = _fetch_quotes(token, [ik])
    q = quotes.get(ik) or {}
    return {
        "instrument_key": ik,
        "trading_symbol": fut.get("trading_symbol"),
        "expiry": fut.get("expiry"),
        "matched_option_expiry": bool(same_exp),
        "ltp": q.get("last_price") or q.get("mid"),
        "quote": q,
    }


def _build_option_universe(rows: List[Dict[str, Any]]) -> Dict[str, Dict[int, List[Dict[str, Any]]]]:
    """family -> expiry_ms -> option rows (CE/PE)."""
    out: Dict[str, Dict[int, List[Dict[str, Any]]]] = {"CL": {}, "NG": {}}
    for r in rows:
        if not _is_mcx_fo(r):
            continue
        fam = _exact_family(r)
        if not fam:
            continue
        itype = str(r.get("instrument_type") or "").upper()
        if itype not in ("CE", "PE"):
            continue
        exp = _expiry_ms(r)
        if exp is None or not r.get("instrument_key"):
            continue
        out[fam].setdefault(exp, []).append(r)
    return out


def _atm_picks(
    options: List[Dict[str, Any]], fut_ltp: float, n_pairs: int
) -> List[Dict[str, Any]]:
    by_strike: Dict[float, Dict[str, Dict[str, Any]]] = {}
    for r in options:
        try:
            k = float(r.get("strike_price"))
        except (TypeError, ValueError):
            continue
        itype = str(r.get("instrument_type") or "").upper()
        by_strike.setdefault(k, {})[itype] = r
    complete = [(k, sides) for k, sides in by_strike.items() if "CE" in sides and "PE" in sides]
    complete.sort(key=lambda x: abs(x[0] - fut_ltp))
    picked: List[Dict[str, Any]] = []
    for k, sides in complete[:n_pairs]:
        for itype in ("CE", "PE"):
            r = sides[itype]
            picked.append(r)
    return picked


def _greek_lookup(merged: Dict[str, Any], ik: str) -> Dict[str, Any]:
    want = _norm_key(ik)
    for k, v in merged.items():
        if not isinstance(v, dict):
            continue
        if _norm_key(k) == want:
            return v
        tok = v.get("instrument_token") or v.get("instrument_key") or ""
        if _norm_key(str(tok)) == want:
            return v
    return {}


def _estimate_credit_spread_max_loss(
    short_mid: Optional[float],
    long_mid: Optional[float],
    width: float,
    lot: float,
) -> Optional[float]:
    """max_loss = (width - credit) * lot; credit ≈ short_mid - long_mid."""
    if short_mid is None or long_mid is None:
        # ceiling with zero credit
        return width * lot
    credit = max(short_mid - long_mid, 0.0)
    return max(width - credit, 0.0) * lot


def main() -> int:
    token = _load_token()
    token_meta = _token_expiry_meta(token)
    rows = _load_master()
    universe = _build_option_universe(rows)
    now = datetime.now(timezone.utc)

    summary: Dict[str, Any] = {
        "ran_at_utc": now.isoformat(),
        "product": "Kosmic Tarang",
        "contract_family": "full",
        "token_meta": token_meta,
        "master_rows": len(rows),
        "families": {},
        "notes": [
            "Exact underlying_symbol CRUDEOIL / NATURALGAS on MCX_FO only (excludes CRUDEOILM / NATGASMINI / NSE_COM).",
            "ATM = strikes nearest futures LTP; nearest two option expiries per family.",
            "Black-76 uses futures LTP as F; T from option expiry; IV from native or implied from mid.",
            "Liquidity gate reference: bid-ask ≤ 15% of mid (strategy doc).",
            "Min max-loss uses narrowest profile width (CL 5 steps, NG 3) and 1-step theoretical floor.",
        ],
    }

    all_keys: List[str] = []
    work: List[Dict[str, Any]] = []

    for fam in ("CL", "NG"):
        exp_map = universe.get(fam) or {}
        expiries = sorted(exp_map.keys())
        # prefer future expiries
        now_ms = int(now.timestamp() * 1000)
        future_exps = [e for e in expiries if e >= now_ms - 86400000]
        chosen = (future_exps or expiries)[:2]
        fam_block: Dict[str, Any] = {
            "name": "CRUDEOIL" if fam == "CL" else "NATURALGAS",
            "expiries_available": len(expiries),
            "expiries_selected": chosen,
            "by_expiry": {},
        }
        for exp in chosen:
            opts = exp_map[exp]
            lot = int(opts[0].get("lot_size") or 0)
            strikes = [float(o.get("strike_price")) for o in opts if o.get("strike_price") is not None]
            step = _strike_step(strikes)
            fut = _pick_futures_ltp(rows, fam, exp, token)
            fut_ltp = fut.get("ltp")
            try:
                fut_ltp_f = float(fut_ltp) if fut_ltp is not None else None
            except (TypeError, ValueError):
                fut_ltp_f = None
            if fut_ltp_f is None:
                # fallback: median strike as proxy (flagged)
                fut_ltp_f = sorted(strikes)[len(strikes) // 2] if strikes else None
                fut["ltp_fallback"] = "median_strike"
            picks = _atm_picks(opts, float(fut_ltp_f or 0), ATM_STRIKE_PAIRS) if fut_ltp_f else []
            keys = [str(p["instrument_key"]) for p in picks]
            all_keys.extend(keys)
            # also pull quotes for a wider delta-band scan later — include ~20 strikes around ATM
            by_strike: Dict[float, Dict[str, Dict]] = {}
            for o in opts:
                try:
                    k = float(o["strike_price"])
                except (TypeError, ValueError, KeyError):
                    continue
                by_strike.setdefault(k, {})[str(o.get("instrument_type")).upper()] = o
            near = sorted(by_strike.keys(), key=lambda k: abs(k - float(fut_ltp_f or 0)))[:25]
            band_keys: List[str] = []
            for k in near:
                for t in ("CE", "PE"):
                    if t in by_strike[k]:
                        band_keys.append(str(by_strike[k][t]["instrument_key"]))
            all_keys.extend(band_keys)
            work.append(
                {
                    "family": fam,
                    "expiry_ms": exp,
                    "lot_size": lot,
                    "strike_step": step,
                    "fut": fut,
                    "fut_ltp": fut_ltp_f,
                    "atm_picks": picks,
                    "band_keys": band_keys,
                    "by_strike": by_strike,
                    "T": _years_to_expiry(exp, now),
                }
            )
            fam_block["by_expiry"][str(exp)] = {
                "expiry_iso": datetime.fromtimestamp(exp / 1000, tz=timezone.utc).isoformat(),
                "option_count": len(opts),
                "lot_size": lot,
                "strike_step": step,
                "futures": fut,
                "atm_pick_count": len(picks),
            }
        summary["families"][fam] = fam_block

    # unique keys preserving order
    seen = set()
    uniq_keys: List[str] = []
    for k in all_keys:
        if k not in seen:
            seen.add(k)
            uniq_keys.append(k)

    greek = _fetch_greeks(token, uniq_keys)
    quotes = _fetch_quotes(token, uniq_keys)
    merged = greek.get("merged") or {}

    per_key: List[Dict[str, Any]] = []
    non_null_iv = 0
    non_null_delta = 0
    divergence_flags: List[Dict[str, Any]] = []
    liquidity_10_16: List[Dict[str, Any]] = []
    sizing_rows: List[Dict[str, Any]] = []

    for job in work:
        fam = job["family"]
        F = job["fut_ltp"]
        T = job["T"]
        step = job["strike_step"] or 0.0
        lot = float(job["lot_size"] or 0)
        by_strike = job["by_strike"]
        atm_report: List[Dict[str, Any]] = []

        for r in job["atm_picks"]:
            ik = str(r["instrument_key"])
            g = _greek_lookup(merged, ik)
            q = quotes.get(ik) or {}
            itype = str(r.get("instrument_type") or "").upper()
            try:
                K = float(r.get("strike_price"))
            except (TypeError, ValueError):
                continue
            iv = g.get("iv")
            delta = g.get("delta")
            try:
                iv_f = float(iv) if iv is not None else None
            except (TypeError, ValueError):
                iv_f = None
            try:
                delta_f = float(delta) if delta is not None else None
            except (TypeError, ValueError):
                delta_f = None
            # IV==0 or delta placeholders (0 / ±1 on ATM) treated as unreliable nulls.
            reliable = bool(
                iv_f is not None
                and iv_f > 0
                and delta_f is not None
                and 0.01 < abs(delta_f) < 0.99
            )
            if reliable:
                non_null_iv += 1
                non_null_delta += 1

            last = q.get("last_price")
            if last is None:
                try:
                    last = float(g.get("last_price")) if g.get("last_price") is not None else None
                except (TypeError, ValueError):
                    last = None
            mid = q.get("mid") or last
            model_iv = None
            if mid and F and T:
                model_iv = implied_vol_black76(float(mid), float(F), K, T, itype)
            sigma_for_delta = iv_f if iv_f and iv_f > 0 else model_iv
            model_delta = None
            if sigma_for_delta and F and T:
                model_delta = black76_delta(float(F), K, T, float(sigma_for_delta), itype)

            flag = None
            if not reliable and (iv_f == 0 or (delta_f is not None and (abs(delta_f) < 1e-9 or abs(abs(delta_f) - 1.0) < 1e-9))):
                flag = "NATIVE_PLACEHOLDER_ZERO"
            if delta_f is not None and model_delta is not None and reliable:
                if abs(delta_f - model_delta) > BLACK76_DELTA_ABS_TOL:
                    flag = "DELTA_DIVERGENCE"
            if iv_f is not None and model_iv is not None and iv_f > 0:
                rel = abs(iv_f - model_iv) / iv_f
                if rel > BLACK76_IV_REL_TOL:
                    flag = (flag or "") + ("|IV_DIVERGENCE" if flag else "IV_DIVERGENCE")

            row_out = {
                "family": fam,
                "expiry_ms": job["expiry_ms"],
                "instrument_key": ik,
                "trading_symbol": r.get("trading_symbol"),
                "type": itype,
                "strike": K,
                "lot_size": lot,
                "futures_ltp": F,
                "iv": iv_f,
                "delta": delta_f,
                "reliable_native": reliable,
                "gamma": g.get("gamma"),
                "theta": g.get("theta"),
                "vega": g.get("vega"),
                "oi": g.get("oi") if g.get("oi") is not None else q.get("oi"),
                "volume": g.get("volume") if g.get("volume") is not None else q.get("volume"),
                "last_price": last,
                "bid": q.get("bid"),
                "ask": q.get("ask"),
                "spread_pct_of_mid": q.get("spread_pct_of_mid"),
                "black76_iv_from_mid": model_iv,
                "black76_delta": model_delta,
                "divergence_flag": flag,
            }
            atm_report.append(row_out)
            per_key.append(row_out)
            if flag:
                divergence_flags.append(row_out)

        # liquidity scan on band keys (10–16 |delta|)
        for ik in job["band_keys"]:
            g = _greek_lookup(merged, ik)
            q = quotes.get(ik) or {}
            try:
                d = float(g.get("delta")) if g.get("delta") is not None else None
            except (TypeError, ValueError):
                d = None
            if d is None:
                continue
            ad = abs(d)
            if ad < DELTA_BAND[0] or ad > DELTA_BAND[1]:
                continue
            sp = q.get("spread_pct_of_mid")
            liquidity_10_16.append(
                {
                    "family": fam,
                    "expiry_ms": job["expiry_ms"],
                    "instrument_key": ik,
                    "delta": d,
                    "bid": q.get("bid"),
                    "ask": q.get("ask"),
                    "mid": q.get("mid"),
                    "spread_pct_of_mid": sp,
                    "passes_15pct_gate": (sp is not None and sp <= 15.0),
                }
            )

        # sizing: narrowest 1-step + profile min/max steps
        w_min_prof, w_max_prof = PROFILE_WIDTH_STEPS[fam]
        widths = []
        if step and step > 0:
            widths.append(("1_step_theoretical", 1 * step, 1))
            widths.append((f"profile_min_{w_min_prof}_steps", w_min_prof * step, w_min_prof))
            widths.append((f"profile_max_{w_max_prof}_steps", w_max_prof * step, w_max_prof))

        # pick short around ~12 delta on call side if available
        short_mid = long_mid = None
        short_strike = None
        for k in sorted(by_strike.keys(), key=lambda x: abs(x - float(F or 0))):
            ce = by_strike[k].get("CE")
            if not ce:
                continue
            g = _greek_lookup(merged, str(ce["instrument_key"]))
            try:
                d = abs(float(g.get("delta"))) if g.get("delta") is not None else None
            except (TypeError, ValueError):
                d = None
            if d is not None and DELTA_BAND[0] <= d <= DELTA_BAND[1]:
                q = quotes.get(str(ce["instrument_key"])) or {}
                short_mid = q.get("mid") or q.get("last_price")
                short_strike = k
                break
        for label, width_pts, steps in widths:
            lm = None
            if short_strike is not None and step:
                long_k = short_strike + width_pts  # OTM call wing
                long_row = by_strike.get(long_k, {}).get("CE")
                if long_row:
                    lq = quotes.get(str(long_row["instrument_key"])) or {}
                    lm = lq.get("mid") or lq.get("last_price")
            max_loss_zero_credit = width_pts * lot if lot else None
            max_loss_est = (
                _estimate_credit_spread_max_loss(short_mid, lm, width_pts, lot)
                if lot
                else None
            )
            sizing_rows.append(
                {
                    "family": fam,
                    "underlying": "CRUDEOIL" if fam == "CL" else "NATURALGAS",
                    "expiry_ms": job["expiry_ms"],
                    "expiry_iso": datetime.fromtimestamp(
                        job["expiry_ms"] / 1000, tz=timezone.utc
                    ).date().isoformat(),
                    "lot_size": lot,
                    "strike_step": step,
                    "width_label": label,
                    "width_points": width_pts,
                    "width_steps": steps,
                    "short_strike_sample": short_strike,
                    "short_mid": short_mid,
                    "long_mid": lm,
                    "max_loss_per_lot_zero_credit_inr": max_loss_zero_credit,
                    "max_loss_per_lot_est_credit_inr": max_loss_est,
                    "fits_budget_3000": (
                        max_loss_est is not None and max_loss_est <= 3000
                    ),
                    "fits_budget_10000": (
                        max_loss_est is not None and max_loss_est <= 10000
                    ),
                }
            )

        # attach atm report under family block
        fam_block = summary["families"][fam]["by_expiry"][str(job["expiry_ms"])]
        fam_block["atm_results"] = atm_report

    summary["greek_chunk_meta"] = greek.get("chunks")
    summary["greek_errors"] = greek.get("errors")
    summary["per_key_atm"] = per_key
    summary["non_null_iv_count"] = non_null_iv
    summary["non_null_delta_count"] = non_null_delta
    summary["reliable_atm_count"] = non_null_iv  # counted only when both reliable
    summary["atm_total"] = len(per_key)
    summary["iv_greeks_non_null"] = non_null_iv > 0 and non_null_delta > 0
    summary["black76_divergence_flags"] = divergence_flags
    summary["liquidity_10_16_delta"] = liquidity_10_16
    if liquidity_10_16:
        spreads = [x["spread_pct_of_mid"] for x in liquidity_10_16 if x.get("spread_pct_of_mid") is not None]
        summary["liquidity_10_16_summary"] = {
            "n": len(liquidity_10_16),
            "n_with_spread": len(spreads),
            "median_spread_pct_of_mid": (
                sorted(spreads)[len(spreads) // 2] if spreads else None
            ),
            "pct_passing_15pct_gate": (
                100.0 * sum(1 for x in liquidity_10_16 if x.get("passes_15pct_gate")) / len(liquidity_10_16)
            ),
        }
    summary["sizing_min_max_loss"] = sizing_rows

    unreliable_n = sum(1 for p in per_key if not p.get("reliable_native"))
    if summary["iv_greeks_non_null"] and unreliable_n == 0:
        summary["verdict"] = "PASS_IV_AND_GREEKS_PRESENT"
        summary["black76_required_phase1"] = False
        summary["black76_note"] = "Native IV/Greeks present and within tolerance; keep Black-76 as fallback."
    elif summary["iv_greeks_non_null"] and unreliable_n > 0:
        summary["verdict"] = "PARTIAL_RESPONSE_BUT_NULLS"
        summary["black76_required_phase1"] = True
        summary["black76_note"] = (
            f"Native IV/Greeks good on {non_null_iv}/{len(per_key)} ATM rows; "
            f"{unreliable_n} rows had IV=0 or placeholder delta — Black-76 required as fill in Phase 1."
        )
    elif token_meta.get("token_expired"):
        summary["verdict"] = "BLOCKED_EXPIRED_UPSTOX_TOKEN"
        summary["black76_required_phase1"] = True
        summary["black76_note"] = "Black-76 required in Phase 1 (auth blocked native Greeks)."
    elif any(
        (c or {}).get("http_status") in (401, 403) for c in (greek.get("chunks") or [])
    ):
        summary["verdict"] = "BLOCKED_HTTP_AUTH"
        summary["black76_required_phase1"] = True
        summary["black76_note"] = "Black-76 required in Phase 1 (auth blocked native Greeks)."
    elif merged:
        summary["verdict"] = "PARTIAL_RESPONSE_BUT_NULLS"
        summary["black76_required_phase1"] = True
        summary["black76_note"] = "Black-76 required in Phase 1 (nulls or no usable native Greeks)."
    else:
        summary["verdict"] = "FAIL_NO_USABLE_GREEK_DATA"
        summary["black76_required_phase1"] = True
        summary["black76_note"] = "Black-76 required in Phase 1 (nulls or no usable native Greeks)."

    if summary["verdict"] == "PASS_IV_AND_GREEKS_PRESENT" and divergence_flags:
        summary["black76_note"] = (
            "Native present but some divergence flags — use Black-76 as sanity check / fill gaps."
        )

    _script = Path(__file__).resolve()
    _default_out = (
        _script.parents[1] / "docs" / "_spike_upstox_mcx_raw.json"
        if _script.parent.name == "scripts"
        else Path("/tmp/_spike_upstox_mcx_raw.json")
    )
    out_path = Path(os.getenv("TARANG_SPIKE_OUT", str(_default_out)))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    # compact stdout
    print(
        json.dumps(
            {
                "verdict": summary["verdict"],
                "non_null_iv": non_null_iv,
                "non_null_delta": non_null_delta,
                "atm_rows": len(per_key),
                "liquidity_summary": summary.get("liquidity_10_16_summary"),
                "divergence_n": len(divergence_flags),
                "sizing_rows": len(sizing_rows),
                "black76_required_phase1": summary["black76_required_phase1"],
                "out": str(out_path),
            },
            indent=2,
            default=str,
        )
    )
    print(f"\nWrote {out_path}", file=sys.stderr)
    return 0 if summary.get("iv_greeks_non_null") else 1


if __name__ == "__main__":
    raise SystemExit(main())

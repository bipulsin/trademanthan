#!/usr/bin/env python3
"""
Kosmic Tarang: Upstox MCX Option Greek probe for mini + full (items 4–5).

ATM CE/PE on the nearest two expiries of CRUDEOILM, NATGASMINI, CRUDEOIL, NATURALGAS.
Reports native IV/delta, OI, volume, bid/ask, bid-ask % of mid for the 10–16Δ band,
and Black-76 divergence vs native Greeks.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_P0 = Path(__file__).resolve().parent / "tarang_phase0_upstox_mcx_greeks.py"
_spec = importlib.util.spec_from_file_location("tarang_p0_greeks", _P0)
_p0 = importlib.util.module_from_spec(_spec)
assert _spec.loader
_spec.loader.exec_module(_p0)

ATM_STRIKE_PAIRS = _p0.ATM_STRIKE_PAIRS
BLACK76_DELTA_ABS_TOL = _p0.BLACK76_DELTA_ABS_TOL
BLACK76_IV_REL_TOL = _p0.BLACK76_IV_REL_TOL
DELTA_BAND = _p0.DELTA_BAND
_atm_picks = _p0._atm_picks
_expiry_ms = _p0._expiry_ms
_fetch_greeks = _p0._fetch_greeks
_fetch_quotes = _p0._fetch_quotes
_greek_lookup = _p0._greek_lookup
_is_mcx_fo = _p0._is_mcx_fo
_load_master = _p0._load_master
_load_token = _p0._load_token
_pick_futures_ltp = _p0._pick_futures_ltp
_strike_step = _p0._strike_step
_token_expiry_meta = _p0._token_expiry_meta
_years_to_expiry = _p0._years_to_expiry
black76_delta = _p0.black76_delta
implied_vol_black76 = _p0.implied_vol_black76

UNDERLYINGS = [
    ("CL_MINI", "CRUDEOILM"),
    ("NG_MINI", "NATGASMINI"),
    ("CL_FULL", "CRUDEOIL"),
    ("NG_FULL", "NATURALGAS"),
]


def _family_from_us(us: str) -> Optional[str]:
    us = us.upper()
    for fam, name in UNDERLYINGS:
        if name == us:
            return fam
    return None


def main() -> int:
    token = _load_token()
    token_meta = _token_expiry_meta(token)
    rows = _load_master()
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)

    universe: Dict[str, Dict[int, List[Dict[str, Any]]]] = {k: {} for k, _ in UNDERLYINGS}
    for r in rows:
        if not _is_mcx_fo(r):
            continue
        us = str(r.get("underlying_symbol") or "").strip().upper()
        fam = _family_from_us(us)
        if not fam:
            continue
        itype = str(r.get("instrument_type") or "").upper()
        if itype not in ("CE", "PE"):
            continue
        exp = _expiry_ms(r)
        if exp is None or not r.get("instrument_key"):
            continue
        universe[fam].setdefault(exp, []).append(r)

    summary: Dict[str, Any] = {
        "ran_at_utc": now.isoformat(),
        "token_meta": token_meta,
        "families": {},
        "per_key_atm": [],
        "liquidity_10_16_delta": [],
        "black76_divergence_flags": [],
        "notes": [
            "Mini = CRUDEOILM / NATGASMINI; full = CRUDEOIL / NATURALGAS.",
            "ATM = nearest CE+PE to matching-family futures LTP; nearest two option expiries.",
            "Black-76 uses futures LTP as F. Flag |Δnative−Δmodel| > 0.12 or IV relative gap > 35%.",
        ],
    }

    all_keys: List[str] = []
    work: List[Dict[str, Any]] = []

    for fam, us in UNDERLYINGS:
        exp_map = universe.get(fam) or {}
        expiries = sorted(exp_map.keys())
        future_exps = [e for e in expiries if e >= now_ms - 86400000]
        chosen = (future_exps or expiries)[:2]
        fam_block: Dict[str, Any] = {
            "underlying": us,
            "expiries_available": len(expiries),
            "expiries_selected": chosen,
            "by_expiry": {},
        }
        for exp in chosen:
            opts = exp_map[exp]
            lot = int(opts[0].get("lot_size") or 0)
            strikes = [float(o.get("strike_price")) for o in opts if o.get("strike_price") is not None]
            step = _strike_step(strikes)
            # reuse futures picker by mapping mini/full onto CL/NG for the original helper
            orig_fam = "CL" if "CL" in fam else "NG"
            # Patch: pick futures for this exact underlying
            futs = [
                r
                for r in rows
                if _is_mcx_fo(r)
                and str(r.get("underlying_symbol") or "").strip().upper() == us
                and str(r.get("instrument_type") or "").upper() in ("FUT", "FUTURES")
                and r.get("instrument_key")
            ]
            # Use original helper only for full; for mini, inline LTP
            if fam.endswith("FULL"):
                fut = _pick_futures_ltp(rows, orig_fam, exp, token)
            else:
                same = [r for r in futs if _expiry_ms(r) == exp]
                pool = same or sorted(futs, key=lambda r: abs((_expiry_ms(r) or 0) - exp))
                if not pool:
                    fut = {"error": "no_futures_in_master"}
                else:
                    ik = str(pool[0]["instrument_key"])
                    quotes = _fetch_quotes(token, [ik])
                    q = quotes.get(ik) or {}
                    fut = {
                        "instrument_key": ik,
                        "trading_symbol": pool[0].get("trading_symbol"),
                        "expiry": pool[0].get("expiry"),
                        "matched_option_expiry": bool(same),
                        "ltp": q.get("last_price") or q.get("mid"),
                        "quote": q,
                    }
            fut_ltp = fut.get("ltp")
            try:
                fut_ltp_f = float(fut_ltp) if fut_ltp is not None else None
            except (TypeError, ValueError):
                fut_ltp_f = None
            if fut_ltp_f is None and strikes:
                fut_ltp_f = sorted(strikes)[len(strikes) // 2]
                fut["ltp_fallback"] = "median_strike"
            picks = _atm_picks(opts, float(fut_ltp_f or 0), ATM_STRIKE_PAIRS) if fut_ltp_f else []
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
            keys = [str(p["instrument_key"]) for p in picks] + band_keys
            all_keys.extend(keys)
            work.append(
                {
                    "family": fam,
                    "underlying": us,
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

    seen = set()
    uniq: List[str] = []
    for k in all_keys:
        if k not in seen:
            seen.add(k)
            uniq.append(k)

    greek = _fetch_greeks(token, uniq)
    quotes = _fetch_quotes(token, uniq)
    merged = greek.get("merged") or {}

    non_null_iv = non_null_delta = 0
    per_key: List[Dict[str, Any]] = []
    divergence: List[Dict[str, Any]] = []
    liq: List[Dict[str, Any]] = []

    for job in work:
        atm_report: List[Dict[str, Any]] = []
        F = job["fut_ltp"]
        T = job["T"]
        for r in job["atm_picks"]:
            ik = str(r["instrument_key"])
            g = _greek_lookup(merged, ik)
            q = quotes.get(ik) or {}
            itype = str(r.get("instrument_type") or "").upper()
            try:
                K = float(r.get("strike_price"))
            except (TypeError, ValueError):
                continue
            try:
                iv_f = float(g.get("iv")) if g.get("iv") is not None else None
            except (TypeError, ValueError):
                iv_f = None
            try:
                delta_f = float(g.get("delta")) if g.get("delta") is not None else None
            except (TypeError, ValueError):
                delta_f = None
            reliable = bool(iv_f and iv_f > 0 and delta_f is not None and 0.01 < abs(delta_f) < 0.99)
            if reliable:
                non_null_iv += 1
                non_null_delta += 1
            last = q.get("last_price")
            mid = q.get("mid") or last
            model_iv = implied_vol_black76(float(mid), float(F), K, T, itype) if mid and F and T else None
            sigma = iv_f if iv_f and iv_f > 0 else model_iv
            model_delta = black76_delta(float(F), K, T, float(sigma), itype) if sigma and F and T else None
            flag = None
            if not reliable and (iv_f == 0 or (delta_f is not None and (abs(delta_f) < 1e-9 or abs(abs(delta_f) - 1.0) < 1e-9))):
                flag = "NATIVE_PLACEHOLDER_ZERO"
            if delta_f is not None and model_delta is not None and reliable:
                if abs(delta_f - model_delta) > BLACK76_DELTA_ABS_TOL:
                    flag = "DELTA_DIVERGENCE"
            if iv_f and model_iv and iv_f > 0:
                if abs(iv_f - model_iv) / iv_f > BLACK76_IV_REL_TOL:
                    flag = (flag or "") + ("|IV_DIVERGENCE" if flag else "IV_DIVERGENCE")
            row_out = {
                "family": job["family"],
                "underlying": job["underlying"],
                "expiry_iso": datetime.fromtimestamp(job["expiry_ms"] / 1000, tz=timezone.utc).date().isoformat(),
                "trading_symbol": r.get("trading_symbol"),
                "type": itype,
                "strike": K,
                "lot_size": job["lot_size"],
                "futures_ltp": F,
                "iv": iv_f,
                "delta": delta_f,
                "delta_sign": ("+" if (delta_f or 0) >= 0 else "-") if delta_f is not None else None,
                "reliable_native": reliable,
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
                divergence.append(row_out)
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
            liq.append(
                {
                    "family": job["family"],
                    "underlying": job["underlying"],
                    "expiry_iso": datetime.fromtimestamp(job["expiry_ms"] / 1000, tz=timezone.utc).date().isoformat(),
                    "instrument_key": ik,
                    "delta": d,
                    "delta_sign": "+" if d >= 0 else "-",
                    "iv": g.get("iv"),
                    "oi": g.get("oi") if g.get("oi") is not None else q.get("oi"),
                    "volume": g.get("volume") if g.get("volume") is not None else q.get("volume"),
                    "bid": q.get("bid"),
                    "ask": q.get("ask"),
                    "mid": q.get("mid"),
                    "spread_pct_of_mid": sp,
                    "passes_15pct_gate": (sp is not None and sp <= 15.0),
                }
            )
        summary["families"][job["family"]]["by_expiry"][str(job["expiry_ms"])]["atm_results"] = atm_report

    summary["greek_chunk_meta"] = greek.get("chunks")
    summary["per_key_atm"] = per_key
    summary["non_null_iv_count"] = non_null_iv
    summary["non_null_delta_count"] = non_null_delta
    summary["atm_total"] = len(per_key)
    summary["black76_divergence_flags"] = divergence
    summary["liquidity_10_16_delta"] = liq
    summary["iv_greeks_non_null"] = non_null_iv > 0 and non_null_delta > 0
    unreliable_n = sum(1 for p in per_key if not p.get("reliable_native"))
    if summary["iv_greeks_non_null"] and unreliable_n == 0:
        summary["verdict"] = "PASS_IV_AND_GREEKS_PRESENT"
    elif summary["iv_greeks_non_null"] and unreliable_n > 0:
        summary["verdict"] = "PARTIAL_RESPONSE_BUT_NULLS"
    elif token_meta.get("token_expired"):
        summary["verdict"] = "BLOCKED_EXPIRED_UPSTOX_TOKEN"
    elif any((c or {}).get("http_status") in (401, 403) for c in (greek.get("chunks") or [])):
        summary["verdict"] = "BLOCKED_HTTP_AUTH"
    elif merged:
        summary["verdict"] = "PARTIAL_RESPONSE_BUT_NULLS"
    else:
        summary["verdict"] = "FAIL_NO_USABLE_GREEK_DATA"

    by_fam = {}
    for p in per_key:
        by_fam.setdefault(p["family"], {"reliable": 0, "total": 0})
        by_fam[p["family"]]["total"] += 1
        if p.get("reliable_native"):
            by_fam[p["family"]]["reliable"] += 1
    summary["verdict_by_family"] = by_fam

    out_path = Path(os.getenv("TARANG_SPIKE_OUT", "docs/_spike_upstox_mcx_mini_full_raw.json"))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(
        json.dumps(
            {
                "verdict": summary["verdict"],
                "verdict_by_family": by_fam,
                "atm_rows": len(per_key),
                "reliable": non_null_iv,
                "divergence_n": len(divergence),
                "liq_n": len(liq),
                "out": str(out_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

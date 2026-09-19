#!/usr/bin/env python3
"""
Kosmic Tarang Phase 0 follow-up: Delta India min max-loss-per-contract sizing
from public products/tickers (no auth). Width steps from strategy profile (2–4).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    raise SystemExit("requests required")

BASE = os.getenv("DELTA_INDIA_BASE", "https://api.india.delta.exchange/v2").rstrip("/")
HEADERS = {"Accept": "application/json", "User-Agent": "KosmicTarang-Phase0/1.0"}
PROFILE_WIDTH_STEPS = {"BTC": (2, 4), "ETH": (2, 4)}
DELTA_BAND = (0.10, 0.16)


def get(path: str, **params: Any) -> Dict[str, Any]:
    r = requests.get(f"{BASE}{path}", params=params or None, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def _f(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except (TypeError, ValueError):
        return None


def _strike_step(strikes: List[float]) -> Optional[float]:
    from collections import Counter

    uniq = sorted(set(strikes))
    diffs = [round(uniq[i + 1] - uniq[i], 8) for i in range(len(uniq) - 1) if uniq[i + 1] > uniq[i]]
    if not diffs:
        return None
    return Counter(diffs).most_common(1)[0][0]


def main() -> int:
    out_rows: List[Dict[str, Any]] = []
    findings: Dict[str, Any] = {
        "ran_at_utc": datetime.now(timezone.utc).isoformat(),
        "product": "Kosmic Tarang",
        "base": BASE,
        "rows": out_rows,
    }

    for asset in ("BTC", "ETH"):
        # gather live call options tickers (have greeks + quotes)
        tickers = get(
            "/tickers",
            contract_types="call_options",
            underlying_asset_symbols=asset,
            page_size=100,
        ).get("result") or []
        # group by settlement / expiry date from symbol DDMMYY
        by_exp: Dict[str, List[Dict[str, Any]]] = {}
        for t in tickers:
            sym = str(t.get("symbol") or "")
            parts = sym.split("-")
            exp_code = parts[-1] if len(parts) >= 4 else "unknown"
            by_exp.setdefault(exp_code, []).append(t)

        # pick nearest two expiries by parsing DDMMYY
        def exp_sort_key(code: str) -> tuple:
            if len(code) == 6 and code.isdigit():
                dd, mm, yy = int(code[:2]), int(code[2:4]), int(code[4:6])
                return (2000 + yy, mm, dd)
            return (9999, 99, 99)

        chosen = sorted(by_exp.keys(), key=exp_sort_key)[:2]
        for exp_code in chosen:
            chain = by_exp[exp_code]
            strikes = [_f(t.get("strike_price")) for t in chain]
            strikes_f = [s for s in strikes if s is not None]
            step = _strike_step(strikes_f) if strikes_f else None
            # spot / underlying
            spot = None
            for t in chain:
                spot = _f(t.get("spot_price")) or _f((t.get("greeks") or {}).get("spot"))
                if spot:
                    break
            cv = None
            for t in chain:
                cv = _f(t.get("contract_value"))
                if cv:
                    break
            # short ~12 delta call
            short = None
            for t in sorted(chain, key=lambda x: abs((_f((x.get("greeks") or {}).get("delta")) or 99) - 0.13)):
                d = _f((t.get("greeks") or {}).get("delta"))
                if d is None:
                    continue
                if DELTA_BAND[0] <= abs(d) <= DELTA_BAND[1]:
                    short = t
                    break
            quotes = (short or {}).get("quotes") or {}
            short_mid = None
            if short:
                bid, ask = _f(quotes.get("best_bid") or quotes.get("bid")), _f(
                    quotes.get("best_ask") or quotes.get("ask")
                )
                mark = _f(short.get("mark_price"))
                if bid is not None and ask is not None:
                    short_mid = 0.5 * (bid + ask)
                else:
                    short_mid = mark

            wmin, wmax = PROFILE_WIDTH_STEPS[asset]
            for label, steps in (
                ("1_step_theoretical", 1),
                (f"profile_min_{wmin}_steps", wmin),
                (f"profile_max_{wmax}_steps", wmax),
            ):
                if not step or not cv:
                    out_rows.append(
                        {
                            "family": asset,
                            "expiry_code": exp_code,
                            "width_label": label,
                            "error": "missing_step_or_contract_value",
                            "strike_step": step,
                            "contract_value": cv,
                        }
                    )
                    continue
                width = steps * step
                # long wing OTM
                long_mid = None
                if short and short.get("strike_price") is not None:
                    target = float(short["strike_price"]) + width
                    long = min(
                        chain,
                        key=lambda t: abs((_f(t.get("strike_price")) or 0) - target),
                    )
                    lq = long.get("quotes") or {}
                    lb, la = _f(lq.get("best_bid") or lq.get("bid")), _f(
                        lq.get("best_ask") or lq.get("ask")
                    )
                    if lb is not None and la is not None:
                        long_mid = 0.5 * (lb + la)
                    else:
                        long_mid = _f(long.get("mark_price"))
                credit = 0.0
                if short_mid is not None and long_mid is not None:
                    credit = max(short_mid - long_mid, 0.0)
                # Strike/premium quoted in USD per 1 coin; risk per contract:
                # (width - credit) * contract_value  → USD (do NOT multiply by spot again).
                # Strategy doc: width $2000, credit $300, cv 0.001 → $1.70 / contract.
                max_loss_usd = max(width - credit, 0.0) * cv
                max_loss_zero_usd = width * cv
                out_rows.append(
                    {
                        "family": asset,
                        "expiry_code": exp_code,
                        "spot": spot,
                        "contract_value": cv,
                        "strike_step": step,
                        "width_label": label,
                        "width_points": width,
                        "width_steps": steps,
                        "short_symbol": (short or {}).get("symbol"),
                        "short_delta": _f(((short or {}).get("greeks") or {}).get("delta")),
                        "short_mid": short_mid,
                        "long_mid": long_mid,
                        "credit_est": credit,
                        "max_loss_per_contract_usd_est": max_loss_usd,
                        "max_loss_zero_credit_usd_est": max_loss_zero_usd,
                        "fits_budget_usd_36": max_loss_usd <= 36,  # ~INR 3000
                        "fits_budget_usd_120": max_loss_usd <= 120,  # ~INR 10000
                    }
                )

    out = Path(
        os.getenv(
            "TARANG_DELTA_SIZING_OUT",
            str(Path(__file__).resolve().parents[1] / "docs" / "_spike_delta_sizing_raw.json"),
        )
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(findings, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"n_rows": len(out_rows), "out": str(out)}, indent=2))
    print(f"\nWrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

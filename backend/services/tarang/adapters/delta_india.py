"""Delta Exchange India adapter — public market data + authenticated read-only.

Credentials (Tarang-specific; do not overwrite algo.py hard-coded keys):
  DELTA_INDIA_API_KEY / DELTA_INDIA_API_SECRET / DELTA_INDIA_API_URL
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from backend.services.tarang.domain.types import OptionChain, OptionQuote
from backend.services.tarang.greeks_service import enrich_quote
from backend.services.tarang.iv_normalize import normalize_iv
from backend.services.tarang.strike_window import delta_snapshot_expiries, select_strikes, years_to_expiry

logger = logging.getLogger(__name__)

DEFAULT_BASE = "https://api.india.delta.exchange"


def _creds() -> Tuple[str, str, str]:
    key = (os.getenv("DELTA_INDIA_API_KEY") or "").strip()
    secret = (os.getenv("DELTA_INDIA_API_SECRET") or "").strip()
    url = (os.getenv("DELTA_INDIA_API_URL") or DEFAULT_BASE).strip().rstrip("/")
    return key, secret, url


class DeltaIndiaAdapter:
    venue = "delta_india"

    def __init__(self) -> None:
        self.api_key, self.api_secret, self.base = _creds()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "TradeManthan-KosmicTarang/1.0",
            }
        )

    def _host(self) -> str:
        b = self.base.rstrip("/")
        return b[:-3] if b.endswith("/v2") else b

    def _sign_path(self, path: str) -> str:
        p = path if path.startswith("/") else f"/{path}"
        return p if p.startswith("/v2") else f"/v2{p}"

    def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self._host()}{self._sign_path(path)}"
        try:
            r = self.session.get(url, params=params, timeout=30)
            try:
                body = r.json()
            except Exception:
                body = {"raw": (r.text or "")[:800]}
            return {"http_status": r.status_code, "body": body, "url": url}
        except requests.RequestException as e:
            return {"http_status": None, "body": {}, "error": str(e), "url": url}

    def _auth_get(self, path: str) -> Dict[str, Any]:
        if not self.api_key or not self.api_secret:
            return {"http_status": None, "body": {}, "error": "missing_credentials"}
        sign_path = self._sign_path(path)
        ts = str(int(time.time()))
        msg = "GET" + ts + sign_path
        sig = hmac.new(self.api_secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
        headers = {
            "api-key": self.api_key,
            "timestamp": ts,
            "signature": sig,
            "Accept": "application/json",
            "User-Agent": "TradeManthan-KosmicTarang/1.0",
        }
        url = f"{self._host()}{sign_path}"
        try:
            r = self.session.get(url, headers=headers, timeout=30)
            try:
                body = r.json()
            except Exception:
                body = {"raw": (r.text or "")[:800]}
            return {"http_status": r.status_code, "body": body, "url": url}
        except requests.RequestException as e:
            return {"http_status": None, "body": {}, "error": str(e), "url": url}

    def health(self) -> Dict[str, Any]:
        pub = self._public_get("/tickers", params={"contract_types": "call_options", "underlying_asset_symbols": "BTC"})
        pub_ok = pub.get("http_status") == 200 and (pub.get("body") or {}).get("success") is True
        auth: Dict[str, Any] = {"configured": bool(self.api_key and self.api_secret)}
        if auth["configured"]:
            bal = self._auth_get("/v2/wallet/balances")
            pos = self._auth_get("/v2/positions/margined")
            bal_body = bal.get("body") or {}
            pos_body = pos.get("body") or {}
            bal_ok = bal.get("http_status") == 200 and bal_body.get("success") is True
            pos_ok = pos.get("http_status") == 200 and pos_body.get("success") is True
            err = ""
            if isinstance(bal_body.get("error"), dict):
                err = str(bal_body["error"].get("code") or bal_body["error"].get("message") or "")
            elif isinstance(bal_body.get("error"), str):
                err = bal_body["error"]
            auth.update(
                {
                    "balances_ok": bal_ok,
                    "positions_ok": pos_ok,
                    "balances_http": bal.get("http_status"),
                    "positions_http": pos.get("http_status"),
                    "error_code": err or None,
                    "n_assets": len(bal_body.get("result") or []) if bal_ok else None,
                    "n_positions": len(pos_body.get("result") or []) if pos_ok else None,
                    "api_key_fingerprint": f"{self.api_key[:4]}…{self.api_key[-4:]}",
                }
            )
            if "ip_not_whitelisted" in (err or ""):
                auth["note"] = "Whitelist paperclip egress IP 140.245.14.17 (and local if needed)."
        return {
            "venue": self.venue,
            "ok": bool(pub_ok),
            "public_ok": pub_ok,
            "auth": auth,
            "status": "ok" if pub_ok else "public_fail",
        }

    def balances(self) -> Dict[str, Any]:
        return self._auth_get("/v2/wallet/balances")

    def positions(self) -> Dict[str, Any]:
        return self._auth_get("/v2/positions/margined")

    def _products(self, underlying: str) -> List[Dict[str, Any]]:
        # Prefer tickers filtered by underlying
        resp = self._public_get(
            "/tickers",
            params={
                "contract_types": "call_options,put_options",
                "underlying_asset_symbols": underlying.upper(),
            },
        )
        body = resp.get("body") or {}
        result = body.get("result")
        if isinstance(result, list):
            return [r for r in result if isinstance(r, dict)]
        return []

    def near_expiries(self, underlying: str, n: int = 2) -> List[str]:
        return self._listed_expiries(underlying)[:n]

    def snapshot_expiries(self, underlying: str) -> List[str]:
        """3 dailies + next 2 Friday weeklies that are listed."""
        return delta_snapshot_expiries(self._listed_expiries(underlying))

    def _listed_expiries(self, underlying: str) -> List[str]:
        us = underlying.upper()
        tickers = self._products(us)
        today = datetime.now(timezone.utc).date().isoformat()
        exps = set()
        for t in tickers:
            sym = str(t.get("symbol") or "")
            parts = sym.split("-")
            if len(parts) < 4:
                continue
            try:
                exp_iso = datetime.strptime(parts[3], "%d%m%y").date().isoformat()
            except ValueError:
                continue
            if exp_iso >= today:
                exps.add(exp_iso)
        return sorted(exps)

    def position_limits(self, underlying: str) -> Dict[str, Any]:
        """Best-effort exchange limits from a live ticker."""
        tickers = self._products(underlying.upper())
        limits: Dict[str, Any] = {"max_contracts_per_order": None, "max_contracts_per_trade": None}
        for t in tickers[:20]:
            for key in ("position_size", "position_limit", "max_leverage_notional", "order_size"):
                if t.get(key) is not None:
                    limits[key] = t.get(key)
            if t.get("contract_value") is not None:
                limits["contract_value"] = t.get("contract_value")
        return limits

    def build_chain(
        self,
        profile_id: str,
        underlying: str,
        expiry: Optional[str] = None,
        atm_window: int = 10,
        window_cfg: Optional[Dict[str, Any]] = None,
    ) -> OptionChain:
        us = underlying.upper()
        tickers = self._products(us)
        # Parse symbols like C-BTC-100000-190926
        parsed: List[Dict[str, Any]] = []
        for t in tickers:
            sym = str(t.get("symbol") or "")
            parts = sym.split("-")
            if len(parts) < 4:
                continue
            right = "CE" if parts[0].upper() in ("C", "CALL") else "PE" if parts[0].upper() in ("P", "PUT") else None
            if not right:
                continue
            try:
                strike = float(parts[2])
            except ValueError:
                continue
            exp_code = parts[3]
            # DDMMYY -> ISO
            try:
                exp_iso = datetime.strptime(exp_code, "%d%m%y").date().isoformat()
            except ValueError:
                continue
            if expiry and exp_iso != expiry[:10]:
                continue
            mark = t.get("mark_price") or t.get("close") or t.get("spot_price")
            greeks = t.get("greeks") or {}
            iv_raw = t.get("mark_vol") or greeks.get("iv") or t.get("implied_volatility")
            iv_dec, iv_r, iv_u = normalize_iv(iv_raw)
            try:
                delta = float(greeks["delta"]) if greeks.get("delta") is not None else None
            except (TypeError, ValueError):
                delta = None
            quotes = t.get("quotes") or {}
            bid = quotes.get("best_bid") or t.get("bid")
            ask = quotes.get("best_ask") or t.get("ask")
            bid_qty = quotes.get("bid_size") or quotes.get("best_bid_size") or t.get("bid_size")
            ask_qty = quotes.get("ask_size") or quotes.get("best_ask_size") or t.get("ask_size")
            try:
                bid_f = float(bid) if bid is not None else None
                ask_f = float(ask) if ask is not None else None
            except (TypeError, ValueError):
                bid_f = ask_f = None
            two_sided = bid_f is not None and ask_f is not None and bid_f > 0 and ask_f > 0
            mid = 0.5 * (bid_f + ask_f) if two_sided else None
            try:
                last = float(mark) if mark is not None else None
            except (TypeError, ValueError):
                last = None
            cv = t.get("contract_value")
            try:
                contract_value = float(cv) if cv is not None else None
            except (TypeError, ValueError):
                contract_value = 0.001 if us == "BTC" else 0.01 if us == "ETH" else None
            spot = t.get("spot_price") or t.get("underlying_price")
            parsed.append(
                {
                    "exp_iso": exp_iso,
                    "strike": strike,
                    "right": right,
                    "symbol": sym,
                    "iv": iv_dec,
                    "iv_raw": iv_r,
                    "iv_unit": iv_u,
                    "delta": delta,
                    "bid": bid_f,
                    "ask": ask_f,
                    "bid_qty": bid_qty,
                    "ask_qty": ask_qty,
                    "mid": mid,
                    "last": last,
                    "contract_value": contract_value,
                    "spot": float(spot) if spot is not None else None,
                    "oi": t.get("oi") or t.get("open_interest"),
                    "volume": t.get("volume"),
                    "two_sided": two_sided,
                    "position_size": t.get("position_size") or t.get("position_limit"),
                }
            )

        if not parsed:
            return OptionChain(
                profile_id=profile_id,
                venue=self.venue,
                underlying=us,
                expiry=expiry or "",
                futures_or_spot=None,
                lot_size=None,
                strike_step=None,
                meta={"error": "no_tickers"},
            )

        # Pick nearest suitable expiry if not specified
        today = datetime.now(timezone.utc).date().isoformat()
        exps = sorted({p["exp_iso"] for p in parsed if p["exp_iso"] >= today})
        chosen = expiry[:10] if expiry else (exps[0] if exps else parsed[0]["exp_iso"])
        rows = [p for p in parsed if p["exp_iso"] == chosen]
        spots = [p["spot"] for p in rows if p.get("spot")]
        S = spots[0] if spots else None
        strikes = sorted({p["strike"] for p in rows})
        strike_step = None
        if len(strikes) >= 2:
            diffs = sorted({round(strikes[i + 1] - strikes[i], 8) for i in range(len(strikes) - 1) if strikes[i + 1] > strikes[i]})
            strike_step = diffs[0] if diffs else None
        cfg = dict(window_cfg or {})
        min_atm = int(cfg.get("min_atm_window") or atm_window or 10)
        T = years_to_expiry(chosen)
        atm_iv = None
        if S and rows:
            atm_row = min(rows, key=lambda p: abs(p["strike"] - S))
            atm_iv = atm_row.get("iv")
        deltas_by_strike: Dict[float, float] = {}
        for p in rows:
            if p.get("delta") is not None:
                prev = deltas_by_strike.get(p["strike"])
                ad = abs(float(p["delta"]))
                if prev is None or ad > abs(float(prev)):
                    deltas_by_strike[p["strike"]] = p["delta"]
        keep, win_meta = select_strikes(
            strikes,
            F=S,
            atm_iv=atm_iv,
            years=T,
            deltas_by_strike=deltas_by_strike,
            delta_abs_min=float(cfg.get("delta_abs_min") or 0.03),
            delta_abs_max=float(cfg.get("delta_abs_max") or 0.97),
            sigma_mult=float(cfg.get("sigma_mult") or 2.5),
            min_atm_window=min_atm,
        )
        if keep:
            rows = [p for p in rows if p["strike"] in keep]

        quotes_out: List[OptionQuote] = []
        cv0 = rows[0].get("contract_value") if rows else None
        for p in rows:
            q = OptionQuote(
                instrument_key=p["symbol"],
                symbol=p["symbol"],
                underlying=us,
                expiry=chosen,
                strike=p["strike"],
                right=p["right"],
                bid=p["bid"],
                ask=p["ask"],
                mid=p["mid"],
                last=p["last"],
                oi=p.get("oi"),
                volume=p.get("volume"),
                iv=p["iv"],
                iv_raw=p["iv_raw"],
                iv_unit=p["iv_unit"],
                delta=p["delta"],
                greeks_source="venue" if p["iv"] and p["delta"] is not None else None,
                contract_value=p.get("contract_value"),
                meta={
                    "bid_qty": p.get("bid_qty"),
                    "ask_qty": p.get("ask_qty"),
                    "two_sided": bool(p.get("two_sided")),
                    "position_size": p.get("position_size"),
                },
            )
            if S:
                enrich_quote(q, F_or_S=S, model="bs", r=0.0)
            quotes_out.append(q)

        return OptionChain(
            profile_id=profile_id,
            venue=self.venue,
            underlying=us,
            expiry=chosen,
            futures_or_spot=S,
            lot_size=None,
            strike_step=strike_step,
            quotes=quotes_out,
            built_at=datetime.now(timezone.utc).isoformat(),
            meta={"n_quotes": len(quotes_out), "contract_value": cv0, "window": win_meta},
        )

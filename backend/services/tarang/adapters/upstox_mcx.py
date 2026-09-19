"""Upstox MCX adapter — minis by default; full CRUDEOIL / NATURALGAS behind config."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests

from backend.config import settings
from backend.services.divtest.instruments import ensure_instrument_master
from backend.services.tarang.adapters.rate_budget import get_tarang_upstox_budget
from backend.services.tarang.domain.types import OptionChain, OptionQuote
from backend.services.tarang.greeks_service import enrich_quote
from backend.services.tarang.iv_normalize import normalize_iv
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

GREEK_URL = "https://api.upstox.com/v3/market-quote/option-greek"
QUOTES_URL = "https://api.upstox.com/v2/market-quote/quotes"

_FAMILY_MINI = {"CL": "CRUDEOILM", "NG": "NATGASMINI"}
_FAMILY_FULL = {"CL": "CRUDEOIL", "NG": "NATURALGAS"}
_ALL_ENERGY = ("CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI")


def _norm_key(k: str) -> str:
    return str(k or "").replace("%7C", "|").replace("%7c", "|").strip().upper()


def _is_mcx_fo(row: Dict[str, Any]) -> bool:
    return "MCX" in str(row.get("segment") or "").upper()


def _expiry_ms(row: Dict[str, Any]) -> Optional[int]:
    exp = row.get("expiry")
    if exp is None:
        return None
    try:
        v = int(exp)
        if v < 10_000_000_000:
            v *= 1000
        return v
    except (TypeError, ValueError):
        return None


def _expiry_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).date().isoformat()


class UpstoxMcxAdapter:
    venue = "upstox_mcx"

    def __init__(self) -> None:
        self._ux = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)

    def _token_ok(self) -> Tuple[bool, str]:
        self._ux.reload_token_from_storage()
        if not self._ux.access_token:
            return False, "missing"
        # UpstoxService may expose expiry helpers; treat empty as ok-unknown
        return True, "present"

    def health(self) -> Dict[str, Any]:
        ok, state = self._token_ok()
        out: Dict[str, Any] = {
            "venue": self.venue,
            "token": state,
            "ok": False,
            "detail": {},
        }
        if not ok:
            out["status"] = "token_missing"
            return out
        # Lightweight instrument master touch
        try:
            rows = ensure_instrument_master()
            counts = {}
            for us in _ALL_ENERGY:
                counts[f"{us.lower()}_option_rows"] = sum(
                    1
                    for r in rows
                    if _is_mcx_fo(r)
                    and str(r.get("underlying_symbol") or "").upper() == us
                    and str(r.get("instrument_type") or "").upper() in ("CE", "PE")
                )
            out["detail"] = counts
            out["ok"] = counts.get("crudeoilm_option_rows", 0) > 0 and counts.get("natgasmini_option_rows", 0) > 0
            out["status"] = "ok" if out["ok"] else "master_incomplete"
        except Exception as e:
            out["status"] = "error"
            out["detail"] = {"error": str(e)[:200]}
        return out

    def _headers(self) -> Dict[str, str]:
        h = dict(self._ux.get_headers())
        h.setdefault("User-Agent", "TradeManthan-KosmicTarang/1.0")
        h.setdefault("Accept", "application/json")
        return h

    def _get(self, url: str) -> Dict[str, Any]:
        budget = get_tarang_upstox_budget()
        if not budget.acquire(timeout=20):
            return {"error": "rate_budget_timeout"}
        try:
            r = requests.get(url, headers=self._headers(), timeout=25)
            try:
                body = r.json()
            except Exception:
                body = {"raw": (r.text or "")[:500]}
            return {"http_status": r.status_code, "body": body}
        except requests.RequestException as e:
            return {"error": str(e)}

    def list_option_rows(self, underlying: str) -> List[Dict[str, Any]]:
        us = underlying.strip().upper()
        rows = ensure_instrument_master()
        out = []
        for r in rows:
            if not _is_mcx_fo(r):
                continue
            if str(r.get("underlying_symbol") or "").strip().upper() != us:
                continue
            itype = str(r.get("instrument_type") or "").upper()
            if itype not in ("CE", "PE"):
                continue
            if not r.get("instrument_key"):
                continue
            out.append(r)
        return out

    def _nearest_expiries(self, rows: List[Dict[str, Any]], n: int = 2) -> List[int]:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        exps = sorted({e for e in (_expiry_ms(r) for r in rows) if e and e > now_ms})
        return exps[:n]

    def near_expiries(self, underlying: str, n: int = 2) -> List[str]:
        rows = self.list_option_rows(underlying)
        return [_expiry_iso(ms) for ms in self._nearest_expiries(rows, n)]

    def contract_spec(self, underlying: str) -> Dict[str, Any]:
        """Lot size + modal strike step from the instrument master."""
        rows = self.list_option_rows(underlying)
        lot = None
        if rows:
            try:
                lot = int(rows[0].get("lot_size") or rows[0].get("quantity_limit") or 0) or None
            except (TypeError, ValueError):
                lot = None
        strikes = sorted(
            {
                float(r.get("strike_price") or r.get("strike") or 0)
                for r in rows
                if r.get("strike_price") or r.get("strike")
            }
        )
        step = None
        if len(strikes) >= 2:
            diffs = sorted(
                {round(strikes[i + 1] - strikes[i], 6) for i in range(len(strikes) - 1) if strikes[i + 1] > strikes[i]}
            )
            step = diffs[0] if diffs else None
        return {"underlying": underlying.upper(), "lot_size": lot, "strike_step": step, "n_options": len(rows)}

    def _futures_ltp(self, underlying: str, expiry_ms: int) -> Tuple[Optional[float], Dict[str, Any]]:
        rows = ensure_instrument_master()
        us = underlying.upper()
        futs = [
            r
            for r in rows
            if _is_mcx_fo(r)
            and str(r.get("underlying_symbol") or "").upper() == us
            and str(r.get("instrument_type") or "").upper() in ("FUT", "FUTURES")
            and r.get("instrument_key")
        ]
        same = [r for r in futs if _expiry_ms(r) == expiry_ms]
        pool = same or sorted(futs, key=lambda r: abs((_expiry_ms(r) or 0) - expiry_ms))
        if not pool:
            return None, {"error": "no_futures"}
        ik = pool[0]["instrument_key"]
        resp = self._get(f"{QUOTES_URL}?instrument_key={quote(ik, safe='')}")
        body = resp.get("body") or {}
        data = body.get("data") if isinstance(body, dict) else None
        ltp = None
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, dict) and v.get("last_price") is not None:
                    try:
                        ltp = float(v["last_price"])
                    except (TypeError, ValueError):
                        pass
                    break
        return ltp, {"instrument_key": ik, "trading_symbol": pool[0].get("trading_symbol"), "ltp": ltp}

    def _fetch_greeks(self, keys: List[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for i in range(0, len(keys), 50):
            chunk = keys[i : i + 50]
            url = f"{GREEK_URL}?instrument_key={quote(','.join(chunk), safe=',')}"
            resp = self._get(url)
            body = resp.get("body") or {}
            data = body.get("data") if isinstance(body, dict) else None
            if not isinstance(data, dict):
                continue
            by = {_norm_key(k): v for k, v in data.items() if isinstance(v, dict)}
            for ik in chunk:
                gd = by.get(_norm_key(ik))
                if isinstance(gd, dict):
                    out[ik] = gd
        return out

    def _fetch_quotes(self, keys: List[str]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for i in range(0, len(keys), 50):
            chunk = keys[i : i + 50]
            url = f"{QUOTES_URL}?instrument_key={quote(','.join(chunk), safe=',')}"
            resp = self._get(url)
            body = resp.get("body") or {}
            data = body.get("data") if isinstance(body, dict) else None
            if not isinstance(data, dict):
                continue
            by = {_norm_key(k): v for k, v in data.items() if isinstance(v, dict)}
            for ik in chunk:
                qd = by.get(_norm_key(ik))
                if not isinstance(qd, dict):
                    continue
                depth = qd.get("depth") or {}
                buys = depth.get("buy") or []
                sells = depth.get("sell") or []
                bid = ask = None
                try:
                    if buys:
                        bid = float(buys[0].get("price"))
                    if sells:
                        ask = float(sells[0].get("price"))
                except (TypeError, ValueError, IndexError):
                    pass
                last = qd.get("last_price")
                try:
                    last_f = float(last) if last is not None else None
                except (TypeError, ValueError):
                    last_f = None
                mid = 0.5 * (bid + ask) if bid is not None and ask is not None else None
                out[ik] = {
                    "bid": bid,
                    "ask": ask,
                    "mid": mid,
                    "last": last_f,
                    "oi": qd.get("oi") or qd.get("open_interest"),
                    "volume": qd.get("volume"),
                }
        return out

    def build_chain(
        self,
        profile_id: str,
        underlying: str,
        expiry: Optional[str] = None,
        atm_window: int = 8,
    ) -> OptionChain:
        us = (underlying or _FAMILY_MINI.get(profile_id.upper()) or "").upper()
        rows = self.list_option_rows(us)
        if expiry:
            # filter to matching date
            target = expiry[:10]
            rows = [r for r in rows if _expiry_ms(r) and _expiry_iso(_expiry_ms(r)) == target]
            exp_ms = _expiry_ms(rows[0]) if rows else None
        else:
            exps = self._nearest_expiries(rows, 1)
            exp_ms = exps[0] if exps else None
            rows = [r for r in rows if _expiry_ms(r) == exp_ms] if exp_ms else []

        if not rows or not exp_ms:
            return OptionChain(
                profile_id=profile_id,
                venue=self.venue,
                underlying=us,
                expiry=expiry or "",
                futures_or_spot=None,
                lot_size=None,
                strike_step=None,
                meta={"error": "no_options"},
            )

        F, fut_meta = self._futures_ltp(us, exp_ms)
        strikes = sorted({float(r.get("strike_price") or r.get("strike") or 0) for r in rows if r.get("strike_price") or r.get("strike")})
        strike_step = None
        if len(strikes) >= 2:
            diffs = sorted({round(strikes[i + 1] - strikes[i], 6) for i in range(len(strikes) - 1) if strikes[i + 1] > strikes[i]})
            strike_step = diffs[0] if diffs else None

        # ATM window
        if F and strikes:
            atm = min(strikes, key=lambda s: abs(s - F))
            idx = strikes.index(atm)
            lo = max(0, idx - atm_window)
            hi = min(len(strikes), idx + atm_window + 1)
            keep = set(strikes[lo:hi])
            rows = [r for r in rows if float(r.get("strike_price") or r.get("strike") or 0) in keep]

        keys = [str(r["instrument_key"]) for r in rows]
        greeks = self._fetch_greeks(keys)
        quotes = self._fetch_quotes(keys)
        lot = None
        try:
            lot = int(rows[0].get("lot_size") or rows[0].get("quantity_limit") or 0) or None
        except (TypeError, ValueError):
            lot = None

        exp_iso = _expiry_iso(exp_ms)
        out_quotes: List[OptionQuote] = []
        for r in rows:
            ik = str(r["instrument_key"])
            gd = greeks.get(ik) or {}
            qd = quotes.get(ik) or {}
            itype = str(r.get("instrument_type") or "").upper()
            try:
                strike = float(r.get("strike_price") or r.get("strike") or 0)
            except (TypeError, ValueError):
                continue
            iv_raw = gd.get("iv") if gd.get("iv") is not None else gd.get("implied_volatility")
            iv_dec, iv_r, iv_u = normalize_iv(iv_raw)
            try:
                delta = float(gd["delta"]) if gd.get("delta") is not None else None
            except (TypeError, ValueError):
                delta = None
            q = OptionQuote(
                instrument_key=ik,
                symbol=str(r.get("trading_symbol") or ""),
                underlying=us,
                expiry=exp_iso,
                strike=strike,
                right=itype,
                bid=qd.get("bid"),
                ask=qd.get("ask"),
                mid=qd.get("mid"),
                last=qd.get("last"),
                oi=qd.get("oi"),
                volume=qd.get("volume"),
                iv=iv_dec,
                iv_raw=iv_r,
                iv_unit=iv_u,
                delta=delta,
                gamma=gd.get("gamma"),
                theta=gd.get("theta"),
                vega=gd.get("vega"),
                greeks_source="venue" if iv_dec and delta is not None else None,
                lot_size=lot,
            )
            if F:
                enrich_quote(q, F_or_S=F, model="black76")
            out_quotes.append(q)

        return OptionChain(
            profile_id=profile_id,
            venue=self.venue,
            underlying=us,
            expiry=exp_iso,
            futures_or_spot=F,
            lot_size=lot,
            strike_step=strike_step,
            quotes=out_quotes,
            built_at=datetime.now(timezone.utc).isoformat(),
            meta={"futures": fut_meta, "n_quotes": len(out_quotes)},
        )

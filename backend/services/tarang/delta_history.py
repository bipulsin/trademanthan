"""Delta India public candle loader + expired-option archive. No live orders."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.adapters.delta_india import DeltaIndiaAdapter, DEFAULT_BASE
from backend.services.tarang.schema import ensure_tarang_tables

logger = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parents[3] / "data" / "delta_archive"
UNDERLYINGS = (("BTC", "BTCUSD"), ("ETH", "ETHUSD"))


def _host() -> str:
    b = (DEFAULT_BASE or "").rstrip("/")
    return b[:-3] if b.endswith("/v2") else b


def fetch_candles(symbol: str, resolution: str, start: int, end: int) -> Dict[str, Any]:
    adapter = DeltaIndiaAdapter()
    return adapter._public_get(
        "/history/candles",
        params={"symbol": symbol, "resolution": resolution, "start": start, "end": end},
    )


def persist_underlying_candles(symbol: str, resolution: str, bars: List[Any]) -> int:
    ensure_tarang_tables()
    n = 0
    db = SessionLocal()
    try:
        for bar in bars:
            if not isinstance(bar, (list, tuple)) or len(bar) < 5:
                if isinstance(bar, dict):
                    ts = bar.get("time") or bar.get("timestamp")
                    o, h, l, c = bar.get("open"), bar.get("high"), bar.get("low"), bar.get("close")
                    vol = bar.get("volume")
                else:
                    continue
            else:
                ts, o, h, l, c = bar[0], bar[1], bar[2], bar[3], bar[4]
                vol = bar[5] if len(bar) > 5 else None
            try:
                if isinstance(ts, str) and not str(ts).isdigit():
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                else:
                    ts_i = int(float(ts))
                    if ts_i > 10_000_000_000:
                        ts_i //= 1000
                    dt = datetime.fromtimestamp(ts_i, tz=timezone.utc)
            except (TypeError, ValueError):
                continue
            db.execute(
                text(
                    """
                    INSERT INTO tarang_hist_underlying (symbol, resolution, bar_at, open, high, low, close, volume)
                    VALUES (:s, :r, :t, :o, :h, :l, :c, :v)
                    ON CONFLICT (symbol, resolution, bar_at) DO NOTHING
                    """
                ),
                {"s": symbol, "r": resolution, "t": dt, "o": o, "h": h, "l": l, "c": c, "v": vol},
            )
            n += 1
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("persist underlying candles failed")
    finally:
        db.close()
    return n


def probe_candle_reach(symbol: str = "BTCUSD", resolution: str = "1h") -> Dict[str, Any]:
    now = int(datetime.now(timezone.utc).timestamp())
    start = now - 86400 * 400
    resp = fetch_candles(symbol, resolution, start, now)
    body = resp.get("body") or {}
    result = body.get("result") or []
    times = []
    for bar in result:
        if isinstance(bar, (list, tuple)) and bar:
            times.append(int(bar[0]))
        elif isinstance(bar, dict) and (bar.get("time") or bar.get("timestamp")):
            times.append(int(bar.get("time") or bar.get("timestamp")))
    if times and max(times) > 10_000_000_000:
        times = [t // 1000 for t in times]
    earliest = datetime.fromtimestamp(min(times), tz=timezone.utc).isoformat() if times else None
    latest = datetime.fromtimestamp(max(times), tz=timezone.utc).isoformat() if times else None
    return {
        "symbol": symbol,
        "resolution": resolution,
        "http": resp.get("http_status"),
        "n": len(result) if isinstance(result, list) else 0,
        "earliest": earliest,
        "latest": latest,
        "url": resp.get("url"),
    }


def load_underlying_1h(*, lookback_days: int = 800, chunk_days: int = 160) -> Dict[str, Any]:
    """Walk backward in chunks until the public endpoint returns empty. Max ~4000 bars/call."""
    ensure_tarang_tables()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    now = int(datetime.now(timezone.utc).timestamp())
    earliest_wanted = now - lookback_days * 86400
    out = []
    for und, sym in UNDERLYINGS:
        end = now
        seen = 0
        chunks = []
        errors = []
        while end > earliest_wanted:
            start = max(earliest_wanted, end - chunk_days * 86400)
            resp = fetch_candles(sym, "1h", start, end)
            body = resp.get("body") or {}
            bars = body.get("result") if isinstance(body.get("result"), list) else []
            raw = RAW_DIR / f"{sym}_1h_{start}_{end}.json"
            raw.write_text(json.dumps({"http": resp.get("http_status"), "n": len(bars), "url": resp.get("url")}, default=str), encoding="utf-8")
            if resp.get("http_status") != 200:
                errors.append({"http": resp.get("http_status"), "start": start, "end": end, "error": str(body.get("error") or "")[:200]})
                break
            if not bars:
                chunks.append({"start": start, "end": end, "n": 0, "note": "empty_page_stop"})
                break
            n = persist_underlying_candles(sym, "1h", bars)
            seen += n
            times = []
            for bar in bars:
                if isinstance(bar, dict) and bar.get("time"):
                    times.append(int(bar["time"]))
                elif isinstance(bar, (list, tuple)) and bar:
                    times.append(int(bar[0]))
            if times and max(times) > 10_000_000_000:
                times = [t // 1000 for t in times]
            oldest = min(times) if times else start
            chunks.append({"start": start, "end": end, "n": len(bars), "oldest": oldest, "http": resp.get("http_status")})
            if oldest >= end:
                break
            end = oldest - 1
            if len(chunks) >= 12:
                break
        db = SessionLocal()
        try:
            rng = db.execute(
                text(
                    """
                    SELECT MIN(bar_at) AS first, MAX(bar_at) AS last, COUNT(*)::int AS n
                    FROM tarang_hist_underlying WHERE symbol = :s AND resolution = '1h'
                    """
                ),
                {"s": sym},
            ).mappings().first()
        finally:
            db.close()
        out.append(
            {
                "underlying": und,
                "symbol": sym,
                "inserted_or_seen": seen,
                "stored_first": rng["first"].isoformat() if rng and rng.get("first") else None,
                "stored_last": rng["last"].isoformat() if rng and rng.get("last") else None,
                "stored_n": int((rng or {}).get("n") or 0),
                "chunks": chunks,
                "errors": errors,
                "http_bar_cap_note": "Delta /history/candles returns at most ~4000 bars per call; we walk backward.",
            }
        )
    return {"ok": True, "results": out}


def _expired_option_products(days: int = 4) -> List[Dict[str, Any]]:
    adapter = DeltaIndiaAdapter()
    today = datetime.now(timezone.utc).date()
    found = []
    for und, _ in UNDERLYINGS:
        resp = adapter._public_get(
            "/products",
            params={"contract_types": "call_options,put_options", "underlying_asset_symbols": und},
        )
        body = resp.get("body") or {}
        result = body.get("result") or []
        if not isinstance(result, list):
            continue
        for p in result:
            if not isinstance(p, dict):
                continue
            sett = p.get("settlement_time") or p.get("expiry_time") or p.get("auction_finish_time")
            exp_iso = None
            sym = str(p.get("symbol") or "")
            parts = sym.split("-")
            if len(parts) >= 4:
                try:
                    exp_iso = datetime.strptime(parts[3], "%d%m%y").date()
                except ValueError:
                    exp_iso = None
            if exp_iso is None:
                continue
            age = (today - exp_iso).days
            if 0 <= age <= days:
                found.append(
                    {
                        "symbol": sym,
                        "underlying": und,
                        "expiry": exp_iso.isoformat(),
                        "settlement_price": p.get("settlement_price") or p.get("close"),
                        "settlement_time": sett,
                        "product_id": p.get("id"),
                    }
                )
    return found


def archive_expired_options(*, days: int = 4) -> Dict[str, Any]:
    ensure_tarang_tables()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    products = _expired_option_products(days)
    now = int(datetime.now(timezone.utc).timestamp())
    start = now - 86400 * 10
    stored = []
    fetch_failed = []
    db = SessionLocal()
    try:
        for p in products:
            resp = fetch_candles(p["symbol"], "30m", start, now)
            body = resp.get("body") or {}
            http = resp.get("http_status")
            bars = body.get("result") if isinstance(body.get("result"), list) else []
            raw_path = RAW_DIR / f"{p['symbol']}_{datetime.now(timezone.utc).date().isoformat()}.json"
            raw_path.write_text(json.dumps({"product": p, "candles": body}, default=str)[:4_000_000], encoding="utf-8")
            if http != 200:
                fetch_failed.append({"symbol": p["symbol"], "http": http, "error": str(body.get("error") or "")[:160]})
            db.execute(
                text(
                    """
                    INSERT INTO tarang_option_archive (
                        symbol, underlying, expiry_date, resolution, settlement_price, settlement_time, payload, raw_path
                    ) VALUES (
                        :s, :u, :e, '30m', :sp, :st, CAST(:p AS jsonb), :rp
                    )
                    ON CONFLICT (symbol, expiry_date, resolution) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        settlement_price = COALESCE(EXCLUDED.settlement_price, tarang_option_archive.settlement_price),
                        raw_path = EXCLUDED.raw_path
                    """
                ),
                {
                    "s": p["symbol"],
                    "u": p["underlying"],
                    "e": p["expiry"],
                    "sp": p.get("settlement_price"),
                    "st": str(p.get("settlement_time") or "")[:80] or None,
                    "p": json.dumps(body, default=str)[:1_000_000],
                    "rp": str(raw_path),
                },
            )
            stored.append(p["symbol"])
        db.commit()
        if fetch_failed:
            from backend.services.tarang.alerts_telegram import notify_ops

            sample = ", ".join(f"{x['symbol']} HTTP {x['http']}" for x in fetch_failed[:12])
            notify_ops(
                db,
                kind="archive_fetch_failed",
                message=f"Expired-option archiver could not fetch candles for {len(fetch_failed)} contract(s): {sample}",
                dedupe_key=f"archive_fetch_failed:{datetime.now(timezone.utc).date().isoformat()}",
                throttle_sec=0,
            )
            db.commit()
    except Exception:
        db.rollback()
        logger.exception("expired option archive failed")
        return {"ok": False, "error": "persist_failed", "n_products": len(products)}
    finally:
        db.close()
    return {"ok": True, "n": len(stored), "symbols": stored[:40], "fetch_failed": fetch_failed[:40], "n_fetch_failed": len(fetch_failed)}


def delta_readiness() -> Dict[str, Any]:
    ensure_tarang_tables()
    db = SessionLocal()
    try:
        snap = db.execute(
            text(
                """
                SELECT underlying_symbol, expiry_date, COUNT(*)::int AS n,
                       MIN(captured_at) AS first, MAX(captured_at) AS last,
                       COUNT(DISTINCT captured_at::date)::int AS days
                FROM tarang_chain_snapshots
                WHERE venue = 'delta_india'
                GROUP BY underlying_symbol, expiry_date
                ORDER BY underlying_symbol, expiry_date
                """
            )
        ).mappings().all()
        iv = db.execute(
            text(
                """
                SELECT underlying_symbol, MIN(captured_at) AS first, COUNT(*)::int AS n
                FROM tarang_iv_snapshots WHERE venue = 'delta_india'
                GROUP BY underlying_symbol
                """
            )
        ).mappings().all()
        und = db.execute(
            text(
                """
                SELECT symbol, resolution, MIN(bar_at) AS first, MAX(bar_at) AS last, COUNT(*)::int AS n
                FROM tarang_hist_underlying GROUP BY symbol, resolution
                """
            )
        ).mappings().all()
        arch = db.execute(
            text(
                """
                SELECT underlying, MIN(expiry_date) AS first_exp, MAX(expiry_date) AS last_exp, COUNT(*)::int AS n
                FROM tarang_option_archive GROUP BY underlying
                """
            )
        ).mappings().all()

        def _iso(v):
            if v is None:
                return None
            if hasattr(v, "isoformat"):
                return v.isoformat()
            return str(v)

        warmup = 60
        iv_ready = []
        for r in iv:
            first = r["first"]
            ready = None
            if first is not None:
                from datetime import timedelta as td

                ready = (first + td(days=warmup)).isoformat() if hasattr(first, "isoformat") else None
            iv_ready.append(
                {
                    "underlying": r["underlying_symbol"],
                    "first_iv": _iso(first),
                    "n": r["n"],
                    "iv_percentile_available_from": ready,
                    "warmup_days": warmup,
                }
            )
        return {
            "snapshot_days_by_expiry": [
                {
                    "underlying": r["underlying_symbol"],
                    "expiry": str(r["expiry_date"]) if r["expiry_date"] else None,
                    "rows": r["n"],
                    "days": r["days"],
                    "first": _iso(r["first"]),
                    "last": _iso(r["last"]),
                }
                for r in snap
            ],
            "underlying_candles": [
                {"symbol": r["symbol"], "resolution": r["resolution"], "first": _iso(r["first"]), "last": _iso(r["last"]), "n": r["n"]}
                for r in und
            ],
            "expired_option_archive": [
                {"underlying": r["underlying"], "first_expiry": str(r["first_exp"]), "last_expiry": str(r["last_exp"]), "n": r["n"]}
                for r in arch
            ],
            "iv_percentile": iv_ready,
        }
    finally:
        db.close()

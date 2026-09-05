"""Weekly volatility grade for arbitrage_master current-month futures.

score = 100 * margin / (LTP * qty)
  <= 20        → Low Risk
  20 < x <= 30 → Moderate
  > 30         → High Risk

CLI: python -m backend.services.arbitrage_volatility_grade --force
"""
from __future__ import annotations

import argparse
import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import text

from backend.database import SessionLocal, engine
from backend.services.market_holiday import should_skip_scheduled_market_jobs_ist

logger = logging.getLogger(__name__)

GRADE_LOW = "Low Risk"
GRADE_MOD = "Moderate"
GRADE_HIGH = "High Risk"

_MARGIN_CHUNK = 20
_QUOTE_CHUNK = 80
_SLEEP_QUOTE_S = 0.25
_SLEEP_MARGIN_S = 0.4

_ENSURE_SQL = """
ALTER TABLE arbitrage_master
    ADD COLUMN IF NOT EXISTS volatility_grade TEXT,
    ADD COLUMN IF NOT EXISTS volatility_score NUMERIC,
    ADD COLUMN IF NOT EXISTS volatility_grade_at TIMESTAMPTZ;
"""

_LOAD_SQL = text(
    """
    SELECT stock, currmth_future_instrument_key
    FROM arbitrage_master
    WHERE currmth_future_instrument_key IS NOT NULL
      AND TRIM(currmth_future_instrument_key) <> ''
    ORDER BY stock
    """
)

_UPDATE_SQL = text(
    """
    UPDATE arbitrage_master
    SET volatility_grade = :grade,
        volatility_score = :score,
        volatility_grade_at = NOW()
    WHERE stock = :stock
    """
)


def ensure_volatility_grade_columns() -> None:
    if engine is None:
        return
    with engine.begin() as conn:
        conn.execute(text(_ENSURE_SQL))


def volatility_score(ltp: float, qty: float, margin: float) -> Optional[float]:
    """100 * margin / (LTP * qty). None if any input is missing/zero/non-finite."""
    try:
        l = float(ltp)
        q = float(qty)
        m = float(margin)
    except (TypeError, ValueError):
        return None
    if l <= 0 or q <= 0 or m <= 0:
        return None
    notional = l * q
    if notional <= 0:
        return None
    return 100.0 * m / notional


def grade_from_score(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    try:
        s = float(score)
    except (TypeError, ValueError):
        return None
    if s <= 20.0:
        return GRADE_LOW
    if s <= 30.0:
        return GRADE_MOD
    return GRADE_HIGH


def _float_pos(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if x > 0 else None


def margin_rupees_from_item(item: Any) -> Optional[float]:
    """Parse one Upstox charges/margin row (or a numeric)."""
    if item is None:
        return None
    if isinstance(item, (int, float)):
        return _float_pos(item)
    if not isinstance(item, dict):
        return None
    for key in ("required_margin", "final_margin", "total_margin"):
        m = _float_pos(item.get(key))
        if m is not None:
            return round(m, 2)
    span = _float_pos(item.get("span_margin")) or 0.0
    expo = _float_pos(item.get("exposure_margin")) or 0.0
    addl = _float_pos(item.get("additional_margin")) or 0.0
    total = span + expo + addl
    return round(total, 2) if total > 0 else None


def margins_from_charges_response(
    resp: Optional[Dict[str, Any]], n_requested: int
) -> List[Optional[float]]:
    """Per-request-index margins. Empty list if the payload is unusable."""
    out: List[Optional[float]] = [None] * max(0, int(n_requested))
    if not isinstance(resp, dict) or resp.get("status") != "success":
        return out
    data = resp.get("data")
    if not isinstance(data, dict):
        return out
    rows = data.get("margins")
    if isinstance(rows, list) and rows:
        for i, row in enumerate(rows):
            if i >= len(out):
                break
            out[i] = margin_rupees_from_item(row)
        if n_requested == 1 and out[0] is None:
            out[0] = margin_rupees_from_item(data)
        return out
    if n_requested == 1:
        out[0] = margin_rupees_from_item(data)
    return out


def scheduled_tick_should_run() -> bool:
    """Friday 17:00 job: skip weekends and NSE holidays."""
    return not should_skip_scheduled_market_jobs_ist()


def _load_universe(db) -> List[Tuple[str, str]]:
    rows = db.execute(_LOAD_SQL).fetchall()
    out: List[Tuple[str, str]] = []
    seen_keys = set()
    for r in rows:
        stock = str(r[0] or "").strip()
        ik = str(r[1] or "").strip()
        if not stock or not ik:
            continue
        out.append((stock, ik))
        seen_keys.add(ik)
    return out


def _fetch_ltps(upstox, keys: Sequence[str]) -> Dict[str, float]:
    ltp_map: Dict[str, float] = {}
    uniq = list(dict.fromkeys(k for k in keys if k))
    for i in range(0, len(uniq), _QUOTE_CHUNK):
        batch = uniq[i : i + _QUOTE_CHUNK]
        snaps = upstox.get_market_quote_snapshots_batch(
            list(batch), max_per_request=len(batch)
        ) or {}
        for ik in batch:
            sn = upstox.snapshot_for_requested_key(snaps, ik)
            px = upstox.ltp_from_quote_snapshot(sn)
            if px is not None and float(px) > 0:
                ltp_map[ik] = float(px)
        if i + _QUOTE_CHUNK < len(uniq):
            time.sleep(_SLEEP_QUOTE_S)
    return ltp_map


def _fetch_margins(
    upstox, items: Sequence[Tuple[str, int]]
) -> Dict[str, float]:
    """instrument_key -> margin rupees for BUY one lot, product D then I."""
    out: Dict[str, float] = {}
    pending = [(ik, int(qty)) for ik, qty in items if ik and int(qty) > 0]
    for product in ("D", "I"):
        need = [(ik, q) for ik, q in pending if ik not in out]
        if not need:
            break
        for i in range(0, len(need), _MARGIN_CHUNK):
            chunk = need[i : i + _MARGIN_CHUNK]
            payload = [
                {
                    "instrument_key": ik,
                    "quantity": q,
                    "transaction_type": "BUY",
                    "product": product,
                }
                for ik, q in chunk
            ]
            resp = upstox.get_charges_margin(payload)
            parsed = margins_from_charges_response(resp, len(chunk))
            got_any = any(v is not None for v in parsed)
            if not got_any and len(chunk) > 1:
                for ik, q in chunk:
                    one = upstox.get_charges_margin(
                        [
                            {
                                "instrument_key": ik,
                                "quantity": q,
                                "transaction_type": "BUY",
                                "product": product,
                            }
                        ]
                    )
                    m = margins_from_charges_response(one, 1)
                    if m and m[0] is not None:
                        out[ik] = float(m[0])
                    time.sleep(_SLEEP_MARGIN_S)
            else:
                for j, (ik, _q) in enumerate(chunk):
                    if j < len(parsed) and parsed[j] is not None:
                        out[ik] = float(parsed[j])
            if i + _MARGIN_CHUNK < len(need):
                time.sleep(_SLEEP_MARGIN_S)
    return out


def run_volatility_grade_job(
    *, force: bool = False, trigger: str = "scheduled"
) -> Dict[str, Any]:
    if not force and not scheduled_tick_should_run():
        logger.info("volatility_grade: skipped (weekend/holiday)")
        return {"ok": True, "skipped": True, "reason": "weekend_or_holiday", "trigger": trigger}

    ensure_volatility_grade_columns()

    from backend.config import settings
    from backend.services.trap_ce.universe import LotSizeLookup
    from backend.services.upstox_service import UpstoxService

    db = SessionLocal()
    graded = 0
    skipped = 0
    by_grade = {GRADE_LOW: 0, GRADE_MOD: 0, GRADE_HIGH: 0}
    skip_reasons: Dict[str, int] = {}

    def _skip(reason: str) -> None:
        nonlocal skipped
        skipped += 1
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1

    try:
        universe = _load_universe(db)
        if not universe:
            logger.warning("volatility_grade: empty arbitrage_master universe")
            return {
                "ok": True,
                "trigger": trigger,
                "universe_n": 0,
                "graded": 0,
                "skipped": 0,
            }

        lots = LotSizeLookup()
        upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        keys = [ik for _, ik in universe]
        ltp_map = _fetch_ltps(upstox, keys)

        margin_inputs: List[Tuple[str, int]] = []
        qty_by_key: Dict[str, int] = {}
        for _stock, ik in universe:
            if ik in qty_by_key:
                continue
            q = lots.get(ik)
            if q > 0:
                qty_by_key[ik] = q
                margin_inputs.append((ik, q))
            else:
                logger.info("volatility_grade: missing lot size for %s", ik)

        margin_map = _fetch_margins(upstox, margin_inputs)

        for stock, ik in universe:
            ltp = ltp_map.get(ik)
            qty = qty_by_key.get(ik)
            margin = margin_map.get(ik)
            if ltp is None or float(ltp) <= 0:
                logger.info("volatility_grade: skip %s missing LTP (%s)", stock, ik)
                _skip("missing_ltp")
                continue
            if not qty:
                logger.info("volatility_grade: skip %s missing qty (%s)", stock, ik)
                _skip("missing_qty")
                continue
            if margin is None or float(margin) <= 0:
                logger.info("volatility_grade: skip %s missing margin (%s)", stock, ik)
                _skip("missing_margin")
                continue
            score = volatility_score(float(ltp), float(qty), float(margin))
            grade = grade_from_score(score)
            if score is None or grade is None:
                logger.info("volatility_grade: skip %s invalid score", stock)
                _skip("invalid_score")
                continue
            db.execute(
                _UPDATE_SQL,
                {"stock": stock, "grade": grade, "score": round(float(score), 4)},
            )
            graded += 1
            by_grade[grade] = by_grade.get(grade, 0) + 1

        db.commit()
        logger.info(
            "volatility_grade: trigger=%s universe=%s graded=%s skipped=%s by_grade=%s reasons=%s",
            trigger,
            len(universe),
            graded,
            skipped,
            by_grade,
            skip_reasons,
        )
        return {
            "ok": True,
            "trigger": trigger,
            "universe_n": len(universe),
            "graded": graded,
            "skipped": skipped,
            "by_grade": by_grade,
            "skip_reasons": skip_reasons,
        }
    except Exception as e:
        db.rollback()
        logger.exception("volatility_grade: job failed: %s", e)
        return {"ok": False, "trigger": trigger, "error": str(e)}
    finally:
        db.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description="Compute arbitrage_master volatility grades")
    p.add_argument(
        "--force",
        action="store_true",
        help="Run off-cycle (weekend/holiday), e.g. to backfill immediately",
    )
    args = p.parse_args(argv)
    out = run_volatility_grade_job(
        force=bool(args.force),
        trigger="cli_force" if args.force else "cli",
    )
    print(out)
    return 0 if out.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

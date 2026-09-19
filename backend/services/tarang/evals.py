"""First in-session eval dump + 24h counterfactual (NOT_EVALUABLE vs FAILED)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.tarang.gates import EVAL_FAILED, EVAL_NOT_EVALUABLE, EVAL_PASSED
from backend.services.tarang.schema import ensure_tarang_tables

IST = ZoneInfo("Asia/Kolkata")


def _gate_eval(g: Dict[str, Any]) -> str:
    ev = str(g.get("evaluability") or "").lower()
    if ev in (EVAL_PASSED, EVAL_FAILED, EVAL_NOT_EVALUABLE):
        return ev
    if g.get("status_hint") == "NOT_EVALUABLE":
        return EVAL_NOT_EVALUABLE
    name = str(g.get("name") or "")
    detail = str(g.get("detail") or "").lower()
    actual = g.get("actual")
    if name == "iv_vs_rv" and isinstance(actual, dict) and (actual.get("rv_20d") is None or actual.get("atm_iv") is None):
        return EVAL_NOT_EVALUABLE
    if "missing" in detail or "unavailable" in detail or "warming" in detail:
        if "warming" in detail:
            return EVAL_PASSED
        return EVAL_NOT_EVALUABLE
    if g.get("passed"):
        return EVAL_PASSED
    return EVAL_FAILED


def analyze_candidates_24h() -> Dict[str, Any]:
    ensure_tarang_tables()
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, created_at, profile_id, status, payload
                FROM tarang_candidates WHERE created_at >= :s
                ORDER BY id
                """
            ),
            {"s": since},
        ).mappings().all()
    finally:
        db.close()
    by_gate: Dict[str, Dict[str, int]] = {}
    by_profile: Dict[str, int] = {}
    would_ignore_missing = {"n": 0, "no_failed_if_missing_ignored": 0}
    samples = []
    from backend.services.tarang.rv import rv_for_profile

    for r in rows:
        payload = r["payload"] if isinstance(r["payload"], dict) else {}
        pid = r["profile_id"]
        by_profile[pid] = by_profile.get(pid, 0) + 1
        gates = payload.get("gates") or []
        failed_now = 0
        failed_if_data = 0
        for g in gates:
            ev = _gate_eval(g)
            name = str(g.get("name") or "?")
            slot = by_gate.setdefault(name, {"passed": 0, "failed": 0, "not_evaluable": 0})
            slot[ev] = slot.get(ev, 0) + 1
            if ev == EVAL_FAILED:
                failed_now += 1
                failed_if_data += 1
            elif ev == EVAL_NOT_EVALUABLE:
                # counterfactual: if this was missing RV, recompute from candles when possible
                if name == "iv_vs_rv":
                    actual = g.get("actual") if isinstance(g.get("actual"), dict) else {}
                    atm = actual.get("atm_iv") if actual else payload.get("atm_iv")
                    rv = rv_for_profile(pid)
                    rel_min = float((g.get("threshold") if not isinstance(g.get("threshold"), dict) else g.get("threshold")) or 0.10)
                    if isinstance(g.get("threshold"), (int, float)):
                        rel_min = float(g["threshold"])
                    if atm is not None and rv:
                        rel = (float(atm) - float(rv)) / float(rv)
                        if rel < rel_min:
                            failed_if_data += 1
                    # else still unknown — do not count as fail
        would_ignore_missing["n"] += 1
        if failed_if_data == 0:
            would_ignore_missing["no_failed_if_missing_ignored"] += 1
        if len(samples) < 8:
            samples.append(
                {
                    "id": r["id"],
                    "profile_id": pid,
                    "atm_iv": payload.get("atm_iv"),
                    "rv_20d": payload.get("rv_20d"),
                    "status": payload.get("status") or r["status"],
                    "iv_vs_rv": next((g for g in gates if g.get("name") == "iv_vs_rv"), None),
                }
            )
    n = len(rows)
    fractions = {}
    for name, c in by_gate.items():
        tot = c["passed"] + c["failed"] + c["not_evaluable"]
        fractions[name] = {
            **c,
            "pass_frac_of_evaluable": (c["passed"] / (c["passed"] + c["failed"])) if (c["passed"] + c["failed"]) else None,
            "not_evaluable_frac": (c["not_evaluable"] / tot) if tot else None,
        }
    return {
        "n_evals": n,
        "by_profile": by_profile,
        "by_gate": fractions,
        "counterfactual_no_strategy_fail_if_missing_filled_or_ignored": would_ignore_missing,
        "samples": samples,
    }


def first_in_session_evals(*, after_date: str, underlyings: Optional[List[str]] = None) -> Dict[str, Any]:
    """First stored eval per underlying after IST 09:00 on after_date (MCX open)."""
    ensure_tarang_tables()
    try:
        d = datetime.fromisoformat(after_date).date()
    except ValueError:
        return {"ok": False, "error": "after_date must be YYYY-MM-DD"}
    start = datetime(d.year, d.month, d.day, 9, 0, tzinfo=IST).astimezone(timezone.utc)
    want = [u.upper() for u in (underlyings or ["CRUDEOILM", "NATGASMINI", "CL", "NG"])]
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT id, created_at, profile_id, status, payload
                FROM tarang_candidates
                WHERE created_at >= :s
                ORDER BY created_at ASC
                """
            ),
            {"s": start},
        ).mappings().all()
        seen = set()
        picked = []
        for r in rows:
            p = r["payload"] if isinstance(r["payload"], dict) else {}
            und = str(p.get("underlying") or r["profile_id"] or "").upper()
            pid = str(r["profile_id"] or "").upper()
            if und not in want and pid not in want:
                continue
            key = und or pid
            if key in seen:
                continue
            seen.add(key)
            picked.append(r)
        rows = picked
        out = []
        for r in rows:
            p = r["payload"] if isinstance(r["payload"], dict) else {}
            out.append(
                {
                    "id": r["id"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "profile_id": r["profile_id"],
                    "underlying": p.get("underlying") or r["profile_id"],
                    "expiry": p.get("expiry"),
                    "status": p.get("status") or r["status"],
                    "atm_iv": p.get("atm_iv"),
                    "rv_20d": p.get("rv_20d"),
                    "gates": p.get("gates") or [],
                    "legs": p.get("legs") or [],
                    "chain_meta": p.get("chain_meta"),
                    "futures_or_spot": p.get("futures_or_spot"),
                }
            )
        return {
            "ok": True,
            "after_ist": f"{after_date} 09:00 IST",
            "n": len(out),
            "rows": out,
            "note": "Monday 21 Sep 2026 MCX session has not happened yet as of this code path; expect empty until then.",
        }
    finally:
        db.close()


def latest_iv_rv_panel() -> List[Dict[str, Any]]:
    """ATM IV, RV, ratio, threshold per underlying from latest candidate."""
    ensure_tarang_tables()
    rel_min = 0.10
    try:
        from backend.services.tarang.config import get_risk

        rel_min = float(get_risk().get("iv_vs_rv_relative_min") or 0.10)
    except Exception:
        pass
    db = SessionLocal()
    try:
        rows = db.execute(
            text(
                """
                SELECT DISTINCT ON (profile_id) id, created_at, profile_id, payload
                FROM tarang_candidates
                ORDER BY profile_id, id DESC
                """
            )
        ).mappings().all()
    finally:
        db.close()
    from backend.services.tarang.rv import rv_for_profile

    out = []
    for r in rows:
        p = r["payload"] if isinstance(r["payload"], dict) else {}
        atm = p.get("atm_iv")
        rv = p.get("rv_20d")
        if rv is None:
            rv = rv_for_profile(r["profile_id"])
        ratio = None
        if atm is not None and rv:
            ratio = (float(atm) - float(rv)) / float(rv)
        out.append(
            {
                "profile_id": r["profile_id"],
                "underlying": p.get("underlying") or r["profile_id"],
                "evaluated_at": p.get("evaluated_at") or (r["created_at"].isoformat() if r["created_at"] else None),
                "atm_iv": atm,
                "realized_vol_20d": rv,
                "iv_minus_rv_over_rv": ratio,
                "threshold": rel_min,
                "gate": "pass" if (ratio is not None and ratio >= rel_min) else ("not_evaluable" if rv is None or atm is None else "fail"),
            }
        )
    return out

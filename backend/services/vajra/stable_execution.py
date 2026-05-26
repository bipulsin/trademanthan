"""Stable Execution Mode — sticky Top 3, ESS ranking overlay, freeze watchlist, rotation hints."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
from sqlalchemy import text

from backend.database import SessionLocal
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.vajra.candles import ist_minutes
from backend.services.vajra.execution_stability_score import enrich_row_ess
from backend.services.vajra.qualification_config import STATE_EXECUTABLE
from backend.services.vajra.stable_execution_tables import ensure_vajra_stable_execution_table

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

STICKY_TOP_N = 3
DEFAULT_STICKY_MINUTES = 30
ALLOWED_STICKY_MINUTES = (15, 30, 60)
# 82 vs 84 — no swap; ~11pt lead required (e.g. 93 vs 82)
RANK_OVERRIDE_GAP = 11.0
ESS_OVERRIDE_GAP = 15.0
DETERIORATION_SCORE_DROP = 12.0
QUAL_DECAY_CONVICTION_DROP = 12.0
FREEZE_WINDOW_START = 9 * 60 + 20
FREEZE_WINDOW_END = 9 * 60 + 45


@dataclass
class StableExecutionConfig:
    stable_mode_enabled: bool = True
    focus_mode_enabled: bool = False
    sticky_persist_minutes: int = DEFAULT_STICKY_MINUTES
    frozen_focus_stocks: List[str] = field(default_factory=list)
    watchlist_frozen_at: Optional[datetime] = None
    sticky_slots: List[Dict[str, Any]] = field(default_factory=list)


def _now_ist() -> datetime:
    return datetime.now(IST)


def is_freeze_watchlist_window_ist(now: Optional[datetime] = None) -> bool:
    """Early freeze 9:20–9:45, or any time from execution window (10:00+)."""
    now = now or _now_ist()
    if now.weekday() >= 5:
        return False
    m = ist_minutes(now)
    if FREEZE_WINDOW_START <= m <= FREEZE_WINDOW_END:
        return True
    try:
        from backend.services.vajra.session_window import is_vajra_execution_window_ist

        return is_vajra_execution_window_ist(now)
    except Exception:
        return False


def _stock_key(row: Dict[str, Any]) -> str:
    return str(row.get("stock") or row.get("security") or "").strip().upper()


def _rank_score(row: Dict[str, Any]) -> float:
    """Stable-mode primary rank: ESS-led with setup quality + sector stability."""
    conv = _f(row, "conviction_score") or _f(row, "confidence")
    base = (
        _f(row, "ess_score") * 0.55
        + _f(row, "setup_quality_score") * 0.25
        + conv * 0.20
    )
    try:
        from backend.services.vajra.sector_intelligence import sector_weighted_rank_adjustment

        base += sector_weighted_rank_adjustment(row)
    except Exception:
        pass
    return base


def _f(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    v = row.get(key)
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _qual(row: Dict[str, Any]) -> str:
    return str(
        row.get("qualification_state") or row.get("qualification") or row.get("entry_state") or ""
    ).upper()


def load_user_state(user_id: int, session_date: Optional[date] = None) -> StableExecutionConfig:
    sd = session_date or effective_session_date_ist_for_trend()
    db = SessionLocal()
    try:
        ensure_vajra_stable_execution_table(db)
        r = db.execute(
            text(
                """
                SELECT stable_mode_enabled, focus_mode_enabled, sticky_persist_minutes,
                       frozen_focus_stocks, watchlist_frozen_at, sticky_slots
                FROM vajra_stable_execution_state
                WHERE user_id = :uid AND session_date = :sd
                """
            ),
            {"uid": user_id, "sd": sd},
        ).fetchone()
        if not r:
            return StableExecutionConfig()
        frozen = r[3]
        if isinstance(frozen, str):
            frozen = json.loads(frozen) if frozen else []
        slots = r[5]
        if isinstance(slots, str):
            slots = json.loads(slots) if slots else []
        mins = int(r[2] or DEFAULT_STICKY_MINUTES)
        if mins not in ALLOWED_STICKY_MINUTES:
            mins = DEFAULT_STICKY_MINUTES
        return StableExecutionConfig(
            stable_mode_enabled=bool(r[0]),
            focus_mode_enabled=bool(r[1]),
            sticky_persist_minutes=mins,
            frozen_focus_stocks=[str(x).upper() for x in (frozen or [])[:STICKY_TOP_N]],
            watchlist_frozen_at=r[4],
            sticky_slots=list(slots or []),
        )
    finally:
        db.close()


def save_user_state(
    user_id: int,
    cfg: StableExecutionConfig,
    session_date: Optional[date] = None,
) -> StableExecutionConfig:
    sd = session_date or effective_session_date_ist_for_trend()
    now = _now_ist()
    mins = cfg.sticky_persist_minutes
    if mins not in ALLOWED_STICKY_MINUTES:
        mins = DEFAULT_STICKY_MINUTES
    db = SessionLocal()
    try:
        ensure_vajra_stable_execution_table(db)
        db.execute(
            text(
                """
                INSERT INTO vajra_stable_execution_state (
                    user_id, session_date, stable_mode_enabled, focus_mode_enabled,
                    sticky_persist_minutes, frozen_focus_stocks, watchlist_frozen_at,
                    sticky_slots, updated_at
                ) VALUES (
                    :uid, :sd, :sm, :fm, :mins, CAST(:frozen AS jsonb), :wfa,
                    CAST(:slots AS jsonb), :now
                )
                ON CONFLICT (user_id, session_date) DO UPDATE SET
                    stable_mode_enabled = EXCLUDED.stable_mode_enabled,
                    focus_mode_enabled = EXCLUDED.focus_mode_enabled,
                    sticky_persist_minutes = EXCLUDED.sticky_persist_minutes,
                    frozen_focus_stocks = EXCLUDED.frozen_focus_stocks,
                    watchlist_frozen_at = EXCLUDED.watchlist_frozen_at,
                    sticky_slots = EXCLUDED.sticky_slots,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "uid": user_id,
                "sd": sd,
                "sm": cfg.stable_mode_enabled,
                "fm": cfg.focus_mode_enabled,
                "mins": mins,
                "frozen": json.dumps(cfg.frozen_focus_stocks[:STICKY_TOP_N]),
                "wfa": cfg.watchlist_frozen_at,
                "slots": json.dumps(cfg.sticky_slots),
                "now": now,
            },
        )
        db.commit()
        return cfg
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _slot_locked(slot: Dict[str, Any], now: datetime, persist_minutes: int) -> bool:
    locked_at = slot.get("locked_at")
    if not locked_at:
        return False
    if isinstance(locked_at, str):
        locked_at = datetime.fromisoformat(locked_at.replace("Z", "+00:00"))
    if locked_at.tzinfo is None:
        locked_at = IST.localize(locked_at)
    else:
        locked_at = locked_at.astimezone(IST)
    return now < locked_at + timedelta(minutes=persist_minutes)


def _materially_deteriorated(slot: Dict[str, Any], row: Dict[str, Any]) -> bool:
    lock_rank = _f(slot, "lock_rank_score")
    lock_ess = _f(slot, "lock_ess_score")
    cur_rank = _rank_score(row)
    cur_ess = _f(row, "ess_score")
    if lock_rank - cur_rank >= DETERIORATION_SCORE_DROP:
        return True
    if lock_ess - cur_ess >= DETERIORATION_SCORE_DROP:
        return True
    if _qual(row) == "REJECT":
        return True
    return False


def _challenger_beats_slot(slot: Dict[str, Any], challenger: Dict[str, Any]) -> bool:
    lock_rank = _f(slot, "lock_rank_score")
    lock_ess = _f(slot, "lock_ess_score")
    cur_rank = _rank_score(challenger)
    cur_ess = _f(challenger, "ess_score")
    if cur_rank < lock_rank + RANK_OVERRIDE_GAP:
        return False
    if cur_ess < lock_ess + ESS_OVERRIDE_GAP:
        return False
    return True


def _new_slot(row: Dict[str, Any], now: datetime) -> Dict[str, Any]:
    return {
        "stock": _stock_key(row),
        "locked_at": now.isoformat(),
        "lock_rank_score": _rank_score(row),
        "lock_ess_score": _f(row, "ess_score"),
        "lock_setup_quality": _f(row, "setup_quality_score"),
        "lock_qualification": _qual(row),
        "lock_conviction": _f(row, "conviction_score") or _f(row, "confidence"),
    }


def _apply_qualification_smoothing(slot: Optional[Dict[str, Any]], row: Dict[str, Any]) -> Dict[str, Any]:
    """Delay downgrade from EXECUTABLE using prior locked state + conviction decay."""
    row = dict(row)
    if not slot:
        return row
    prev_q = str(slot.get("lock_qualification") or "").upper()
    cur_q = _qual(row)
    if prev_q != STATE_EXECUTABLE or cur_q == STATE_EXECUTABLE:
        return row
    lock_conv = _f(slot, "lock_conviction")
    cur_conv = _f(row, "conviction_score") or _f(row, "confidence")
    if lock_conv - cur_conv < QUAL_DECAY_CONVICTION_DROP:
        row["stable_display_qualification"] = STATE_EXECUTABLE
        row["qualification_decay"] = True
        row["stable_qual_note"] = "Executable held — conviction decay buffer"
    return row


def _eligible_focus_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        if _qual(r) == "REJECT":
            continue
        if not _stock_key(r):
            continue
        out.append(r)
    return out


def _update_sticky_slots(
    rows_by_stock: Dict[str, Dict[str, Any]],
    cfg: StableExecutionConfig,
    now: datetime,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (sticky_top3_rows, suggested_rotations, updated_slots).
    """
    ranked = sorted(
        _eligible_focus_rows(list(rows_by_stock.values())),
        key=lambda r: (-_rank_score(r), -_f(r, "ess_score"), _stock_key(r)),
    )
    slots = [dict(s) for s in cfg.sticky_slots if s.get("stock")]
    slots = slots[:STICKY_TOP_N]
    persist = cfg.sticky_persist_minutes
    frozen_set = set(cfg.frozen_focus_stocks)

    # Force frozen focus stocks into slots when watchlist frozen
    if frozen_set:
        for sym in cfg.frozen_focus_stocks[:STICKY_TOP_N]:
            if sym in rows_by_stock and not any(s.get("stock") == sym for s in slots):
                slots.append(_new_slot(rows_by_stock[sym], now))

    # Trim expired / deteriorated unless frozen
    kept: List[Dict[str, Any]] = []
    for slot in slots:
        sym = slot.get("stock")
        row = rows_by_stock.get(sym)
        if not row:
            continue
        if sym in frozen_set:
            slot = {**slot, **_new_slot(row, now), "frozen": True}
            kept.append(slot)
            continue
        if _slot_locked(slot, now, persist) and not _materially_deteriorated(slot, row):
            kept.append(slot)
            continue
        if _materially_deteriorated(slot, row):
            continue
        kept.append(slot)

    slots = kept[:STICKY_TOP_N]
    used = {s.get("stock") for s in slots}
    suggestions: List[Dict[str, Any]] = []

    while len(slots) < STICKY_TOP_N:
        for cand in ranked:
            sym = _stock_key(cand)
            if sym in used:
                continue
            slots.append(_new_slot(cand, now))
            used.add(sym)
            break
        else:
            break

    # Rotation suggestions (no auto swap when frozen)
    if not frozen_set:
        for i, slot in enumerate(list(slots)):
            sym = slot.get("stock")
            row = rows_by_stock.get(sym)
            if not row or not _slot_locked(slot, now, persist):
                continue
            for cand in ranked:
                csym = _stock_key(cand)
                if csym in used or csym == sym:
                    continue
                if _challenger_beats_slot(slot, cand) and _materially_deteriorated(slot, row):
                    suggestions.append(
                        {
                            "from_stock": sym,
                            "to_stock": csym,
                            "reason": (
                                f"{csym} ESS {_f(cand, 'ess_score'):.0f} vs "
                                f"{sym} ESS {_f(row, 'ess_score'):.0f}; "
                                "material deterioration on incumbent"
                            ),
                        }
                    )
                    break

    sticky_rows: List[Dict[str, Any]] = []
    for slot in slots[:STICKY_TOP_N]:
        sym = slot.get("stock")
        row = rows_by_stock.get(sym)
        if not row:
            continue
        display = _apply_qualification_smoothing(slot, row)
        display["sticky_leader"] = True
        display["sticky_locked_until"] = (
            datetime.fromisoformat(slot["locked_at"].replace("Z", "+00:00"))
            + timedelta(minutes=persist)
        ).isoformat() if slot.get("locked_at") else None
        if sym in frozen_set:
            display["watchlist_frozen"] = True
        sticky_rows.append(display)

    return sticky_rows, suggestions, slots[:STICKY_TOP_N]


def apply_stable_execution_overlay(
    rows: List[Dict[str, Any]],
    user_id: int,
    *,
    session_date: Optional[date] = None,
    persist_state: bool = True,
) -> Dict[str, Any]:
    """
    Enrich rows with ESS and produce stable Top 3 + UI metadata.
    Does not mutate underlying DB ratings.
    """
    cfg = load_user_state(user_id, session_date)
    now = _now_ist()
    enriched = [enrich_row_ess(dict(r)) for r in rows]
    sector_meta: Dict[str, Any] = {}
    try:
        from backend.services.vajra.sector_intelligence import apply_sector_intelligence_to_rows
        from backend.services.vajra.session_window import vajra_workflow_phase_fields

        sector_meta = apply_sector_intelligence_to_rows(enriched, session_date=session_date)
        enriched = sector_meta.get("rows") or enriched
        workflow = vajra_workflow_phase_fields(now)
    except Exception as e:
        logger.debug("sector intelligence overlay skipped: %s", e)
        workflow = {}
    co_meta: Dict[str, Any] = {}
    try:
        from backend.services.vajra.execution_co_pilot import apply_execution_co_pilot

        co_meta = apply_execution_co_pilot(enriched, user_id, session_date=session_date)
        enriched = co_meta.get("rows") or enriched
    except Exception as e:
        logger.debug("execution co-pilot overlay skipped: %s", e)
    co_pilot_payload: Dict[str, Any] = {
        "market_context": co_meta.get("market_context") or {},
        "execution_events": co_meta.get("execution_events") or [],
    }
    by_stock = {_stock_key(r): r for r in enriched if _stock_key(r)}

    if not cfg.stable_mode_enabled:
        return {
            "stable_mode_enabled": False,
            "focus_mode_enabled": cfg.focus_mode_enabled,
            "sticky_persist_minutes": cfg.sticky_persist_minutes,
            "rows": enriched,
            "sticky_top3": [],
            "suggested_rotations": [],
            "freeze_window_open": is_freeze_watchlist_window_ist(now),
            "watchlist_frozen": bool(cfg.frozen_focus_stocks),
            "frozen_focus_stocks": cfg.frozen_focus_stocks,
            "attention_banner": None,
            "sector_heatmap": sector_meta.get("sector_heatmap") or [],
            "co_pilot": co_pilot_payload,
            **workflow,
        }

    sticky_rows, suggestions, new_slots = _update_sticky_slots(by_stock, cfg, now)
    if persist_state:
        cfg.sticky_slots = new_slots
        save_user_state(user_id, cfg, session_date)

    for r in enriched:
        sym = _stock_key(r)
        if any(s.get("stock") == sym for s in suggestions):
            r["suggested_rotation"] = next(
                x for x in suggestions if x.get("from_stock") == sym
            )

    banner = None
    if cfg.focus_mode_enabled:
        banner = "Focus Mode — trade selected setups. Ignore market noise."
    elif cfg.frozen_focus_stocks:
        banner = "Watchlist frozen — primary focus list locked for this session."
    elif workflow.get("execution_window"):
        banner = (
            workflow.get("workflow_notice")
            or "Execution window — sector-aligned Top 3 with sticky leadership."
        )

    focus_syms = set(cfg.frozen_focus_stocks or [])
    for sr in sticky_rows:
        sym = _stock_key(sr)
        if sym:
            focus_syms.add(sym)

    if cfg.focus_mode_enabled and focus_syms:
        try:
            from backend.services.vajra.execution_co_pilot import apply_execution_co_pilot

            co_meta = apply_execution_co_pilot(
                enriched,
                user_id,
                session_date=session_date,
                narrative_symbols=focus_syms,
            )
            enriched = co_meta.get("rows") or enriched
            by_stock = {_stock_key(r): r for r in enriched if _stock_key(r)}
            refreshed_sticky: List[Dict[str, Any]] = []
            for sr in sticky_rows:
                sym = _stock_key(sr)
                row = by_stock.get(sym)
                if row:
                    row = dict(row)
                    row["sticky_leader"] = True
                    if sr.get("watchlist_frozen"):
                        row["watchlist_frozen"] = True
                    refreshed_sticky.append(row)
            sticky_rows = refreshed_sticky
            co_pilot_payload["market_context"] = co_meta.get("market_context") or co_pilot_payload.get(
                "market_context"
            )
            co_pilot_payload["execution_events"] = co_meta.get("execution_events") or co_pilot_payload.get(
                "execution_events"
            )
        except Exception as e:
            logger.debug("focus narrative co-pilot: %s", e)

        try:
            from backend.services.vajra.focus_mode_telegram import maybe_send_focus_mode_telegram

            sd = session_date or effective_session_date_ist_for_trend()
            tg = maybe_send_focus_mode_telegram(
                user_id,
                enriched,
                session_date=sd,
                focus_mode_enabled=True,
                sticky_top3=sticky_rows,
                frozen_focus_stocks=cfg.frozen_focus_stocks,
                market_bias=(co_pilot_payload.get("market_context") or {}).get("market_bias"),
            )
            co_pilot_payload["focus_telegram"] = tg
        except Exception as e:
            logger.debug("focus telegram: %s", e)

    return {
        "stable_mode_enabled": True,
        "focus_mode_enabled": cfg.focus_mode_enabled,
        "sticky_persist_minutes": cfg.sticky_persist_minutes,
        "sticky_top3": sticky_rows,
        "suggested_rotations": suggestions,
        "freeze_window_open": is_freeze_watchlist_window_ist(now),
        "watchlist_frozen": bool(cfg.frozen_focus_stocks),
        "frozen_focus_stocks": cfg.frozen_focus_stocks,
        "watchlist_frozen_at": (
            cfg.watchlist_frozen_at.isoformat() if cfg.watchlist_frozen_at else None
        ),
        "attention_banner": banner,
        "rows": enriched,
        "sector_heatmap": sector_meta.get("sector_heatmap") or [],
        "co_pilot": co_pilot_payload,
        "server_telegram_alerts": True,
        **workflow,
    }


def freeze_watchlist_focus(
    user_id: int,
    stocks: List[str],
    session_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Trader-selected Top 3 during 9:20–9:45 IST (allowed outside window for manual fix)."""
    cfg = load_user_state(user_id, session_date)
    syms = [str(s).strip().upper() for s in stocks if str(s).strip()][:STICKY_TOP_N]
    cfg.frozen_focus_stocks = syms
    cfg.watchlist_frozen_at = _now_ist()
    save_user_state(user_id, cfg, session_date)
    return {
        "success": True,
        "frozen_focus_stocks": syms,
        "watchlist_frozen_at": cfg.watchlist_frozen_at.isoformat(),
    }

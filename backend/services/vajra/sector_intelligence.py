"""Sector persistence, stability score (SSS), and row enrichment for Vajra Stable Execution."""
from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.services.sector_movers import get_sector_movers_cached, nifty_sector_label_for_nse_equity
from backend.services.smart_futures_session_date import effective_session_date_ist_for_trend
from backend.services.vajra.candles import ist_minutes

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

TOP_GAINER_RANK_TRACK = 3
TOP_LOSER_RANK_TRACK = 3
PERSISTENT_LEADER_MINUTES = 30.0
ROTATION_RANK_CHANGES = 2
SSS_STRONG = 55.0
SSS_WEAK = 40.0
SECTOR_RANK_BOOST_MAX = 8.0
CONVICTION_BOOST_MAX = 5.0

# In-memory session tracker (resets per IST session date).
_session_snapshots: Dict[str, Dict[str, Any]] = {}


@dataclass
class _SectorRuntime:
    label: str
    pct_change: float = 0.0
    in_top_gainer_rank: Optional[int] = None
    in_top_loser_rank: Optional[int] = None
    minutes_top_gainer: float = 0.0
    minutes_top_loser: float = 0.0
    minutes_weak: float = 0.0
    pct_history: List[float] = field(default_factory=list)
    rank_history: List[int] = field(default_factory=list)
    last_snapshot_at: Optional[datetime] = None


def _now_ist() -> datetime:
    return datetime.now(IST)


def _session_key(session_date: Optional[date] = None) -> str:
    sd = session_date or effective_session_date_ist_for_trend()
    return sd.isoformat()


def _market_index_pct() -> float:
    try:
        from backend.config import settings
        from backend.services.market_sentiment_dials import build_dial_rows
        from backend.services.upstox_service import UpstoxService

        u = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
        rows = build_dial_rows(u, basis="today")
        by_id = {str(r.get("id") or ""): r for r in rows}
        return float((by_id.get("nifty50") or {}).get("pct_change") or 0.0)
    except Exception:
        return 0.0


def _sector_by_label(movers: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for side in ("gainers", "losers"):
        for i, row in enumerate(movers.get(side) or []):
            lbl = str(row.get("sector") or "").strip()
            if not lbl:
                continue
            out[lbl] = {**row, "_side": side, "_rank": i + 1}
    return out


def record_sector_snapshot(
    *,
    session_date: Optional[date] = None,
    now: Optional[datetime] = None,
    top_n: int = 3,
) -> Dict[str, Any]:
    """
    Append a sector-movers snapshot for persistence / SSS.
    Called from stable execution overlay (~every ratings poll).
    """
    now = now or _now_ist()
    sk = _session_key(session_date)
    bucket = _session_snapshots.setdefault(
        sk,
        {"sectors": {}, "snapshots": [], "last_recorded": None},
    )
    sectors: Dict[str, _SectorRuntime] = bucket["sectors"]
    try:
        movers = get_sector_movers_cached(top_n=top_n)
    except Exception as e:
        logger.debug("sector snapshot movers failed: %s", e)
        return {"success": False, "message": str(e)}

    gainer_labels = [
        str(r.get("sector") or "").strip()
        for r in (movers.get("gainers") or [])
        if r.get("sector")
    ]
    loser_labels = [
        str(r.get("sector") or "").strip()
        for r in (movers.get("losers") or [])
        if r.get("sector")
    ]
    by_lbl = _sector_by_label(movers)

    last_at = bucket.get("last_recorded")
    if isinstance(last_at, str):
        last_at = datetime.fromisoformat(last_at.replace("Z", "+00:00"))
    if last_at and last_at.tzinfo is None:
        last_at = IST.localize(last_at)
    delta_min = 0.0
    if last_at:
        delta_min = max(0.0, (now - last_at.astimezone(IST)).total_seconds() / 60.0)

    all_labels = set(sectors.keys()) | set(by_lbl.keys())
    for lbl in all_labels:
        if lbl not in sectors:
            sectors[lbl] = _SectorRuntime(label=lbl)
        st = sectors[lbl]
        info = by_lbl.get(lbl)
        pct = float((info or {}).get("pct_change") or 0.0)
        st.pct_change = pct
        g_rank = gainer_labels.index(lbl) + 1 if lbl in gainer_labels else None
        l_rank = loser_labels.index(lbl) + 1 if lbl in loser_labels else None
        st.in_top_gainer_rank = g_rank if g_rank and g_rank <= TOP_GAINER_RANK_TRACK else None
        st.in_top_loser_rank = l_rank if l_rank and l_rank <= TOP_LOSER_RANK_TRACK else None

        if delta_min > 0 and delta_min < 120:
            if st.in_top_gainer_rank:
                st.minutes_top_gainer += delta_min
            if st.in_top_loser_rank:
                st.minutes_top_loser += delta_min
            if pct < 0 and not st.in_top_gainer_rank:
                st.minutes_weak += delta_min

        st.pct_history = (st.pct_history + [pct])[-24:]
        rank_code = 0
        if st.in_top_gainer_rank:
            rank_code = st.in_top_gainer_rank
        elif st.in_top_loser_rank:
            rank_code = -st.in_top_loser_rank
        st.rank_history = (st.rank_history + [rank_code])[-24:]
        st.last_snapshot_at = now

    bucket["last_recorded"] = now.isoformat()
    bucket["snapshots"] = (bucket.get("snapshots") or [])[-120:]
    bucket["snapshots"].append(
        {
            "at": now.isoformat(),
            "gainers": gainer_labels[:top_n],
            "losers": loser_labels[:top_n],
        }
    )
    bucket["nifty_pct"] = _market_index_pct()
    return {"success": True, "recorded_at": now.isoformat(), "sectors_tracked": len(sectors)}


def _breadth_score(st: _SectorRuntime) -> float:
    """Proxy breadth from participation stability (pct sign consistency)."""
    hist = st.pct_history[-8:]
    if len(hist) < 2:
        return 50.0
    pos = sum(1 for x in hist if x > 0)
    ratio = pos / len(hist)
    return min(100.0, max(0.0, 50.0 + (ratio - 0.5) * 80.0))


def _persistence_score(st: _SectorRuntime) -> float:
    mins = st.minutes_top_gainer if st.in_top_gainer_rank else st.minutes_top_loser
    if not mins and st.minutes_weak:
        mins = st.minutes_weak
    return min(100.0, mins * (100.0 / 60.0))


def _volatility_stability_score(st: _SectorRuntime) -> float:
    hist = st.pct_history[-10:]
    if len(hist) < 3:
        return 55.0
    try:
        sd = statistics.pstdev(hist)
        mean_abs = max(0.05, abs(statistics.mean(hist)))
        cv = sd / mean_abs
        return min(100.0, max(0.0, 100.0 - cv * 120.0))
    except statistics.StatisticsError:
        return 55.0


def _leadership_continuity_score(st: _SectorRuntime) -> float:
    hist = st.rank_history[-8:]
    if len(hist) < 2:
        return 50.0
    changes = sum(1 for i in range(1, len(hist)) if hist[i] != hist[i - 1])
    return min(100.0, max(0.0, 100.0 - changes * 22.0))


def _rs_vs_index_score(st: _SectorRuntime, nifty_pct: float) -> float:
    diff = st.pct_change - nifty_pct
    if st.in_top_loser_rank:
        return min(100.0, max(0.0, 50.0 - diff * 15.0))
    return min(100.0, max(0.0, 50.0 + diff * 15.0))


def compute_sector_stability_score(st: _SectorRuntime, nifty_pct: float) -> float:
    """Sector Stability Score 0–100."""
    parts = (
        _breadth_score(st) * 0.25,
        _persistence_score(st) * 0.30,
        _volatility_stability_score(st) * 0.20,
        _leadership_continuity_score(st) * 0.15,
        _rs_vs_index_score(st, nifty_pct) * 0.10,
    )
    return round(min(100.0, max(0.0, sum(parts))), 1)


def _sector_tag(st: _SectorRuntime, sss: float) -> str:
    if st.in_top_loser_rank or sss < SSS_WEAK:
        return "WEAK_SECTOR"
    rank_changes = 0
    rh = st.rank_history[-6:]
    for i in range(1, len(rh)):
        if rh[i] != rh[i - 1]:
            rank_changes += 1
    if rank_changes >= ROTATION_RANK_CHANGES:
        return "ROTATIONAL"
    if st.minutes_top_gainer >= PERSISTENT_LEADER_MINUTES:
        return "PERSISTENT_LEADER"
    if st.in_top_gainer_rank and st.in_top_gainer_rank <= 2 and sss >= SSS_STRONG:
        return "TOP_SECTOR"
    if st.in_top_gainer_rank:
        return "TOP_SECTOR"
    return "NEUTRAL"


def _sector_status_display(st: _SectorRuntime, sss: float, tag: str) -> str:
    if st.in_top_gainer_rank:
        return f"🟢 Top #{st.in_top_gainer_rank}"
    if tag == "PERSISTENT_LEADER":
        return "🟢 Persistent Leader"
    if st.in_top_loser_rank:
        return f"🔴 Weak #{st.in_top_loser_rank}"
    if tag == "WEAK_SECTOR" or sss < SSS_WEAK:
        return "🔴 Weak Sector"
    if tag == "ROTATIONAL":
        return "🟠 Rotational"
    if sss >= SSS_STRONG:
        return "🟢 Stable"
    return "🟠 Neutral"


def sector_trade_badge(
    *,
    bull: bool,
    sss: float,
    in_top_gainer: bool,
    in_top_loser: bool,
    index_supportive: bool,
) -> Tuple[str, str]:
    """
    Returns (badge_code, display_label).
    badge_code: SECTOR_ALIGNED | SECTOR_CONTRADICTION | SECTOR_CONFIRMED_WEAKNESS | NEUTRAL
    """
    sector_strong = in_top_gainer or sss >= SSS_STRONG
    sector_weak = in_top_loser or sss < SSS_WEAK

    if bull:
        if sector_strong and index_supportive:
            return "SECTOR_ALIGNED", "🟢 Sector Aligned"
        if sector_strong and sector_weak:
            return "SECTOR_CONTRADICTION", "🟠 Sector Contradiction"
        if sector_strong and not sector_weak:
            return "SECTOR_ALIGNED", "🟢 Sector Aligned"
        if sector_weak:
            return "SECTOR_CONTRADICTION", "🟠 Sector Contradiction"
        return "NEUTRAL", "Sector Neutral"

    # SHORT
    if not bull and sector_weak:
        return "SECTOR_CONFIRMED_WEAKNESS", "🔴 Sector Confirmed Weakness"
    if sector_strong and not sector_weak:
        return "SECTOR_CONTRADICTION", "🟠 Sector Contradiction"
    if sector_weak:
        return "SECTOR_CONFIRMED_WEAKNESS", "🔴 Sector Confirmed Weakness"
    return "NEUTRAL", "Sector Neutral"


def sector_alignment_for_stock(stock: str, bull: bool) -> str:
    """Validation-engine alignment: aligned | neutral | conflicting."""
    lbl = nifty_sector_label_for_nse_equity(stock)
    if not lbl:
        return "neutral"
    sk = _session_key()
    bucket = _session_snapshots.get(sk) or {}
    st = (bucket.get("sectors") or {}).get(lbl)
    if not st:
        record_sector_snapshot()
        bucket = _session_snapshots.get(sk) or {}
        st = (bucket.get("sectors") or {}).get(lbl)
    if not st:
        return "neutral"
    nifty = float(bucket.get("nifty_pct") or 0.0)
    sss = compute_sector_stability_score(st, nifty)
    badge, _ = sector_trade_badge(
        bull=bull,
        sss=sss,
        in_top_gainer=bool(st.in_top_gainer_rank),
        in_top_loser=bool(st.in_top_loser_rank),
        index_supportive=nifty >= 0,
    )
    if badge == "SECTOR_ALIGNED":
        return "aligned"
    if badge in ("SECTOR_CONTRADICTION", "SECTOR_CONFIRMED_WEAKNESS"):
        return "conflicting"
    return "neutral"


def build_sector_persistence_heatmap(
    session_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Dashboard / API heatmap rows."""
    sk = _session_key(session_date)
    record_sector_snapshot(session_date=session_date)
    bucket = _session_snapshots.get(sk) or {}
    nifty = float(bucket.get("nifty_pct") or 0.0)
    rows: List[Dict[str, Any]] = []
    sectors: Dict[str, _SectorRuntime] = bucket.get("sectors") or {}
    for lbl, st in sorted(
        sectors.items(),
        key=lambda x: (-x[1].pct_change, x[0]),
    ):
        sss = compute_sector_stability_score(st, nifty)
        tag = _sector_tag(st, sss)
        persist_mins = (
            st.minutes_top_gainer
            if st.in_top_gainer_rank
            else (st.minutes_top_loser if st.in_top_loser_rank else st.minutes_weak)
        )
        rows.append(
            {
                "sector": lbl,
                "pct_change": round(st.pct_change, 4),
                "sector_stability_score": sss,
                "persistence_minutes": round(persist_mins, 1),
                "breadth_quality": round(_breadth_score(st), 1),
                "volatility_stability": round(_volatility_stability_score(st), 1),
                "sector_tag": tag,
                "sector_rank_gainer": st.in_top_gainer_rank,
                "sector_rank_loser": st.in_top_loser_rank,
                "sector_status": _sector_status_display(st, sss, tag),
            }
        )
    rows.sort(key=lambda r: (-float(r.get("sector_stability_score") or 0), r.get("sector")))
    return rows


def _sector_rank_boost(sss: float, in_top_gainer: bool, in_top_loser: bool) -> float:
    boost = (sss - 50.0) * (SECTOR_RANK_BOOST_MAX / 50.0)
    if in_top_gainer:
        boost += 2.0
    if in_top_loser:
        boost -= 4.0
    return max(-SECTOR_RANK_BOOST_MAX, min(SECTOR_RANK_BOOST_MAX, boost))


def enrich_row_sector_context(
    row: Dict[str, Any],
    *,
    nifty_pct: float,
    sector_states: Dict[str, _SectorRuntime],
) -> Dict[str, Any]:
    row = dict(row)
    stock = str(row.get("stock") or row.get("security") or "").strip().upper()
    lbl = nifty_sector_label_for_nse_equity(stock)
    row["sector_name"] = lbl
    if not lbl or lbl not in sector_states:
        row["sector_stability_score"] = 50.0
        row["sector_tag"] = "NEUTRAL"
        row["sector_status"] = "—"
        row["sector_trade_badge"] = "NEUTRAL"
        row["sector_trade_badge_label"] = ""
        row["sector_rank_boost"] = 0.0
        return row

    st = sector_states[lbl]
    sss = compute_sector_stability_score(st, nifty_pct)
    tag = _sector_tag(st, sss)
    bull = str(row.get("execution_bias") or row.get("direction") or "LONG").upper() != "SHORT"
    index_ok = nifty_pct >= 0 if bull else nifty_pct <= 0
    badge, badge_lbl = sector_trade_badge(
        bull=bull,
        sss=sss,
        in_top_gainer=bool(st.in_top_gainer_rank),
        in_top_loser=bool(st.in_top_loser_rank),
        index_supportive=index_ok,
    )
    boost = _sector_rank_boost(sss, bool(st.in_top_gainer_rank), bool(st.in_top_loser_rank))

    row["sector_strength_pct"] = round(st.pct_change, 4)
    row["sector_stability_score"] = sss
    row["sector_rank"] = st.in_top_gainer_rank or st.in_top_loser_rank
    row["sector_rank_side"] = (
        "gainer" if st.in_top_gainer_rank else ("loser" if st.in_top_loser_rank else None)
    )
    row["sector_persistence_minutes"] = round(
        st.minutes_top_gainer or st.minutes_top_loser or st.minutes_weak, 1
    )
    row["sector_tag"] = tag
    row["sector_status"] = _sector_status_display(st, sss, tag)
    row["sector_alignment_state"] = badge
    row["sector_trade_badge"] = badge
    row["sector_trade_badge_label"] = badge_lbl
    row["sector_rank_boost"] = round(boost, 2)
    if boost > 0 and row.get("conviction_score") is not None:
        try:
            row["conviction_score"] = min(
                100.0,
                float(row["conviction_score"]) + min(CONVICTION_BOOST_MAX, boost * 0.6),
            )
        except (TypeError, ValueError):
            pass
    if boost > 0 and row.get("tps_score") is not None:
        try:
            row["tps_score"] = min(100.0, float(row["tps_score"]) + boost * 0.35)
        except (TypeError, ValueError):
            pass
    if boost < 0 and row.get("ees_score") is not None:
        try:
            row["ees_score"] = max(0.0, float(row["ees_score"]) + boost * 0.25)
        except (TypeError, ValueError):
            pass
    return row


def apply_sector_intelligence_to_rows(
    rows: List[Dict[str, Any]],
    *,
    session_date: Optional[date] = None,
) -> Dict[str, Any]:
    """Record snapshot + enrich all rating rows with sector context."""
    record_sector_snapshot(session_date=session_date)
    sk = _session_key(session_date)
    bucket = _session_snapshots.get(sk) or {}
    nifty = float(bucket.get("nifty_pct") or 0.0)
    sector_states: Dict[str, _SectorRuntime] = bucket.get("sectors") or {}
    enriched = [enrich_row_sector_context(r, nifty_pct=nifty, sector_states=sector_states) for r in rows]
    heatmap = build_sector_persistence_heatmap(session_date=session_date)
    return {
        "rows": enriched,
        "sector_heatmap": heatmap,
        "nifty_pct": nifty,
    }


def sector_weighted_rank_adjustment(row: Dict[str, Any]) -> float:
    """Additive adjustment to stable rank score from sector SSS."""
    sss = float(row.get("sector_stability_score") or 50.0)
    boost = float(row.get("sector_rank_boost") or 0.0)
    return (sss - 50.0) * 0.12 + boost * 0.5

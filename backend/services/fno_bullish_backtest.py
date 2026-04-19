"""
F&O Bullish Trend scanner backtest.

Driven by a CSV of 15-minute scanner hits (``date`` columns like
``17-02-2026 1:00 pm``) and turns each symbol's appearance-run into a simulated
futures trade:

* Entry = LTP at ``anchor_scan_time + 5min`` IST (open of that 1-min candle).
  Default anchor = first 15-min scan of the run; optional anchor = second scan when
  ``entry_scan_index=1`` and runs with fewer than ``min_scan_count`` slots are omitted.
* Exit 1 = LTP at ``disappear_scan_time + 5min`` IST (1-min open) -- the first
  15-min scan where the symbol is *absent* after having appeared.
* Exit 2 = LTP at 15:15 IST (hard end-of-day).
* Quantity = 1 lot of the current-listed front-month future.

Re-entries within the same date are generated when a symbol vanishes and then
reappears at a later scan. Each contiguous run-of-scans becomes one trade.

Two PnLs are reported per trade (one per exit) so the caller can evaluate the
"ride till disappearance" vs "hold to EOD" styles independently. The JSON
artifact is the same shape as the NKS intraday backtest so the frontend follows
the same patterns.
"""
from __future__ import annotations

import csv
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

from backend.config import settings
from backend.services.nks_intraday_backtest import (
    _bucket_candles_by_hhmm,
    _candle_ohlcv,
    _index_instruments,
    _load_instruments,
    _price_at_slot,
    fetch_intraday_1m_candles,
    resolve_instrument,
)
from backend.services.upstox_service import UpstoxService

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

MARKET_OPEN_HHMM = (9, 15)
EOD_EXIT_HHMM = (15, 15)      # Exit-2: hard EOD
ENTRY_OFFSET_MIN = 5          # Entry  = anchor_scan + 5 min (anchor = run[entry_scan_index])
EXIT1_OFFSET_MIN = 5          # Exit-1 = disappear_scan + 5 min
SCAN_STEP_MIN = 15            # The scanner is a 15-minute schedule.


# ---------------------------------------------------------------------------
# Data shapes
# ---------------------------------------------------------------------------


@dataclass
class TradeRow:
    """One simulated trade = one contiguous scan-run for a (symbol, date)."""

    trade_date: str                # YYYY-MM-DD
    symbol: str
    marketcapname: str = ""
    sector: str = ""
    source: Optional[str] = None   # "FUT" or "EQ"
    trading_symbol: Optional[str] = None
    instrument_key: Optional[str] = None
    expiry_date: Optional[str] = None
    lot_size: Optional[int] = None
    fut_lot_size: Optional[int] = None

    run_index: int = 1             # 1 for first entry, 2+ for re-entries that day
    is_reentry: bool = False
    scan_count: int = 0
    first_scan_time: Optional[str] = None  # HH:MM
    last_scan_time: Optional[str] = None   # HH:MM
    disappear_scan_time: Optional[str] = None  # HH:MM or None if never disappeared

    entry_time: Optional[str] = None      # HH:MM of the 1-min candle used
    entry_price: Optional[float] = None

    exit1_time: Optional[str] = None
    exit1_price: Optional[float] = None
    exit1_pnl_points: Optional[float] = None
    exit1_pnl_rupees: Optional[float] = None
    exit1_kind: Optional[str] = None      # "disappear" | "never_disappeared" | "capped_15:15"

    exit2_time: Optional[str] = None      # always 15:15 if data present
    exit2_price: Optional[float] = None
    exit2_pnl_points: Optional[float] = None
    exit2_pnl_rupees: Optional[float] = None

    upstox_margin_rupees: Optional[float] = None   # POST /v2/charges/margin (FUT rows)
    upstox_margin_product: Optional[str] = None   # "I" or "D" when margin succeeded

    # Conviction (basis first 15-min scan slot of the run; VWAP 09:15 → first_scan; OI vs open)
    conviction_session_vwap_first_scan: Optional[float] = None
    conviction_price_vs_vwap_pct: Optional[float] = None   # (close@first_scan − VWAP) / VWAP · 100
    conviction_oi_change_pct: Optional[float] = None     # session OI % change (total OI proxy)

    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_date": self.trade_date,
            "symbol": self.symbol,
            "marketcapname": self.marketcapname,
            "sector": self.sector,
            "source": self.source,
            "trading_symbol": self.trading_symbol,
            "instrument_key": self.instrument_key,
            "expiry_date": self.expiry_date,
            "lot_size": self.lot_size,
            "fut_lot_size": self.fut_lot_size,
            "run_index": self.run_index,
            "is_reentry": self.is_reentry,
            "scan_count": self.scan_count,
            "first_scan_time": self.first_scan_time,
            "last_scan_time": self.last_scan_time,
            "disappear_scan_time": self.disappear_scan_time,
            "entry_time": self.entry_time,
            "entry_price": self.entry_price,
            "exit1_time": self.exit1_time,
            "exit1_price": self.exit1_price,
            "exit1_pnl_points": self.exit1_pnl_points,
            "exit1_pnl_rupees": self.exit1_pnl_rupees,
            "exit1_kind": self.exit1_kind,
            "exit2_time": self.exit2_time,
            "exit2_price": self.exit2_price,
            "exit2_pnl_points": self.exit2_pnl_points,
            "exit2_pnl_rupees": self.exit2_pnl_rupees,
            "upstox_margin_rupees": self.upstox_margin_rupees,
            "upstox_margin_product": self.upstox_margin_product,
            "conviction_session_vwap_first_scan": self.conviction_session_vwap_first_scan,
            "conviction_price_vs_vwap_pct": self.conviction_price_vs_vwap_pct,
            "conviction_oi_change_pct": self.conviction_oi_change_pct,
            "error": self.error,
            "notes": self.notes,
        }


# ---------------------------------------------------------------------------
# CSV loader (date like "17-02-2026 1:00 pm")
# ---------------------------------------------------------------------------


def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    # Upper-case am/pm for strptime('%p') portability.
    s_norm = s.replace(" am", " AM").replace(" pm", " PM").replace(" AM", " AM").replace(" PM", " PM")
    for fmt in ("%d-%m-%Y %I:%M %p", "%d-%m-%Y %I:%M:%S %p",
                "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s_norm, fmt)
        except ValueError:
            continue
    return None


def load_scanner_csv(csv_path: Path) -> List[Dict[str, Any]]:
    """Parse scanner CSV into ``{trade_date, hhmm, symbol, marketcapname, sector}`` rows.

    Rows with an unparseable datetime or empty symbol are silently dropped.
    """
    rows: List[Dict[str, Any]] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            dt = _parse_dt(str(raw.get("date") or ""))
            sym = str(raw.get("symbol") or "").strip().upper()
            if dt is None or not sym:
                continue
            rows.append(
                {
                    "trade_date": dt.date(),
                    "hhmm": (dt.hour, dt.minute),
                    "symbol": sym,
                    "marketcapname": (str(raw.get("marketcapname") or "").strip()),
                    "sector": (str(raw.get("sector") or "").strip()),
                }
            )
    return rows


def _group_by_date_symbol(
    rows: List[Dict[str, Any]],
) -> Dict[Tuple[date, str], List[Tuple[Tuple[int, int], str, str]]]:
    """``{(date, symbol): [((h, m), marketcap, sector), ...]}`` sorted by time."""
    out: Dict[Tuple[date, str], List[Tuple[Tuple[int, int], str, str]]] = {}
    for r in rows:
        key = (r["trade_date"], r["symbol"])
        out.setdefault(key, []).append(
            (r["hhmm"], r.get("marketcapname", ""), r.get("sector", ""))
        )
    # Sort each list by HH:MM ascending and dedupe exact HH:MM dupes.
    for k, v in out.items():
        seen: Dict[Tuple[int, int], Tuple[str, str]] = {}
        for hhmm, mcap, sec in v:
            if hhmm not in seen:
                seen[hhmm] = (mcap, sec)
        out[k] = sorted(
            [(hhmm, mcap, sec) for hhmm, (mcap, sec) in seen.items()],
            key=lambda x: x[0],
        )
    return out


def _split_runs(
    hhmm_list: List[Tuple[int, int]],
    *,
    step_min: int = SCAN_STEP_MIN,
) -> List[List[Tuple[int, int]]]:
    """Chunk ``hhmm_list`` into consecutive-scan runs. A gap of more than
    ``step_min`` between adjacent entries starts a new run (e.g., symbol was at
    11:00, 11:15, then absent at 11:30, then back at 11:45 -> two runs).
    """
    runs: List[List[Tuple[int, int]]] = []
    cur: List[Tuple[int, int]] = []
    for hhmm in hhmm_list:
        if not cur:
            cur.append(hhmm)
            continue
        prev_h, prev_m = cur[-1]
        prev_total = prev_h * 60 + prev_m
        h, m = hhmm
        cur_total = h * 60 + m
        if cur_total - prev_total == step_min:
            cur.append(hhmm)
        else:
            runs.append(cur)
            cur = [hhmm]
    if cur:
        runs.append(cur)
    return runs


def _add_minutes(hhmm: Tuple[int, int], mins: int) -> Tuple[int, int]:
    total = hhmm[0] * 60 + hhmm[1] + mins
    total = max(0, min(23 * 60 + 59, total))
    return (total // 60, total % 60)


def _fmt_hhmm(hhmm: Optional[Tuple[int, int]]) -> Optional[str]:
    if hhmm is None:
        return None
    return f"{hhmm[0]:02d}:{hhmm[1]:02d}"


def _cap_to_eod(hhmm: Tuple[int, int]) -> Tuple[Tuple[int, int], bool]:
    """Cap a target HH:MM to EOD_EXIT_HHMM. Returns (capped, was_capped)."""
    if (hhmm[0], hhmm[1]) > EOD_EXIT_HHMM:
        return EOD_EXIT_HHMM, True
    return hhmm, False


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------


def _session_candles(
    upstox: UpstoxService,
    instrument_key: str,
    d: date,
    cache: Dict[Tuple[str, date], Dict[Tuple[int, int], Dict[str, Any]]],
) -> Dict[Tuple[int, int], Dict[str, Any]]:
    key = (instrument_key, d)
    if key in cache:
        return cache[key]
    candles = fetch_intraday_1m_candles(upstox, instrument_key, d) or []
    buckets = _bucket_candles_by_hhmm(candles, d)
    cache[key] = buckets
    return buckets


def _margin_required_from_charge_response(resp: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(resp, dict) or resp.get("status") != "success":
        return None
    data = resp.get("data")
    if not isinstance(data, dict):
        return None
    for key in ("required_margin", "final_margin"):
        v = data.get(key)
        if v is not None:
            try:
                return round(float(v), 2)
            except (TypeError, ValueError):
                pass
    return None


def _attach_upstox_margin(upstox: UpstoxService, tr: TradeRow) -> None:
    """Set ``upstox_margin_rupees`` via Upstox Margin Details API for one long FUT lot."""
    if tr.source != "FUT" or not tr.instrument_key or not tr.fut_lot_size:
        return
    try:
        qty_i = int(tr.fut_lot_size)
    except (TypeError, ValueError):
        return
    if qty_i <= 0:
        return
    base: Dict[str, Any] = {
        "instrument_key": tr.instrument_key,
        "quantity": qty_i,
        "transaction_type": "BUY",
    }
    if tr.entry_price is not None:
        try:
            base["price"] = round(float(tr.entry_price), 2)
        except (TypeError, ValueError):
            pass
    # Try intraday (I) first for same-day closed simulation; fall back to delivery-style (D).
    for product in ("I", "D"):
        payload = [dict(base, product=product)]
        resp = upstox.get_charges_margin(payload)
        m = _margin_required_from_charge_response(resp)
        if m is not None:
            tr.upstox_margin_rupees = m
            tr.upstox_margin_product = product
            return
    tr.notes.append("upstox_margin_unavailable")


def _pnl(exit_px: Optional[float], entry_px: Optional[float], lot: Optional[int]) -> Tuple[Optional[float], Optional[float]]:
    if entry_px is None or exit_px is None:
        return None, None
    pts = float(exit_px) - float(entry_px)
    rs = pts * int(lot) if lot else None
    return round(pts, 2), (round(rs, 2) if rs is not None else None)


def _candle_oi(c: Any) -> Optional[float]:
    """Open interest on a 1-minute candle (F&O); EQ cash often has no OI."""
    if isinstance(c, dict):
        v = c.get("oi")
        if v is None:
            v = c.get("open_interest")
    elif isinstance(c, (list, tuple)) and len(c) >= 7:
        v = c[6]
    else:
        return None
    try:
        x = float(v)
        return x if x >= 0 else None
    except (TypeError, ValueError):
        return None


def _close_price_at_bucket(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    hhmm: Tuple[int, int],
) -> Optional[float]:
    c = buckets.get(hhmm)
    if not c:
        return None
    _, _, _, cl, _ = _candle_ohlcv(c)
    try:
        return float(cl) if cl is not None and float(cl) > 0 else None
    except (TypeError, ValueError):
        return None


def _first_oi_time_ordered(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    start_hhmm: Tuple[int, int],
    end_hhmm: Tuple[int, int],
) -> Optional[float]:
    """First non-null OI in ``[start_hhmm, end_hhmm]`` by clock order."""
    keys = sorted(k for k in buckets if start_hhmm <= k <= end_hhmm)
    for k in keys:
        oi = _candle_oi(buckets[k])
        if oi is not None:
            return float(oi)
    return None


def _oi_at_or_after(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    hhmm: Tuple[int, int],
    max_steps: int = 15,
) -> Optional[float]:
    """OI at ``hhmm`` or the next few minutes if missing."""
    keys = sorted(k for k in buckets if k >= hhmm)
    for i, k in enumerate(keys[: max_steps + 5]):
        if i > max_steps:
            break
        oi = _candle_oi(buckets[k])
        if oi is not None:
            return float(oi)
    return None


def _vwap_session_through(
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    start_hhmm: Tuple[int, int],
    end_hhmm: Tuple[int, int],
) -> Optional[float]:
    """Session VWAP from ``start_hhmm`` through ``end_hhmm`` inclusive (typical price × volume)."""
    tp_num = 0.0
    den = 0.0
    for k in sorted(k for k in buckets if start_hhmm <= k <= end_hhmm):
        c = buckets[k]
        o, h, l, cl, vol = _candle_ohlcv(c)
        try:
            v = float(vol) if vol is not None else 0.0
        except (TypeError, ValueError):
            v = 0.0
        if v <= 0:
            continue
        tp = None
        try:
            if h is not None and l is not None and cl is not None:
                tp = (float(h) + float(l) + float(cl)) / 3.0
            elif cl is not None:
                tp = float(cl)
        except (TypeError, ValueError):
            tp = None
        if tp is None or tp <= 0:
            continue
        tp_num += tp * v
        den += v
    if den <= 0:
        return None
    return round(tp_num / den, 4)


def _fill_conviction_raw_metrics(
    tr: TradeRow,
    buckets: Dict[Tuple[int, int], Dict[str, Any]],
    run: List[Tuple[int, int]],
    *,
    conviction_scan_index: int = 0,
) -> None:
    """Reference = scan slot ``run[conviction_scan_index]`` (15-min grid). No gating."""
    if not run:
        return
    i = max(0, min(conviction_scan_index, len(run) - 1))
    ref_scan = run[i]
    vwap = _vwap_session_through(buckets, MARKET_OPEN_HHMM, ref_scan)
    tr.conviction_session_vwap_first_scan = vwap
    cl = _close_price_at_bucket(buckets, ref_scan)
    if vwap is not None and float(vwap) > 0 and cl is not None:
        tr.conviction_price_vs_vwap_pct = round(
            (float(cl) - float(vwap)) / float(vwap) * 100.0, 4
        )

    # OI: % change from first available morning OI (09:15–09:29) to OI at/near first scan.
    # Total contract OI is used as proxy for positioning; separate long/short OI is not in feed.
    oi_open = _first_oi_time_ordered(buckets, MARKET_OPEN_HHMM, (9, 29))
    if oi_open is None:
        oi_open = _first_oi_time_ordered(buckets, MARKET_OPEN_HHMM, (10, 30))
    oi_ref = _oi_at_or_after(buckets, ref_scan)
    if (
        oi_open is not None
        and oi_ref is not None
        and float(oi_open) > 0
    ):
        tr.conviction_oi_change_pct = round(
            (float(oi_ref) - float(oi_open)) / float(oi_open) * 100.0, 4
        )


def _vwap_proximity_score_0_50(dist_pct: Optional[float]) -> float:
    """Reward price just above session VWAP (sweet spot ~+0.2% to +0.8%); soft otherwise."""
    if dist_pct is None:
        return 25.0
    d = float(dist_pct)
    if d < 0:
        return max(0.0, min(35.0, 35.0 + d * 4.0))
    if d < 0.2:
        return 35.0 + (d / 0.2) * 15.0
    if d <= 0.8:
        return 50.0
    if d <= 2.0:
        return 50.0 - ((d - 0.8) / 1.2) * 28.0
    return max(5.0, 22.0 - (d - 2.0) * 6.0)


def finalize_conviction_scores(rows: List[Dict[str, Any]]) -> None:
    """Populate ``conviction_score`` (0–100) and breakdown. OI leg: per-day rank → 0–50."""
    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_day[str(r.get("trade_date") or "")].append(r)

    for _day, day_rows in by_day.items():
        with_oi: List[Dict[str, Any]] = []
        for r in day_rows:
            v = r.get("conviction_oi_change_pct")
            if isinstance(v, (int, float)):
                with_oi.append(r)
        with_oi.sort(key=lambda x: float(x["conviction_oi_change_pct"]), reverse=True)
        n = len(with_oi)
        rank_map = {id(r): idx for idx, r in enumerate(with_oi)}

        for r in day_rows:
            vw = _vwap_proximity_score_0_50(
                float(r["conviction_price_vs_vwap_pct"])
                if isinstance(r.get("conviction_price_vs_vwap_pct"), (int, float))
                else None
            )
            rid = id(r)
            if rid in rank_map and n > 1:
                rk = rank_map[rid]
                oi_s = (n - 1 - rk) / (n - 1) * 50.0
            elif rid in rank_map and n == 1:
                oi_s = 50.0
            else:
                oi_s = 25.0
            total = round(min(100.0, max(0.0, oi_s + vw)), 1)
            r["conviction_score"] = total
            r["conviction_score_breakdown"] = {
                "oi": round(oi_s, 1),
                "vwap": round(vw, 1),
            }


def compute_trade_row(
    *,
    trade_date: date,
    symbol: str,
    run: List[Tuple[int, int]],
    run_index: int,
    marketcapname: str,
    sector: str,
    upstox: UpstoxService,
    fut_by_und: Dict[str, List[Dict[str, Any]]],
    eq_by_symbol: Dict[str, Dict[str, Any]],
    candle_cache: Dict[Tuple[str, date], Dict[Tuple[int, int], Dict[str, Any]]],
    entry_scan_index: int = 0,
    conviction_scan_index: int = 0,
) -> TradeRow:
    tr = TradeRow(
        trade_date=trade_date.isoformat(),
        symbol=symbol,
        marketcapname=marketcapname,
        sector=sector,
        run_index=run_index,
        is_reentry=(run_index > 1),
        scan_count=len(run),
        first_scan_time=_fmt_hhmm(run[0]),
        last_scan_time=_fmt_hhmm(run[-1]),
    )

    ref = resolve_instrument(
        symbol, trade_date, fut_by_und=fut_by_und, eq_by_symbol=eq_by_symbol
    )
    if ref is None or not ref.instrument_key:
        tr.error = "instrument_not_resolved"
        return tr
    tr.source = ref.source
    tr.trading_symbol = ref.trading_symbol
    tr.instrument_key = ref.instrument_key
    tr.expiry_date = ref.expiry_date.isoformat() if ref.expiry_date else None
    tr.lot_size = ref.lot_size
    tr.fut_lot_size = ref.fut_lot_size
    if ref.source == "EQ":
        tr.notes.append("EQ cash proxy (front-month FUT not in instruments snapshot)")

    buckets = _session_candles(upstox, ref.instrument_key, trade_date, candle_cache)
    if not buckets:
        tr.error = "no_session_candles"
        return tr

    # Entry = anchor scan (run[entry_scan_index]) + 5 min.
    anchor = run[entry_scan_index]
    entry_t = _add_minutes(anchor, ENTRY_OFFSET_MIN)
    entry_t, entry_capped = _cap_to_eod(entry_t)
    tr.entry_time = _fmt_hhmm(entry_t)
    tr.entry_price = _price_at_slot(buckets, entry_t)
    if entry_capped:
        tr.notes.append("entry_capped_to_15:15 (anchor scan too late)")

    # Disappear scan. The run ends at ``run[-1]``; the "next" scan slot on the
    # 15-min grid is where the symbol would be absent. We don't re-check the
    # raw dict here -- by construction of ``_split_runs`` the next slot is
    # exactly ``last + 15min`` and is the first time the symbol is missing.
    next_slot = _add_minutes(run[-1], SCAN_STEP_MIN)
    # If the run's last scan is the EOD scan (15:15), there's no "next" slot
    # in-session -- the symbol never "disappeared" intraday.
    never_disappeared = False
    if (run[-1][0], run[-1][1]) >= EOD_EXIT_HHMM:
        never_disappeared = True

    if never_disappeared:
        tr.disappear_scan_time = None
        tr.exit1_kind = "never_disappeared"
        tr.exit1_time = _fmt_hhmm(EOD_EXIT_HHMM)
        tr.exit1_price = _price_at_slot(buckets, EOD_EXIT_HHMM)
    else:
        tr.disappear_scan_time = _fmt_hhmm(next_slot)
        exit1_t = _add_minutes(next_slot, EXIT1_OFFSET_MIN)
        exit1_t, exit1_capped = _cap_to_eod(exit1_t)
        tr.exit1_time = _fmt_hhmm(exit1_t)
        tr.exit1_price = _price_at_slot(buckets, exit1_t)
        tr.exit1_kind = "capped_15:15" if exit1_capped else "disappear"

    # Exit 2 = 15:15 sharp.
    tr.exit2_time = _fmt_hhmm(EOD_EXIT_HHMM)
    tr.exit2_price = _price_at_slot(buckets, EOD_EXIT_HHMM)

    # PnLs (use fut_lot_size so EQ fallbacks still report a 1-lot position).
    lot = tr.fut_lot_size
    tr.exit1_pnl_points, tr.exit1_pnl_rupees = _pnl(tr.exit1_price, tr.entry_price, lot)
    tr.exit2_pnl_points, tr.exit2_pnl_rupees = _pnl(tr.exit2_price, tr.entry_price, lot)

    _fill_conviction_raw_metrics(
        tr, buckets, run, conviction_scan_index=conviction_scan_index
    )

    _attach_upstox_margin(upstox, tr)

    return tr


def run_backtest(
    rows: List[Dict[str, Any]],
    *,
    throttle_sec: float = 0.05,
    logger_fn=None,
    min_scan_count: int = 1,
    entry_scan_index: int = 0,
    conviction_scan_index: int = 0,
) -> List[Dict[str, Any]]:
    log = logger_fn or (lambda _msg: None)
    instruments = _load_instruments()
    fut_by_und, eq_by_symbol = _index_instruments(instruments)
    if not fut_by_und and not eq_by_symbol:
        raise RuntimeError("No instruments loaded -- cannot resolve FUTs")

    upstox = UpstoxService(settings.UPSTOX_API_KEY, settings.UPSTOX_API_SECRET)
    candle_cache: Dict[Tuple[str, date], Dict[Tuple[int, int], Dict[str, Any]]] = {}

    grouped = _group_by_date_symbol(rows)
    # Deterministic order: date asc, symbol asc.
    keys = sorted(grouped.keys(), key=lambda k: (k[0], k[1]))

    results: List[Dict[str, Any]] = []
    total_keys = len(keys)
    for idx, key in enumerate(keys, start=1):
        trade_date, symbol = key
        entries = grouped[key]
        hhmm_list = [e[0] for e in entries]
        mcap = entries[0][1] if entries else ""
        sector = entries[0][2] if entries else ""

        runs = _split_runs(hhmm_list, step_min=SCAN_STEP_MIN)
        qualifying = [
            r
            for r in runs
            if len(r) >= min_scan_count and entry_scan_index < len(r)
        ]
        for run_idx, run in enumerate(qualifying, start=1):
            try:
                tr = compute_trade_row(
                    trade_date=trade_date,
                    symbol=symbol,
                    run=run,
                    run_index=run_idx,
                    marketcapname=mcap,
                    sector=sector,
                    upstox=upstox,
                    fut_by_und=fut_by_und,
                    eq_by_symbol=eq_by_symbol,
                    candle_cache=candle_cache,
                    entry_scan_index=entry_scan_index,
                    conviction_scan_index=conviction_scan_index,
                )
            except Exception as e:  # noqa: BLE001
                tr = TradeRow(
                    trade_date=trade_date.isoformat(),
                    symbol=symbol,
                    run_index=run_idx,
                    is_reentry=(run_idx > 1),
                    scan_count=len(run),
                    first_scan_time=_fmt_hhmm(run[0]),
                    last_scan_time=_fmt_hhmm(run[-1]),
                )
                tr.error = f"compute_error: {e}"
            results.append(tr.to_dict())
            if throttle_sec > 0:
                time.sleep(throttle_sec)
        if idx % 25 == 0 or idx == total_keys:
            log(f"fno_bullish_backtest: {idx}/{total_keys} (date, symbol) pairs processed")

    finalize_conviction_scores(results)
    return results


# ---------------------------------------------------------------------------
# Output document
# ---------------------------------------------------------------------------


def build_output_document(
    results: List[Dict[str, Any]],
    *,
    min_scan_count: int = 1,
    entry_scan_index: int = 0,
    conviction_scan_index: int = 0,
) -> Dict[str, Any]:
    total = len(results)
    reentries = sum(1 for r in results if r.get("is_reentry"))
    with_entry = sum(1 for r in results if r.get("entry_price") is not None)
    fut_rows = sum(1 for r in results if r.get("source") == "FUT")
    eq_rows = sum(1 for r in results if r.get("source") == "EQ")

    sum_e1_rs = sum_e2_rs = 0.0
    pos_e1 = neg_e1 = pos_e2 = neg_e2 = 0
    worst_e1: Optional[float] = None
    best_e1: Optional[float] = None
    worst_e2: Optional[float] = None
    best_e2: Optional[float] = None
    never_disappeared = 0
    capped_exit1 = 0
    for r in results:
        if r.get("exit1_kind") == "never_disappeared":
            never_disappeared += 1
        elif r.get("exit1_kind") == "capped_15:15":
            capped_exit1 += 1

        v1 = r.get("exit1_pnl_rupees")
        if isinstance(v1, (int, float)):
            sum_e1_rs += float(v1)
            if v1 > 0:
                pos_e1 += 1
            elif v1 < 0:
                neg_e1 += 1
            if worst_e1 is None or v1 < worst_e1:
                worst_e1 = float(v1)
            if best_e1 is None or v1 > best_e1:
                best_e1 = float(v1)

        v2 = r.get("exit2_pnl_rupees")
        if isinstance(v2, (int, float)):
            sum_e2_rs += float(v2)
            if v2 > 0:
                pos_e2 += 1
            elif v2 < 0:
                neg_e2 += 1
            if worst_e2 is None or v2 < worst_e2:
                worst_e2 = float(v2)
            if best_e2 is None or v2 > best_e2:
                best_e2 = float(v2)

    strategy_note = (
        "F&O Bullish Trend (MA + ADX + MACD) — 15-min scanner · EOD 15:15 · "
        f"≥{min_scan_count} scan(s) per streak · entry @ scan[{entry_scan_index}] + {ENTRY_OFFSET_MIN} min"
        if (min_scan_count > 1 or entry_scan_index > 0)
        else "F&O Bullish Trend (MA + ADX + MACD) — 15-min scanner"
    )
    return {
        "generated_at": datetime.now(IST).isoformat(),
        "strategy": strategy_note,
        "summary": {
            "total_trades": total,
            "trades_with_entry": with_entry,
            "reentry_trades": reentries,
            "fut_rows": fut_rows,
            "eq_rows": eq_rows,
            "never_disappeared_rows": never_disappeared,
            "exit1_capped_to_1515": capped_exit1,
            "min_scan_count": min_scan_count,
            "entry_scan_index": entry_scan_index,
            "conviction_scan_index": conviction_scan_index,
            "entry_offset_min": ENTRY_OFFSET_MIN,
            "exit1_offset_min": EXIT1_OFFSET_MIN,
            "scan_step_min": SCAN_STEP_MIN,
            "eod_exit_time": "15:15",
            "exit1": {
                "sum_pnl_rupees": round(sum_e1_rs, 2),
                "positive_rows": pos_e1,
                "negative_rows": neg_e1,
                "worst_pnl_rupees": (round(worst_e1, 2) if worst_e1 is not None else None),
                "best_pnl_rupees": (round(best_e1, 2) if best_e1 is not None else None),
            },
            "exit2": {
                "sum_pnl_rupees": round(sum_e2_rs, 2),
                "positive_rows": pos_e2,
                "negative_rows": neg_e2,
                "worst_pnl_rupees": (round(worst_e2, 2) if worst_e2 is not None else None),
                "best_pnl_rupees": (round(best_e2, 2) if best_e2 is not None else None),
            },
        },
        "rows": results,
    }

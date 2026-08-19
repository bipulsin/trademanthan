"""Build frontend/public/hamoment.html from v2 multi-variant backtest results."""
from __future__ import annotations

import html as htmlmod
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.fetch_candles import BACKTEST_FROM, BACKTEST_TO
from backtest.run_backtest_v2 import VARIANTS

LOG_DIR = ROOT / "logs"
OUT_HTML = ROOT / "frontend" / "public" / "hamoment.html"
RESULTS_JSON = ROOT / "data" / "ha_backtest_results_v2.json"
logger = logging.getLogger("ha_report_v2")


def _setup_log() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_DIR / "report_v2.log"), logging.StreamHandler(sys.stdout)],
    )


def _esc(v: Any) -> str:
    if v is None:
        return ""
    return htmlmod.escape(str(v))


def _money(n: Any) -> str:
    try:
        return f"₹{float(n):,.0f}"
    except (TypeError, ValueError):
        return "₹0"


def _iso(v: Any) -> Any:
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _norm(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{k: _iso(v) for k, v in r.items()} for r in rows]


def _load_db() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    from backend.database import SessionLocal

    if SessionLocal is None:
        return [], []
    db = SessionLocal()
    try:
        trades = [dict(r) for r in db.execute(text("SELECT * FROM ha_backtest_trades ORDER BY signal_time")).mappings()]
        skipped = [dict(r) for r in db.execute(text("SELECT * FROM ha_skipped_trades ORDER BY signal_time")).mappings()]
        return trades, skipped
    except Exception as exc:
        logger.warning("DB read failed: %s", exc)
        return [], []
    finally:
        db.close()


def _row_class(t: Dict[str, Any]) -> str:
    if str(t.get("exit_reason") or "") == "TIME_EXIT":
        return "time"
    try:
        pnl = float(t.get("actual_pnl_rs") or 0)
    except (TypeError, ValueError):
        return ""
    if pnl > 0:
        return "win"
    if pnl < 0:
        return "loss"
    return ""


def _yn(v: Any) -> str:
    if v is None:
        return ""
    try:
        return "Y" if int(v) else "N"
    except (TypeError, ValueError):
        return "Y" if v else "N"


def _sl_pct(t: Dict[str, Any]) -> str:
    try:
        entry = float(t.get("entry_price") or 0)
        if not entry:
            return ""
        return f"{abs(float(t.get('sl_distance') or 0)) / entry * 100:.2f}"
    except (TypeError, ValueError):
        return ""


def _summarize(name: str, desc: str, trades: List[Dict[str, Any]], skipped: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    t1w = sum(1 for t in trades if int(t.get("t1_hit") or 0))
    t2w = sum(1 for t in trades if int(t.get("t2_hit") or 0))
    pnl = round(sum(float(t.get("actual_pnl_rs") or 0) for t in trades), 2)
    holds = [int(t.get("holding_min") or t.get("holding_minutes") or 0) for t in trades]
    best = max(trades, key=lambda t: float(t.get("actual_pnl_rs") or 0), default=None)
    worst = min(trades, key=lambda t: float(t.get("actual_pnl_rs") or 0), default=None)
    reasons = Counter(str(t.get("exit_reason") or "") for t in trades)
    skip_reasons = Counter(str(t.get("reason") or "") for t in skipped)
    sls = [float(t.get("sl_rs") or 0) for t in trades]
    long_t = [t for t in trades if str(t.get("direction")) == "LONG"]
    short_t = [t for t in trades if str(t.get("direction")) == "SHORT"]

    def side(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        nn = len(rows)
        wins = sum(1 for t in rows if float(t.get("actual_pnl_rs") or 0) > 0)
        return {
            "n": nn,
            "pnl": round(sum(float(t.get("actual_pnl_rs") or 0) for t in rows), 2),
            "wr": round(100.0 * wins / nn, 1) if nn else 0.0,
        }

    return {
        "name": name,
        "description": desc,
        "trades": n,
        "skipped": len(skipped),
        "wr_t1": round(100.0 * t1w / n, 1) if n else 0.0,
        "wr_t2": round(100.0 * t2w / n, 1) if n else 0.0,
        "pnl": pnl,
        "avg_hold": round(sum(holds) / len(holds), 1) if holds else 0,
        "best": f"{(best or {}).get('symbol', '')} {_money((best or {}).get('actual_pnl_rs'))}",
        "worst": f"{(worst or {}).get('symbol', '')} {_money((worst or {}).get('actual_pnl_rs'))}",
        "avg_sl_rs": round(sum(sls) / len(sls), 1) if sls else 0,
        "exit_counts": dict(reasons),
        "skip_counts": dict(skip_reasons),
        "long": side(long_t),
        "short": side(short_t),
        "sl_hit_pct": round(100.0 * reasons.get("SL_HIT", 0) / n, 1) if n else 0,
        "time_exit_pct": round(100.0 * reasons.get("TIME_EXIT", 0) / n, 1) if n else 0,
        "t_hit_pct": round(100.0 * (reasons.get("T1_THEN_T2", 0) + reasons.get("T2_HIT", 0) + reasons.get("T1_HIT", 0)) / n, 1) if n else 0,
    }


def _pnl_class(pnl: float) -> str:
    if pnl > 0:
        return "pos"
    if pnl >= -5000:
        return "warn"
    return "neg"


def _insights(summaries: List[Dict[str, Any]], trades: List[Dict[str, Any]], skipped: List[Dict[str, Any]]) -> str:
    if not summaries:
        return "No variant results."
    best = max(summaries, key=lambda s: float(s.get("pnl") or 0))
    base = next((s for s in summaries if s["name"] == "v1_baseline"), summaries[0])
    delta = float(best["pnl"]) - float(base["pnl"])
    pct = (delta / abs(float(base["pnl"])) * 100) if float(base["pnl"]) else 0
    long_best = max(summaries, key=lambda s: float((s.get("long") or {}).get("pnl") or 0))
    short_best = max(summaries, key=lambda s: float((s.get("short") or {}).get("pnl") or 0))
    skip_c = Counter(str(t.get("reason") or "") for t in skipped)
    top_skip, top_n = (skip_c.most_common(1)[0] if skip_c else ("none", 0))
    skip_pct = round(100.0 * top_n / len(skipped), 1) if skipped else 0
    candle_sl = [float(t.get("sl_rs") or 0) for t in trades if str(t.get("sl_logic_used")) == "CANDLE_LOW"]
    fixed_sl = [float(t.get("sl_rs") or 0) for t in trades if str(t.get("sl_logic_used")) == "FIXED_PCT"]
    avg_c = sum(candle_sl) / len(candle_sl) if candle_sl else 0
    avg_f = sum(fixed_sl) / len(fixed_sl) if fixed_sl else 0
    sl_red = round((avg_c - avg_f) / avg_c * 100, 1) if avg_c else 0
    time_skips = [t for t in skipped if str(t.get("reason")) == "ENTRY_AFTER_1330_CUTOFF"]
    v3 = next((s for s in summaries if s["name"] == "v3_time_fix"), None)
    v2 = next((s for s in summaries if s["name"] == "v2_rr_fix"), None)
    removed = (v2["trades"] - v3["trades"]) if v2 and v3 else len(time_skips)
    lines = [
        f"Best variant: {best['name']} ({best['description']}) with Net P&amp;L {_money(best['pnl'])} and Win Rate T1 {best['wr_t1']}%.",
        f"vs Baseline: improvement of {_money(delta)} ({delta:+.0f} / {pct:+.1f}% vs {base['name']}).",
        f"LONG trades best variant: {long_best['name']} (Win Rate: {(long_best.get('long') or {}).get('wr')}%, P&amp;L: {_money((long_best.get('long') or {}).get('pnl'))})",
        f"SHORT trades best variant: {short_best['name']} (Win Rate: {(short_best.get('short') or {}).get('wr')}%, P&amp;L: {_money((short_best.get('short') or {}).get('pnl'))})",
        f"Most trades filtered by: {htmlmod.escape(str(top_skip))} ({skip_pct}% of skips)",
        f"Fixed SL reduced SL Rs avg by {sl_red}% vs candle SL (₹{avg_f:,.0f} vs ₹{avg_c:,.0f}).",
        f"Time filter removed {removed} trades vs the R:R-only run (cutoff skip rows: {len(time_skips)}).",
    ]
    return "<br>\n".join(lines)


def _trade_rows(trades: List[Dict[str, Any]]) -> str:
    parts = []
    for i, t in enumerate(trades, 1):
        st = str(t.get("signal_time") or "")
        parts.append(
            "<tr class='{cls}'><td>{n}</td><td>{sym}</td><td>{d}</td><td>{tm}</td><td>{side}</td>"
            "<td>{entry}</td><td>{sl}</td><td>{slp}</td><td>{slrs}</td><td>{t1}</td><td>{t2}</td>"
            "<td>{t1h}</td><td>{t2h}</td><td>{slh}</td><td>{why}</td><td>{ax}</td>"
            "<td>{pnl}</td><td>{p1}</td><td>{p2}</td><td>{mfe}</td><td>{nv}</td><td>{hold}</td><td>{logic}</td></tr>".format(
                cls=_row_class(t),
                n=i,
                sym=_esc(t.get("symbol")),
                d=_esc(st[:10]),
                tm=_esc(st[11:19]),
                side=_esc(t.get("direction")),
                entry=_esc(t.get("entry_price")),
                sl=_esc(t.get("sl_price")),
                slp=_esc(_sl_pct(t)),
                slrs=_esc(t.get("sl_rs")),
                t1=_esc(t.get("t1_price")),
                t2=_esc(t.get("t2_price")),
                t1h=_yn(t.get("t1_hit")),
                t2h=_yn(t.get("t2_hit")),
                slh=_yn(t.get("sl_hit")),
                why=_esc(t.get("exit_reason")),
                ax=_esc(t.get("actual_exit_price")),
                pnl=_esc(t.get("actual_pnl_rs")),
                p1=_esc(t.get("pnl_t1_rs")),
                p2=_esc(t.get("pnl_t2_rs")),
                mfe=_esc(t.get("max_favorable")),
                nv=_yn(t.get("nifty_above_vwap")),
                hold=_esc(t.get("holding_min") or t.get("holding_minutes")),
                logic=_esc(t.get("sl_logic_used")),
            )
        )
    return "\n".join(parts)


def _skip_rows(rows: List[Dict[str, Any]]) -> str:
    parts = []
    for t in rows:
        st = str(t.get("signal_time") or "")
        parts.append(
            "<tr data-variant='{v}' data-reason='{r}'><td>{v}</td><td>{sym}</td><td>{d}</td><td>{tm}</td>"
            "<td>{side}</td><td>{entry}</td><td>{sl}</td><td>{slrs}</td><td>{lot}</td><td>{why}</td></tr>".format(
                v=_esc(t.get("variant")),
                r=_esc(t.get("reason")),
                sym=_esc(t.get("symbol")),
                d=_esc(st[:10]),
                tm=_esc(st[11:19]),
                side=_esc(t.get("direction")),
                entry=_esc(t.get("entry_price")),
                sl=_esc(t.get("sl_price")),
                slrs=_esc(t.get("sl_rs")),
                lot=_esc(t.get("lot_qty")),
                why=_esc(t.get("reason")),
            )
        )
    return "\n".join(parts)


def _variant_kpis(s: Dict[str, Any]) -> str:
    skip_txt = ", ".join(f"{k}:{v}" for k, v in (s.get("skip_counts") or {}).items()) or "0"
    exit_txt = f"SL {s.get('sl_hit_pct')}% · Time {s.get('time_exit_pct')}% · T1/T2 {s.get('t_hit_pct')}%"
    long_s = s.get("long") or {}
    short_s = s.get("short") or {}
    cards = [
        ("Total Trades", str(s.get("trades") or 0), ""),
        ("Win Rate T1 %", f"{s.get('wr_t1')}%", ""),
        ("Win Rate T2 %", f"{s.get('wr_t2')}%", ""),
        ("Net P&L (Rs)", _money(s.get("pnl")), _pnl_class(float(s.get("pnl") or 0))),
        ("Trades Skipped", f"{s.get('skipped')} ({skip_txt})", ""),
        ("Exit mix", exit_txt, ""),
        ("LONG vs SHORT P&L", f"{_money(long_s.get('pnl'))} / {_money(short_s.get('pnl'))}", ""),
        ("Avg SL distance (Rs)", f"₹{float(s.get('avg_sl_rs') or 0):,.0f}", ""),
    ]
    out = []
    for label, val, tone in cards:
        cls = ("val " + tone).strip()
        out.append(f'<div class="card"><div class="lbl">{_esc(label)}</div><div class="{cls}">{val}</div></div>')
    return "".join(out)


def _html(summaries: List[Dict[str, Any]], by_trades: Dict[str, List], by_skips: Dict[str, List], all_skips: List, insights: str) -> str:
    best_name = max(summaries, key=lambda s: float(s.get("pnl") or 0))["name"] if summaries else ""
    cmp_rows = []
    for s in summaries:
        gold = " gold" if s["name"] == best_name else ""
        cls = _pnl_class(float(s.get("pnl") or 0))
        cmp_rows.append(
            f"<tr class='{gold}'><td>{_esc(s['name'])}</td><td>{_esc(s['description'])}</td>"
            f"<td>{s['trades']}</td><td>{s['skipped']}</td><td>{s['wr_t1']}%</td><td>{s['wr_t2']}%</td>"
            f"<td class='{cls}'>{_money(s['pnl'])}</td><td>{s['avg_hold']}</td>"
            f"<td>{_esc(s['best'])}</td><td>{_esc(s['worst'])}</td></tr>"
        )
    details = []
    tabs = []
    panels = []
    for s in summaries:
        name = s["name"]
        details.append(
            f"<details class='variant' id='sec-{_esc(name)}'><summary>{_esc(name)} — {_esc(s['description'])}</summary>"
            f"<div class='kpis'>{_variant_kpis(s)}</div>"
            f"<div class='charts'>"
            f"<div class='card wide'><canvas id='cPnl_{_esc(name)}'></canvas></div>"
            f"<div class='card'><canvas id='cT1_{_esc(name)}'></canvas></div>"
            f"<div class='card'><canvas id='cT2_{_esc(name)}'></canvas></div>"
            f"<div class='card'><canvas id='cExit_{_esc(name)}'></canvas></div>"
            f"<div class='card wide'><canvas id='cSym_{_esc(name)}'></canvas></div>"
            f"</div></details>"
        )
        tabs.append(f"<button type='button' class='tab' data-tab='{_esc(name)}'>{_esc(name)}</button>")
        panels.append(
            f"<div class='tab-panel' id='tab-{_esc(name)}' hidden>"
            f"<div class='tbl-wrap'><table id='tbl-{_esc(name)}' class='ha display'>"
            "<thead><tr><th>#</th><th>Symbol</th><th>Date</th><th>Time</th><th>Dir</th>"
            "<th>Entry</th><th>SL</th><th>SL%</th><th>SL Rs</th><th>T1</th><th>T2</th>"
            "<th>T1 Hit</th><th>T2 Hit</th><th>SL Hit</th><th>Exit Reason</th>"
            "<th>Actual Exit</th><th>Actual P&amp;L Rs</th><th>P&amp;L T1 Rs</th><th>P&amp;L T2 Rs</th>"
            "<th>Max Favorable</th><th>Nifty&gt;VWAP?</th><th>Hold(min)</th><th>SL Logic</th></tr></thead>"
            f"<tbody>{_trade_rows(by_trades.get(name) or [])}</tbody></table></div></div>"
        )
    payload = {
        "summaries": summaries,
        "trades_by": {k: v for k, v in by_trades.items()},
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S IST"),
    }
    blob = json.dumps(payload, default=str).replace("<", "\\u003c")
    page = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HA Momentum Strategy — Multi-Variant Backtest v2</title>
<link rel="icon" type="image/x-icon" href="favicon.ico?v=3">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/datatables/1.10.21/css/jquery.dataTables.min.css">
<style>
:root { --bg:#0b1220; --card:#121a2b; --text:#e5e7eb; --muted:#94a3b8; --green:#22c55e; --red:#ef4444; --orange:#f59e0b; --line:#1f2a44; --gold:#fbbf24; }
* { box-sizing:border-box; }
body { margin:0; font-family: Inter, system-ui, sans-serif; background:var(--bg); color:var(--text); }
.wrap { max-width:1400px; margin:0 auto; padding:24px 16px 64px; }
h1 { margin:0 0 6px; font-size:1.55rem; }
.sub { color:var(--muted); margin:0 0 10px; }
.badge { display:inline-block; background:#1e293b; border:1px solid var(--line); border-radius:999px; padding:4px 12px; font-size:.8rem; margin-bottom:14px; }
nav a { color:#93c5fd; margin-right:12px; text-decoration:none; }
.sticky { position:sticky; top:0; z-index:5; background:rgba(11,18,32,.96); padding:8px 0 12px; }
table.ha { width:100%; border-collapse:collapse; font-size:0.8rem; }
table.ha th, table.ha td { border-bottom:1px solid var(--line); padding:7px 6px; text-align:left; white-space:nowrap; color:var(--text) !important; }
table.dataTable tbody tr, table.dataTable tbody tr.odd, table.dataTable tbody tr.even {
  background-color: var(--bg) !important; color: var(--text) !important;
}
table.dataTable tbody tr.win { background-color: rgba(34,197,94,.18) !important; }
table.dataTable tbody tr.loss { background-color: rgba(239,68,68,.18) !important; }
table.dataTable tbody tr.time { background-color: rgba(245,158,11,.18) !important; }
.win { background-color: rgba(34,197,94,.18); }
.loss { background-color: rgba(239,68,68,.18); }
.time { background-color: rgba(245,158,11,.18); }
.pos { color:var(--green); font-weight:700; }
.neg { color:var(--red); font-weight:700; }
.warn { color:var(--orange); font-weight:700; }
tr.gold td { box-shadow: inset 0 0 0 2px var(--gold); }
.kpis { display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:10px; margin:12px 0; }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px 14px; }
.card .lbl { font-size:0.72rem; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); }
.card .val { font-size:1.1rem; font-weight:800; margin-top:4px; }
.charts { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
.charts .wide { grid-column:1 / -1; }
.tbl-wrap { overflow-x:auto; }
details.variant { margin:12px 0; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:10px 12px; }
.tabs { display:flex; flex-wrap:wrap; gap:6px; margin:12px 0; }
.tab { background:#1e293b; color:var(--text); border:1px solid var(--line); border-radius:8px; padding:6px 10px; cursor:pointer; }
.tab.active { border-color:var(--gold); }
.dataTables_wrapper, .dataTables_wrapper .dataTables_length, .dataTables_wrapper .dataTables_filter,
.dataTables_wrapper .dataTables_info, .dataTables_wrapper .dataTables_paginate { color:var(--text) !important; }
.dataTables_wrapper .dataTables_filter input, .dataTables_wrapper .dataTables_length select {
  background: var(--card); color: var(--text); border: 1px solid var(--line);
}
.insight { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px; line-height:1.55; }
@media (max-width:900px) { .charts { grid-template-columns:1fr; } }
</style>
</head>
<body>
<div class="wrap">
  <h1>HA Momentum Strategy — Multi-Variant Backtest v2</h1>
  <p class="sub">15-Min Futures | NSE F&amp;O | 17-Jul-2026 to 19-Aug-2026</p>
  <div class="badge">9 Variants Tested (v1–v8 + v6b 0.3% SL) | 200 Symbols · Last run: %%UPDATED%%</div>
  <nav>
    <a href="#summary">Summary</a>
    <a href="#charts">Charts</a>
    <a href="#trades">Trades</a>
    <a href="#skipped">Skipped</a>
    <a href="#insights">Insights</a>
  </nav>
  <section id="summary" class="sticky">
    <h2>Variant comparison</h2>
    <div class="tbl-wrap">
    <table class="ha">
      <thead><tr>
        <th>Variant</th><th>Description</th><th>Trades</th><th>Skipped</th>
        <th>Win% T1</th><th>Win% T2</th><th>Net P&amp;L (Rs)</th>
        <th>Avg Hold (min)</th><th>Best</th><th>Worst</th>
      </tr></thead>
      <tbody>%%CMP%%</tbody>
    </table>
    </div>
  </section>
  <section id="charts">
    <h2>Side-by-side key metrics</h2>
    <div class="card wide"><canvas id="cCompare"></canvas></div>
    %%DETAILS%%
  </section>
  <section id="trades">
    <h2>All trades</h2>
    <div class="tabs">%%TABS%%</div>
    %%PANELS%%
  </section>
  <section id="skipped">
    <h2>Skipped trades</h2>
    <label>Variant <select id="fVar"><option value="">All</option>%%VAROPTS%%</select></label>
    <label>Reason <select id="fReason"><option value="">All</option>%%REASONOPTS%%</select></label>
    <div class="tbl-wrap">
    <table id="tblSkip" class="ha display">
      <thead><tr>
        <th>Variant</th><th>Symbol</th><th>Date</th><th>Time</th><th>Direction</th>
        <th>Entry Price</th><th>SL Price</th><th>SL Rs</th><th>Lot Qty</th><th>Skip Reason</th>
      </tr></thead>
      <tbody>%%SKIP%%</tbody>
    </table>
    </div>
  </section>
  <section id="insights">
    <h2>Insight panel</h2>
    <div class="insight">%%INSIGHT%%</div>
  </section>
</div>
<script>window.HA_DATA = %%PAYLOAD%%;</script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.7.1/jquery.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/datatables/1.10.21/js/jquery.dataTables.min.js"></script>
<script>
(function () {
  var DATA = window.HA_DATA || {summaries:[], trades_by:{}};
  var sums = DATA.summaries || [];
  try {
    if (window.jQuery && jQuery.fn.dataTable) {
      sums.forEach(function (s) {
        jQuery('#tbl-' + s.name).DataTable({ pageLength: 25, order: [[2, 'desc'], [3, 'desc']] });
      });
      jQuery('#tblSkip').DataTable({ pageLength: 25 });
    }
  } catch (e) {}
  var tabs = document.querySelectorAll('.tab');
  var panels = document.querySelectorAll('.tab-panel');
  function showTab(name) {
    tabs.forEach(function (b) { b.classList.toggle('active', b.getAttribute('data-tab') === name); });
    panels.forEach(function (p) { p.hidden = p.id !== ('tab-' + name); });
  }
  tabs.forEach(function (b) { b.addEventListener('click', function () { showTab(b.getAttribute('data-tab')); }); });
  if (sums[0]) showTab(sums[0].name);
  function applySkipFilter() {
    var v = document.getElementById('fVar').value;
    var r = document.getElementById('fReason').value;
    document.querySelectorAll('#tblSkip tbody tr').forEach(function (tr) {
      var okv = !v || tr.getAttribute('data-variant') === v;
      var okr = !r || tr.getAttribute('data-reason') === r;
      tr.style.display = (okv && okr) ? '' : 'none';
    });
  }
  document.getElementById('fVar').addEventListener('change', applySkipFilter);
  document.getElementById('fReason').addEventListener('change', applySkipFilter);
  try {
    if (typeof Chart === 'undefined') return;
    var opt = { plugins: { legend: { labels: { color: '#e5e7eb' } } }, scales: {
      x: { ticks: { color: '#94a3b8' } },
      y: { type: 'linear', position: 'left', ticks: { color: '#94a3b8' } },
      y2: { type: 'linear', position: 'right', grid: { drawOnChartArea: false }, ticks: { color: '#94a3b8' } }
    }};
    new Chart(document.getElementById('cCompare'), {
      type: 'bar',
      data: {
        labels: sums.map(function (s) { return s.name; }),
        datasets: [
          { label: 'Net P&L (Rs)', data: sums.map(function (s) { return s.pnl; }), backgroundColor: '#38bdf8', yAxisID: 'y' },
          { label: 'Win Rate T1 %', data: sums.map(function (s) { return s.wr_t1; }), backgroundColor: '#22c55e', yAxisID: 'y2' },
          { label: 'Trades', data: sums.map(function (s) { return s.trades; }), backgroundColor: '#a78bfa', yAxisID: 'y2' },
          { label: 'Skipped', data: sums.map(function (s) { return s.skipped; }), backgroundColor: '#f59e0b', yAxisID: 'y2' }
        ]
      },
      options: opt
    });
    var pieOpt = { plugins: { legend: { labels: { color: '#e5e7eb' } } } };
    sums.forEach(function (s) {
      var trades = (DATA.trades_by && DATA.trades_by[s.name]) || [];
      var byDate = {};
      trades.forEach(function (t) {
        var d = String(t.signal_time || '').slice(0, 10);
        byDate[d] = (byDate[d] || 0) + Number(t.actual_pnl_rs || 0);
      });
      var dates = Object.keys(byDate).sort();
      var run = 0;
      var cum = dates.map(function (d) { run += byDate[d]; return run; });
      new Chart(document.getElementById('cPnl_' + s.name), { type: 'line', data: { labels: dates, datasets: [{ label: 'Cumulative P&L', data: cum, borderColor: '#22c55e', tension: 0.2 }] }, options: { plugins: { legend: { labels: { color: '#e5e7eb' } } }, scales: { x: { ticks: { color: '#94a3b8' } }, y: { ticks: { color: '#94a3b8' } } } } });
      var t1w = trades.filter(function (t) { return Number(t.t1_hit); }).length;
      var t2w = trades.filter(function (t) { return Number(t.t2_hit); }).length;
      new Chart(document.getElementById('cT1_' + s.name), { type: 'doughnut', data: { labels: ['T1 Win', 'T1 Loss'], datasets: [{ data: [t1w, Math.max(0, trades.length - t1w)], backgroundColor: ['#22c55e', '#ef4444'] }] }, options: pieOpt });
      new Chart(document.getElementById('cT2_' + s.name), { type: 'doughnut', data: { labels: ['T2 Win', 'T2 Loss'], datasets: [{ data: [t2w, Math.max(0, trades.length - t2w)], backgroundColor: ['#22c55e', '#ef4444'] }] }, options: pieOpt });
      var reasons = {};
      trades.forEach(function (t) { var k = t.exit_reason || 'NA'; reasons[k] = (reasons[k] || 0) + 1; });
      new Chart(document.getElementById('cExit_' + s.name), { type: 'bar', data: { labels: Object.keys(reasons), datasets: [{ label: 'Exits', data: Object.values(reasons), backgroundColor: '#38bdf8' }] }, options: { plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#94a3b8' } }, y: { ticks: { color: '#94a3b8' } } } } });
      var bySym = {};
      trades.forEach(function (t) { var sy = t.symbol || ''; bySym[sy] = (bySym[sy] || 0) + Number(t.actual_pnl_rs || 0); });
      var top = Object.entries(bySym).sort(function (a, b) { return Math.abs(b[1]) - Math.abs(a[1]); }).slice(0, 15);
      new Chart(document.getElementById('cSym_' + s.name), { type: 'bar', data: { labels: top.map(function (x) { return x[0]; }), datasets: [{ label: 'P&L', data: top.map(function (x) { return x[1]; }), backgroundColor: top.map(function (x) { return x[1] >= 0 ? '#22c55e' : '#ef4444'; }) }] }, options: { indexAxis: 'y', plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#94a3b8' } }, y: { ticks: { color: '#94a3b8' } } } } });
    });
  } catch (e) {}
})();
</script>
</body>
</html>
"""
    names = [s["name"] for s in summaries]
    reasons = sorted({str(t.get("reason") or "") for t in all_skips if t.get("reason")})
    varopts = "".join(f"<option value='{_esc(n)}'>{_esc(n)}</option>" for n in names)
    reasonopts = "".join(f"<option value='{_esc(r)}'>{_esc(r)}</option>" for r in reasons)
    return (
        page.replace("%%UPDATED%%", htmlmod.escape(datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")))
        .replace("%%CMP%%", "\n".join(cmp_rows))
        .replace("%%DETAILS%%", "\n".join(details))
        .replace("%%TABS%%", "\n".join(tabs))
        .replace("%%PANELS%%", "\n".join(panels))
        .replace("%%SKIP%%", _skip_rows(all_skips))
        .replace("%%INSIGHT%%", insights)
        .replace("%%VAROPTS%%", varopts)
        .replace("%%REASONOPTS%%", reasonopts)
        .replace("%%PAYLOAD%%", blob)
    )


def main() -> None:
    _setup_log()
    trades, skipped = _load_db()
    if RESULTS_JSON.exists():
        blob = json.loads(RESULTS_JSON.read_text(encoding="utf-8"))
        if blob.get("trades"):
            trades = blob.get("trades") or trades
            skipped = blob.get("skipped") or skipped
            logger.info("loaded results JSON")
    trades = _norm(trades)
    skipped = _norm(skipped)
    names = [v["name"] for v in VARIANTS]
    desc = {v["name"]: v["description"] for v in VARIANTS}
    by_t: Dict[str, List] = defaultdict(list)
    by_s: Dict[str, List] = defaultdict(list)
    for t in trades:
        v = str(t.get("variant") or "")
        if v in names:
            by_t[v].append(t)
    for t in skipped:
        v = str(t.get("variant") or "")
        if v in names:
            by_s[v].append(t)
    summaries = [_summarize(n, desc.get(n, n), by_t.get(n) or [], by_s.get(n) or []) for n in names]
    insights = _insights(summaries, trades, skipped)
    html = _html(summaries, by_t, by_s, skipped, insights)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.write_text(html, encoding="utf-8")
    size = OUT_HTML.stat().st_size
    if size < 500_000:
        logger.warning("hamoment.html is %s bytes (<500KB) — some variant data may be missing", size)
    print(f"Wrote {OUT_HTML}  size={size} bytes  variants={len(summaries)}")


if __name__ == "__main__":
    main()

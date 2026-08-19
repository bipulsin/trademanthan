"""Build public frontend/public/hamoment.html from HA backtest tables (no login)."""
from __future__ import annotations

import html as htmlmod
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtest.fetch_candles import BACKTEST_FROM, BACKTEST_TO

LOG_DIR = ROOT / "logs"
OUT_HTML = ROOT / "frontend" / "public" / "hamoment.html"
RESULTS_JSON = ROOT / "data" / "ha_backtest_results.json"

logger = logging.getLogger("ha_report")


def _setup_log() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "report.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _load_db() -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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


def _iso(v: Any) -> Any:
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


def _norm(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        item = {k: _iso(v) for k, v in r.items()}
        out.append(item)
    return out


def _summarize(trades: List[Dict[str, Any]], skipped: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    t1w = sum(1 for t in trades if int(t.get("t1_hit") or 0))
    t2w = sum(1 for t in trades if int(t.get("t2_hit") or 0))
    pnl = lambda k: round(sum(float(t.get(k) or 0) for t in trades), 2)
    rr = []
    holds = []
    beyond = []
    for t in trades:
        sl_rs = float(t.get("sl_rs") or 0)
        if sl_rs:
            rr.append(float(t.get("actual_pnl_rs") or 0) / sl_rs)
        if t.get("holding_min") is not None:
            holds.append(int(t.get("holding_min") or 0))
        direction = str(t.get("direction") or "")
        mfe = float(t.get("max_favorable") or 0)
        t2 = float(t.get("t2_price") or 0)
        if direction == "LONG" and t2:
            beyond.append(max(0.0, mfe - t2))
        elif direction == "SHORT" and t2:
            beyond.append(max(0.0, t2 - mfe))
    best = max(trades, key=lambda t: float(t.get("actual_pnl_rs") or 0), default=None)
    worst = min(trades, key=lambda t: float(t.get("actual_pnl_rs") or 0), default=None)

    def dir_stats(side: str) -> Dict[str, Any]:
        sub = [t for t in trades if str(t.get("direction")) == side]
        wins = sum(1 for t in sub if float(t.get("actual_pnl_rs") or 0) > 0)
        losses = sum(1 for t in sub if float(t.get("actual_pnl_rs") or 0) <= 0)
        return {
            "n": len(sub),
            "wins": wins,
            "losses": losses,
            "pnl": round(sum(float(t.get("actual_pnl_rs") or 0) for t in sub), 2),
        }

    return {
        "signals": n + len(skipped),
        "trades": n,
        "skipped": len(skipped),
        "wr_t1": round(100.0 * t1w / n, 2) if n else 0,
        "wr_t2": round(100.0 * t2w / n, 2) if n else 0,
        "pnl_t1": pnl("pnl_t1_rs"),
        "pnl_t2": pnl("pnl_t2_rs"),
        "pnl_actual": pnl("actual_pnl_rs"),
        "avg_rr": round(sum(rr) / len(rr), 2) if rr else 0,
        "best": {"symbol": (best or {}).get("symbol"), "pnl": float((best or {}).get("actual_pnl_rs") or 0)},
        "worst": {"symbol": (worst or {}).get("symbol"), "pnl": float((worst or {}).get("actual_pnl_rs") or 0)},
        "avg_hold": round(sum(holds) / len(holds), 1) if holds else 0,
        "long": dir_stats("LONG"),
        "short": dir_stats("SHORT"),
        "time_exits": sum(1 for t in trades if str(t.get("exit_reason")) == "TIME_EXIT"),
        "mfe_beyond_t2": round(sum(beyond) / len(beyond), 2) if beyond else 0,
        "from": BACKTEST_FROM,
        "to": BACKTEST_TO,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S IST"),
    }
def _esc(v: Any) -> str:
    if v is None:
        return ""
    return htmlmod.escape(str(v))


def _money(n: Any) -> str:
    try:
        return f"₹{float(n):,.0f}"
    except (TypeError, ValueError):
        return "₹0"


def _yn(v: Any) -> str:
    try:
        return "Y" if int(v) else ""
    except (TypeError, ValueError):
        return "Y" if v else ""


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


def _kpi_card(label: str, value: str, tone: str = "") -> str:
    cls = ("val " + tone).strip()
    return f'<div class="card"><div class="lbl">{_esc(label)}</div><div class="{cls}">{value}</div></div>'


def _trade_rows(trades: List[Dict[str, Any]]) -> str:
    parts = []
    for i, t in enumerate(trades, 1):
        st = str(t.get("signal_time") or "")
        parts.append(
            "<tr class='{cls}'><td>{n}</td><td>{sym}</td><td>{d}</td><td>{tm}</td>"
            "<td>{side}</td><td>{entry}</td><td>{sl}</td><td>{t1}</td><td>{t2}</td>"
            "<td>{csl}</td><td>{sz}</td><td>{t1h}</td><td>{t2h}</td><td>{slh}</td>"
            "<td>{why}</td><td>{ax}</td><td>{pnl}</td><td>{p1}</td><td>{p2}</td>"
            "<td>{mfe}</td><td>{lot}</td><td>{hold}</td></tr>".format(
                cls=_row_class(t),
                n=i,
                sym=_esc(t.get("symbol")),
                d=_esc(st[:10]),
                tm=_esc(st[11:19]),
                side=_esc(t.get("direction")),
                entry=_esc(t.get("entry_price")),
                sl=_esc(t.get("sl_price")),
                t1=_esc(t.get("t1_price")),
                t2=_esc(t.get("t2_price")),
                csl=_yn(t.get("sl_used_prev_candle")),
                sz=_esc(t.get("entry_candle_size_pct")),
                t1h=_yn(t.get("t1_hit")),
                t2h=_yn(t.get("t2_hit")),
                slh=_yn(t.get("sl_hit")),
                why=_esc(t.get("exit_reason")),
                ax=_esc(t.get("actual_exit_price")),
                pnl=_esc(t.get("actual_pnl_rs")),
                p1=_esc(t.get("pnl_t1_rs")),
                p2=_esc(t.get("pnl_t2_rs")),
                mfe=_esc(t.get("max_favorable")),
                lot=_esc(t.get("lot_qty")),
                hold=_esc(t.get("holding_min")),
            )
        )
    return "\n".join(parts)


def _skip_rows(rows: List[Dict[str, Any]]) -> str:
    parts = []
    for i, t in enumerate(rows, 1):
        st = str(t.get("signal_time") or "")
        parts.append(
            "<tr><td>{n}</td><td>{sym}</td><td>{d}</td><td>{tm}</td><td>{side}</td>"
            "<td>{entry}</td><td>{sl}</td><td>{slrs}</td><td>{lot}</td><td>{why}</td></tr>".format(
                n=i,
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


def _html(summary: Dict[str, Any], trades: List[Dict[str, Any]], skipped: List[Dict[str, Any]]) -> str:
    s = summary
    long_s = s.get("long") or {}
    short_s = s.get("short") or {}
    best = s.get("best") or {}
    worst = s.get("worst") or {}
    kpis = "".join(
        [
            _kpi_card("Total signals", str(s.get("signals") or 0)),
            _kpi_card("Trades taken", str(s.get("trades") or 0)),
            _kpi_card("Skipped (SL>5K)", str(s.get("skipped") or 0)),
            _kpi_card("Win rate T1", f"{s.get('wr_t1') or 0}%"),
            _kpi_card("Win rate T2", f"{s.get('wr_t2') or 0}%"),
            _kpi_card("P&L T1", _money(s.get("pnl_t1")), "pos" if float(s.get("pnl_t1") or 0) >= 0 else "neg"),
            _kpi_card("P&L T2", _money(s.get("pnl_t2")), "pos" if float(s.get("pnl_t2") or 0) >= 0 else "neg"),
            _kpi_card(
                "Actual P&L",
                _money(s.get("pnl_actual")),
                "pos" if float(s.get("pnl_actual") or 0) >= 0 else "neg",
            ),
            _kpi_card("Avg R:R", str(s.get("avg_rr") or 0)),
            _kpi_card("Best trade", f"{_esc(best.get('symbol'))} {_money(best.get('pnl'))}"),
            _kpi_card("Worst trade", f"{_esc(worst.get('symbol'))} {_money(worst.get('pnl'))}"),
            _kpi_card("Avg hold (min)", str(s.get("avg_hold") or 0)),
            _kpi_card(
                "LONG w/l/pnl",
                f"{long_s.get('wins', 0)}/{long_s.get('losses', 0)} {_money(long_s.get('pnl'))}",
            ),
            _kpi_card(
                "SHORT w/l/pnl",
                f"{short_s.get('wins', 0)}/{short_s.get('losses', 0)} {_money(short_s.get('pnl'))}",
            ),
            _kpi_card("Time exits", str(s.get("time_exits") or 0)),
            _kpi_card("Avg MFE beyond T2", str(s.get("mfe_beyond_t2") or 0)),
        ]
    )
    payload = json.dumps({"summary": summary, "trades": trades, "skipped": skipped}, default=str)
    payload = payload.replace("<", "\\u003c")
    page = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HA Momentum Strategy — Backtest Results</title>
<link rel="icon" type="image/x-icon" href="favicon.ico?v=3">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/datatables/1.10.21/css/jquery.dataTables.min.css">
<style>
:root { --bg:#0b1220; --card:#121a2b; --text:#e5e7eb; --muted:#94a3b8; --green:#22c55e; --red:#ef4444; --orange:#f59e0b; --line:#1f2a44; }
* { box-sizing:border-box; }
body { margin:0; font-family: Inter, system-ui, sans-serif; background:var(--bg); color:var(--text); }
.wrap { max-width:1280px; margin:0 auto; padding:24px 16px 48px; }
h1 { margin:0 0 6px; font-size:1.6rem; }
.sub { color:var(--muted); margin:0 0 18px; }
.kpis { display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:10px; margin-bottom:20px; }
.card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px 14px; }
.card .lbl { font-size:0.72rem; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); }
.card .val { font-size:1.25rem; font-weight:800; margin-top:4px; }
.pos { color:var(--green); } .neg { color:var(--red); }
.charts { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:20px; }
.charts .wide { grid-column:1 / -1; }
.tbl-wrap { overflow-x:auto; }
table.ha { width:100%; border-collapse:collapse; font-size:0.82rem; color:var(--text); }
table.ha th, table.ha td { border-bottom:1px solid var(--line); padding:7px 6px; text-align:left; white-space:nowrap; }
.win { background:rgba(34,197,94,.12); }
.loss { background:rgba(239,68,68,.12); }
.time { background:rgba(245,158,11,.12); }
details { margin-top:18px; background:var(--card); border-radius:12px; padding:12px; border:1px solid var(--line); }
.dataTables_wrapper { color:var(--text); }
@media (max-width:900px) { .charts { grid-template-columns:1fr; } }
</style>
</head>
<body>
<div class="wrap">
  <h1>HA Momentum Strategy — Backtest Results</h1>
  <p class="sub">15-Min | NSE F&amp;O | 17-Jul-2026 to 19-Aug-2026 · Last updated %%UPDATED%%</p>
  <div class="kpis" id="kpis">%%KPIS%%</div>
  <div class="charts">
    <div class="card wide"><canvas id="cPnl"></canvas></div>
    <div class="card"><canvas id="cT1"></canvas></div>
    <div class="card"><canvas id="cT2"></canvas></div>
    <div class="card wide"><canvas id="cSym"></canvas></div>
    <div class="card"><canvas id="cDir"></canvas></div>
  </div>
  <h2>All trades (%%NTRADES%%)</h2>
  <div class="tbl-wrap">
  <table id="tbl" class="ha display">
    <thead><tr>
      <th>#</th><th>Symbol</th><th>Date</th><th>Time</th><th>Direction</th>
      <th>Entry</th><th>SL</th><th>T1</th><th>T2</th><th>Candle SL?</th>
      <th>Entry candle %</th><th>T1 Hit</th><th>T2 Hit</th><th>SL Hit</th>
      <th>Exit reason</th><th>Actual exit</th><th>Actual P&amp;L</th>
      <th>P&amp;L T1</th><th>P&amp;L T2</th><th>Max favorable</th><th>Lot</th><th>Hold min</th>
    </tr></thead>
    <tbody>
%%TRADES%%
    </tbody>
  </table>
  </div>
  <details open>
    <summary>Skipped trades (SL &gt; ₹5,000) — %%NSKIP%%</summary>
    <div class="tbl-wrap">
    <table id="tblSkip" class="ha display">
      <thead><tr>
        <th>#</th><th>Symbol</th><th>Date</th><th>Time</th><th>Direction</th>
        <th>Entry</th><th>SL</th><th>SL Rs</th><th>Lot</th><th>Reason</th>
      </tr></thead>
      <tbody>
%%SKIP%%
      </tbody>
    </table>
    </div>
  </details>
</div>
<script>
window.HA_DATA = %%PAYLOAD%%;
</script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.7.1/jquery.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/datatables/1.10.21/js/jquery.dataTables.min.js"></script>
<script>
(function () {
  var DATA = window.HA_DATA || {summary:{}, trades:[], skipped:[]};
  var trades = DATA.trades || [];
  try {
    if (window.jQuery && jQuery.fn.dataTable) {
      jQuery('#tbl').DataTable({ pageLength: 25, order: [[2, 'desc'], [3, 'desc']] });
      jQuery('#tblSkip').DataTable({ pageLength: 25 });
    }
  } catch (e) {}
  try {
    if (typeof Chart === 'undefined') return;
    var byDate = {};
    trades.forEach(function (t) {
      var d = String(t.signal_time || '').slice(0, 10);
      byDate[d] = (byDate[d] || 0) + Number(t.actual_pnl_rs || 0);
    });
    var dates = Object.keys(byDate).sort();
    var run = 0;
    var cum = dates.map(function (d) { run += byDate[d]; return run; });
    var opt = { plugins: { legend: { labels: { color: '#e5e7eb' } } }, scales: { x: { ticks: { color: '#94a3b8' } }, y: { ticks: { color: '#94a3b8' } } } };
    new Chart(document.getElementById('cPnl'), { type: 'line', data: { labels: dates, datasets: [{ label: 'Cumulative actual P&L', data: cum, borderColor: '#22c55e', tension: 0.2 }] }, options: opt });
    var t1w = trades.filter(function (t) { return Number(t.t1_hit); }).length;
    var t2w = trades.filter(function (t) { return Number(t.t2_hit); }).length;
    var pieOpt = { plugins: { legend: { labels: { color: '#e5e7eb' } } } };
    new Chart(document.getElementById('cT1'), { type: 'pie', data: { labels: ['T1 Win', 'T1 Loss'], datasets: [{ data: [t1w, trades.length - t1w], backgroundColor: ['#22c55e', '#ef4444'] }] }, options: pieOpt });
    new Chart(document.getElementById('cT2'), { type: 'pie', data: { labels: ['T2 Win', 'T2 Loss'], datasets: [{ data: [t2w, trades.length - t2w], backgroundColor: ['#22c55e', '#ef4444'] }] }, options: pieOpt });
    var bySym = {};
    trades.forEach(function (t) { var s = t.symbol || ''; bySym[s] = (bySym[s] || 0) + Number(t.actual_pnl_rs || 0); });
    var top = Object.entries(bySym).sort(function (a, b) { return Math.abs(b[1]) - Math.abs(a[1]); }).slice(0, 20);
    new Chart(document.getElementById('cSym'), { type: 'bar', data: { labels: top.map(function (x) { return x[0]; }), datasets: [{ label: 'Actual P&L', data: top.map(function (x) { return x[1]; }), backgroundColor: top.map(function (x) { return x[1] >= 0 ? '#22c55e' : '#ef4444'; }) }] }, options: { plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#94a3b8', maxRotation: 90 } }, y: { ticks: { color: '#94a3b8' } } } } });
    var S = DATA.summary || {};
    var ln = (S.long && S.long.n) || 0, sn = (S.short && S.short.n) || 0;
    new Chart(document.getElementById('cDir'), { type: 'doughnut', data: { labels: ['LONG', 'SHORT'], datasets: [{ data: [ln, sn], backgroundColor: ['#38bdf8', '#a78bfa'] }] }, options: pieOpt });
  } catch (e) {}
})();
</script>
</body>
</html>
"""
    return (
        page.replace("%%UPDATED%%", htmlmod.escape(str(s.get("generated_at") or "")))
        .replace("%%KPIS%%", kpis)
        .replace("%%TRADES%%", _trade_rows(trades))
        .replace("%%SKIP%%", _skip_rows(skipped))
        .replace("%%NTRADES%%", str(len(trades)))
        .replace("%%NSKIP%%", str(len(skipped)))
        .replace("%%PAYLOAD%%", payload)
    )


def _load_existing_html() -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not OUT_HTML.exists():
        return [], []
    raw = OUT_HTML.read_text(encoding="utf-8")
    marker = "window.HA_DATA = "
    if marker in raw:
        blob = raw.split(marker, 1)[1].split(";</script>", 1)[0]
        try:
            data = json.loads(blob)
            return data.get("trades") or [], data.get("skipped") or []
        except json.JSONDecodeError:
            pass
    if 'id="ha-data"' in raw:
        try:
            blob = raw.split('id="ha-data"', 1)[1].split(">", 1)[1].split("</script>", 1)[0]
            data = json.loads(blob)
            return data.get("trades") or [], data.get("skipped") or []
        except (json.JSONDecodeError, IndexError):
            return [], []
    return [], []


def main() -> None:
    _setup_log()
    trades, skipped = _load_db()
    if not trades and RESULTS_JSON.exists():
        blob = json.loads(RESULTS_JSON.read_text(encoding="utf-8"))
        trades = blob.get("trades") or []
        skipped = blob.get("skipped") or []
        logger.info("loaded results JSON fallback")
    if not trades:
        trades, skipped = _load_existing_html()
        if trades:
            logger.info("loaded trades from existing hamoment.html")
    trades = _norm(trades)
    skipped = _norm(skipped)
    html = _html(_summarize(trades, skipped), trades, skipped)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}  size={OUT_HTML.stat().st_size} bytes  trades={len(trades)} skipped={len(skipped)}")


if __name__ == "__main__":
    main()

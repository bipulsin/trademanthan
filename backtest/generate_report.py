"""Build public frontend/public/hamoment.html from HA backtest tables (no login)."""
from __future__ import annotations

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


def _html(summary: Dict[str, Any], trades: List[Dict[str, Any]], skipped: List[Dict[str, Any]]) -> str:
    payload = json.dumps({"summary": summary, "trades": trades, "skipped": skipped}, default=str)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HA Momentum Strategy — Backtest Results</title>
<link rel="icon" type="image/x-icon" href="favicon.ico?v=3">
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
<style>
:root {{ --bg:#0b1220; --card:#121a2b; --text:#e5e7eb; --muted:#94a3b8; --green:#22c55e; --red:#ef4444; --orange:#f59e0b; --line:#1f2a44; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family: Inter, system-ui, sans-serif; background:var(--bg); color:var(--text); }}
.wrap {{ max-width:1280px; margin:0 auto; padding:24px 16px 48px; }}
h1 {{ margin:0 0 6px; font-size:1.6rem; }}
.sub {{ color:var(--muted); margin:0 0 18px; }}
.kpis {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:10px; margin-bottom:20px; }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px 14px; }}
.card .lbl {{ font-size:0.72rem; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); }}
.card .val {{ font-size:1.25rem; font-weight:800; margin-top:4px; }}
.pos {{ color:var(--green); }} .neg {{ color:var(--red); }}
.charts {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:20px; }}
.charts .wide {{ grid-column:1 / -1; }}
table.dataTable {{ color:var(--text); }}
table.dataTable tbody tr {{ background:var(--card); }}
table.dataTable.stripe tbody tr.odd {{ background:#0f172a; }}
.win {{ background:rgba(34,197,94,.12) !important; }}
.loss {{ background:rgba(239,68,68,.12) !important; }}
.time {{ background:rgba(245,158,11,.12) !important; }}
details {{ margin-top:18px; background:var(--card); border-radius:12px; padding:12px; border:1px solid var(--line); }}
@media (max-width:900px) {{ .charts {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body>
<div class="wrap">
  <h1>HA Momentum Strategy — Backtest Results</h1>
  <p class="sub">15-Min | NSE F&amp;O | 17-Jul-2026 to 19-Aug-2026 · Last updated <span id="updated"></span></p>
  <div class="kpis" id="kpis"></div>
  <div class="charts">
    <div class="card wide"><canvas id="cPnl"></canvas></div>
    <div class="card"><canvas id="cT1"></canvas></div>
    <div class="card"><canvas id="cT2"></canvas></div>
    <div class="card wide"><canvas id="cSym"></canvas></div>
    <div class="card"><canvas id="cDir"></canvas></div>
  </div>
  <h2>All trades</h2>
  <table id="tbl" class="display" style="width:100%"></table>
  <details>
    <summary>Skipped trades (SL &gt; ₹5,000)</summary>
    <table id="tblSkip" class="display" style="width:100%;margin-top:10px"></table>
  </details>
</div>
<script id="ha-data" type="application/json">{payload}</script>
<script>
const DATA = JSON.parse(document.getElementById('ha-data').textContent);
const S = DATA.summary || {{}};
document.getElementById('updated').textContent = S.generated_at || '';
const money = n => '₹' + Number(n||0).toLocaleString('en-IN', {{maximumFractionDigits:0}});
const kpis = [
  ['Total signals', S.signals],
  ['Trades taken', S.trades],
  ['Skipped (SL>5K)', S.skipped],
  ['Win rate T1', (S.wr_t1||0)+'%'],
  ['Win rate T2', (S.wr_t2||0)+'%'],
  ['P&L T1', money(S.pnl_t1), S.pnl_t1],
  ['P&L T2', money(S.pnl_t2), S.pnl_t2],
  ['Actual P&L', money(S.pnl_actual), S.pnl_actual],
  ['Avg R:R', S.avg_rr],
  ['Best trade', (S.best&&S.best.symbol?S.best.symbol+' ':'') + money(S.best&&S.best.pnl)],
  ['Worst trade', (S.worst&&S.worst.symbol?S.worst.symbol+' ':'') + money(S.worst&&S.worst.pnl)],
  ['Avg hold (min)', S.avg_hold],
  ['LONG w/l/pnl', (S.long?S.long.wins+'/'+S.long.losses+' '+money(S.long.pnl):'—')],
  ['SHORT w/l/pnl', (S.short?S.short.wins+'/'+S.short.losses+' '+money(S.short.pnl):'—')],
  ['Time exits', S.time_exits],
  ['Avg MFE beyond T2', S.mfe_beyond_t2]
];
document.getElementById('kpis').innerHTML = kpis.map(function(k){{
  var cls = '';
  if (typeof k[2]==='number') cls = k[2]>=0?' pos':' neg';
  return '<div class="card"><div class="lbl">'+k[0]+'</div><div class="val'+cls+'">'+k[1]+'</div></div>';
}}).join('');

const trades = DATA.trades || [];
const byDate = {{}};
trades.forEach(function(t){{
  const d = String(t.signal_time||'').slice(0,10);
  byDate[d] = (byDate[d]||0) + Number(t.actual_pnl_rs||0);
}});
const dates = Object.keys(byDate).sort();
let run = 0;
const cum = dates.map(function(d){{ run += byDate[d]; return run; }});
new Chart(document.getElementById('cPnl'), {{
  type:'line', data:{{ labels:dates, datasets:[{{ label:'Cumulative actual P&L', data:cum, borderColor:'#22c55e', tension:.2 }}] }},
  options:{{ plugins:{{ legend:{{ labels:{{ color:'#e5e7eb' }} }} }}, scales:{{ x:{{ ticks:{{ color:'#94a3b8' }} }}, y:{{ ticks:{{ color:'#94a3b8' }} }} }} }}
}});
const t1w = trades.filter(t=>Number(t.t1_hit)).length, t1l = trades.length-t1w;
const t2w = trades.filter(t=>Number(t.t2_hit)).length, t2l = trades.length-t2w;
new Chart(document.getElementById('cT1'), {{ type:'pie', data:{{ labels:['T1 Win','T1 Loss'], datasets:[{{ data:[t1w,t1l], backgroundColor:['#22c55e','#ef4444'] }}] }}, options:{{ plugins:{{ title:{{ display:true, text:'T1', color:'#e5e7eb' }}, legend:{{ labels:{{ color:'#e5e7eb' }} }} }} }} }});
new Chart(document.getElementById('cT2'), {{ type:'pie', data:{{ labels:['T2 Win','T2 Loss'], datasets:[{{ data:[t2w,t2l], backgroundColor:['#22c55e','#ef4444'] }}] }}, options:{{ plugins:{{ title:{{ display:true, text:'T2', color:'#e5e7eb' }}, legend:{{ labels:{{ color:'#e5e7eb' }} }} }} }} }});
const bySym = {{}};
trades.forEach(function(t){{ const s=t.symbol||''; bySym[s]=(bySym[s]||0)+Number(t.actual_pnl_rs||0); }});
const top = Object.entries(bySym).sort((a,b)=>Math.abs(b[1])-Math.abs(a[1])).slice(0,20);
new Chart(document.getElementById('cSym'), {{
  type:'bar', data:{{ labels:top.map(x=>x[0]), datasets:[{{ label:'Actual P&L', data:top.map(x=>x[1]), backgroundColor:top.map(x=>x[1]>=0?'#22c55e':'#ef4444') }}] }},
  options:{{ plugins:{{ legend:{{ display:false }} }}, scales:{{ x:{{ ticks:{{ color:'#94a3b8', maxRotation:90 }} }}, y:{{ ticks:{{ color:'#94a3b8' }} }} }} }}
}});
const ln = (S.long&&S.long.n)||0, sn=(S.short&&S.short.n)||0;
new Chart(document.getElementById('cDir'), {{ type:'doughnut', data:{{ labels:['LONG','SHORT'], datasets:[{{ data:[ln,sn], backgroundColor:['#38bdf8','#a78bfa'] }}] }}, options:{{ plugins:{{ title:{{ display:true, text:'Direction', color:'#e5e7eb' }}, legend:{{ labels:{{ color:'#e5e7eb' }} }} }} }} }});

function yn(v){{ return Number(v) ? 'Y' : ''; }}
$('#tbl').DataTable({{
  data: trades,
  pageLength: 25,
  order: [[2,'desc'],[3,'desc']],
  createdRow: function(row, data){{
    const pnl = Number(data.actual_pnl_rs||0);
    if (String(data.exit_reason)==='TIME_EXIT') $(row).addClass('time');
    else if (pnl>0) $(row).addClass('win');
    else if (pnl<0) $(row).addClass('loss');
  }},
  columns: [
    {{ title:'#', data:null, render:(d,t,r,m)=>m.row+1 }},
    {{ title:'Symbol', data:'symbol' }},
    {{ title:'Date', data:'signal_time', render:v=>String(v||'').slice(0,10) }},
    {{ title:'Time', data:'signal_time', render:v=>String(v||'').slice(11,19) }},
    {{ title:'Direction', data:'direction' }},
    {{ title:'Entry', data:'entry_price' }},
    {{ title:'SL', data:'sl_price' }},
    {{ title:'T1', data:'t1_price' }},
    {{ title:'T2', data:'t2_price' }},
    {{ title:'Candle SL?', data:'sl_used_prev_candle', render:yn }},
    {{ title:'Entry candle %', data:'entry_candle_size_pct' }},
    {{ title:'T1 Hit', data:'t1_hit', render:yn }},
    {{ title:'T2 Hit', data:'t2_hit', render:yn }},
    {{ title:'SL Hit', data:'sl_hit', render:yn }},
    {{ title:'Exit reason', data:'exit_reason' }},
    {{ title:'Actual exit', data:'actual_exit_price' }},
    {{ title:'Actual P&L', data:'actual_pnl_rs' }},
    {{ title:'P&L T1', data:'pnl_t1_rs' }},
    {{ title:'P&L T2', data:'pnl_t2_rs' }},
    {{ title:'Max favorable', data:'max_favorable' }},
    {{ title:'Lot', data:'lot_qty' }},
    {{ title:'Hold min', data:'holding_min' }}
  ]
}});
$('#tblSkip').DataTable({{
  data: DATA.skipped || [],
  pageLength: 25,
  columns: [
    {{ title:'#', data:null, render:(d,t,r,m)=>m.row+1 }},
    {{ title:'Symbol', data:'symbol' }},
    {{ title:'Date', data:'signal_time', render:v=>String(v||'').slice(0,10) }},
    {{ title:'Time', data:'signal_time', render:v=>String(v||'').slice(11,19) }},
    {{ title:'Direction', data:'direction' }},
    {{ title:'Entry', data:'entry_price' }},
    {{ title:'SL', data:'sl_price' }},
    {{ title:'SL Rs', data:'sl_rs' }},
    {{ title:'Lot', data:'lot_qty' }},
    {{ title:'Reason', data:'reason' }}
  ]
}});
</script>
</body>
</html>
"""


def main() -> None:
    _setup_log()
    trades, skipped = _load_db()
    if not trades and RESULTS_JSON.exists():
        blob = json.loads(RESULTS_JSON.read_text(encoding="utf-8"))
        trades = blob.get("trades") or []
        skipped = blob.get("skipped") or []
        logger.info("loaded results JSON fallback")
    trades = _norm(trades)
    skipped = _norm(skipped)
    html = _html(_summarize(trades, skipped), trades, skipped)
    OUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote {OUT_HTML}  size={OUT_HTML.stat().st_size} bytes  trades={len(trades)} skipped={len(skipped)}")


if __name__ == "__main__":
    main()

"""Self-contained HTML report + CSV writer for OpenHighLow."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

from backend.services.open_high_low.config import TRADE_FIELDS


def write_trades_csv(trades: Sequence[Dict[str, Any]], path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=TRADE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for t in trades:
            writer.writerow({k: t.get(k, "") for k in TRADE_FIELDS})
    return path


def build_html(
    trades: Sequence[Dict[str, Any]],
    summary: Dict[str, Any],
    *,
    meta: Dict[str, Any] | None = None,
) -> str:
    payload = {
        "meta": meta or {},
        "summary": summary,
        "trades": list(trades),
        "fields": TRADE_FIELDS,
    }
    data_json = json.dumps(payload, default=str).replace("</", "<\\/")
    return _HTML_TEMPLATE.replace("__OHL_DATA__", data_json)


def write_html(
    trades: Sequence[Dict[str, Any]],
    summary: Dict[str, Any],
    path: Path,
    *,
    meta: Dict[str, Any] | None = None,
) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(build_html(trades, summary, meta=meta), encoding="utf-8")
    return path


_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>OpenHighLow Backtest | TradeWithCTO</title>
<style>
:root {
  --bg:#0f1419; --panel:#1a2332; --text:#e7ecf3; --muted:#8b9bb4;
  --win:#22c55e; --loss:#ef4444; --accent:#38bdf8; --border:#2a3548;
  --flag:#f59e0b;
}
* { box-sizing:border-box; }
body {
  margin:0; font-family: "Segoe UI", system-ui, sans-serif;
  background: radial-gradient(1200px 600px at 10% -10%, #1e3a5f 0%, transparent 55%),
              radial-gradient(900px 500px at 100% 0%, #1a2e1a 0%, transparent 50%),
              var(--bg);
  color: var(--text); line-height:1.45;
}
.wrap { max-width:1280px; margin:0 auto; padding:1.25rem 1rem 3rem; }
h1 { font-size:1.6rem; margin:0 0 .35rem; letter-spacing:.02em; }
.lede { color:var(--muted); margin:0 0 1.25rem; max-width:70ch; font-size:.95rem; }
.kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:.75rem; margin-bottom:1.25rem; }
.kpi { background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:.85rem 1rem; }
.kpi .l { display:block; color:var(--muted); font-size:.75rem; text-transform:uppercase; letter-spacing:.04em; }
.kpi .v { font-size:1.25rem; font-weight:650; margin-top:.2rem; }
.grid2 { display:grid; grid-template-columns:1fr 1fr; gap:1rem; margin-bottom:1.25rem; }
@media (max-width:900px){ .grid2{ grid-template-columns:1fr; } }
.card { background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:.9rem 1rem; }
.card h2 { margin:0 0 .75rem; font-size:1rem; color:var(--muted); font-weight:600; }
canvas { width:100%; height:220px; display:block; }
.filters { display:flex; flex-wrap:wrap; gap:.6rem; align-items:end; margin:1rem 0; }
.filters label { font-size:.8rem; color:var(--muted); display:flex; flex-direction:column; gap:.25rem; }
.filters input, .filters select {
  background:#0d121a; color:var(--text); border:1px solid var(--border);
  border-radius:6px; padding:.4rem .55rem; min-width:120px;
}
.btn {
  background:#243044; color:var(--text); border:1px solid var(--border);
  border-radius:6px; padding:.45rem .8rem; cursor:pointer; font-size:.85rem;
}
.btn:hover { border-color:var(--accent); }
.table-wrap { overflow:auto; max-height:70vh; border:1px solid var(--border); border-radius:10px; }
table { width:100%; border-collapse:collapse; font-size:.78rem; }
th, td { padding:.4rem .5rem; border-bottom:1px solid var(--border); white-space:nowrap; }
th {
  position:sticky; top:0; background:#152033; z-index:2; text-align:left;
  cursor:pointer; user-select:none;
}
tr.win td.pnl { color:var(--win); }
tr.loss td.pnl { color:var(--loss); }
.badge {
  display:inline-block; padding:.1rem .4rem; border-radius:4px; font-size:.7rem; font-weight:700;
}
.badge-yes { background:rgba(245,158,11,.2); color:var(--flag); }
.badge-no { background:rgba(139,155,180,.15); color:var(--muted); }
.breakdown { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:.75rem; margin-bottom:1rem; }
.breakdown ul { margin:0; padding-left:1.1rem; color:var(--muted); font-size:.85rem; }
.skips { font-size:.8rem; color:var(--muted); max-height:160px; overflow:auto; }
.count { color:var(--muted); font-size:.85rem; margin-left:.5rem; }
</style>
</head>
<body>
<div class="wrap">
  <h1>OpenHighLow Intraday Backtest</h1>
  <p class="lede" id="lede">Front-month stock futures · previous-day H/L break + VWAP · 5m trail · EOD square-off</p>

  <div class="kpis" id="kpis"></div>

  <div class="grid2">
    <div class="card"><h2>Equity curve (₹)</h2><canvas id="eqChart" width="600" height="220"></canvas></div>
    <div class="card"><h2>Daily P&amp;L (₹)</h2><canvas id="dayChart" width="600" height="220"></canvas></div>
  </div>

  <div class="breakdown" id="breakdowns"></div>

  <div class="card" style="margin-bottom:1rem">
    <h2>Data-quality skips</h2>
    <div class="skips" id="skips">None</div>
  </div>

  <div class="filters">
    <label>Side
      <select id="fSide"><option value="">All</option><option>BULLISH</option><option>BEARISH</option></select>
    </label>
    <label>Exit
      <select id="fExit"><option value="">All</option></select>
    </label>
    <label>Flag
      <select id="fFlag"><option value="">All</option><option>YES</option><option>NO</option></select>
    </label>
    <label>Symbol
      <input type="search" id="fSym" placeholder="e.g. RELIANCE">
    </label>
    <label>From <input type="date" id="fFrom"></label>
    <label>To <input type="date" id="fTo"></label>
    <button class="btn" type="button" id="btnApply">Apply</button>
    <button class="btn" type="button" id="btnReset">Reset</button>
    <button class="btn" type="button" id="btnCsv">Download CSV (filtered)</button>
    <button class="btn" type="button" id="btnCsvAll">Download all</button>
    <span class="count" id="count"></span>
  </div>

  <div class="table-wrap">
    <table>
      <thead id="thead"></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>
<script>
const DATA = __OHL_DATA__;
const FIELDS = DATA.fields || [];
let sortKey = "date";
let sortAsc = true;
let filtered = DATA.trades.slice();

function fmt(n, d=2){
  if (n==null || n==="") return "—";
  const x = Number(n);
  if (!Number.isFinite(x)) return String(n);
  return x.toLocaleString(undefined,{maximumFractionDigits:d, minimumFractionDigits:0});
}
function money(n){ return "₹" + fmt(n,0); }

function renderKpis(){
  const s = DATA.summary || {};
  const meta = DATA.meta || {};
  document.getElementById("lede").textContent =
    `Period ${meta.start||"?"} → ${meta.end||"?"} · ${meta.stocks_scanned||"?"} stocks · ` +
    `square-off ${meta.square_off||"15:15"} · lot fallback ${meta.lot_fallback||1}`;
  const items = [
    ["Trades", s.trades],
    ["Win rate", fmt(s.win_rate,1)+"%"],
    ["Net P&L", money(s.net_pnl)],
    ["Avg R", fmt(s.avg_r,2)],
    ["Max DD", money(s.max_drawdown)],
    ["Skips", s.skip_count||0],
  ];
  document.getElementById("kpis").innerHTML = items.map(([l,v])=>
    `<div class="kpi"><span class="l">${l}</span><span class="v">${v}</span></div>`
  ).join("");
}

function renderBreakdowns(){
  const s = DATA.summary || {};
  const side = s.by_side || {};
  const exits = s.by_exit || {};
  const wd = s.by_weekday || {};
  const fc = s.flag_comparison || {};
  const top = s.top10 || [];
  const bot = s.bottom10 || [];
  const card = (title, html) => `<div class="card"><h2>${title}</h2>${html}</div>`;
  const ul = (obj) => `<ul>${Object.entries(obj).map(([k,v])=>{
    if (typeof v === "object") return `<li><strong>${k}</strong>: ${v.trades||0} trades, WR ${fmt(v.win_rate,1)}%, ${money(v.net_pnl)}</li>`;
    return `<li><strong>${k}</strong>: ${v}</li>`;
  }).join("")}</ul>`;
  const listPairs = (arr) => `<ul>${arr.map(r=>`<li>${r.symbol}: ${money(r.pnl)}</li>`).join("")||"<li>—</li>"}</ul>`;
  const flagHtml = (key) => {
    const block = (fc[key]||{});
    return ul({All: block.all||{}, Bullish: block.BULLISH||{}, Bearish: block.BEARISH||{}});
  };
  document.getElementById("breakdowns").innerHTML = [
    card("By side", ul(side)),
    card("By exit reason", ul(exits)),
    card("By weekday", ul(wd)),
    card("Flag YES", flagHtml("YES")),
    card("Flag NO", flagHtml("NO")),
    card("Top 10 stocks", listPairs(top)),
    card("Bottom 10 stocks", listPairs(bot)),
  ].join("");
  const skips = s.skips || [];
  document.getElementById("skips").innerHTML = skips.length
    ? `<ul>${skips.slice(0,200).map(x=>`<li>${x.date||""} ${x.symbol||""}: ${x.reason||JSON.stringify(x)}</li>`).join("")}</ul>`
    : "None";
  const exitSel = document.getElementById("fExit");
  Object.keys(exits).sort().forEach(k=>{
    const o=document.createElement("option"); o.value=k; o.textContent=k; exitSel.appendChild(o);
  });
}

function drawLine(canvas, points, color){
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0,0,w,h);
  ctx.fillStyle = "#0d121a"; ctx.fillRect(0,0,w,h);
  if (!points.length) return;
  const ys = points.map(p=>p.y);
  let mn = Math.min(...ys,0), mx = Math.max(...ys,0);
  if (mn===mx){ mn-=1; mx+=1; }
  const pad=24;
  const xAt = i => pad + (w-2*pad) * (points.length===1?0.5:i/(points.length-1));
  const yAt = v => h-pad - (h-2*pad) * ((v-mn)/(mx-mn));
  ctx.strokeStyle = "#2a3548"; ctx.beginPath();
  ctx.moveTo(pad,yAt(0)); ctx.lineTo(w-pad,yAt(0)); ctx.stroke();
  ctx.strokeStyle = color; ctx.lineWidth=2; ctx.beginPath();
  points.forEach((p,i)=>{ const x=xAt(i), y=yAt(p.y); i?ctx.lineTo(x,y):ctx.moveTo(x,y); });
  ctx.stroke();
}

function drawBars(canvas, points){
  const ctx = canvas.getContext("2d");
  const w = canvas.width, h = canvas.height;
  ctx.clearRect(0,0,w,h);
  ctx.fillStyle = "#0d121a"; ctx.fillRect(0,0,w,h);
  if (!points.length) return;
  const ys = points.map(p=>p.y);
  let mn = Math.min(...ys,0), mx = Math.max(...ys,0);
  if (mn===mx){ mn-=1; mx+=1; }
  const pad=24, bw = Math.max(2, (w-2*pad)/points.length - 2);
  const yAt = v => h-pad - (h-2*pad) * ((v-mn)/(mx-mn));
  ctx.strokeStyle="#2a3548"; ctx.beginPath();
  ctx.moveTo(pad,yAt(0)); ctx.lineTo(w-pad,yAt(0)); ctx.stroke();
  points.forEach((p,i)=>{
    const x = pad + i*((w-2*pad)/points.length);
    const y0 = yAt(0), y1 = yAt(p.y);
    ctx.fillStyle = p.y>=0 ? "#22c55e" : "#ef4444";
    ctx.fillRect(x, Math.min(y0,y1), bw, Math.abs(y1-y0)||1);
  });
}

function renderCharts(){
  const eq = (DATA.summary.equity||[]).map(e=>({y:Number(e.cum_pnl)||0}));
  drawLine(document.getElementById("eqChart"), eq, "#38bdf8");
  const daily = DATA.summary.daily_pnl || {};
  const bars = Object.keys(daily).sort().map(k=>({y:Number(daily[k])||0}));
  drawBars(document.getElementById("dayChart"), bars);
}

function applyFilters(){
  const side = document.getElementById("fSide").value;
  const exit = document.getElementById("fExit").value;
  const flag = document.getElementById("fFlag").value;
  const sym = document.getElementById("fSym").value.trim().toUpperCase();
  const from = document.getElementById("fFrom").value;
  const to = document.getElementById("fTo").value;
  filtered = DATA.trades.filter(t=>{
    if (side && t.side!==side) return false;
    if (exit && t.exit_reason!==exit) return false;
    if (flag && t.open_eq_extreme!==flag) return false;
    if (sym && !(String(t.symbol||"").toUpperCase().includes(sym))) return false;
    if (from && String(t.date) < from) return false;
    if (to && String(t.date) > to) return false;
    return true;
  });
  sortTable();
}

function sortTable(){
  filtered.sort((a,b)=>{
    const av=a[sortKey], bv=b[sortKey];
    const an=Number(av), bn=Number(bv);
    let cmp;
    if (Number.isFinite(an) && Number.isFinite(bn) && String(av)!=="" && String(bv)!=="") cmp = an-bn;
    else cmp = String(av??"").localeCompare(String(bv??""));
    return sortAsc ? cmp : -cmp;
  });
  renderTable();
}

function renderTable(){
  const thead = document.getElementById("thead");
  thead.innerHTML = "<tr>" + FIELDS.map(f=>`<th data-k="${f}">${f}</th>`).join("") + "</tr>";
  thead.querySelectorAll("th").forEach(th=>{
    th.onclick = ()=>{
      const k = th.dataset.k;
      if (sortKey===k) sortAsc=!sortAsc; else { sortKey=k; sortAsc=true; }
      sortTable();
    };
  });
  const tb = document.getElementById("tbody");
  tb.innerHTML = filtered.map(t=>{
    const win = Number(t.rupee_pnl)>0;
    return `<tr class="${win?"win":"loss"}">` + FIELDS.map(f=>{
      let v = t[f];
      if (f==="open_eq_extreme"){
        const yes = v==="YES";
        return `<td><span class="badge ${yes?"badge-yes":"badge-no"}">${v||"NO"}</span></td>`;
      }
      if (f==="rupee_pnl" || f==="points_pnl" || f==="r_multiple" || f==="pct_return"){
        return `<td class="pnl">${fmt(v, f==="rupee_pnl"?0:2)}</td>`;
      }
      return `<td>${v??""}</td>`;
    }).join("") + "</tr>";
  }).join("");
  document.getElementById("count").textContent = `${filtered.length} / ${DATA.trades.length} trades`;
}

function toCsv(rows){
  const esc = (v)=>{
    const s = String(v??"");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g,'""')}"` : s;
  };
  const lines = [FIELDS.join(",")];
  rows.forEach(r=> lines.push(FIELDS.map(f=>esc(r[f])).join(",")));
  return lines.join("\n");
}
function download(name, text){
  const blob = new Blob([text], {type:"text/csv;charset=utf-8"});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  a.click();
  URL.revokeObjectURL(a.href);
}

document.getElementById("btnApply").onclick = applyFilters;
document.getElementById("btnReset").onclick = ()=>{
  ["fSide","fExit","fFlag","fSym","fFrom","fTo"].forEach(id=>{
    const el=document.getElementById(id); if(el) el.value="";
  });
  applyFilters();
};
document.getElementById("btnCsv").onclick = ()=> download("open_high_low_filtered.csv", toCsv(filtered));
document.getElementById("btnCsvAll").onclick = ()=> download("open_high_low_all.csv", toCsv(DATA.trades));

renderKpis();
renderBreakdowns();
renderCharts();
applyFilters();
</script>
</body>
</html>
"""

"""Rich terminal tear sheet + Plotly HTML export (rocket.html)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from rich.console import Console
from rich.table import Table


def print_tearsheet(metrics: Dict[str, Any], console: Optional[Console] = None) -> None:
    con = console or Console()
    table = Table(title="Rocket — Performance Tear Sheet", show_header=True)
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    rows = [
        ("Initial Capital", f"₹{metrics['initial_capital']:,.2f}"),
        ("Final Equity", f"₹{metrics['final_equity']:,.2f}"),
        ("Net Return %", f"{metrics['net_return_pct']:.4f}%"),
        ("Total Trades", str(metrics["total_trades"])),
        ("Win Rate %", f"{metrics['win_rate_pct']:.2f}%"),
        (
            "Profit Factor",
            "∞" if metrics.get("profit_factor") is None and metrics.get("profit_factor_raw") == float("inf") else str(metrics.get("profit_factor")),
        ),
        ("Expectancy", f"₹{metrics['expectancy']:,.2f}"),
        ("Max Drawdown %", f"{metrics['max_drawdown_pct']:.4f}%"),
        ("Max DD Duration", str(metrics.get("max_drawdown_duration") or "—")),
        ("Sharpe", f"{metrics['sharpe']:.4f}"),
        ("Sortino", f"{metrics['sortino']:.4f}"),
        ("Calmar", f"{metrics['calmar']:.4f}"),
    ]
    for k, v in rows:
        table.add_row(k, v)
    con.print(table)

    costs = metrics.get("costs") or {}
    ct = Table(title="Taxes & Friction (₹)", show_header=True)
    ct.add_column("Component")
    ct.add_column("Amount", justify="right")
    for key in ("brokerage", "stt", "exchange", "sebi", "stamp_duty", "gst", "slippage", "total"):
        ct.add_row(key, f"{float(costs.get(key, 0)):,.2f}")
    con.print(ct)


def export_html(
    metrics: Dict[str, Any],
    output_path: Path,
    *,
    title: str = "Rocket — ML Institutional Futures Backtest",
) -> Path:
    """Standalone interactive HTML with equity curve + tables (Plotly CDN)."""
    output_path = Path(output_path)
    eq = metrics.get("equity_curve") or []
    xs = [e.get("timestamp") for e in eq]
    ys = [e.get("equity") for e in eq]
    trades = metrics.get("trades") or []
    costs = metrics.get("costs") or {}

    # Build trade table rows
    trade_rows = []
    for t in trades:
        pnl = float(t.get("pnl") or 0)
        cls = "pos" if pnl > 0 else ("neg" if pnl < 0 else "")
        trade_rows.append(
            "<tr class='{cls}'>"
            "<td>{symbol}</td><td>{side}</td><td class='num'>{qty}</td>"
            "<td class='num'>{entry:.2f}</td><td class='num'>{exit:.2f}</td>"
            "<td>{entry_time}</td><td>{exit_time}</td>"
            "<td class='num'>{pnl:.2f}</td><td class='num'>{costs:.2f}</td>"
            "<td>{reason}</td></tr>".format(cls=cls, **{k: t.get(k) for k in (
                "symbol", "side", "qty", "entry", "exit", "entry_time", "exit_time", "pnl", "costs", "reason"
            )})
        )

    pf = metrics.get("profit_factor")
    pf_s = "∞" if pf is None and metrics.get("profit_factor_raw") == float("inf") else (f"{pf}" if pf is not None else "—")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {{
      --bg: #0b1220;
      --panel: #121a2b;
      --text: #e8eefc;
      --muted: #93a0b8;
      --accent: #3dd6c6;
      --pos: #3dd68c;
      --neg: #ff6b7a;
      --line: #243049;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, #1a2744 0%, var(--bg) 55%);
      color: var(--text);
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 28px 18px 60px; }}
    h1 {{ margin: 0 0 6px; font-size: 1.75rem; letter-spacing: -0.02em; }}
    .sub {{ color: var(--muted); margin-bottom: 22px; }}
    .brand {{ color: var(--accent); font-weight: 700; }}
    .grid {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px; margin-bottom: 18px;
    }}
    .metric {{
      background: var(--panel); border: 1px solid var(--line);
      border-radius: 10px; padding: 12px 14px;
    }}
    .metric .k {{ color: var(--muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; }}
    .metric .v {{ font-size: 1.15rem; font-weight: 650; margin-top: 4px; font-variant-numeric: tabular-nums; }}
    .panel {{
      background: var(--panel); border: 1px solid var(--line);
      border-radius: 12px; padding: 14px; margin-bottom: 16px;
    }}
    h2 {{ margin: 0 0 10px; font-size: 1.05rem; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.86rem; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 7px 6px; text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    tr.pos td.num:nth-last-child(3) {{ color: var(--pos); }}
    tr.neg td.num:nth-last-child(3) {{ color: var(--neg); }}
    .scroll {{ overflow-x: auto; max-height: 480px; overflow-y: auto; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1><span class="brand">Rocket</span> Performance Dashboard</h1>
    <div class="sub">{title}</div>
    <div class="grid">
      <div class="metric"><div class="k">Initial Capital</div><div class="v">₹{metrics['initial_capital']:,.0f}</div></div>
      <div class="metric"><div class="k">Final Equity</div><div class="v">₹{metrics['final_equity']:,.0f}</div></div>
      <div class="metric"><div class="k">Net Return</div><div class="v">{metrics['net_return_pct']:.3f}%</div></div>
      <div class="metric"><div class="k">Trades</div><div class="v">{metrics['total_trades']}</div></div>
      <div class="metric"><div class="k">Win Rate</div><div class="v">{metrics['win_rate_pct']:.2f}%</div></div>
      <div class="metric"><div class="k">Profit Factor</div><div class="v">{pf_s}</div></div>
      <div class="metric"><div class="k">Max Drawdown</div><div class="v">{metrics['max_drawdown_pct']:.3f}%</div></div>
      <div class="metric"><div class="k">Sharpe</div><div class="v">{metrics['sharpe']:.3f}</div></div>
      <div class="metric"><div class="k">Sortino</div><div class="v">{metrics['sortino']:.3f}</div></div>
      <div class="metric"><div class="k">Calmar</div><div class="v">{metrics['calmar']:.3f}</div></div>
      <div class="metric"><div class="k">Expectancy</div><div class="v">₹{metrics['expectancy']:,.0f}</div></div>
      <div class="metric"><div class="k">Total Costs</div><div class="v">₹{float(costs.get('total', 0)):,.0f}</div></div>
    </div>

    <div class="panel">
      <h2>Equity Curve</h2>
      <div id="eqChart" style="height:380px;"></div>
    </div>

    <div class="panel">
      <h2>Itemized Taxes &amp; Slippage</h2>
      <table>
        <thead><tr><th>Component</th><th class="num">₹</th></tr></thead>
        <tbody>
          <tr><td>Brokerage</td><td class="num">{float(costs.get('brokerage',0)):,.2f}</td></tr>
          <tr><td>STT</td><td class="num">{float(costs.get('stt',0)):,.2f}</td></tr>
          <tr><td>Exchange</td><td class="num">{float(costs.get('exchange',0)):,.2f}</td></tr>
          <tr><td>SEBI</td><td class="num">{float(costs.get('sebi',0)):,.2f}</td></tr>
          <tr><td>Stamp Duty</td><td class="num">{float(costs.get('stamp_duty',0)):,.2f}</td></tr>
          <tr><td>GST</td><td class="num">{float(costs.get('gst',0)):,.2f}</td></tr>
          <tr><td>Slippage</td><td class="num">{float(costs.get('slippage',0)):,.2f}</td></tr>
          <tr><td><strong>Total</strong></td><td class="num"><strong>{float(costs.get('total',0)):,.2f}</strong></td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h2>Trade Log ({len(trades)})</h2>
      <div class="scroll">
        <table>
          <thead>
            <tr>
              <th>Symbol</th><th>Side</th><th class="num">Qty</th>
              <th class="num">Entry</th><th class="num">Exit</th>
              <th>Entry Time</th><th>Exit Time</th>
              <th class="num">PnL</th><th class="num">Costs</th><th>Reason</th>
            </tr>
          </thead>
          <tbody>
            {''.join(trade_rows) if trade_rows else '<tr><td colspan="10">No closed trades</td></tr>'}
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <script>
    const xs = {json.dumps(xs)};
    const ys = {json.dumps(ys)};
    Plotly.newPlot('eqChart', [{{
      x: xs, y: ys, type: 'scatter', mode: 'lines',
      line: {{ color: '#3dd6c6', width: 2 }},
      fill: 'tozeroy', fillcolor: 'rgba(61,214,198,0.08)',
      name: 'Equity'
    }}], {{
      margin: {{ t: 10, r: 20, b: 40, l: 60 }},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {{ color: '#93a0b8' }},
      xaxis: {{ gridcolor: '#243049' }},
      yaxis: {{ gridcolor: '#243049', tickprefix: '₹' }},
    }}, {{responsive: true, displayModeBar: false}});
  </script>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path

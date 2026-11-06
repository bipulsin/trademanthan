/* MTF Swing results — reads /api/mtf-swing/results (CSV-backed). */
(function () {
  var PAGE_SIZE = 50;
  var state = {
    tf: "60m",
    trades: [],
    summary: null,
    months: [],
    charts: null,
    sortKey: "entry_time",
    sortDir: -1,
    page: 1,
  };

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    return "";
  }

  function token() {
    return localStorage.getItem("trademanthan_token") || "";
  }

  function money(n) {
    if (n === null || n === undefined || n === "") return "—";
    var v = Number(n);
    if (!isFinite(v)) return "∞";
    var sign = v < 0 ? "-" : "";
    return sign + "₹" + Math.abs(v).toLocaleString("en-IN", { maximumFractionDigits: 2 });
  }

  function pct(n) {
    if (n === null || n === undefined || n === "") return "—";
    return (Number(n) * 100).toFixed(1) + "%";
  }

  function pf(n) {
    if (n === null || n === undefined || n === "") return "—";
    if (!isFinite(Number(n))) return "∞";
    return Number(n).toFixed(2);
  }

  function num(n, d) {
    if (n === null || n === undefined || n === "") return "—";
    var v = Number(n);
    if (!isFinite(v)) return "∞";
    return v.toLocaleString("en-IN", { maximumFractionDigits: d == null ? 2 : d });
  }

  function pnlClass(n) {
    var v = Number(n);
    if (!isFinite(v) || v === 0) return "";
    return v > 0 ? "pos" : "neg";
  }

  function qsTf() {
    var p = new URLSearchParams(window.location.search);
    var tf = (p.get("tf") || "60m").toLowerCase();
    if (tf !== "15m" && tf !== "60m" && tf !== "all") tf = "60m";
    return tf;
  }

  function setTf(tf) {
    state.tf = tf;
    state.page = 1;
    var url = new URL(window.location.href);
    url.searchParams.set("tf", tf);
    history.replaceState(null, "", url.pathname + "?" + url.searchParams.toString());
    document.querySelectorAll(".ms-tf button").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-tf") === tf);
    });
    load();
  }

  function setStatus(msg) {
    var el = document.getElementById("msStatus");
    if (el) el.textContent = msg || "";
  }

  async function load() {
    setStatus("Loading results…");
    try {
      var res = await fetch(apiBase() + "/api/mtf-swing/results?timeframe=" + encodeURIComponent(state.tf), {
        headers: { Authorization: "Bearer " + token() },
        cache: "no-store",
      });
      if (!res.ok) throw new Error(res.status + " " + (await res.text()).slice(0, 180));
      var data = await res.json();
      state.trades = data.trades || [];
      state.summary = data.summary || {};
      state.months = data.months || [];
      state.charts = data.charts || {};
      render();
      var n = state.trades.length;
      setStatus(n ? n + " rows from the results CSV" : "No rows yet. The backtest CSV is empty.");
    } catch (err) {
      setStatus("Could not load results. " + err.message);
    }
  }

  function card(label, value, extra) {
    return '<div class="ms-card"><h3>' + label + '</h3><div class="val ' + (extra || "") + '">' + value + "</div></div>";
  }

  function renderCards() {
    var s = state.summary || {};
    var host = document.getElementById("msCards");
    host.innerHTML = [
      card("Total trades", num(s.trades, 0)),
      card("Net P&L", money(s.net_pnl), pnlClass(s.net_pnl)),
      card("Win rate", pct(s.win_rate)),
      card("Avg P&L / trade", money(s.avg_pnl), pnlClass(s.avg_pnl)),
      card("Profit factor", pf(s.profit_factor)),
      card("Max profit", money(s.max_profit), "pos"),
      card("Max loss", money(s.max_loss), "neg"),
      card("Max drawdown", money(s.max_drawdown)),
      card("Profitable", num(s.profitable, 0)),
      card("Losing", num(s.losing, 0)),
    ].join("");
  }

  function renderMonths() {
    var body = document.getElementById("msMonths");
    body.innerHTML = (state.months || []).map(function (m) {
      return "<tr><td>" + m.month + "</td><td>" + num(m.trades, 0) + "</td><td class=\"" + pnlClass(m.net_pnl) + "\">" + money(m.net_pnl) +
        "</td><td class=\"" + pnlClass(m.avg_pnl) + "\">" + money(m.avg_pnl) + "</td><td class=\"pos\">" + money(m.max_profit) +
        "</td><td class=\"neg\">" + money(m.max_loss) + "</td><td>" + money(m.max_drawdown) + "</td><td>" + num(m.profitable, 0) +
        "</td><td>" + num(m.losing, 0) + "</td><td>" + pct(m.win_rate) + "</td></tr>";
    }).join("");
  }

  function svgEl(name, attrs) {
    var el = document.createElementNS("http://www.w3.org/2000/svg", name);
    Object.keys(attrs || {}).forEach(function (k) { el.setAttribute(k, attrs[k]); });
    return el;
  }

  function renderBarChart(host, rows) {
    host.innerHTML = "";
    var w = Math.max(320, host.clientWidth || 640);
    var h = 220;
    var pad = { l: 48, r: 8, t: 16, b: 48 };
    var svg = svgEl("svg", { viewBox: "0 0 " + w + " " + h, width: "100%", height: h });
    var vals = rows.map(function (r) { return Number(r.net_pnl) || 0; });
    var max = Math.max(1, Math.max.apply(null, vals.map(Math.abs)));
    var innerW = w - pad.l - pad.r;
    var innerH = h - pad.t - pad.b;
    var zero = pad.t + innerH / 2;
    svg.appendChild(svgEl("line", { x1: pad.l, x2: w - pad.r, y1: zero, y2: zero, stroke: "rgba(180,210,190,0.35)" }));
    var bw = innerW / Math.max(rows.length, 1);
    rows.forEach(function (r, i) {
      var v = Number(r.net_pnl) || 0;
      var bh = (Math.abs(v) / max) * (innerH / 2);
      var y = v >= 0 ? zero - bh : zero;
      var rect = svgEl("rect", {
        x: pad.l + i * bw + 4,
        y: y,
        width: Math.max(2, bw - 8),
        height: Math.max(1, bh),
        fill: v >= 0 ? "#3dba7a" : "#e06b6b",
      });
      svg.appendChild(rect);
      var label = svgEl("text", { x: pad.l + i * bw + bw / 2, y: h - 28, "text-anchor": "middle", fill: "#8fa398", "font-size": "9" });
      label.textContent = String(r.month || "").slice(2);
      svg.appendChild(label);
    });
    host.appendChild(svg);
  }

  function renderLineChart(host, points) {
    host.innerHTML = "";
    var w = Math.max(320, host.clientWidth || 640);
    var h = 220;
    var pad = { l: 48, r: 8, t: 12, b: 20 };
    var svg = svgEl("svg", { viewBox: "0 0 " + w + " " + h, width: "100%", height: h });
    if (!points.length) {
      host.appendChild(svg);
      return;
    }
    var eqs = points.map(function (p) { return Number(p.equity) || 0; });
    var dds = points.map(function (p) { return Number(p.drawdown) || 0; });
    var minE = Math.min(0, Math.min.apply(null, eqs));
    var maxE = Math.max(1, Math.max.apply(null, eqs));
    var maxD = Math.max(1, Math.max.apply(null, dds));
    var innerW = w - pad.l - pad.r;
    var innerH = h - pad.t - pad.b;
    function x(i) { return pad.l + (points.length === 1 ? innerW / 2 : (i / (points.length - 1)) * innerW); }
    function yE(v) { return pad.t + (1 - (v - minE) / (maxE - minE)) * innerH; }
    function yD(v) { return pad.t + (v / maxD) * innerH; }
    var eq = points.map(function (p, i) { return x(i) + "," + yE(Number(p.equity) || 0); }).join(" ");
    var dd = points.map(function (p, i) { return x(i) + "," + yD(Number(p.drawdown) || 0); }).join(" ");
    svg.appendChild(svgEl("polyline", { points: eq, fill: "none", stroke: "#3dba7a", "stroke-width": "2" }));
    svg.appendChild(svgEl("polyline", { points: dd, fill: "none", stroke: "#e06b6b", "stroke-width": "1.5", "stroke-dasharray": "4 3" }));
    host.appendChild(svg);
  }

  function renderSplit(host, map, colors) {
    host.innerHTML = "";
    var keys = Object.keys(map || {});
    var total = keys.reduce(function (s, k) { return s + (Number(map[k]) || 0); }, 0);
    if (!total) {
      host.textContent = "No closed trades";
      return;
    }
    var w = Math.max(280, host.clientWidth || 360);
    var svg = svgEl("svg", { viewBox: "0 0 " + w + " 36", width: "100%", height: "36" });
    var x = 0;
    keys.forEach(function (k) {
      var n = Number(map[k]) || 0;
      if (!n) return;
      var bw = (n / total) * w;
      svg.appendChild(svgEl("rect", { x: x, y: 8, width: Math.max(bw, 1), height: 16, fill: colors[k] || "#64748b" }));
      x += bw;
    });
    host.appendChild(svg);
    var legend = document.createElement("div");
    legend.className = "ms-legend";
    keys.forEach(function (k) {
      if (!map[k]) return;
      var item = document.createElement("span");
      item.innerHTML = '<i style="background:' + (colors[k] || "#64748b") + '"></i>' + k + " " + map[k];
      legend.appendChild(item);
    });
    host.appendChild(legend);
  }

  function renderCharts() {
    var c = state.charts || {};
    renderBarChart(document.getElementById("chartMonth"), c.month_pnl || []);
    renderLineChart(document.getElementById("chartEquity"), c.equity || []);
    renderSplit(document.getElementById("chartReasons"), c.exit_reasons || {}, { SL: "#e06b6b", T1: "#3dba7a", TIME: "#d4a017", OPEN: "#64748b" });
    renderSplit(document.getElementById("chartSides"), c.sides || {}, { LONG: "#3dba7a", SHORT: "#e06b6b" });
  }

  function filtered() {
    var q = (document.getElementById("fSymbol").value || "").trim().toUpperCase();
    var month = document.getElementById("fMonth").value;
    var side = document.getElementById("fSide").value;
    var reason = document.getElementById("fReason").value;
    return state.trades.filter(function (r) {
      if (q && String(r.symbol || "").toUpperCase().indexOf(q) === -1) return false;
      if (month && r.month !== month) return false;
      if (side && r.side !== side) return false;
      if (reason && r.exit_reason !== reason) return false;
      return true;
    });
  }

  function sortRows(rows) {
    var key = state.sortKey;
    var dir = state.sortDir;
    var numeric = { entry_price: 1, exit_price: 1, sl: 1, t1: 1, lot_size: 1, sessions_held: 1, gross_pnl: 1, charges: 1, net_pnl: 1, r_multiple: 1 };
    rows.sort(function (a, b) {
      var av = a[key] == null ? "" : a[key];
      var bv = b[key] == null ? "" : b[key];
      if (numeric[key]) {
        var an = av === "" ? -Infinity : Number(av);
        var bn = bv === "" ? -Infinity : Number(bv);
        return (an - bn) * dir;
      }
      return String(av).localeCompare(String(bv)) * dir;
    });
    return rows;
  }

  function renderTable() {
    var rows = sortRows(filtered());
    var pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
    if (state.page > pages) state.page = pages;
    var start = (state.page - 1) * PAGE_SIZE;
    var slice = rows.slice(start, start + PAGE_SIZE);
    var body = document.getElementById("msTrades");
    body.innerHTML = slice.map(function (r) {
      var net = r.net_pnl;
      return "<tr><td>" + (r.symbol || "") + "</td><td>" + (r.timeframe || "") + "</td><td>" + (r.month || "") +
        "</td><td>" + (r.side || "") + "</td><td>" + (r.entry_type || "") + "</td><td>" + (r.entry_time || "").replace("T", " ").slice(0, 16) +
        "</td><td>" + (r.entry_price || "") + "</td><td>" + (r.exit_time || "").replace("T", " ").slice(0, 16) +
        "</td><td>" + (r.exit_price || "") + "</td><td>" + (r.exit_reason || "") + "</td><td>" + (r.sessions_held || "") +
        "</td><td class=\"" + pnlClass(net) + "\">" + (net === "" || net == null ? "—" : money(net)) + "</td><td>" + (r.r_multiple || "—") + "</td></tr>";
    }).join("");
    document.getElementById("msPage").textContent = "Page " + state.page + " / " + pages + " · " + rows.length + " rows";
    fillMonthOptions();
  }

  function fillMonthOptions() {
    var sel = document.getElementById("fMonth");
    var current = sel.value;
    var months = {};
    state.trades.forEach(function (r) { if (r.month) months[r.month] = 1; });
    var keys = Object.keys(months).sort();
    sel.innerHTML = '<option value="">All months</option>' + keys.map(function (m) {
      return '<option value="' + m + '">' + m + "</option>";
    }).join("");
    sel.value = current;
  }

  function render() {
    renderCards();
    renderMonths();
    renderCharts();
    renderTable();
  }

  function download() {
    var url = apiBase() + "/api/mtf-swing/trades.csv?timeframe=" + encodeURIComponent(state.tf);
    fetch(url, { headers: { Authorization: "Bearer " + token() } })
      .then(function (res) {
        if (!res.ok) throw new Error("download failed");
        return res.text();
      })
      .then(function (text) {
        var blob = new Blob([text], { type: "text/csv" });
        var a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "mtf-swing-" + state.tf + ".csv";
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) { setStatus(err.message); });
  }

  document.addEventListener("DOMContentLoaded", function () {
    state.tf = qsTf();
    document.querySelectorAll(".ms-tf button").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-tf") === state.tf);
      btn.addEventListener("click", function () { setTf(btn.getAttribute("data-tf")); });
    });
    ["fSymbol", "fMonth", "fSide", "fReason"].forEach(function (id) {
      document.getElementById(id).addEventListener("input", function () { state.page = 1; renderTable(); });
      document.getElementById(id).addEventListener("change", function () { state.page = 1; renderTable(); });
    });
    document.getElementById("msPrev").addEventListener("click", function () {
      if (state.page > 1) { state.page -= 1; renderTable(); }
    });
    document.getElementById("msNext").addEventListener("click", function () {
      state.page += 1; renderTable();
    });
    document.getElementById("msDownload").addEventListener("click", download);
    document.querySelectorAll("#msTradeTable th[data-key]").forEach(function (th) {
      th.addEventListener("click", function () {
        var key = th.getAttribute("data-key");
        if (state.sortKey === key) state.sortDir *= -1;
        else { state.sortKey = key; state.sortDir = 1; }
        renderTable();
      });
    });
    load();
  });
})();

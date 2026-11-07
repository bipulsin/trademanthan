/* MTF Swing results — v1 CSV + v2 Experiment tab. */
(function () {
  var PAGE_SIZE = 50;
  var state = {
    mode: "v1",
    tf: "60m",
    trades: [],
    summary: null,
    months: [],
    charts: null,
    sortKey: "entry_time",
    sortDir: -1,
    page: 1,
  };
  var v2 = {
    tf: "all",
    month: "all",
    trades: [],
    summary: null,
    months: [],
    charts: null,
    comparison: null,
    funnel: {},
    open_mtm: {},
    ablation: [],
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

  function qs() {
    return new URLSearchParams(window.location.search);
  }

  function qsTf() {
    var tf = (qs().get("tf") || "60m").toLowerCase();
    if (tf !== "15m" && tf !== "60m" && tf !== "all") tf = "60m";
    return tf;
  }

  function qsMode() {
    var m = (qs().get("mode") || "v1").toLowerCase();
    return m === "v2" ? "v2" : "v1";
  }

  function qsV2Tf() {
    var tf = (qs().get("v2tf") || "all").toLowerCase();
    if (tf !== "15m" && tf !== "60m" && tf !== "all") tf = "all";
    return tf;
  }

  function qsV2Month() {
    var m = qs().get("v2month") || "all";
    if (m !== "2026-06" && m !== "2026-10" && m !== "all") m = "all";
    return m;
  }

  function replaceParams(patch) {
    var url = new URL(window.location.href);
    Object.keys(patch).forEach(function (k) {
      if (patch[k] == null || patch[k] === "") url.searchParams.delete(k);
      else url.searchParams.set(k, patch[k]);
    });
    history.replaceState(null, "", url.pathname + "?" + url.searchParams.toString());
  }

  function setMode(mode) {
    state.mode = mode;
    replaceParams({ mode: mode });
    document.querySelectorAll(".ms-mode button").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-mode") === mode);
    });
    document.getElementById("msV1").hidden = mode !== "v1";
    document.getElementById("msV2").hidden = mode !== "v2";
    if (mode === "v2") loadV2();
    else load();
  }

  function setTf(tf) {
    state.tf = tf;
    state.page = 1;
    replaceParams({ tf: tf });
    document.querySelectorAll("#msV1 .ms-tf button[data-tf]").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-tf") === tf);
    });
    load();
  }

  function setV2Tf(tf) {
    v2.tf = tf;
    v2.page = 1;
    replaceParams({ v2tf: tf });
    document.querySelectorAll("[data-v2-tf]").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-v2-tf") === tf);
    });
    loadV2();
  }

  function setV2Month(month) {
    v2.month = month;
    v2.page = 1;
    replaceParams({ v2month: month });
    document.querySelectorAll("[data-v2-month]").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-v2-month") === month);
    });
    loadV2();
  }

  function setStatus(msg) {
    var el = document.getElementById("msStatus");
    if (el) el.textContent = msg || "";
  }

  function setV2Status(msg) {
    var el = document.getElementById("msV2Status");
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

  async function loadV2() {
    setV2Status("Loading v2 experiment…");
    try {
      var url =
        apiBase() +
        "/api/mtf-swing/v2/results?timeframe=" +
        encodeURIComponent(v2.tf) +
        "&month=" +
        encodeURIComponent(v2.month);
      var res = await fetch(url, {
        headers: { Authorization: "Bearer " + token() },
        cache: "no-store",
      });
      if (!res.ok) throw new Error(res.status + " " + (await res.text()).slice(0, 180));
      var data = await res.json();
      v2.trades = data.trades || [];
      v2.summary = data.summary || {};
      v2.months = data.months || [];
      v2.charts = data.charts || {};
      v2.comparison = data.comparison || null;
      v2.funnel = data.funnel || {};
      v2.open_mtm = data.open_mtm || {};
      v2.ablation = data.ablation || [];
      renderV2();
      var n = v2.trades.length;
      setV2Status(n ? n + " v2 rows from " + (data.csv || "CSV") : "No v2 rows yet. Run the v2 backtest.");
    } catch (err) {
      setV2Status("Could not load v2. " + err.message);
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
    var max = Math.max(1, Math.max.apply(null, vals.map(Math.abs).concat([1])));
    var innerW = w - pad.l - pad.r;
    var innerH = h - pad.t - pad.b;
    var zero = pad.t + innerH / 2;
    svg.appendChild(svgEl("line", { x1: pad.l, x2: w - pad.r, y1: zero, y2: zero, stroke: "rgba(180,210,190,0.35)" }));
    var bw = innerW / Math.max(rows.length, 1);
    rows.forEach(function (r, i) {
      var v = Number(r.net_pnl) || 0;
      var bh = (Math.abs(v) / max) * (innerH / 2);
      var y = v >= 0 ? zero - bh : zero;
      svg.appendChild(svgEl("rect", {
        x: pad.l + i * bw + 4,
        y: y,
        width: Math.max(2, bw - 8),
        height: Math.max(1, bh),
        fill: v >= 0 ? "#3dba7a" : "#e06b6b",
      }));
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

  function sortRows(rows, key, dir) {
    var numeric = {
      entry_price: 1, exit_price: 1, sl: 1, t1: 1, lot_size: 1, sessions_held: 1,
      gross_pnl: 1, charges: 1, net_pnl: 1, r_multiple: 1, potential_rr: 1, mtm_pnl: 1, mtm_price: 1, tp_r: 1,
    };
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
    var rows = sortRows(filtered(), state.sortKey, state.sortDir);
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

  function renderV2() {
    var s = v2.summary || {};
    document.getElementById("msV2Cards").innerHTML = [
      card("Closed trades", num(s.trades, 0)),
      card("Net P&L", money(s.net_pnl), pnlClass(s.net_pnl)),
      card("Win rate", pct(s.win_rate)),
      card("Avg P&L", money(s.avg_pnl), pnlClass(s.avg_pnl)),
      card("Profit factor", pf(s.profit_factor)),
      card("Max DD", money(s.max_drawdown)),
      card("Open trades", num(s.open_trades != null ? s.open_trades : v2.open_mtm.open_trades, 0)),
      card("Open MTM sum", money(s.mtm_pnl_sum != null ? s.mtm_pnl_sum : v2.open_mtm.mtm_pnl_sum), pnlClass(s.mtm_pnl_sum)),
    ].join("");

    var om = v2.open_mtm || {};
    document.getElementById("msV2OpenCards").innerHTML = [
      card("Open count", num(om.open_trades, 0)),
      card("MTM P&L sum", money(om.mtm_pnl_sum), pnlClass(om.mtm_pnl_sum)),
      card("MTM rows", num(om.mtm_available, 0)),
    ].join("");

    var cmp = v2.comparison || { v1: {}, v2: {}, delta: {} };
    var metrics = [
      ["trades", "Closed trades", false],
      ["net_pnl", "Net P&L", true],
      ["win_rate", "Win rate", false],
      ["avg_pnl", "Avg P&L", true],
      ["profit_factor", "Profit factor", false],
      ["max_drawdown", "Max drawdown", true],
      ["open_trades", "Open trades", false],
    ];
    document.getElementById("msV2Compare").innerHTML = metrics.map(function (row) {
      var k = row[0];
      var label = row[1];
      var isMoney = row[2];
      var a = cmp.v1[k];
      var b = cmp.v2[k];
      var d = cmp.delta[k];
      function fmt(v) {
        if (k === "win_rate") return pct(v);
        if (k === "profit_factor") return pf(v);
        if (isMoney) return money(v);
        return num(v, k === "trades" || k === "open_trades" ? 0 : 2);
      }
      return "<tr><td>" + label + "</td><td>" + fmt(a) + "</td><td>" + fmt(b) +
        "</td><td class=\"" + pnlClass(d) + "\">" + (d == null ? "—" : (isMoney || k === "max_drawdown" || k === "avg_pnl" || k === "net_pnl" ? money(d) : num(d, 2))) + "</td></tr>";
    }).join("");

    var funnel = v2.funnel || {};
    var funnelKeys = Object.keys(funnel).sort();
    document.getElementById("msV2Funnel").innerHTML = funnelKeys.length
      ? funnelKeys.map(function (k) { return card(k, num(funnel[k], 0)); }).join("")
      : card("Funnel", "No discard rows yet");

    document.getElementById("msV2Months").innerHTML = (v2.months || []).map(function (m) {
      return "<tr><td>" + m.month + "</td><td>" + num(m.trades, 0) + "</td><td class=\"" + pnlClass(m.net_pnl) + "\">" + money(m.net_pnl) +
        "</td><td class=\"" + pnlClass(m.avg_pnl) + "\">" + money(m.avg_pnl) + "</td><td>" + pct(m.win_rate) +
        "</td><td>" + pf(m.profit_factor) + "</td></tr>";
    }).join("");

    var ab = v2.ablation || [];
    var abSec = document.getElementById("msV2AblationSection");
    if (ab.length) {
      abSec.hidden = false;
      document.getElementById("msV2Ablation").innerHTML = ab.map(function (r) {
        return "<tr><td>" + (r.variant || "") + "</td><td>" + (r.timeframe || "") + "</td><td>" + (r.month || "") +
          "</td><td>" + num(r.trades, 0) + "</td><td class=\"" + pnlClass(r.net_pnl) + "\">" + money(r.net_pnl) +
          "</td><td>" + pct(r.win_rate) + "</td><td>" + pf(r.profit_factor) + "</td><td>" + num(r.open_trades, 0) +
          "</td><td class=\"" + pnlClass(r.mtm_pnl_sum) + "\">" + money(r.mtm_pnl_sum) + "</td></tr>";
      }).join("");
    } else {
      abSec.hidden = true;
    }

    renderV2Table();
  }

  function filteredV2() {
    var q = (document.getElementById("v2Symbol").value || "").trim().toUpperCase();
    var side = document.getElementById("v2Side").value;
    var reason = document.getElementById("v2Reason").value;
    var regime = document.getElementById("v2Regime").value;
    return v2.trades.filter(function (r) {
      if (q && String(r.symbol || "").toUpperCase().indexOf(q) === -1) return false;
      if (side && r.side !== side) return false;
      if (reason && r.exit_reason !== reason) return false;
      if (regime && r.regime !== regime) return false;
      return true;
    });
  }

  function renderV2Table() {
    var rows = sortRows(filteredV2(), v2.sortKey, v2.sortDir);
    var pages = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
    if (v2.page > pages) v2.page = pages;
    var start = (v2.page - 1) * PAGE_SIZE;
    var slice = rows.slice(start, start + PAGE_SIZE);
    document.getElementById("msV2Trades").innerHTML = slice.map(function (r) {
      var net = r.net_pnl;
      var mtm = r.mtm_pnl;
      return "<tr><td>" + (r.symbol || "") + "</td><td>" + (r.timeframe || "") + "</td><td>" + (r.month || "") +
        "</td><td>" + (r.side || "") + "</td><td>" + (r.regime || "") + "</td><td>" + (r.entry_time || "").replace("T", " ").slice(0, 16) +
        "</td><td>" + (r.entry_price || "") + "</td><td>" + (r.sl || "") + "</td><td>" + (r.t1 || "") +
        "</td><td>" + (r.potential_rr || "—") + "</td><td>" + (r.exit_reason || "") +
        "</td><td class=\"" + pnlClass(net) + "\">" + (net === "" || net == null ? "—" : money(net)) +
        "</td><td class=\"" + pnlClass(mtm) + "\">" + (mtm === "" || mtm == null ? "—" : money(mtm)) +
        "</td><td>" + (r.r_multiple || "—") + "</td></tr>";
    }).join("");
    document.getElementById("msV2Page").textContent = "Page " + v2.page + " / " + pages + " · " + rows.length + " rows";
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

  function downloadV2() {
    var url =
      apiBase() +
      "/api/mtf-swing/v2/trades.csv?timeframe=" +
      encodeURIComponent(v2.tf) +
      "&month=" +
      encodeURIComponent(v2.month);
    fetch(url, { headers: { Authorization: "Bearer " + token() } })
      .then(function (res) {
        if (!res.ok) throw new Error("download failed");
        return res.text();
      })
      .then(function (text) {
        var blob = new Blob([text], { type: "text/csv" });
        var a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "mtf-swing-v2-" + v2.tf + "-" + v2.month + ".csv";
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) { setV2Status(err.message); });
  }

  document.addEventListener("DOMContentLoaded", function () {
    state.mode = qsMode();
    state.tf = qsTf();
    v2.tf = qsV2Tf();
    v2.month = qsV2Month();

    document.querySelectorAll(".ms-mode button").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-mode") === state.mode);
      btn.addEventListener("click", function () { setMode(btn.getAttribute("data-mode")); });
    });
    document.getElementById("msV1").hidden = state.mode !== "v1";
    document.getElementById("msV2").hidden = state.mode !== "v2";

    document.querySelectorAll("#msV1 .ms-tf button[data-tf]").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-tf") === state.tf);
      btn.addEventListener("click", function () { setTf(btn.getAttribute("data-tf")); });
    });
    document.querySelectorAll("[data-v2-tf]").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-v2-tf") === v2.tf);
      btn.addEventListener("click", function () { setV2Tf(btn.getAttribute("data-v2-tf")); });
    });
    document.querySelectorAll("[data-v2-month]").forEach(function (btn) {
      btn.classList.toggle("on", btn.getAttribute("data-v2-month") === v2.month);
      btn.addEventListener("click", function () { setV2Month(btn.getAttribute("data-v2-month")); });
    });

    ["fSymbol", "fMonth", "fSide", "fReason"].forEach(function (id) {
      document.getElementById(id).addEventListener("input", function () { state.page = 1; renderTable(); });
      document.getElementById(id).addEventListener("change", function () { state.page = 1; renderTable(); });
    });
    ["v2Symbol", "v2Side", "v2Reason", "v2Regime"].forEach(function (id) {
      document.getElementById(id).addEventListener("input", function () { v2.page = 1; renderV2Table(); });
      document.getElementById(id).addEventListener("change", function () { v2.page = 1; renderV2Table(); });
    });
    document.getElementById("msPrev").addEventListener("click", function () {
      if (state.page > 1) { state.page -= 1; renderTable(); }
    });
    document.getElementById("msNext").addEventListener("click", function () {
      state.page += 1; renderTable();
    });
    document.getElementById("msV2Prev").addEventListener("click", function () {
      if (v2.page > 1) { v2.page -= 1; renderV2Table(); }
    });
    document.getElementById("msV2Next").addEventListener("click", function () {
      v2.page += 1; renderV2Table();
    });
    document.getElementById("msDownload").addEventListener("click", download);
    document.getElementById("msV2Download").addEventListener("click", downloadV2);
    document.querySelectorAll("#msTradeTable th[data-key]").forEach(function (th) {
      th.addEventListener("click", function () {
        var key = th.getAttribute("data-key");
        if (state.sortKey === key) state.sortDir *= -1;
        else { state.sortKey = key; state.sortDir = 1; }
        renderTable();
      });
    });
    document.querySelectorAll("#msV2TradeTable th[data-v2-key]").forEach(function (th) {
      th.addEventListener("click", function () {
        var key = th.getAttribute("data-v2-key");
        if (v2.sortKey === key) v2.sortDir *= -1;
        else { v2.sortKey = key; v2.sortDir = 1; }
        renderV2Table();
      });
    });

    if (state.mode === "v2") loadV2();
    else load();
  });
})();

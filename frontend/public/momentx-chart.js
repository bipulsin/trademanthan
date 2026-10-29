/* Shared MomentX chart modal — candles + EMA(5/15/50) + RSI + Williams %R */
(function (global) {
  "use strict";

  var LWC_URL = "https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js";
  var modalEl = null;
  var chartObjs = [];

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function authHeaders() {
    var t = "";
    try { t = localStorage.getItem("trademanthan_token") || ""; } catch (e) { t = ""; }
    var h = { Accept: "application/json" };
    if (t) h.Authorization = "Bearer " + t;
    return h;
  }

  function loadLwc() {
    if (global.LightweightCharts) return Promise.resolve(global.LightweightCharts);
    return new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = LWC_URL;
      s.async = true;
      s.onload = function () {
        if (global.LightweightCharts) resolve(global.LightweightCharts);
        else reject(new Error("LightweightCharts missing"));
      };
      s.onerror = function () { reject(new Error("Failed to load chart lib")); };
      document.head.appendChild(s);
    });
  }

  function ensureModal() {
    if (modalEl) return modalEl;
    modalEl = document.createElement("div");
    modalEl.className = "mx-chart-modal";
    modalEl.hidden = true;
    modalEl.innerHTML =
      '<div class="mx-chart-modal-card">' +
      '<header class="mx-chart-modal-head">' +
      '<div><h2 class="mx-chart-title" data-mxc-title>Chart</h2>' +
      '<p class="mx-chart-sub" data-mxc-sub></p></div>' +
      '<button type="button" class="mx-btn" data-mxc-close aria-label="Close">&times;</button>' +
      "</header>" +
      '<div class="mx-chart-status" data-mxc-status>Loading…</div>' +
      '<div class="mx-chart-panes">' +
      '<div class="mx-chart-pane mx-chart-pane-main" data-mxc-main></div>' +
      '<div class="mx-chart-pane mx-chart-pane-rsi" data-mxc-rsi></div>' +
      '<div class="mx-chart-pane mx-chart-pane-wr" data-mxc-wr></div>' +
      "</div></div>";
    document.body.appendChild(modalEl);
    modalEl.addEventListener("click", function (ev) {
      if (ev.target === modalEl) closeMomentxChart();
    });
    modalEl.querySelector("[data-mxc-close]").addEventListener("click", closeMomentxChart);
    return modalEl;
  }

  function destroyCharts() {
    chartObjs.forEach(function (c) {
      try { c.remove(); } catch (e) { /* */ }
    });
    chartObjs = [];
  }

  function closeMomentxChart() {
    destroyCharts();
    if (modalEl) modalEl.hidden = true;
  }

  function toLcTime(sec) {
    // Lightweight Charts expects UTCTimestamp (seconds)
    return Number(sec);
  }

  function openMomentxChart(opts) {
    opts = opts || {};
    var ik = opts.instrumentKey || opts.future_instrument_key || "";
    var title = opts.displaySymbol || opts.future_symbol || opts.stock || "Chart";
    if (!ik) {
      window.alert("No instrument key for chart");
      return;
    }
    var root = ensureModal();
    root.hidden = false;
    root.querySelector("[data-mxc-title]").textContent = title;
    root.querySelector("[data-mxc-sub]").textContent = "5m · EMA(5/15/50) · RSI · Williams %R";
    var status = root.querySelector("[data-mxc-status]");
    status.hidden = false;
    status.textContent = "Loading chart…";
    destroyCharts();
    root.querySelector("[data-mxc-main]").innerHTML = "";
    root.querySelector("[data-mxc-rsi]").innerHTML = "";
    root.querySelector("[data-mxc-wr]").innerHTML = "";

    loadLwc()
      .then(function (LWC) {
        var q =
          apiBase() +
          "/api/momentx/chart?instrument_key=" +
          encodeURIComponent(ik) +
          "&interval=minutes/5&days_back=10";
        return fetch(q, { headers: authHeaders() }).then(function (res) {
          return res.json().then(function (data) {
            if (!res.ok) throw new Error((data && data.detail) || res.statusText || "Chart failed");
            return { LWC: LWC, data: data };
          });
        });
      })
      .then(function (pack) {
        var LWC = pack.LWC;
        var bars = (pack.data && pack.data.bars) || [];
        if (!bars.length) {
          status.textContent = "No candle data";
          return;
        }
        status.hidden = true;
        var candles = [];
        var ema5 = [];
        var ema15 = [];
        var ema50 = [];
        var rsi = [];
        var wr = [];
        bars.forEach(function (b) {
          var t = toLcTime(b.time);
          if (!t) return;
          candles.push({ time: t, open: b.open, high: b.high, low: b.low, close: b.close });
          if (b.ema5 != null) ema5.push({ time: t, value: b.ema5 });
          if (b.ema15 != null) ema15.push({ time: t, value: b.ema15 });
          if (b.ema50 != null) ema50.push({ time: t, value: b.ema50 });
          if (b.rsi != null) rsi.push({ time: t, value: b.rsi });
          if (b.williams_r != null) wr.push({ time: t, value: b.williams_r });
        });

        var dark = true;
        var layout = {
          layout: { background: { color: "#0c1210" }, textColor: "#8fa398" },
          grid: {
            vertLines: { color: "rgba(180,210,190,0.12)" },
            horzLines: { color: "rgba(180,210,190,0.12)" },
          },
          rightPriceScale: { borderColor: "rgba(180,210,190,0.2)" },
          timeScale: { borderColor: "rgba(180,210,190,0.2)", timeVisible: true },
        };

        var mainEl = root.querySelector("[data-mxc-main]");
        var main = LWC.createChart(mainEl, Object.assign({ width: mainEl.clientWidth, height: 320 }, layout));
        var candle = main.addCandlestickSeries({
          upColor: "#22c55e",
          downColor: "#ef4444",
          borderVisible: false,
          wickUpColor: "#22c55e",
          wickDownColor: "#ef4444",
        });
        candle.setData(candles);
        main.addLineSeries({ color: "#22c55e", lineWidth: 2, title: "EMA5" }).setData(ema5);
        main.addLineSeries({ color: "#38bdf8", lineWidth: 2, title: "EMA15" }).setData(ema15);
        main.addLineSeries({ color: "#f97316", lineWidth: 2, title: "EMA50" }).setData(ema50);
        chartObjs.push(main);

        var rsiEl = root.querySelector("[data-mxc-rsi]");
        var rsiChart = LWC.createChart(rsiEl, Object.assign({ width: rsiEl.clientWidth, height: 120 }, layout));
        rsiChart.addLineSeries({ color: "#a78bfa", lineWidth: 2, title: "RSI" }).setData(rsi);
        chartObjs.push(rsiChart);

        var wrEl = root.querySelector("[data-mxc-wr]");
        var wrChart = LWC.createChart(wrEl, Object.assign({ width: wrEl.clientWidth, height: 120 }, layout));
        wrChart.addLineSeries({ color: "#f472b6", lineWidth: 2, title: "W%R" }).setData(wr);
        chartObjs.push(wrChart);

        main.timeScale().fitContent();
        rsiChart.timeScale().fitContent();
        wrChart.timeScale().fitContent();
      })
      .catch(function (err) {
        status.hidden = false;
        status.textContent = (err && err.message) || "Chart failed";
      });
  }

  global.openMomentxChart = openMomentxChart;
  global.closeMomentxChart = closeMomentxChart;
})(typeof window !== "undefined" ? window : globalThis);

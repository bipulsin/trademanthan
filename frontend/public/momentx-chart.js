/* MomentX chart — delegates to SecurityChartEngine (dailyfutures layout). */
(function (global) {
  "use strict";

  var ENGINE_SRC = "security-chart/security-chart-engine.js?v=16";

  function ensureChartEngine() {
    if (global.SecurityChartEngine) return Promise.resolve(global.SecurityChartEngine);
    return new Promise(function (resolve, reject) {
      var existing = document.querySelector('script[data-uscm-engine="1"]');
      if (existing) {
        existing.addEventListener("load", function () {
          if (global.SecurityChartEngine) resolve(global.SecurityChartEngine);
          else reject(new Error("Chart module unavailable"));
        });
        existing.addEventListener("error", function () {
          reject(new Error("Failed to load chart module"));
        });
        return;
      }
      var s = document.createElement("script");
      s.src = ENGINE_SRC;
      s.async = true;
      s.setAttribute("data-uscm-engine", "1");
      s.onload = function () {
        if (global.SecurityChartEngine) resolve(global.SecurityChartEngine);
        else reject(new Error("Chart module unavailable"));
      };
      s.onerror = function () {
        reject(new Error("Failed to load chart module"));
      };
      document.head.appendChild(s);
    });
  }

  function underlyingSymbol(opts) {
    var stock = (opts.stock || "").trim();
    if (stock) return stock.toUpperCase();
    var disp = String(opts.displaySymbol || opts.future_symbol || "").trim();
    // Strip common FUT suffixes like RELIANCE25OCTFUT / RELIANCE FUT
    var m = disp.match(/^([A-Z0-9&-]+?)(?:\d{2}[A-Z]{3}FUT|\s*FUT)?$/i);
    if (m && m[1]) return m[1].toUpperCase();
    return disp.toUpperCase() || "UNKNOWN";
  }

  function openMomentxChart(opts) {
    opts = opts || {};
    var ik = opts.instrumentKey || opts.future_instrument_key || "";
    if (!ik) {
      window.alert("No instrument key for chart");
      return;
    }
    var display = opts.displaySymbol || opts.future_symbol || opts.stock || "Chart";
    ensureChartEngine()
      .then(function (eng) {
        if (!eng || typeof eng.openSecurityChart !== "function") {
          throw new Error("Chart module unavailable");
        }
        return eng.openSecurityChart({
          symbol: underlyingSymbol(opts),
          instrumentType: "FUT",
          instrumentKey: ik,
          displaySymbol: display,
          exchange: "NSE",
          timeframe: opts.timeframe || "5m",
          skipSavedPrefs: true,
          emaEnabled: true,
          emaPeriod: 5,
          ema2Enabled: true,
          ema2Period: 15,
          ema3Enabled: true,
          ema3Period: 50,
          vwapEnabled: true,
          volumeEnabled: true,
          rsiEnabled: true,
          williamsEnabled: true,
          rsiPeriod: 14,
          rsiUpper: 55,
          rsiLower: 35,
          wrPeriod: 14,
          wrUpper: -25,
          wrLower: -75,
          metadata: { algo: "momentx" },
        });
      })
      .catch(function (err) {
        if (global.console && global.console.warn) {
          global.console.warn("MomentX chart:", err);
        }
        window.alert((err && err.message) || "Chart failed");
      });
  }

  function closeMomentxChart() {
    if (global.SecurityChartEngine && typeof global.SecurityChartEngine.closeSecurityChart === "function") {
      global.SecurityChartEngine.closeSecurityChart();
    }
  }

  global.openMomentxChart = openMomentxChart;
  global.closeMomentxChart = closeMomentxChart;
})(typeof window !== "undefined" ? window : globalThis);

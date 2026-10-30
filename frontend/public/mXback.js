/* MomentX Stage-1 backtest desk — results from momentx_backtest_qualified */
(function () {
  "use strict";

  function apiBase() {
    if (typeof trademanthanApiBase === "function") return trademanthanApiBase();
    var h = window.location.hostname;
    if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
    return window.location.origin;
  }

  function authHeaders() {
    var t = "";
    try {
      t = localStorage.getItem("trademanthan_token") || "";
    } catch (e) {
      t = "";
    }
    var h = { Accept: "application/json", "Content-Type": "application/json" };
    if (t) h.Authorization = "Bearer " + t;
    return h;
  }

  function $(id) {
    return document.getElementById(id);
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtNum(v, d) {
    if (v == null || v === "" || !Number.isFinite(Number(v))) return "—";
    return Number(v).toFixed(d == null ? 2 : d);
  }

  function fmtVol(v) {
    if (v == null || v === "" || !Number.isFinite(Number(v))) return "—";
    var n = Number(v);
    if (n >= 1e7) return (n / 1e7).toFixed(2) + " Cr";
    if (n >= 1e5) return (n / 1e5).toFixed(2) + " L";
    if (n >= 1e3) return (n / 1e3).toFixed(1) + " K";
    return String(Math.round(n));
  }

  function fmtIstTime(iso) {
    if (!iso) return "—";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return "—";
      return d.toLocaleTimeString("en-IN", {
        timeZone: "Asia/Kolkata",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
    } catch (e) {
      return "—";
    }
  }

  var pollTimer = null;
  var activeRunId = null;
  var runBusy = false;

  function setRunEnabled(on) {
    var btn = $("mxBtRun");
    if (!btn) return;
    btn.disabled = !on;
    btn.classList.toggle("is-disabled", !on);
    btn.setAttribute("aria-disabled", on ? "false" : "true");
    btn.title = on ? "Run backtest (reuses stored days)" : "Backtest running…";
  }

  function setStatus(text) {
    var el = $("mxBtStatus");
    if (el) el.textContent = "Status: " + text;
  }

  function dayBlock(day) {
    var rows = day.symbols || [];
    var body = rows
      .map(function (r) {
        return (
          "<tr>" +
          "<td>" +
          esc(r.stock) +
          "</td>" +
          "<td>" +
          esc(r.future_symbol || "—") +
          "</td>" +
          "<td>" +
          esc(fmtIstTime(r.first_filtered_at)) +
          "</td>" +
          "<td>" +
          esc(fmtVol(r.today_volume)) +
          "</td>" +
          "<td>" +
          esc(fmtVol(r.volume_sma5)) +
          "</td>" +
          "<td>" +
          esc(fmtNum(r.price)) +
          "</td>" +
          "<td>" +
          esc(fmtNum(r.pct_change)) +
          "%</td>" +
          "<td>" +
          esc(fmtNum(r.rsi)) +
          "</td>" +
          "<td>" +
          esc(fmtNum(r.williams_r)) +
          "</td>" +
          "<td>" +
          esc(fmtNum(r.adx)) +
          "</td>" +
          "<td>" +
          esc(fmtNum(r.sma200, 1)) +
          "</td>" +
          "<td>" +
          esc(r.lot_size != null ? r.lot_size : "—") +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
    return (
      '<section class="mx-bt-day">' +
      '<div class="mx-log-day-head">' +
      "<h2>" +
      esc(day.trade_date) +
      "</h2>" +
      '<span class="mx-badge mx-badge-accent">' +
      esc(String(day.count || rows.length)) +
      " qualified</span>" +
      "</div>" +
      '<div class="mx-log-table-wrap"><table class="mx-log-table">' +
      "<thead><tr>" +
      "<th>Stock</th><th>Future</th><th>1st filter</th><th>Day vol</th><th>Vol SMA5</th>" +
      "<th>Price</th><th>%Chg</th>" +
      "<th>RSI</th><th>W%R</th><th>ADX</th><th>SMA200</th><th>Lot</th>" +
      "</tr></thead><tbody>" +
      (body || '<tr><td colspan="12">No rows</td></tr>') +
      "</tbody></table></div></section>"
    );
  }

  function renderResults(data) {
    var root = $("mxBtRoot");
    var empty = $("mxBtEmpty");
    var summary = $("mxBtSummary");
    var days = (data && data.days) || [];
    if (!days.length) {
      root.hidden = true;
      summary.hidden = true;
      empty.hidden = false;
      return;
    }
    empty.hidden = true;
    summary.hidden = false;
    var cov = data.coverage;
    var covNote = "";
    if (cov) {
      covNote =
        " · stored " +
        esc(String(cov.covered_days || 0)) +
        "/" +
        esc(String(cov.trading_days || 0)) +
        " days";
      if (cov.complete) covNote += " (complete)";
    }
    summary.innerHTML =
      "<strong>" +
      (data.from_storage ? "Stored results" : "Run #" + esc(data.run_id != null ? data.run_id : "—")) +
      "</strong> · " +
      esc(String(data.day_count || days.length)) +
      " days with hits · " +
      esc(String(data.total || 0)) +
      " qualified rows" +
      covNote;
    root.hidden = false;
    root.innerHTML = days.map(dayBlock).join("");
  }

  async function loadResults() {
    $("mxBtLoading").hidden = false;
    $("mxBtError").hidden = true;
    var from = $("mxBtFrom").value;
    var to = $("mxBtTo").value;
    // Always load merged storage for the date window (no run_id) so appended
    // history is reused. activeRunId is only for status polling.
    var q =
      "/api/momentx/backtest/results?date_from=" +
      encodeURIComponent(from) +
      "&date_to=" +
      encodeURIComponent(to);
    try {
      var res = await fetch(apiBase() + q, { headers: authHeaders() });
      var data = await res.json();
      if (!res.ok) throw new Error((data && data.detail) || res.statusText);
      renderResults(data);
      if (!runBusy) setRunEnabled(true);
    } catch (err) {
      $("mxBtError").hidden = false;
      $("mxBtError").textContent = (err && err.message) || "Load failed";
      if (!runBusy) setRunEnabled(true);
    } finally {
      $("mxBtLoading").hidden = true;
    }
  }

  async function pollStatus() {
    try {
      var q = "/api/momentx/backtest/status";
      if (activeRunId) q += "?run_id=" + encodeURIComponent(String(activeRunId));
      var res = await fetch(apiBase() + q, { headers: authHeaders() });
      var data = await res.json();
      var run = data && data.run;
      if (data && data.active_run_id) {
        activeRunId = data.active_run_id;
        runBusy = true;
        setRunEnabled(false);
      }
      if (!run && !(data && data.active_run_id)) {
        setStatus("idle");
        runBusy = false;
        setRunEnabled(true);
        return false;
      }
      if (!run) {
        setStatus("running…");
        return true;
      }
      var bits = [
        "#" + run.id,
        run.status,
        run.trading_days != null ? "days=" + run.trading_days : null,
        run.qualified_rows != null ? "qual=" + run.qualified_rows : null,
      ].filter(Boolean);
      setStatus(bits.join(" · "));
      if (run.status === "done") {
        activeRunId = run.id;
        runBusy = false;
        await loadResults();
        setRunEnabled(true);
        return false;
      }
      if (run.status === "error") {
        runBusy = false;
        setRunEnabled(true);
        $("mxBtError").hidden = false;
        $("mxBtError").textContent = run.error || "Backtest failed";
        return false;
      }
      if (run.status === "running" || run.status === "queued") {
        runBusy = true;
        setRunEnabled(false);
        return true;
      }
      runBusy = false;
      setRunEnabled(true);
      return false;
    } catch (e) {
      setStatus("status check failed");
      runBusy = false;
      setRunEnabled(true);
      return false;
    }
  }

  function startPolling() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(function () {
      pollStatus().then(function (keep) {
        if (!keep && pollTimer) {
          clearInterval(pollTimer);
          pollTimer = null;
        }
      });
    }, 4000);
  }

  async function runBacktest() {
    if (runBusy) return;
    $("mxBtError").hidden = true;
    var body = {
      date_from: $("mxBtFrom").value,
      date_to: $("mxBtTo").value,
      asof_hhmm: ($("mxBtAsof").value || "14:45").slice(0, 5),
      // Append / reuse stored days — do not wipe prior ranges.
      replace_range: false,
    };
    setStatus("checking storage…");
    setRunEnabled(false);
    runBusy = true;
    try {
      var res = await fetch(apiBase() + "/api/momentx/backtest/run", {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(body),
      });
      var data = await res.json();
      if (!res.ok || !data.ok) {
        throw new Error((data && (data.error || data.detail)) || res.statusText);
      }
      if (data.cached) {
        setStatus("from storage (no rescan)");
        runBusy = false;
        await loadResults();
        setRunEnabled(true);
        return;
      }
      activeRunId = data.run_id;
      var miss =
        data.coverage && data.coverage.missing_days != null
          ? " · missing " + data.coverage.missing_days + " days"
          : "";
      setStatus("queued #" + data.run_id + miss);
      startPolling();
      await pollStatus();
    } catch (err) {
      runBusy = false;
      setRunEnabled(true);
      $("mxBtError").hidden = false;
      $("mxBtError").textContent = (err && err.message) || "Run failed";
      setStatus("error");
    }
  }

  function bind() {
    $("mxBtRun").addEventListener("click", runBacktest);
    $("mxBtReload").addEventListener("click", function () {
      loadResults();
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    bind();
    setRunEnabled(true);
    loadResults();
    pollStatus().then(function (keep) {
      if (keep) startPolling();
    });
  });
})();

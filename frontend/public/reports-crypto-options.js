/**
 * Crypto Options Report — paired BTC straddles from Delta Exchange India.
 */
(function () {
  const API_BASE =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      ? "http://localhost:8000"
      : window.location.origin;

  const REPORT_API = "/api/crypto-options/report";
  const REFRESH_API = "/api/crypto-options/refresh";
  let reportData = [];

  function $(id) {
    return document.getElementById("cryptoOpt_" + id);
  }

  function authHeaders() {
    const t = localStorage.getItem("trademanthan_token") || "";
    const headers = { Accept: "application/json" };
    if (t) headers.Authorization = "Bearer " + t;
    return headers;
  }

  function firstDayOfCurrentMonth() {
    const d = new Date();
    return new Date(d.getFullYear(), d.getMonth(), 1);
  }

  function num(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "—";
    return n.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function pnlClass(v) {
    if (v == null || v === "") return "";
    return Number(v) >= 0 ? "intraday-positive" : "intraday-negative";
  }

  function money(v, currency) {
    if (v == null || v === "") return "—";
    const label = currency ? " " + currency : "";
    return num(v) + label;
  }

  function when(v) {
    if (!v) return "—";
    return esc(String(v).replace("T", " ").slice(0, 16));
  }

  function buyCell(buy, mark, open) {
    if (buy != null && buy !== "") return esc(num(buy));
    if (open && mark != null && mark !== "") return esc(num(mark)) + ' <span class="co-mark">mark</span>';
    return "—";
  }

  function strikeLabel(row) {
    const n = Number(row.strike);
    const strike = Number.isFinite(n) ? n.toLocaleString("en-IN", { maximumFractionDigits: 0 }) : "—";
    return strike;
  }

  async function loadReport() {
    const startDate = $("startDate").value;
    const endDate = $("endDate").value;
    $("loading").style.display = "block";
    $("noData").style.display = "none";
    $("reportTable").style.display = "none";
    $("summaryCards").style.display = "none";
    try {
      const params = new URLSearchParams();
      if (startDate) params.append("start_date", startDate);
      if (endDate) params.append("end_date", endDate);
      const res = await fetch(`${API_BASE}${REPORT_API}?${params.toString()}`, {
        headers: authHeaders(),
        cache: "no-store",
      });
      const result = await res.json().catch(function () { return {}; });
      $("loading").style.display = "none";
      if (!res.ok) {
        $("noDataMsg").textContent = res.status === 401 ? "Sign in to view this report" : "Could not load the report";
        $("noData").style.display = "block";
        return;
      }
      if (result.success && Array.isArray(result.data) && result.data.length > 0) {
        reportData = result.data;
        displayReport(result.data, result.summary || {});
      } else {
        reportData = [];
        $("noDataMsg").textContent = "Try adjusting your date range";
        $("noData").style.display = "block";
      }
    } catch (e) {
      console.error("crypto options report error", e);
      $("loading").style.display = "none";
      $("noDataMsg").textContent = "Could not load the report";
      $("noData").style.display = "block";
    }
  }

  function displayReport(data, summary) {
    const currency = summary.quote_currency || (data[0] && data[0].quote_currency) || "";
    const pnlLabel = currency ? "P&L (" + currency + ")" : "P&L";
    $("summaryCards").innerHTML = `
      <div class="intraday-summary-card">
        <h3>Trades</h3>
        <div class="intraday-value">${summary.total_trades || 0}</div>
      </div>
      <div class="intraday-summary-card">
        <h3>Open</h3>
        <div class="intraday-value">${summary.open_trades || 0}</div>
      </div>
      <div class="intraday-summary-card">
        <h3>Closed</h3>
        <div class="intraday-value">${summary.closed_trades || 0}</div>
      </div>
      <div class="intraday-summary-card">
        <h3>${esc(pnlLabel)}</h3>
        <div class="intraday-value ${pnlClass(summary.total_pnl)}">${esc(money(summary.total_pnl, currency))}</div>
      </div>
    `;
    $("summaryCards").style.display = "grid";

    const rows = data.map(function (row) {
      const open = row.status === "OPEN";
      const statusClass = open ? "co-status co-status-open" : "co-status co-status-closed";
      return `
        <tr>
          <td>${esc(row.session_date || "—")}</td>
          <td>${esc(strikeLabel(row))}</td>
          <td>${esc(row.expiry || "—")}</td>
          <td>${esc(num(row.ce_sell_price))}</td>
          <td>${esc(num(row.pe_sell_price))}</td>
          <td>${buyCell(row.ce_buy_price, row.ce_mark, open)}</td>
          <td>${buyCell(row.pe_buy_price, row.pe_mark, open)}</td>
          <td>${when(row.entry_time)}</td>
          <td>${when(row.exit_time)}</td>
          <td class="${statusClass}">${esc(row.status || "—")}</td>
          <td>${esc(money(row.entry_premium, row.quote_currency))}</td>
          <td>${esc(money(row.exit_premium, row.quote_currency))}</td>
          <td class="${pnlClass(row.pnl)}"><strong>${esc(money(row.pnl, row.quote_currency))}</strong></td>
        </tr>
      `;
    }).join("");

    $("reportTable").innerHTML = `
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Strike</th>
            <th>Expiry</th>
            <th>CE sell</th>
            <th>PE sell</th>
            <th>CE buy</th>
            <th>PE buy</th>
            <th>Entry</th>
            <th>Exit</th>
            <th>Status</th>
            <th>Entry prem.</th>
            <th>Exit prem.</th>
            <th>${esc(pnlLabel)}</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
    $("reportTable").style.display = "block";
  }

  function clearFilters() {
    $("endDate").valueAsDate = new Date();
    $("startDate").valueAsDate = firstDayOfCurrentMonth();
    loadReport();
  }

  async function refreshFromExchange() {
    $("loading").style.display = "block";
    $("noData").style.display = "none";
    try {
      const res = await fetch(`${API_BASE}${REFRESH_API}`, {
        method: "POST",
        headers: authHeaders(),
      });
      const body = await res.json().catch(function () { return {}; });
      if (!res.ok) {
        const detail = body && (body.detail || body.error);
        $("loading").style.display = "none";
        $("noDataMsg").textContent = typeof detail === "string" ? detail : "Refresh failed";
        $("noData").style.display = "block";
        return;
      }
    } catch (e) {
      console.error("crypto options refresh error", e);
    }
    await loadReport();
  }

  function boot() {
    const root = document.getElementById("cryptoOptReportRoot");
    if (!root) return;
    const end = $("endDate");
    const start = $("startDate");
    if (end && !end.value) end.valueAsDate = new Date();
    if (start && !start.value) start.valueAsDate = firstDayOfCurrentMonth();
    loadReport();
  }

  window.cryptoOptLoadReport = loadReport;
  window.cryptoOptClearFilters = clearFilters;
  window.cryptoOptRefresh = refreshFromExchange;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();

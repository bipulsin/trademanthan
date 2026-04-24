/**
 * Futures Trading Report (Daily Futures + Smart Futures sold trades).
 */
(function () {
  const API_BASE =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      ? "http://localhost:8000"
      : window.location.origin;

  const REPORT_API = "/api/futures-reports/trading-report";
  const DAILY_API = "/api/futures-reports/daily-trades";
  const expandedRows = {};
  let reportData = [];

  function $(id) {
    return document.getElementById("futures_" + id);
  }

  function authHeaders() {
    const t = localStorage.getItem("trademanthan_token") || "";
    return t ? { Authorization: "Bearer " + t } : {};
  }

  function firstDayOfCurrentMonth() {
    const d = new Date();
    return new Date(d.getFullYear(), d.getMonth(), 1);
  }

  async function loadReport() {
    const startDate = $("startDate").value;
    const endDate = $("endDate").value;
    const source = $("source").value;

    $("loading").style.display = "block";
    $("noData").style.display = "none";
    $("reportTable").style.display = "none";
    $("summaryCards").style.display = "none";

    try {
      const params = new URLSearchParams();
      if (startDate) params.append("start_date", startDate);
      if (endDate) params.append("end_date", endDate);
      if (source) params.append("source", source);

      const res = await fetch(`${API_BASE}${REPORT_API}?${params.toString()}`, {
        headers: authHeaders(),
        cache: "no-store",
      });
      const result = await res.json();
      $("loading").style.display = "none";

      if (result.success && Array.isArray(result.data) && result.data.length > 0) {
        reportData = result.data;
        displayReport(result.data, result.summary || {});
      } else {
        $("noData").style.display = "block";
      }
    } catch (e) {
      console.error("futures report error", e);
      $("loading").style.display = "none";
      $("noData").style.display = "block";
    }
  }

  function displayReport(data, summary) {
    const summaryHTML = `
      <div class="intraday-summary-card">
        <h3>Days</h3>
        <div class="intraday-value">${summary.total_days || 0}</div>
      </div>
      <div class="intraday-summary-card">
        <h3>Total Trades</h3>
        <div class="intraday-value">${summary.total_trades || 0}</div>
      </div>
      <div class="intraday-summary-card intraday-mobile-hide">
        <h3>Overall P&L</h3>
        <div class="intraday-value">₹${Number(summary.overall_pnl || 0).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
      </div>
    `;
    $("summaryCards").innerHTML = summaryHTML;
    $("summaryCards").style.display = "grid";

    const tableHTML = `
      <table>
        <thead>
          <tr>
            <th class="intraday-th-date">Date</th>
            <th>Total Trades</th>
            <th class="intraday-col-hide-mobile">Daily Futures</th>
            <th class="intraday-col-hide-mobile">Smart Futures</th>
            <th class="intraday-th-winrate">Win Rate</th>
            <th class="intraday-th-pnl">P&L</th>
          </tr>
        </thead>
        <tbody>
          ${data
            .map(
              (day, index) => `
              <tr class="intraday-data-row" onclick="window.futuresToggleTradeDetails('${day.date}', ${index})">
                <td class="intraday-date-cell">
                  <i class="fas fa-chevron-right" id="futures-expand-icon-${index}" style="font-size: 10px; margin-right: 8px; transition: transform 0.3s;"></i>
                  <strong><span class="intraday-date-full">${formatDate(day.date)}</span><span class="intraday-date-short">${formatDateShort(day.date)}</span></strong>
                </td>
                <td>${day.total_trades}</td>
                <td class="intraday-col-hide-mobile">${day.daily_futures_trades}</td>
                <td class="intraday-col-hide-mobile">${day.smart_futures_trades}</td>
                <td class="intraday-cell-winrate"><strong>${Number(day.win_rate || 0).toFixed(1)}%</strong></td>
                <td class="${Number(day.total_pnl || 0) >= 0 ? "intraday-positive" : "intraday-negative"} intraday-cell-pnl">
                  <strong>₹${Number(day.total_pnl || 0).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</strong>
                </td>
              </tr>
              <tr class="intraday-expandable-row" id="futures-details-row-${index}">
                <td colspan="6">
                  <div class="intraday-trade-details" id="futures-trade-details-${index}">
                    <div class="intraday-details-loading">
                      <i class="fas fa-spinner fa-spin"></i> Loading trade details...
                    </div>
                  </div>
                </td>
              </tr>
            `
            )
            .join("")}
        </tbody>
      </table>
    `;
    $("reportTable").innerHTML = tableHTML;
    $("reportTable").style.display = "block";
  }

  async function toggleTradeDetails(date, rowIndex) {
    const detailsRow = document.getElementById(`futures-details-row-${rowIndex}`);
    const expandIcon = document.getElementById(`futures-expand-icon-${rowIndex}`);
    if (expandedRows[rowIndex]) {
      detailsRow.classList.remove("expanded");
      expandIcon.style.transform = "rotate(0deg)";
      expandedRows[rowIndex] = false;
      return;
    }
    detailsRow.classList.add("expanded");
    expandIcon.style.transform = "rotate(90deg)";
    expandedRows[rowIndex] = true;
    try {
      const source = $("source").value || "all";
      const res = await fetch(`${API_BASE}${DAILY_API}/${date}?source=${encodeURIComponent(source)}`, {
        headers: authHeaders(),
        cache: "no-store",
      });
      const result = await res.json();
      const trades = result.success && Array.isArray(result.trades) ? result.trades : [];
      if (!trades.length) {
        document.getElementById(`futures-trade-details-${rowIndex}`).innerHTML =
          '<div class="intraday-details-empty"><i class="fas fa-info-circle"></i> No sold futures trades for this day</div>';
        return;
      }
      const detailsHTML = `
        <table class="intraday-trade-details-table">
          <thead>
            <tr>
              <th>Source</th><th>Symbol</th><th>Qty</th><th>Entry Price</th><th>Entry Time</th><th>Exit Price</th><th>Exit Time</th><th>P&L</th>
            </tr>
          </thead>
          <tbody>
            ${trades
              .map(
                (t) => `
                <tr class="intraday-trade-detail-row">
                  <td data-intraday-detail-label="Source">${t.source || "-"}</td>
                  <td data-intraday-detail-label="Symbol"><strong>${symbolWithDirection(t.symbol, t.direction_type)}</strong></td>
                  <td data-intraday-detail-label="Qty">${t.qty != null ? t.qty : "-"}</td>
                  <td data-intraday-detail-label="Entry">₹${num2(t.entry_price)}</td>
                  <td data-intraday-detail-label="Entry Time">${t.entry_time || "-"}</td>
                  <td data-intraday-detail-label="Exit">₹${num2(t.exit_price)}</td>
                  <td data-intraday-detail-label="Exit Time">${t.exit_time || "-"}</td>
                  <td class="${Number(t.pnl || 0) >= 0 ? "intraday-positive" : "intraday-negative"}" data-intraday-detail-label="P&L">
                    <strong>₹${num2(t.pnl)}</strong>
                  </td>
                </tr>
              `
              )
              .join("")}
          </tbody>
        </table>
      `;
      document.getElementById(`futures-trade-details-${rowIndex}`).innerHTML = detailsHTML;
    } catch (e) {
      document.getElementById(`futures-trade-details-${rowIndex}`).innerHTML =
        '<div class="intraday-details-error"><i class="fas fa-exclamation-triangle"></i> Error loading trade details</div>';
    }
  }

  function num2(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "0.00";
    return n.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function symbolWithDirection(sym, dir) {
    const s = String(sym || "-");
    const d = String(dir || "").trim().toUpperCase();
    if (!d) return s;
    return s + " (" + d + ")";
  }

  function formatDate(dateStr) {
    const date = new Date(dateStr);
    const options = { year: "numeric", month: "short", day: "numeric", weekday: "short" };
    return date.toLocaleDateString("en-IN", options);
  }

  function formatDateShort(dateStr) {
    if (!dateStr || typeof dateStr !== "string") return "";
    const parts = dateStr.split("-");
    if (parts.length !== 3) return dateStr;
    const m = parseInt(parts[1], 10) - 1;
    const d = parseInt(parts[2], 10);
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    if (m < 0 || m > 11 || !d) return dateStr;
    return `${d}-${months[m]}`;
  }

  function clearFilters() {
    const today = new Date();
    $("endDate").valueAsDate = today;
    $("startDate").valueAsDate = firstDayOfCurrentMonth();
    $("source").value = "all";
    loadReport();
  }

  function downloadReport() {
    if (!reportData.length) {
      alert("No data to download");
      return;
    }
    const headers = ["Date", "Total Trades", "Daily Futures Trades", "Smart Futures Trades", "Win Rate %", "Total P&L"];
    const rows = reportData.map((d) => [
      d.date,
      d.total_trades,
      d.daily_futures_trades,
      d.smart_futures_trades,
      Number(d.win_rate || 0).toFixed(2),
      Number(d.total_pnl || 0).toFixed(2),
    ]);
    const csv = [headers.join(","), ...rows.map((r) => r.join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `futures-trading-report-${new Date().toISOString().split("T")[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
  }

  function init() {
    const root = document.getElementById("futuresReportRoot");
    if (!root) return;
    const today = new Date();
    $("endDate").valueAsDate = today;
    $("startDate").valueAsDate = firstDayOfCurrentMonth();
    loadReport();
  }

  window.futuresLoadReport = loadReport;
  window.futuresClearFilters = clearFilters;
  window.futuresDownloadReport = downloadReport;
  window.futuresToggleTradeDetails = toggleTradeDetails;
  document.addEventListener("DOMContentLoaded", init);
})();


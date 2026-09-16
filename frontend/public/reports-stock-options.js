/**
 * Stock Options Selling report (Live completed trades from stockOptions Trade Report).
 */
(function () {
  const API_BASE =
    window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      ? "http://localhost:8000"
      : window.location.origin;

  const REPORT_API = "/api/stock-options/selling-report";
  const expandedRows = {};
  let reportData = [];

  function $(id) {
    return document.getElementById("soSelling_" + id);
  }

  function authHeaders() {
    const t = localStorage.getItem("trademanthan_token") || "";
    return t ? { Authorization: "Bearer " + t } : {};
  }

  function firstDayOfCurrentMonth() {
    const d = new Date();
    return new Date(d.getFullYear(), d.getMonth(), 1);
  }

  function num2(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return "0.00";
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

  function formatDateTime(v) {
    if (!v) return "—";
    const s = String(v).replace("T", " ");
    if (/ 00:00(:00)?$/.test(s) || !/\d{1,2}:\d{2}/.test(s)) {
      return s.slice(0, 10);
    }
    return s.replace(/:\d{2}$/, "");
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

  function pnlClass(v) {
    return Number(v || 0) >= 0 ? "intraday-positive" : "intraday-negative";
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
      const result = await res.json();
      $("loading").style.display = "none";

      if (result.success && Array.isArray(result.data) && result.data.length > 0) {
        reportData = result.data;
        displayReport(result.data, result.summary || {});
      } else {
        $("noData").style.display = "block";
      }
    } catch (e) {
      console.error("stock options selling report error", e);
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
      <div class="intraday-summary-card">
        <h3>Overall P&amp;L</h3>
        <div class="intraday-value ${pnlClass(summary.overall_pnl)}">₹${num2(summary.overall_pnl)}</div>
      </div>
    `;
    $("summaryCards").innerHTML = summaryHTML;
    $("summaryCards").style.display = "grid";

    const tableHTML = `
      <table>
        <thead>
          <tr>
            <th class="intraday-th-date">Date</th>
            <th>Trades</th>
            <th class="intraday-col-hide-mobile">Win Rate</th>
            <th class="intraday-th-pnl">Day P&amp;L</th>
            <th class="intraday-th-pnl">Cumulative</th>
          </tr>
        </thead>
        <tbody>
          ${data
            .map(
              (day, index) => `
              <tr class="intraday-data-row" onclick="window.soSellingToggleTradeDetails('${day.date}', ${index})">
                <td class="intraday-date-cell">
                  <i class="fas fa-chevron-right" id="soSelling-expand-icon-${index}" style="font-size: 10px; margin-right: 8px; transition: transform 0.3s;"></i>
                  <strong><span class="intraday-date-full">${formatDate(day.date)}</span><span class="intraday-date-short">${formatDateShort(day.date)}</span></strong>
                </td>
                <td>${day.total_trades}</td>
                <td class="intraday-col-hide-mobile intraday-cell-winrate"><strong>${Number(day.win_rate || 0).toFixed(1)}%</strong></td>
                <td class="${pnlClass(day.total_pnl)} intraday-cell-pnl">
                  <strong>₹${num2(day.total_pnl)}</strong>
                </td>
                <td class="${pnlClass(day.cumulative_pnl)} intraday-cell-pnl">
                  <strong>₹${num2(day.cumulative_pnl)}</strong>
                </td>
              </tr>
              <tr class="intraday-expandable-row" id="soSelling-details-row-${index}">
                <td colspan="5">
                  <div class="intraday-trade-details" id="soSelling-trade-details-${index}"></div>
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

  function strikeLine(t) {
    const sell = t.user_sell_strike != null ? t.user_sell_strike : "—";
    const buy = t.user_buy_strike != null ? t.user_buy_strike : "—";
    return "Sell " + esc(sell) + " / Buy " + esc(buy);
  }

  function costLine(sell, buy) {
    const s = sell != null && sell !== "" ? num2(sell) : "—";
    const b = buy != null && buy !== "" ? num2(buy) : "—";
    return "Sell " + s + " / Buy " + b;
  }

  function detailsHtml(day) {
    const trades = Array.isArray(day.trades) ? day.trades : [];
    if (!trades.length) {
      return '<div class="intraday-details-empty"><i class="fas fa-info-circle"></i> No live completed trades for this day</div>';
    }
    return `
      <table class="intraday-trade-details-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Side</th>
            <th class="intraday-col-hide-mobile">Contract</th>
            <th>Strikes</th>
            <th class="intraday-col-hide-mobile">Entry</th>
            <th class="intraday-col-hide-mobile">Exit</th>
            <th>Exit date</th>
            <th>P&amp;L</th>
          </tr>
        </thead>
        <tbody>
          ${trades
            .map((t) => {
              const pnl = t.pnl != null ? t.pnl : t.pnl_points;
              return `
                <tr class="intraday-trade-detail-row">
                  <td data-intraday-detail-label="Symbol"><strong>${esc(t.symbol || "—")}</strong></td>
                  <td data-intraday-detail-label="Side">${esc(t.side || "—")}</td>
                  <td class="intraday-col-hide-mobile" data-intraday-detail-label="Contract">${esc(t.contract_mmm_yyyy || "—")}</td>
                  <td data-intraday-detail-label="Strikes">${strikeLine(t)}</td>
                  <td class="intraday-col-hide-mobile" data-intraday-detail-label="Entry">${costLine(t.sell_cost, t.buy_cost)}</td>
                  <td class="intraday-col-hide-mobile" data-intraday-detail-label="Exit">${costLine(t.sell_exit_price, t.buy_exit_price)}</td>
                  <td data-intraday-detail-label="Exit date">${esc(formatDateTime(t.exit_date))}</td>
                  <td class="${pnlClass(pnl)}" data-intraday-detail-label="P&amp;L">
                    <strong>${t.pnl != null ? "₹" + num2(t.pnl) : t.pnl_points != null ? num2(t.pnl_points) + " pts" : "—"}</strong>
                  </td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      </table>
    `;
  }

  function toggleTradeDetails(date, rowIndex) {
    const detailsRow = document.getElementById(`soSelling-details-row-${rowIndex}`);
    const expandIcon = document.getElementById(`soSelling-expand-icon-${rowIndex}`);
    if (!detailsRow || !expandIcon) return;
    if (expandedRows[rowIndex]) {
      detailsRow.classList.remove("expanded");
      expandIcon.style.transform = "rotate(0deg)";
      expandedRows[rowIndex] = false;
      return;
    }
    detailsRow.classList.add("expanded");
    expandIcon.style.transform = "rotate(90deg)";
    expandedRows[rowIndex] = true;
    const day = reportData[rowIndex];
    document.getElementById(`soSelling-trade-details-${rowIndex}`).innerHTML = detailsHtml(day || { trades: [] });
  }

  function clearFilters() {
    const today = new Date();
    $("endDate").valueAsDate = today;
    $("startDate").valueAsDate = firstDayOfCurrentMonth();
    loadReport();
  }

  function downloadReport() {
    if (!reportData.length) {
      alert("No data to download");
      return;
    }
    const headers = ["Date", "Trades", "Win Rate %", "Day P&L", "Cumulative P&L"];
    const rows = reportData.map((d) => [
      d.date,
      d.total_trades,
      Number(d.win_rate || 0).toFixed(2),
      Number(d.total_pnl || 0).toFixed(2),
      Number(d.cumulative_pnl || 0).toFixed(2),
    ]);
    const csv = [headers.join(","), ...rows.map((r) => r.join(","))].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `stock-options-selling-report-${new Date().toISOString().split("T")[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);
  }

  function init() {
    const root = document.getElementById("soSellingReportRoot");
    if (!root) return;
    const today = new Date();
    $("endDate").valueAsDate = today;
    $("startDate").valueAsDate = firstDayOfCurrentMonth();
    loadReport();
  }

  window.soSellingLoadReport = loadReport;
  window.soSellingClearFilters = clearFilters;
  window.soSellingDownloadReport = downloadReport;
  window.soSellingToggleTradeDetails = toggleTradeDetails;
  document.addEventListener("DOMContentLoaded", init);
})();

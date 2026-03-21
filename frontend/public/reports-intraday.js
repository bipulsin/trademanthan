/**
 * Intraday scan trading report (embedded in reports.html).
 * Logic aligned with reportscan.html; IDs prefixed with intraday_.
 */
(function () {
    const TRADING_REPORT_API = "/scan/trading-report";
    const DAILY_TRADES_API = "/scan/daily-trades";

    let reportData = [];
    const expandedRows = {};

    function $(id) {
        return document.getElementById("intraday_" + id);
    }

    function loadReport() {
        const startDate = $("startDate").value;
        const endDate = $("endDate").value;
        const alertType = $("alertType").value;

        $("loading").style.display = "block";
        $("noData").style.display = "none";
        $("reportTable").style.display = "none";
        $("summaryCards").style.display = "none";

        (async () => {
            try {
                const params = new URLSearchParams();
                if (startDate) params.append("start_date", startDate);
                if (endDate) params.append("end_date", endDate);
                if (alertType) params.append("alert_type", alertType);

                const response = await fetch(`${TRADING_REPORT_API}?${params.toString()}`, {
                    cache: "no-store",
                });
                const result = await response.json();

                $("loading").style.display = "none";

                if (result.success && result.data && result.data.length > 0) {
                    reportData = result.data;
                    displayReport(result.data, result.summary);
                } else {
                    $("noData").style.display = "block";
                }
            } catch (error) {
                console.error("Error loading intraday report:", error);
                $("loading").style.display = "none";
                $("noData").style.display = "block";
            }
        })();
    }

    function displayReport(data, summary) {
        let totalWins = 0;
        let totalLosses = 0;
        data.forEach((day) => {
            totalWins += day.bullish_wins + day.bearish_wins;
            totalLosses += day.bullish_losses + day.bearish_losses;
        });
        const totalClosed = totalWins + totalLosses;
        const cumulativeWinRate = totalClosed > 0 ? (totalWins / totalClosed) * 100 : 0;

        const summaryHTML = `
                <div class="intraday-summary-card">
                    <h3>Days</h3>
                    <div class="intraday-value">${summary.total_days}</div>
                </div>
                <div class="intraday-summary-card">
                    <h3>Alerts</h3>
                    <div class="intraday-value">${summary.total_alerts}</div>
                </div>
                <div class="intraday-summary-card">
                    <h3>Trades</h3>
                    <div class="intraday-value">${summary.total_trades}</div>
                </div>
                <div class="intraday-summary-card intraday-mobile-hide">
                    <h3>Cumulative Win Rate</h3>
                    <div class="intraday-value">${cumulativeWinRate.toFixed(1)}%</div>
                </div>
                <div class="intraday-summary-card intraday-mobile-hide">
                    <h3>Overall P&L</h3>
                    <div class="intraday-value">₹${summary.overall_pnl.toLocaleString("en-IN")}</div>
                </div>
            `;
        $("summaryCards").innerHTML = summaryHTML;
        $("summaryCards").style.display = "grid";

        const tableHTML = `
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Market<br/>Trend</th>
                            <th>Total Alerts</th>
                            <th>Total Trades</th>
                            <th>Bullish<br/>W/L</th>
                            <th>Bearish<br/>W/L</th>
                            <th>Win Rate</th>
                            <th>Total P&L</th>
                            <th>Best Trade</th>
                            <th>Worst Trade</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data
                            .map(
                                (day, index) => `
                            <tr class="intraday-data-row" onclick="window.intradayToggleTradeDetails('${day.date}', ${index})">
                                <td class="intraday-date-cell">
                                    <i class="fas fa-chevron-right" id="intraday-expand-icon-${index}" style="font-size: 10px; margin-right: 8px; transition: transform 0.3s;"></i>
                                    <strong>${formatDate(day.date)}</strong>
                                </td>
                                <td style="text-align: center; font-size: 24px;">
                                    ${getMarketTrendIcon(day.market_trend)}
                                </td>
                                <td>${day.total_alerts}</td>
                                <td>${day.total_trades}</td>
                                <td>
                                    <span class="intraday-badge intraday-badge-success">${day.bullish_wins}</span> / 
                                    <span class="intraday-badge intraday-badge-danger">${day.bullish_losses}</span>
                                </td>
                                <td>
                                    <span class="intraday-badge intraday-badge-success">${day.bearish_wins}</span> / 
                                    <span class="intraday-badge intraday-badge-danger">${day.bearish_losses}</span>
                                </td>
                                <td><strong>${day.win_rate.toFixed(1)}%</strong></td>
                                <td class="${day.total_pnl >= 0 ? "intraday-positive" : "intraday-negative"}">
                                    <strong>₹${day.total_pnl.toLocaleString("en-IN")}</strong>
                                </td>
                                <td>
                                    ${
                                        day.total_closed === 0
                                            ? '<div class="intraday-muted-italic">Not losing is winning</div>'
                                            : day.best_trade.stock
                                              ? `<div><strong>${day.best_trade.stock}</strong></div>
                                             <div class="intraday-positive">+₹${day.best_trade.pnl.toLocaleString("en-IN")}</div>`
                                              : "-"
                                    }
                                </td>
                                <td>
                                    ${
                                        day.total_closed === 0
                                            ? ""
                                            : day.worst_trade.stock
                                              ? `<div><strong>${day.worst_trade.stock}</strong></div>
                                             <div class="intraday-negative">₹${day.worst_trade.pnl.toLocaleString("en-IN")}</div>`
                                              : "-"
                                    }
                                </td>
                            </tr>
                            <tr class="intraday-expandable-row" id="intraday-details-row-${index}">
                                <td colspan="10">
                                    <div class="intraday-trade-details" id="intraday-trade-details-${index}">
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

    function formatDate(dateStr) {
        const date = new Date(dateStr);
        const options = { year: "numeric", month: "short", day: "numeric", weekday: "short" };
        return date.toLocaleDateString("en-IN", options);
    }

    function getMarketTrendIcon(trend) {
        if (trend === "bullish") {
            return '<span class="intraday-trend-up" title="Bullish: Both NIFTY50 & BANKNIFTY trending up">↑</span>';
        } else if (trend === "bearish") {
            return '<span class="intraday-trend-down" title="Bearish: Both NIFTY50 & BANKNIFTY trending down">↓</span>';
        } else {
            return '<span class="intraday-trend-sideways" title="Sideways: Indexes in opposite directions or mixed">↔</span>';
        }
    }

    async function toggleTradeDetails(date, rowIndex) {
        const detailsRow = document.getElementById(`intraday-details-row-${rowIndex}`);
        const expandIcon = document.getElementById(`intraday-expand-icon-${rowIndex}`);

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
            const response = await fetch(`${DAILY_TRADES_API}/${date}`, { cache: "no-store" });
            const result = await response.json();

            if (result.success && result.trades && result.trades.length > 0) {
                const detailsHTML = `
                        <table class="intraday-trade-details-table">
                            <thead>
                                <tr>
                                    <th>Stock Name</th>
                                    <th>Option Contract</th>
                                    <th>Qty</th>
                                    <th>Buy Price</th>
                                    <th>Buy Time</th>
                                    <th>Sell Price</th>
                                    <th>Sell Time</th>
                                    <th>P&L</th>
                                    <th>Status</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${result.trades
                                    .map(
                                        (trade) => `
                                    <tr>
                                        <td><strong>${trade.stock_name}</strong></td>
                                        <td class="intraday-contract">${trade.option_contract || "-"}</td>
                                        <td>${trade.qty || 0}</td>
                                        <td>₹${trade.buy_price.toFixed(2)}</td>
                                        <td>${trade.buy_time || "-"}</td>
                                        <td>${trade.sell_price > 0 ? trade.sell_price.toFixed(2) : "-"}</td>
                                        <td>${trade.sell_time || "-"}</td>
                                        <td class="${trade.pnl >= 0 ? "intraday-positive" : "intraday-negative"}">
                                            <strong>₹${trade.pnl.toLocaleString("en-IN")}</strong>
                                        </td>
                                        <td>
                                            <span class="intraday-badge ${getStatusBadgeClass(trade.status)}">
                                                ${formatStatus(trade.status, trade.exit_reason, trade.no_entry_reason)}
                                            </span>
                                        </td>
                                    </tr>
                                `
                                    )
                                    .join("")}
                            </tbody>
                        </table>
                    `;
                document.getElementById(`intraday-trade-details-${rowIndex}`).innerHTML = detailsHTML;
            } else {
                document.getElementById(`intraday-trade-details-${rowIndex}`).innerHTML = `
                        <div class="intraday-details-empty">
                            <i class="fas fa-info-circle"></i> No executed trades for this day
                        </div>
                    `;
            }
        } catch (error) {
            console.error("Error loading trade details:", error);
            document.getElementById(`intraday-trade-details-${rowIndex}`).innerHTML = `
                    <div class="intraday-details-error">
                        <i class="fas fa-exclamation-triangle"></i> Error loading trade details
                    </div>
                `;
        }
    }

    function getStatusBadgeClass(status) {
        const s = status ? String(status) : "";
        if (s === "sold" || s.includes("exit")) return "intraday-badge-success";
        if (s === "hold") return "intraday-badge-warning";
        return "intraday-badge-secondary";
    }

    function formatStatus(status, exitReason, noEntryReason) {
        const s = status ? String(status) : "";
        if ((s === "sold" || s.includes("exit")) && exitReason) {
            const exitReasonMap = {
                time_based: "EXITED-TM",
                stock_vwap_cross: "EXITED-VW",
                profit_target: "EXITED-TG",
                stop_loss: "EXITED-SL",
                manual: "EXITED-MN",
                "Exit-VWAP Cross": "EXITED-VW",
                "Exit-Target": "EXITED-TG",
                "Exit-SL": "EXITED-SL",
            };
            return exitReasonMap[exitReason] || "EXITED";
        }

        if (s === "no_entry" && noEntryReason) {
            return `NO ENTRY - ${noEntryReason}`;
        }

        return s
            .replace(/_/g, " ")
            .split(" ")
            .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
            .join(" ");
    }

    function clearFilters() {
        const today = new Date();
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(today.getDate() - 30);

        $("endDate").valueAsDate = today;
        $("startDate").valueAsDate = thirtyDaysAgo;
        $("alertType").value = "";

        loadReport();
    }

    function downloadReport() {
        if (reportData.length === 0) {
            alert("No data to download");
            return;
        }

        const headers = [
            "Date",
            "Market Trend",
            "Total Alerts",
            "Total Trades",
            "Bullish Wins",
            "Bullish Losses",
            "Bearish Wins",
            "Bearish Losses",
            "Total Closed",
            "Win Rate %",
            "Total P&L",
            "Best Trade Stock",
            "Best Trade P&L",
            "Worst Trade Stock",
            "Worst Trade P&L",
        ];

        const rows = reportData.map((day) => [
            day.date,
            day.market_trend ? day.market_trend.charAt(0).toUpperCase() + day.market_trend.slice(1) : "Unknown",
            day.total_alerts,
            day.total_trades,
            day.bullish_wins,
            day.bullish_losses,
            day.bearish_wins,
            day.bearish_losses,
            day.total_closed,
            day.win_rate.toFixed(2),
            day.total_pnl.toFixed(2),
            day.total_closed === 0 ? "Not losing is winning" : day.best_trade.stock || "",
            day.total_closed === 0 ? "" : day.best_trade.pnl || 0,
            day.total_closed === 0 ? "" : day.worst_trade.stock || "",
            day.total_closed === 0 ? "" : day.worst_trade.pnl || 0,
        ]);

        const csvContent = [headers.join(","), ...rows.map((row) => row.join(","))].join("\n");

        const blob = new Blob([csvContent], { type: "text/csv" });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `intraday-trading-report-${new Date().toISOString().split("T")[0]}.csv`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
    }

    function init() {
        const root = document.getElementById("intradayReportRoot");
        if (!root) return;

        const today = new Date();
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(today.getDate() - 30);

        $("endDate").valueAsDate = today;
        $("startDate").valueAsDate = thirtyDaysAgo;

        loadReport();
    }

    window.intradayLoadReport = loadReport;
    window.intradayClearFilters = clearFilters;
    window.intradayDownloadReport = downloadReport;
    window.intradayToggleTradeDetails = toggleTradeDetails;

    document.addEventListener("DOMContentLoaded", init);
})();

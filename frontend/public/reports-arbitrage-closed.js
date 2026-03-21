/**
 * Closed arbitrage orders only — embedded in reports.html (My Arbitrage Report).
 */
(function () {
    const ORDER_REPORT_API = "/scan/arbitrage/orders";

    function fmt(v) {
        if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
        return Number(v).toFixed(2);
    }

    function fmtDateTime(v) {
        if (!v) return "-";
        const d = new Date(v);
        if (Number.isNaN(d.getTime())) return String(v);
        return d.toLocaleString("en-IN");
    }

    function renderClosedOrders(rows) {
        const closedBodyEl = document.getElementById("reportsClosedArbitrageBody");
        const closedCardsEl = document.getElementById("reportsClosedArbitrageCards");
        if (!closedBodyEl || !closedCardsEl) return;

        if (!rows || rows.length === 0) {
            closedBodyEl.innerHTML =
                '<tr><td colspan="9" class="state-cell">No closed arbitrage positions</td></tr>';
            closedCardsEl.innerHTML = '<div class="arbitrage-card-state">No closed arbitrage positions</div>';
            return;
        }

        closedBodyEl.innerHTML = rows
            .map(
                (r) => `
            <tr>
                <td>${fmtDateTime(r.trade_entry_time)}</td>
                <td>${fmtDateTime(r.trade_exit_time)}</td>
                <td>${r.stock || "-"}</td>
                <td class="num">${fmt(r.buy_cost)}</td>
                <td class="num">${fmt(r.sell_cost)}</td>
                <td class="num">${r.quantity ?? "-"}</td>
                <td class="num">${fmt(r.trade_entry_value)}</td>
                <td class="num">${fmt(r.trade_exit_value)}</td>
                <td>${r.trade_status || "-"}</td>
            </tr>
        `
            )
            .join("");

        closedCardsEl.innerHTML = rows
            .map(
                (r) => `
            <article class="arbitrage-card">
                <div class="arbitrage-card-head">
                    <div>
                        <p class="arbitrage-card-title">${r.stock || "-"}</p>
                        <p class="arbitrage-card-subtitle">Entry ${fmtDateTime(r.trade_entry_time)}</p>
                    </div>
                </div>
                <p class="arbitrage-card-line"><span class="label">Exit Time</span><span>${fmtDateTime(r.trade_exit_time)}</span></p>
                <p class="arbitrage-card-line"><span class="label">Buy/Sell</span><span>${fmt(r.buy_cost)} / ${fmt(r.sell_cost)}</span></p>
                <p class="arbitrage-card-line"><span class="label">Quantity</span><span>${r.quantity ?? "-"}</span></p>
                <p class="arbitrage-card-line"><span class="label">Entry Value</span><span>${fmt(r.trade_entry_value)}</span></p>
                <p class="arbitrage-card-line"><span class="label">Exit Value</span><span>${fmt(r.trade_exit_value)}</span></p>
                <p class="arbitrage-card-line"><span class="label">Status</span><span>${r.trade_status || "-"}</span></p>
            </article>
        `
            )
            .join("");
    }

    async function loadClosedOrders() {
        const closedBodyEl = document.getElementById("reportsClosedArbitrageBody");
        const closedSummaryEl = document.getElementById("reportsClosedSummaryBar");
        if (!closedBodyEl || !closedSummaryEl) return;

        closedBodyEl.innerHTML = '<tr><td colspan="9" class="state-cell">Loading...</td></tr>';
        closedSummaryEl.textContent = "Loading data...";

        try {
            const res = await fetch(`${ORDER_REPORT_API}?trade_status=CLOSED`, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || "Failed to fetch closed orders");
            }
            renderClosedOrders(data.rows);
            closedSummaryEl.textContent = `Total CLOSED records: ${data.count}`;
        } catch (err) {
            console.error("Arbitrage closed report:", err);
            closedBodyEl.innerHTML = `<tr><td colspan="9" class="state-cell">${err.message || "Error loading"}</td></tr>`;
            closedSummaryEl.textContent = "Failed to load";
        }
    }

    document.addEventListener("DOMContentLoaded", () => {
        if (document.getElementById("reportsClosedArbitrageBody")) {
            loadClosedOrders();
        }
    });

    window.refreshReportsArbitrageClosed = loadClosedOrders;
})();

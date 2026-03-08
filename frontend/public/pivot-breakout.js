(function () {
    const bullishBody = document.getElementById("bullishBody");
    const bearishBody = document.getElementById("bearishBody");
    const bullishSummary = document.getElementById("bullishSummary");
    const bearishSummary = document.getElementById("bearishSummary");
    const refreshBtn = document.getElementById("refreshBtn");
    const API = "/scan/arbitrage/pivot-breakout";

    function fmt(v) {
        if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
        return Number(v).toFixed(2);
    }

    function renderRows(tbody, rows, pivotKey) {
        if (!rows || rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="state-cell">No records found.</td></tr>';
            return;
        }
        tbody.innerHTML = rows.map((r) => `
            <tr>
                <td>${r.stock || "-"}</td>
                <td>${r.currmth_future_symbol || "-"}</td>
                <td class="num">${fmt(r.currmth_future_ltp)}</td>
                <td class="num">${fmt(r[pivotKey])}</td>
            </tr>
        `).join("");
    }

    async function loadData() {
        bullishBody.innerHTML = '<tr><td colspan="4" class="state-cell">Loading...</td></tr>';
        bearishBody.innerHTML = '<tr><td colspan="4" class="state-cell">Loading...</td></tr>';
        bullishSummary.textContent = "Loading data...";
        bearishSummary.textContent = "Loading data...";

        try {
            const res = await fetch(API, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || "Failed to fetch pivot breakout data");
            }

            renderRows(bullishBody, data.bullish || [], "r3_pivot");
            renderRows(bearishBody, data.bearish || [], "s3_pivot");
            bullishSummary.textContent = `Total bullish records: ${data.bullish_count || 0}`;
            bearishSummary.textContent = `Total bearish records: ${data.bearish_count || 0}`;
        } catch (err) {
            bullishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bearishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bullishSummary.textContent = "Failed to load data.";
            bearishSummary.textContent = "Failed to load data.";
        }
    }

    refreshBtn.addEventListener("click", loadData);
    document.addEventListener("DOMContentLoaded", loadData);
})();

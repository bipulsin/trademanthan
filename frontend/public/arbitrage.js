(function () {
    const bodyEl = document.getElementById("arbitrageBody");
    const summaryEl = document.getElementById("summaryBar");
    const refreshBtn = document.getElementById("refreshBtn");

    function fmt(v) {
        if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
        return Number(v).toFixed(2);
    }

    function renderRows(rows) {
        if (!rows || rows.length === 0) {
            bodyEl.innerHTML = '<tr><td colspan="6" class="state-cell">No matching arbitrage records found.</td></tr>';
            return;
        }

        bodyEl.innerHTML = rows.map((r) => {
            return `
                <tr>
                    <td>${r.stock || "-"}</td>
                    <td class="num">${fmt(r.stock_ltp)}</td>
                    <td>${r.currmth_future_symbol || "-"}</td>
                    <td class="num">${fmt(r.currmth_future_ltp)}</td>
                    <td>${r.nextmth_future_symbol || "-"}</td>
                    <td class="num">${fmt(r.nextmth_future_ltp)}</td>
                </tr>
            `;
        }).join("");
    }

    async function loadData() {
        bodyEl.innerHTML = '<tr><td colspan="6" class="state-cell">Loading...</td></tr>';
        summaryEl.textContent = "Loading data...";
        try {
            const res = await fetch("/scan/arbitrage/selection", { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || "Failed to fetch data");
            }
            renderRows(data.rows);
            summaryEl.textContent = `Total matching records: ${data.count}`;
        } catch (err) {
            bodyEl.innerHTML = `<tr><td colspan="6" class="state-cell">Error loading data: ${err.message}</td></tr>`;
            summaryEl.textContent = "Failed to load data.";
        }
    }

    refreshBtn.addEventListener("click", loadData);
    document.addEventListener("DOMContentLoaded", loadData);
})();

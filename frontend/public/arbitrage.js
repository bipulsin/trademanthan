(function () {
    const bodyEl = document.getElementById("arbitrageBody");
    const summaryEl = document.getElementById("summaryBar");
    const refreshBtn = document.getElementById("refreshBtn");
    const ORDER_API = "/scan/arbitrage/order";
    const SELECTION_API = "/scan/arbitrage/selection";
    let placingOrder = false;

    function fmt(v) {
        if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
        return Number(v).toFixed(2);
    }

    function renderRows(rows) {
        if (!rows || rows.length === 0) {
            bodyEl.innerHTML = '<tr><td colspan="7" class="state-cell">No matching arbitrage records found.</td></tr>';
            return;
        }

        bodyEl.innerHTML = rows.map((r) => {
            const hasOpenOrder = Boolean(r.has_open_order);
            const disabledAttr = hasOpenOrder ? "disabled" : "";
            const buttonText = hasOpenOrder ? "Ordered" : "Order";
            return `
                <tr>
                    <td>${r.stock || "-"}</td>
                    <td class="num">${fmt(r.stock_ltp)}</td>
                    <td>${r.currmth_future_symbol || "-"}</td>
                    <td class="num">${fmt(r.currmth_future_ltp)}</td>
                    <td>${r.nextmth_future_symbol || "-"}</td>
                    <td class="num">${fmt(r.nextmth_future_ltp)}</td>
                    <td class="order-cell">
                        <button
                            class="btn-order"
                            data-stock-key="${r.stock_instrument_key || ""}"
                            ${disabledAttr}
                        >${buttonText}</button>
                    </td>
                </tr>
            `;
        }).join("");

        bodyEl.querySelectorAll(".btn-order").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const stockKey = btn.getAttribute("data-stock-key");
                if (!stockKey || btn.disabled || placingOrder) return;
                await placeOrder(stockKey, btn);
            });
        });
    }

    async function placeOrder(stockInstrumentKey, btn) {
        placingOrder = true;
        const originalText = btn.textContent;
        btn.disabled = true;
        btn.textContent = "Ordering...";
        try {
            const res = await fetch(ORDER_API, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ stock_instrument_key: stockInstrumentKey }),
            });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || data.message || "Failed to place order");
            }
            summaryEl.textContent = `${data.message}. Reloading...`;
            await loadData();
        } catch (err) {
            summaryEl.textContent = `Order failed: ${err.message}`;
            btn.disabled = false;
            btn.textContent = originalText;
        } finally {
            placingOrder = false;
        }
    }

    async function loadData() {
        bodyEl.innerHTML = '<tr><td colspan="7" class="state-cell">Loading...</td></tr>';
        summaryEl.textContent = "Loading data...";
        try {
            const res = await fetch(SELECTION_API, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || "Failed to fetch data");
            }
            renderRows(data.rows);
            summaryEl.textContent = `Total matching records: ${data.count}`;
        } catch (err) {
            bodyEl.innerHTML = `<tr><td colspan="7" class="state-cell">Error loading data: ${err.message}</td></tr>`;
            summaryEl.textContent = "Failed to load data.";
        }
    }

    refreshBtn.addEventListener("click", loadData);
    document.addEventListener("DOMContentLoaded", loadData);
})();

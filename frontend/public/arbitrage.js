(function () {
    const bodyEl = document.getElementById("arbitrageBody");
    const summaryEl = document.getElementById("summaryBar");
    const cardsEl = document.getElementById("arbitrageCards");
    const openBodyEl = document.getElementById("openArbitrageBody");
    const openSummaryEl = document.getElementById("openSummaryBar");
    const openCardsEl = document.getElementById("openArbitrageCards");
    const closedBodyEl = document.getElementById("closedArbitrageBody");
    const closedSummaryEl = document.getElementById("closedSummaryBar");
    const closedCardsEl = document.getElementById("closedArbitrageCards");
    const refreshBtn = document.getElementById("refreshBtn");
    const ORDER_API = "/scan/arbitrage/order";
    const EXIT_API = "/scan/arbitrage/order/exit";
    const SELECTION_API = "/scan/arbitrage/selection";
    const ORDER_REPORT_API = "/scan/arbitrage/orders";
    let placingOrder = false;
    let exitingOrder = false;

    function fmt(v) {
        if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
        return Number(v).toFixed(2);
    }

    function ellipsize(value, maxLen = 28) {
        const text = (value || "-").toString().trim();
        if (text.length <= maxLen) return text;
        return `${text.slice(0, maxLen - 1)}…`;
    }

    function renderRows(rows) {
        if (!rows || rows.length === 0) {
            bodyEl.innerHTML = '<tr><td colspan="7" class="state-cell">No matching arbitrage records found.</td></tr>';
            cardsEl.innerHTML = '<div class="arbitrage-card-state">No matching arbitrage records found.</div>';
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

        cardsEl.innerHTML = rows.map((r) => {
            const hasOpenOrder = Boolean(r.has_open_order);
            const disabledAttr = hasOpenOrder ? "disabled" : "";
            const buttonText = hasOpenOrder ? "Ordered" : "Order";
            return `
                <article class="arbitrage-card">
                    <div class="arbitrage-card-head">
                        <div>
                            <p class="arbitrage-card-title">${r.stock || "-"} [${fmt(r.stock_ltp)}]</p>
                        </div>
                        <button
                            class="btn-order"
                            data-order-btn="1"
                            data-stock-key="${r.stock_instrument_key || ""}"
                            ${disabledAttr}
                        >${buttonText}</button>
                    </div>
                    <p class="arbitrage-card-compact-line">${ellipsize(r.currmth_future_symbol)} [${fmt(r.currmth_future_ltp)}]</p>
                    <p class="arbitrage-card-compact-line">${ellipsize(r.nextmth_future_symbol)} [${fmt(r.nextmth_future_ltp)}]</p>
                </article>
            `;
        }).join("");

        document.querySelectorAll("[data-order-btn='1']").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const stockKey = btn.getAttribute("data-stock-key");
                if (!stockKey || btn.disabled || placingOrder) return;
                await placeOrder(stockKey, btn);
            });
        });
    }

    function fmtDateTime(v) {
        if (!v) return "-";
        const d = new Date(v);
        if (Number.isNaN(d.getTime())) return String(v);
        return d.toLocaleString("en-IN");
    }

    function renderOpenOrders(rows) {
        if (!rows || rows.length === 0) {
            openBodyEl.innerHTML = '<tr><td colspan="8" class="state-cell">No Open positin</td></tr>';
            openCardsEl.innerHTML = '<div class="arbitrage-card-state">No Open positin</div>';
            return;
        }
        openBodyEl.innerHTML = rows.map((r) => `
            <tr>
                <td>${fmtDateTime(r.trade_entry_time)}</td>
                <td>${r.stock || "-"}</td>
                <td class="num">${fmt(r.buy_cost)}</td>
                <td class="num">${fmt(r.sell_cost)}</td>
                <td class="num">${r.quantity ?? "-"}</td>
                <td class="num">${fmt(r.trade_entry_value)}</td>
                <td>${r.trade_status || "-"}</td>
                <td class="order-cell">
                    <button class="btn-exit" data-order-id="${r.id}">Exit</button>
                </td>
            </tr>
        `).join("");

        openCardsEl.innerHTML = rows.map((r) => `
            <article class="arbitrage-card">
                <div class="arbitrage-card-head">
                    <div>
                        <p class="arbitrage-card-title">${r.stock || "-"}</p>
                        <p class="arbitrage-card-subtitle">${fmtDateTime(r.trade_entry_time)}</p>
                    </div>
                    <button class="btn-exit" data-exit-btn="1" data-order-id="${r.id}">Exit</button>
                </div>
                <p class="arbitrage-card-line"><span class="label">Buy Cost</span><span>${fmt(r.buy_cost)}</span></p>
                <p class="arbitrage-card-line"><span class="label">Sell Cost</span><span>${fmt(r.sell_cost)}</span></p>
                <p class="arbitrage-card-line"><span class="label">Quantity</span><span>${r.quantity ?? "-"}</span></p>
                <p class="arbitrage-card-line"><span class="label">Entry Value</span><span>${fmt(r.trade_entry_value)}</span></p>
                <p class="arbitrage-card-line"><span class="label">Status</span><span>${r.trade_status || "-"}</span></p>
            </article>
        `).join("");

        document.querySelectorAll("[data-exit-btn='1']").forEach((btn) => {
            btn.addEventListener("click", async () => {
                const orderId = Number(btn.getAttribute("data-order-id"));
                if (!orderId || btn.disabled || exitingOrder) return;
                await exitOrder(orderId, btn);
            });
        });
    }

    function renderClosedOrders(rows) {
        if (!rows || rows.length === 0) {
            closedBodyEl.innerHTML = '<tr><td colspan="9" class="state-cell">No Open positin</td></tr>';
            closedCardsEl.innerHTML = '<div class="arbitrage-card-state">No Open positin</div>';
            return;
        }
        closedBodyEl.innerHTML = rows.map((r) => `
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
        `).join("");

        closedCardsEl.innerHTML = rows.map((r) => `
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
        `).join("");
    }

    async function loadOrdersByStatus(tradeStatus) {
        const targetBody = tradeStatus === "OPEN" ? openBodyEl : closedBodyEl;
        const targetSummary = tradeStatus === "OPEN" ? openSummaryEl : closedSummaryEl;
        const emptyColspan = tradeStatus === "OPEN" ? 7 : 9;
        targetBody.innerHTML = `<tr><td colspan="${emptyColspan}" class="state-cell">Loading...</td></tr>`;
        targetSummary.textContent = "Loading data...";
        const res = await fetch(`${ORDER_REPORT_API}?trade_status=${encodeURIComponent(tradeStatus)}`, { cache: "no-store" });
        const data = await res.json();
        if (!res.ok || !data.success) {
            throw new Error(data.detail || `Failed to fetch ${tradeStatus} orders`);
        }
        if (tradeStatus === "OPEN") {
            renderOpenOrders(data.rows);
            targetSummary.textContent = `Total OPEN records: ${data.count}`;
        } else {
            renderClosedOrders(data.rows);
            targetSummary.textContent = `Total CLOSED records: ${data.count}`;
        }
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

    async function exitOrder(orderId, btn) {
        exitingOrder = true;
        const originalText = btn.textContent;
        btn.disabled = true;
        btn.textContent = "Exiting...";
        try {
            const res = await fetch(EXIT_API, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ order_id: orderId }),
            });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || data.message || "Failed to exit order");
            }
            openSummaryEl.textContent = `${data.message}. Reloading...`;
            await loadData();
        } catch (err) {
            openSummaryEl.textContent = `Exit failed: ${err.message}`;
            btn.disabled = false;
            btn.textContent = originalText;
        } finally {
            exitingOrder = false;
        }
    }

    async function loadData() {
        bodyEl.innerHTML = '<tr><td colspan="7" class="state-cell">Loading...</td></tr>';
        cardsEl.innerHTML = '<div class="arbitrage-card-state">Loading...</div>';
        summaryEl.textContent = "Loading data...";
        openBodyEl.innerHTML = '<tr><td colspan="8" class="state-cell">Loading...</td></tr>';
        openCardsEl.innerHTML = '<div class="arbitrage-card-state">Loading...</div>';
        openSummaryEl.textContent = "Loading data...";
        closedBodyEl.innerHTML = '<tr><td colspan="9" class="state-cell">Loading...</td></tr>';
        closedCardsEl.innerHTML = '<div class="arbitrage-card-state">Loading...</div>';
        closedSummaryEl.textContent = "Loading data...";
        try {
            const res = await fetch(SELECTION_API, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || "Failed to fetch data");
            }
            renderRows(data.rows);
            summaryEl.textContent = `Total matching records: ${data.count}`;
            await loadOrdersByStatus("OPEN");
            await loadOrdersByStatus("CLOSED");
        } catch (err) {
            bodyEl.innerHTML = `<tr><td colspan="7" class="state-cell">Error loading data: ${err.message}</td></tr>`;
            cardsEl.innerHTML = `<div class="arbitrage-card-state">Error loading data: ${err.message}</div>`;
            summaryEl.textContent = "Failed to load data.";
            openBodyEl.innerHTML = `<tr><td colspan="8" class="state-cell">Error loading data: ${err.message}</td></tr>`;
            openCardsEl.innerHTML = `<div class="arbitrage-card-state">Error loading data: ${err.message}</div>`;
            openSummaryEl.textContent = "Failed to load data.";
            closedBodyEl.innerHTML = `<tr><td colspan="9" class="state-cell">Error loading data: ${err.message}</td></tr>`;
            closedCardsEl.innerHTML = `<div class="arbitrage-card-state">Error loading data: ${err.message}</div>`;
            closedSummaryEl.textContent = "Failed to load data.";
        }
    }

    refreshBtn.addEventListener("click", loadData);
    document.addEventListener("DOMContentLoaded", loadData);
})();

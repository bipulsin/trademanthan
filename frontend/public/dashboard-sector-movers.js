/**
 * Dashboard: Top 3 Nifty sector gainers & losers by intraday % vs open.
 */
(function () {
    const API = "/scan/dashboard-sector-movers";
    const POLL_MS = 5 * 60 * 1000;
    let timer = null;

    function fmtPct(n) {
        if (n == null || Number.isNaN(Number(n))) return "—";
        const x = Number(n);
        const sign = x > 0 ? "+" : "";
        return sign + x.toFixed(2) + "%";
    }

    function renderList(ul, items, positiveColumn) {
        if (!ul) return;
        if (!items || items.length === 0) {
            ul.innerHTML = '<li class="sector-movers-empty">No data</li>';
            return;
        }
        ul.innerHTML = items
            .map((row) => {
                const cls = positiveColumn ? "sector-movers-pct--gain" : "sector-movers-pct--lose";
                const pct = row.pct_change;
                return `<li><span class="sector-movers-name">${escapeHtml(row.sector || "")}</span><span class="sector-movers-pct ${cls}">${fmtPct(pct)}</span></li>`;
            })
            .join("");
    }

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
    }

    function render(data) {
        const g = document.getElementById("sectorMoversGainers");
        const l = document.getElementById("sectorMoversLosers");
        const t = document.getElementById("sectorMoversUpdated");
        if (data && data.updated_at && t) {
            try {
                const d = new Date(data.updated_at);
                t.textContent =
                    "Updated: " +
                    d.toLocaleString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
            } catch (_) {
                t.textContent = "";
            }
        }
        renderList(g, data && data.gainers, true);
        renderList(l, data && data.losers, false);
    }

    async function fetchMovers() {
        const grid = document.getElementById("sectorMoversGrid");
        if (!grid) return;
        try {
            const res = await fetch(API, { cache: "no-store", credentials: "same-origin" });
            const data = await res.json();
            if (data.success) {
                render(data);
            } else {
                render({ gainers: [], losers: [] });
            }
        } catch (e) {
            console.warn("dashboard-sector-movers:", e);
            render({ gainers: [], losers: [] });
        }
    }

    function startPolling() {
        if (timer) clearInterval(timer);
        timer = setInterval(fetchMovers, POLL_MS);
    }

    function init() {
        if (!document.getElementById("sectorMoversGrid")) return;
        fetchMovers();
        startPolling();
        const btn = document.getElementById("sectorMoversRefresh");
        if (btn) {
            btn.addEventListener("click", () => {
                btn.disabled = true;
                fetchMovers().finally(() => {
                    btn.disabled = false;
                });
            });
        }
        document.addEventListener("visibilitychange", () => {
            if (document.visibilityState === "visible") fetchMovers();
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();

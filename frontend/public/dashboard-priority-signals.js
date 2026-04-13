/**
 * Dashboard: Smart Futures rows that are pre-market Top N and/or top OI movers (session ranks on row).
 */
(function () {
    const API_BASE =
        window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
            ? "http://localhost:8000"
            : window.location.origin;

    function authHeaders() {
        const t = localStorage.getItem("trademanthan_token") || "";
        return {
            Authorization: "Bearer " + t,
            "Content-Type": "application/json",
        };
    }

    async function fetchDailyJson() {
        const paths = ["/api/smart-futures/daily", "/smart-futures/daily"];
        let lastErr = null;
        for (const p of paths) {
            try {
                const res = await fetch(API_BASE + p, { headers: authHeaders(), cache: "no-store" });
                const raw = await res.text();
                const ct = (res.headers.get("content-type") || "").toLowerCase();
                const looksJson =
                    ct.includes("application/json") || /^\s*[\[{]/.test(raw.slice(0, 20));
                if (res.ok && looksJson) {
                    return JSON.parse(raw);
                }
                lastErr = new Error(raw.slice(0, 200) || res.statusText);
            } catch (e) {
                lastErr = e;
            }
        }
        throw lastErr || new Error("Failed to load Smart Futures daily");
    }

    function flattenRows(data) {
        if (data.rows && data.rows.length) return data.rows;
        const out = [];
        (data.groups || []).forEach(function (g) {
            (g.rows || []).forEach(function (r) {
                out.push(r);
            });
        });
        return out;
    }

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s == null ? "" : String(s);
        return d.innerHTML;
    }

    function render(rows) {
        const pri = rows.filter(function (r) {
            const pr = r.premkt_rank;
            const oh = r.oi_heat_rank;
            const inP = pr != null && pr !== "" && Number(pr) >= 1 && Number(pr) <= 50;
            const inO = oh != null && oh !== "" && Number(oh) >= 1 && Number(oh) <= 50;
            return inP || inO;
        });
        if (pri.length === 0) {
            return '<p class="priority-signals-empty">No priority-tagged signals for this session yet (ranks appear when a pick is in pre-market Top N or OI heatmap Top movers).</p>';
        }
        const head =
            "<thead><tr><th>Symbol</th><th>Side</th><th>Final CMS</th><th>Premkt #</th><th>OI heat #</th><th>Tier</th></tr></thead>";
        const body = pri
            .map(function (r) {
                const sym = (r.fut_symbol || "").replace(/FUT.*/i, "").trim() || r.fut_symbol || "—";
                return (
                    "<tr><td><strong>" +
                    escapeHtml(String(sym)) +
                    "</strong></td><td>" +
                    escapeHtml(String(r.side || "—")) +
                    "</td><td>" +
                    escapeHtml(r.final_cms != null ? Number(r.final_cms).toFixed(3) : "—") +
                    "</td><td>" +
                    escapeHtml(r.premkt_rank != null ? String(r.premkt_rank) : "—") +
                    "</td><td>" +
                    escapeHtml(r.oi_heat_rank != null ? String(r.oi_heat_rank) : "—") +
                    "</td><td>" +
                    escapeHtml(String(r.signal_tier || "—")) +
                    "</td></tr>"
                );
            })
            .join("");
        return (
            '<div class="oi-heatmap-table-wrap"><table class="oi-heatmap-table">' +
            head +
            "<tbody>" +
            body +
            "</tbody></table></div>"
        );
    }

    async function load() {
        const host = document.getElementById("prioritySignalsHost");
        const msg = document.getElementById("prioritySignalsMsg");
        if (!host) return;
        if (msg) msg.textContent = "Loading…";
        try {
            const data = await fetchDailyJson();
            const rows = flattenRows(data);
            host.innerHTML = render(rows);
            if (msg) {
                msg.textContent =
                    "Session " + (data.session_date || "—") + " · " + rows.length + " total rows · priority filter: premkt rank or OI heat rank set";
            }
        } catch (e) {
            host.innerHTML =
                '<p class="oi-heatmap-error">' + escapeHtml(e.message || String(e)) + "</p>";
            if (msg) msg.textContent = "Sign in to load Smart Futures, or open the Smart Futures page.";
        }
    }

    document.addEventListener("DOMContentLoaded", function () {
        const btn = document.getElementById("prioritySignalsRefresh");
        if (btn) btn.addEventListener("click", load);
        load();
    });
})();

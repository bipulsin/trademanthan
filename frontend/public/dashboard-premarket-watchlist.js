/**
 * Dashboard: Pre-market F&O Top 10 watchlist (OBV slope + gap strength + 20d range position).
 */
(function () {
    const API = "/scan/premarket-watchlist";
    const POLL_MS = 5 * 60 * 1000;
    let timer = null;

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
    }

    function fmtNum(n, d) {
        if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
        return Number(n).toFixed(d);
    }

    function fmtTime(iso) {
        if (!iso) return "—";
        try {
            const x = new Date(iso);
            if (Number.isNaN(x.getTime())) return escapeHtml(String(iso));
            return x.toLocaleString("en-IN", {
                timeZone: "Asia/Kolkata",
                dateStyle: "short",
                timeStyle: "short",
            });
        } catch (e) {
            return "—";
        }
    }

    function renderTable(rows) {
        if (!rows || rows.length === 0) {
            return '<p class="premarket-watchlist-empty">No watchlist for this session yet. It is built on weekday mornings after the scheduled scan.</p>';
        }
        const head =
            "<thead><tr>" +
            "<th>#</th><th>Symbol</th><th>OBV slope</th><th>Gap %</th><th>Range pos</th>" +
            "<th>Score</th><th>LTP</th>" +
            "</tr></thead>";
        const body = rows
            .map(function (r) {
                const g = r.gap_pct_signed;
                const gapCls =
                    g != null && Number(g) > 0
                        ? "premarket-gap--up"
                        : g != null && Number(g) < 0
                          ? "premarket-gap--down"
                          : "";
                return (
                    "<tr>" +
                    "<td>" +
                    escapeHtml(String(r.rank != null ? r.rank : "")) +
                    "</td>" +
                    "<td><strong>" +
                    escapeHtml(String(r.stock || "")) +
                    "</strong></td>" +
                    "<td>" +
                    fmtNum(r.obv_slope, 3) +
                    "</td>" +
                    '<td class="' +
                    gapCls +
                    '">' +
                    fmtNum(r.gap_pct_signed, 2) +
                    "</td>" +
                    "<td>" +
                    fmtNum(r.range_position, 3) +
                    "</td>" +
                    "<td>" +
                    fmtNum(r.composite_score, 3) +
                    "</td>" +
                    "<td>" +
                    fmtNum(r.ltp, 2) +
                    "</td>" +
                    "</tr>"
                );
            })
            .join("");
        return (
            '<div class="premarket-watchlist-table-wrap"><table class="premarket-watchlist-table">' +
            head +
            "<tbody>" +
            body +
            "</tbody></table></div>"
        );
    }

    async function load() {
        const host = document.getElementById("premarketWatchlistHost");
        const msg = document.getElementById("premarketWatchlistMsg");
        const updated = document.getElementById("premarketWatchlistUpdated");
        if (!host) return;

        if (msg) {
            msg.textContent = "Loading…";
            msg.style.display = "block";
        }

        try {
            const res = await fetch(API, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error((data && data.message) || res.statusText || "Failed");
            }
            const rows = data.rows || [];
            const sessionDate = data.session_date || "—";
            const inner = renderTable(rows);
            host.innerHTML = inner;
            if (msg) {
                msg.textContent =
                    rows.length > 0
                        ? "Session " + sessionDate + " · " + rows.length + " names"
                        : "Session " + sessionDate + " — no rows yet.";
                msg.style.display = "block";
            }
            if (updated && rows.length && rows[0].computed_at) {
                updated.textContent = "Updated " + fmtTime(rows[0].computed_at);
            } else if (updated) {
                updated.textContent = rows.length ? "—" : "Awaiting scan";
            }
        } catch (e) {
            host.innerHTML =
                '<p class="premarket-watchlist-error">' +
                escapeHtml(e.message || String(e)) +
                "</p>";
            if (msg) {
                msg.textContent = "";
                msg.style.display = "none";
            }
            if (updated) updated.textContent = "—";
        }
    }

    function startPoll() {
        if (timer) clearInterval(timer);
        timer = setInterval(load, POLL_MS);
    }

    document.addEventListener("DOMContentLoaded", function () {
        const btn = document.getElementById("premarketWatchlistRefresh");
        if (btn) btn.addEventListener("click", function () {
            load();
        });
        load();
        startPoll();
    });
})();

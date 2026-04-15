/**
 * Dashboard: Pre-market F&O Top N (OBV + gap + 52w range + momentum; matches premarket_scoring / test harness).
 * If today's session has no rows yet (scan not run), shows the most recent prior trading session —
 * except on a trading day after 09:00 IST, when we never show a prior calendar day's list.
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

    /** YYYY-MM-DD for "now" in Asia/Kolkata */
    function istTodayYmd() {
        return new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Kolkata" });
    }

    /** Previous calendar day in UTC civil math, then skip Sat/Sun */
    function previousTradingDayYmd(ymd) {
        const p = ymd.split("-").map(function (x) {
            return parseInt(x, 10);
        });
        let y = p[0];
        let m = p[1];
        let d = p[2];
        for (let i = 0; i < 14; i++) {
            const dt = new Date(Date.UTC(y, m - 1, d - 1));
            y = dt.getUTCFullYear();
            m = dt.getUTCMonth() + 1;
            d = dt.getUTCDate();
            const wd = new Date(Date.UTC(y, m - 1, d)).getUTCDay();
            if (wd !== 0 && wd !== 6) {
                return y + "-" + String(m).padStart(2, "0") + "-" + String(d).padStart(2, "0");
            }
        }
        return ymd;
    }

    function fmtNum(n, d) {
        if (n == null || n === "" || Number.isNaN(Number(n))) return "—";
        return Number(n).toFixed(d);
    }

    /** YYYY-MM-DD → dd-MMM-yyyy (e.g. 13-Apr-2026) */
    function formatSessionDateLabel(ymd) {
        if (!ymd || ymd === "—") return "—";
        const p = String(ymd).trim().split("-");
        if (p.length !== 3) return ymd;
        const y = parseInt(p[0], 10);
        const m = parseInt(p[1], 10);
        const d = parseInt(p[2], 10);
        if (!y || !m || !d || m < 1 || m > 12) return ymd;
        const months = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ];
        return String(d).padStart(2, "0") + "-" + months[m - 1] + "-" + y;
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
            return '<p class="premarket-watchlist-empty">No watchlist data found for recent sessions.</p>';
        }
        const head =
            "<thead><tr>" +
            "<th>#</th><th>Symbol</th><th>OBV slope</th><th>Gap %</th><th>Range pos</th>" +
            "<th>Momentum</th><th>Score</th><th>LTP</th>" +
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
                    fmtNum(r.momentum, 3) +
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

    async function fetchSession(ymd) {
        const url = API + "?session_date=" + encodeURIComponent(ymd);
        const res = await fetch(url, { cache: "no-store" });
        const data = await res.json();
        return { res, data };
    }

    /**
     * Prefer today's IST session. If the API sets show_today_session_only (after 09:00 IST on a trading day),
     * do not walk back to prior sessions. Otherwise keep the legacy fallback chain.
     */
    async function loadWithFallback() {
        const todayYmd = istTodayYmd();
        const { res, data } = await fetchSession(todayYmd);
        if (!res.ok || !data.success) {
            return {
                error: (data && data.message) || res.statusText || "Failed",
                rows: [],
                session_date: todayYmd,
                isFallback: false,
                asOf: todayYmd,
                todayOnly: false,
            };
        }
        const showTodayOnly = data.show_today_session_only === true;
        const rows0 = data.rows || [];
        if (showTodayOnly) {
            return {
                error: null,
                rows: rows0,
                session_date: data.session_date || todayYmd,
                isFallback: false,
                asOf: todayYmd,
                todayOnly: true,
            };
        }
        if (rows0.length > 0) {
            return {
                error: null,
                rows: rows0,
                session_date: data.session_date || todayYmd,
                isFallback: false,
                asOf: todayYmd,
                todayOnly: false,
            };
        }

        let cur = previousTradingDayYmd(todayYmd);
        let isFallback = true;
        let steps = 0;
        const maxSteps = 12;

        while (steps < maxSteps) {
            const r2 = await fetchSession(cur);
            const res = r2.res;
            const data2 = r2.data;
            if (!res.ok || !data2.success) {
                return {
                    error: (data2 && data2.message) || res.statusText || "Failed",
                    rows: [],
                    session_date: cur,
                    isFallback: false,
                    asOf: todayYmd,
                    todayOnly: false,
                };
            }
            const rows = data2.rows || [];
            if (rows.length > 0) {
                return {
                    error: null,
                    rows: rows,
                    session_date: data2.session_date || cur,
                    isFallback: isFallback,
                    asOf: todayYmd,
                    todayOnly: false,
                };
            }
            cur = previousTradingDayYmd(cur);
            steps++;
        }
        return {
            error: null,
            rows: [],
            session_date: todayYmd,
            isFallback: false,
            asOf: todayYmd,
            todayOnly: false,
        };
    }

    async function load() {
        const host = document.getElementById("premarketWatchlistHost");
        const msg = document.getElementById("premarketWatchlistMsg");
        const updated = document.getElementById("premarketWatchlistUpdated");
        const sessionHead = document.getElementById("premarketWatchlistSessionDate");
        if (!host) return;

        if (sessionHead) sessionHead.textContent = "—";
        if (msg) {
            msg.textContent = "Loading…";
            msg.style.display = "block";
        }

        try {
            const out = await loadWithFallback();
            if (out.error) {
                throw new Error(out.error);
            }
            const rows = out.rows || [];
            const sessionDate = out.session_date || "—";
            if (sessionHead) {
                sessionHead.textContent =
                    rows.length > 0 ? formatSessionDateLabel(sessionDate) : "—";
            }
            const inner = renderTable(rows);
            host.innerHTML = inner;
            if (msg) {
                if (rows.length > 0) {
                    let line =
                        "Session " +
                        sessionDate +
                        " · " +
                        rows.length +
                        " names";
                    if (out.isFallback) {
                        line +=
                            " · Showing last available session (today " +
                            out.asOf +
                            " has no scan yet)";
                    }
                    msg.textContent = line;
                } else {
                    if (out.todayOnly) {
                        msg.textContent =
                            "No watchlist for today (" +
                            out.asOf +
                            ") yet after 9:00 AM IST — previous days are hidden until today's scan completes.";
                    } else {
                        msg.textContent =
                            "No rows for " +
                            out.asOf +
                            " or prior trading days in range — scan may not have run.";
                    }
                }
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
            if (sessionHead) sessionHead.textContent = "—";
        }
    }

    function startPoll() {
        if (timer) clearInterval(timer);
        timer = setInterval(load, POLL_MS);
    }

    document.addEventListener("DOMContentLoaded", function () {
        const btn = document.getElementById("premarketWatchlistRefresh");
        if (btn)
            btn.addEventListener("click", function () {
                load();
            });
        load();
        startPoll();
    });
})();

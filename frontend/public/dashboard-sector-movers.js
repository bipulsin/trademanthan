/**
 * Dashboard: Top 3 Nifty sector gainers & losers (intraday % vs day open when API provides it).
 * Sector names expand to show 3 stocks in that sector (same basis as the API).
 */
(function () {
    const API = "/scan/dashboard-sector-movers";
    const DETAIL_API = "/scan/dashboard-sector-movers-detail";
    const POLL_MS = 5 * 60 * 1000;
    let timer = null;
    /** @type {Record<string, unknown>} */
    const detailCache = {};

    function fmtPct(n) {
        if (n == null || Number.isNaN(Number(n))) return "—";
        const x = Number(n);
        const sign = x > 0 ? "+" : "";
        return sign + x.toFixed(2) + "%";
    }

    function fmtLtp(n) {
        if (n == null || Number.isNaN(Number(n))) return "—";
        return Number(n).toLocaleString(undefined, {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        });
    }

    function escapeHtml(s) {
        const d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
    }

    function cacheKey(sector, mode) {
        return sector + "\0" + mode;
    }

    function renderList(ul, items, positiveColumn, listMode) {
        if (!ul) return;
        if (!items || items.length === 0) {
            ul.innerHTML = '<li class="sector-movers-empty">No data</li>';
            return;
        }
        ul.innerHTML = items
            .map((row) => {
                const cls = positiveColumn ? "sector-movers-pct--gain" : "sector-movers-pct--lose";
                const pct = row.pct_change;
                const sector = row.sector || "";
                const sectorAttr = encodeURIComponent(sector);
                const sectorLabel = escapeHtml(sector);
                const modeLabel = listMode === "losers" ? "bottom 3 losers" : "top 3 gainers";
                return (
                    `<li class="sector-movers-item">` +
                    `<div class="sector-movers-row">` +
                    `<button type="button" class="sector-movers-toggle" data-sector="${sectorAttr}" data-mode="${listMode}" aria-expanded="false" title="Show ${modeLabel} for this sector">` +
                    `<span class="sector-movers-chevron" aria-hidden="true">▸</span>` +
                    `<span class="sector-movers-name">${sectorLabel}</span>` +
                    `</button>` +
                    `<span class="sector-movers-pct ${cls}">${fmtPct(pct)}</span>` +
                    `</div>` +
                    `<div class="sector-movers-panel" hidden>` +
                    `<p class="sector-movers-panel-placeholder">Loading…</p>` +
                    `</div>` +
                    `</li>`
                );
            })
            .join("");
    }

    function renderDetailPanel(panel, data) {
        if (!panel) return;
        const stocks = (data && data.stocks) || [];
        const mode = (data && data.mode) || "gainers";
        const heading = mode === "losers" ? "Bottom 3 losers" : "Top 3 gainers";
        const subheading = data && data.fo_only
            ? "F&O stocks only"
            : "No F&O stocks found in this sector";
        if (!data || !data.success) {
            panel.innerHTML =
                `<p class="sector-movers-panel-msg">${escapeHtml(data && data.message ? data.message : "Could not load stocks.")}</p>`;
            return;
        }
        if (stocks.length === 0) {
            panel.innerHTML = `<p class="sector-movers-panel-msg">No stock quotes available.</p>`;
            return;
        }
        panel.innerHTML =
            `<p class="sector-movers-panel-title">${heading}</p>` +
            `<p class="sector-movers-panel-subtitle">${subheading}</p>` +
            `<ul class="sector-movers-stocks">` +
            stocks
                .map((s) => {
                    const sym = escapeHtml(s.symbol || "");
                    const hasFo = s.is_fo !== false;
                    const foBadge = hasFo
                        ? ""
                        : `<span class="sector-movers-stock-fo-badge" title="Not available in F&O" aria-label="Not available in F&O">F</span>`;
                    const pctCls =
                        s.pct_change != null && Number(s.pct_change) >= 0
                            ? "sector-movers-stock-pct--up"
                            : "sector-movers-stock-pct--down";
                    return (
                        `<li>` +
                        `<span class="sector-movers-stock-sym">${foBadge}${sym}</span>` +
                        `<span class="sector-movers-stock-ltp">₹${fmtLtp(s.ltp)}</span>` +
                        `<span class="sector-movers-stock-pct ${pctCls}">${fmtPct(s.pct_change)}</span>` +
                        `</li>`
                    );
                })
                .join("") +
            `</ul>`;
    }

    async function loadDetail(sector, mode, panel) {
        const key = cacheKey(sector, mode);
        if (detailCache[key] !== undefined) {
            renderDetailPanel(panel, detailCache[key]);
            return;
        }
        try {
            const qs = new URLSearchParams({ sector, mode });
            const res = await fetch(`${DETAIL_API}?${qs.toString()}`, {
                cache: "no-store",
                credentials: "same-origin",
            });
            const raw = await res.text();
            const ct = (res.headers.get("content-type") || "").toLowerCase();
            const looksJson = ct.includes("application/json") || /^\s*[\[{]/.test(raw.slice(0, 40));
            if (!looksJson) {
                detailCache[key] = { success: false, message: "Invalid response" };
                renderDetailPanel(panel, detailCache[key]);
                return;
            }
            const data = JSON.parse(raw);
            detailCache[key] = data;
            renderDetailPanel(panel, data);
        } catch (e) {
            console.warn("dashboard-sector-movers-detail:", e);
            panel.innerHTML = `<p class="sector-movers-panel-msg">Failed to load.</p>`;
        }
    }

    function onGridClick(e) {
        const btn = e.target.closest(".sector-movers-toggle");
        if (!btn) return;
        const rawSector = btn.getAttribute("data-sector");
        const mode = btn.getAttribute("data-mode") || "gainers";
        if (!rawSector) return;
        const sector = decodeURIComponent(rawSector);

        const item = btn.closest(".sector-movers-item");
        const panel = item && item.querySelector(".sector-movers-panel");
        if (!panel) return;

        const expanded = btn.getAttribute("aria-expanded") === "true";
        if (expanded) {
            btn.setAttribute("aria-expanded", "false");
            panel.hidden = true;
            btn.classList.remove("sector-movers-toggle--open");
            return;
        }

        btn.setAttribute("aria-expanded", "true");
        panel.hidden = false;
        btn.classList.add("sector-movers-toggle--open");

        if (panel.querySelector(".sector-movers-stocks")) {
            return;
        }
        panel.innerHTML = `<p class="sector-movers-panel-placeholder">Loading ${mode === "losers" ? "bottom 3 losers" : "top 3 gainers"}…</p>`;
        loadDetail(sector, mode, panel);
    }

    function render(data) {
        const g = document.getElementById("sectorMoversGainers");
        const l = document.getElementById("sectorMoversLosers");
        const t = document.getElementById("sectorMoversUpdated");
        Object.keys(detailCache).forEach((k) => delete detailCache[k]);
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
        renderList(g, data && data.gainers, true, "gainers");
        renderList(l, data && data.losers, false, "losers");
    }

    async function fetchMovers() {
        const grid = document.getElementById("sectorMoversGrid");
        if (!grid) return;
        try {
            const res = await fetch(API, { cache: "no-store", credentials: "same-origin" });
            const raw = await res.text();
            const ct = (res.headers.get("content-type") || "").toLowerCase();
            const looksJson = ct.includes("application/json") || /^\s*[\[{]/.test(raw.slice(0, 40));
            if (!looksJson) {
                console.warn("dashboard-sector-movers: non-JSON response");
                render({ gainers: [], losers: [] });
                return;
            }
            const data = JSON.parse(raw);
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
        const grid = document.getElementById("sectorMoversGrid");
        if (!grid) return;
        grid.addEventListener("click", onGridClick);
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

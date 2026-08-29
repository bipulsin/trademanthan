(function () {
    const API = "/api/breakfast-strategy/data";

    function $(id) { return document.getElementById(id); }

    function fmt(n, d) {
        if (n == null || Number.isNaN(Number(n))) return "—";
        return Number(n).toLocaleString(undefined, { minimumFractionDigits: d || 0, maximumFractionDigits: d || 2 });
    }

    function pnlClass(v) {
        var x = Number(v);
        if (Number.isNaN(x)) return "";
        return x >= 0 ? "bf-pnl-pos" : "bf-pnl-neg";
    }

    function setStatus(msg, err) {
        var el = $("bfStatus");
        if (!el) return;
        if (!msg) { el.hidden = true; return; }
        el.hidden = false;
        el.textContent = msg;
        el.style.borderColor = err ? "#f87171" : "#34d399";
        el.style.color = err ? "#fecaca" : "#bbf7d0";
    }

    async function parseJsonResponse(res) {
        var text = await res.text();
        try {
            return JSON.parse(text);
        } catch (_e) {
            if (text && text.trim().charAt(0) === "<") {
                throw new Error("Server timed out or returned an error page. Try again in a moment.");
            }
            throw new Error("Invalid server response");
        }
    }

    function renderSummary(s) {
        var box = $("bfSummary");
        if (!box || !s) return;
        var cards = [
            ["Trades", s.total_trades],
            ["Win rate", (s.win_rate_pct != null ? s.win_rate_pct + "%" : "—")],
            ["Total P&L", "₹" + fmt(s.total_pnl_inr, 0)],
            ["Avg P&L", "₹" + fmt(s.avg_pnl_inr, 0)],
            ["Avg R", fmt(s.avg_r, 2)],
            ["Long", (s.long_trades || 0) + " · ₹" + fmt(s.long_pnl_inr, 0)],
            ["Short", (s.short_trades || 0) + " · ₹" + fmt(s.short_pnl_inr, 0)],
        ];
        box.innerHTML = cards.map(function (c) {
            return '<div class="vmb-stat"><span class="vmb-stat-label">' + c[0] +
                '</span><span class="vmb-stat-val">' + c[1] + "</span></div>";
        }).join("");
    }

    function renderSectorTable(rows) {
        var tb = $("bfSectorTable") && $("bfSectorTable").querySelector("tbody");
        if (!tb) return;
        if (!rows || !rows.length) {
            tb.innerHTML = "<tr><td colspan='4'>No sector data</td></tr>";
            return;
        }
        tb.innerHTML = rows.map(function (r) {
            return "<tr><td>" + (r.sector || "") + "</td><td>" + (r.trades || 0) +
                "</td><td>" + (r.win_rate_pct != null ? r.win_rate_pct + "%" : "—") +
                "</td><td class='" + pnlClass(r.pnl_inr) + "'>₹" + fmt(r.pnl_inr, 0) + "</td></tr>";
        }).join("");
    }

    function renderTrades(rows) {
        var tb = $("bfTradesTable") && $("bfTradesTable").querySelector("tbody");
        if (!tb) return;
        if (!rows || !rows.length) {
            tb.innerHTML = "<tr><td colspan='15'>No trades</td></tr>";
            return;
        }
        tb.innerHTML = rows.map(function (t) {
            var sd = String(t.session_date || "").slice(0, 10);
            var anchor = t.anchor_price != null ? t.anchor_price : t.setup_close_5m;
            var extreme = t.pre_exit_extreme;
            var sym = t.symbol || "";
            if (!sym && t.underlying_symbol) {
                sym = t.underlying_symbol + (t.instrument_label ? " " + t.instrument_label : "");
            }
            return "<tr>" +
                "<td>" + sd + "</td>" +
                "<td>" + sym + "</td>" +
                "<td>" + (t.direction || "") + "</td>" +
                "<td>" + (t.sector || "") + "</td>" +
                "<td>" + fmt(t.nifty_bias_pct, 2) + "%</td>" +
                "<td>" + fmt(t.stock_move_pct_at_entry, 2) + "%</td>" +
                "<td>" + fmt(anchor, 2) + "</td>" +
                "<td>" + fmt(t.tp_price, 2) + "</td>" +
                "<td>" + fmt(t.sl_price, 2) + "</td>" +
                "<td>" + fmt(extreme, 2) + "</td>" +
                "<td>" + fmt(t.entry_price, 2) + "</td>" +
                "<td>" + fmt(t.exit_price, 2) + "</td>" +
                "<td>" + (t.exit_trigger_type || "") + "</td>" +
                "<td>" + (t.lot_size != null ? t.lot_size : "") + "</td>" +
                "<td class='" + pnlClass(t.pnl_inr) + "'>₹" + fmt(t.pnl_inr, 0) + "</td>" +
                "</tr>";
        }).join("");
    }

    function renderData(data) {
        renderSummary(data.summary || {});
        renderSectorTable((data.summary && data.summary.by_sector) || []);
        renderTrades(data.trades || []);
        var cap = $("bfPnlCapToggle");
        if (cap && data.pnl_cap_enabled != null) {
            cap.checked = !!data.pnl_cap_enabled;
        }
        setStatus(
            "Loaded " + (data.trades || []).length + " trades · " +
            (data.date_from || "?") + " → " + (data.date_to || "?") +
            (data.pnl_cap_enabled ? " · ₹5K cap ON" : " · ₹5K cap OFF"),
            false
        );
    }

    async function load(pnlCapEnabled) {
        var cap = $("bfPnlCapToggle");
        if (pnlCapEnabled == null && cap) {
            pnlCapEnabled = cap.checked;
        }
        var url = API + "?pnl_cap_enabled=" + (pnlCapEnabled ? "true" : "false");
        setStatus("Loading…", false);
        if (cap) cap.disabled = true;
        try {
            var res = await fetch(url, { cache: "no-store", credentials: "same-origin" });
            var data = await parseJsonResponse(res);
            if (!res.ok) throw new Error(data.detail || data.message || "Load failed");
            renderData(data);
        } catch (e) {
            setStatus(String(e), true);
            renderSummary({});
            renderSectorTable([]);
            renderTrades([]);
        } finally {
            if (cap) cap.disabled = false;
        }
    }

    function initPnlCapToggle() {
        var cap = $("bfPnlCapToggle");
        if (!cap) return;
        cap.addEventListener("change", function () {
            load(cap.checked);
        });
    }

    function initTheme() {
        var btn = $("bfThemeBtn");
        if (!btn) return;
        btn.addEventListener("click", function () {
            var body = document.body;
            var dark = body.getAttribute("data-theme") !== "light";
            body.setAttribute("data-theme", dark ? "light" : "dark");
            btn.querySelector("i").className = dark ? "fas fa-sun" : "fas fa-moon";
        });
    }

    document.addEventListener("DOMContentLoaded", function () {
        initTheme();
        initPnlCapToggle();
        var reload = $("bfReloadBtn");
        if (reload) reload.addEventListener("click", function () { load(null); });
        load(null);
    });
})();

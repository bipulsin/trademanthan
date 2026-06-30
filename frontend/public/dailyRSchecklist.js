/**
 * Daily RS Trade Checklist — left-menu layout, bull/bear columns, modal checklist.
 * System fields auto-fill from RS scanner; user only edits news, ADX 9:35 override, notes, counter-RS.
 */
(function () {
    "use strict";

    var API = "/api/dashboard/daily-checklist";
    var state = null;
    var saveTimers = {};
    var cardEls = {};
    var modalSymbol = null;

    var AUTO_FIELDS = [
        "entry_time", "kavach_score_entry", "confidence", "trading_state",
        "ema_vs_vwap", "supertrend", "macd", "adx_entry", "volume", "di_alignment"
    ];
    var AUTO_LABELS = {
        entry_time: "Entry Time",
        kavach_score_entry: "Kavach Score @ Entry",
        confidence: "Confidence Grade",
        trading_state: "Trading State",
        ema_vs_vwap: "EMA5 vs VWAP",
        supertrend: "Supertrend",
        macd: "MACD",
        adx_entry: "ADX @ Entry",
        di_alignment: "DI+ vs DI-",
        volume: "Volume"
    };
    var FLAG = {
        entry_time: "time_ok", kavach_score_entry: "score_ok", confidence: "confidence_ok",
        trading_state: "state_ok", ema_vs_vwap: "ema_ok", supertrend: "st_ok",
        macd: "macd_ok", adx_entry: "adx_ok", volume: "volume_ok"
    };
    var SECTION_ORDER = { GO: 0, WATCH: 1, OUT: 2, NONE: 1 };

    function $(id) { return document.getElementById(id); }
    function el(tag, cls, txt) {
        var e = document.createElement(tag);
        if (cls) e.className = cls;
        if (txt != null) e.textContent = txt;
        return e;
    }
    function lsKey() { return "dc_state_" + (state ? state.session_date : "today"); }

    function api(path, opts) {
        return fetch(API + path, opts).then(function (r) { return r.json(); });
    }

    function toast(msg) {
        var t = $("dcToast");
        t.textContent = msg;
        t.classList.add("show");
        setTimeout(function () { t.classList.remove("show"); }, 1800);
    }

    function nowIST() {
        var parts = new Intl.DateTimeFormat("en-GB", {
            timeZone: "Asia/Kolkata", hour12: false,
            hour: "2-digit", minute: "2-digit", second: "2-digit"
        }).formatToParts(new Date());
        var o = {};
        parts.forEach(function (p) { if (p.type !== "literal") o[p.type] = p.value; });
        var h = parseInt(o.hour, 10), m = parseInt(o.minute, 10), s = parseInt(o.second, 10);
        return { minutes: h * 60 + m, secs: h * 3600 + m * 60 + s,
                 str: o.hour + ":" + o.minute + ":" + o.second };
    }

    function fmtDate(iso) {
        if (!iso) return "—";
        var d = new Date(iso + "T00:00:00");
        return ("0" + d.getDate()).slice(-2) + "-" +
            ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][d.getMonth()] +
            "-" + d.getFullYear();
    }

    function decisionClass(stock) {
        if (!stock.decision || stock.decision.indexOf("⬜") === 0) return "NONE";
        return stock.section || "WATCH";
    }

    function scoreClass(score) {
        var s = Number(score);
        if (s >= 90) return "dc-score--green";
        if (s >= 70) return "dc-score--amber";
        return "dc-score--red";
    }

    function currentStock(symbol) {
        if (!state || !state.stocks) return null;
        for (var i = 0; i < state.stocks.length; i++) {
            if (state.stocks[i].symbol === symbol) return state.stocks[i];
        }
        return null;
    }

    function hintFor(field, stock) {
        if (field === "adx_935") {
            var st = stock.adx_935_status;
            if (st === "immediate") return { text: "✓ Immediate list", cls: "dc-item-hint--ok" };
            if (st === "recheck") return { text: "⚠ Recheck 10AM & 10:30AM", cls: "dc-item-hint--warn" };
            if (st === "watch") return { text: "✗ Watch only", cls: "dc-item-hint--bad" };
            return null;
        }
        var flag = FLAG[field];
        if (!flag) return null;
        var v = stock[flag];
        if (v === true) return { text: "✓ PASS", cls: "dc-item-hint--ok" };
        if (v === false) {
            if (field === "entry_time") return { text: "✗ HARD FAIL (outside 10:15–14:30)", cls: "dc-item-hint--bad" };
            return { text: "✗ FAIL", cls: "dc-item-hint--bad" };
        }
        return null;
    }

    function autoValClass(field, stock) {
        var flag = FLAG[field];
        if (!flag) return "neutral";
        var v = stock[flag];
        if (v === true) return "pass";
        if (v === false) return "fail";
        return "neutral";
    }

    // ---- cards (bull / bear columns) ----
    function ensureCard(symbol) {
        if (cardEls[symbol]) return cardEls[symbol];
        var node = $("dcCardTpl").content.firstElementChild.cloneNode(true);
        node.dataset.symbol = symbol;
        node.addEventListener("click", function () { openModal(symbol); });
        cardEls[symbol] = node;
        return node;
    }

    function patchCard(card, stock) {
        var dcls = decisionClass(stock);
        card.className = "dc-card";
        if (dcls === "GO") card.classList.add("dc-card--go");
        if (dcls === "OUT") card.classList.add("dc-card--out");
        card.querySelector(".dc-symbol").textContent = stock.symbol;
        var rsv = stock.rs_pct;
        var rs = card.querySelector(".dc-rs");
        rs.textContent = rsv == null ? "" : "RS " + (rsv > 0 ? "+" : "") + Number(rsv).toFixed(2) + "%";
        rs.className = "dc-rs " + (Number(rsv) >= 0 ? "dc-rs--pos" : "dc-rs--neg");
        var score = card.querySelector(".dc-score");
        if (stock.dashboard_score != null) {
            score.textContent = stock.dashboard_score;
            score.className = "dc-score " + scoreClass(stock.dashboard_score);
            score.style.display = "";
        } else { score.style.display = "none"; }
        var conf = card.querySelector(".dc-conf");
        conf.textContent = stock.confidence || stock.dashboard_kavach || "";
        var dec = card.querySelector(".dc-decision");
        dec.textContent = stock.decision || "⬜ Not assessed";
        dec.className = "dc-decision dc-decision--" + dcls;
    }

    function sortStocks(list) {
        return list.slice().sort(function (a, b) {
            var oa = SECTION_ORDER[decisionClass(a)] != null ? SECTION_ORDER[decisionClass(a)] : 1;
            var ob = SECTION_ORDER[decisionClass(b)] != null ? SECTION_ORDER[decisionClass(b)] : 1;
            if (oa !== ob) return oa - ob;
            return (b.rs_pct || 0) - (a.rs_pct || 0);
        });
    }

    function render() {
        if (!state) return;
        $("dcDate").textContent = fmtDate(state.session_date);
        var nifty = "";
        if (state.nifty50 != null) nifty += "NIFTY <b>" + state.nifty50 + "</b>";
        if (state.banknifty != null) nifty += (nifty ? " · " : "") + "BANKNIFTY <b>" + state.banknifty + "</b>";
        $("dcNifty").innerHTML = nifty;
        var c = state.counts || { go: 0, watch: 0, out: 0 };
        $("dcPillGo").textContent = "🟢 " + c.go + " GO";
        $("dcPillWatch").textContent = "🟡 " + c.watch + " WATCH";
        $("dcPillOut").textContent = "🔴 " + c.out + " OUT";

        var sel = $("dcNiftyDir");
        if (document.activeElement !== sel) sel.value = state.nifty_open_direction || "";
        $("dcGapWarn").classList.toggle("show", (state.nifty_open_direction || "") === "Gap reversed");

        var stocks = state.stocks || [];
        var empty = stocks.length === 0;
        $("dcEmpty").hidden = !empty;
        $("dcColumns").hidden = empty;

        var bull = sortStocks(stocks.filter(function (s) { return s.direction === "LONG"; }));
        var bear = sortStocks(stocks.filter(function (s) { return s.direction === "SHORT"; }));
        var bullGrid = $("dcBullGrid");
        var bearGrid = $("dcBearGrid");
        bull.forEach(function (stock) {
            var card = ensureCard(stock.symbol);
            patchCard(card, stock);
            if (card.parentNode !== bullGrid) bullGrid.appendChild(card);
        });
        bear.forEach(function (stock) {
            var card = ensureCard(stock.symbol);
            patchCard(card, stock);
            if (card.parentNode !== bearGrid) bearGrid.appendChild(card);
        });
        // remove stale cards
        [bullGrid, bearGrid].forEach(function (grid) {
            var syms = {};
            stocks.forEach(function (s) { syms[s.symbol] = true; });
            Array.prototype.slice.call(grid.children).forEach(function (ch) {
                if (!syms[ch.dataset.symbol]) grid.removeChild(ch);
            });
        });

        if (modalSymbol) renderModal(currentStock(modalSymbol));
    }

    function applyState(s) {
        if (!s) return;
        if (s.error) { toast("Error: " + s.error); return; }
        state = s;
        try { localStorage.setItem(lsKey(), JSON.stringify(s)); } catch (e) {}
        render();
    }

    // ---- modal ----
    function openModal(symbol) {
        modalSymbol = symbol;
        $("dcModal").hidden = false;
        $("dcModal").setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";
        toast("Refreshing from RS…");
        api("/sync", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ symbol: symbol })
        }).then(function (s) {
            applyState(s);
            renderModal(currentStock(symbol));
        }).catch(function () {
            renderModal(currentStock(symbol));
        });
    }

    function closeModal() {
        modalSymbol = null;
        $("dcModal").hidden = true;
        $("dcModal").setAttribute("aria-hidden", "true");
        document.body.style.overflow = "";
    }

    function renderModal(stock) {
        if (!stock) return;
        $("dcModalTitle").textContent = stock.symbol + " · " + stock.direction;
        var sub = [];
        if (stock.rs_pct != null) sub.push("RS " + (stock.rs_pct > 0 ? "+" : "") + Number(stock.rs_pct).toFixed(2) + "%");
        if (stock.dashboard_score != null) sub.push("Score " + stock.dashboard_score);
        if (stock.vol_multiplier != null) sub.push("Vol " + Number(stock.vol_multiplier).toFixed(2) + "×");
        $("dcModalSub").textContent = sub.join(" · ");

        var body = $("dcModalBody");
        body.innerHTML = "";

        // recheck banner
        var rc = el("div", "dc-recheck", "⏰ Recheck ADX at 10:00 AM");
        if (stock.adx_935_status === "recheck") {
            rc.classList.add("show");
            var t = nowIST();
            if (t.minutes >= 600) {
                rc.classList.add("flash");
                rc.textContent = "⏰ Now is 10:00 AM — recheck ADX for this stock";
            }
        }
        body.appendChild(rc);

        body.appendChild(el("div", "dc-group-title", "Pre-market"));

        // News — manual
        body.appendChild(buildNewsItem(stock));

        // ADX 9:35 — manual override (pre-filled from system)
        body.appendChild(buildAdx935Item(stock));

        body.appendChild(el("div", "dc-group-title", "Entry gate (auto from RS scanner)"));

        AUTO_FIELDS.forEach(function (field) {
            body.appendChild(buildAutoItem(field, stock));
        });

        // Counter-RS — manual
        var cr = el("label", "dc-counter");
        var cb = el("input"); cb.type = "checkbox";
        cb.checked = !!stock.counter_rs;
        cb.addEventListener("change", function () { onChange(stock.symbol, "counter_rs", cb.checked); });
        cr.appendChild(cb);
        cr.appendChild(el("span", null, "Counter-RS direction? (A-grade mandatory)"));
        body.appendChild(cr);

        // Progress
        var gs = Number(stock.gate_score || 0);
        var pw = el("div", "dc-progress-wrap");
        pw.appendChild(el("div", "dc-progress-label", gs + " / 9 entry conditions met"));
        var pbar = el("div", "dc-progress");
        var pfill = el("div", "dc-progress-fill");
        pfill.style.width = Math.round((gs / 9) * 100) + "%";
        pbar.appendChild(pfill);
        pw.appendChild(pbar);
        body.appendChild(pw);

        var dec = el("div", "dc-modal-decision dc-decision dc-decision--" + decisionClass(stock), stock.decision || "⬜ Not assessed");
        body.appendChild(dec);

        // Notes — manual
        var notes = el("textarea", "dc-notes");
        notes.placeholder = "Trade notes…";
        notes.value = stock.notes || "";
        notes.addEventListener("input", function () { onChange(stock.symbol, "notes", notes.value); });
        body.appendChild(notes);

        if (stock.updated_at) {
            var d = new Date(stock.updated_at);
            body.appendChild(el("div", "dc-saved",
                "Last saved: " + ("0" + d.getHours()).slice(-2) + ":" +
                ("0" + d.getMinutes()).slice(-2) + ":" + ("0" + d.getSeconds()).slice(-2)));
        }
    }

    function buildAutoItem(field, stock) {
        var it = el("div", "dc-item");
        var lab = el("div", "dc-item-label");
        lab.appendChild(el("span", null, AUTO_LABELS[field] || field));
        lab.appendChild(el("span", "dc-sys-badge", "System"));
        var hint = el("span", "dc-item-hint");
        var h = hintFor(field, stock);
        if (h) { hint.textContent = h.text; hint.className = "dc-item-hint " + h.cls; }
        lab.appendChild(hint);
        it.appendChild(lab);
        var val = el("div", "dc-auto-val " + autoValClass(field, stock));
        val.textContent = stock[field] == null ? "—" : String(stock[field]);
        it.appendChild(val);
        return it;
    }

    function buildNewsItem(stock) {
        var it = el("div", "dc-item");
        it.appendChild(el("div", "dc-item-label", "News Clean?"));
        var row = el("div", "dc-toggle-row");
        [["CLEAN", "true"], ["ADVERSE NEWS", "false"]].forEach(function (pair) {
            var b = el("button", "dc-toggle", pair[0]);
            b.type = "button";
            if (stock.news_clean === (pair[1] === "true")) {
                b.classList.add(pair[1] === "true" ? "sel-pass" : "sel-fail");
            }
            b.addEventListener("click", function () {
                var cur = currentStock(stock.symbol);
                var isSel = cur && cur.news_clean === (pair[1] === "true");
                onChange(stock.symbol, "news_clean", isSel ? "" : pair[1]);
            });
            row.appendChild(b);
        });
        it.appendChild(row);
        return it;
    }

    function buildAdx935Item(stock) {
        var it = el("div", "dc-item");
        var lab = el("div", "dc-item-label");
        lab.appendChild(el("span", null, "ADX at 9:35 AM"));
        lab.appendChild(el("span", "dc-sys-badge", "Override"));
        var hint = el("span", "dc-item-hint");
        var h = hintFor("adx_935", stock);
        if (h) { hint.textContent = h.text; hint.className = "dc-item-hint " + h.cls; }
        lab.appendChild(hint);
        it.appendChild(lab);
        var inp = el("input", "dc-num"); inp.type = "number"; inp.step = "0.01"; inp.inputMode = "decimal";
        inp.placeholder = "TradingView 9:35 close";
        inp.value = stock.adx_935 == null ? "" : stock.adx_935;
        inp.addEventListener("input", function () { onChange(stock.symbol, "adx_935", inp.value); });
        it.appendChild(inp);
        return it;
    }

    // ---- updates ----
    function onChange(symbol, field, value) {
        var stock = currentStock(symbol);
        if (stock) {
            if (field === "counter_rs") stock[field] = !!value;
            else if (field === "news_clean") stock[field] = value === "" ? null : (value === "true" || value === true);
            else stock[field] = value === "" ? null : value;
        }
        var key = (symbol || "_page") + "|" + field;
        clearTimeout(saveTimers[key]);
        saveTimers[key] = setTimeout(function () {
            api("/update", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ symbol: symbol, field: field, value: value })
            }).then(applyState).catch(function () { toast("Save failed"); });
        }, 500);
    }

    function pull() {
        toast("Refreshing from RS scanner…");
        api("/populate", { method: "POST" }).then(applyState).catch(function () { toast("Refresh failed"); });
    }

    function resetDay() {
        if (!confirm("Reset today's checklist? Saved values will be cleared (history is kept).")) return;
        api("/reset", { method: "POST" })
            .then(function (s) {
                cardEls = {};
                $("dcBullGrid").innerHTML = "";
                $("dcBearGrid").innerHTML = "";
                closeModal();
                applyState(s);
                toast("Day reset");
            });
    }

    function tickClock() {
        var t = nowIST();
        $("dcClock").textContent = t.str + " IST";
        var w = $("dcWindow");
        var start = 10 * 60 + 15, end = 14 * 60 + 30;
        if (t.minutes < start) {
            var rem = (start * 60) - t.secs;
            w.textContent = "Entry opens in " + ("0" + Math.floor(rem / 60)).slice(-2) + ":" + ("0" + (rem % 60)).slice(-2);
            w.className = "dc-window pre";
        } else if (t.minutes <= end) {
            w.textContent = "Entry window open";
            w.className = "dc-window open";
        } else {
            w.textContent = "Entry window closed";
            w.className = "dc-window closed";
        }
        if (modalSymbol) {
            var stock = currentStock(modalSymbol);
            if (stock && stock.adx_935_status === "recheck") renderModal(stock);
        }
    }

    function boot() {
        try {
            var cached = localStorage.getItem("dc_state_" + new Date().toISOString().slice(0, 10));
            if (cached) { state = JSON.parse(cached); render(); }
        } catch (e) {}

        $("dcNiftyDir").addEventListener("change", function () {
            onChange("", "nifty_open_direction", this.value);
        });
        $("dcPull").addEventListener("click", pull);
        $("dcPullEmpty").addEventListener("click", pull);
        $("dcReset").addEventListener("click", resetDay);
        $("dcPrint").addEventListener("click", function () { window.print(); });
        $("dcSave").addEventListener("click", function () {
            try { localStorage.setItem(lsKey(), JSON.stringify(state)); } catch (e) {}
            toast("Session saved");
        });
        $("dcModalClose").addEventListener("click", closeModal);
        $("dcModalBackdrop").addEventListener("click", closeModal);

        api("/data").then(function (s) {
            if (!s.stocks || s.stocks.length === 0) return api("/populate", { method: "POST" });
            return s;
        }).then(applyState).catch(function () { $("dcEmpty").querySelector("p").textContent = "Could not load checklist."; });

        tickClock();
        setInterval(tickClock, 1000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else { boot(); }
})();

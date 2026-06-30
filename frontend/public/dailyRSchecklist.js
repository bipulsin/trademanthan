/**
 * Daily RS Trade Checklist — client logic.
 *
 * Server (services/daily_checklist) is the source of truth: the browser sends raw
 * field values and re-renders from the returned full state. We never recompute
 * the decision here. Vanilla JS only (matches the rest of the app); no frameworks.
 */
(function () {
    "use strict";

    var API = "/api/dashboard/daily-checklist";
    var state = null;            // last full state from server
    var saveTimers = {};         // per (symbol|field) debounce timers
    var cardEls = {};            // symbol -> card DOM node

    // Entry-gate option lists.
    var OPTIONS = {
        confidence: ["A", "B", "C", "D"],
        trading_state: ["BUY", "MANAGE LONG", "SELL", "MANAGE SHORT", "HOLD/WATCH"],
        ema_vs_vwap: ["Above", "Below", "At VWAP"],
        supertrend: ["Bullish", "Bearish"],
        macd: ["Bullish", "Bearish", "Crossing"],
        di_alignment: ["DI+>DI-", "DI->DI+"],
        volume: ["High", "Normal", "Low"]
    };
    // field -> derived PASS/FAIL flag key (for colouring the selected option).
    var FLAG = {
        entry_time: "time_ok", kavach_score_entry: "score_ok", confidence: "confidence_ok",
        trading_state: "state_ok", ema_vs_vwap: "ema_ok", supertrend: "supertrend_ok",
        macd: "macd_ok", adx_entry: "adx_ok", volume: "volume_ok"
    };
    // (supertrend flag is st_ok on the server; remap below.)
    FLAG.supertrend = "st_ok";

    // ---- helpers ----
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

    // IST clock independent of the device timezone.
    function nowIST() {
        var parts = new Intl.DateTimeFormat("en-GB", {
            timeZone: "Asia/Kolkata", hour12: false,
            hour: "2-digit", minute: "2-digit", second: "2-digit"
        }).formatToParts(new Date());
        var o = {};
        parts.forEach(function (p) { if (p.type !== "literal") o[p.type] = p.value; });
        var h = parseInt(o.hour, 10), m = parseInt(o.minute, 10), s = parseInt(o.second, 10);
        return { h: h, m: m, s: s, minutes: h * 60 + m, secs: h * 3600 + m * 60 + s,
                 str: o.hour + ":" + o.minute + ":" + o.second };
    }

    // ---- rendering ----
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

    function hintFor(field, stock) {
        // returns {text, cls} for value fields, else null
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

    function buildOptionGroup(card, symbol, field, labels, values) {
        var wrap = el("div", "dc-toggle-row");
        wrap.dataset.field = field;
        (values || labels).forEach(function (val, i) {
            var b = el("button", "dc-toggle", labels[i]);
            b.type = "button";
            b.dataset.field = field;
            b.dataset.value = val;
            b.addEventListener("click", function () {
                var cur = currentStock(symbol);
                var newVal = (cur && cur[field] === val) ? "" : val; // tap again to clear
                onChange(symbol, field, newVal);
            });
            wrap.appendChild(b);
        });
        return wrap;
    }

    function item(labelText) {
        var it = el("div", "dc-item");
        var lab = el("div", "dc-item-label");
        lab.appendChild(el("span", null, labelText));
        var hint = el("span", "dc-item-hint");
        hint.dataset.hint = "1";
        lab.appendChild(hint);
        it.appendChild(lab);
        return it;
    }

    function buildBody(card, symbol) {
        var body = card.querySelector(".dc-card-body");
        body.innerHTML = "";

        // Recheck banner
        var rc = el("div", "dc-recheck");
        rc.dataset.recheck = "1";
        rc.textContent = "⏰ Recheck ADX at 10:00 AM";
        body.appendChild(rc);

        // PRE-MARKET
        body.appendChild(el("div", "dc-group-title", "Pre-market (8:45 AM)"));

        var news = item("News Clean?");
        var newsRow = el("div", "dc-toggle-row");
        [["CLEAN", "true"], ["ADVERSE NEWS", "false"]].forEach(function (pair) {
            var b = el("button", "dc-toggle", pair[0]);
            b.type = "button"; b.dataset.field = "news_clean"; b.dataset.value = pair[1];
            b.addEventListener("click", function () {
                var cur = currentStock(symbol);
                var want = pair[1] === "true";
                var newVal = (cur && cur.news_clean === want) ? "" : pair[1];
                onChange(symbol, "news_clean", newVal);
            });
            newsRow.appendChild(b);
        });
        news.appendChild(newsRow);
        body.appendChild(news);

        var adx935 = item("ADX at 9:35 AM");
        var n1 = el("input", "dc-num"); n1.type = "number"; n1.step = "0.01"; n1.inputMode = "decimal";
        n1.dataset.field = "adx_935"; n1.placeholder = "e.g. 25.27";
        n1.addEventListener("input", function () { onChange(symbol, "adx_935", n1.value); });
        adx935.appendChild(n1);
        body.appendChild(adx935);

        // ENTRY GATE
        body.appendChild(el("div", "dc-group-title", "Entry gate (9 conditions)"));

        var t = item("Entry Time");
        var tin = el("input", "dc-time"); tin.type = "time"; tin.dataset.field = "entry_time";
        tin.addEventListener("input", function () { onChange(symbol, "entry_time", tin.value); });
        t.appendChild(tin);
        body.appendChild(t);

        var sc = item("Kavach Score @ Entry");
        var n2 = el("input", "dc-num"); n2.type = "number"; n2.inputMode = "numeric";
        n2.dataset.field = "kavach_score_entry"; n2.placeholder = "0–100";
        n2.addEventListener("input", function () { onChange(symbol, "kavach_score_entry", n2.value); });
        sc.appendChild(n2);
        body.appendChild(sc);

        var conf = item("Confidence Grade");
        conf.appendChild(buildOptionGroup(card, symbol, "confidence", OPTIONS.confidence));
        body.appendChild(conf);

        var stt = item("Trading State");
        stt.appendChild(buildOptionGroup(card, symbol, "trading_state", OPTIONS.trading_state));
        body.appendChild(stt);

        var ema = item("EMA5 vs VWAP");
        ema.appendChild(buildOptionGroup(card, symbol, "ema_vs_vwap", OPTIONS.ema_vs_vwap));
        body.appendChild(ema);

        var st = item("Supertrend");
        st.appendChild(buildOptionGroup(card, symbol, "supertrend", OPTIONS.supertrend));
        body.appendChild(st);

        var macd = item("MACD");
        macd.appendChild(buildOptionGroup(card, symbol, "macd", OPTIONS.macd));
        body.appendChild(macd);

        var adxe = item("ADX @ Entry (≥25)");
        var n3 = el("input", "dc-num"); n3.type = "number"; n3.step = "0.1"; n3.inputMode = "decimal";
        n3.dataset.field = "adx_entry"; n3.placeholder = "e.g. 34.5";
        n3.addEventListener("input", function () { onChange(symbol, "adx_entry", n3.value); });
        adxe.appendChild(n3);
        var diLab = el("div", "dc-item-label"); diLab.appendChild(el("span", null, "DI+ vs DI-"));
        adxe.appendChild(diLab);
        adxe.appendChild(buildOptionGroup(card, symbol, "di_alignment", OPTIONS.di_alignment));
        body.appendChild(adxe);

        var vol = item("Volume");
        vol.appendChild(buildOptionGroup(card, symbol, "volume", OPTIONS.volume));
        body.appendChild(vol);

        // Counter-RS
        var cr = el("label", "dc-counter");
        var cb = el("input"); cb.type = "checkbox"; cb.dataset.field = "counter_rs";
        cb.style.width = "20px"; cb.style.height = "20px";
        cb.addEventListener("change", function () { onChange(symbol, "counter_rs", cb.checked); });
        cr.appendChild(cb);
        cr.appendChild(el("span", null, "Counter-RS direction? (A-grade confidence mandatory)"));
        body.appendChild(cr);

        // Progress
        var pw = el("div", "dc-progress-wrap");
        var pl = el("div", "dc-progress-label"); pl.dataset.prog = "1"; pl.textContent = "0 / 9 entry conditions met";
        var pbar = el("div", "dc-progress");
        var pfill = el("div", "dc-progress-fill"); pfill.dataset.fill = "1";
        pbar.appendChild(pfill); pw.appendChild(pl); pw.appendChild(pbar);
        body.appendChild(pw);

        // Notes
        var notes = el("textarea", "dc-notes"); notes.dataset.field = "notes";
        notes.placeholder = "Trade notes…";
        notes.addEventListener("input", function () { onChange(symbol, "notes", notes.value); });
        body.appendChild(notes);

        var saved = el("div", "dc-saved"); saved.dataset.saved = "1"; saved.textContent = "";
        body.appendChild(saved);
    }

    function ensureCard(symbol) {
        if (cardEls[symbol]) return cardEls[symbol];
        var tpl = $("dcCardTpl");
        var node = tpl.content.firstElementChild.cloneNode(true);
        node.dataset.symbol = symbol;
        node.querySelector(".dc-card-face").addEventListener("click", function (e) {
            if (e.target.closest("button, input, select, textarea")) return;
            node.classList.toggle("open");
        });
        buildBody(node, symbol);
        cardEls[symbol] = node;
        return node;
    }

    function currentStock(symbol) {
        if (!state) return null;
        for (var i = 0; i < state.stocks.length; i++) {
            if (state.stocks[i].symbol === symbol) return state.stocks[i];
        }
        return null;
    }

    function setOptionColors(card, field, stock) {
        var btns = card.querySelectorAll('button.dc-toggle[data-field="' + field + '"]');
        var flag = FLAG[field];
        var passFail = flag ? stock[flag] : null;
        btns.forEach(function (b) {
            b.classList.remove("sel-pass", "sel-fail", "sel-neutral");
            var sel = String(stock[field] == null ? "" : stock[field]) === b.dataset.value;
            if (!sel) return;
            if (field === "news_clean") {
                b.classList.add(b.dataset.value === "true" ? "sel-pass" : "sel-fail");
            } else if (passFail === true) {
                b.classList.add("sel-pass");
            } else if (passFail === false) {
                b.classList.add("sel-fail");
            } else {
                b.classList.add("sel-neutral");
            }
        });
    }

    function setHint(card, field, stock) {
        // find the hint span belonging to the input/group with this field
        var input = card.querySelector('[data-field="' + field + '"]');
        if (!input) return;
        var itemEl = input.closest(".dc-item");
        if (!itemEl) return;
        var hint = itemEl.querySelector('[data-hint="1"]');
        if (!hint) return;
        var h = hintFor(field, stock);
        hint.textContent = h ? h.text : "";
        hint.className = "dc-item-hint" + (h ? " " + h.cls : "");
    }

    function patchCard(card, stock) {
        // News toggle
        setOptionColors(card, "news_clean", stock);
        // value fields: only set if not focused (avoid clobbering typing)
        ["adx_935", "entry_time", "kavach_score_entry", "adx_entry"].forEach(function (f) {
            var inp = card.querySelector('input[data-field="' + f + '"]');
            if (inp && document.activeElement !== inp) {
                inp.value = stock[f] == null ? "" : stock[f];
            }
            setHint(card, f, stock);
        });
        // option groups
        ["confidence", "trading_state", "ema_vs_vwap", "supertrend", "macd", "di_alignment", "volume"].forEach(function (f) {
            setOptionColors(card, f, stock);
        });
        // counter-rs checkbox
        var cb = card.querySelector('input[data-field="counter_rs"]');
        if (cb && document.activeElement !== cb) cb.checked = !!stock.counter_rs;
        // notes
        var notes = card.querySelector('textarea[data-field="notes"]');
        if (notes && document.activeElement !== notes) notes.value = stock.notes || "";

        // recheck banner
        var rc = card.querySelector('[data-recheck="1"]');
        if (rc) {
            if (stock.adx_935_status === "recheck") {
                rc.classList.add("show");
                var t = nowIST();
                if (t.minutes >= 600) { // 10:00
                    rc.classList.add("flash");
                    rc.textContent = "⏰ Now is 10:00 AM — recheck ADX for this stock";
                } else {
                    rc.classList.remove("flash");
                    rc.textContent = "⏰ Recheck ADX at 10:00 AM";
                }
            } else {
                rc.classList.remove("show", "flash");
            }
        }

        // progress
        var gs = Number(stock.gate_score || 0);
        var pl = card.querySelector('[data-prog="1"]');
        var pf = card.querySelector('[data-fill="1"]');
        if (pl) pl.textContent = gs + " / 9 entry conditions met";
        if (pf) pf.style.width = Math.round((gs / 9) * 100) + "%";

        // saved time
        var saved = card.querySelector('[data-saved="1"]');
        if (saved && stock.updated_at) {
            var d = new Date(stock.updated_at);
            saved.textContent = "Last saved: " + ("0" + d.getHours()).slice(-2) + ":" +
                ("0" + d.getMinutes()).slice(-2) + ":" + ("0" + d.getSeconds()).slice(-2);
        }

        // face
        var dcls = decisionClass(stock);
        card.classList.toggle("dc-card--go", dcls === "GO");
        card.classList.toggle("dc-card--out", dcls === "OUT");
        var sym = card.querySelector(".dc-symbol"); sym.textContent = stock.symbol;
        var dir = card.querySelector(".dc-dir");
        dir.textContent = stock.direction;
        dir.className = "dc-dir " + (stock.direction === "LONG" ? "dc-dir--long" : "dc-dir--short");
        var rs = card.querySelector(".dc-rs");
        var rsv = stock.rs_pct;
        rs.textContent = rsv == null ? "" : "RS " + (rsv > 0 ? "+" : "") + Number(rsv).toFixed(2) + "%";
        rs.className = "dc-rs " + (Number(rsv) >= 0 ? "dc-rs--pos" : "dc-rs--neg");
        var score = card.querySelector(".dc-score");
        if (stock.dashboard_score != null) {
            score.textContent = "Score " + stock.dashboard_score;
            score.className = "dc-score " + scoreClass(stock.dashboard_score);
            score.style.display = "";
        } else { score.style.display = "none"; }
        var conf = card.querySelector(".dc-conf");
        conf.textContent = stock.confidence ? "Conf " + stock.confidence : (stock.dashboard_kavach || "");
        var volx = card.querySelector(".dc-volx");
        volx.textContent = stock.vol_multiplier != null ? Number(stock.vol_multiplier).toFixed(2) + "×" : "";
        var dec = card.querySelector(".dc-decision");
        dec.textContent = stock.decision || "⬜ Not assessed";
        dec.className = "dc-decision dc-decision--" + dcls;
    }

    function bucketOf(stock) {
        var d = decisionClass(stock);
        if (d === "GO") return "Go";
        if (d === "OUT") return "Out";
        return "Watch";
    }

    function render() {
        if (!state || !state.stocks) return;
        $("dcDate").textContent = fmtDate(state.session_date);
        var nifty = "";
        if (state.nifty50 != null) nifty += "NIFTY <b>" + state.nifty50 + "</b>";
        if (state.banknifty != null) nifty += (nifty ? " · " : "") + "BANKNIFTY <b>" + state.banknifty + "</b>";
        $("dcNifty").innerHTML = nifty;
        var c = state.counts || { go: 0, watch: 0, out: 0 };
        $("dcPillGo").textContent = "🟢 " + c.go + " GO";
        $("dcPillWatch").textContent = "🟡 " + c.watch + " WATCH";
        $("dcPillOut").textContent = "🔴 " + c.out + " OUT";

        // nifty direction select + gap warning
        var sel = $("dcNiftyDir");
        if (document.activeElement !== sel) sel.value = state.nifty_open_direction || "";
        $("dcGapWarn").classList.toggle("show", (state.nifty_open_direction || "") === "Gap reversed");

        var empty = state.stocks.length === 0;
        $("dcEmpty").style.display = empty ? "" : "none";

        var grids = { Go: $("dcGoGrid"), Watch: $("dcWatchGrid"), Out: $("dcOutGrid") };
        var secs = { Go: $("dcGoSection"), Watch: $("dcWatchSection"), Out: $("dcOutSection") };
        var counts = { Go: 0, Watch: 0, Out: 0 };

        // sort: bullish first already from server; within, keep order
        state.stocks.forEach(function (stock) {
            var card = ensureCard(stock.symbol);
            patchCard(card, stock);
            var b = bucketOf(stock);
            counts[b]++;
            if (card.parentNode !== grids[b]) grids[b].appendChild(card);
        });
        ["Go", "Watch", "Out"].forEach(function (b) {
            secs[b].hidden = counts[b] === 0;
        });
    }

    function applyState(s) {
        if (!s || s.error) { if (s && s.error) toast("Error: " + s.error); if (!s) return; }
        state = s;
        try { localStorage.setItem(lsKey(), JSON.stringify(s)); } catch (e) {}
        render();
    }

    // ---- updates ----
    function onChange(symbol, field, value) {
        // optimistic local update so the UI feels instant before the round-trip
        var stock = currentStock(symbol);
        if (stock) {
            if (field === "counter_rs") stock[field] = !!value;
            else if (field === "news_clean") stock[field] = value === "" ? null : (value === "true" || value === true);
            else stock[field] = value === "" ? null : value;
            var card = cardEls[symbol];
            if (card && field !== "nifty_open_direction") {
                setOptionColors(card, field, stock);
            }
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
        toast("Pulling RS scanner…");
        api("/populate", { method: "POST" }).then(applyState).catch(function () { toast("Pull failed"); });
    }

    function resetDay() {
        if (!confirm("Reset today's checklist? Saved values will be cleared (history is kept).")) return;
        api("/reset", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" })
            .then(function (s) { cardEls = {}; ["dcGoGrid","dcWatchGrid","dcOutGrid"].forEach(function(id){$(id).innerHTML="";}); applyState(s); toast("Day reset"); });
    }

    // ---- clock / entry window ----
    function tickClock() {
        var t = nowIST();
        $("dcClock").textContent = t.str + " IST";
        var w = $("dcWindow");
        var start = 10 * 60 + 15, end = 14 * 60 + 30;
        if (t.minutes < start) {
            var rem = (start * 60) - t.secs;
            var mm = Math.floor(rem / 60), ss = rem % 60;
            w.textContent = "Entry opens in " + ("0" + mm).slice(-2) + ":" + ("0" + ss).slice(-2);
            w.className = "dc-window pre";
        } else if (t.minutes <= end) {
            w.textContent = "Entry window open";
            w.className = "dc-window open";
        } else {
            w.textContent = "Entry window closed";
            w.className = "dc-window closed";
        }
        // refresh recheck banners' flash state without a server round-trip
        if (state && state.stocks) {
            state.stocks.forEach(function (stock) {
                if (stock.adx_935_status === "recheck" && cardEls[stock.symbol]) {
                    patchRecheckOnly(cardEls[stock.symbol], stock, t);
                }
            });
        }
    }
    function patchRecheckOnly(card, stock, t) {
        var rc = card.querySelector('[data-recheck="1"]');
        if (!rc) return;
        rc.classList.add("show");
        if (t.minutes >= 600) {
            rc.classList.add("flash");
            rc.textContent = "⏰ Now is 10:00 AM — recheck ADX for this stock";
        }
    }

    // ---- boot ----
    function boot() {
        // instant render from localStorage, then refresh from server
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
        $("dcPrint").addEventListener("click", function () {
            // expand all so the print view shows full checklists
            Object.keys(cardEls).forEach(function (s) { cardEls[s].classList.add("open"); });
            setTimeout(function () { window.print(); }, 50);
        });
        $("dcSave").addEventListener("click", function () {
            try { localStorage.setItem(lsKey(), JSON.stringify(state)); } catch (e) {}
            toast("Session saved");
        });

        api("/data").then(applyState).catch(function () { /* keep cached */ });
        tickClock();
        setInterval(tickClock, 1000);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else { boot(); }
})();

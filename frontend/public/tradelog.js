(function () {
    "use strict";

    function apiBase() {
        var h = window.location.hostname;
        if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
        return window.location.origin;
    }

    function headers() {
        var t = localStorage.getItem("trademanthan_token") || "";
        var h = { "Content-Type": "application/json" };
        if (t) h.Authorization = "Bearer " + t;
        return h;
    }

    function $(id) { return document.getElementById(id); }

    function showTab(name) {
        var tabs = document.querySelectorAll(".tl-tab[data-tab]");
        Array.prototype.forEach.call(tabs, function (btn) {
            var on = btn.getAttribute("data-tab") === name;
            btn.setAttribute("aria-selected", on ? "true" : "false");
            var pane = document.getElementById(btn.getAttribute("aria-controls"));
            if (pane) pane.hidden = !on;
        });
    }

    function setStatus(msg, ok) {
        var el = $("tlStatus");
        el.textContent = msg || "";
        el.className = "tl-status " + (ok === true ? "ok" : ok === false ? "err" : "");
    }

    function errDetail(data) {
        var d = data && data.detail;
        if (typeof d === "string") return d;
        if (Array.isArray(d) && d.length) {
            return d.map(function (x) { return (x && x.msg) || JSON.stringify(x); }).join("; ");
        }
        if (d && typeof d === "object" && d.msg) return d.msg;
        return "Save failed";
    }

    function clockForInput(v) {
        if (v == null || v === "") return "";
        var s = String(v).trim();
        if (s.indexOf("T") >= 0) s = s.split("T").pop();
        s = s.split(".")[0].split("+")[0].replace("Z", "");
        var m = s.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
        if (!m) return "";
        var hh = ("0" + m[1]).slice(-2);
        var mm = ("0" + m[2]).slice(-2);
        var ss = m[3] != null ? ("0" + m[3]).slice(-2) : "00";
        return hh + ":" + mm + ":" + ss;
    }

    function fillForm(row) {
        $("tlEditId").value = row && row.id ? String(row.id) : "";
        $("tlDate").value = (row.session_date || "").slice(0, 10);
        $("tlSymbol").value = row.symbol || "";
        $("tlDir").value = (row.direction || "LONG").toUpperCase();
        $("tlEntryTime").value = clockForInput(row.entry_time);
        $("tlEntryPx").value = row.entry_price != null ? row.entry_price : "";
        $("tlExitTime").value = clockForInput(row.exit_time);
        $("tlExitPx").value = row.exit_price != null ? row.exit_price : "";
        $("tlQty").value = row.qty != null ? row.qty : "";
        $("tlSlip").value = row.slippage_pts != null ? row.slippage_pts : "";
        $("tlIntended").value = row.exit_price_intended != null ? row.exit_price_intended : "";
        $("tlExitType").value = row.exit_trigger_type || "rule_compliant";
        $("tlExitTrig").value = row.exit_trigger || "";
        $("tlNotes").value = row.notes || "";
    }

    function formPayload() {
        var qty = $("tlQty").value;
        var entryPx = $("tlEntryPx").value;
        var exitPx = $("tlExitPx").value;
        return {
            session_date: $("tlDate").value,
            symbol: $("tlSymbol").value.trim(),
            direction: $("tlDir").value,
            entry_time: clockForInput($("tlEntryTime").value) || $("tlEntryTime").value,
            entry_price: entryPx === "" ? null : Number(entryPx),
            exit_time: $("tlExitTime").value ? (clockForInput($("tlExitTime").value) || $("tlExitTime").value) : null,
            exit_price: exitPx === "" ? null : Number(exitPx),
            qty: qty === "" ? null : Number(qty),
            slippage_pts: $("tlSlip").value === "" ? null : Number($("tlSlip").value),
            exit_price_intended: $("tlIntended").value === "" ? null : Number($("tlIntended").value),
            exit_trigger_type: $("tlExitType").value,
            exit_trigger: $("tlExitTrig").value || null,
            notes: $("tlNotes").value || ""
        };
    }

    async function parsePaste() {
        var text = $("tlPaste").value.trim();
        if (text.length < 10) {
            setStatus("Paste a journal note first.", false);
            return null;
        }
        var res = await fetch(apiBase() + "/api/trade-log/parse", {
            method: "POST",
            headers: headers(),
            body: JSON.stringify({ text: text })
        });
        var data = await res.json();
        if (!res.ok) {
            setStatus(errDetail(data) || "Parse failed", false);
            return null;
        }
        var p = data.parsed || {};
        $("tlPreview").hidden = false;
        $("tlPreview").textContent = JSON.stringify(p, null, 2);
        // Keep edit id empty for a new paste save.
        fillForm(Object.assign({ notes: text, id: null }, p));
        if (p.parse_warnings && p.parse_warnings.length) {
            setStatus("Parsed with warnings: " + p.parse_warnings.join("; "), false);
        } else {
            setStatus("Parsed. Review the form, then save.", true);
        }
        return p;
    }

    async function saveForm() {
        var editId = $("tlEditId").value;
        var payload = formPayload();
        if (!payload.session_date || !payload.symbol || !payload.entry_time || payload.entry_price == null) {
            setStatus("Date, symbol, entry time, and entry price are required.", false);
            return;
        }
        var url = apiBase() + "/api/trade-log";
        var method = "POST";
        var body = payload;
        if (editId) {
            url += "/" + editId;
            method = "PATCH";
            // Send the full form so date/symbol/side/qty edits also persist.
            body = payload;
        }
        var res = await fetch(url, { method: method, headers: headers(), body: JSON.stringify(body) });
        var data = await res.json().catch(function () { return {}; });
        if (!res.ok) {
            setStatus(errDetail(data), false);
            return;
        }
        var t = data.trade || {};
        var pnl = t.gross_pnl_inr;
        setStatus(
            "Saved #" + (t.id || editId) + " " + t.symbol + " " + t.direction +
            (pnl != null ? (" · gross ₹" + pnl) : ""),
            true
        );
        $("tlEditId").value = String(t.id || editId || "");
        if (t && t.id) fillForm(t);
        loadList();
    }

    function clearForm() {
        $("tlEditId").value = "";
        fillForm({
            session_date: $("tlDate").value,
            direction: "LONG",
            exit_trigger_type: "rule_compliant"
        });
        setStatus("Form cleared for a new trade.", true);
    }

    function pnlClass(v) {
        if (v == null) return "";
        return Number(v) >= 0 ? "tl-pnl-pos" : "tl-pnl-neg";
    }

    async function loadList() {
        var from = $("tlFrom").value;
        var to = $("tlTo").value;
        var q = [];
        if (from) q.push("start_date=" + encodeURIComponent(from));
        if (to) q.push("end_date=" + encodeURIComponent(to));
        var res = await fetch(apiBase() + "/api/trade-log" + (q.length ? "?" + q.join("&") : ""), {
            headers: headers()
        });
        var data = await res.json();
        var tb = $("tlTable").querySelector("tbody");
        tb.innerHTML = "";
        (data.trades || []).forEach(function (row) {
            var tr = document.createElement("tr");
            tr.dataset.id = String(row.id);
            var pnl = row.gross_pnl_inr;
            tr.innerHTML =
                "<td>" + (row.session_date || "").slice(0, 10) + "</td>" +
                "<td>" + (row.symbol || "") + "</td>" +
                "<td>" + (row.direction || "") + "</td>" +
                "<td>" + (row.qty != null ? row.qty : "") + "</td>" +
                "<td>" + (row.entry_price != null ? row.entry_price : "") + " @ " + (row.entry_time || "") + "</td>" +
                "<td>" + (row.exit_price != null ? row.exit_price : "") + " @ " + (row.exit_time || "") + "</td>" +
                "<td class='" + pnlClass(pnl) + "'>" + (pnl != null ? "₹" + pnl : "") + "</td>" +
                "<td>" + (row.exit_trigger_type || "") + "</td>";
            tr.onclick = function () {
                Array.prototype.forEach.call(tb.querySelectorAll("tr"), function (x) { x.classList.remove("active"); });
                tr.classList.add("active");
                fillForm(row);
                showTab("form");
                setStatus("Editing trade #" + row.id + ". Change fields, then Save.", true);
                var focusEl = $("tlEntryTime");
                if (focusEl && focusEl.focus) focusEl.focus();
            };
            tb.appendChild(tr);
        });
        if (!(data.trades || []).length) {
            var empty = document.createElement("tr");
            empty.innerHTML = "<td colspan='8'>No trades in this range.</td>";
            tb.appendChild(empty);
        }
    }

    function todayISO() {
        var n = new Date();
        var ist = new Date(n.getTime() + (5.5 * 60 - n.getTimezoneOffset()) * 60000);
        return ist.toISOString().slice(0, 10);
    }

    document.addEventListener("DOMContentLoaded", function () {
        var today = todayISO();
        $("tlDate").value = today;
        $("tlFrom").value = today;
        $("tlTo").value = today;
        document.querySelectorAll(".tl-tab[data-tab]").forEach(function (btn) {
            btn.onclick = function () { showTab(btn.getAttribute("data-tab")); };
        });
        $("tlParseBtn").onclick = function () { parsePaste().catch(function (e) { setStatus(String(e), false); }); };
        $("tlSavePasteBtn").onclick = function () {
            parsePaste().then(function (p) {
                if (!p) return;
                if (p.master_ok === false) {
                    setStatus("Cannot save: symbol not found in arbitrage_master. Fix symbol then Parse again.", false);
                    return;
                }
                if (p.parse_warnings && p.parse_warnings.length) {
                    setStatus("Cannot save until required fields parse cleanly: " + p.parse_warnings.join("; "), false);
                    return;
                }
                showTab("form");
                return saveForm();
            }).catch(function (e) { setStatus(String(e), false); });
        };
        $("tlSaveFormBtn").onclick = function () { saveForm().catch(function (e) { setStatus(String(e), false); }); };
        $("tlClearFormBtn").onclick = clearForm;
        $("tlLoadBtn").onclick = function () { loadList().catch(function (e) { setStatus(String(e), false); }); };
        loadList().catch(function () {});
    });
})();

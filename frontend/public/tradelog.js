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

    function setStatus(msg, ok) {
        var el = $("tlStatus");
        el.textContent = msg || "";
        el.className = "tl-status " + (ok === true ? "ok" : ok === false ? "err" : "");
    }

    function clockForInput(v) {
        if (!v) return "";
        var s = String(v);
        if (s.length >= 8) return s.slice(0, 8);
        if (s.length === 5) return s + ":00";
        return s;
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
        return {
            session_date: $("tlDate").value,
            symbol: $("tlSymbol").value.trim(),
            direction: $("tlDir").value,
            entry_time: $("tlEntryTime").value,
            entry_price: Number($("tlEntryPx").value),
            exit_time: $("tlExitTime").value || null,
            exit_price: $("tlExitPx").value === "" ? null : Number($("tlExitPx").value),
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
            setStatus(data.detail || "Parse failed", false);
            return null;
        }
        var p = data.parsed || {};
        $("tlPreview").hidden = false;
        $("tlPreview").textContent = JSON.stringify(p, null, 2);
        fillForm(Object.assign({ notes: text }, p));
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
        if (!payload.session_date || !payload.symbol || !payload.entry_time || !payload.entry_price) {
            setStatus("Date, symbol, entry time, and entry price are required.", false);
            return;
        }
        var url = apiBase() + "/api/trade-log";
        var method = "POST";
        var body = payload;
        if (editId) {
            url += "/" + editId;
            method = "PATCH";
            body = {
                entry_time: payload.entry_time,
                entry_price: payload.entry_price,
                exit_time: payload.exit_time,
                exit_price: payload.exit_price,
                slippage_pts: payload.slippage_pts,
                exit_price_intended: payload.exit_price_intended,
                exit_trigger_type: payload.exit_trigger_type,
                exit_trigger: payload.exit_trigger,
                notes: payload.notes
            };
        }
        var res = await fetch(url, { method: method, headers: headers(), body: JSON.stringify(body) });
        var data = await res.json().catch(function () { return {}; });
        if (!res.ok) {
            var d = data.detail;
            setStatus(typeof d === "string" ? d : (d && d[0] && d[0].msg) || "Save failed", false);
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
                setStatus("Editing trade #" + row.id + ". Change times/prices/notes/exit type, then Save.", true);
                window.scrollTo({ top: $("tlDate").getBoundingClientRect().top + window.scrollY - 80, behavior: "smooth" });
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
        $("tlParseBtn").onclick = function () { parsePaste().catch(function (e) { setStatus(String(e), false); }); };
        $("tlSavePasteBtn").onclick = function () {
            parsePaste().then(function (p) {
                if (p && p.master_ok !== false) return saveForm();
            }).catch(function (e) { setStatus(String(e), false); });
        };
        $("tlSaveFormBtn").onclick = function () { saveForm().catch(function (e) { setStatus(String(e), false); }); };
        $("tlClearFormBtn").onclick = clearForm;
        $("tlLoadBtn").onclick = function () { loadList().catch(function (e) { setStatus(String(e), false); }); };
        loadList().catch(function () {});
    });
})();

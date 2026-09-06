(function () {
    const apiBase = (function () {
        const h = window.location.hostname;
        if (h === "localhost" || h === "127.0.0.1") return "http://localhost:8000";
        return window.location.origin;
    })();

    let rows = [];
    let sortKey = "symbol";
    let sortDir = 1;

    const el = (id) => document.getElementById(id);

    function chipHtml(v) {
        if (v === "Bullish" || v === "OverSold") {
            return `<span class="an-chip an-chip-blue">${esc(v)}</span>`;
        }
        if (v === "Bearish" || v === "OverBought") {
            return `<span class="an-chip an-chip-red">${esc(v)}</span>`;
        }
        return esc(v || "—");
    }
    function actionHtml(v) {
        const a = v || "--";
        if (a === "BUY") return `<span class="an-chip an-act an-act-buy">BUY</span>`;
        if (a === "SELL") return `<span class="an-chip an-act an-act-sell">SELL</span>`;
        if (a === "SELL (Exhaustion)") return `<span class="an-chip an-act an-act-sell">SELL (Exhaustion)</span>`;
        if (a === "WATCH") return `<span class="an-chip an-act an-act-watch">WATCH</span>`;
        return `<span class="an-act-none">--</span>`;
    }
    function rsClass(v) {
        if (v === "Above") return "an-above";
        if (v === "Below") return "an-below";
        return "";
    }
    function esc(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;");
    }

    function unique(field) {
        const s = new Set();
        rows.forEach((r) => {
            const v = r[field];
            if (Array.isArray(v)) v.forEach((x) => x && s.add(x));
            else if (v) s.add(v);
        });
        return [...s].sort();
    }

    function fillSelect(sel, values) {
        const keep = sel.value;
        const opts = ['<option value="">All</option>'].concat(
            values.map((v) => `<option value="${esc(v)}">${esc(v)}</option>`)
        );
        sel.innerHTML = opts.join("");
        if ([...sel.options].some((o) => o.value === keep)) sel.value = keep;
    }

    function filtered() {
        const sec = el("anSector").value;
        const act = el("anAction").value;
        const wt = el("anWTrend").value;
        const dt = el("anDTrend").value;
        const rsi = el("anRsi").value;
        const rs = el("anRs").value;
        const pat = el("anPat").value;
        const q = (el("anSearch").value || "").trim().toUpperCase();
        return rows.filter((r) => {
            if (sec && r.sector !== sec) return false;
            if (act && (r.action || "--") !== act) return false;
            if (wt && r.weekly_trend !== wt) return false;
            if (dt && r.daily_trend !== dt) return false;
            if (rsi && r.weekly_rsi_zone !== rsi) return false;
            if (rs && r.rs_ma50 !== rs) return false;
            if (pat && !(r.patterns || []).includes(pat)) return false;
            if (q && !(r.symbol || "").toUpperCase().includes(q)) return false;
            return true;
        });
    }

    function cmp(a, b, k) {
        const av = a[k], bv = b[k];
        if (k === "patterns") {
            const as = (av || []).join(","), bs = (bv || []).join(",");
            return as.localeCompare(bs) * sortDir;
        }
        const as = av == null ? "" : String(av);
        const bs = bv == null ? "" : String(bv);
        return as.localeCompare(bs) * sortDir;
    }

    function render() {
        const data = filtered().slice().sort((a, b) => cmp(a, b, sortKey));
        el("anVisible").textContent = String(data.length);
        const body = el("anBody");
        if (!data.length) {
            body.innerHTML = '<tr><td colspan="8" class="vmb-empty">No rows match filters.</td></tr>';
            return;
        }
        body.innerHTML = data.map((r) => {
            const pats = (r.patterns || []).join(", ") || "—";
            return `<tr>
                <td>${esc(r.symbol)}</td>
                <td>${esc(r.sector || "")}</td>
                <td>${actionHtml(r.action)}</td>
                <td>${chipHtml(r.weekly_trend)}</td>
                <td>${chipHtml(r.daily_trend)}</td>
                <td>${chipHtml(r.weekly_rsi_zone)}</td>
                <td class="${rsClass(r.rs_ma50)}">${esc(r.rs_ma50 || "—")}</td>
                <td class="an-pats">${esc(pats)}</td>
            </tr>`;
        }).join("");
    }

    async function load() {
        el("anBody").innerHTML = '<tr><td colspan="8" class="vmb-empty">Loading…</td></tr>';
        try {
            let res = await fetch(apiBase + "/api/analysis/symbols", { cache: "no-store" });
            if (!res.ok) res = await fetch(apiBase + "/scan/analysis", { cache: "no-store" });
            const doc = await res.json();
            rows = Array.isArray(doc.symbols) ? doc.symbols : [];
            el("anCount").textContent = String(doc.count != null ? doc.count : rows.length);
            el("anUpdated").textContent = doc.updated_at ? String(doc.updated_at).replace("T", " ").slice(0, 19) : "—";
            el("anFooter").textContent = rows.length ? "Source: analysis_symbol_snapshot (equity daily/weekly)." : "No snapshot yet. Run scripts/run_analysis_snapshot.py after market.";
            fillSelect(el("anSector"), unique("sector"));
            const pats = new Set();
            rows.forEach((r) => (r.patterns || []).forEach((p) => pats.add(p)));
            fillSelect(el("anPat"), [...pats].sort());
            render();
        } catch (e) {
            el("anBody").innerHTML = '<tr><td colspan="8" class="vmb-empty">Failed to load analysis.</td></tr>';
            el("anFooter").textContent = String(e);
        }
    }

    document.querySelectorAll("#anTable thead th[data-k]").forEach((th) => {
        th.addEventListener("click", () => {
            const k = th.getAttribute("data-k");
            if (sortKey === k) sortDir *= -1;
            else { sortKey = k; sortDir = 1; }
            document.querySelectorAll("#anTable thead th").forEach((x) => x.classList.remove("sort-asc", "sort-desc"));
            th.classList.add(sortDir > 0 ? "sort-asc" : "sort-desc");
            render();
        });
    });
    ["anSector", "anAction", "anWTrend", "anDTrend", "anRsi", "anRs", "anPat"].forEach((id) => {
        el(id).addEventListener("change", render);
    });
    el("anSearch").addEventListener("input", render);
    el("anReloadBtn").addEventListener("click", load);
    load();
})();

(function () {
    const bullishBody = document.getElementById("bullishBody");
    const bearishBody = document.getElementById("bearishBody");
    const bullishSummary = document.getElementById("bullishSummary");
    const bearishSummary = document.getElementById("bearishSummary");
    const refreshBtn = document.getElementById("refreshBtn");
    const API = "/scan/arbitrage/pivot-breakout";
    const STREAM_API = "/scan/arbitrage/pivot-breakout-stream";

    let bullishData = [];
    let bearishData = [];
    let bullishSortDir = "asc";
    let bearishSortDir = "asc";

    function fmt(v) {
        if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
        return Number(v).toFixed(2);
    }

    function toDdMmYy(isoDate) {
        if (!isoDate || typeof isoDate !== "string") return "";
        const m = isoDate.match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (!m) return "";
        return `${m[3]}/${m[2]}/${m[1].slice(-2)}`;
    }

    function updateHeaderDates(data) {
        const ltpDateFmt = toDdMmYy(data.ltp_date || "");
        const pivotDateFmt = toDdMmYy(data.pivot_date || "");
        ["bullishLtpDate", "bearishLtpDate"].forEach((id) => {
            const el = document.getElementById(id);
            if (el) el.textContent = ltpDateFmt || "";
        });
        const r3El = document.getElementById("bullishR3Date");
        if (r3El) r3El.textContent = pivotDateFmt || "";
        const s3El = document.getElementById("bearishS3Date");
        if (s3El) s3El.textContent = pivotDateFmt || "";
    }

    function fmtPct(v) {
        if (v === null || v === undefined || Number.isNaN(Number(v))) return "-";
        return `${Number(v).toFixed(2)}%`;
    }

    function rowHtml(r, pivotKey, opts = {}) {
        const cells =
            `<td>${r.stock || "-"}</td>` +
            `<td>${r.currmth_future_symbol || "-"}</td>` +
            `<td class="num">${fmt(r.currmth_future_ltp)}</td>` +
            `<td class="num">${fmt(r[pivotKey])}</td>`;
        if (opts.showPct && pivotKey === "r3_pivot") {
            const pct = r.difference_from_r3_pct;
            return `<tr>${cells}<td class="num">${fmtPct(pct)}</td></tr>`;
        }
        if (opts.showPct && pivotKey === "s3_pivot") {
            const pct = r.difference_from_s3_pct;
            return `<tr>${cells}<td class="num">${fmtPct(pct)}</td></tr>`;
        }
        return `<tr>${cells}</tr>`;
    }

    function renderRows(tbody, rows, pivotKey, opts = {}) {
        const colspan = opts.colspan || (opts.showPct ? 5 : 4);
        if (!rows || rows.length === 0) {
            tbody.innerHTML = `<tr><td colspan="${colspan}" class="state-cell">No records found.</td></tr>`;
            return;
        }
        tbody.innerHTML = rows.map((r) => rowHtml(r, pivotKey, opts)).join("");
    }

    function applyBullishSort(usePct) {
        if (!bullishData || bullishData.length === 0) {
            renderRows(bullishBody, [], "r3_pivot", { showPct: true });
            return;
        }
        bullishData.sort((a, b) => {
            const aVal = usePct ? (a.difference_from_r3_pct ?? 1e9) : (a.difference_from_r3 ?? 1e9);
            const bVal = usePct ? (b.difference_from_r3_pct ?? 1e9) : (b.difference_from_r3 ?? 1e9);
            const cmp = aVal - bVal || (a.stock || "").localeCompare(b.stock || "");
            return bullishSortDir === "asc" ? cmp : -cmp;
        });
        renderRows(bullishBody, bullishData, "r3_pivot", { showPct: true });
    }

    function applyBearishSort(usePct) {
        if (!bearishData || bearishData.length === 0) {
            renderRows(bearishBody, [], "s3_pivot", { showPct: true });
            return;
        }
        bearishData.sort((a, b) => {
            const aVal = usePct ? (a.difference_from_s3_pct ?? 1e9) : (a.difference_from_s3 ?? 1e9);
            const bVal = usePct ? (b.difference_from_s3_pct ?? 1e9) : (b.difference_from_s3 ?? 1e9);
            const cmp = aVal - bVal || (a.stock || "").localeCompare(b.stock || "");
            return bearishSortDir === "asc" ? cmp : -cmp;
        });
        renderRows(bearishBody, bearishData, "s3_pivot", { showPct: true });
    }

    function updateSortIndicators() {
        const bullishEl = document.getElementById("bullishPctSort");
        const bearishEl = document.getElementById("bearishPctSort");
        if (bullishEl) bullishEl.textContent = bullishData.length ? (bullishSortDir === "asc" ? "▲" : "▼") : "";
        if (bearishEl) bearishEl.textContent = bearishData.length ? (bearishSortDir === "asc" ? "▲" : "▼") : "";
    }

    async function loadDataStream() {
        bullishBody.innerHTML = '<tr><td colspan="5" class="state-cell">Loading...</td></tr>';
        bearishBody.innerHTML = '<tr><td colspan="5" class="state-cell">Loading...</td></tr>';
        bullishSummary.textContent = "Loading...";
        bearishSummary.textContent = "Loading...";

        bullishData = [];
        bearishData = [];

        const ohlcInterval = (document.getElementById("ohlcInterval") || {}).value || "daily";
        const thresholdPct = (document.getElementById("thresholdPct") || {}).value || "5";
        const params = new URLSearchParams({
            ohlc_interval: ohlcInterval,
            threshold_pct: thresholdPct,
        });
        const streamUrl = `${STREAM_API}?${params.toString()}`;
        try {
            const res = await fetch(streamUrl, { cache: "no-store" });
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buffer = "";

            while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n");
                buffer = lines.pop() || "";
                for (const line of lines) {
                    if (!line.trim()) continue;
                    let data;
                    try {
                        data = JSON.parse(line);
                    } catch (_) {
                        continue;
                    }
                    if (data.error) {
                        throw new Error(data.error);
                    }
                    if (data.done) {
                        updateHeaderDates(data);
                        bullishSummary.textContent = `Total bullish records: ${data.bullish_count || 0}`;
                        bearishSummary.textContent = `Total bearish records: ${data.bearish_count || 0}`;
                        continue;
                    }
                    if (data.bullish && data.bullish.length > 0) {
                        data.bullish.forEach((r) => bullishData.push(r));
                        applyBullishSort(false);
                        bullishSummary.textContent = `Bullish: ${bullishData.length} (loading...)`;
                    }
                    if (data.bearish && data.bearish.length > 0) {
                        data.bearish.forEach((r) => bearishData.push(r));
                        applyBearishSort(false);
                        bearishSummary.textContent = `Bearish: ${bearishData.length} (loading...)`;
                    }
                }
            }
            if (buffer.trim()) {
                try {
                    const data = JSON.parse(buffer);
                    if (data.done) {
                        updateHeaderDates(data);
                        bullishSummary.textContent = `Total bullish records: ${data.bullish_count || 0}`;
                        bearishSummary.textContent = `Total bearish records: ${data.bearish_count || 0}`;
                    }
                } catch (_) {}
            }
            if (bullishData.length === 0 && bearishData.length === 0) {
                bullishBody.innerHTML = '<tr><td colspan="5" class="state-cell">No records found.</td></tr>';
                bearishBody.innerHTML = '<tr><td colspan="5" class="state-cell">No records found.</td></tr>';
            }
            updateSortIndicators();
        } catch (err) {
            bullishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bearishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bullishSummary.textContent = "Failed to load data.";
            bearishSummary.textContent = "Failed to load data.";
        }
    }

    async function loadData() {
        bullishBody.innerHTML = '<tr><td colspan="5" class="state-cell">Loading...</td></tr>';
        bearishBody.innerHTML = '<tr><td colspan="5" class="state-cell">Loading...</td></tr>';
        bullishSummary.textContent = "Loading data...";
        bearishSummary.textContent = "Loading data...";

        try {
            const ohlcInterval = (document.getElementById("ohlcInterval") || {}).value || "daily";
            const thresholdPct = (document.getElementById("thresholdPct") || {}).value || "5";
            const params = new URLSearchParams({
                ohlc_interval: ohlcInterval,
                threshold_pct: thresholdPct,
            });
            const res = await fetch(`${API}?${params.toString()}`, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || "Failed to fetch pivot breakout data");
            }
            updateHeaderDates(data);
            bullishData = data.bullish || [];
            bearishData = data.bearish || [];
            applyBullishSort(false);
            applyBearishSort(false);
            bullishSummary.textContent = `Total bullish records: ${data.bullish_count || 0}`;
            bearishSummary.textContent = `Total bearish records: ${data.bearish_count || 0}`;
            updateSortIndicators();
        } catch (err) {
            bullishBody.innerHTML = `<tr><td colspan="5" class="state-cell">Error: ${err.message}</td></tr>`;
            bearishBody.innerHTML = `<tr><td colspan="5" class="state-cell">Error: ${err.message}</td></tr>`;
            bullishSummary.textContent = "Failed to load data.";
            bearishSummary.textContent = "Failed to load data.";
        }
    }

    refreshBtn.addEventListener("click", () => loadDataStream());
    const ohlcIntervalEl = document.getElementById("ohlcInterval");
    if (ohlcIntervalEl) ohlcIntervalEl.addEventListener("change", () => loadDataStream());
    const thresholdEl = document.getElementById("thresholdPct");
    if (thresholdEl) thresholdEl.addEventListener("change", () => loadDataStream());
    const bullishPctHeader = document.getElementById("bullishPctHeader");
    if (bullishPctHeader) {
        bullishPctHeader.addEventListener("click", () => {
            bullishSortDir = bullishSortDir === "asc" ? "desc" : "asc";
            applyBullishSort(true);
            updateSortIndicators();
        });
    }
    const bearishPctHeader = document.getElementById("bearishPctHeader");
    if (bearishPctHeader) {
        bearishPctHeader.addEventListener("click", () => {
            bearishSortDir = bearishSortDir === "asc" ? "desc" : "asc";
            applyBearishSort(true);
            updateSortIndicators();
        });
    }
    document.addEventListener("DOMContentLoaded", () => {
        document.title = "Pivot Breakout - Tradentical";
        loadDataStream();
    });
})();

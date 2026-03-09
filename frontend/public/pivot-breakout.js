(function () {
    const bullishBody = document.getElementById("bullishBody");
    const bearishBody = document.getElementById("bearishBody");
    const bullishSummary = document.getElementById("bullishSummary");
    const bearishSummary = document.getElementById("bearishSummary");
    const refreshBtn = document.getElementById("refreshBtn");
    const API = "/scan/arbitrage/pivot-breakout";
    const STREAM_API = "/scan/arbitrage/pivot-breakout-stream";

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

    function rowHtml(r, pivotKey) {
        return `<tr><td>${r.stock || "-"}</td><td>${r.currmth_future_symbol || "-"}</td><td class="num">${fmt(r.currmth_future_ltp)}</td><td class="num">${fmt(r[pivotKey])}</td></tr>`;
    }

    function renderRows(tbody, rows, pivotKey) {
        if (!rows || rows.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="state-cell">No records found.</td></tr>';
            return;
        }
        tbody.innerHTML = rows.map((r) => rowHtml(r, pivotKey)).join("");
    }

    async function loadDataStream() {
        bullishBody.innerHTML = '<tr><td colspan="4" class="state-cell">Loading...</td></tr>';
        bearishBody.innerHTML = '<tr><td colspan="4" class="state-cell">Loading...</td></tr>';
        bullishSummary.textContent = "Loading...";
        bearishSummary.textContent = "Loading...";

        const allBullish = [];
        const allBearish = [];

        const ohlcInterval = (document.getElementById("ohlcInterval") || {}).value || "daily";
        const streamUrl = `${STREAM_API}?ohlc_interval=${encodeURIComponent(ohlcInterval)}`;
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
                        data.bullish.forEach((r) => allBullish.push(r));
                        allBullish.sort((a, b) => (a.difference_from_r3 ?? 1e9) - (b.difference_from_r3 ?? 1e9) || (a.stock || "").localeCompare(b.stock || ""));
                        bullishBody.innerHTML = allBullish.map((r) => rowHtml(r, "r3_pivot")).join("");
                        bullishSummary.textContent = `Bullish: ${allBullish.length} (loading...)`;
                    }
                    if (data.bearish && data.bearish.length > 0) {
                        data.bearish.forEach((r) => allBearish.push(r));
                        allBearish.sort((a, b) => (a.difference_from_s3 ?? 1e9) - (b.difference_from_s3 ?? 1e9) || (a.stock || "").localeCompare(b.stock || ""));
                        bearishBody.innerHTML = allBearish.map((r) => rowHtml(r, "s3_pivot")).join("");
                        bearishSummary.textContent = `Bearish: ${allBearish.length} (loading...)`;
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
            if (allBullish.length === 0 && allBearish.length === 0) {
                bullishBody.innerHTML = '<tr><td colspan="4" class="state-cell">No records found.</td></tr>';
                bearishBody.innerHTML = '<tr><td colspan="4" class="state-cell">No records found.</td></tr>';
            }
        } catch (err) {
            bullishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bearishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bullishSummary.textContent = "Failed to load data.";
            bearishSummary.textContent = "Failed to load data.";
        }
    }

    async function loadData() {
        bullishBody.innerHTML = '<tr><td colspan="4" class="state-cell">Loading...</td></tr>';
        bearishBody.innerHTML = '<tr><td colspan="4" class="state-cell">Loading...</td></tr>';
        bullishSummary.textContent = "Loading data...";
        bearishSummary.textContent = "Loading data...";

        try {
            const res = await fetch(API, { cache: "no-store" });
            const data = await res.json();
            if (!res.ok || !data.success) {
                throw new Error(data.detail || "Failed to fetch pivot breakout data");
            }
            updateHeaderDates(data);
            renderRows(bullishBody, data.bullish || [], "r3_pivot");
            renderRows(bearishBody, data.bearish || [], "s3_pivot");
            bullishSummary.textContent = `Total bullish records: ${data.bullish_count || 0}`;
            bearishSummary.textContent = `Total bearish records: ${data.bearish_count || 0}`;
        } catch (err) {
            bullishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bearishBody.innerHTML = `<tr><td colspan="4" class="state-cell">Error: ${err.message}</td></tr>`;
            bullishSummary.textContent = "Failed to load data.";
            bearishSummary.textContent = "Failed to load data.";
        }
    }

    refreshBtn.addEventListener("click", () => loadDataStream());
    const ohlcIntervalEl = document.getElementById("ohlcInterval");
    if (ohlcIntervalEl) ohlcIntervalEl.addEventListener("change", () => loadDataStream());
    document.addEventListener("DOMContentLoaded", () => loadDataStream());
})();

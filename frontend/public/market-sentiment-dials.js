/**
 * Market sentiment half-dials: NIFTY50, BANKNIFTY, INDIA VIX (% from session open).
 * Fetches /scan/market-sentiment-dials on load, every 5 minutes, and on refresh.
 */
(function () {
    const API = "/scan/market-sentiment-dials";
    const POLL_MS = 5 * 60 * 1000;
    let timer = null;

    function needleRotationDeg(pct) {
        if (pct == null || Number.isNaN(Number(pct))) return 0;
        const p = Math.max(-10, Math.min(10, Number(pct)));
        return (p / 10) * 90;
    }

    function formatPct(pct) {
        if (pct == null || Number.isNaN(Number(pct))) return "—";
        const n = Number(pct);
        const sign = n > 0 ? "+" : "";
        return sign + n.toFixed(2) + "%";
    }

    function dialSvg(rotationDeg, pct, gradId) {
        const gid = gradId || "g0";
        const neg = pct != null && pct < 0;
        const pos = pct != null && pct > 0;
        const needleColor = neg ? "#dc2626" : pos ? "#16a34a" : "#475569";
        return `
<svg class="sentiment-dial-svg" viewBox="0 0 200 118" aria-hidden="true">
  <defs>
    <linearGradient id="sentimentArcGrad_${gid}" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" style="stop-color:#fecaca"/>
      <stop offset="50%" style="stop-color:#e2e8f0"/>
      <stop offset="100%" style="stop-color:#bbf7d0"/>
    </linearGradient>
  </defs>
  <path d="M 22 96 A 78 78 0 0 1 178 96" fill="none" stroke="url(#sentimentArcGrad_${gid})" stroke-width="10" stroke-linecap="round"/>
  <path d="M 22 96 A 78 78 0 0 1 178 96" fill="none" stroke="rgba(148,163,184,0.35)" stroke-width="1"/>
  <g font-size="9" fill="#64748b" text-anchor="middle">
    <text x="28" y="112">-10%</text>
    <text x="100" y="116">0%</text>
    <text x="172" y="112">+10%</text>
  </g>
  <g transform="translate(100,96)">
    <line x1="0" y1="0" x2="0" y2="-58" stroke="${needleColor}" stroke-width="3.5" stroke-linecap="round"
      transform="rotate(${rotationDeg})"/>
    <circle cx="0" cy="0" r="5" fill="#1e293b"/>
  </g>
</svg>`;
    }

    function render(indices, updatedAt) {
        const grid = document.getElementById("marketSentimentDialsGrid");
        const elTime = document.getElementById("sentimentDialsUpdated");
        if (!grid) return;

        if (elTime && updatedAt) {
            try {
                const d = new Date(updatedAt);
                elTime.textContent = "Updated: " + d.toLocaleString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
            } catch (_) {
                elTime.textContent = "Updated: " + updatedAt;
            }
        }

        grid.innerHTML = (indices || [])
            .map((row) => {
                const pct = row.pct_change;
                const rot = needleRotationDeg(pct);
                const cls =
                    pct == null ? "neutral" : pct >= 0 ? "positive" : "negative";
                const gid = String(row.id || "x").replace(/[^a-z0-9_-]/gi, "");
                return `
<div class="sentiment-dial sentiment-dial--${cls}" data-id="${row.id || ""}">
  <div class="sentiment-dial-head">
    <span class="sentiment-dial-name">${row.label || ""}</span>
  </div>
  <div class="sentiment-dial-chart">${dialSvg(rot, pct, gid)}</div>
  <div class="sentiment-dial-footer">
    <span class="sentiment-dial-pct">${formatPct(pct)}</span>
    <span class="sentiment-dial-meta">vs open</span>
  </div>
</div>`;
            })
            .join("");
    }

    async function fetchDials() {
        const grid = document.getElementById("marketSentimentDialsGrid");
        if (!grid) return;
        grid.setAttribute("aria-busy", "true");
        try {
            const res = await fetch(API, { cache: "no-store", credentials: "same-origin" });
            const data = await res.json();
            if (data.success && Array.isArray(data.indices)) {
                render(data.indices, data.updated_at);
            } else {
                render([], null);
            }
        } catch (e) {
            console.warn("market-sentiment-dials:", e);
            render([], null);
        } finally {
            grid.removeAttribute("aria-busy");
        }
    }

    function startPolling() {
        if (timer) clearInterval(timer);
        timer = setInterval(fetchDials, POLL_MS);
    }

    function init() {
        const root = document.getElementById("marketSentimentDials");
        if (!root) return;

        fetchDials();
        startPolling();

        const btn = document.getElementById("sentimentDialsRefresh");
        if (btn) {
            btn.addEventListener("click", () => {
                btn.setAttribute("disabled", "disabled");
                fetchDials().finally(() => btn.removeAttribute("disabled"));
            });
        }

        document.addEventListener("visibilitychange", () => {
            if (document.visibilityState === "visible") fetchDials();
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();

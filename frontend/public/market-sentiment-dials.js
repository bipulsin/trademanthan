/**
 * Market sentiment half-dials: NIFTY / BANKNIFTY (% vs open, tri-zone arc),
 * INDIA VIX (spot 0–35 scale). Fetches /scan/market-sentiment-dials.
 */
(function () {
    const API = "/scan/market-sentiment-dials";
    const POLL_MS = 5 * 60 * 1000;
    let timer = null;

    const CX = 100;
    const CY = 96;
    const R = 78;

    function pt(phi) {
        return { x: CX + R * Math.cos(phi), y: CY - R * Math.sin(phi) };
    }

    function arcPath(phi1, phi2) {
        const p1 = pt(phi1);
        const p2 = pt(phi2);
        return `M ${p1.x.toFixed(2)} ${p1.y.toFixed(2)} A ${R} ${R} 0 0 1 ${p2.x.toFixed(2)} ${p2.y.toFixed(2)}`;
    }

    /** pct in [-10,10] → angle along upper semicircle (π = left, 0 = right) */
    function phiFromPct(pct) {
        const p = Math.max(-10, Math.min(10, Number(pct)));
        return Math.PI - (Math.PI * (p + 10)) / 20;
    }

    /** VIX 0–35 → same φ mapping (0 → π, 35 → 0) */
    function phiFromVix(v) {
        const x = Math.max(0, Math.min(35, Number(v)));
        return Math.PI * (1 - x / 35);
    }

    /**
     * SVG rotation (deg, + = clockwise) so the default pointer (triangle pointing up)
     * aims along the radius toward the arc at angle phi (center CX,CY; arc on upper semicircle).
     */
    function needleRotationDegFromPhi(phi) {
        const vx = Math.cos(phi);
        const vy = -Math.sin(phi);
        const targetRad = Math.atan2(vy, vx);
        const upRad = Math.atan2(-1, 0);
        let delta = targetRad - upRad;
        while (delta > Math.PI) delta -= 2 * Math.PI;
        while (delta < -Math.PI) delta += 2 * Math.PI;
        return (delta * 180) / Math.PI;
    }

    function formatPct(pct) {
        if (pct == null || Number.isNaN(Number(pct))) return "—";
        const n = Number(pct);
        const sign = n > 0 ? "+" : "";
        return sign + n.toFixed(2) + "%";
    }

    function formatVix(v) {
        if (v == null || Number.isNaN(Number(v))) return "—";
        return Number(v).toFixed(2);
    }

    function pointerGroup(rotationDeg, gid, needleFill) {
        const f = needleFill || "#0f172a";
        /* Double-pointed needle (kite): sharp tip toward the arc, smaller tip toward the hub */
        const needlePath =
            "M 0 -56 L 5.5 -6 L 0 9 L -5.5 -6 Z";
        return `
  <g transform="translate(${CX},${CY}) rotate(${rotationDeg})" filter="url(#ptrShadow_${gid})">
    <path d="${needlePath}" fill="${f}" stroke="#ffffff" stroke-width="1.15" stroke-linejoin="round"/>
    <circle cx="0" cy="0" r="6" fill="#1e293b" stroke="#94a3b8" stroke-width="1"/>
  </g>`;
    }

    function dialSvgPct(pct, gid) {
        const has = pct != null && !Number.isNaN(Number(pct));
        const p = has ? Number(pct) : 0;
        const rot = has ? needleRotationDegFromPhi(phiFromPct(pct)) : 0;
        const phiL = Math.PI;
        const phiM1 = phiFromPct(-3);
        const phiM2 = phiFromPct(3);
        const phiR = 0;
        const pBear = arcPath(phiL, phiM1);
        const pMed = arcPath(phiM1, phiM2);
        const pBull = arcPath(phiM2, phiR);
        const needleTint = !has
            ? "#64748b"
            : p < -3
              ? "#b91c1c"
              : p > 3
                ? "#15803d"
                : "#b45309";

        return `
<svg class="sentiment-dial-svg sentiment-dial-svg--pct" viewBox="0 0 200 124" aria-hidden="true">
  <defs>
    <filter id="ptrShadow_${gid}" x="-60" y="-70" width="180" height="180" color-interpolation-filters="sRGB">
      <feDropShadow dx="0" dy="2" stdDeviation="2" flood-opacity="0.4"/>
    </filter>
    <linearGradient id="trackGlow_${gid}" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#fecdd3"/>
      <stop offset="35%" stop-color="#fde68a"/>
      <stop offset="65%" stop-color="#fde68a"/>
      <stop offset="100%" stop-color="#bbf7d0"/>
    </linearGradient>
  </defs>
  <path d="${pBear}" fill="none" stroke="#dc2626" stroke-width="14" stroke-linecap="round" opacity="0.95"/>
  <path d="${pMed}" fill="none" stroke="#f59e0b" stroke-width="14" stroke-linecap="round" opacity="0.95"/>
  <path d="${pBull}" fill="none" stroke="#16a34a" stroke-width="14" stroke-linecap="round" opacity="0.95"/>
  <path d="${arcPath(phiL, phiR)}" fill="none" stroke="url(#trackGlow_${gid})" stroke-width="4" stroke-linecap="round" opacity="0.45"/>
  <path d="${arcPath(phiL, phiR)}" fill="none" stroke="rgba(15,23,42,0.35)" stroke-width="1"/>
  <g font-size="8.5" font-weight="700" fill="#475569" text-anchor="middle">
    <text x="24" y="118">-10%</text>
    <text x="100" y="122">0%</text>
    <text x="176" y="118">+10%</text>
  </g>
  <g font-size="7" fill="#94a3b8" text-anchor="middle">
    <text x="${pt(phiFromPct(-3)).x.toFixed(0)}" y="${pt(phiFromPct(-3)).y + 14}">-3</text>
    <text x="${pt(phiFromPct(3)).x.toFixed(0)}" y="${pt(phiFromPct(3)).y + 14}">+3</text>
  </g>
  ${pointerGroup(rot, gid, needleTint)}
</svg>`;
    }

    function dialSvgVix(vixVal, gid) {
        const v = vixVal == null || Number.isNaN(Number(vixVal)) ? null : Number(vixVal);
        const rot = v == null ? 0 : needleRotationDegFromPhi(phiFromVix(v));
        const phiL = Math.PI;
        const phiR = 0;
        const needleTint =
            v == null ? "#64748b" : v < 12 ? "#15803d" : v < 22 ? "#d97706" : "#b91c1c";

        return `
<svg class="sentiment-dial-svg sentiment-dial-svg--vix" viewBox="0 0 200 124" aria-hidden="true">
  <defs>
    <filter id="ptrShadow_${gid}" x="-60" y="-70" width="180" height="180" color-interpolation-filters="sRGB">
      <feDropShadow dx="0" dy="2" stdDeviation="2" flood-opacity="0.4"/>
    </filter>
    <linearGradient id="vixArc_${gid}" x1="0%" y1="0%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#22c55e"/>
      <stop offset="45%" stop-color="#fbbf24"/>
      <stop offset="100%" stop-color="#ef4444"/>
    </linearGradient>
  </defs>
  <path d="${arcPath(phiL, phiR)}" fill="none" stroke="url(#vixArc_${gid})" stroke-width="14" stroke-linecap="round"/>
  <path d="${arcPath(phiL, phiR)}" fill="none" stroke="rgba(255,255,255,0.35)" stroke-width="3" stroke-linecap="round" opacity="0.6"/>
  <path d="${arcPath(phiL, phiR)}" fill="none" stroke="rgba(15,23,42,0.25)" stroke-width="1"/>
  <g font-size="8.5" font-weight="700" fill="#475569" text-anchor="middle">
    <text x="22" y="118">0</text>
    <text x="100" y="122">17.5</text>
    <text x="178" y="118">35</text>
  </g>
  ${pointerGroup(rot, gid, needleTint)}
</svg>`;
    }

    function render(indices, updatedAt) {
        const grid = document.getElementById("marketSentimentDialsGrid");
        const elTime = document.getElementById("sentimentDialsUpdated");
        if (!grid) return;

        if (elTime && updatedAt) {
            try {
                const d = new Date(updatedAt);
                elTime.textContent =
                    "Updated: " +
                    d.toLocaleString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
            } catch (_) {
                elTime.textContent = "Updated: " + updatedAt;
            }
        }

        grid.innerHTML = (indices || [])
            .map((row) => {
                const gid = String(row.id || "x").replace(/[^a-z0-9_-]/gi, "");
                const mode = row.dial_mode || (row.id === "indiavix" ? "vix" : "pct");
                let chartHtml;
                let footerMain;
                let footerMeta;
                let cls;

                if (mode === "vix") {
                    const vv = row.vix_value != null ? row.vix_value : row.last;
                    chartHtml = dialSvgVix(vv, gid);
                    footerMain = formatVix(vv);
                    footerMeta = "VIX spot (0–35 scale)";
                    cls =
                        vv == null
                            ? "neutral"
                            : Number(vv) < 12
                              ? "vix-low"
                              : Number(vv) < 22
                                ? "vix-mid"
                                : "vix-high";
                } else {
                    const pct = row.pct_change;
                    chartHtml = dialSvgPct(pct, gid);
                    footerMain = formatPct(pct);
                    footerMeta = "vs session open";
                    if (pct == null) cls = "neutral";
                    else if (Number(pct) < -3) cls = "negative";
                    else if (Number(pct) > 3) cls = "positive";
                    else cls = "medium";
                }

                return `
<div class="sentiment-dial sentiment-dial--${cls}" data-id="${row.id || ""}">
  <div class="sentiment-dial-head">
    <span class="sentiment-dial-name">${row.label || ""}</span>
  </div>
  <div class="sentiment-dial-chart">${chartHtml}</div>
  <div class="sentiment-dial-footer">
    <span class="sentiment-dial-pct">${footerMain}</span>
    <span class="sentiment-dial-meta">${footerMeta}</span>
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

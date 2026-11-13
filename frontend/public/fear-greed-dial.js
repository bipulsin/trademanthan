/**
 * Reusable Fear & Greed semi-circular dial (0–100).
 *
 * Context/filter only — not an entry/exit signal.
 *
 * Usage (embed on any page):
 *   <div id="myFg"></div>
 *   <script src="fear-greed-dial.js"></script>
 *   <script>FearGreedDial.mount("#myFg", { instrument: "RELIANCE", size: "medium" });</script>
 *
 * Or: FearGreedDial.mount(el, { instrument: "NIFTY", size: "small" });
 */
(function (global) {
    const API = "/api/fear-greed";
    const POLL_MS = 5 * 60 * 1000;
    const CX = 100;
    const CY = 96;
    const R = 78;

    const ZONE_COLORS = {
        "Extreme Fear": { arc: "#b91c1c", cls: "fg-extreme-fear" },
        Fear: { arc: "#ea580c", cls: "fg-fear" },
        Greed: { arc: "#86efac", cls: "fg-greed" },
        "Extreme Greed": { arc: "#15803d", cls: "fg-extreme-greed" },
    };

    function pt(phi) {
        return { x: CX + R * Math.cos(phi), y: CY - R * Math.sin(phi) };
    }

    function arcPath(phi1, phi2) {
        const p1 = pt(phi1);
        const p2 = pt(phi2);
        return `M ${p1.x.toFixed(2)} ${p1.y.toFixed(2)} A ${R} ${R} 0 0 1 ${p2.x.toFixed(2)} ${p2.y.toFixed(2)}`;
    }

    /** score 0–100 → φ (π left / fear → 0 right / greed) */
    function phiFromScore(score) {
        const s = Math.max(0, Math.min(100, Number(score)));
        return Math.PI * (1 - s / 100);
    }

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

    function zoneFor(score) {
        const s = Number(score);
        if (!Number.isFinite(s)) return null;
        if (s <= 24) return "Extreme Fear";
        if (s <= 49) return "Fear";
        if (s <= 74) return "Greed";
        return "Extreme Greed";
    }

    function isMarketHoursIST(now) {
        const d = now || new Date();
        const parts = new Intl.DateTimeFormat("en-GB", {
            timeZone: "Asia/Kolkata",
            weekday: "short",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
        }).formatToParts(d);
        const map = {};
        parts.forEach((p) => {
            map[p.type] = p.value;
        });
        const wd = map.weekday;
        if (wd === "Sat" || wd === "Sun") return false;
        const hh = Number(map.hour);
        const mm = Number(map.minute);
        const mins = hh * 60 + mm;
        return mins >= 9 * 60 + 15 && mins <= 15 * 60 + 30;
    }

    function escapeHtml(s) {
        return String(s == null ? "" : s)
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function dialSvg(score, gid, animatedFrom) {
        const has = score != null && Number.isFinite(Number(score));
        const s = has ? Number(score) : 50;
        const zone = has ? zoneFor(s) : null;
        const needleTint = zone ? ZONE_COLORS[zone].arc : "#64748b";
        const rot = has ? needleRotationDegFromPhi(phiFromScore(s)) : 0;
        const fromScore =
            animatedFrom != null && Number.isFinite(Number(animatedFrom))
                ? Number(animatedFrom)
                : s;
        const fromRot = needleRotationDegFromPhi(phiFromScore(fromScore));

        const phiL = Math.PI;
        const phi24 = phiFromScore(24.5);
        const phi49 = phiFromScore(49.5);
        const phi74 = phiFromScore(74.5);
        const phiR = 0;

        const needlePath = "M 0 -56 L 5.5 -6 L 0 9 L -5.5 -6 Z";

        return `
<svg class="sentiment-dial-svg fear-greed-dial-svg" viewBox="0 0 200 124" aria-hidden="true">
  <defs>
    <filter id="ptrShadow_fg_${gid}" x="-60" y="-70" width="180" height="180" color-interpolation-filters="sRGB">
      <feDropShadow dx="0" dy="2" stdDeviation="2" flood-opacity="0.4"/>
    </filter>
  </defs>
  <path d="${arcPath(phiL, phi24)}" fill="none" stroke="#b91c1c" stroke-width="14" stroke-linecap="butt" opacity="0.95"/>
  <path d="${arcPath(phi24, phi49)}" fill="none" stroke="#ea580c" stroke-width="14" stroke-linecap="butt" opacity="0.95"/>
  <path d="${arcPath(phi49, phi74)}" fill="none" stroke="#86efac" stroke-width="14" stroke-linecap="butt" opacity="0.95"/>
  <path d="${arcPath(phi74, phiR)}" fill="none" stroke="#15803d" stroke-width="14" stroke-linecap="butt" opacity="0.95"/>
  <path d="${arcPath(phiL, phiR)}" fill="none" stroke="rgba(12,18,16,0.35)" stroke-width="1"/>
  <g font-size="8" font-weight="700" fill="#8fa398" text-anchor="middle">
    <text x="22" y="118">0</text>
    <text x="100" y="122">50</text>
    <text x="178" y="118">100</text>
  </g>
  <g class="fg-needle" data-rot="${rot}" transform="translate(${CX},${CY}) rotate(${fromRot})" filter="url(#ptrShadow_fg_${gid})" style="transition: transform 0.7s cubic-bezier(0.22,1,0.36,1)">
    <path d="${needlePath}" fill="${needleTint}" stroke="#e7f0ea" stroke-width="1.15" stroke-linejoin="round"/>
    <circle cx="0" cy="0" r="6" fill="#1a2621" stroke="#8fa398" stroke-width="1"/>
  </g>
</svg>`;
    }

    function breakdownHtml(data) {
        const inds = data.indicators || {};
        const skipped = data.skipped || {};
        const rows = Object.keys(inds)
            .map((k) => {
                const r = inds[k] || {};
                return `<li><strong>${escapeHtml(k)}</strong>: ${
                    r.normalized != null ? Math.round(r.normalized) : "—"
                } <span class="fg-ind-note">${escapeHtml(r.note || "")}</span></li>`;
            })
            .join("");
        const skipRows = Object.keys(skipped)
            .map(
                (k) =>
                    `<li class="fg-skipped"><strong>${escapeHtml(k)}</strong>: ${escapeHtml(
                        skipped[k]
                    )}</li>`
            )
            .join("");
        if (!rows && !skipRows) return "";
        return `<details class="fg-breakdown"><summary>Indicator breakdown</summary><ul>${rows}${skipRows}</ul></details>`;
    }

    function renderCard(host, data, opts, prevScore) {
        const size = opts.size || "medium";
        const instrument = (data && data.instrument) || opts.instrument || "";
        const score = data && data.success ? data.score : null;
        const zone = data && data.success ? data.zone || zoneFor(score) : null;
        const zmeta = zone ? ZONE_COLORS[zone] : null;
        const cls = zmeta ? zmeta.cls : "fg-neutral";
        const gid = String(instrument || "x").replace(/[^a-z0-9_-]/gi, "") || "x";
        const state =
            !data
                ? "loading"
                : data.success && score != null
                  ? "ok"
                  : data.success === false
                    ? "empty"
                    : "error";

        let footerMain = "—";
        let footerMeta = "Fear & Greed · context only";
        let chartHtml = dialSvg(null, gid, null);

        if (state === "loading") {
            footerMain = "…";
            footerMeta = "Loading…";
        } else if (state === "error") {
            footerMain = "—";
            footerMeta = escapeHtml((data && data.message) || "Unavailable");
        } else if (state === "empty") {
            footerMain = "—";
            footerMeta = "No data";
        } else {
            footerMain = String(Math.round(Number(score)));
            footerMeta = escapeHtml(zone || "");
            chartHtml = dialSvg(score, gid, prevScore);
        }

        host.className =
            "sentiment-dial fear-greed-dial sentiment-dial--" +
            cls +
            " fear-greed-dial--" +
            size +
            (state !== "ok" ? " fear-greed-dial--" + state : "");
        host.setAttribute("data-id", "feargreed");
        host.setAttribute("data-instrument", instrument);
        host.setAttribute("role", "listitem");
        host.innerHTML = `
  <div class="sentiment-dial-head">
    <span class="sentiment-dial-name">Fear / Greed</span>
    <span class="sentiment-dial-src">${escapeHtml(instrument)}</span>
  </div>
  <div class="sentiment-dial-chart">${chartHtml}</div>
  <div class="sentiment-dial-footer">
    <span class="sentiment-dial-pct fg-score">${footerMain}</span>
    <span class="sentiment-dial-meta">${footerMeta}</span>
  </div>
  ${state === "ok" ? breakdownHtml(data) : ""}
`;

        // Smooth needle to target after paint
        if (state === "ok") {
            const needle = host.querySelector(".fg-needle");
            if (needle) {
                const rot = needle.getAttribute("data-rot");
                requestAnimationFrame(() => {
                    needle.setAttribute(
                        "transform",
                        "translate(" + CX + "," + CY + ") rotate(" + rot + ")"
                    );
                });
            }
        }
    }

    function mount(target, options) {
        const opts = Object.assign({ instrument: "NIFTY", size: "medium", autoRefresh: true }, options || {});
        const el =
            typeof target === "string" ? document.querySelector(target) : target;
        if (!el) {
            console.warn("FearGreedDial.mount: target not found", target);
            return null;
        }

        let timer = null;
        let prevScore = null;
        let destroyed = false;

        renderCard(el, null, opts, null);

        async function refresh() {
            if (destroyed) return;
            el.setAttribute("aria-busy", "true");
            try {
                const url =
                    API +
                    "?instrument=" +
                    encodeURIComponent(opts.instrument) +
                    "&_=" +
                    Date.now();
                const res = await fetch(url, { cache: "no-store", credentials: "same-origin" });
                const data = await res.json();
                const nextScore = data && data.score != null ? Number(data.score) : null;
                renderCard(el, data, opts, prevScore);
                if (nextScore != null) prevScore = nextScore;
            } catch (e) {
                console.warn("FearGreedDial:", e);
                renderCard(el, { success: false, message: "Network error" }, opts, prevScore);
            } finally {
                el.removeAttribute("aria-busy");
            }
        }

        function schedule() {
            if (timer) clearInterval(timer);
            if (!opts.autoRefresh) return;
            timer = setInterval(() => {
                if (document.visibilityState === "hidden") return;
                if (!isMarketHoursIST()) return;
                refresh();
            }, POLL_MS);
        }

        refresh();
        schedule();

        const onVis = () => {
            if (document.visibilityState === "visible" && isMarketHoursIST()) refresh();
        };
        document.addEventListener("visibilitychange", onVis);

        return {
            refresh,
            destroy() {
                destroyed = true;
                if (timer) clearInterval(timer);
                document.removeEventListener("visibilitychange", onVis);
            },
            el,
        };
    }

    /** Build a mount host node suitable for the Market Sentiments grid. */
    function createHost() {
        const div = document.createElement("div");
        div.className = "sentiment-dial fear-greed-dial";
        div.setAttribute("data-id", "feargreed");
        return div;
    }

    global.FearGreedDial = {
        mount,
        createHost,
        zoneFor,
        isMarketHoursIST,
        API,
    };
})(typeof window !== "undefined" ? window : globalThis);

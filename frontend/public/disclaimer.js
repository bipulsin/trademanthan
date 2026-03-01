/*
 * Tradentical / TradeWithCTO Disclaimer Modal
 * - Reusable across all pages
 * - Supports optional mandatory acceptance flow
 */
(function () {
    const STORAGE_KEY = "tradentical_disclaimer_accepted_v1";
    const STYLE_ID = "tm-disclaimer-style";
    const MODAL_ID = "tmDisclaimerModal";

    const DISCLAIMER_HTML = `
        <h2>Disclaimer</h2>
        <h3>Market Risk Disclosure</h3>
        <p>Investments in securities markets are subject to market risks. Please read all related documents carefully before investing.</p>

        <h3>Educational Purpose Only</h3>
        <p>All information, views, charts, trade ideas, analysis, reports, posts, and communications shared through our website, social media channels, webinars, courses, or any other platform are strictly for educational and informational purposes only.</p>

        <h3>Not Investment Advice</h3>
        <p>Nothing published or communicated by TradeWithCTO, Tradentical.com, or Trademanthan.in should be construed as investment advice, research recommendation, trading advice, or solicitation to buy or sell any securities or financial instruments.</p>

        <h3>Consult a SEBI-Registered Advisor</h3>
        <p>Investors and traders should independently evaluate all information and are strongly advised to consult a SEBI-registered Investment Advisor, Research Analyst, or financial professional before making any investment or trading decisions.</p>

        <h3>Illustrative Use of Securities</h3>
        <p>Any securities, stocks, derivatives, indices, commodities, or financial instruments mentioned in our content are used purely for academic discussion, case studies, research illustration, or market analysis, and should not be interpreted as recommendations.</p>

        <h3>No Liability for Losses</h3>
        <p>TradeWithCTO, Tradentical.com, Trademanthan.in, including their owners, promoters, partners, employees, affiliates, contributors, and associates, shall not be responsible or liable for any direct or indirect losses, damages, or consequences arising from the use of any information shared through our platforms.</p>

        <h3>No Guarantee of Returns</h3>
        <p>We do not guarantee the accuracy, completeness, reliability, or timeliness of any information shared. We also do not guarantee profits, returns, success, or performance from any ideas, analysis, or strategies discussed.</p>

        <h3>Past Performance Disclaimer</h3>
        <p>Any references to past performance, historical data, or back-tested results are for informational purposes only and are not indicative of future performance or results.</p>

        <h3>Independent Decision Making</h3>
        <p>All investment and trading decisions must be taken solely at the investor’s own discretion, based on their personal financial situation, investment objectives, and risk tolerance.</p>

        <h3>Risk Disclosure</h3>
        <p>Trading in equities, derivatives, commodities, or other financial instruments may involve significant financial risk. Higher expected returns are generally associated with higher levels of risk, and investors should carefully assess their risk capacity before participating in the markets.</p>

        <h3>No SEBI Registration</h3>
        <p>TradeWithCTO, Tradentical.com, and Trademanthan.in are not registered with the Securities and Exchange Board of India (SEBI) as Investment Advisors, Research Analysts, Portfolio Managers, or in any other advisory capacity. The content provided is purely educational in nature.</p>

        <h3>No Material Financial Interest</h3>
        <p>Unless explicitly disclosed, TradeWithCTO and its promoters, partners, employees, and associates do not have any material financial interest in the securities discussed.</p>

        <h3>Open Trade Discussions</h3>
        <p>Some research views, discussions, or examples may be presented without predefined stop-loss or target levels, and are shared solely for the purpose of explaining market concepts or strategies. Market conditions, news events, or company-specific developments can result in substantial financial losses.</p>

        <h3>Third-Party Platforms and Communication</h3>
        <p>We may share content through websites, social media platforms, messaging channels, or educational sessions. Any interpretation or use of such content is entirely at the viewer’s own risk.</p>

        <h3>Beware of Impersonation</h3>
        <p>Please be aware of fake profiles or channels impersonating our brand. Our official social media handle is @TradeWithCTO on Instagram, Twitter (X), YouTube, and Telegram. The handle is written exactly as TradeWithCTO, without spaces, hyphens (-), or underscores (_).</p>

        <h3>Grievances and Support</h3>
        <p>For any grievances, feedback, or suggestions, please contact us at:</p>
        <p>support@tradewithcto.com</p>
        <p>support@tradentical.com</p>
    `;

    function injectStyle() {
        if (document.getElementById(STYLE_ID)) return;
        const style = document.createElement("style");
        style.id = STYLE_ID;
        style.textContent = `
            .tm-disclaimer-overlay{position:fixed;inset:0;background:rgba(2,6,23,.75);display:none;align-items:center;justify-content:center;z-index:100000;padding:20px}
            .tm-disclaimer-overlay.visible{display:flex}
            .tm-disclaimer-dialog{width:min(920px,96vw);max-height:min(88vh,900px);background:#0f172a;color:#f1f5f9;border:1px solid rgba(148,163,184,.3);border-radius:12px;overflow:hidden;display:flex;flex-direction:column}
            .tm-disclaimer-header{display:flex;align-items:center;justify-content:space-between;padding:12px 16px;border-bottom:1px solid rgba(148,163,184,.25)}
            .tm-disclaimer-header h2{margin:0;font-size:1rem}
            .tm-disclaimer-close{background:transparent;border:1px solid rgba(148,163,184,.35);color:#f1f5f9;border-radius:8px;width:34px;height:34px;cursor:pointer}
            .tm-disclaimer-body{padding:16px;overflow-y:auto;line-height:1.55}
            .tm-disclaimer-body h2{font-size:1.25rem;margin:0 0 10px 0}
            .tm-disclaimer-body h3{font-size:1rem;margin:16px 0 6px 0;color:#93c5fd}
            .tm-disclaimer-body p{margin:0 0 8px 0;color:#e2e8f0}
            .tm-disclaimer-footer{padding:12px 16px;border-top:1px solid rgba(148,163,184,.25);display:flex;justify-content:flex-end}
            .tm-disclaimer-agree{background:#16a34a;color:#fff;border:none;border-radius:8px;padding:10px 16px;font-weight:600;cursor:pointer}
            .tm-disclaimer-agree:hover{background:#15803d}
            a.disclaimer-link{cursor:pointer}
        `;
        document.head.appendChild(style);
    }

    function createModal() {
        if (document.getElementById(MODAL_ID)) return document.getElementById(MODAL_ID);
        injectStyle();
        const overlay = document.createElement("div");
        overlay.id = MODAL_ID;
        overlay.className = "tm-disclaimer-overlay";
        overlay.innerHTML = `
            <div class="tm-disclaimer-dialog" role="dialog" aria-modal="true" aria-labelledby="tmDisclaimerTitle">
                <div class="tm-disclaimer-header">
                    <h2 id="tmDisclaimerTitle">Disclaimer</h2>
                    <button class="tm-disclaimer-close" type="button" aria-label="Close disclaimer">&times;</button>
                </div>
                <div class="tm-disclaimer-body">${DISCLAIMER_HTML}</div>
                <div class="tm-disclaimer-footer">
                    <button class="tm-disclaimer-agree" type="button">I Agree</button>
                </div>
            </div>
        `;
        document.body.appendChild(overlay);
        return overlay;
    }

    function isAccepted() {
        return localStorage.getItem(STORAGE_KEY) === "true";
    }

    function markAccepted() {
        localStorage.setItem(STORAGE_KEY, "true");
    }

    function openModal(opts) {
        const options = Object.assign({ required: false }, opts || {});
        const modal = createModal();
        const closeBtn = modal.querySelector(".tm-disclaimer-close");
        const agreeBtn = modal.querySelector(".tm-disclaimer-agree");

        closeBtn.style.display = options.required ? "none" : "inline-flex";
        modal.classList.add("visible");

        const close = () => {
            modal.classList.remove("visible");
            modal.removeEventListener("click", overlayCloseHandler);
            document.removeEventListener("keydown", escHandler);
        };

        const overlayCloseHandler = (e) => {
            if (!options.required && e.target === modal) close();
        };
        const escHandler = (e) => {
            if (!options.required && e.key === "Escape") close();
        };

        closeBtn.onclick = () => close();
        agreeBtn.onclick = () => {
            markAccepted();
            close();
        };
        modal.addEventListener("click", overlayCloseHandler);
        document.addEventListener("keydown", escHandler);
    }

    function bindLinks() {
        document.querySelectorAll(".disclaimer-link").forEach((el) => {
            if (el.dataset.tmBound === "1") return;
            el.dataset.tmBound = "1";
            el.addEventListener("click", (e) => {
                e.preventDefault();
                openModal({ required: false });
            });
        });
    }

    window.TradenticalDisclaimer = {
        open: (required) => openModal({ required: !!required }),
        isAccepted,
        bindLinks,
    };

    document.addEventListener("DOMContentLoaded", () => {
        bindLinks();
    });
})();

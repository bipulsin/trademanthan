/**
 * Vajra Futures — dedicated page bootstrap.
 */
(function () {
    document.addEventListener('DOMContentLoaded', function () {
        const wfInit =
            window.VajraTradeWorkflow &&
            window.VajraTradeWorkflow.init({
                platform: 'vajra_futures',
                listAllPlatforms: true,
                runningElId: 'vfVajraActiveTrades',
                closedElId: 'vfVajraClosedTrades',
                compactSections: true,
                emptyOpenHtml:
                    '<p class="vajra-meta">No open Vajra positions. Use ENTER on the screen above to activate a trade.</p>',
                emptyClosedHtml: '<p class="vajra-meta">No closed Vajra trades today (IST).</p>',
            });
        const ratingsOpts = {
            prefix: 'vf',
            listElId: 'vfVajraTable',
            moreBtnId: 'vfVajraMoreBtn',
            metaElId: 'vfVajraMeta',
            msgElId: 'vfVajraMsg',
            watchMs: 15000,
            refreshBtnId: 'vfVajraRefreshBtn',
        };
        const startRatings = function () {
            if (window.VajraStableExecution) {
                window.VajraStableExecution.init({ prefix: 'vf' });
            }
            if (window.VajraFuturesRatings) {
                window.VajraFuturesRatings.init(ratingsOpts);
            }
            if (window.VajraHelpGuide) {
                window.VajraHelpGuide.init({ prefix: 'vf' });
            }
        };
        if (wfInit && typeof wfInit.then === 'function') {
            wfInit.then(startRatings).catch(startRatings);
        } else {
            startRatings();
        }
    });
})();

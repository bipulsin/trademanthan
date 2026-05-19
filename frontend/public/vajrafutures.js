/**
 * Vajra Futures — dedicated page bootstrap.
 */
(function () {
    document.addEventListener('DOMContentLoaded', function () {
        if (window.VajraFuturesRatings) {
            window.VajraFuturesRatings.init({
                prefix: 'vf',
                listElId: 'vfVajraTable',
                moreBtnId: 'vfVajraMoreBtn',
                metaElId: 'vfVajraMeta',
                msgElId: 'vfVajraMsg',
                watchMs: 20000,
            });
        }
        if (window.VajraTradeWorkflow) {
            window.VajraTradeWorkflow.init({
                platform: 'vajra_futures',
                listAllPlatforms: true,
                runningElId: 'vfVajraActiveTrades',
                closedElId: 'vfVajraClosedTrades',
                compactSections: true,
                emptyOpenHtml: '<p class="vajra-meta">No open Vajra positions. Use ENTER on the screen above to activate a trade.</p>',
                emptyClosedHtml: '<p class="vajra-meta">No closed Vajra trades this session.</p>',
            });
        }
        if (window.VajraHelpGuide) {
            window.VajraHelpGuide.init({ prefix: 'vf' });
        }
    });
})();

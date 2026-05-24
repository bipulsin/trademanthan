/**
 * Toggle visibility of the existing volume histogram series (no rebuild).
 */
(function (global) {
    'use strict';

    const MARGINS_VISIBLE = { top: 0.82, bottom: 0 };
    const MARGINS_HIDDEN = { top: 0.02, bottom: 0 };

    /**
     * @param {object|null} volumeSeries LWC histogram series
     * @param {object|null} chart LWC chart instance
     * @param {boolean} enabled
     */
    function applyVolumeVisibility(volumeSeries, chart, enabled) {
        if (!volumeSeries) return;
        volumeSeries.applyOptions({ visible: !!enabled });
        if (chart) {
            try {
                chart.priceScale('').applyOptions({
                    scaleMargins: enabled ? MARGINS_VISIBLE : MARGINS_HIDDEN,
                });
            } catch (e) {
                /* ignore */
            }
        }
    }

    global.ChartVolumeVisibility = {
        apply: applyVolumeVisibility,
        marginsVisible: MARGINS_VISIBLE,
        marginsHidden: MARGINS_HIDDEN,
    };
})(typeof window !== 'undefined' ? window : globalThis);

/**
 * Run before app JS. Clears legacy / invalid auth tokens once per client version bump.
 * Fixes "works in Incognito only" when normal mode still has old localStorage or cached bundles.
 */
(function () {
    'use strict';
    try {
        var KEY = 'trademanthan_client_version';
        var VER = '2026-04-05-v1';
        var prev = localStorage.getItem(KEY);
        if (prev === VER) return;

        var t = localStorage.getItem('trademanthan_token') || '';
        var bad =
            !t ||
            t.indexOf('.') === -1 ||
            t.indexOf('google_token_') === 0 ||
            t.indexOf('email_token_') === 0 ||
            t.indexOf('demo_token_') === 0;
        if (bad) {
            localStorage.removeItem('trademanthan_token');
            localStorage.removeItem('trademanthan_user');
        }
        localStorage.setItem(KEY, VER);
    } catch (e) {
        /* ignore */
    }
})();

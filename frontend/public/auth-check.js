/**
 * Auth check for protected pages - redirects to login if not authenticated.
 * Include this script at the top of protected pages (before any content).
 */
(function() {
    function trademanthanApiBase() {
        const h = window.location.hostname;
        if (h === 'localhost' || h === '127.0.0.1') return 'http://localhost:8000';
        return window.location.origin;
    }
    function isAuthenticated() {
        try {
            const token = localStorage.getItem('trademanthan_token');
            const userData = localStorage.getItem('trademanthan_user');
            if (!token || !userData) return false;
            // Strict session: must be backend JWT
            if (!token.includes('.')) {
                localStorage.removeItem('trademanthan_token');
                localStorage.removeItem('trademanthan_user');
                return false;
            }
            const user = JSON.parse(userData);
            if (!user.email || !user.name) return false;
            return true;
        } catch (e) {
            localStorage.removeItem('trademanthan_token');
            localStorage.removeItem('trademanthan_user');
            return false;
        }
    }
    if (!isAuthenticated()) {
        window.location.replace('index.html');
        return;
    }

    // Best-effort page activity tracking for protected pages
    (function trackPageVisit() {
        try {
            const token = localStorage.getItem('trademanthan_token');
            if (!token || !token.includes('.')) return;
            const page = window.location.pathname.split('/').pop() || 'unknown';
            const payload = { page: String(page).slice(0, 255), title: String(document.title || '').slice(0, 255) };
            const opts = {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${token}`
                },
                body: JSON.stringify(payload),
                keepalive: true
            };
            const b = trademanthanApiBase();
            fetch(b + '/api/auth/activity/page-view', opts).catch(() =>
                fetch(b + '/auth/activity/page-view', opts).catch(() => {})
            );
        } catch (e) {
            // ignore
        }
    })();
})();

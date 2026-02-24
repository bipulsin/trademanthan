/**
 * Auth check for protected pages - redirects to login if not authenticated.
 * Include this script at the top of protected pages (before any content).
 */
(function() {
    function isAuthenticated() {
        try {
            const token = localStorage.getItem('trademanthan_token');
            const userData = localStorage.getItem('trademanthan_user');
            if (!token || !userData) return false;
            if (!token.startsWith('google_token_') && !token.startsWith('email_token_') && !token.includes('.')) {
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
    }
})();

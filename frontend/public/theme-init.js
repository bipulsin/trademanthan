/**
 * Theme initialization - runs before CSS to prevent flash.
 * Sets theme class on html element from localStorage.
 */
(function() {
    var theme = localStorage.getItem('tradentical_theme') || 'theme-dark';
    document.documentElement.classList.add(theme);
})();

/**
 * Tradentical Left Menu - Unified panel for all post-login pages
 * Loads left-menu.html, handles collapse/expand, navigation, auth
 */
let isAuthenticating = false;
let hasRedirected = false;
let isAuthenticated = false;

const MENU_HTML_PATH = 'left-menu.html';

class LeftMenu {
    constructor() {
        this.currentPage = this.getCurrentPage();
        this.isAuthenticated = false;
        this.collapsed = localStorage.getItem('leftMenuCollapsed') === 'true';
        this.applyThemeImmediate();
        this.init();
    }

    applyThemeImmediate() {
        const theme = localStorage.getItem('tradentical_theme') || 'dark';
        document.body.setAttribute('data-theme', theme);
        if (this.isThemePage()) document.body.classList.add('theme-page');
    }

    isThemePage() {
        const path = window.location.pathname;
        return /dashboard|cargpt|broker|strategy|reports|settings|carsetup/.test(path);
    }

    getCurrentPage() {
        const path = window.location.pathname;
        if (path.includes('dashboard')) return 'dashboard';
        if (path.includes('strategy')) return 'strategy';
        if (path.includes('broker')) return 'broker';
        if (path.includes('algo')) return 'algo';
        if (path.includes('scan')) return 'scan';
        if (path.includes('settings')) return 'settings';
        if (path.includes('reports')) return 'reports';
        if (path.includes('carsetup') || path.includes('cargpt')) return 'cargpt';
        return 'dashboard';
    }

    async init() {
        if (isAuthenticating) return;
        isAuthenticating = true;

        setTimeout(async () => {
            if (this.checkAuthentication()) {
                this.isAuthenticated = true;
                isAuthenticated = true;
                await this.loadMenu();
                this.setupCollapseToggle();
                this.setupMobileMenu();
                this.loadUserData();
                this.setupNavigation();
                this.setActiveNavigation();
                this.syncMainContentMargin();
            } else {
                const currentPath = window.location.pathname;
                const isProtectedPage = currentPath.includes('dashboard') || currentPath.includes('strategy') ||
                    currentPath.includes('broker') || currentPath.includes('algo') || currentPath.includes('scan') ||
                    currentPath.includes('reports') || currentPath.includes('settings') ||
                    currentPath.includes('carsetup') || currentPath.includes('cargpt');
                if (!hasRedirected && isProtectedPage) {
                    hasRedirected = true;
                    window.location.replace('index.html');
                }
            }
            isAuthenticating = false;
        }, 100);
    }

    checkAuthentication() {
        if (isAuthenticated) return true;
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
            return false;
        }
    }

    async loadMenu() {
        const container = document.getElementById('left-menu-container');
        if (!container) return;

        try {
            const res = await fetch(MENU_HTML_PATH);
            const html = await res.text();
            container.innerHTML = html;
        } catch (e) {
            console.warn('LeftMenu: Could not fetch left-menu.html, using inline', e);
            container.innerHTML = this.getInlineMenuHTML();
        }

        // Hide page's mobile toggle if exists - we use our own
        const pageToggle = document.getElementById('mobileMenuToggle');
        if (pageToggle && pageToggle.closest('.mobile-title-bar')) {
            pageToggle.style.display = 'none';
        }

        this.setupThemeToggle();
        this.updateDateTime();
        setInterval(() => this.updateDateTime(), 1000);
    }

    setupThemeToggle() {
        const theme = localStorage.getItem('tradentical_theme') || 'dark';
        this.updateThemeButtons(theme);

        document.querySelectorAll('.theme-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const t = btn.dataset.theme;
                localStorage.setItem('tradentical_theme', t);
                document.body.setAttribute('data-theme', t);
                this.updateThemeButtons(t);
            });
        });
    }

    updateThemeButtons(theme) {
        document.querySelectorAll('.theme-btn').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.theme === theme);
        });
    }

    updateDateTime() {
        const el = document.getElementById('userDateTime');
        if (!el) return;
        const now = new Date();
        const options = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' };
        el.textContent = now.toLocaleDateString('en-IN', options);
    }

    getInlineMenuHTML() {
        return `
<div class="left-menu-wrapper">
    <aside class="left-panel" id="leftPanel">
        <button class="panel-toggle" id="panelToggle" aria-label="Toggle menu"><i class="fas fa-angles-left" id="panelToggleIcon"></i></button>
        <div class="panel-header">
            <a href="dashboard.html" class="logo-link">
                <img src="tradentical-logo.png" alt="Tradentical" class="panel-logo">
            </a>
        </div>
        <nav class="panel-nav">
            <div class="theme-toggle" id="themeToggle"><button class="theme-btn" data-theme="light" aria-label="Light mode"><i class="fas fa-sun"></i></button><button class="theme-btn" data-theme="dark" aria-label="Dark mode"><i class="fas fa-moon"></i></button></div>
            <ul class="nav-list">
                <li class="nav-item" data-page="dashboard.html"><i class="fas fa-chart-line"></i><span>Dashboard</span></li>
                <li class="nav-item" data-page="cargpt.html"><i class="fas fa-chart-area"></i><span>Cumulative Avg</span></li>
                <li class="nav-item" data-page="broker.html"><i class="fas fa-university"></i><span>Broker Management</span></li>
                <li class="nav-item" data-page="strategy.html"><i class="fas fa-robot"></i><span>Strategy Management</span></li>
                <li class="nav-item" data-page="reports.html"><i class="fas fa-chart-bar"></i><span>Reports</span></li>
                <li class="nav-item" data-page="settings.html"><i class="fas fa-cog"></i><span>Settings</span></li>
                <li class="nav-item nav-item-logout" data-action="logout"><i class="fas fa-sign-out-alt"></i><span>Logout</span></li>
            </ul>
        </nav>
        <div class="panel-footer">
            <div class="user-info">
                <img src="https://via.placeholder.com/40" alt="User" class="user-avatar" id="userAvatar">
                <div class="user-details"><span class="user-name" id="userName">User</span><span class="user-datetime" id="userDateTime">--</span></div>
            </div>
        </div>
    </aside>
</div>
<button class="mobile-menu-toggle" id="leftMenuMobileToggle" aria-label="Open menu"><i class="fas fa-bars"></i></button>
<div class="mobile-menu-overlay" id="mobileMenuOverlay"></div>`;
    }

    setupCollapseToggle() {
        const panel = document.getElementById('leftPanel');
        const toggle = document.getElementById('panelToggle');
        const toggleIcon = document.getElementById('panelToggleIcon');
        const mainContent = this.getMainContent();

        if (!panel || !toggle) return;

        if (this.collapsed) {
            panel.classList.add('collapsed');
            if (mainContent) mainContent.classList.add('menu-collapsed');
            if (toggleIcon) {
                toggleIcon.className = 'fas fa-angles-right';
            }
        } else if (toggleIcon) {
            toggleIcon.className = 'fas fa-angles-left';
        }

        toggle.addEventListener('click', () => {
            this.collapsed = !this.collapsed;
            localStorage.setItem('leftMenuCollapsed', this.collapsed);
            panel.classList.toggle('collapsed', this.collapsed);
            if (mainContent) mainContent.classList.toggle('menu-collapsed', this.collapsed);
            if (toggleIcon) toggleIcon.className = this.collapsed ? 'fas fa-angles-right' : 'fas fa-angles-left';
        });
    }

    getMainContent() {
        const container = document.getElementById('left-menu-container');
        if (!container) return null;
        const sibling = container.nextElementSibling;
        if (sibling && (sibling.classList.contains('right-panel') || sibling.classList.contains('main-content-area') || sibling.tagName === 'MAIN')) {
            sibling.classList.add('main-content-area');
            return sibling;
        }
        return document.querySelector('.right-panel') || document.querySelector('.main-content-area') || document.querySelector('main');
    }

    syncMainContentMargin() {
        const mainContent = this.getMainContent();
        if (mainContent && this.collapsed) {
            mainContent.classList.add('menu-collapsed');
        }
    }

    setupMobileMenu() {
        const panel = document.getElementById('leftPanel');
        const overlay = document.getElementById('mobileMenuOverlay');
        const toggle = document.getElementById('leftMenuMobileToggle') || document.getElementById('mobileMenuToggle');

        if (!panel) return;

        const open = () => {
            panel.classList.add('mobile-open');
            if (overlay) overlay.classList.add('visible');
        };
        const close = () => {
            panel.classList.remove('mobile-open');
            if (overlay) overlay.classList.remove('visible');
        };

        if (toggle) toggle.addEventListener('click', () => panel.classList.contains('mobile-open') ? close() : open());
        if (overlay) overlay.addEventListener('click', close);

        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', () => setTimeout(close, 150));
        });
    }

    loadUserData() {
        const userData = localStorage.getItem('trademanthan_user');
        if (userData) {
            try {
                const user = JSON.parse(userData);
                const el = document.getElementById('userName');
                const avatar = document.getElementById('userAvatar');
                if (el) el.textContent = user.name || 'User';
                if (avatar && user.picture) avatar.src = user.picture;
            } catch (e) {}
        }
    }

    setupNavigation() {
        document.querySelectorAll('.nav-item[data-page]').forEach(item => {
            item.addEventListener('click', () => {
                const page = item.dataset.page;
                if (page) window.location.replace(page);
            });
        });
        document.querySelectorAll('.nav-item[data-action="logout"]').forEach(item => {
            item.addEventListener('click', () => LeftMenu.logout());
        });
    }

    setActiveNavigation() {
        const targetPage = this.getTargetPageForSection();
        document.querySelectorAll('.nav-item[data-page]').forEach(item => {
            item.classList.toggle('active', item.dataset.page === targetPage);
        });
    }

    getTargetPageForSection() {
        switch (this.currentPage) {
            case 'dashboard': return 'dashboard.html';
            case 'cargpt': return 'cargpt.html';
            case 'broker': return 'broker.html';
            case 'strategy': return 'strategy.html';
            case 'reports': return 'reports.html';
            case 'settings': return 'settings.html';
            default: return 'dashboard.html';
        }
    }

    static logout() {
        localStorage.removeItem('trademanthan_user');
        localStorage.removeItem('trademanthan_token');
        window.location.href = 'index.html';
    }
}

function logout() {
    LeftMenu.logout();
}

document.addEventListener('DOMContentLoaded', () => new LeftMenu());

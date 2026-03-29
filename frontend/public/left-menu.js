/**
 * Tradentical Left Menu - Unified panel for all post-login pages
 * Loads left-menu.html, handles collapse/expand, navigation, auth
 */
let isAuthenticating = false;
let hasRedirected = false;
let isAuthenticated = false;

const MENU_HTML_PATH = 'left-menu.html?v=3.12';
const DISCLAIMER_SCRIPT_PATH = 'disclaimer.js?v=1.1';
const NOTIFY_TRADE_CHANNEL_SCRIPT = 'notify-trade-channel.js?v=3';

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
        return /dashboard|cargpt|broker|strategy|reports|settings|carsetup|arbitrage|pivot-breakout|intraoption|admintwc/.test(path);
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
        if (path.includes('intraoption')) return 'intraoption';
        if (path.includes('pivot-breakout')) return 'pivot-breakout';
        if (path.includes('arbitrage')) return 'arbitrage';
        if (path.includes('carsetup') || path.includes('cargpt')) return 'cargpt';
        if (path.includes('admintwc')) return 'admin';
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
                // Show Admin from login payload before /auth/me returns
                this.applyAdminNavVisibility();
                await this.refreshUserProfileFromApi();
                this.applyAdminNavVisibility();
                this.injectMobileFooter();
                this.injectPanelSheetHandle();
                this.setupCollapseToggle();
                this.setupMobileMenu();
                this.setupMobileFooterIndices();
                this.syncMobileTitle();
                this.loadUserData();
                this.setupNavigation();
                this.setActiveNavigation();
                this.syncMainContentMargin();
                await this.setupDisclaimer();
                await this.setupTelegramNotifyModal();
            } else {
                const currentPath = window.location.pathname;
                const isProtectedPage = currentPath.includes('dashboard') || currentPath.includes('strategy') ||
                    currentPath.includes('broker') || currentPath.includes('algo') || currentPath.includes('scan') ||
                    currentPath.includes('reports') || currentPath.includes('settings') ||
                    currentPath.includes('carsetup') || currentPath.includes('cargpt') ||
                    currentPath.includes('arbitrage') || currentPath.includes('pivot-breakout') ||
                    currentPath.includes('intraoption') || currentPath.includes('admintwc');
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

        this.setupThemeToggle();
        this.updateDateTime();
        setInterval(() => this.updateDateTime(), 1000);
    }

    /**
     * True if user is system admin (DB isAdmin column "Yes"; API exposes camelCase isAdmin).
     * Accepts case/whitespace variants and legacy is_admin key.
     */
    static isUserAdmin(user) {
        if (!user || typeof user !== 'object') return false;
        const raw = user.isAdmin != null ? user.isAdmin : user.is_admin;
        if (raw == null || raw === '') return false;
        return String(raw).trim().toLowerCase() === 'yes';
    }

    /** Merge /auth/me into localStorage so id / isAdmin / page_permitted stay current */
    async refreshUserProfileFromApi() {
        try {
            const token = localStorage.getItem('trademanthan_token');
            // Always try JWT-capable sessions. Legacy google_token_/demo_token_ will 401 — ignored.
            if (!token) return;
            const paths = ['/api/auth/me', '/auth/me'];
            let me = null;
            for (const path of paths) {
                try {
                    const res = await fetch(path, {
                        headers: { Authorization: `Bearer ${token}` },
                        cache: 'no-store',
                    });
                    if (res.ok) {
                        me = await res.json();
                        break;
                    }
                } catch (err) {
                    console.warn('LeftMenu: auth/me try', path, err);
                }
            }
            if (!me) return;
            const prev = JSON.parse(localStorage.getItem('trademanthan_user') || '{}');
            const merged = { ...prev, ...me };
            localStorage.setItem('trademanthan_user', JSON.stringify(merged));
            this.applyAdminNavVisibility();
            this.loadUserData();
            try {
                window.dispatchEvent(new CustomEvent('tradentical:user-updated', { detail: { user: merged } }));
            } catch (e) { /* ignore */ }
        } catch (e) {
            console.warn('LeftMenu: refreshUserProfileFromApi', e);
        }
    }

    /** Show Admin nav link when user is admin (isAdmin Yes in DB) */
    applyAdminNavVisibility() {
        let user = {};
        try {
            user = JSON.parse(localStorage.getItem('trademanthan_user') || '{}');
        } catch (e) {}
        const show = LeftMenu.isUserAdmin(user);
        document.querySelectorAll('.nav-item.nav-item-admin[data-page="admintwc.html"]').forEach((el) => {
            el.style.display = show ? 'flex' : 'none';
        });
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
                <img src="tradewithcto-logo.png" alt="TradeWithCTO" class="panel-logo">
            </a>
        </div>
        <nav class="panel-nav">
            <div class="panel-nav-toolbar">
                <div class="theme-toggle" id="themeToggle">
                    <button type="button" class="theme-btn" data-theme="light" data-tooltip="Light Theme" title="Light Theme" aria-label="Light Theme"><i class="fas fa-sun"></i></button>
                    <button type="button" class="theme-btn active" data-theme="dark" data-tooltip="Dark Theme" title="Dark Theme" aria-label="Dark Theme"><i class="fas fa-moon"></i></button>
                </div>
                <button type="button" class="panel-nav-telegram-btn" id="leftMenuTelegramBtn" data-tooltip="Support" title="Support" aria-label="Support"><i class="fab fa-telegram" aria-hidden="true"></i></button>
            </div>
            <ul class="nav-list">
                <li class="nav-item" data-page="dashboard.html"><i class="fas fa-chart-line"></i><span>Dashboard</span></li>
                <li class="nav-item" data-page="intraoption.html"><i class="fas fa-bolt"></i><span>Intraday Option</span></li>
                <li class="nav-item" data-page="pivot-breakout.html"><i class="fas fa-bullseye"></i><span>Pivot Breakout</span></li>
                <li class="nav-item" data-page="arbitrage.html"><i class="fas fa-shuffle"></i><span>Arbitrage Selection</span></li>
                <li class="nav-item" data-page="cargpt.html"><i class="fas fa-chart-area"></i><span>Composite Avg</span></li>
                <li class="nav-item" data-page="broker.html"><i class="fas fa-university"></i><span>Broker Management</span></li>
                <li class="nav-item" data-page="strategy.html"><i class="fas fa-robot"></i><span>Strategy Management</span></li>
                <li class="nav-item" data-page="reports.html"><i class="fas fa-chart-bar"></i><span>Reports</span></li>
                <li class="nav-item" data-page="settings.html"><i class="fas fa-cog"></i><span>Settings</span></li>
                <li class="nav-item nav-item-admin" data-page="admintwc.html" style="display: none;" title="Administrator only"><i class="fas fa-user-shield"></i><span>Admin</span></li>
                <li class="nav-item nav-item-logout" data-action="logout"><i class="fas fa-sign-out-alt"></i><span>Logout</span></li>
            </ul>
        </nav>
        <div class="panel-footer">
            <div class="user-info">
                <img src="https://via.placeholder.com/40" alt="User" class="user-avatar" id="userAvatar">
                <div class="user-details"><span class="user-name" id="userName">User</span><span class="user-meta" id="userMetaLine" hidden></span><span class="user-datetime" id="userDateTime">--</span></div>
            </div>
            <div class="panel-footer-links">
                <a href="#" class="disclaimer-link">Disclaimer</a>
            </div>
        </div>
    </aside>
</div>
<button class="mobile-menu-toggle" id="leftMenuMobileToggle" aria-label="Open menu"><i class="fas fa-bars"></i></button>
<div class="mobile-menu-overlay" id="mobileMenuOverlay"></div>`;
    }

    async setupDisclaimer() {
        await this.loadDisclaimerScript();
        if (!window.TradenticalDisclaimer) return;
        window.TradenticalDisclaimer.bindLinks();

        const isDashboard = this.currentPage === 'dashboard';
        const sessionAccepted = window.TradenticalDisclaimer.isSessionAccepted
            ? window.TradenticalDisclaimer.isSessionAccepted()
            : false;
        if (isDashboard && !sessionAccepted) {
            // Enforce acknowledgement once per browser session on dashboard.
            window.TradenticalDisclaimer.open(true);
        }
    }

    loadDisclaimerScript() {
        if (window.TradenticalDisclaimer) return Promise.resolve();
        if (window.__tmDisclaimerLoading) return window.__tmDisclaimerLoading;

        window.__tmDisclaimerLoading = new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = DISCLAIMER_SCRIPT_PATH;
            script.onload = () => resolve();
            script.onerror = () => reject(new Error('Failed to load disclaimer.js'));
            document.head.appendChild(script);
        });

        return window.__tmDisclaimerLoading.catch((error) => {
            console.warn(error);
        });
    }

    loadNotifyTradeChannelScript() {
        if (typeof window.notifyTelegramUserMessage === 'function') return Promise.resolve();
        if (window.__tmNotifyScriptLoading) return window.__tmNotifyScriptLoading;
        window.__tmNotifyScriptLoading = new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.src = NOTIFY_TRADE_CHANNEL_SCRIPT;
            script.onload = () => resolve();
            script.onerror = () => reject(new Error('Failed to load notify-trade-channel.js'));
            document.head.appendChild(script);
        });
        return window.__tmNotifyScriptLoading.catch((e) => {
            console.warn('LeftMenu:', e);
        });
    }

    injectTelegramNotifyModal() {
        if (document.getElementById('tmTelegramNotifyModal')) return;
        const wrap = document.createElement('div');
        wrap.innerHTML = `
<div id="tmTelegramNotifyModal" class="tm-telegram-modal" role="dialog" aria-modal="true" aria-labelledby="tmTelegramNotifyTitle">
    <div class="tm-telegram-modal__backdrop" aria-hidden="true"></div>
    <div class="tm-telegram-modal__box">
        <h2 id="tmTelegramNotifyTitle" class="tm-telegram-modal__title">Message TradeWithCTO</h2>
        <p class="tm-telegram-modal__hint">Your text is sent to the Telegram channel <strong>@TradeWithCTO</strong>, with your account name appended.</p>
        <label class="tm-telegram-modal__label" for="tmTelegramNotifyText">Message</label>
        <textarea id="tmTelegramNotifyText" class="tm-telegram-modal__textarea" rows="5" maxlength="2000" placeholder="Type your message…"></textarea>
        <div class="tm-telegram-modal__actions">
            <button type="button" class="tm-telegram-modal__btn tm-telegram-modal__btn--primary" id="tmTelegramNotifySend">Notify</button>
            <button type="button" class="tm-telegram-modal__btn tm-telegram-modal__btn--secondary" id="tmTelegramNotifyClose">Close</button>
        </div>
        <p id="tmTelegramNotifyStatus" class="tm-telegram-modal__status" role="status"></p>
    </div>
</div>`;
        document.body.appendChild(wrap.firstElementChild);
    }

    async setupTelegramNotifyModal() {
        try {
            await this.loadNotifyTradeChannelScript();
        } catch (e) {
            console.warn('LeftMenu: notify script', e);
        }
        this.injectTelegramNotifyModal();
        const btn = document.getElementById('leftMenuTelegramBtn');
        const modal = document.getElementById('tmTelegramNotifyModal');
        const closeBtn = document.getElementById('tmTelegramNotifyClose');
        const notifyBtn = document.getElementById('tmTelegramNotifySend');
        const textarea = document.getElementById('tmTelegramNotifyText');
        const statusEl = document.getElementById('tmTelegramNotifyStatus');
        if (!btn || !modal) return;

        const close = () => {
            modal.classList.remove('show');
            document.body.style.overflow = '';
        };

        const open = () => {
            if (statusEl) statusEl.textContent = '';
            if (textarea) textarea.value = '';
            modal.classList.add('show');
            document.body.style.overflow = 'hidden';
            setTimeout(() => textarea?.focus(), 50);
        };

        btn.addEventListener('click', (e) => {
            e.preventDefault();
            open();
        });

        closeBtn?.addEventListener('click', close);
        modal.addEventListener('click', (e) => {
            if (e.target === modal) close();
        });

        notifyBtn?.addEventListener('click', async () => {
            const msg = (textarea?.value || '').trim();
            if (!msg) {
                if (statusEl) statusEl.textContent = 'Please enter a message.';
                return;
            }
            if (typeof window.notifyTelegramUserMessage !== 'function') {
                if (statusEl) statusEl.textContent = 'Notify unavailable. Refresh the page.';
                return;
            }
            if (statusEl) statusEl.textContent = 'Sending...';
            try {
                await window.notifyTelegramUserMessage(msg);
                if (statusEl) statusEl.textContent = 'Sent. Thank you.';
            } catch (err) {
                if (statusEl) statusEl.textContent = err.message || 'Failed to send.';
            }
        });

        document.addEventListener('keydown', (ev) => {
            if (ev.key !== 'Escape') return;
            if (modal.classList.contains('show')) close();
        });
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
        const toggles = [
            document.getElementById('mobileMenuToggle'),
            document.getElementById('leftMenuMobileToggle'),
            document.getElementById('tmFooterNavToggle'),
        ].filter(Boolean);

        if (!panel) return;

        const footerBtn = document.getElementById('tmFooterNavToggle');

        const setExpanded = (open) => {
            if (footerBtn) footerBtn.setAttribute('aria-expanded', open ? 'true' : 'false');
        };

        const open = () => {
            panel.classList.add('mobile-open');
            if (overlay) overlay.classList.add('visible');
            document.body.classList.add('left-menu-mobile-open');
            setExpanded(true);
        };
        const close = () => {
            panel.classList.remove('mobile-open');
            if (overlay) overlay.classList.remove('visible');
            document.body.classList.remove('left-menu-mobile-open');
            setExpanded(false);
        };

        this.closeMobileNav = close;
        window.closeMobileNavSheet = close;

        toggles.forEach((toggle) => {
            toggle.addEventListener('click', () => (panel.classList.contains('mobile-open') ? close() : open()));
        });
        if (overlay) overlay.addEventListener('click', close);

        document.querySelectorAll('.nav-item').forEach((item) => {
            item.addEventListener('click', () => setTimeout(close, 150));
        });

        document.addEventListener('keydown', (ev) => {
            if (ev.key === 'Escape' && panel.classList.contains('mobile-open')) close();
        });
    }

    injectMobileFooter() {
        if (document.getElementById('tmMobileFooter')) return;

        const liveBadge =
            this.currentPage === 'intraoption'
                ? '<span id="mobileLiveBadge" class="mobile-live-badge" aria-hidden="true">L</span>'
                : '';

        const html = `
<div id="tmMobileFooter" class="tm-mobile-footer" role="navigation" aria-label="Indices and app menu">
    <div class="tm-footer-index tm-footer-index-left" id="tmFooterNiftyWrap" role="button" tabindex="0">
        <div class="footer-index-row">
            <span class="footer-index-name">NIFTY50</span>
            <span class="footer-index-arrow" id="footer-nifty-arrow">↑</span>
        </div>
        <div class="footer-index-price" id="footer-nifty-price">...</div>
    </div>
    <div class="tm-footer-center">
        ${liveBadge}
        <button type="button" id="tmFooterNavToggle" class="tm-footer-nav-btn" aria-label="Open navigation menu" aria-expanded="false">
            <i class="fas fa-bars"></i>
        </button>
    </div>
    <div class="tm-footer-index tm-footer-index-right" id="tmFooterBankWrap" role="button" tabindex="0">
        <div class="footer-index-row">
            <span class="footer-index-name">BANKNIFTY</span>
            <span class="footer-index-arrow" id="footer-banknifty-arrow">↑</span>
        </div>
        <div class="footer-index-price" id="footer-banknifty-price">...</div>
    </div>
</div>`;

        document.body.insertAdjacentHTML('beforeend', html);
        document.body.classList.add('left-menu-footer-mode');

        const refresh = () => {
            if (typeof window.loadIndexPrices === 'function') {
                window.loadIndexPrices();
            } else {
                this.refreshMobileFooterIndices();
            }
        };

        document.getElementById('tmFooterNiftyWrap')?.addEventListener('click', refresh);
        document.getElementById('tmFooterBankWrap')?.addEventListener('click', refresh);
        document.getElementById('tmFooterNiftyWrap')?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                refresh();
            }
        });
        document.getElementById('tmFooterBankWrap')?.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                refresh();
            }
        });
    }

    injectPanelSheetHandle() {
        const panel = document.getElementById('leftPanel');
        if (!panel || panel.querySelector('.left-panel-sheet-handle')) return;
        const handle = document.createElement('div');
        handle.className = 'left-panel-sheet-handle';
        handle.setAttribute('aria-hidden', 'true');
        handle.innerHTML = '<div class="left-panel-sheet-handle-bar"></div>';
        handle.addEventListener('click', () => {
            if (window.innerWidth <= 1024 && typeof this.closeMobileNav === 'function') {
                this.closeMobileNav();
            }
        });
        panel.insertBefore(handle, panel.firstChild);
    }

    setupMobileFooterIndices() {
        const run = () => {
            if (typeof window.loadIndexPrices === 'function') {
                window.loadIndexPrices();
            } else {
                this.refreshMobileFooterIndices();
            }
        };
        run();
        setInterval(run, 60000);
    }

    refreshMobileFooterIndices() {
        const API_BASE =
            window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
                ? 'http://localhost:8000'
                : 'https://trademanthan.in';

        const formatPrice = (price) => {
            if (price === null || price === undefined || Number.isNaN(Number(price))) return '--';
            return Number(price).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        };

        fetch(`${API_BASE}/scan/index-prices`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' },
        })
            .then((r) => r.json())
            .then((result) => {
                if (result.status === 'success' && result.data) {
                    this.updateFooterIndicesFromData(result.data, formatPrice);
                }
            })
            .catch(() => {});
    }

    updateFooterIndicesFromData(data, formatPrice) {
        const footerNiftyPrice = document.getElementById('footer-nifty-price');
        const footerNiftyArrow = document.getElementById('footer-nifty-arrow');
        const footerBankPrice = document.getElementById('footer-banknifty-price');
        const footerBankArrow = document.getElementById('footer-banknifty-arrow');

        if (footerNiftyPrice && footerNiftyArrow && data.nifty) {
            const price = data.market_status === 'closed' ? data.nifty.close_price : data.nifty.ltp;
            footerNiftyPrice.textContent = '₹' + formatPrice(price);
            footerNiftyArrow.className = 'footer-index-arrow';
            if (data.nifty.trend === 'bullish') {
                footerNiftyArrow.textContent = '↑';
                footerNiftyArrow.classList.add('bullish');
            } else if (data.nifty.trend === 'bearish') {
                footerNiftyArrow.textContent = '↓';
                footerNiftyArrow.classList.add('bearish');
            } else {
                footerNiftyArrow.textContent = '→';
                footerNiftyArrow.classList.add('neutral');
            }
        }

        if (footerBankPrice && footerBankArrow && data.banknifty) {
            const price = data.market_status === 'closed' ? data.banknifty.close_price : data.banknifty.ltp;
            footerBankPrice.textContent = '₹' + formatPrice(price);
            footerBankArrow.className = 'footer-index-arrow';
            if (data.banknifty.trend === 'bullish') {
                footerBankArrow.textContent = '↑';
                footerBankArrow.classList.add('bullish');
            } else if (data.banknifty.trend === 'bearish') {
                footerBankArrow.textContent = '↓';
                footerBankArrow.classList.add('bearish');
            } else {
                footerBankArrow.textContent = '→';
                footerBankArrow.classList.add('neutral');
            }
        }
    }

    syncMobileTitle() {
        const mobileTitle = document.querySelector('.mobile-title');
        if (!mobileTitle) return;

        const pageTitles = {
            dashboard: 'Dashboard',
            intraoption: 'Intraday Stock Options Algo',
            'pivot-breakout': 'Pivot Breakout',
            arbitrage: 'Arbitrage Selection',
            cargpt: 'Composite Average Reversal',
            broker: 'Broker Management',
            strategy: 'Strategy Management',
            reports: 'Trading Reports',
            settings: 'Settings',
            algo: 'Algo Trading',
            admin: 'Admin',
        };

        mobileTitle.textContent = pageTitles[this.currentPage] || 'Tradentical';
    }

    loadUserData() {
        const userData = localStorage.getItem('trademanthan_user');
        if (!userData) return;
        try {
            const user = JSON.parse(userData);
            const el = document.getElementById('userName');
            const avatar = document.getElementById('userAvatar');
            const meta = document.getElementById('userMetaLine');
            if (el) el.textContent = user.name || user.full_name || 'User';
            if (avatar && user.picture) avatar.src = user.picture;

            let uid = user.id ?? user.user_id;
            if (uid == null || uid === '') {
                try {
                    const token = localStorage.getItem('trademanthan_token');
                    if (token && token.includes('.')) {
                        const parts = String(token).split('.');
                        if (parts.length === 3) {
                            let b64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
                            while (b64.length % 4) b64 += '=';
                            const payload = JSON.parse(atob(b64));
                            if (payload && payload.sub != null) uid = payload.sub;
                        }
                    }
                } catch (e) { /* ignore */ }
            }
            if (meta) {
                const parts = [];
                if (uid != null && uid !== '') parts.push(`User ID: ${uid}`);
                if (LeftMenu.isUserAdmin(user)) parts.push('Admin');
                meta.textContent = parts.join(' · ');
                meta.hidden = parts.length === 0;
            }
        } catch (e) {}
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
            case 'intraoption': return 'intraoption.html';
            case 'pivot-breakout': return 'pivot-breakout.html';
            case 'arbitrage': return 'arbitrage.html';
            case 'settings': return 'settings.html';
            case 'admin': return 'admintwc.html';
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

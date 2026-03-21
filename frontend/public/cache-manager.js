/**
 * Cache Management Utility for Trade Manthan
 * Provides manual cache management and refresh options
 * Mobile: floats above the app footer band; draggable; smaller icon
 */

const CACHE_FLOAT_POS_KEY = 'tm_cache_float_pos';
const CACHE_FLOAT_CUSTOM_KEY = 'tm_cache_float_custom';

class CacheManager {
    constructor() {
        this._dragMoved = false;
        this._dragging = false;
        this._dragStartX = 0;
        this._dragStartY = 0;
        this._elStartLeft = 0;
        this._elStartTop = 0;
        this._floatWrap = null;
        this.init();
    }

    init() {
        this.addCacheManagementUI();
        this.setupCacheMonitoring();
        console.log('🔧 Cache Manager initialized');
    }

    /** Space reserved at bottom for mobile footer band (tradewithcto / left-menu-footer-mode) */
    getMobileFooterReserve() {
        if (!window.matchMedia('(max-width: 1024px)').matches) return 20;
        if (document.body.classList.contains('left-menu-footer-mode')) {
            return 72; /* ~64px bar + gap above it */
        }
        return 20;
    }

    getFloatButtonSize() {
        return window.matchMedia('(max-width: 1024px)').matches ? 36 : 50;
    }

    /** Apply default or saved position; clamp into viewport */
    applyFloatPosition(isInitial) {
        const wrap = this._floatWrap || document.getElementById('tmCacheFloatWrap');
        if (!wrap) return;

        const margin = 12;
        const size = this.getFloatButtonSize();
        const w = window.innerWidth;
        const h = window.innerHeight;
        const bottomReserve = this.getMobileFooterReserve();

        const custom = localStorage.getItem(CACHE_FLOAT_CUSTOM_KEY) === '1';
        const raw = localStorage.getItem(CACHE_FLOAT_POS_KEY);

        if (custom && raw) {
            try {
                const pos = JSON.parse(raw);
                let left = Number(pos.left);
                let top = Number(pos.top);
                left = Math.max(margin, Math.min(w - size - margin, left));
                top = Math.max(margin, Math.min(h - size - margin, top));
                wrap.style.left = `${left}px`;
                wrap.style.top = `${top}px`;
                wrap.style.right = 'auto';
                wrap.style.bottom = 'auto';
                return;
            } catch (e) {
                /* fall through */
            }
        }

        /* Default: bottom-right, above mobile footer */
        const left = w - size - margin;
        const top = h - size - bottomReserve;
        wrap.style.left = `${Math.max(margin, left)}px`;
        wrap.style.top = `${Math.max(margin, top)}px`;
        wrap.style.right = 'auto';
        wrap.style.bottom = 'auto';
    }

    saveFloatPosition(wrap) {
        const r = wrap.getBoundingClientRect();
        localStorage.setItem(
            CACHE_FLOAT_POS_KEY,
            JSON.stringify({ left: Math.round(r.left), top: Math.round(r.top) })
        );
        localStorage.setItem(CACHE_FLOAT_CUSTOM_KEY, '1');
    }

    attachDrag(wrap) {
        const onPointerDown = (e) => {
            if (e.pointerType === 'mouse' && e.button !== 0) return;
            document.getElementById('cacheMenu')?.classList.remove('show');
            this._dragging = true;
            this._dragMoved = false;
            this._dragStartX = e.clientX;
            this._dragStartY = e.clientY;
            const r = wrap.getBoundingClientRect();
            this._elStartLeft = r.left;
            this._elStartTop = r.top;
            wrap.classList.add('tm-cache-dragging');
            try {
                wrap.setPointerCapture(e.pointerId);
            } catch (err) {
                /* ignore */
            }
        };

        const onPointerMove = (e) => {
            if (!this._dragging) return;
            const dx = e.clientX - this._dragStartX;
            const dy = e.clientY - this._dragStartY;
            if (Math.abs(dx) + Math.abs(dy) > 6) this._dragMoved = true;

            const size = this.getFloatButtonSize();
            const margin = 8;
            let left = this._elStartLeft + dx;
            let top = this._elStartTop + dy;
            left = Math.max(margin, Math.min(window.innerWidth - size - margin, left));
            top = Math.max(margin, Math.min(window.innerHeight - size - margin, top));

            wrap.style.left = `${left}px`;
            wrap.style.top = `${top}px`;
            wrap.style.right = 'auto';
            wrap.style.bottom = 'auto';
        };

        const onPointerUp = (e) => {
            if (!this._dragging) return;
            this._dragging = false;
            wrap.classList.remove('tm-cache-dragging');
            try {
                wrap.releasePointerCapture(e.pointerId);
            } catch (err) {
                /* ignore */
            }
            if (this._dragMoved) {
                this.saveFloatPosition(wrap);
            } else {
                this.showCacheMenu();
            }
        };

        wrap.addEventListener('pointerdown', onPointerDown);
        wrap.addEventListener('pointermove', onPointerMove);
        wrap.addEventListener('pointerup', onPointerUp);
        wrap.addEventListener('pointercancel', onPointerUp);
    }

    addCacheManagementUI() {
        const cacheButton = document.createElement('div');
        cacheButton.id = 'tmCacheFloatWrap';
        cacheButton.className = 'cache-management-button';
        cacheButton.innerHTML = `
            <button type="button" id="tmCacheFloatBtn" title="Cache Management — drag to move">
                <i class="fas fa-sync-alt"></i>
            </button>
        `;

        const style = document.createElement('style');
        style.id = 'tmCacheManagerStyles';
        style.textContent = `
            .cache-management-button {
                position: fixed;
                z-index: 10006;
                touch-action: none;
                user-select: none;
                -webkit-user-select: none;
            }

            .cache-management-button.tm-cache-dragging {
                cursor: grabbing;
            }

            .cache-management-button button {
                background: linear-gradient(135deg, #2196f3, #1976d2);
                color: white;
                border: none;
                border-radius: 50%;
                width: 50px;
                height: 50px;
                cursor: grab;
                box-shadow: 0 4px 15px rgba(0,0,0,0.3);
                transition: box-shadow 0.2s ease, transform 0.15s ease;
                font-size: 18px;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 0;
            }

            .cache-management-button button:hover {
                box-shadow: 0 6px 20px rgba(0,0,0,0.4);
            }

            .cache-management-button.tm-cache-dragging button {
                transform: scale(1.05);
                box-shadow: 0 8px 24px rgba(0,0,0,0.45);
            }

            @media (max-width: 1024px) {
                .cache-management-button button {
                    width: 36px;
                    height: 36px;
                    font-size: 14px;
                }
            }

            .cache-menu {
                position: fixed;
                background: white;
                border-radius: 10px;
                box-shadow: 0 8px 25px rgba(0,0,0,0.2);
                padding: 15px;
                min-width: 250px;
                max-width: calc(100vw - 24px);
                z-index: 10007;
                display: none;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }

            .cache-menu.show {
                display: block;
                animation: tmCacheMenuIn 0.25s ease-out;
            }

            @keyframes tmCacheMenuIn {
                from { transform: translateY(8px); opacity: 0; }
                to { transform: translateY(0); opacity: 1; }
            }

            .cache-menu h3 {
                margin: 0 0 15px 0;
                color: #1976d2;
                font-size: 16px;
                border-bottom: 2px solid #e3f2fd;
                padding-bottom: 8px;
            }

            .cache-menu button {
                background: #f5f5f5;
                border: 1px solid #ddd;
                padding: 8px 12px;
                margin: 5px;
                border-radius: 5px;
                cursor: pointer;
                font-size: 12px;
                transition: all 0.2s ease;
            }

            .cache-menu button:hover {
                background: #e3f2fd;
                border-color: #2196f3;
            }

            .cache-menu button.danger {
                background: #ffebee;
                border-color: #f44336;
                color: #d32f2f;
            }

            .cache-menu button.danger:hover {
                background: #ffcdd2;
            }

            .cache-status {
                font-size: 11px;
                color: #666;
                margin-top: 10px;
                padding-top: 10px;
                border-top: 1px solid #eee;
            }
        `;
        document.head.appendChild(style);

        this._floatWrap = cacheButton;
        document.body.appendChild(cacheButton);

        this.attachDrag(cacheButton);

        window.addEventListener(
            'resize',
            () => {
                this.applyFloatPosition(false);
                const menu = document.getElementById('cacheMenu');
                if (menu && menu.classList.contains('show')) {
                    requestAnimationFrame(() => this.positionCacheMenu());
                }
            },
            { passive: true }
        );

        this.applyFloatPosition(true);
        this.createCacheMenu();

        /* Reposition after left-menu mobile footer may attach (body.left-menu-footer-mode) */
        [250, 800, 2000].forEach((t) => {
            setTimeout(() => {
                if (localStorage.getItem(CACHE_FLOAT_CUSTOM_KEY) !== '1') {
                    this.applyFloatPosition(false);
                }
            }, t);
        });
    }

    createCacheMenu() {
        const menu = document.createElement('div');
        menu.className = 'cache-menu';
        menu.id = 'cacheMenu';

        const versionInfo = window.VERSION_INFO || { version: 'Unknown', cacheBuster: 'Unknown' };

        menu.innerHTML = `
            <h3><i class="fas fa-cog"></i> Cache Management</h3>

            <button type="button" onclick="cacheManager.refreshCurrentPage()">
                <i class="fas fa-sync-alt"></i> Refresh Page
            </button>

            <button type="button" onclick="cacheManager.refreshAllResources()">
                <i class="fas fa-redo"></i> Refresh All Resources
            </button>

            <button type="button" onclick="cacheManager.clearBrowserCache()" class="danger">
                <i class="fas fa-trash"></i> Clear Browser Cache
            </button>

            <button type="button" onclick="cacheManager.forceHardRefresh()" class="danger">
                <i class="fas fa-exclamation-triangle"></i> Force Hard Refresh
            </button>

            <div class="cache-status">
                <div>Version: ${versionInfo.version}</div>
                <div>Cache: ${versionInfo.cacheBuster}</div>
                <div>Last: ${new Date().toLocaleTimeString()}</div>
            </div>
        `;

        document.body.appendChild(menu);
    }

    /** Position menu above the float when possible, else below; stay in viewport */
    positionCacheMenu() {
        const menu = document.getElementById('cacheMenu');
        const wrap = this._floatWrap || document.getElementById('tmCacheFloatWrap');
        if (!menu || !wrap) return;

        const mw = menu.offsetWidth || 250;
        const mh = menu.offsetHeight || 200;
        const r = wrap.getBoundingClientRect();
        const pad = 8;
        const vw = window.innerWidth;
        const vh = window.innerHeight;

        let left = r.left + r.width / 2 - mw / 2;
        left = Math.max(pad, Math.min(vw - mw - pad, left));

        let top = r.top - mh - pad;
        if (top < pad) {
            top = r.bottom + pad;
        }
        if (top + mh > vh - pad) {
            top = Math.max(pad, vh - mh - pad);
        }

        menu.style.left = `${left}px`;
        menu.style.top = `${top}px`;
        menu.style.right = 'auto';
        menu.style.bottom = 'auto';
    }

    showCacheMenu() {
        const menu = document.getElementById('cacheMenu');
        if (!menu) return;

        const opening = !menu.classList.contains('show');
        if (opening) {
            menu.classList.add('show');
            clearTimeout(this._menuHideTimer);
            requestAnimationFrame(() => {
                this.positionCacheMenu();
                this._menuHideTimer = setTimeout(() => {
                    menu.classList.remove('show');
                }, 10000);
            });
        } else {
            menu.classList.remove('show');
            clearTimeout(this._menuHideTimer);
        }
    }

    refreshCurrentPage() {
        console.log('🔄 Refreshing current page...');
        location.reload();
    }

    refreshAllResources() {
        console.log('🔄 Refreshing all resources...');

        if (window.versionManager) {
            window.versionManager.autoVersionResources();
        }

        this.showNotification('All resources refreshed!', 'success');
    }

    clearBrowserCache() {
        console.log('🗑️ Clearing browser cache...');

        if ('caches' in window) {
            caches.keys().then((names) => {
                names.forEach((name) => {
                    caches.delete(name);
                });
                console.log('✅ Browser cache cleared');
                this.showNotification('Browser cache cleared!', 'success');
            });
        } else {
            this.showNotification('Cache API not supported', 'warning');
        }
    }

    forceHardRefresh() {
        console.log('💥 Force hard refresh...');

        if ('caches' in window) {
            caches.keys().then((names) => {
                names.forEach((name) => {
                    caches.delete(name);
                });
            });
        }

        localStorage.clear();
        sessionStorage.clear();

        window.location.reload(true);
    }

    setupCacheMonitoring() {
        window.addEventListener('error', (event) => {
            if (event.target.tagName === 'SCRIPT' || event.target.tagName === 'LINK') {
                console.warn('🔄 Resource loading error detected, suggesting cache refresh');
                this.suggestCacheRefresh();
            }
        });

        window.addEventListener('unhandledrejection', (event) => {
            if (event.reason && event.reason.message && event.reason.message.includes('404')) {
                console.warn('🔄 404 error detected, suggesting cache refresh');
                this.suggestCacheRefresh();
            }
        });
    }

    suggestCacheRefresh() {
        const suggestion = document.createElement('div');
        suggestion.className = 'cache-suggestion';
        suggestion.innerHTML = `
            <i class="fas fa-info-circle"></i>
            Resource loading issue detected.
            <button type="button" onclick="cacheManager.refreshAllResources()">Refresh Cache</button>
        `;

        suggestion.style.cssText = `
            position: fixed;
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            background: #fff3cd;
            color: #856404;
            padding: 10px 15px;
            border-radius: 5px;
            border: 1px solid #ffeaa7;
            z-index: 10008;
            font-size: 14px;
            display: flex;
            align-items: center;
            gap: 10px;
        `;

        document.body.appendChild(suggestion);

        setTimeout(() => {
            if (suggestion.parentElement) {
                suggestion.remove();
            }
        }, 8000);
    }

    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `cache-notification ${type}`;
        notification.textContent = message;

        notification.style.cssText = `
            position: fixed;
            top: 80px;
            right: 20px;
            background: ${type === 'success' ? '#4caf50' : type === 'warning' ? '#ff9800' : '#2196f3'};
            color: ${type === 'warning' ? 'black' : 'white'};
            padding: 10px 15px;
            border-radius: 5px;
            z-index: 10008;
            font-size: 14px;
            animation: slideIn 0.3s ease-out;
            white-space: nowrap;
        `;

        document.body.appendChild(notification);

        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 3000);
    }
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        window.cacheManager = new CacheManager();
    });
} else {
    window.cacheManager = new CacheManager();
}

if (typeof module !== 'undefined' && module.exports) {
    module.exports = CacheManager;
}

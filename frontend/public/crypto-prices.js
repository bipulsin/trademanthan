// Live Crypto Prices — Delta Exchange India via the app proxy.
// Cards keep their existing DOM ids. Prices refresh every 5 minutes.
class CryptoPriceManager {
    constructor() {
        this.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'DOTUSDT', 'MATICUSDT', 'LINKUSDT'];
        this.prices = {};
        this.lastUpdate = null;
        this.timerId = null;
        this.countdownId = null;
        this.isUpdating = false;
        this.minIntervalMs = 300000;
        this.refreshSeconds = 300;
        this.apiPath = '/scan/dashboard/crypto-prices';

        this.init();
    }

    init() {
        if (!this.isDashboardPage()) {
            return;
        }
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.initializeAfterDOM());
        } else {
            this.initializeAfterDOM();
        }
    }

    initializeAfterDOM() {
        if (!this.checkRequiredElements()) {
            return;
        }
        this.setupEventListeners();
        this.showInitialLoadingState();
        this.startPriceUpdates();
        this.startCountdownTimer();
    }

    isDashboardPage() {
        return window.location.pathname.includes('dashboard') ||
            window.location.pathname.includes('desktop') ||
            document.querySelector('.crypto-card') !== null;
    }

    checkRequiredElements() {
        return Boolean(document.getElementById('cryptoGrid') && document.getElementById('lastUpdated'));
    }

    showInitialLoadingState() {
        this.symbols.forEach(symbol => {
            const priceElement = document.getElementById(`${symbol}-price`);
            const changeElement = document.getElementById(`${symbol}-change`);
            if (priceElement) {
                priceElement.textContent = 'Loading...';
                priceElement.style.color = '#999';
            }
            if (changeElement) {
                changeElement.textContent = '--';
                changeElement.style.color = '#999';
            }
        });
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            lastUpdatedElement.textContent = 'Loading Delta Exchange India...';
        }
    }

    updateDebugInfo(status, elements, errors) {
        try {
            const statusDiv = document.getElementById('cryptoStatus');
            const elementsDiv = document.getElementById('cryptoElements');
            const errorsDiv = document.getElementById('cryptoErrors');
            if (statusDiv) statusDiv.textContent = `Status: ${status}`;
            if (elementsDiv) elementsDiv.textContent = `Elements: ${elements}`;
            if (errorsDiv) errorsDiv.textContent = `Errors: ${errors}`;
        } catch (error) {
            console.error('CryptoPriceManager: Error updating debug info:', error);
        }
    }

    setupEventListeners() {
        window.addEventListener('online', () => {
            this.showOnlineStatus();
            this.refreshPrices();
        });
        window.addEventListener('offline', () => {
            this.showOfflineStatus();
        });
    }

    startPriceUpdates() {
        this.fetchPrices();
        this.setUpdateInterval(this.getUpdateInterval());
    }

    getUpdateInterval() {
        let interval = this.minIntervalMs;
        try {
            const savedSettings = localStorage.getItem('trademanthan_settings');
            if (savedSettings) {
                const settings = JSON.parse(savedSettings);
                const configured = settings && settings.trading && settings.trading.crypto
                    ? settings.trading.crypto.updateInterval
                    : null;
                if (typeof configured === 'number' && configured > interval) {
                    interval = configured;
                }
            }
        } catch (error) {
            interval = this.minIntervalMs;
        }
        if (!Number.isFinite(interval) || interval < this.minIntervalMs) {
            interval = this.minIntervalMs;
        }
        return interval;
    }

    setUpdateInterval(interval) {
        const wait = Math.max(this.minIntervalMs, interval);
        if (this.timerId) {
            clearInterval(this.timerId);
        }
        this.timerId = setInterval(() => {
            this.fetchPrices();
        }, wait);
    }

    authHeaders() {
        const headers = { 'Accept': 'application/json', 'Cache-Control': 'no-cache' };
        try {
            const token = localStorage.getItem('trademanthan_token') || '';
            if (token) {
                headers.Authorization = 'Bearer ' + token;
            }
        } catch (error) {
            // Public market-data route still works without a token.
        }
        return headers;
    }

    async fetchPrices() {
        if (this.isUpdating) {
            return;
        }
        this.isUpdating = true;
        try {
            const response = await fetch(this.apiPath, {
                method: 'GET',
                cache: 'no-store',
                headers: this.authHeaders()
            });
            let payload = null;
            try {
                payload = await response.json();
            } catch (error) {
                payload = null;
            }
            if (!response.ok || !payload || payload.ok !== true || !Array.isArray(payload.prices)) {
                this.showUnavailable(payload && payload.error ? payload.error : 'unavailable');
                return;
            }
            if (typeof payload.refresh_seconds === 'number' && payload.refresh_seconds >= 300) {
                this.refreshSeconds = payload.refresh_seconds;
            }
            this.applyPayload(payload);
        } catch (error) {
            console.error('CryptoPriceManager: price fetch failed', error);
            this.showUnavailable('unavailable');
        } finally {
            this.isUpdating = false;
            this.clearRefreshingState();
        }
    }

    applyPayload(payload) {
        const byCard = {};
        payload.prices.forEach(row => {
            if (row && row.card) {
                byCard[row.card] = row;
            }
        });
        this.symbols.forEach(symbol => {
            const row = byCard[symbol];
            if (!row || row.available !== true || row.price == null || Number.isNaN(Number(row.price))) {
                this.markCardUnavailable(symbol);
                delete this.prices[symbol];
                return;
            }
            const data = {
                symbol: symbol,
                productSymbol: row.product_symbol || '',
                price: Number(row.price),
                changePercent: row.change_percent == null ? null : Number(row.change_percent)
            };
            this.prices[symbol] = data;
            this.updatePriceDisplay(symbol, data);
        });
        const stamp = payload.updated_at ? new Date(payload.updated_at) : new Date();
        this.lastUpdate = Number.isNaN(stamp.getTime()) ? new Date() : stamp;
        this.updateLastUpdatedDisplay();
        this.updateDebugInfo('Live', 'Delta Exchange India', 'No errors');
    }

    markCardUnavailable(symbol) {
        const priceElement = document.getElementById(`${symbol}-price`);
        const changeElement = document.getElementById(`${symbol}-change`);
        if (priceElement) {
            priceElement.textContent = 'Unavailable';
            priceElement.style.color = '#999';
        }
        if (changeElement) {
            changeElement.textContent = '--';
            changeElement.className = 'crypto-change';
            changeElement.style.color = '#999';
        }
    }

    showUnavailable(reason) {
        this.prices = {};
        this.lastUpdate = null;
        this.symbols.forEach(symbol => this.markCardUnavailable(symbol));
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            const checked = new Date().toLocaleTimeString();
            lastUpdatedElement.textContent = `Unavailable · checked ${checked}`;
            lastUpdatedElement.style.color = '#f44336';
        }
        this.updateDebugInfo('Unavailable', 'Delta Exchange India', reason || 'unavailable');
    }

    updatePriceDisplay(symbol, data) {
        const priceElement = document.getElementById(`${symbol}-price`);
        const changeElement = document.getElementById(`${symbol}-change`);
        if (!priceElement || !changeElement) {
            return;
        }
        priceElement.style.color = '';
        priceElement.textContent = this.formatPrice(data.price);
        if (data.changePercent == null || Number.isNaN(data.changePercent)) {
            changeElement.textContent = '--';
            changeElement.className = 'crypto-change';
            changeElement.style.color = '#999';
            return;
        }
        changeElement.style.color = '';
        changeElement.textContent = this.formatChange(data.changePercent);
        changeElement.className = 'crypto-change';
        if (data.changePercent > 0) {
            changeElement.classList.add('positive');
        } else if (data.changePercent < 0) {
            changeElement.classList.add('negative');
        } else {
            changeElement.classList.add('neutral');
        }
    }

    formatPrice(price) {
        if (price >= 1000) {
            return `$${price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        } else if (price >= 1) {
            return `$${price.toFixed(2)}`;
        }
        return `$${price.toFixed(4)}`;
    }

    formatChange(changePercent) {
        const sign = changePercent >= 0 ? '+' : '';
        return `${sign}${changePercent.toFixed(2)}%`;
    }

    updateLastUpdatedDisplay() {
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (!lastUpdatedElement || !this.lastUpdate) {
            return;
        }
        lastUpdatedElement.style.color = '';
        const clock = this.lastUpdate.toLocaleTimeString();
        const next = this.nextRefreshLabel();
        lastUpdatedElement.textContent = next
            ? `Last updated: ${clock} · ${next}`
            : `Last updated: ${clock}`;
    }

    nextRefreshLabel() {
        if (!this.lastUpdate) {
            return '';
        }
        const interval = Math.max(this.minIntervalMs, this.refreshSeconds * 1000);
        const remaining = this.lastUpdate.getTime() + interval - Date.now();
        if (remaining <= 0) {
            return 'Next update soon';
        }
        const minutes = Math.floor(remaining / 60000);
        const seconds = Math.floor((remaining % 60000) / 1000);
        return `Next update in ${minutes}:${seconds.toString().padStart(2, '0')}`;
    }

    startCountdownTimer() {
        if (this.countdownId) {
            clearInterval(this.countdownId);
        }
        this.countdownId = setInterval(() => {
            if (this.lastUpdate) {
                this.updateLastUpdatedDisplay();
            }
        }, 1000);
    }

    refreshPrices() {
        this.showRefreshingState();
        this.fetchPrices();
    }

    forceRefresh() {
        this.prices = {};
        this.isUpdating = false;
        this.showRefreshingState();
        this.fetchPrices();
    }

    showRefreshingState() {
        const refreshBtn = document.querySelector('.crypto-controls .btn-icon');
        if (refreshBtn) {
            const icon = refreshBtn.querySelector('i');
            if (icon) {
                icon.className = 'fas fa-spinner fa-spin';
                refreshBtn.disabled = true;
            }
        }
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            lastUpdatedElement.textContent = 'Refreshing...';
        }
    }

    clearRefreshingState() {
        const refreshBtn = document.querySelector('.crypto-controls .btn-icon');
        if (refreshBtn) {
            const icon = refreshBtn.querySelector('i');
            if (icon) {
                icon.className = 'fas fa-sync-alt';
            }
            refreshBtn.disabled = false;
        }
    }

    stopUpdates() {
        if (this.timerId) {
            clearInterval(this.timerId);
            this.timerId = null;
        }
    }

    showOfflineStatus() {
        this.showUnavailable('offline');
    }

    showOnlineStatus() {
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            lastUpdatedElement.style.color = '';
        }
    }
}

function refreshCryptoPrices() {
    if (window.cryptoPriceManager) {
        window.cryptoPriceManager.refreshPrices();
    }
}

function forceRefreshCryptoPrices() {
    if (window.cryptoPriceManager) {
        window.cryptoPriceManager.forceRefresh();
    }
}

document.addEventListener('DOMContentLoaded', () => {
    window.cryptoPriceManager = new CryptoPriceManager();
});

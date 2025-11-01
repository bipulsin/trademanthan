// Crypto Price Manager for Real-Time Binance Data
class CryptoPriceManager {
    constructor() {
        this.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'SOLUSDT', 'DOTUSDT', 'MATICUSDT', 'LINKUSDT'];
        this.prices = {};
        this.lastUpdate = null;
        this.updateInterval = null;
        this.isUpdating = false;
        
        this.init();
    }
    
    init() {
        console.log('CryptoPriceManager: Initializing...');
        
        // Check if we're on the right page
        if (!this.isDashboardPage()) {
            console.log('CryptoPriceManager: Not on dashboard page, skipping initialization');
            return;
        }
        
        // Wait for DOM to be ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.initializeAfterDOM());
        } else {
            this.initializeAfterDOM();
        }
    }

    initializeAfterDOM() {
        console.log('CryptoPriceManager: DOM ready, checking elements...');
        
        // Check if required elements exist
        if (!this.checkRequiredElements()) {
            console.error('CryptoPriceManager: Required DOM elements not found, cannot initialize');
            return;
        }
        
        console.log('CryptoPriceManager: All required elements found, proceeding with initialization');
        this.setupEventListeners();
        this.startPriceUpdates();
        this.startCountdownTimer();
        
        // Show initial loading state
        this.showInitialLoadingState();
    }

    isDashboardPage() {
        return window.location.pathname.includes('dashboard') || 
               document.querySelector('.crypto-card') !== null;
    }

    checkRequiredElements() {
        const requiredElements = [
            'cryptoGrid',
            'lastUpdated'
        ];
        
        const missingElements = [];
        requiredElements.forEach(id => {
            if (!document.getElementById(id)) {
                missingElements.push(id);
            }
        });
        
        if (missingElements.length > 0) {
            console.error('CryptoPriceManager: Missing required elements:', missingElements);
            return false;
        }
        
        return true;
    }

    showInitialLoadingState() {
        console.log('CryptoPriceManager: Showing initial loading state');
        
        // Update debug info
        this.updateDebugInfo('Initializing...', 'Loading crypto prices...', 'No errors');
        
        // Show loading state for each crypto item
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
        
        // Update last updated display
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            lastUpdatedElement.textContent = 'Initializing...';
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
            
            // Add timestamp
            const timestamp = new Date().toLocaleTimeString();
            if (statusDiv) statusDiv.textContent += ` (${timestamp})`;
            
            // Show current prices if available
            if (this.prices && Object.keys(this.prices).length > 0) {
                const priceInfo = Object.keys(this.prices).map(symbol => {
                    const price = this.prices[symbol];
                    return `${symbol}: $${this.formatPrice(price.price)}`;
                }).join(', ');
                
                if (elementsDiv) elementsDiv.textContent += ` | Prices: ${priceInfo}`;
            }
        } catch (error) {
            console.error('CryptoPriceManager: Error updating debug info:', error);
        }
    }
    
    setupEventListeners() {
        // Setup refresh button
        const refreshBtn = document.querySelector('.crypto-controls .btn-icon');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => this.refreshPrices());
        }
        
        // Setup network status listeners
        window.addEventListener('online', () => {
            console.log('CryptoPriceManager: Network is online');
            this.showOnlineStatus();
            this.refreshPrices();
        });
        
        window.addEventListener('offline', () => {
            console.log('CryptoPriceManager: Network is offline');
            this.showOfflineStatus();
        });
        
        // Check initial network status
        if (!this.checkNetworkConnectivity()) {
            this.useAllFallbackData();
        }
    }
    
    startPriceUpdates() {
        // Initial price fetch with timeout
        this.fetchPricesWithTimeout();
        
        // Get update interval from settings or use default (5 minutes)
        const interval = this.getUpdateInterval();
        this.setUpdateInterval(interval);
        
        console.log(`CryptoPriceManager: Started automatic price updates every ${interval/1000} seconds`);
    }

    async fetchPricesWithTimeout() {
        const timeout = 10000; // 10 seconds timeout
        
        try {
            const timeoutPromise = new Promise((_, reject) => {
                setTimeout(() => reject(new Error('Price fetch timeout')), timeout);
            });
            
            const fetchPromise = this.fetchPrices();
            
            await Promise.race([fetchPromise, timeoutPromise]);
        } catch (error) {
            console.warn('CryptoPriceManager: Price fetch timed out or failed, using fallback data');
            this.useAllFallbackData();
        }
    }

    getUpdateInterval() {
        // Try to get interval from settings
        try {
            const savedSettings = localStorage.getItem('trademanthan_settings');
            if (savedSettings) {
                const settings = JSON.parse(savedSettings);
                if (settings.trading && settings.trading.crypto && settings.trading.crypto.updateInterval) {
                    return settings.trading.crypto.updateInterval;
                }
            }
        } catch (error) {
            console.warn('CryptoPriceManager: Error reading settings, using default interval');
        }
        
        // Default to 5 minutes (300000ms)
        return 300000;
    }

    setUpdateInterval(interval) {
        // Clear existing interval
        if (this.updateInterval) {
            clearInterval(this.updateInterval);
        }
        
        // Set new interval
        this.updateInterval = setInterval(() => {
            this.fetchPrices();
        }, interval);
        
        console.log(`CryptoPriceManager: Updated interval to ${interval/1000} seconds`);
    }

    updateInterval(newInterval) {
        this.setUpdateInterval(newInterval);
    }
    
    async fetchPrices() {
        if (this.isUpdating) {
            console.log('CryptoPriceManager: Update already in progress, skipping...');
            return;
        }
        
        this.isUpdating = true;
        console.log('CryptoPriceManager: Fetching latest prices...');
        
        try {
            // Fetch prices for all symbols
            const pricePromises = this.symbols.map(symbol => this.fetchSymbolPrice(symbol));
            const results = await Promise.allSettled(pricePromises);
            
            let successCount = 0;
            let failureCount = 0;
            
            // Process results
            results.forEach((result, index) => {
                if (result.status === 'fulfilled') {
                    const symbol = this.symbols[index];
                    this.prices[symbol] = result.value;
                    this.updatePriceDisplay(symbol, result.value);
                    successCount++;
                } else {
                    console.error(`CryptoPriceManager: Failed to fetch ${this.symbols[index]}:`, result.reason);
                    failureCount++;
                    // Try to use fallback data for failed symbols
                    this.useFallbackData(this.symbols[index]);
                }
            });
            
            console.log(`CryptoPriceManager: Fetch completed. Success: ${successCount}, Failures: ${failureCount}`);
            
            // Update debug info
            this.updateDebugInfo(
                `Fetch completed - Success: ${successCount}, Failures: ${failureCount}`,
                `Updated ${successCount} prices`,
                failureCount > 0 ? `${failureCount} fetch failures` : 'No errors'
            );
            
            if (successCount > 0) {
                this.lastUpdate = new Date();
                this.updateLastUpdatedDisplay();
            } else {
                console.warn('CryptoPriceManager: All price fetches failed, using fallback data');
                this.useAllFallbackData();
            }
            
        } catch (error) {
            console.error('CryptoPriceManager: Error fetching prices:', error);
            console.warn('CryptoPriceManager: Using fallback data due to fetch error');
            this.useAllFallbackData();
        } finally {
            this.isUpdating = false;
        }
    }

    async testAPIConnectivity() {
        try {
            // Test Binance API
            const binanceTest = await fetch('https://api.binance.com/api/v3/ping', { 
                method: 'GET',
                cache: 'no-cache'
            });
            
            // Test CoinGecko API
            const coinGeckoTest = await fetch('https://api.coingecko.com/api/v3/ping', { 
                method: 'GET',
                cache: 'no-cache'
            });
            
            return {
                binance: binanceTest.ok ? 'OK' : `HTTP ${binanceTest.status}`,
                coinGecko: coinGeckoTest.ok ? 'OK' : `HTTP ${coinGeckoTest.status}`,
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            return {
                binance: 'ERROR',
                coinGecko: 'ERROR',
                error: error.message,
                timestamp: new Date().toISOString()
            };
        }
    }
    
    async fetchSymbolPrice(symbol) {
        try {
            // Try Binance API first
            const binanceData = await this.fetchFromBinance(symbol);
            if (binanceData) {
                return binanceData;
            }
            
            // Fallback to alternative API if Binance fails
            console.warn(`CryptoPriceManager: Binance API failed for ${symbol}, trying alternative...`);
            const alternativeData = await this.fetchFromAlternative(symbol);
            if (alternativeData) {
                return alternativeData;
            }
            
            throw new Error('All API endpoints failed');
            
        } catch (error) {
            console.error(`CryptoPriceManager: Error fetching ${symbol}:`, error);
            throw error;
        }
    }

    async fetchFromBinance(symbol) {
        try {
            const timestamp = Date.now();
            const response = await fetch(`https://api.binance.com/api/v3/ticker/24hr?symbol=${symbol}&_t=${timestamp}`, {
                cache: 'no-cache',
                headers: {
                    'Cache-Control': 'no-cache, no-store, must-revalidate',
                    'Pragma': 'no-cache',
                    'Expires': '0'
                }
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            
            return {
                symbol: symbol,
                price: parseFloat(data.lastPrice),
                change: parseFloat(data.priceChange),
                changePercent: parseFloat(data.priceChangePercent),
                volume: parseFloat(data.volume),
                high24h: parseFloat(data.highPrice),
                low24h: parseFloat(data.lowPrice)
            };
            
        } catch (error) {
            console.warn(`CryptoPriceManager: Binance API failed for ${symbol}:`, error);
            return null;
        }
    }

    async fetchFromAlternative(symbol) {
        try {
            // Try using CoinGecko API as alternative
            const response = await fetch(`https://api.coingecko.com/api/v3/simple/price?ids=${this.getCoinGeckoId(symbol)}&vs_currencies=usd&include_24hr_change=true`, {
                cache: 'no-cache'
            });
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            const coinId = this.getCoinGeckoId(symbol);
            
            if (data[coinId]) {
                const coinData = data[coinId];
                return {
                    symbol: symbol,
                    price: parseFloat(coinData.usd),
                    change: 0, // CoinGecko doesn't provide absolute change
                    changePercent: parseFloat(coinData.usd_24h_change || 0),
                    volume: 1000000, // Default volume
                    high24h: parseFloat(coinData.usd) * 1.02, // Approximate
                    low24h: parseFloat(coinData.usd) * 0.98 // Approximate
                };
            }
            
            return null;
            
        } catch (error) {
            console.warn(`CryptoPriceManager: Alternative API failed for ${symbol}:`, error);
            return null;
        }
    }

    getCoinGeckoId(symbol) {
        // Map Binance symbols to CoinGecko IDs
        const symbolMap = {
            'BTCUSDT': 'bitcoin',
            'ETHUSDT': 'ethereum',
            'BNBUSDT': 'binancecoin',
            'ADAUSDT': 'cardano',
            'SOLUSDT': 'solana',
            'DOTUSDT': 'polkadot',
            'MATICUSDT': 'matic-network',
            'LINKUSDT': 'chainlink'
        };
        
        return symbolMap[symbol] || 'bitcoin';
    }

    useFallbackData(symbol) {
        console.log(`CryptoPriceManager: Using fallback data for ${symbol}`);
        
        // Use cached prices if available, otherwise use default fallback
        if (this.prices[symbol]) {
            console.log(`CryptoPriceManager: Using cached price for ${symbol}:`, this.prices[symbol]);
            this.updatePriceDisplay(symbol, this.prices[symbol]);
            return;
        }
        
        // Default fallback data
        const fallbackData = this.getDefaultFallbackData(symbol);
        this.prices[symbol] = fallbackData;
        this.updatePriceDisplay(symbol, fallbackData);
        console.log(`CryptoPriceManager: Applied default fallback for ${symbol}:`, fallbackData);
    }

    useAllFallbackData() {
        console.log('CryptoPriceManager: Applying fallback data for all symbols');
        this.symbols.forEach(symbol => {
            this.useFallbackData(symbol);
        });
        
        // Set a fallback timestamp
        this.lastUpdate = new Date();
        this.updateLastUpdatedDisplay();
    }

    getDefaultFallbackData(symbol) {
        // Updated fallback prices as of recent market data (should be updated periodically)
        const fallbackPrices = {
            'BTCUSDT': { price: 62000, changePercent: 1.2 },
            'ETHUSDT': { price: 3200, changePercent: 0.8 },
            'BNBUSDT': { price: 580, changePercent: 1.5 },
            'ADAUSDT': { price: 0.52, changePercent: -0.5 },
            'SOLUSDT': { price: 140, changePercent: 2.8 },
            'DOTUSDT': { price: 8.5, changePercent: 1.2 },
            'MATICUSDT': { price: 1.15, changePercent: 1.8 },
            'LINKUSDT': { price: 18.2, changePercent: 0.6 }
        };
        
        const fallback = fallbackPrices[symbol] || { price: 100, changePercent: 0 };
        
        return {
            symbol: symbol,
            price: fallback.price,
            change: fallback.price * (fallback.changePercent / 100),
            changePercent: fallback.changePercent,
            volume: 1000000,
            high24h: fallback.price * 1.05,
            low24h: fallback.price * 0.95
        };
    }
    
    updatePriceDisplay(symbol, data) {
        const priceElement = document.getElementById(`${symbol}-price`);
        const changeElement = document.getElementById(`${symbol}-change`);
        
        if (!priceElement || !changeElement) {
            console.warn(`CryptoPriceManager: DOM elements not found for ${symbol}. Price: ${priceElement ? 'found' : 'missing'}, Change: ${changeElement ? 'found' : 'missing'}`);
            return;
        }
        
        try {
            // Update price
            priceElement.textContent = this.formatPrice(data.price);
            
            // Update change
            const changeText = this.formatChange(data.changePercent);
            changeElement.textContent = changeText;
            
            // Update change styling
            changeElement.className = 'crypto-change';
            if (data.changePercent > 0) {
                changeElement.classList.add('positive');
            } else if (data.changePercent < 0) {
                changeElement.classList.add('negative');
            } else {
                changeElement.classList.add('neutral');
            }
            
            // Add animation
            priceElement.style.animation = 'none';
            priceElement.offsetHeight; // Trigger reflow
            priceElement.style.animation = 'priceUpdate 0.5s ease-in-out';
            
            console.log(`CryptoPriceManager: Successfully updated display for ${symbol}: ${this.formatPrice(data.price)} (${changeText})`);
        } catch (error) {
            console.error(`CryptoPriceManager: Error updating display for ${symbol}:`, error);
        }
    }
    
    formatPrice(price) {
        if (price >= 1000) {
            return `$${price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        } else if (price >= 1) {
            return `$${price.toFixed(2)}`;
        } else {
            return `$${price.toFixed(4)}`;
        }
    }
    
    formatChange(changePercent) {
        const sign = changePercent >= 0 ? '+' : '';
        return `${sign}${changePercent.toFixed(2)}%`;
    }
    
    updateLastUpdatedDisplay() {
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement && this.lastUpdate) {
            const now = new Date();
            const diffMs = now - this.lastUpdate;
            const diffSec = Math.floor(diffMs / 1000);
            
            if (diffSec < 60) {
                lastUpdatedElement.textContent = `Last updated: ${diffSec}s ago`;
            } else if (diffSec < 3600) {
                const diffMin = Math.floor(diffSec / 60);
                lastUpdatedElement.textContent = `Last updated: ${diffMin}m ago`;
            } else {
                lastUpdatedElement.textContent = `Last updated: ${this.lastUpdate.toLocaleTimeString()}`;
            }
            
            // Update countdown for next update
            this.updateCountdownDisplay();
        }
    }
    
    updateCountdownDisplay() {
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement && this.lastUpdate) {
            const now = new Date();
            const interval = this.getUpdateInterval();
            const nextUpdate = new Date(this.lastUpdate.getTime() + interval);
            const timeUntilNext = nextUpdate - now;
            
            if (timeUntilNext > 0) {
                const minutes = Math.floor(timeUntilNext / 60000);
                const seconds = Math.floor((timeUntilNext % 60000) / 1000);
                
                // Add countdown info to the display
                const countdownText = ` | Next update in: ${minutes}:${seconds.toString().padStart(2, '0')}`;
                
                // Only show countdown if it's not already there
                if (!lastUpdatedElement.textContent.includes('Next update in:')) {
                    lastUpdatedElement.textContent += countdownText;
                }
            }
        }
    }
    
    startCountdownTimer() {
        // Update countdown every second
        setInterval(() => {
            if (this.shouldShowCountdown()) {
                this.updateCountdownDisplay();
            }
        }, 1000);
    }

    shouldShowCountdown() {
        try {
            const savedSettings = localStorage.getItem('trademanthan_settings');
            if (savedSettings) {
                const settings = JSON.parse(savedSettings);
                if (settings.trading && settings.trading.crypto && settings.trading.crypto.showCountdown !== undefined) {
                    return settings.trading.crypto.showCountdown;
                }
            }
        } catch (error) {
            console.warn('CryptoPriceManager: Error reading countdown setting, showing by default');
        }
        
        // Default to showing countdown
        return true;
    }
    
    refreshPrices() {
        console.log('CryptoPriceManager: Manual refresh requested');
        
        // Show refreshing state
        this.showRefreshingState();
        
        // Fetch prices with timeout
        this.fetchPricesWithTimeout();
        
        // Reset the countdown timer for manual refresh
        setTimeout(() => {
            this.updateCountdownDisplay();
        }, 100);
    }

    forceRefresh() {
        console.log('CryptoPriceManager: Force refresh requested');
        
        // Show refreshing state
        this.showRefreshingState();
        
        // Clear cache and force fresh fetch
        this.prices = {};
        this.isUpdating = false;
        
        // Test API connectivity first
        this.testAPIConnectivity().then(result => {
            console.log('CryptoPriceManager: API connectivity test:', result);
            this.updateDebugInfo('API Test', result.binance + ' / ' + result.coinGecko, 'Testing connectivity...');
        });
        
        // Force immediate refresh
        this.fetchPricesWithTimeout();
        
        // Reset the countdown timer for manual refresh
        setTimeout(() => {
            this.updateCountdownDisplay();
        }, 1000);
    }

    showRefreshingState() {
        // Show refreshing indicator
        const refreshBtn = document.querySelector('.crypto-controls .btn-icon');
        if (refreshBtn) {
            const icon = refreshBtn.querySelector('i');
            if (icon) {
                icon.className = 'fas fa-spinner fa-spin';
                refreshBtn.disabled = true;
                
                // Reset after 2 seconds
                setTimeout(() => {
                    icon.className = 'fas fa-sync-alt';
                    refreshBtn.disabled = false;
                }, 2000);
            }
        }
        
        // Update last updated text
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            lastUpdatedElement.textContent = 'Refreshing...';
        }
    }
    
    stopUpdates() {
        if (this.updateInterval) {
            clearInterval(this.updateInterval);
            this.updateInterval = null;
            console.log('CryptoPriceManager: Stopped automatic price updates');
        }
    }

    checkNetworkConnectivity() {
        if (!navigator.onLine) {
            console.warn('CryptoPriceManager: Network is offline, using fallback data');
            this.showOfflineStatus();
            return false;
        }
        return true;
    }

    showOfflineStatus() {
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            lastUpdatedElement.textContent = 'Offline - Using cached data';
            lastUpdatedElement.style.color = '#f44336';
        }
    }

    showOnlineStatus() {
        const lastUpdatedElement = document.getElementById('lastUpdated');
        if (lastUpdatedElement) {
            lastUpdatedElement.style.color = '#4caf50';
        }
    }
}

// Global function for refresh button
function refreshCryptoPrices() {
    if (window.cryptoPriceManager) {
        window.cryptoPriceManager.refreshPrices();
    }
}

// Global function for force refresh button
function forceRefreshCryptoPrices() {
    if (window.cryptoPriceManager) {
        window.cryptoPriceManager.forceRefresh();
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.cryptoPriceManager = new CryptoPriceManager();
});

// Add CSS animation for price updates
const style = document.createElement('style');
style.textContent = `
    @keyframes priceUpdate {
        0% { transform: scale(1); }
        50% { transform: scale(1.05); }
        100% { transform: scale(1); }
    }
    
    .crypto-item {
        transition: all 0.3s ease;
    }
    
    .crypto-item:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
    }
`;
document.head.appendChild(style);

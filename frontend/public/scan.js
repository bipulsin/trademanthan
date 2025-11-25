// Scan Page JavaScript
console.log('Scan.js loaded successfully');

const API_BASE_URL = window.location.hostname === 'localhost' 
    ? 'http://localhost:8000' 
    : 'https://trademanthan.in';

let autoRefreshInterval = null;
let currentBullishData = null;
let currentBearishData = null;

// Toggle Day Summary collapse/expand
function toggleDaySummary() {
    const content = document.getElementById('daySummaryContent');
    const caret = document.getElementById('summaryCaret');
    
    if (content.classList.contains('collapsed')) {
        content.classList.remove('collapsed');
        caret.classList.remove('collapsed');
        caret.textContent = '▼';
    } else {
        content.classList.add('collapsed');
        caret.classList.add('collapsed');
        caret.textContent = '▶';
    }
}

// Toggle Bullish/Bearish section collapse/expand
function toggleSection(sectionType) {
    const content = document.getElementById(`${sectionType}Content`);
    const caret = document.getElementById(`${sectionType}Caret`);
    
    if (content && caret) {
        if (content.classList.contains('collapsed')) {
            content.classList.remove('collapsed');
            caret.textContent = '▼';
        } else {
            content.classList.add('collapsed');
            caret.textContent = '▶';
        }
    }
}

// Check if section should be collapsed by default
// Returns true if section is empty OR all trades are 'no_entry' status
function shouldCollapseByDefault(data) {
    if (!data || !data.alerts || data.alerts.length === 0) {
        return true; // Empty section → collapsed
    }
    
    // Check all stocks across all alerts
    let totalStocks = 0;
    let noEntryStocks = 0;
    
    data.alerts.forEach(alert => {
        if (alert.stocks && alert.stocks.length > 0) {
            alert.stocks.forEach(stock => {
                totalStocks++;
                // Check if it's a "No Entry" trade
                // Use status field if available, otherwise fallback to buy_price/qty check
                const isNoEntry = stock.status === 'no_entry' || (!stock.buy_price || stock.buy_price === 0 || stock.qty === 0);
                if (isNoEntry) {
                    noEntryStocks++;
                }
            });
        }
    });
    
    // If all stocks are "No Entry", collapse by default
    if (totalStocks > 0 && noEntryStocks === totalStocks) {
        return true; // All "No Entry" → collapsed
    }
    
    return false; // Has actual trades → expanded
}

// Calculate and update day summary
function updateDaySummary(bullishData, bearishData) {
    try {
        let totalAlerts = 0;
        let tradesEntered = 0;
        let bullishTrades = 0;
        let bearishTrades = 0;
        let slExits = 0;
        let timeExits = 0;
        let targetExits = 0;
        let vwapExits = 0;
        let totalPnL = 0;
        let winners = 0;
        let losers = 0;
        
        // Process bullish alerts
        if (bullishData && bullishData.alerts) {
            bullishData.alerts.forEach(alert => {
                if (alert.stocks) {
                    totalAlerts += alert.stocks.length;
                    alert.stocks.forEach(stock => {
                        // Check if trade was entered - use status field first, then fallback to buy_price/qty
                        // A trade is entered if status is 'bought' or 'sold', NOT 'no_entry'
                        const tradeWasEntered = stock.status !== 'no_entry' && 
                                              stock.buy_price && stock.buy_price > 0 && 
                                              stock.qty && stock.qty > 0;
                        
                        if (tradeWasEntered) {
                            tradesEntered++;
                            bullishTrades++;
                            
                            // Calculate P&L
                            if (stock.pnl) {
                                totalPnL += stock.pnl;
                                if (stock.pnl > 0) winners++;
                                else if (stock.pnl < 0) losers++;
                            }
                        }
                        
                        // Count exits for any trade that has an exit_reason (even if it wasn't "entered" properly)
                        // This ensures we capture all exits including time_based exits at 3:25 PM
                        if (stock.exit_reason) {
                            if (stock.exit_reason === 'stop_loss') {
                                slExits++;
                            } else if (stock.exit_reason === 'time_based') {
                                timeExits++;
                            } else if (stock.exit_reason === 'profit_target') {
                                targetExits++;
                            } else if (stock.exit_reason === 'stock_vwap_cross') {
                                vwapExits++;
                            }
                        }
                    });
                }
            });
        }
        
        // Process bearish alerts
        if (bearishData && bearishData.alerts) {
            bearishData.alerts.forEach(alert => {
                if (alert.stocks) {
                    totalAlerts += alert.stocks.length;
                    alert.stocks.forEach(stock => {
                        // Check if trade was entered - use status field first, then fallback to buy_price/qty
                        // A trade is entered if status is 'bought' or 'sold', NOT 'no_entry'
                        const tradeWasEntered = stock.status !== 'no_entry' && 
                                              stock.buy_price && stock.buy_price > 0 && 
                                              stock.qty && stock.qty > 0;
                        
                        if (tradeWasEntered) {
                            tradesEntered++;
                            bearishTrades++;
                            
                            // Calculate P&L
                            if (stock.pnl) {
                                totalPnL += stock.pnl;
                                if (stock.pnl > 0) winners++;
                                else if (stock.pnl < 0) losers++;
                            }
                        }
                        
                        // Count exits for any trade that has an exit_reason (even if it wasn't "entered" properly)
                        // This ensures we capture all exits including time_based exits at 3:25 PM
                        if (stock.exit_reason) {
                            if (stock.exit_reason === 'stop_loss') {
                                slExits++;
                            } else if (stock.exit_reason === 'time_based') {
                                timeExits++;
                            } else if (stock.exit_reason === 'profit_target') {
                                targetExits++;
                            } else if (stock.exit_reason === 'stock_vwap_cross') {
                                vwapExits++;
                            }
                        }
                    });
                }
            });
        }
        
        // Calculate win rate
        const winRate = tradesEntered > 0 ? ((winners / tradesEntered) * 100).toFixed(1) : 0;
        
        // Update summary values
        document.getElementById('summaryNetPnL').textContent = `₹${totalPnL.toFixed(2)}`;
        document.getElementById('summaryNetPnL').className = 'summary-value pnl ' + (totalPnL >= 0 ? 'positive' : 'negative');
        
        document.getElementById('summaryWinRate').textContent = `${winRate}%`;
        document.getElementById('summaryTotalAlerts').textContent = totalAlerts;
        document.getElementById('summaryTradesEntered').textContent = tradesEntered;
        document.getElementById('summaryBullishTrades').textContent = bullishTrades;
        document.getElementById('summaryBearishTrades').textContent = bearishTrades;
        document.getElementById('summaryTargetExits').textContent = targetExits;
        document.getElementById('summaryVWAPExits').textContent = vwapExits;
        document.getElementById('summarySLExits').textContent = slExits;
        document.getElementById('summaryTimeExits').textContent = timeExits;
        
        // Update quick stats (shown when collapsed)
        const quickStatsHTML = `
            <span class="${totalPnL >= 0 ? 'positive' : 'negative'}" style="color: ${totalPnL >= 0 ? '#a7f3d0' : '#fca5a5'};">
                P&L: ₹${totalPnL.toFixed(2)}
            </span>
            <span style="opacity: 0.9;">
                ${tradesEntered} Trades
            </span>
            <span style="opacity: 0.9;">
                ${winRate}% Win
            </span>
        `;
        document.getElementById('summaryQuickStats').innerHTML = quickStatsHTML;
        
    } catch (error) {
        console.error('Error updating day summary:', error);
    }
}

// Check token health periodically
async function checkTokenHealth() {
    try {
        const response = await fetch(`${API_BASE_URL}/scan/upstox/status`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const result = await response.json();
        
        // If token is not authenticated, show warning banner
        if (result.status === 'success' && !result.authenticated) {
            console.warn('⚠️ Upstox token is not valid:', result.message);
            showTokenExpiredMessage();
        } else if (result.authenticated) {
            console.log('✅ Upstox token is valid');
            hideTokenExpiredMessage();
        }
    } catch (error) {
        console.error('Error checking token health:', error);
    }
}

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    console.log('Scan page loaded');
    
    // Check for OAuth success parameter
    checkOAuthSuccess();
    
    loadIndexPrices();
    loadLatestData();
    checkTokenHealth(); // Check token status immediately on load
    startAutoRefresh();
    
    // Check token health every 5 minutes
    setInterval(checkTokenHealth, 5 * 60 * 1000);
});

// Check if returning from successful OAuth
function checkOAuthSuccess() {
    const urlParams = new URLSearchParams(window.location.search);
    const authStatus = urlParams.get('auth');
    
    if (authStatus === 'success') {
        // Show success message
        alert('✅ Upstox authentication successful! Your access token has been updated. The backend service is restarting...');
        
        // Hide any expired token messages
        hideTokenExpiredMessage();
        
        // Clean up URL
        window.history.replaceState({}, document.title, window.location.pathname);
        
        // Reload data after a short delay to allow service restart
        setTimeout(() => {
            window.location.reload();
        }, 3000);
    } else if (authStatus === 'error') {
        alert('❌ Upstox authentication failed. Please try again or use manual token entry.');
        
        // Clean up URL
        window.history.replaceState({}, document.title, window.location.pathname);
    }
}

// Load index prices (NIFTY and BANKNIFTY)
async function loadIndexPrices() {
    try {
        const response = await fetch(`${API_BASE_URL}/scan/index-prices`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache'
            }
        });
        
        const result = await response.json();
        
        // Check for 401 status code (Unauthorized) or error_type
        if (response.status === 401 || result.error_type === 'token_expired') {
            console.log('Token expired, showing expired message');
            showTokenExpiredMessage();
            return;
        }
        
        if (result.status === 'success' && result.data) {
            updateIndexDisplay(result.data);
        } else {
            console.error('Failed to load index prices:', result.message);
            showIndexError();
        }
    } catch (error) {
        console.error('Error loading index prices:', error);
        
        // Check if it's a 401 error (Unauthorized)
        if (error.toString().includes('401') || error.toString().includes('Unauthorized')) {
            console.log('Token expired, showing expired message');
            showTokenExpiredMessage();
        } else {
            showIndexError();
        }
    }
}

// Update index display with prices and trends
function updateIndexDisplay(data) {
    // Update NIFTY (Desktop)
    const niftyPrice = document.getElementById('nifty-price');
    const niftyArrow = document.getElementById('nifty-arrow');
    
    if (niftyPrice && niftyArrow && data.nifty) {
        // Use close_price if market is closed, otherwise use ltp
        const price = data.market_status === 'closed' ? data.nifty.close_price : data.nifty.ltp;
        niftyPrice.textContent = formatPrice(price);
        
        // Update arrow based on trend
        niftyArrow.className = 'trend-arrow';
        if (data.nifty.trend === 'bullish') {
            niftyArrow.textContent = '↑';
            niftyArrow.classList.add('up');
        } else if (data.nifty.trend === 'bearish') {
            niftyArrow.textContent = '↓';
            niftyArrow.classList.add('down');
        } else {
            niftyArrow.textContent = '→';
            niftyArrow.classList.add('neutral');
        }
    }
    
    // Update BANKNIFTY (Desktop)
    const bankniftyPrice = document.getElementById('banknifty-price');
    const bankniftyArrow = document.getElementById('banknifty-arrow');
    
    if (bankniftyPrice && bankniftyArrow && data.banknifty) {
        // Use close_price if market is closed, otherwise use ltp
        const price = data.market_status === 'closed' ? data.banknifty.close_price : data.banknifty.ltp;
        bankniftyPrice.textContent = formatPrice(price);
        
        // Update arrow based on trend
        bankniftyArrow.className = 'trend-arrow';
        if (data.banknifty.trend === 'bullish') {
            bankniftyArrow.textContent = '↑';
            bankniftyArrow.classList.add('up');
        } else if (data.banknifty.trend === 'bearish') {
            bankniftyArrow.textContent = '↓';
            bankniftyArrow.classList.add('down');
        } else {
            bankniftyArrow.textContent = '→';
            bankniftyArrow.classList.add('neutral');
        }
    }
    
    // Update NIFTY (Mobile Footer)
    const footerNiftyPrice = document.getElementById('footer-nifty-price');
    const footerNiftyArrow = document.getElementById('footer-nifty-arrow');
    
    if (footerNiftyPrice && footerNiftyArrow && data.nifty) {
        const price = data.market_status === 'closed' ? data.nifty.close_price : data.nifty.ltp;
        footerNiftyPrice.textContent = '₹' + formatPrice(price);
        
        // Update footer arrow based on trend
        footerNiftyArrow.className = 'footer-index-arrow';
        if (data.nifty.trend === 'bullish') {
            footerNiftyArrow.classList.add('bullish');
            footerNiftyArrow.textContent = '↑';
        } else if (data.nifty.trend === 'bearish') {
            footerNiftyArrow.classList.add('bearish');
            footerNiftyArrow.textContent = '↓';
        } else {
            footerNiftyArrow.classList.add('neutral');
            footerNiftyArrow.textContent = '→';
        }
    }
    
    // Update BANKNIFTY (Mobile Footer)
    const footerBankniftyPrice = document.getElementById('footer-banknifty-price');
    const footerBankniftyArrow = document.getElementById('footer-banknifty-arrow');
    
    if (footerBankniftyPrice && footerBankniftyArrow && data.banknifty) {
        const price = data.market_status === 'closed' ? data.banknifty.close_price : data.banknifty.ltp;
        footerBankniftyPrice.textContent = '₹' + formatPrice(price);
        
        // Update footer arrow based on trend
        footerBankniftyArrow.className = 'footer-index-arrow';
        if (data.banknifty.trend === 'bullish') {
            footerBankniftyArrow.classList.add('bullish');
            footerBankniftyArrow.textContent = '↑';
        } else if (data.banknifty.trend === 'bearish') {
            footerBankniftyArrow.classList.add('bearish');
            footerBankniftyArrow.textContent = '↓';
        } else {
            footerBankniftyArrow.classList.add('neutral');
            footerBankniftyArrow.textContent = '→';
        }
    }
}

// Show error state for index prices
function showIndexError() {
    const niftyPrice = document.getElementById('nifty-price');
    const niftyArrow = document.getElementById('nifty-arrow');
    const bankniftyPrice = document.getElementById('banknifty-price');
    const bankniftyArrow = document.getElementById('banknifty-arrow');
    
    if (niftyPrice) niftyPrice.textContent = '--';
    if (niftyArrow) {
        niftyArrow.textContent = '?';
        niftyArrow.className = 'trend-arrow neutral';
    }
    
    if (bankniftyPrice) bankniftyPrice.textContent = '--';
    if (bankniftyArrow) {
        bankniftyArrow.textContent = '?';
        bankniftyArrow.className = 'trend-arrow neutral';
    }
}

// Load latest webhook data
async function loadLatestData() {
    try {
        // Check if token expired banner is showing - if so, don't load data
        const banner = document.getElementById('tokenExpiredBanner');
        if (banner && banner.style.display !== 'none') {
            console.log('Token expired banner showing, skipping data load');
            return;
        }
        
        showLoading();
        
        const response = await fetch(`${API_BASE_URL}/scan/latest`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache, no-store, must-revalidate',
                'Pragma': 'no-cache'
            }
        });
        
        const result = await response.json();
        console.log('API Response:', result);
        
        // Check for 401 (Unauthorized) or token expired
        if (response.status === 401 || result.error_type === 'token_expired') {
            console.log('Token expired detected in loadLatestData');
            showTokenExpiredMessage();
            return;
        }
        
        if (result.status === 'success' && result.data) {
            currentBullishData = result.data.bullish;
            currentBearishData = result.data.bearish;
            console.log('Bullish Data:', currentBullishData);
            console.log('Bearish Data:', currentBearishData);
            
            // ALWAYS display sections regardless of index trends
            // Index trends only affect trade ENTRY status, not alert DISPLAY
            console.log('About to call displaySectionsBasedOnTrends with:', result.data.index_check);
            displaySectionsBasedOnTrends(result.data.index_check, currentBullishData, currentBearishData);
        } else {
            // Even if no data or error, show both empty sections
            console.log('No data received or error in response:', result);
            displayNoData();
        }
    } catch (error) {
        console.error('Error loading data:', error);
        console.error('Error details:', error.message);
        displayNoData();
    }
}

// Display Bullish alert data
function displayBullishData(data) {
    const container = document.getElementById('bullishContainer');
    
    // Determine if should be collapsed by default
    const shouldCollapse = shouldCollapseByDefault(data);
    const collapsedClass = shouldCollapse ? 'collapsed' : '';
    const caretSymbol = shouldCollapse ? '▶' : '▼';
    
    if (!data || !data.alerts || data.alerts.length === 0) {
        container.innerHTML = `
            <div class="alert-section bullish-section">
                <div class="section-header-collapsible" onclick="toggleSection('bullish')" style="cursor: pointer; user-select: none;">
                    <div class="section-title">
                        <!-- Desktop version -->
                        <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                            <h2 style="margin: 0; color: #10b981; display: flex; align-items: center; gap: 10px;">
                                <span id="bullishCaret" class="section-caret">${caretSymbol}</span>
                                📈 BULLISH ALERTS (CALL)
                            </h2>
                            <button class="download-btn" onclick="event.stopPropagation(); downloadCSV('bullish')" disabled style="opacity: 0.5; cursor: not-allowed;">
                                📥 Download CSV
                            </button>
                        </div>
                        <!-- Mobile version -->
                        <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
                            <h2 style="margin: 0; color: #10b981; font-weight: bold; display: flex; align-items: center; gap: 8px;">
                                <span id="bullishCaret" class="section-caret">${caretSymbol}</span>
                                BULLISH
                            </h2>
                            <a href="#" onclick="event.stopPropagation(); downloadCSV('bullish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer; pointer-events: auto;" disabled="true">
                                CSV
                            </a>
                        </div>
                    </div>
                </div>
                <div id="bullishContent" class="section-content ${collapsedClass}">
                    <div class="no-data">
                        <div class="no-data-icon">📭</div>
                        <h3>No Bullish Alerts Yet</h3>
                        <p>Waiting for Bullish webhook data from Chartink...</p>
                    </div>
                </div>
            </div>
        `;
        return;
    }
    
    const html = `
        <div class="alert-section bullish-section">
            <div class="section-header-collapsible" onclick="toggleSection('bullish')" style="cursor: pointer; user-select: none;">
                <div class="section-title">
                    <!-- Desktop version -->
                    <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                        <h2 style="margin: 0; color: #10b981; display: flex; align-items: center; gap: 10px;">
                            <span id="bullishCaret" class="section-caret">${caretSymbol}</span>
                            📈 BULLISH ALERTS (CALL)
                        </h2>
                        <button class="download-btn" onclick="event.stopPropagation(); downloadCSV('bullish')">
                            📥 Download CSV
                        </button>
                    </div>
                    <!-- Mobile version -->
                    <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
                        <h2 style="margin: 0; color: #10b981; font-weight: bold; display: flex; align-items: center; gap: 8px;">
                            <span id="bullishCaret" class="section-caret">${caretSymbol}</span>
                            BULLISH
                        </h2>
                        <a href="#" onclick="event.stopPropagation(); downloadCSV('bullish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer; pointer-events: auto;">
                            CSV
                        </a>
                    </div>
                </div>
            </div>
            <div id="bullishContent" class="section-content ${collapsedClass}">
                ${data.alerts.map(alert => renderAlertGroup(alert, 'bullish')).join('')}
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// Display Bearish alert data
function displayBearishData(data) {
    const container = document.getElementById('bearishContainer');
    
    // Determine if should be collapsed by default
    const shouldCollapse = shouldCollapseByDefault(data);
    const collapsedClass = shouldCollapse ? 'collapsed' : '';
    const caretSymbol = shouldCollapse ? '▶' : '▼';
    
    if (!data || !data.alerts || data.alerts.length === 0) {
        container.innerHTML = `
            <div class="alert-section bearish-section">
                <div class="section-header-collapsible" onclick="toggleSection('bearish')" style="cursor: pointer; user-select: none;">
                    <div class="section-title">
                        <!-- Desktop version -->
                        <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                            <h2 style="margin: 0; color: #ef4444; display: flex; align-items: center; gap: 10px;">
                                <span id="bearishCaret" class="section-caret">${caretSymbol}</span>
                                📉 BEARISH ALERTS (PUT)
                            </h2>
                            <button class="download-btn" onclick="event.stopPropagation(); downloadCSV('bearish')" disabled style="opacity: 0.5; cursor: not-allowed;">
                                📥 Download CSV
                            </button>
                        </div>
                        <!-- Mobile version -->
                        <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
                            <h2 style="margin: 0; color: #ef4444; font-weight: bold; display: flex; align-items: center; gap: 8px;">
                                <span id="bearishCaret" class="section-caret">${caretSymbol}</span>
                                BEARISH
                            </h2>
                            <a href="#" onclick="event.stopPropagation(); downloadCSV('bearish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer; pointer-events: auto;" disabled="true">
                                CSV
                            </a>
                        </div>
                    </div>
                </div>
                <div id="bearishContent" class="section-content ${collapsedClass}">
                    <div class="no-data">
                        <div class="no-data-icon">📭</div>
                        <h3>No Bearish Alerts Yet</h3>
                        <p>Waiting for Bearish webhook data from Chartink...</p>
                    </div>
                </div>
            </div>
        `;
        return;
    }
    
    const html = `
        <div class="alert-section bearish-section">
            <div class="section-header-collapsible" onclick="toggleSection('bearish')" style="cursor: pointer; user-select: none;">
                <div class="section-title">
                    <!-- Desktop version -->
                    <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                        <h2 style="margin: 0; color: #ef4444; display: flex; align-items: center; gap: 10px;">
                            <span id="bearishCaret" class="section-caret">${caretSymbol}</span>
                            📉 BEARISH ALERTS (PUT)
                        </h2>
                        <button class="download-btn" onclick="event.stopPropagation(); downloadCSV('bearish')">
                            📥 Download CSV
                        </button>
                    </div>
                    <!-- Mobile version -->
                    <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
                        <h2 style="margin: 0; color: #ef4444; font-weight: bold; display: flex; align-items: center; gap: 8px;">
                            <span id="bearishCaret" class="section-caret">${caretSymbol}</span>
                            BEARISH
                        </h2>
                        <a href="#" onclick="event.stopPropagation(); downloadCSV('bearish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer; pointer-events: auto;">
                            CSV
                        </a>
                    </div>
                </div>
            </div>
            <div id="bearishContent" class="section-content ${collapsedClass}">
                ${data.alerts.map(alert => renderAlertGroup(alert, 'bearish')).join('')}
            </div>
        </div>
    `;
    
    container.innerHTML = html;
}

// Render a single alert group
function renderAlertGroup(alert, type) {
    if (!alert.stocks || alert.stocks.length === 0) {
        return '';
    }
    
    // Sort stocks alphabetically by stock_name within this time frame
    const sortedStocks = [...alert.stocks].sort((a, b) => {
        const nameA = (a.stock_name || '').toUpperCase();
        const nameB = (b.stock_name || '').toUpperCase();
        return nameA.localeCompare(nameB);
    });
    
    return `
        <div class="time-group">
            <div class="time-header">
                ⏰ ${formatDateTime(alert.triggered_at)} 
                <span class="stocks-count">${alert.stocks.length} Stock${alert.stocks.length > 1 ? 's' : ''}</span>
            </div>
            
            <table class="stocks-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Stock Name</th>
                        <th>Stock LTP</th>
                        <th>Stock VWAP</th>
                        <th>VWAP Slope</th>
                        <th>Candle Size</th>
                        <th>Option Contract (OTM-1)</th>
                        <th>Qty</th>
                        <th>Buy Price</th>
                        <th>Stop Loss</th>
                        <th>Sell Price</th>
                        <th>Status</th>
                        <th>PnL</th>
                    </tr>
                </thead>
                <tbody>
                    ${sortedStocks.map(function(stock, index) {
                        const stock_ltp = stock.last_traded_price || stock.trigger_price || 0;
                        const stock_vwap = stock.stock_vwap || 0;
                        const shouldHold = stock_ltp > stock_vwap;
                        const iconText = shouldHold ? '' : '✖';
                        const iconClass = shouldHold ? 'hold-icon' : 'exit-icon';
                        
                        // Determine exit status display - Check ALL exit criteria
                        let statusDisplay = '';
                        
                        // Get current time in IST first to check market close
                        const now = new Date();
                        const utcTime = now.getTime() + (now.getTimezoneOffset() * 60000);
                        const istOffset = 5.5 * 60 * 60000; // IST is UTC+5:30
                        const istTime = new Date(utcTime + istOffset);
                        const currentHour = istTime.getHours();
                        const currentMinute = istTime.getMinutes();
                        const currentTimeMinutes = currentHour * 60 + currentMinute;
                        const marketCloseMinutes = 15 * 60 + 30; // 3:30 PM IST (market close)
                        
                        // PRIORITY CHECK: If after 3:30 PM IST, always show EXITED-TM
                        if (currentTimeMinutes >= marketCloseMinutes && !stock.exit_reason) {
                            statusDisplay = '<span style="background: #f59e0b; color: black; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">⏰ EXITED-TM</span>';
                        }
                        // Check if trade was already closed (has exit_reason from backend)
                        else if (stock.exit_reason === 'stop_loss') {
                            statusDisplay = '<span style="background: #dc2626; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">🛑 EXITED-SL</span>';
                        } else if (stock.exit_reason === 'profit_target') {
                            statusDisplay = '<span style="background: #16a34a; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">🎯 EXITED-TG</span>';
                        } else if (stock.exit_reason === 'time_based') {
                            statusDisplay = '<span style="background: #f59e0b; color: black; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">⏰ EXITED-TM</span>';
                        } else if (stock.exit_reason === 'stock_vwap_cross') {
                            statusDisplay = '<span style="background: #8b5cf6; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">📉 EXITED-VW</span>';
                        } else if (stock.exit_reason) {
                            statusDisplay = '<span style="background: #6b7280; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">✖ EXITED</span>';
                        } else {
                            // For open trades: Check ALL exit criteria in priority order
                            const option_ltp = stock.sell_price || 0;  // Current option price
                            const buy_price = stock.buy_price || 0;
                            const stop_loss = stock.stop_loss || 0;
                            const option_type = stock.option_type || 'CE';
                            
                            // Calculate time thresholds in IST
                            const exitTimeMinutes = 15 * 60 + 25; // 3:25 PM IST (exit signal time)
                            const marketCloseMinutesDesktop = 15 * 60 + 30; // 3:30 PM IST (market close)
                            const vwapCheckMinutes = 11 * 60 + 15; // 11:15 AM IST
                            
                            // Check exit conditions in priority order
                            if (currentTimeMinutes >= marketCloseMinutesDesktop) {
                                // After market close - show as already exited
                                statusDisplay = '<span style="background: #f59e0b; color: black; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">⏰ EXITED-TM</span>';
                            } else if (currentTimeMinutes >= exitTimeMinutes) {
                                // Between 3:25 PM and 3:30 PM - show exit now
                                statusDisplay = '<span style="background: #f59e0b; color: black; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">⏰ EXIT NOW</span>';
                            } else if (stop_loss > 0 && option_ltp > 0 && option_ltp <= stop_loss) {
                                // Stop loss hit
                                statusDisplay = '<span style="background: #dc2626; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">🛑 EXIT SL</span>';
                            } else if (currentTimeMinutes >= vwapCheckMinutes && stock_vwap > 0) {
                                // VWAP cross check (directional)
                                if ((option_type === 'CE' && stock_ltp < stock_vwap) || 
                                    (option_type === 'PE' && stock_ltp > stock_vwap)) {
                                    statusDisplay = '<span style="background: #8b5cf6; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">📉 EXIT VWAP</span>';
                                } else if (buy_price > 0 && option_ltp >= (buy_price * 1.5)) {
                                    // Profit target hit
                                    statusDisplay = '<span style="background: #16a34a; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">🎯 EXIT TARGET</span>';
                                } else {
                                    // No exit - holding
                                    statusDisplay = '<span style="background: #10b981; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">✓ HOLD</span>';
                                }
                            } else if (buy_price > 0 && option_ltp >= (buy_price * 1.5)) {
                                // Profit target hit (before 11:15 AM)
                                statusDisplay = '<span style="background: #16a34a; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">🎯 EXIT TARGET</span>';
                            } else {
                                // Before 11:15 AM - no exit
                                statusDisplay = '<span style="background: #10b981; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">✓ HOLD</span>';
                            }
                        }
                        
                        // Check if this is a "No Entry" trade
                        // Use status field if available, otherwise fallback to buy_price/qty check
                        const isNoEntry = stock.status === 'no_entry' || (!stock.buy_price || stock.buy_price === 0 || stock.qty === 0);
                        const buyPriceDisplay = isNoEntry ? '-' : '₹' + formatPrice(stock.buy_price);
                        const stopLossDisplay = isNoEntry ? '-' : '₹' + formatPrice(stock.stop_loss || 0);
                        const sellPriceDisplay = isNoEntry ? '-' : '₹' + formatPrice(stock.sell_price || 0);
                        const pnlDisplay = isNoEntry ? '-' : formatPNL(stock.pnl || 0);
                        
                        // Format VWAP slope status
                        let vwapSlopeDisplay = '-';
                        if (stock.vwap_slope_status) {
                            const slopeColor = stock.vwap_slope_status === 'Yes' ? '#10b981' : '#dc2626';
                            const slopeIcon = stock.vwap_slope_status === 'Yes' ? '✅' : '❌';
                            vwapSlopeDisplay = `<span style="color: ${slopeColor}; font-weight: 600;">${slopeIcon} ${stock.vwap_slope_status}</span>`;
                        }
                        
                        // Format candle size status
                        let candleSizeDisplay = '-';
                        if (stock.candle_size_ratio !== null && stock.candle_size_ratio !== undefined) {
                            const sizeColor = stock.candle_size_status === 'Pass' ? '#10b981' : '#dc2626';
                            const sizeIcon = stock.candle_size_status === 'Pass' ? '✅' : '❌';
                            candleSizeDisplay = `<span style="color: ${sizeColor}; font-weight: 600;" title="Ratio: ${stock.candle_size_ratio.toFixed(2)}×">${sizeIcon} ${stock.candle_size_status} (${stock.candle_size_ratio.toFixed(2)}×)</span>`;
                        }
                        
                        return '<tr>' +
                            '<td>' + (index + 1) + '</td>' +
                            '<td class="stock-name">' + escapeHtml(stock.stock_name) + '</td>' +
                            '<td class="trigger-price">₹' + formatPrice(stock_ltp) + '</td>' +
                            '<td class="stock-vwap-col">₹' + formatPrice(stock_vwap) + '</td>' +
                            '<td class="vwap-slope-col" style="font-size: 12px;">' + vwapSlopeDisplay + '</td>' +
                            '<td class="candle-size-col" style="font-size: 12px;">' + candleSizeDisplay + '</td>' +
                            '<td class="option-contract">' + escapeHtml(stock.option_contract || 'N/A') + '</td>' +
                            '<td class="qty">' + (stock.qty || 0) + '</td>' +
                            '<td class="buy-price">' + buyPriceDisplay + '</td>' +
                            '<td class="stop-loss" style="color: #dc2626; font-weight: 600;">' + stopLossDisplay + '</td>' +
                            '<td class="sell-price">' + sellPriceDisplay + '</td>' +
                            '<td class="status-col">' + (isNoEntry ? '<span style="color: #dc2626; font-weight: 700;">No Entry</span>' : statusDisplay) + '</td>' +
                            '<td class="pnl">' + pnlDisplay + '</td>' +
                            '</tr>';
                    }).join('')}
                </tbody>
            </table>
            
            <!-- Mobile Card View -->
            <div class="stock-card-container">
                ${sortedStocks.map(function(stock, index) {
                    const stock_ltp = stock.last_traded_price || stock.trigger_price || 0;
                    const stock_vwap = stock.stock_vwap || 0;
                    const shouldHold = stock_ltp > stock_vwap;
                    const iconText = shouldHold ? '' : '✖';
                    const iconClass = shouldHold ? 'hold-icon' : 'exit-icon';
                    
                    // Check if this is a "No Entry" trade
                    // Use status field if available, otherwise fallback to buy_price/qty check
                const isNoEntry = stock.status === 'no_entry' || (!stock.buy_price || stock.buy_price === 0 || stock.qty === 0);
                    
                    // Parse PnL for color coding
                    let pnlValue = parseFloat(stock.pnl || 0);
                    let pnlColor = pnlValue > 0 ? 'green' : (pnlValue < 0 ? 'red' : '');
                    
                    // Determine exit status display (mobile) - Check ALL exit criteria
                    let statusDisplay = '';
                    
                    // Get current time in IST first to check market close
                    const now = new Date();
                    const utcTime = now.getTime() + (now.getTimezoneOffset() * 60000);
                    const istOffset = 5.5 * 60 * 60000; // IST is UTC+5:30
                    const istTime = new Date(utcTime + istOffset);
                    const currentHour = istTime.getHours();
                    const currentMinute = istTime.getMinutes();
                    const currentTimeMinutes = currentHour * 60 + currentMinute;
                    const marketCloseMinutes = 15 * 60 + 30; // 3:30 PM IST (market close)
                    
                    // PRIORITY CHECK: If after 3:30 PM IST, always show EXT-TM (mobile abbreviation)
                    if (currentTimeMinutes >= marketCloseMinutes && !isNoEntry && !stock.exit_reason) {
                        statusDisplay = '<span style="background: #f59e0b; color: black; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXT-TM</span>';
                    }
                    else if (isNoEntry) {
                        statusDisplay = '<span style="color: #dc2626; font-weight: 700; font-size: 11px; white-space: nowrap;">No Entry</span>';
                    } else if (stock.exit_reason === 'stop_loss') {
                        statusDisplay = '<span style="background: #dc2626; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXD-SL</span>';
                    } else if (stock.exit_reason === 'profit_target') {
                        statusDisplay = '<span style="background: #16a34a; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXD-TG</span>';
                    } else if (stock.exit_reason === 'time_based') {
                        statusDisplay = '<span style="background: #f59e0b; color: black; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXT-TM</span>';
                    } else if (stock.exit_reason === 'stock_vwap_cross') {
                        statusDisplay = '<span style="background: #8b5cf6; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXD-VW</span>';
                    } else if (stock.exit_reason) {
                        statusDisplay = '<span style="background: #6b7280; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXD</span>';
                    } else {
                        // For open trades: Check ALL exit criteria in priority order
                        const option_ltp = stock.sell_price || 0;  // Current option price
                        const buy_price = stock.buy_price || 0;
                        const stop_loss = stock.stop_loss || 0;
                        const option_type = stock.option_type || 'CE';
                        
                        // Note: IST time already calculated above in this scope
                        const exitTimeMinutes = 15 * 60 + 25; // 3:25 PM IST (exit signal time)
                        const vwapCheckMinutes = 11 * 60 + 15; // 11:15 AM IST
                        
                        // Check exit conditions in priority order
                        // Note: marketCloseMinutes already checked at top level
                        if (currentTimeMinutes >= exitTimeMinutes) {
                            // Between 3:25 PM and 3:30 PM - show exit now
                            statusDisplay = '<span style="background: #f59e0b; color: black; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXIT NOW</span>';
                        } else if (stop_loss > 0 && option_ltp > 0 && option_ltp <= stop_loss) {
                            // Stop loss hit
                            statusDisplay = '<span style="background: #dc2626; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXIT SL</span>';
                        } else if (currentTimeMinutes >= vwapCheckMinutes && stock_vwap > 0) {
                            // VWAP cross check (directional)
                            if ((option_type === 'CE' && stock_ltp < stock_vwap) || 
                                (option_type === 'PE' && stock_ltp > stock_vwap)) {
                                statusDisplay = '<span style="background: #8b5cf6; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXIT VWAP</span>';
                            } else if (buy_price > 0 && option_ltp >= (buy_price * 1.5)) {
                                // Profit target hit
                                statusDisplay = '<span style="background: #16a34a; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXIT TG</span>';
                            } else {
                                // No exit - holding
                                statusDisplay = '<span style="background: #10b981; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">HOLD</span>';
                            }
                        } else if (buy_price > 0 && option_ltp >= (buy_price * 1.5)) {
                            // Profit target hit (before 11:15 AM)
                            statusDisplay = '<span style="background: #16a34a; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">EXIT TG</span>';
                        } else {
                            // Before 11:15 AM - no exit
                            statusDisplay = '<span style="background: #10b981; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px; white-space: nowrap; display: inline-block;">HOLD</span>';
                        }
                    }
                    
                    return `
                        <div class="stock-card">
                            <!-- Row 1: Stock Name, Stock LTP, VWAP, Status -->
                            <div class="stock-card-row">
                                <div style="flex: 2;">
                                    <div class="stock-card-value large">${escapeHtml(stock.stock_name)}</div>
                                </div>
                                <div style="flex: 1;">
                                    <div class="stock-card-value">₹${formatPrice(stock_ltp)}</div>
                                </div>
                                <div style="flex: 1;">
                                    <div class="stock-card-value">V:₹${formatPrice(stock_vwap)}</div>
                                </div>
                                <div style="flex: 1; text-align: right;">
                                    ${statusDisplay}
                                </div>
                            </div>
                            
                            <!-- Row 2: Entry Criteria (VWAP Slope & Candle Size) -->
                            ${stock.vwap_slope_status || stock.candle_size_status ? `
                            <div class="stock-card-row" style="background: rgba(59, 130, 246, 0.1); padding: 4px 8px; border-radius: 4px; margin: 4px 0;">
                                <div style="flex: 1; font-size: 11px; color: #93c5fd;">
                                    ${stock.vwap_slope_status ? `<span style="color: ${stock.vwap_slope_status === 'Yes' ? '#10b981' : '#dc2626'}; font-weight: 600;">Slope: ${stock.vwap_slope_status === 'Yes' ? '✅' : '❌'}</span>` : ''}
                                    ${stock.candle_size_status ? `<span style="color: ${stock.candle_size_status === 'Pass' ? '#10b981' : '#dc2626'}; font-weight: 600; margin-left: 8px;">Size: ${stock.candle_size_status === 'Pass' ? '✅' : '❌'} ${stock.candle_size_ratio ? '(' + stock.candle_size_ratio.toFixed(2) + '×)' : ''}</span>` : ''}
                                </div>
                            </div>
                            ` : ''}
                            
                            <!-- Row 3: Option Contract, Qty -->
                            <div class="stock-card-row">
                                <div style="flex: 2;">
                                    <div class="stock-card-value" style="font-size: 12px;">${escapeHtml(stock.option_contract || 'N/A')}</div>
                                </div>
                                <div style="flex: 1;">
                                    <div class="stock-card-value">Q:${stock.qty || 0}</div>
                                </div>
                            </div>
                            
                            <!-- Row 3: Buy Price, SL, Sell Price, PnL -->
                            <div class="stock-card-row">
                                <div style="flex: 1;">
                                    <div class="stock-card-value">${isNoEntry ? '-' : 'B:₹' + formatPrice(stock.buy_price)}</div>
                                </div>
                                <div style="flex: 1;">
                                    <div class="stock-card-value" style="color: #dc2626; font-weight: 600;">${isNoEntry ? '-' : 'SL:₹' + formatPrice(stock.stop_loss || 0)}</div>
                                </div>
                                <div style="flex: 1;">
                                    <div class="stock-card-value">${isNoEntry ? '-' : 'S:₹' + formatPrice(stock.sell_price || 0)}</div>
                                </div>
                                <div style="flex: 1;">
                                    <div class="stock-card-value ${pnlColor}" style="font-weight: 700;">${isNoEntry ? '-' : (pnlValue > 0 ? '+' : '') + '₹' + formatPrice(stock.pnl || 0)}</div>
                                </div>
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        </div>
    `;
}

// Download CSV for a section
function downloadCSV(type) {
    const data = type === 'bullish' ? currentBullishData : currentBearishData;
    
    if (!data || !data.alerts || data.alerts.length === 0) {
        alert(`No ${type} data to download`);
        return;
    }
    
    // Prepare CSV content
    let csv = 'Alert Time,Scan Name,Stock Name,Stock LTP,Stock VWAP,Option Contract,Option Type,Qty,Buy Price,Sell Price,PnL\n';
    
    data.alerts.forEach(alert => {
        const alertTime = formatDateTime(alert.triggered_at);
        const scanName = alert.scan_name || 'N/A';
        
        alert.stocks.forEach(stock => {
            const row = [
                alertTime,
                scanName,
                stock.stock_name,
                formatPrice(stock.last_traded_price || stock.trigger_price),
                formatPrice(stock.stock_vwap || 0),
                stock.option_contract || 'N/A',
                stock.option_type || (type === 'bullish' ? 'CE' : 'PE'),
                stock.qty || 'N/A',
                formatPrice(stock.buy_price || 0),
                formatPrice(stock.sell_price || 0),
                formatPrice(stock.pnl || 0)
            ];
            
            // Escape CSV values
            csv += row.map(val => `"${String(val).replace(/"/g, '""')}"`).join(',') + '\n';
        });
    });
    
    // Create download
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    
    const timestamp = new Date().toISOString().split('T')[0];
    link.setAttribute('href', url);
    link.setAttribute('download', `${type}_alerts_${timestamp}.csv`);
    link.style.visibility = 'hidden';
    
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

// Display loading state
function showLoading() {
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    
    const loadingHTML = `
        <div class="alert-section">
            <div class="loading">
                <div class="spinner"></div>
                <p>Loading latest scan data...</p>
            </div>
        </div>
    `;
    
    bullishContainer.innerHTML = loadingHTML;
    bearishContainer.innerHTML = loadingHTML;
}

// Display sections based on index trends
function displaySectionsBasedOnTrends(indexCheck, bullishData, bearishData) {
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    
    const niftyTrend = indexCheck.nifty_trend || 'unknown';
    const bankniftyTrend = indexCheck.banknifty_trend || 'unknown';
    
    console.log('Display sections based on trends:', { niftyTrend, bankniftyTrend });
    
    // NEW LOGIC: ALWAYS display both sections regardless of index trends
    // Index trends only affect trade ENTRY (buy_price), not alert DISPLAY
    
    // Ensure containers are visible
    if (bullishContainer) bullishContainer.style.display = 'block';
    if (bearishContainer) bearishContainer.style.display = 'block';
    
    // Clear both containers first
    bullishContainer.innerHTML = '';
    bearishContainer.innerHTML = '';
    
    // ALWAYS show both bullish and bearish sections
    console.log('Showing BOTH sections - alerts displayed regardless of index trends');
    displayBullishData(bullishData);
    displayBearishData(bearishData);
    
    // Update day summary with calculated metrics
    updateDaySummary(bullishData, bearishData);
    
    // DO NOT show opposite trends banner - let individual trade status show 'No Entry'
    // Always hide the banner - individual trades will show their status
    hideOppositeTrendsBanner();
    
    // Log for debugging
    if (niftyTrend !== bankniftyTrend && niftyTrend !== 'unknown' && bankniftyTrend !== 'unknown') {
        console.log('Opposite trends detected - trades will show "No Entry" status');
    }
    
}

// Display opposite trends warning - DEPRECATED
// This function is no longer used - we don't show the banner anymore
// Individual trades show "No Entry" status instead
function displayOppositeTrends(indexCheck) {
    console.log('displayOppositeTrends called - but banner disabled, showing sections with No Entry status');
    
    // Don't show the banner - let individual trade status speak
    hideOppositeTrendsBanner();
    
    // Display the sections normally with trade status showing "No Entry"
    const niftyTrend = indexCheck.nifty_trend || 'unknown';
    const bankniftyTrend = indexCheck.banknifty_trend || 'unknown';
    
    console.log(`Index trends: NIFTY=${niftyTrend}, BANKNIFTY=${bankniftyTrend} - trades will show individual status`);
}

// Show opposite trends banner - DISABLED
// We no longer show this banner - individual trade status is sufficient
function showOppositeTrendsBanner(niftyTrend, bankniftyTrend) {
    // Do nothing - banner is disabled
    // Individual trades will show "No Entry" status instead
    console.log(`Index trends: NIFTY=${niftyTrend}, BANKNIFTY=${bankniftyTrend} - banner disabled, using trade status`);
}

function hideOppositeTrendsBanner() {
    // Hide the banner
    const banner = document.getElementById('oppositeTrendsBanner');
    if (banner) {
        banner.style.display = 'none';
        console.log('Opposite trends banner hidden');
    }
    
    // Show the alert sections
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    if (bullishContainer) bullishContainer.style.display = 'block';
    if (bearishContainer) bearishContainer.style.display = 'block';
}

// Display no data state - always show both sections
function displayNoData() {
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    
    bullishContainer.innerHTML = `
        <div class="alert-section bullish-section">
            <div class="section-title">
                <!-- Desktop version -->
                <div class="desktop-header">
                <h2 style="margin: 0; color: #10b981; display: flex; align-items: center; gap: 10px;">
                    <span class="bullish-badge">📈 BULLISH ALERTS (CALL)</span>
                </h2>
                <button class="download-btn" onclick="downloadCSV('bullish')" disabled style="opacity: 0.5; cursor: not-allowed;">
                    📥 Download CSV
                </button>
                </div>
                <!-- Mobile version -->
                <div class="mobile-header" style="display: flex; justify-content: space-between; align-items: center;">
                    <h2 style="margin: 0; color: #10b981; font-weight: bold;">
                        BULLISH
                    </h2>
                    <a href="#" onclick="downloadCSV('bullish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer;" disabled="true">
                        CSV
                    </a>
                </div>
            </div>
            <div class="no-data">
                <div class="no-data-icon">📭</div>
                <h3>No Bullish Alerts Yet</h3>
                <p>Waiting for Bullish webhook data from Chartink...</p>
            </div>
        </div>
    `;
    
    bearishContainer.innerHTML = `
        <div class="alert-section bearish-section">
            <div class="section-title">
                <!-- Desktop version -->
                <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                    <h2 style="margin: 0; color: #ef4444;">
                        📉 BEARISH ALERTS (PUT)
                </h2>
                <button class="download-btn" onclick="downloadCSV('bearish')" disabled style="opacity: 0.5; cursor: not-allowed;">
                    📥 Download CSV
                </button>
                </div>
                <!-- Mobile version -->
                <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
                    <h2 style="margin: 0; color: #ef4444; font-weight: bold;">
                        BEARISH
                    </h2>
                    <a href="#" onclick="downloadCSV('bearish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer;" disabled="true">
                        CSV
                    </a>
                </div>
            </div>
            <div class="no-data">
                <div class="no-data-icon">📭</div>
                <h3>No Bearish Alerts Yet</h3>
                <p>Waiting for Bearish webhook data from Chartink...</p>
            </div>
        </div>
    `;
    
    // Update summary with empty data
    updateDaySummary(null, null);
}

// Display error message
function displayError(message) {
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    
    const errorHTML = `
        <div class="alert-section">
            <div class="error-message">
                <strong>⚠️ Error:</strong> ${escapeHtml(message)}
            </div>
            <div class="no-data">
                <div class="no-data-icon">❌</div>
                <h3>Failed to Load Data</h3>
                <p>Please try refreshing the page or check your connection.</p>
            </div>
        </div>
    `;
    
    bullishContainer.innerHTML = errorHTML;
    bearishContainer.innerHTML = errorHTML;
}

// Manual refresh - refreshes current hour VWAP and index prices
function refreshData() {
    console.log('Manual refresh triggered - updating LTP, option strikes, and index prices');
    // Keep option VWAP refresh separate; index updates are hourly
    refreshCurrentVWAP();
}

// Auto refresh every 1 hour starting at 9:15 AM IST
function startAutoRefresh() {
    // Clear any existing interval
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
    }
    
    const targetMinutes = 15; // :15 minute mark (9:15, 10:15, 11:15, etc.)

    // Helper to check if it's :15 minute mark in IST during market hours
    function isTargetTimeIST(date) {
        try {
            const istDate = new Date(date.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
            const h = istDate.getHours();
            const m = istDate.getMinutes();
            const s = istDate.getSeconds();
            
            // Trigger at :15 minute mark, ONLY during market hours (9:15 AM to 3:45 PM)
            // Check: 9:15 AM to 3:15 PM (hour 9-15, minute 15)
            // Trigger within first 10 seconds of the target minute
            const isDuringMarketHours = (h >= 9 && h <= 15);
            const isTargetMinute = (m === targetMinutes && s < 10);
            
            return isDuringMarketHours && isTargetMinute;
        } catch (e) {
            return false;
        }
    }

    // Poll every 5 seconds and fire only at the hourly :15 mark
    autoRefreshInterval = setInterval(() => {
        const now = new Date();
        if (isTargetTimeIST(now)) {
            console.log('Hourly refresh at :15 IST - refreshing index prices and webhook data');
            loadIndexPrices();
            loadLatestData();
        }
    }, 5000);
    
    console.log('Auto-refresh started: Will refresh every hour at :15 minute mark (9:15 AM onwards)');
}

// Refresh LTP and option strikes without reloading full data
async function refreshCurrentVWAP() {
    try {
        console.log('Refreshing LTP and option strikes...');
        
        // Call backend to refresh current VWAP
        const response = await fetch(`${API_BASE_URL}/scan/refresh-current-vwap`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache'
            }
        });
        
        const result = await response.json();
        
        if (result.status === 'success') {
            console.log('LTP and option strikes refreshed successfully');
            // Reload the data to show updated prices and strikes
            loadLatestData();
        } else {
            console.log('Refresh returned:', result.status);
        }
    } catch (error) {
        console.error('Error refreshing LTP and strikes:', error);
        // Fallback to full data refresh
        loadLatestData();
    }
}

// Stop auto refresh (call this if user leaves page)
function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
        autoRefreshInterval = null;
    }
}

// Format datetime
function formatDateTime(dateString) {
    if (!dateString) return 'N/A';
    
    try {
        const date = new Date(dateString);
        
        // Check if date is valid
        if (isNaN(date.getTime())) {
            // If invalid, return the original string
            return dateString;
        }
        
        // Format: Oct 19, 2025, 10:15 AM
        return date.toLocaleString('en-IN', {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
            hour12: true,
            timeZone: 'Asia/Kolkata'
        });
    } catch (e) {
        console.error('Error formatting date:', e, dateString);
        return dateString;
    }
}

// Format price
function formatPrice(price) {
    if (!price) return '0.00';
    
    try {
        return parseFloat(price).toFixed(2);
    } catch (e) {
        return price;
    }
}

// Format PnL with color coding
function formatPNL(pnl) {
    if (!pnl) return '<span style="color: #718096;">0.00</span>';
    
    try {
        const pnlValue = parseFloat(pnl);
        const formatted = pnlValue.toFixed(2);
        
        if (pnlValue > 0) {
            return '<span style="color: #10b981; font-weight: bold;">+₹' + formatted + '</span>';
        } else if (pnlValue < 0) {
            return '<span style="color: #ef4444; font-weight: bold;">-₹' + Math.abs(formatted) + '</span>';
        } else {
            return '<span style="color: #718096;">₹' + formatted + '</span>';
        }
    } catch (e) {
        return '<span style="color: #718096;">0.00</span>';
    }
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    if (!text) return '';
    
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    
    return text.toString().replace(/[&<>"']/g, m => map[m]);
}

// Clean up on page unload
window.addEventListener('beforeunload', function() {
    stopAutoRefresh();
});

// Export functions for testing
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        loadLatestData,
        loadIndexPrices,
        refreshData,
        formatDateTime,
        formatPrice,
        downloadCSV
    };
}

// Token Expiration Handling Functions
function showTokenExpiredMessage() {
    // Show the banner between title bar and alert sections
    const banner = document.getElementById('tokenExpiredBanner');
    if (banner) {
        banner.style.display = 'block';
        console.log('Token expired banner shown');
    }
    
    // Hide the alert sections
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    if (bullishContainer) bullishContainer.style.display = 'none';
    if (bearishContainer) bearishContainer.style.display = 'none';
}

function hideTokenExpiredMessage() {
    // Hide the banner
    const banner = document.getElementById('tokenExpiredBanner');
    if (banner) {
        banner.style.display = 'none';
        console.log('Token expired banner hidden');
    }
    
    // Show the alert sections
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    if (bullishContainer) bullishContainer.style.display = 'block';
    if (bearishContainer) bearishContainer.style.display = 'block';
}

function openTokenPopup() {
    document.getElementById('tokenExpiredPopup').style.display = 'flex';
}

function closeTokenPopup() {
    document.getElementById('tokenExpiredPopup').style.display = 'none';
}

// Initiate Upstox OAuth login flow
function initiateUpstoxOAuth() {
    console.log('Initiating Upstox OAuth login...');
    
    // Redirect to backend OAuth endpoint which will redirect to Upstox
    const oauthUrl = `${API_BASE_URL}/scan/upstox/login`;
    
    // Show loading state
    const button = event.target;
    if (button) {
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Redirecting to Upstox...';
    }
    
    // Redirect to Upstox OAuth page
    window.location.href = oauthUrl;
}

async function updateTokenFromPopup() {
    const tokenInput = document.getElementById('popupUpstoxToken');
    const newToken = tokenInput.value.trim();
    
    if (!newToken) {
        alert('Please enter a valid access token');
        return;
    }
    
    // Show loading state
    const button = event.target;
    button.disabled = true;
    button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Updating...';
    
    try {
        const response = await fetch('https://trademanthan.in/scan/update-upstox-token', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                access_token: newToken
            })
        });
        
        const result = await response.json();
        
        if (result.status === 'success') {
            alert('✅ Token updated successfully! Backend service is restarting. The page will reload shortly...');
            tokenInput.value = '';
            closeTokenPopup();
            hideTokenExpiredMessage();
            
            // Reload the page after a short delay to get fresh data
            setTimeout(() => {
                window.location.reload();
            }, 3000);
        } else {
            alert('❌ Error: ' + result.message);
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Token';
        }
    } catch (error) {
        alert('❌ Error updating token: ' + error.message);
        button.disabled = false;
        button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Token';
    }
}

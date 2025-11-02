// Scan Page JavaScript
console.log('Scan.js loaded successfully');

const API_BASE_URL = window.location.hostname === 'localhost' 
    ? 'http://localhost:8000' 
    : 'https://trademanthan.in';

let autoRefreshInterval = null;
let currentBullishData = null;
let currentBearishData = null;

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    console.log('Scan page loaded');
    
    // Check for OAuth success parameter
    checkOAuthSuccess();
    
    loadIndexPrices();
    loadLatestData();
    startAutoRefresh();
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
    // Update NIFTY
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
    
    // Update BANKNIFTY
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
            
            // Check if trading is allowed based on index trends
            if (result.data.index_check && !result.data.allow_trading) {
                displayOppositeTrends(result.data.index_check);
                return;
            }
            
            // Display sections based on index trends
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
    
    if (!data || !data.alerts || data.alerts.length === 0) {
        container.innerHTML = `
            <div class="alert-section bullish-section">
                <div class="section-title">
                    <!-- Desktop version -->
                    <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                        <h2 style="margin: 0; color: #10b981;">
                            📈 BULLISH ALERTS (CALL)
                    </h2>
                    <button class="download-btn" onclick="downloadCSV('bullish')" disabled style="opacity: 0.5; cursor: not-allowed;">
                        📥 Download CSV
                    </button>
                    </div>
                    <!-- Mobile version -->
                    <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
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
        return;
    }
    
    const html = `
        <div class="alert-section bullish-section">
            <div class="section-title">
                <!-- Desktop version -->
                <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                    <h2 style="margin: 0; color: #10b981;">
                        📈 BULLISH ALERTS (CALL)
                </h2>
                <button class="download-btn" onclick="downloadCSV('bullish')">
                    📥 Download CSV
                </button>
                </div>
                <!-- Mobile version -->
                <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
                    <h2 style="margin: 0; color: #10b981; font-weight: bold;">
                        BULLISH
                    </h2>
                    <a href="#" onclick="downloadCSV('bullish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer;">
                        CSV
                    </a>
                </div>
            </div>
            
            ${data.alerts.map(alert => renderAlertGroup(alert, 'bullish')).join('')}
        </div>
    `;
    
    container.innerHTML = html;
}

// Display Bearish alert data
function displayBearishData(data) {
    const container = document.getElementById('bearishContainer');
    
    if (!data || !data.alerts || data.alerts.length === 0) {
        container.innerHTML = `
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
        return;
    }
    
    const html = `
        <div class="alert-section bearish-section">
            <div class="section-title">
                <!-- Desktop version -->
                <div class="desktop-header" style="display: flex; justify-content: space-between; align-items: center; width: 100%;">
                    <h2 style="margin: 0; color: #ef4444;">
                        📉 BEARISH ALERTS (PUT)
                </h2>
                <button class="download-btn" onclick="downloadCSV('bearish')">
                    📥 Download CSV
                </button>
                </div>
                <!-- Mobile version -->
                <div class="mobile-header" style="display: none; justify-content: space-between; align-items: center; width: 100%;">
                    <h2 style="margin: 0; color: #ef4444; font-weight: bold;">
                        BEARISH
                    </h2>
                    <a href="#" onclick="downloadCSV('bearish'); return false;" style="color: #3b82f6; text-decoration: none; font-weight: bold; cursor: pointer;">
                        CSV
                    </a>
                </div>
            </div>
            
            ${data.alerts.map(alert => renderAlertGroup(alert, 'bearish')).join('')}
        </div>
    `;
    
    container.innerHTML = html;
}

// Render a single alert group
function renderAlertGroup(alert, type) {
    if (!alert.stocks || alert.stocks.length === 0) {
        return '';
    }
    
    return `
        <div class="time-group">
            <div class="time-header">
                ⏰ ${formatDateTime(alert.triggered_at)} 
                <span class="stocks-count">${alert.stocks.length} Stock${alert.stocks.length > 1 ? 's' : ''}</span>
            </div>
            
            <div style="margin-bottom: 10px; color: #718096; font-size: 14px;">
                📋 <strong>Scan:</strong> ${escapeHtml(alert.scan_name || 'N/A')}
            </div>
            
            <table class="stocks-table">
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Stock Name</th>
                        <th>Stock LTP</th>
                        <th>Stock VWAP</th>
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
                    ${alert.stocks.map(function(stock, index) {
                        const stock_ltp = stock.last_traded_price || stock.trigger_price || 0;
                        const stock_vwap = stock.stock_vwap || 0;
                        const shouldHold = stock_ltp > stock_vwap;
                        const iconText = shouldHold ? '' : '✖';
                        const iconClass = shouldHold ? 'hold-icon' : 'exit-icon';
                        
                        // Determine exit status display
                        let statusDisplay = '';
                        if (stock.exit_reason === 'stop_loss') {
                            statusDisplay = '<span style="background: #dc2626; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px;">🛑 SL HIT</span>';
                        } else if (stock.exit_reason === 'profit_target') {
                            statusDisplay = '<span style="background: #16a34a; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px;">🎯 TARGET</span>';
                        } else if (stock.exit_reason) {
                            statusDisplay = '<span style="background: #6b7280; color: white; padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 11px;">CLOSED</span>';
                        } else {
                            // Only show Hold/Exit for open trades
                            statusDisplay = '<span class="' + iconClass + '">' + iconText + '</span>';
                        }
                        
                        // Check if this is a "No Entry" trade
                        const isNoEntry = !stock.buy_price || stock.buy_price === 0 || stock.qty === 0;
                        const buyPriceDisplay = isNoEntry ? '<span style="color: #dc2626; font-weight: 700;">No Entry</span>' : '₹' + formatPrice(stock.buy_price);
                        const stopLossDisplay = isNoEntry ? '-' : '₹' + formatPrice(stock.stop_loss || 0);
                        const sellPriceDisplay = isNoEntry ? '-' : '₹' + formatPrice(stock.sell_price || 0);
                        const pnlDisplay = isNoEntry ? '-' : formatPNL(stock.pnl || 0);
                        
                        return '<tr>' +
                            '<td>' + (index + 1) + '</td>' +
                            '<td class="stock-name">' + escapeHtml(stock.stock_name) + '</td>' +
                            '<td class="trigger-price">₹' + formatPrice(stock_ltp) + '</td>' +
                            '<td class="stock-vwap-col">₹' + formatPrice(stock_vwap) + '</td>' +
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
                ${alert.stocks.map(function(stock, index) {
                    const stock_ltp = stock.last_traded_price || stock.trigger_price || 0;
                    const stock_vwap = stock.stock_vwap || 0;
                    const shouldHold = stock_ltp > stock_vwap;
                    const iconText = shouldHold ? '' : '✖';
                    const iconClass = shouldHold ? 'hold-icon' : 'exit-icon';
                    
                    // Check if this is a "No Entry" trade
                    const isNoEntry = !stock.buy_price || stock.buy_price === 0 || stock.qty === 0;
                    
                    // Parse PnL for color coding
                    let pnlValue = parseFloat(stock.pnl || 0);
                    let pnlColor = pnlValue > 0 ? 'green' : (pnlValue < 0 ? 'red' : '');
                    
                    // Determine exit status display
                    let statusDisplay = '';
                    if (isNoEntry) {
                        statusDisplay = '<span style="background: #dc2626; color: white; padding: 3px 6px; border-radius: 4px; font-weight: 700; font-size: 10px;">No Entry</span>';
                    } else if (stock.exit_reason === 'stop_loss') {
                        statusDisplay = '<span style="background: #dc2626; color: white; padding: 3px 6px; border-radius: 4px; font-weight: 700; font-size: 10px;">🛑 SL HIT</span>';
                    } else if (stock.exit_reason === 'profit_target') {
                        statusDisplay = '<span style="background: #16a34a; color: white; padding: 3px 6px; border-radius: 4px; font-weight: 700; font-size: 10px;">🎯 TARGET</span>';
                    } else if (stock.exit_reason === 'time_based') {
                        statusDisplay = '<span style="background: #f59e0b; color: white; padding: 3px 6px; border-radius: 4px; font-weight: 700; font-size: 10px;">⏰ TIME</span>';
                    } else if (stock.exit_reason) {
                        statusDisplay = '<span style="background: #6b7280; color: white; padding: 3px 6px; border-radius: 4px; font-weight: 700; font-size: 10px;">CLOSED</span>';
                    } else {
                        statusDisplay = '<span class="${iconClass}" style="font-size: 10px;">${iconText}</span>';
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
                            
                            <!-- Row 2: Option Contract, Qty -->
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
                                    <div class="stock-card-value">${isNoEntry ? '<span style="color: #dc2626; font-weight: 700;">No Entry</span>' : 'B:₹' + formatPrice(stock.buy_price)}</div>
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
    
    // Clear both containers first
    bullishContainer.innerHTML = '';
    bearishContainer.innerHTML = '';
    
    // ALWAYS show both bullish and bearish sections
    console.log('Showing BOTH sections - alerts displayed regardless of index trends');
    displayBullishData(bullishData);
    displayBearishData(bearishData);
    
    // Show opposite trends warning banner if indices are not aligned
    if (niftyTrend !== bankniftyTrend && niftyTrend !== 'unknown' && bankniftyTrend !== 'unknown') {
        console.log('Opposite trends detected - trades will show "No Entry"');
        // Note: Backend will handle "No Entry" logic when setting buy_price
        // Alerts are still displayed, but trades won't be entered
        // This banner is informational only - we still show all alerts
        // Commented out showOppositeTrendsBanner() as we're changing the logic
        // showOppositeTrendsBanner(niftyTrend, bankniftyTrend);
    } else {
        hideOppositeTrendsBanner();
    }
    
}

// Display opposite trends warning
function displayOppositeTrends(indexCheck) {
    const niftyTrend = indexCheck.nifty_trend || 'unknown';
    const bankniftyTrend = indexCheck.banknifty_trend || 'unknown';
    
    // Show the banner
    const banner = document.getElementById('oppositeTrendsBanner');
    if (banner) {
        banner.style.display = 'block';
        
        // Update trend displays
        const niftyDisplay = document.getElementById('niftyTrendDisplay');
        const bankniftyDisplay = document.getElementById('bankniftyTrendDisplay');
        
        if (niftyDisplay) {
            niftyDisplay.textContent = niftyTrend.toUpperCase();
        }
        if (bankniftyDisplay) {
            bankniftyDisplay.textContent = bankniftyTrend.toUpperCase();
        }
        
        console.log('Opposite trends banner shown');
    }
    
    // Hide the alert sections
    const bullishContainer = document.getElementById('bullishContainer');
    const bearishContainer = document.getElementById('bearishContainer');
    if (bullishContainer) bullishContainer.style.display = 'none';
    if (bearishContainer) bearishContainer.style.display = 'none';
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

    // Helper to check if it's :15 minute mark in IST
    function isTargetTimeIST(date) {
        try {
            const istDate = new Date(date.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
            const h = istDate.getHours();
            const m = istDate.getMinutes();
            const s = istDate.getSeconds();
            
            // Trigger at :15 minute mark, starting from 9:15 AM
            // Trigger within first 10 seconds of the target minute
            return h >= 9 && m === targetMinutes && s < 10;
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

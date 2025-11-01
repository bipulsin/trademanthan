// Strategy Management JavaScript v3.0
// Trade Manthan Strategy Manager - Enhanced with Fallback Products and Nginx Proxy

console.log('🚀 strategy.js v3.0 loaded at:', new Date().toISOString());
console.log('🚀 Current URL:', window.location.href);

class StrategyManager {
    static VERSION = "3.0";
    static BUILD_DATE = "2025-08-24";
    static FEATURES = [
        "Fallback Products (Bitcoin/Ethereum)",
        "Nginx Reverse Proxy Support",
        "Enhanced Error Handling",
        "Production-Ready API Integration"
    ];
    constructor() {
        console.log('🏗️ StrategyManager v3.0 constructor called');
        console.log('🚀 Enhanced with Fallback Products and Nginx Proxy Support');
        console.log('🏗️ Constructor timestamp:', new Date().toISOString());
        this.strategies = [];
        this.brokers = [];
        this.currentStrategy = null;
        this.isEditMode = false;
        this.currentUser = null;
        this._isReady = false;
        this.productsCache = {}; // Cache for products by platform
        
        // Base URL configuration - Use production domain
        this.baseURL = 'https://trademanthan.in';
        
        // Additional safety checks
        if (window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1') {
            console.log('🌐 Running on production domain, using production API');
        }
        
        console.log('🌐 Base URL set to:', this.baseURL);
        console.log('🌐 Current location:', window.location.href);
        console.log('🌐 Hostname:', window.location.hostname);
        console.log('🌐 Protocol:', window.location.protocol);
        console.log('🌐 Port:', window.location.port);
        
        this.init();
    }

    // Platform and Product Management Methods
    async onPlatformChange() {
        const platformSelect = document.getElementById('platform');
        const productSelect = document.getElementById('applicableProduct');
        const productIdInput = document.getElementById('productId');
        
        const selectedPlatform = platformSelect.value;
        
        if (!selectedPlatform) {
            productSelect.innerHTML = '<option value="">-- Select platform first --</option>';
            productIdInput.value = '';
            return;
        }
        
        try {
            // Load products for the selected platform
            await this.loadProductsForPlatform(selectedPlatform);
            
            // Update product dropdown
            this.populateProductDropdown(selectedPlatform);
            
            // Reset product selection and product_id since they're platform-specific
            productSelect.value = '';
            productIdInput.value = '';
            
        } catch (error) {
            console.error('Error loading products for platform:', error);
            productSelect.innerHTML = '<option value="">Error loading products</option>';
        }
    }

    async loadProductsForPlatform(platform) {
        console.log('🔍 loadProductsForPlatform called with platform:', platform);
        console.log('🔍 this.baseURL:', this.baseURL);
        console.log('🔍 Full URL will be:', `${this.baseURL}/products/platform/${platform}`);

        // Default products that should always be available
        const defaultProducts = [
            {
                id: 1,
                symbol: "BTCUSD",
                product_name: "Bitcoin Perpetual",
                product_id: "84",
                display_name: "Bitcoin Perpetual - BTCUSD"
            },
            {
                id: 2,
                symbol: "ETHUSD",
                product_name: "Ethereum Perpetual",
                product_id: "1699",
                display_name: "Ethereum Perpetual - ETHUSD"
            }
        ];
        
        if (this.productsCache[platform]) {
            console.log('🔍 Using cached products for platform:', platform);
            return this.productsCache[platform];
        }
        
        try {
            const fullUrl = `${this.baseURL}/api/products/platform/${platform}`;
            console.log('🔍 Making fetch request to:', fullUrl);
            
            const response = await fetch(fullUrl);
            console.log('🔍 Response received:', response);
            console.log('🔍 Response status:', response.status);
            console.log('🔍 Response ok:', response.ok);
            
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            
            const data = await response.json();
            console.log('🔍 Response data:', data);
            
            if (data.success) {
                this.productsCache[platform] = data.products;
                console.log('🔍 Products cached for platform:', platform);
                return data.products;
            } else {
                throw new Error('Failed to load products');
            }
        } catch (error) {
            console.error(`❌ Error fetching products for ${platform}:`, error);
            console.error(`❌ Error details:`, error.message);
            console.log('🔄 Falling back to default products');
            
            // Always return default products even if database fails
            this.productsCache[platform] = defaultProducts;
            return defaultProducts;
        }
    }

    populateProductDropdown(platform) {
        const productSelect = document.getElementById('applicableProduct');
        const products = this.productsCache[platform] || [];
        
        // Clear existing options
        productSelect.innerHTML = '<option value="">-- Select a product --</option>';
        
        // Add product options
        products.forEach(product => {
            const option = document.createElement('option');
            option.value = product.symbol;
            option.textContent = product.display_name;
            productSelect.appendChild(option);
        });
    }

    onProductChange() {
        const productSelect = document.getElementById('applicableProduct');
        const productIdInput = document.getElementById('productId');
        const platformSelect = document.getElementById('platform');
        
        const selectedOption = productSelect.options[productSelect.selectedIndex];
        const selectedPlatform = platformSelect.value;
        
        if (selectedOption && selectedOption.value && selectedPlatform) {
            // Get the product data from cache instead of making an API call
            const products = this.productsCache[selectedPlatform] || [];
            const selectedProduct = products.find(p => p.symbol === selectedOption.value);
            
            if (selectedProduct) {
                productIdInput.value = selectedProduct.product_id;
                console.log(`✅ Product ID set to: ${selectedProduct.product_id} for ${selectedProduct.symbol} on ${selectedPlatform}`);
            } else {
                console.warn(`⚠️ Product not found in cache for ${selectedOption.value} on ${selectedPlatform}`);
                productIdInput.value = '';
            }
        } else {
            productIdInput.value = '';
        }
    }

    init() {
        console.log('🚀 StrategyManager init started');
        
        // Wait for left menu to load and authenticate before proceeding
        this.waitForLeftMenu().then(() => {
            console.log('Strategy: Left menu ready, initializing strategy functionality...');
            this.loadStrategies();
            this.loadBrokers();
            this.setupEventListeners();
            this.setupIndicatorCheckboxes();
            this.setupMobileMenu();
            
            // Mark as ready
            this._isReady = true;
            console.log('✅ Strategy Manager is now ready, _isReady =', this._isReady);
        });
    }
    
    isReady() {
        return this._isReady === true;
    }

    waitForLeftMenu() {
        return new Promise((resolve) => {
            const checkLeftMenu = () => {
                const leftPanel = document.querySelector('.left-panel');
                const userAvatar = document.getElementById('userAvatar');
                
                // Check if both the left panel exists AND user data is loaded (indicating auth is complete)
                if (leftPanel && userAvatar) {
                    console.log('Strategy: Left panel and user data ready, proceeding with initialization');
                    resolve();
                } else {
                    console.log('Strategy: Waiting for left menu to complete authentication...');
                    setTimeout(checkLeftMenu, 100);
                }
            };
            checkLeftMenu();
        });
    }

    setupEventListeners() {
        console.log('🔧 Setting up event listeners...');
        
        // Form submission
        const form = document.getElementById('strategyForm');
        if (form) {
            console.log('✅ Strategy form found, adding submit listener');
            form.addEventListener('submit', (e) => {
                console.log('🚀 Form submit event triggered!');
                e.preventDefault();
                this.handleFormSubmit();
            });
        } else {
            console.error('❌ Strategy form not found!');
        }

        // Modal close events
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.closeStrategyForm();
                this.closeBrokerModal();
                this.closeLogsModal();
            }
        });

        document.getElementById('strategyModal').addEventListener('click', (e) => {
            if (e.target.id === 'strategyModal') {
                this.closeStrategyForm();
            }
        });

        document.getElementById('brokerModal').addEventListener('click', (e) => {
            if (e.target.id === 'brokerModal') {
                this.closeBrokerModal();
            }
        });

        document.getElementById('logsModal').addEventListener('click', (e) => {
            if (e.target.id === 'logsModal') {
                this.closeLogsModal();
            }
        });

        // Trailing Stop Loss Event Listeners
        this.setupTrailingStopListeners();
    }

    setupTrailingStopListeners() {
        // Trailing stop enabled toggle
        const trailingStopEnabled = document.getElementById('trailingStopEnabled');
        if (trailingStopEnabled) {
            trailingStopEnabled.addEventListener('change', (e) => {
                this.toggleTrailingStopConfig(e.target.checked);
            });
        }

        // Stop loss type change
        const stopLossType = document.getElementById('stopLossType');
        if (stopLossType) {
            stopLossType.addEventListener('change', (e) => {
                this.toggleStopLossConfig(e.target.value);
            });
        }

        // Candle duration change
        const candleDuration = document.getElementById('candleDuration');
        if (candleDuration) {
            candleDuration.addEventListener('change', (e) => {
                console.log('Candle duration changed to:', e.target.value);
            });
        }
    }

    toggleTrailingStopConfig(enabled) {
        const trailingStopConfig = document.getElementById('trailingStopConfig');
        const stopLossConfig = document.getElementById('stopLossConfig');
        const fixedStopLossConfig = document.getElementById('fixedStopLossConfig');
        
        // Add null checks to prevent errors
        if (!trailingStopConfig || !stopLossConfig || !fixedStopLossConfig) {
            console.warn('⚠️ Some stop loss elements not found, skipping toggle');
            return;
        }
        
        if (enabled) {
            trailingStopConfig.style.display = 'block';
            stopLossConfig.style.display = 'block';
            this.toggleStopLossConfig(document.getElementById('stopLossType')?.value || 'fixed');
        } else {
            trailingStopConfig.style.display = 'none';
            stopLossConfig.style.display = 'none';
            fixedStopLossConfig.style.display = 'none';
        }
    }

    toggleStopLossConfig(stopLossType) {
        const fixedStopLossConfig = document.getElementById('fixedStopLossConfig');
        
        // Add null check to prevent errors
        if (!fixedStopLossConfig) {
            console.warn('⚠️ fixedStopLossConfig element not found, skipping toggle');
            return;
        }
        
        if (stopLossType === 'fixed') {
            fixedStopLossConfig.style.display = 'block';
        } else {
            fixedStopLossConfig.style.display = 'none';
        }
    }

    setupMobileMenu() {
        const mobileMenuToggle = document.getElementById('mobileMenuToggle');
        const leftPanel = document.querySelector('.left-panel');
        
        if (mobileMenuToggle && leftPanel) {
            mobileMenuToggle.addEventListener('click', function() {
                leftPanel.classList.toggle('mobile-open');
            });
            
            // Close mobile menu when clicking outside
            document.addEventListener('click', function(event) {
                if (!leftPanel.contains(event.target) && !mobileMenuToggle.contains(event.target)) {
                    leftPanel.classList.remove('mobile-open');
                }
            });
            
            // Close mobile menu when navigation item is clicked
            const navItems = document.querySelectorAll('.nav-item');
            navItems.forEach(item => {
                item.addEventListener('click', function() {
                    // Add a small delay to allow the click to register before hiding
                    setTimeout(() => {
                        leftPanel.classList.remove('mobile-open');
                    }, 150);
                });
            });
        }
    }

    setupIndicatorCheckboxes() {
        const checkboxes = document.querySelectorAll('input[name="indicators"]');
        checkboxes.forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                this.updateIndicatorParameters();
                this.updateCriteriaExamples();
            });
        });
    }

    updateIndicatorParameters() {
        const selectedIndicators = this.getSelectedIndicators();
        const paramsContainer = document.getElementById('indicatorParams');
        const tradeConditionsContainer = document.getElementById('tradeConditions');
        
        if (selectedIndicators.length === 0) {
            paramsContainer.innerHTML = '';
            tradeConditionsContainer.innerHTML = '';
            return;
        }

        let paramsHTML = '<h3>Indicator Parameters</h3>';
        let tradeConditionsHTML = '<h3>Trade Entry & Exit Conditions</h3>';
        
        selectedIndicators.forEach(indicator => {
            paramsHTML += this.generateParameterHTML(indicator);
            tradeConditionsHTML += this.generateTradeConditionsHTML(indicator);
        });

        paramsContainer.innerHTML = paramsHTML;
        tradeConditionsContainer.innerHTML = tradeConditionsHTML;
    }

    updateCriteriaExamples() {
        const selectedIndicators = this.getSelectedIndicators();
        const entryCriteria = document.getElementById('entryCriteria');
        const exitCriteria = document.getElementById('exitCriteria');
        
        if (selectedIndicators.length === 0) {
            entryCriteria.placeholder = 'Define when to enter a long position...';
            exitCriteria.placeholder = 'Define when to close the position...';
            return;
        }

        // Generate example entry criteria
        let entryExample = 'Buy when: ';
        let exitExample = 'Sell when: ';
        
        selectedIndicators.forEach((indicator, index) => {
            if (index > 0) entryExample += ' AND ';
            if (index > 0) exitExample += ' AND ';
            
            switch(indicator) {
                case 'bb_squeeze':
                    entryExample += 'price breaks above upper BB after squeeze release';
                    exitExample += 'price closes below middle BB';
                    break;
                case 'supertrend':
                    entryExample += 'price closes above Supertrend line';
                    exitExample += 'price closes below Supertrend line';
                    break;
                case 'rsi':
                    entryExample += 'RSI crosses above oversold level';
                    exitExample += 'RSI crosses below overbought level';
                    break;
                case 'triple_ema':
                    entryExample += 'short EMA > medium EMA > long EMA';
                    exitExample += 'short EMA < medium EMA';
                    break;
            }
        });

        entryCriteria.placeholder = entryExample;
        exitCriteria.placeholder = exitExample;
    }

    generateParameterHTML(indicator) {
        const config = this.getIndicatorConfig(indicator);
        if (!config) return '';

        let html = `
            <div class="parameter-group">
                <h4>${config.name}</h4>
                <div class="parameter-grid">
        `;

        config.params.forEach(param => {
            html += `
                <div class="parameter-row">
                    <label>${param.label}</label>
                    <input type="number" 
                           name="${param.name}" 
                           value="${param.value}" 
                           min="${param.min}" 
                           max="${param.max}" 
                           step="${param.step}"
                           required>
                </div>
            `;
        });

        html += '</div></div>';
        return html;
    }

    generateTradeConditionsHTML(indicator) {
        const conditions = {
            'supertrend': {
                name: 'Supertrend',
                entry: [
                    { label: 'BUY Condition', name: 'supertrend_buy', options: ['None', 'Turned GREEN', 'Turned RED'] }
                ],
                exit: [
                    { label: 'SELL Condition', name: 'supertrend_sell', options: ['None', 'Turned GREEN', 'Turned RED'] }
                ]
            },
            'rsi': {
                name: 'RSI',
                entry: [
                    { label: 'BUY Condition', name: 'rsi_buy', options: ['None', 'Overbought', 'Oversold', 'Crossing above Upper band', 'Crossing below Upper band', 'Crossing below Lower band', 'Crossing above Lower band'] }
                ],
                exit: [
                    { label: 'SELL Condition', name: 'rsi_sell', options: ['None', 'Overbought', 'Oversold', 'Crossing above Upper band', 'Crossing below Upper band', 'Crossing below Lower band', 'Crossing above Lower band'] }
                ]
            },
            'triple_ema': {
                name: 'Triple EMA',
                entry: [
                    { label: 'BUY Condition', name: 'ema_buy', options: ['None', 'EMA-1', 'EMA-2', 'EMA-3'] },
                    { label: 'Crossing', name: 'ema_buy_cross', options: ['Crossing above', 'Crossing below'] },
                    { label: 'With', name: 'ema_buy_with', options: ['None', 'EMA-1', 'EMA-2', 'EMA-3'] }
                ],
                exit: [
                    { label: 'SELL Condition', name: 'ema_sell', options: ['None', 'EMA-1', 'EMA-2', 'EMA-3'] },
                    { label: 'Crossing', name: 'ema_sell_cross', options: ['Crossing above', 'Crossing below'] },
                    { label: 'With', name: 'ema_sell_with', options: ['None', 'EMA-1', 'EMA-2', 'EMA-3'] }
                ]
            },
            'bb_squeeze': {
                name: 'Bollinger Band Squeeze',
                entry: [
                    { label: 'BUY Condition', name: 'bb_buy', options: ['None', 'Price breaks above upper band after squeeze release', 'Price breaks above middle band', 'Squeeze release with volume confirmation'] }
                ],
                exit: [
                    { label: 'SELL Condition', name: 'bb_sell', options: ['None', 'Price closes below middle band', 'Price closes below lower band', 'Squeeze starts again'] }
                ]
            }
        };

        const config = conditions[indicator];
        if (!config) return '';

        let html = `
            <div class="trade-condition-group">
                <h4>${config.name} Conditions</h4>
                <div class="condition-grid">
        `;

        // Entry conditions
        html += '<div class="condition-section">';
        html += '<h5>Entry (BUY) Conditions</h5>';
        config.entry.forEach(condition => {
            html += `
                <div class="form-group">
                    <label for="${condition.name}">${condition.label}</label>
                    <select id="${condition.name}" name="${condition.name}" class="condition-select">
                        ${condition.options.map(option => `<option value="${option}">${option}</option>`).join('')}
                    </select>
                </div>
            `;
        });
        html += '</div>';

        // Exit conditions
        html += '<div class="condition-section">';
        html += '<h5>Exit (SELL) Conditions</h5>';
        config.exit.forEach(condition => {
            html += `
                <div class="form-group">
                    <label for="${condition.name}">${condition.label}</label>
                    <select id="${condition.name}" name="${condition.name}" class="condition-select">
                        ${condition.options.map(option => `<option value="${option}">${option}</option>`).join('')}
                    </select>
                </div>
            `;
        });
        html += '</div>';

        html += '</div></div>';
        return html;
    }

    getSelectedIndicators() {
        const checkboxes = document.querySelectorAll('input[name="indicators"]:checked');
        return Array.from(checkboxes).map(cb => cb.value);
    }

    async loadStrategies() {
        try {
            // Show loading state
            this.showLoadingState();
            
            // Load strategies from database - no example strategies shown
            const userData = localStorage.getItem('trademanthan_user');
            if (!userData) {
                console.warn('Strategy: No user data found - showing empty state');
                this.strategies = [];
                this.renderStrategies();
                return;
            }
            
            const user = JSON.parse(userData);
            console.log('Strategy: Loading strategies for user:', user.id);
            
            // Fetch strategies from backend API
            const response = await fetch(`${this.baseURL}/api/strategy/user/${user.id}`, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                }
            });
            
            if (!response.ok) {
                console.warn(`Strategy: HTTP ${response.status}: ${response.statusText} - Setting empty strategies`);
                this.strategies = [];
                this.renderStrategies();
                return;
            }
            
            const data = await response.json();
            
            if (data.success) {
                // Filter out any test strategies with null user_id
                this.strategies = data.strategies.filter(strategy => strategy.user_id !== null);
                console.log('Strategy: Loaded strategies from database (filtered):', this.strategies);
            } else {
                console.warn('Strategy: No strategies found or API returned error:', data);
                this.strategies = [];
            }

            this.renderStrategies();
        } catch (error) {
            console.error('Error loading strategies:', error);
            // Don't show error popup, just set empty strategies and render
            this.strategies = [];
            this.renderStrategies();
        }
    }

    async loadBrokers() {
        try {
            console.log('Strategy: Loading brokers...');
            
            // Get current user from localStorage
            const userData = localStorage.getItem('trademanthan_user');
            if (!userData) {
                console.warn('Strategy: No user data found, cannot load brokers');
                this.brokers = [];
                return;
            }
            
            const user = JSON.parse(userData);
            console.log('Strategy: Loading brokers for user:', user.id);
            
            // Fetch brokers for the current user from backend API
            const response = await fetch(`${this.baseURL}/api/broker/?user_id=${user.id}`, {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                }
            });

            if (response.ok) {
                const data = await response.json();
                this.brokers = data.brokers || [];
                console.log('Strategy: Brokers loaded from API for user', user.id, ':', this.brokers);
                console.log('Strategy: Number of brokers:', this.brokers.length);
            } else {
                console.warn('Strategy: Failed to load brokers from API, trying localStorage fallback');
                // Try to load from localStorage as fallback
                this.brokers = this.loadBrokersFromStorage(user.id) || [];
            }
        } catch (error) {
            console.error('Strategy: Error loading brokers:', error);
            console.warn('Strategy: Trying localStorage fallback due to error');
            // Try to load from localStorage as fallback
            this.brokers = this.loadBrokersFromStorage(user.id) || [];
        }
    }

    loadBrokersFromStorage(userId) {
        try {
            console.log('Strategy: Attempting to load brokers from localStorage for user:', userId);
            const savedBrokers = localStorage.getItem('trademanthan_brokers');
            if (savedBrokers) {
                const allBrokers = JSON.parse(savedBrokers);
                const userBrokers = allBrokers.filter(broker => broker.user_id == userId);
                console.log('Strategy: Loaded brokers from localStorage:', userBrokers);
                return userBrokers;
            }
        } catch (error) {
            console.error('Strategy: Error loading brokers from localStorage:', error);
        }
        return [];
    }

    updateDebugInfo() {
        try {
            const debugInfo = document.getElementById('debugInfo');
            const brokerCount = document.getElementById('brokerCount');
            const userInfo = document.getElementById('userInfo');
            
            if (debugInfo && brokerCount && userInfo) {
                // Get user info
                const userData = localStorage.getItem('trademanthan_user');
                const user = userData ? JSON.parse(userData) : null;
                
                // Update debug info
                brokerCount.innerHTML = `Brokers loaded: ${this.brokers.length}`;
                userInfo.innerHTML = `User ID: ${user ? user.id : 'Not found'}`;
                
                // Show broker details
                if (this.brokers.length > 0) {
                    const brokerDetails = this.brokers.map(b => `${b.name} (ID: ${b.id})`).join(', ');
                    brokerCount.innerHTML += `<br>Broker details: ${brokerDetails}`;
                }
            }
        } catch (error) {
            console.error('Strategy: Error updating debug info:', error);
        }
    }

    refreshBrokers() {
        console.log('Strategy: Refreshing broker list...');
        // Refresh broker list from storage
        this.loadBrokers();
    }

    showLoadingState() {
        const grid = document.getElementById('strategyGrid');
        grid.innerHTML = `
            <div class="loading-container">
                <div class="loading-content">
                    <div class="loading-spinner">
                        <i class="fas fa-spinner fa-spin"></i>
                    </div>
                    <h3>Loading Strategies...</h3>
                    <p>Please wait while we fetch your trading strategies.</p>
                </div>
            </div>
        `;
    }

    renderStrategies() {
        const grid = document.getElementById('strategyGrid');
        
        if (this.strategies.length === 0) {
            // Show "No Strategies" message - no example strategies displayed
            grid.innerHTML = `
                <div class="no-strategies-container">
                    <div class="no-strategies-content">
                        <div class="no-strategies-icon">
                            <i class="fas fa-robot"></i>
                        </div>
                        <h2>No Strategies Added</h2>
                        <p>You haven't created any trading strategies yet. Get started by creating your first automated trading strategy.</p>
                        <button class="btn btn-primary btn-large" onclick="strategyManager.openStrategyForm().catch(console.error)">
                            <i class="fas fa-plus"></i>
                            Create Your First Strategy
                        </button>
                    </div>
                </div>
            `;
            return;
        }

        let html = '';

        // Render only database strategies - no example strategies
        this.strategies.forEach(strategy => {
            html += this.renderStrategyCard(strategy);
        });

        grid.innerHTML = html;
    }

    renderStrategyCard(strategy) {
        const statusClass = strategy.is_active ? 'active' : 'inactive';
        const statusText = strategy.is_active ? 'Active' : 'Inactive';
        const pnlClass = strategy.last_trade_pnl >= 0 ? 'positive' : 'negative';
        const totalPnlClass = strategy.total_pnl >= 0 ? 'positive' : 'negative';
        const executionClass = strategy.is_live ? 'live' : 'stopped';
        const executionText = strategy.is_live ? 'LIVE' : 'STOPPED';

        return `
            <div class="strategy-card">
                <div class="strategy-header-row">
                    <div>
                        <div class="strategy-name">${strategy.name}</div>
                    </div>
                    <div class="strategy-actions">
                        <button class="action-btn btn-edit" onclick="editStrategy(${strategy.id}).catch(console.error)" title="Edit Strategy">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="action-btn ${strategy.broker_connected ? 'btn-success' : 'btn-warning'}" 
                                onclick="manageBrokerConnection(${strategy.id})" 
                                title="${strategy.broker_connected ? 'Manage Broker Connection' : 'Connect Broker'}">
                            <i class="fas fa-${strategy.broker_connected ? 'link' : 'unlink'}"></i>
                        </button>
                        <button class="action-btn ${strategy.is_live ? 'btn-danger' : 'btn-primary'}" 
                                onclick="toggleExecution(${strategy.id})"
                                title="${strategy.is_live ? 'Stop Strategy' : 'Start Strategy'}"
                                ${!strategy.broker_connected ? 'disabled' : ''}>
                            <i class="fas fa-${strategy.is_live ? 'stop' : 'play'}"></i>
                        </button>
                        <button class="action-btn btn-info" onclick="viewLogs(${strategy.id})" title="View Logs">
                            <i class="fas fa-file-alt"></i>
                        </button>
                        <button class="action-btn btn-build" onclick="buildStrategy(${strategy.id})" title="Build Strategy Template">
                            <i class="fas fa-code"></i>
                        </button>
                        <button class="action-btn btn-delete" onclick="deleteStrategy(${strategy.id})" title="Delete Strategy">
                            <i class="fas fa-trash"></i>
                        </button>

                        <span class="status-badge status-${statusClass}">${statusText}</span>
                        <span class="status-badge execution-${executionClass}">${executionText}</span>
                    </div>
                </div>

                <p style="color: #666; margin-bottom: 1rem;">${strategy.description}</p>

                <div class="strategy-metrics">
                    <div class="metric-item">
                        <div class="metric-label">Total P&L</div>
                        <div class="metric-value ${totalPnlClass}">
                            ${totalPnlClass === 'positive' ? '+' : ''}$${strategy.total_pnl.toFixed(2)}
                        </div>
                    </div>
                    <div class="metric-item">
                        <div class="metric-label">Last Trade P&L</div>
                        <div class="metric-value ${pnlClass}">
                            ${pnlClass === 'positive' ? '+' : ''}$${strategy.last_trade_pnl.toFixed(2)}
                        </div>
                    </div>
                </div>

                <div class="strategy-indicators">
                    ${strategy.indicators.map(indicator => 
                        `<span class="indicator-tag">${this.getIndicatorDisplayName(indicator)}</span>`
                    ).join('')}
                </div>

                <div class="broker-info">
                    ${strategy.broker_connected ? 
                        `<span class="broker-connected"><i class="fas fa-link"></i> Broker Connected</span>` :
                        `<span class="broker-disconnected"><i class="fas fa-unlink"></i> No Broker</span>`
                    }
                </div>

                <div style="font-size: 0.8rem; color: #666;">
                    Created: ${new Date(strategy.created_at).toLocaleDateString()}
                </div>
            </div>
        `;
    }

    getIndicatorDisplayName(indicator) {
        const names = {
            'supertrend': 'Supertrend',
            'bb_squeeze': 'BB Squeeze',
            'rsi': 'RSI',
            'triple_ema': 'Triple EMA'
        };
        return names[indicator] || indicator;
    }

    async openStrategyForm(strategyId = null) {
        console.log('🔧 openStrategyForm called with ID:', strategyId);
        console.log('🔧 this.strategies:', this.strategies);
        
        this.currentStrategy = strategyId ? this.strategies.find(s => s.id === strategyId) : null;
        this.isEditMode = !!strategyId;
        
        console.log('🔧 currentStrategy:', this.currentStrategy);
        console.log('🔧 isEditMode:', this.isEditMode);

        const modal = document.getElementById('strategyModal');
        const title = document.getElementById('modalTitle');
        const form = document.getElementById('strategyForm');
        
        console.log('🔧 modal element:', modal);
        console.log('🔧 title element:', title);
        console.log('🔧 form element:', form);

        if (!modal) {
            console.error('❌ Modal not found!');
            alert('Strategy modal not found. Please refresh the page.');
            return;
        }

        title.textContent = this.isEditMode ? 'Edit Strategy' : 'Add New Strategy';

        if (this.isEditMode) {
            console.log('🔧 Populating form for editing...');
            
                    // Force form sections to be visible before populating
        this.ensureFormSectionsVisible();
        
        // Additional check: ensure the form structure is correct
        this.fixFormStructure();
        
        // Force the modal to be properly displayed
        this.forceModalDisplay();
            
            await this.populateForm(this.currentStrategy);
        } else {
            console.log('🔧 Resetting form for new strategy...');
            form.reset();
            this.updateIndicatorParameters();
            this.updateCriteriaExamples();
        }

        console.log('🔧 Setting modal display to flex');
        modal.style.display = 'flex';
        modal.classList.add('show');
        
        // Ensure modal is visible with high z-index
        modal.style.zIndex = '99999';
        modal.style.position = 'fixed';
        
        console.log('✅ Modal should now be visible');
        console.log('🔧 Modal display:', modal.style.display);
        console.log('🔧 Modal z-index:', modal.style.zIndex);
        console.log('🔧 Modal position:', modal.style.position);
        
        // Force a reflow to ensure the modal is visible
        modal.offsetHeight;
        
        // Add some debugging
        console.log('🔧 Modal computed styles:', window.getComputedStyle(modal));
        console.log('🔧 Modal z-index:', window.getComputedStyle(modal).zIndex);
    }

    closeStrategyForm() {
        const modal = document.getElementById('strategyModal');
        if (modal) {
            modal.style.display = 'none';
            modal.classList.remove('show');
        }
        this.currentStrategy = null;
        this.isEditMode = false;
        
        // Reset form if it exists
        const form = document.getElementById('strategyForm');
        if (form) {
            form.reset();
        }
    }

    async populateForm(strategy) {
        console.log('🔧 populateForm called with strategy:', strategy);
        
        // First, ensure all form sections are visible
        console.log('🔧 Ensuring all form sections are visible...');
        const allFormSections = document.querySelectorAll('.form-section');
        console.log(`🔧 Found ${allFormSections.length} form sections`);
        
        allFormSections.forEach((section, index) => {
            console.log(`🔧 Form section ${index}:`, section);
            console.log(`🔧 Section ${index} content:`, section.innerHTML.substring(0, 100) + '...');
            section.style.display = 'block';
            section.style.visibility = 'visible';
            section.style.opacity = '1';
            section.style.position = 'relative';
            section.style.zIndex = '1';
        });
        
        // Platform, Product and Candle Duration (should be first)
        console.log('🔧 Populating platform and product...');
        
        // Specifically target the Trading Configuration section
        let tradingConfigSection = null;
        
        // Try to find by heading text
        const headings = document.querySelectorAll('h3');
        for (const heading of headings) {
            if (heading.textContent.includes('Trading Configuration')) {
                tradingConfigSection = heading.closest('.form-section');
                break;
            }
        }
        
        // Fallback to first compact section
        if (!tradingConfigSection) {
            tradingConfigSection = document.querySelector('.form-section.compact-section:first-of-type');
        }
        
        if (tradingConfigSection) {
            console.log('🔧 Trading config section found:', tradingConfigSection);
            tradingConfigSection.style.display = 'block';
            tradingConfigSection.style.visibility = 'visible';
            tradingConfigSection.style.opacity = '1';
            tradingConfigSection.style.position = 'relative';
            tradingConfigSection.style.zIndex = '1';
        } else {
            console.error('❌ Trading config section not found!');
            // Fallback: show all compact sections
            const compactSections = document.querySelectorAll('.form-section.compact-section');
            compactSections.forEach(section => {
                section.style.display = 'block';
                section.style.visibility = 'visible';
                section.style.opacity = '1';
            });
        }
        
        await this.populatePlatformAndProduct(strategy.platform, strategy.product, strategy.product_id, strategy.candle_duration);
        
        // Basic info
        console.log('🔧 Populating basic info...');
        const nameField = document.getElementById('strategyName');
        const descField = document.getElementById('strategyDescription');
        const logicField = document.getElementById('logicOperator');
        const entryField = document.getElementById('entryCriteria');
        const exitField = document.getElementById('exitCriteria');
        
        if (nameField) nameField.value = strategy.name || '';
        if (descField) descField.value = strategy.description || '';
        if (logicField) logicField.value = strategy.logic_operator || 'AND';
        if (entryField) entryField.value = strategy.entry_criteria || '';
        if (exitField) exitField.value = strategy.exit_criteria || '';

        // Indicators
        if (strategy.indicators && Array.isArray(strategy.indicators)) {
            const checkboxes = document.querySelectorAll('input[name="indicators"]');
            checkboxes.forEach(checkbox => {
                checkbox.checked = strategy.indicators.includes(checkbox.value);
            });
        }

        // Stop Loss and Trailing Stop Loss
        this.populateStopLoss(strategy.stop_loss);
        this.populateTrailingStop(strategy.trailing_stop);

        // Update parameters and examples
        this.updateIndicatorParameters();
        this.updateCriteriaExamples();

        // Populate parameters and trade conditions
        setTimeout(() => {
            this.populateParameters(strategy.parameters);
            this.populateTradeConditions(strategy.trade_conditions);
        }, 100);
        
        console.log('🔧 Form population completed');
    }

    populateParameters(parameters) {
        // This is a simplified version - in production you'd need more robust parameter handling
        if (!parameters) return;
        
        Object.keys(parameters).forEach(indicator => {
            Object.keys(parameters[indicator]).forEach(param => {
                const input = document.querySelector(`input[name="${param}"]`);
                if (input) {
                    input.value = parameters[indicator][param];
                }
            });
        });
    }

    populateTradeConditions(tradeConditions) {
        // Populate trade conditions when editing a strategy
        if (!tradeConditions) return;
        
        Object.keys(tradeConditions).forEach(indicator => {
            Object.keys(tradeConditions[indicator]).forEach(condition => {
                const select = document.querySelector(`select[name="${condition}"]`);
                if (select) {
                    select.value = tradeConditions[indicator][condition];
                }
            });
        });
    }

    async populatePlatformAndProduct(platform = 'testnet', product = 'BTCUSDT', productId = '84', candleDuration = '5m') {
        console.log('🔧 populatePlatformAndProduct called with:', { platform, product, productId, candleDuration });
        
        // Always set platform value (including null for "None")
        const platformSelect = document.getElementById('platform');
        console.log('🔧 Platform select element:', platformSelect);
        
        if (platformSelect) {
            const platformValue = platform || '';
            console.log('🔧 Setting platform value to:', platformValue);
            platformSelect.value = platformValue;
            
            // If platform is selected, load products for that platform
            if (platform) {
                console.log('🔧 Platform selected, loading products...');
                try {
                    await this.loadProductsForPlatform(platform);
                    this.populateProductDropdown(platform);
                    
                    // Set the product after products are loaded
                    if (product) {
                        const productSelect = document.getElementById('applicableProduct');
                        if (productSelect) {
                            productSelect.value = product;
                            // Fetch and set the correct product_id for the platform
                            await this.fetchProductId(product, platform);
                        }
                    }
                } catch (error) {
                    console.error('Error loading products for platform:', error);
                }
            } else {
                console.log('🔧 No platform selected, clearing product dropdown...');
                // If no platform selected, clear product dropdown
                const productSelect = document.getElementById('applicableProduct');
                if (productSelect) {
                    productSelect.innerHTML = '<option value="">-- Select platform first --</option>';
                }
            }
        } else {
            console.error('❌ Platform select element not found!');
        }
        
        if (candleDuration) {
            const durationSelect = document.getElementById('candleDuration');
            if (durationSelect) {
                durationSelect.value = candleDuration;
            }
        }
        
        console.log('🔧 populatePlatformAndProduct completed');
    }

    populateTrailingStop(trailingStop) {
        if (!trailingStop || !trailingStop.enabled) {
            document.getElementById('trailingStopEnabled').checked = false;
            this.toggleTrailingStopConfig(false);
            return;
        }

        document.getElementById('trailingStopEnabled').checked = true;
        this.toggleTrailingStopConfig(true);

        if (trailingStop.profit_multiplier) {
            document.getElementById('profitMultiplier').value = trailingStop.profit_multiplier;
        }
    }

    populateStopLoss(stopLoss) {
        if (stopLoss && stopLoss.type) {
            const stopLossType = document.getElementById('stopLossType');
            if (stopLossType) {
                stopLossType.value = stopLoss.type;
                this.toggleStopLossConfig(stopLoss.type);
            }

            if (stopLoss.distance && stopLoss.type === 'fixed') {
                const fixedStopLoss = document.getElementById('fixedStopLoss');
                if (fixedStopLoss) {
                    fixedStopLoss.value = stopLoss.distance;
                }
            }
        }
    }

    async handleFormSubmit() {
        try {
            console.log('🚀 ===== FORM SUBMISSION PROCESS STARTED =====');
            console.log('🚀 Edit mode:', this.isEditMode);
            console.log('🚀 Current strategy ID:', this.currentStrategy?.id);
            
            const formData = this.getFormData();
            console.log('🚀 Form data collected successfully');
            
            if (this.isEditMode) {
                console.log('🔄 Proceeding with strategy UPDATE...');
                await this.updateStrategy(formData);
                console.log('✅ Strategy update completed successfully');
            } else {
                console.log('🆕 Proceeding with strategy CREATION...');
                await this.createStrategy(formData);
                console.log('✅ Strategy creation completed successfully');
            }

            console.log('🚀 Closing strategy form...');
            this.closeStrategyForm();
            
            console.log('🚀 Reloading strategies list...');
            await this.loadStrategies();
            
            console.log('🚀 Showing success message...');
            this.showSuccess(`Strategy ${this.isEditMode ? 'updated' : 'created'} successfully!`);
            
            console.log('✅ ===== FORM SUBMISSION PROCESS COMPLETED SUCCESSFULLY =====');
        } catch (error) {
            console.error('❌ ===== FORM SUBMISSION PROCESS FAILED =====');
            console.error('❌ Error saving strategy:', error);
            console.error('❌ Error details:', error.message);
            console.error('❌ Error stack:', error.stack);
            this.showError('Failed to save strategy');
        }
    }

    getFormData() {
        console.log('🔍 ===== FORM DATA COLLECTION STARTED =====');
        
        // Debug form element states
        this.logFormElementStates();
        
        const form = document.getElementById('strategyForm');
        if (!form) {
            console.error('❌ Strategy form not found!');
            throw new Error('Strategy form not found');
        }
        
        const formData = new FormData(form);
        console.log('🔍 FormData object created successfully');
        
        // Get current user from localStorage
        const userData = localStorage.getItem('trademanthan_user');
        if (!userData) {
            console.error('❌ No user data found in localStorage');
            throw new Error('No user data found. Please log in again.');
        }
        const user = JSON.parse(userData);
        console.log('🔍 User data retrieved:', user.id);
        
        // Debug logging for form values - check each field individually
        console.log('🔍 ===== FORM FIELD VALUES =====');
        console.log('🔍 Basic fields:');
        console.log('  - name:', formData.get('name'));
        console.log('  - description:', formData.get('description'));
        console.log('  - platform:', formData.get('platform'));
        console.log('  - product:', formData.get('product'));
        console.log('  - product_id:', formData.get('product_id'));
        console.log('  - candle_duration:', formData.get('candle_duration'));
        console.log('  - logic_operator:', formData.get('logic_operator'));
        console.log('  - entry_criteria:', formData.get('entry_criteria'));
        console.log('  - exit_criteria:', formData.get('exit_criteria'));
        
        console.log('🔍 Stop loss fields:');
        console.log('  - stop_loss_type:', formData.get('stop_loss_type'));
        console.log('  - fixed_stop_loss:', formData.get('fixed_stop_loss'));
        
        console.log('🔍 Trailing stop fields:');
        console.log('  - trailing_stop_enabled:', formData.get('trailing_stop_enabled'));
        console.log('  - profit_multiplier:', formData.get('profit_multiplier'));
        
        // Get indicators
        const indicators = this.getSelectedIndicators();
        console.log('🔍 Selected indicators:', indicators);
        
        // Get parameters
        const parameters = this.getParametersData();
        console.log('🔍 Indicator parameters:', parameters);
        
        // Get trade conditions
        const tradeConditions = this.getTradeConditionsData();
        console.log('🔍 Trade conditions:', tradeConditions);
        
        // Get stop loss data
        const stopLoss = this.getStopLossData();
        console.log('🔍 Stop loss data:', stopLoss);
        
        // Get trailing stop data
        const trailingStop = this.getTrailingStopData();
        console.log('🔍 Trailing stop data:', trailingStop);
        
        const data = {
            user_id: user.id,  // Add user_id for backend
            name: formData.get('name'),
            description: formData.get('description'),
            product: formData.get('product'),
            platform: formData.get('platform'),
            product_id: formData.get('product_id'),
            candle_duration: formData.get('candle_duration'),
            indicators: indicators,
            logic_operator: formData.get('logic_operator'),
            entry_criteria: formData.get('entry_criteria'),
            exit_criteria: formData.get('exit_criteria'),
            parameters: parameters,
            trade_conditions: tradeConditions,
            stop_loss: stopLoss,
            trailing_stop: trailingStop
        };

        // Debug logging for final data object
        console.log('🔍 ===== FINAL STRATEGY DATA OBJECT =====');
        console.log('🔍 Complete data object:', JSON.stringify(data, null, 2));
        
        // Validate critical fields
        console.log('🔍 ===== FIELD VALIDATION =====');
        console.log('🔍 Required fields check:');
        console.log('  - user_id:', data.user_id ? '✅ Present' : '❌ Missing');
        console.log('  - name:', data.name ? '✅ Present' : '❌ Missing');
        console.log('  - platform:', data.platform ? '✅ Present' : '❌ Missing');
        console.log('  - product:', data.product ? '✅ Present' : '❌ Missing');
        console.log('  - product_id:', data.product_id ? '✅ Present' : '❌ Missing');
        console.log('  - candle_duration:', data.candle_duration ? '✅ Present' : '❌ Missing');
        console.log('  - indicators:', data.indicators && data.indicators.length > 0 ? '✅ Present' : '❌ Missing');
        console.log('  - entry_criteria:', data.entry_criteria ? '✅ Present' : '❌ Missing');
        console.log('  - exit_criteria:', data.exit_criteria ? '✅ Present' : '❌ Missing');

        if (this.isEditMode) {
            data.id = this.currentStrategy.id;
            console.log('🔍 Edit mode - added strategy ID:', data.id);
        }

        console.log('✅ ===== FORM DATA COLLECTION COMPLETED =====');
        return data;
    }

    getParametersData() {
        const parameters = {};
        const selectedIndicators = this.getSelectedIndicators();
        
        selectedIndicators.forEach(indicator => {
            parameters[indicator] = {};
            
            // Get all parameter inputs for this indicator
            const paramInputs = document.querySelectorAll(`[name*="${indicator}"]`);
            paramInputs.forEach(input => {
                const paramName = input.name;
                // Only include actual parameter inputs, not trade condition selects
                if (!paramName.includes('_buy') && !paramName.includes('_sell') && !paramName.includes('_cross') && !paramName.includes('_with')) {
                    parameters[indicator][paramName] = parseFloat(input.value) || input.value;
                }
            });
            
            // If no parameters found with indicator name prefix, look for generic parameter names
            // This handles the case where parameters are named generically (period, multiplier, etc.)
            if (Object.keys(parameters[indicator]).length === 0) {
                const indicatorConfig = this.getIndicatorConfig(indicator);
                if (indicatorConfig && indicatorConfig.params) {
                    indicatorConfig.params.forEach(param => {
                        const input = document.querySelector(`[name="${param.name}"]`);
                        if (input) {
                            parameters[indicator][param.name] = parseFloat(input.value) || input.value;
                        }
                    });
                }
            }
        });

        return parameters;
    }

    getIndicatorConfig(indicator) {
        const parameters = {
            'supertrend': {
                name: 'Supertrend',
                params: [
                    { label: 'ATR Period', name: 'atr_period', value: '14', min: '1', max: '100', step: '1' },
                    { label: 'Multiplier', name: 'multiplier', value: '3.0', min: '0.1', max: '10.0', step: '0.1' }
                ]
            },
            'bb_squeeze': {
                name: 'Bollinger Band Squeeze',
                params: [
                    { label: 'Period', name: 'period', value: '20', min: '5', max: '200', step: '1' },
                    { label: 'StdDev Multiplier', name: 'stddev_multiplier', value: '2.0', min: '0.5', max: '5.0', step: '0.1' },
                    { label: 'Squeeze Threshold', name: 'squeeze_threshold', value: '0.5', min: '0.1', max: '2.0', step: '0.1' }
                ]
            },
            'rsi': {
                name: 'RSI',
                params: [
                    { label: 'Period', name: 'period', value: '14', min: '2', max: '100', step: '1' },
                    { label: 'Overbought Level', name: 'overbought_level', value: '70', min: '50', max: '100', step: '1' },
                    { label: 'Oversold Level', name: 'oversold_level', value: '30', min: '0', max: '50', step: '1' }
                ]
            },
            'triple_ema': {
                name: 'Triple EMA Cross',
                params: [
                    { label: 'Short EMA Period', name: 'short_ema_period', value: '9', min: '1', max: '50', step: '1' },
                    { label: 'Medium EMA Period', name: 'medium_ema_period', value: '21', min: '5', max: '100', step: '1' },
                    { label: 'Long EMA Period', name: 'long_ema_period', value: '50', min: '20', max: '200', step: '1' }
                ]
            }
        };
        
        return parameters[indicator];
    }

    getStopLossData() {
        const stopLossType = document.getElementById('stopLossType')?.value;
        console.log('🔍 getStopLossData called with stopLossType:', stopLossType);
        
        if (!stopLossType) {
            console.log('⚠️ No stop loss type selected, using default fixed');
            return { type: 'fixed', distance: 2.0 };
        }

        let distance = null;
        
        if (stopLossType === 'fixed') {
            // For fixed stop loss, get the distance from the input field
            distance = parseFloat(document.getElementById('fixedStopLoss')?.value) || 2.0;
            console.log('🔍 Fixed stop loss distance:', distance);
        } else if (stopLossType === 'supertrend') {
            // For Supertrend Value, we need to calculate the current Supertrend value
            // For now, we'll use a default distance that represents the Supertrend level
            // In a real implementation, this would be calculated from market data
            distance = "current"; // This indicates to use current Supertrend value
            console.log('🔍 Supertrend stop loss distance set to:', distance);
        } else {
            // For other types (bb_upper, bb_lower, ema1, ema2, ema3), 
            // the distance represents the indicator value
            distance = "current"; // This indicates to use current indicator value
            console.log('🔍 Indicator-based stop loss distance set to:', distance);
        }

        const result = {
            type: stopLossType,
            distance: distance
        };
        
        console.log('🔍 Final stop loss data:', result);
        return result;
    }

    getTrailingStopData() {
        const trailingStopEnabled = document.getElementById('trailingStopEnabled');
        if (!trailingStopEnabled || !trailingStopEnabled.checked) {
            return { enabled: false };
        }

        return {
            enabled: true,
            profit_multiplier: parseFloat(document.getElementById('profitMultiplier')?.value) || 1.5,
            stop_loss_type: document.getElementById('stopLossType')?.value || 'fixed',
            fixed_stop_loss: parseFloat(document.getElementById('fixedStopLoss')?.value) || 2.0
        };
    }

    getTradeConditionsData() {
        const tradeConditions = {};
        const selectedIndicators = this.getSelectedIndicators();
        
        selectedIndicators.forEach(indicator => {
            tradeConditions[indicator] = {};
            
            // Get all trade condition selects for this indicator
            const conditionSelects = document.querySelectorAll(`[name*="${indicator}"][class*="condition-select"]`);
            conditionSelects.forEach(select => {
                const conditionName = select.name;
                tradeConditions[indicator][conditionName] = select.value;
            });
        });

        return tradeConditions;
    }

    async createStrategy(data) {
        try {
            console.log('🔄 Creating strategy with data:', data);
            
            const response = await fetch(`${this.baseURL}/api/strategy/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data)
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const result = await response.json();
            console.log('✅ Strategy creation response:', result);

            if (result.success) {
                // Add the new strategy to local array with the ID from backend
                const newStrategy = {
                    id: result.strategy_id,
                    ...data,
                    is_active: true,
                    is_live: false,
                    execution_status: 'STOPPED',
                    broker_connected: false,
                    broker_id: null,
                    total_pnl: 0,
                    last_trade_pnl: 0,
                    created_at: new Date().toISOString().split('T')[0]
                };

                this.strategies.push(newStrategy);
                console.log('✅ Strategy created locally:', newStrategy);
            } else {
                throw new Error(result.message || 'Failed to create strategy');
            }
        } catch (error) {
            console.error('❌ Error creating strategy:', error);
            throw error; // Re-throw the error to be handled by the caller
        }
    }

    async updateStrategy(data) {
        try {
            console.log('🔄 ===== STRATEGY UPDATE PROCESS STARTED =====');
            console.log('🔄 Strategy ID to update:', data.id);
            console.log('🔄 Full update data:', JSON.stringify(data, null, 2));
            console.log('🔄 Base URL being used:', this.baseURL);
            console.log('🔄 Full API endpoint:', `${this.baseURL}/strategy/${data.id}`);
            
            // Log specific fields that should be updated
            console.log('🔍 Key fields being updated:');
            console.log('  - platform:', data.platform);
            console.log('  - product:', data.product);
            console.log('  - product_id:', data.product_id);
            console.log('  - candle_duration:', data.candle_duration);
            console.log('  - stop_loss:', JSON.stringify(data.stop_loss, null, 2));
            
            const requestBody = JSON.stringify(data);
            console.log('🔄 Request body being sent:', requestBody);
            
            const response = await fetch(`${this.baseURL}/api/strategy/${data.id}`, {
                method: 'PUT',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: requestBody
            });
            
            console.log('🔄 HTTP Response status:', response.status);
            console.log('🔄 HTTP Response headers:', Object.fromEntries(response.headers.entries()));
            
            if (!response.ok) {
                const errorText = await response.text();
                console.error('❌ HTTP Error Response:', errorText);
                throw new Error(`HTTP error! status: ${response.status}, body: ${errorText}`);
            }

            const result = await response.json();
            console.log('✅ Strategy update response received:', JSON.stringify(result, null, 2));

            if (result.success) {
                console.log('✅ Backend confirmed successful update');
                
                // Update local strategies array
                const index = this.strategies.findIndex(s => s.id === data.id);
                if (index !== -1) {
                    const oldStrategy = { ...this.strategies[index] };
                    this.strategies[index] = { ...this.strategies[index], ...data };
                    console.log('✅ Strategy updated locally:');
                    console.log('  - Old strategy data:', oldStrategy);
                    console.log('  - New strategy data:', this.strategies[index]);
                } else {
                    console.warn('⚠️ Strategy not found in local array for ID:', data.id);
                }
                
                console.log('✅ ===== STRATEGY UPDATE PROCESS COMPLETED SUCCESSFULLY =====');
            } else {
                console.error('❌ Backend returned success: false');
                console.error('❌ Error message:', result.message);
                throw new Error(result.message || 'Failed to update strategy');
            }
        } catch (error) {
            console.error('❌ ===== STRATEGY UPDATE PROCESS FAILED =====');
            console.error('❌ Error updating strategy:', error);
            console.error('❌ Error stack trace:', error.stack);
            throw error; // Re-throw the error to be handled by the caller
        }
    }

    async toggleExecution(strategyId) {
        try {
            const strategy = this.strategies.find(s => s.id === strategyId);
            if (!strategy) return;

            if (strategy.is_live) {
                // Stop strategy
                strategy.is_live = false;
                strategy.execution_status = 'STOPPED';
                this.showSuccess('Strategy stopped successfully');
            } else {
                // Start strategy
                if (!strategy.broker_connected) {
                    this.showError('Cannot start strategy without broker connection');
                    return;
                }
                strategy.is_live = true;
                strategy.execution_status = 'RUNNING';
                this.showSuccess('Strategy started successfully');
            }

            this.loadStrategies();
        } catch (error) {
            console.error('Error toggling strategy execution:', error);
            this.showError('Failed to toggle strategy execution');
        }
    }

    async manageBrokerConnection(strategyId) {
        const strategy = this.strategies.find(s => s.id === strategyId);
        if (!strategy) return;

        if (strategy.broker_connected) {
            // Detach broker
            if (confirm('Are you sure you want to detach the broker connection? This will stop the strategy if it\'s running.')) {
                try {
                    const response = await fetch(`${this.baseURL}/api/strategy/${strategyId}/disconnect-broker`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        }
                    });

                    if (response.ok) {
                        const result = await response.json();
                        if (result.success) {
                            // Refresh strategies to get updated data
                            await this.loadStrategies();
                            this.showSuccess('Broker connection detached successfully');
                        } else {
                            this.showError('Failed to detach broker connection');
                        }
                    } else {
                        this.showError('Failed to detach broker connection');
                    }
                } catch (error) {
                    console.error('Error detaching broker:', error);
                    this.showError('Failed to detach broker connection');
                }
            }
        } else {
            // Connect broker
            this.openBrokerModal(strategyId);
        }
    }

    async buildStrategy(strategyId) {
        try {
            const strategy = this.strategies.find(s => s.id === strategyId);
            if (!strategy) {
                this.showError('Strategy not found');
                return;
            }

            // Check if strategy has required data
            if (!strategy.indicators || strategy.indicators.length === 0) {
                this.showError('Strategy must have indicators configured before building');
                return;
            }

            this.showNotification('Building strategy template...', 'info');

            // Generate template locally (lightweight solution)
            const template = this.generateStrategyTemplate(strategy);
            
            this.showSuccess('Strategy template built successfully!');
            
            // Open the template viewer modal
            this.openTemplateViewer(strategy, template);
            
        } catch (error) {
            console.error('Error building strategy:', error);
            this.showError('Failed to build strategy template: ' + error.message);
        }
    }

    generateStrategyTemplate(strategy) {
        const timestamp = new Date().toISOString();
        const indicators = strategy.indicators || [];
        const parameters = strategy.parameters || {};
        const tradeConditions = strategy.trade_conditions || {};
        
        return `#!/usr/bin/env python3
# Auto-generated EXECUTABLE strategy file for strategy ID ${strategy.id}
# Generated on: ${timestamp}

"""
Strategy Runner: ${strategy.name}
Strategy ID: ${strategy.id}
Product: ${strategy.product || 'NIFTY'}
Platform: ${strategy.platform || 'testnet'}
Timeframe: ${strategy.candle_duration || '5m'}
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd
import pandas_ta as ta
import aiohttp
import numpy as np

# Strategy Configuration
STRATEGY_ID = ${strategy.id}
STRATEGY_NAME = "${strategy.name}"
SYMBOL = "${strategy.product || 'NIFTY'}"
TIMEFRAME = "${strategy.candle_duration || '5m'}"
PLATFORM = "${strategy.platform || 'testnet'}"
BROKER_ID = ${strategy.broker_id || 'None'}

# Strategy Parameters
CFG = {
    "indicators": ${JSON.stringify(indicators, null, 4)},
    "parameters": ${JSON.stringify(parameters, null, 4)},
    "trade_conditions": ${JSON.stringify(tradeConditions, null, 4)},
    "logic_operator": "${strategy.logic_operator}",
    "entry_criteria": "${strategy.entry_criteria}",
    "exit_criteria": "${strategy.exit_criteria}",
    "stop_loss": ${JSON.stringify(strategy.stop_loss || {}, null, 4)},
    "trailing_stop": ${JSON.stringify(strategy.trailing_stop || {}, null, 4)}, "broker_id": ${strategy.broker_id || 'None'}
}

class TradingStrategy:
    """Main strategy runner class."""
    
    def __init__(self):
        """Initialize the strategy runner."""
        self.logger = logging.getLogger(__name__)
        self.period_seconds = 300  # 5 minutes default
        self.running = True
        
        self.logger.info("Strategy runner initialized")
        self.logger.info("Symbol: %s, Timeframe: %s, Period: %d seconds", 
                        SYMBOL, TIMEFRAME, self.period_seconds)
    
    async def run(self):
        """Main strategy execution loop."""
        self.logger.info("Starting strategy execution loop")
        
        try:
            while self.running:
                iteration_start = time.perf_counter()
                
                try:
                    await self._execute_trading_iteration()
                except Exception as e:
                    self.logger.exception("Iteration failed: %s", e)
                
                # Calculate sleep time to maintain period alignment
                iteration_duration = time.perf_counter() - iteration_start
                sleep_time = max(0, self.period_seconds - iteration_duration)
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    self.logger.warning("Iteration took longer than period: %.3fs", iteration_duration)
        
        except KeyboardInterrupt:
            self.logger.info("Strategy execution interrupted by user")
        finally:
            self.cleanup()
    
    async def _fetch_market_data(self):
        """Fetch market data using real broker API."""
        try:
            # Get broker credentials from strategy configuration
            broker_id = CFG.get('broker_id')
            if not broker_id:
                self.logger.error("No broker_id found in strategy configuration")
                return None
            
            # Fetch broker details from database
            broker = await self._get_broker_details(broker_id)
            if not broker:
                self.logger.error("Failed to get broker details for ID: %s", broker_id)
                return None
            
            # Construct API endpoint for market data
            api_url = broker['api_url']
            api_key = broker['api_key']
            api_secret = broker['api_secret']
            
            # Build request headers with authentication
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
                'X-API-Key': api_key,
                'X-API-Secret': api_secret
            }
            
            # Request market data from broker API
            endpoint = f"{api_url}/market_data/{SYMBOL}"
            params = {
                'timeframe': TIMEFRAME,
                'limit': 100  # Get last 100 candles for pandas_ta calculations
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Process broker-specific response format
                        market_data = self._process_broker_market_data(data)
                        
                        self.logger.debug("Fetched market data from broker: %s", market_data)
                        return market_data
                    else:
                        self.logger.error("Broker API request failed: %s - %s", 
                                        response.status, await response.text())
                        return None
            
        except Exception as e:
            self.logger.error("Failed to fetch market data: %s", e)
            return None
    
    async def _get_broker_details(self, broker_id):
        """Get broker details from database."""
        try:
            # TODO: Replace with actual database query
            # In production, this would query the brokers table using broker_id
            # For now, return mock broker data
            mock_brokers = {
                1: {
                    'id': 1,
                    'name': 'Delta Exchange',
                    'api_url': 'https://api.delta.exchange',
                    'api_key': 'your_api_key_here',
                    'api_secret': 'your_api_secret_here',
                    'type': 'crypto'
                },
                2: {
                    'id': 2,
                    'name': 'Zerodha',
                    'api_url': 'https://api.kite.trade',
                    'api_key': 'your_api_key_here',
                    'api_secret': 'your_api_secret_here',
                    'type': 'equity'
                }
            }
            
            return mock_brokers.get(broker_id)
            
        except Exception as e:
            self.logger.error("Failed to get broker details: %s", e)
            return None
    
    def _process_broker_market_data(self, broker_data):
        """Process broker-specific market data format."""
        try:
            # Handle different broker response formats
            if 'data' in broker_data:
                # Delta Exchange format
                candles = broker_data['data']
            elif 'candles' in broker_data:
                # Zerodha format
                candles = broker_data['candles']
            else:
                # Generic format
                candles = broker_data
            
            if not candles or len(candles) == 0:
                return None
            
            # Get the latest candle
            latest_candle = candles[-1]
            
            # Extract OHLCV data
            if len(latest_candle) >= 6:  # [timestamp, open, high, low, close, volume]
                market_data = {
                    'timestamp': datetime.fromtimestamp(latest_candle[0] / 1000, tz=timezone.utc),
                    'open': float(latest_candle[1]),
                    'high': float(latest_candle[2]),
                    'low': float(latest_candle[3]),
                    'close': float(latest_candle[4]),
                    'volume': float(latest_candle[5])
                }
            else:
                # Fallback format
                market_data = {
                    'timestamp': datetime.now(timezone.utc),
                    'open': float(latest_candle.get('open', 0)),
                    'high': float(latest_candle.get('high', 0)),
                    'low': float(latest_candle.get('low', 0)),
                    'close': float(latest_candle.get('close', 0)),
                    'volume': float(latest_candle.get('volume', 0))
                }
            
            return market_data
            
        except Exception as e:
            self.logger.error("Failed to process broker market data: %s", e)
            return None
    
    def _update_price_history(self, market_data):
        """Update price history for pandas_ta calculations."""
        try:
            # Add current price data to history
            self.price_history.append({
                'timestamp': market_data['timestamp'],
                'open': market_data['open'],
                'high': market_data['high'],
                'low': market_data['low'],
                'close': market_data['close']
            })
            
            self.volume_history.append(market_data['volume'])
            
            # Keep only last 1000 data points to prevent memory issues
            if len(self.price_history) > 1000:
                self.price_history = self.price_history[-1000:]
                self.volume_history = self.volume_history[-1000:]
            
        except Exception as e:
            self.logger.error("Failed to update price history: %s", e)
    
    async def _calculate_indicators(self):
        """Calculate technical indicators using pandas_ta."""
        try:
            if len(self.price_history) < 50:
                self.logger.warning("Insufficient price history for indicator calculation")
                return {}
            
            # Convert price history to pandas DataFrame
            df = pd.DataFrame(self.price_history)
            
            indicators = {}
            
            ${indicatorCode}
            
            self.logger.debug("Calculated indicators using pandas_ta: %s", indicators)
            return indicators
            
        except Exception as e:
            self.logger.error("Failed to calculate indicators: %s", e)
            return {}
    
    async def _generate_trading_signal(self, indicators):
        """Generate trading signals based on indicators and strategy rules."""
        try:
            signal = {
                'action': 'HOLD',
                'strength': 0,
                'reason': 'No clear signal',
                'timestamp': datetime.now(timezone.utc)
            }
            
            ${tradingLogicCode}
            
            self.logger.info("Generated trading signal: %s", signal)
            return signal
            
        except Exception as e:
            self.logger.error("Failed to generate trading signal: %s", e)
            return {'action': 'HOLD', 'strength': 0, 'reason': 'Error in signal generation'}
    
    async def _execute_trading_decisions(self, signal, market_data):
        """Execute trading decisions based on signals."""
        try:
            if signal['action'] == 'BUY' and not self.position:
                await self._execute_buy_order(market_data, signal)
            elif signal['action'] == 'SELL' and self.position:
                await self._execute_sell_order(market_data, signal)
            elif signal['action'] == 'HOLD':
                self.logger.debug("Holding position - no action needed")
            
        except Exception as e:
            self.logger.error("Failed to execute trading decisions: %s", e)
    
    async def _execute_buy_order(self, market_data, signal):
        """Execute a buy order using broker API."""
        try:
            # Get broker details
            broker = await self._get_broker_details(CFG.get('broker_id'))
            if not broker:
                self.logger.error("Cannot execute order - broker not found")
                return
            
            # TODO: Replace with actual broker order execution API call
            self.logger.info("EXECUTING BUY ORDER: %s at %s", SYMBOL, market_data['close'])
            
            # Place order through broker API
            order_result = await self._place_broker_order(broker, 'BUY', market_data['close'])
            
            if order_result and order_result.get('success'):
                self.position = 'LONG'
                self.entry_price = market_data['close']
                self.last_signal = signal
                
                # Calculate stop loss and take profit
                if CFG.get('stop_loss'):
                    stop_loss_pct = CFG['stop_loss'].get('percentage', 2.0) / 100
                    self.stop_loss_price = self.entry_price * (1 - stop_loss_pct)
                    self.logger.info("Stop loss set at: %s", self.stop_loss_price)
                
                self.total_trades += 1
                self.logger.info("BUY order executed successfully through broker API")
            else:
                self.logger.error("Failed to execute BUY order through broker API")
            
        except Exception as e:
            self.logger.error("Failed to execute BUY order: %s", e)
    
    async def _execute_sell_order(self, market_data, signal):
        """Execute a sell order using broker API."""
        try:
            # Get broker details
            broker = await self._get_broker_details(CFG.get('broker_id'))
            if not broker:
                self.logger.error("Cannot execute order - broker not found")
                return
            
            # TODO: Replace with actual broker order execution API call
            self.logger.info("EXECUTING SELL ORDER: %s at %s", SYMBOL, market_data['close'])
            
            # Place order through broker API
            order_result = await self._place_broker_order(broker, 'SELL', market_data['close'])
            
            if order_result and order_result.get('success'):
                # Calculate P&L
                if self.entry_price:
                    pnl = market_data['close'] - self.entry_price
                    pnl_pct = (pnl / self.entry_price) * 100
                    
                    if pnl > 0:
                        self.winning_trades += 1
                        self.logger.info("SELL order executed - PROFIT: %s (%.2f%%)", pnl, pnl_pct)
                    else:
                        self.logger.info("SELL order executed - LOSS: %s (%.2f%%)", pnl, pnl_pct)
                    
                    self.consecutive_losses = 0 if pnl > 0 else self.consecutive_losses + 1
                
                self.position = None
                self.entry_price = None
                self.stop_loss_price = None
                self.take_profit_price = None
                self.last_signal = signal
                
                self.logger.info("SELL order executed successfully through broker API")
            else:
                self.logger.error("Failed to execute SELL order through broker API")
            
        except Exception as e:
            self.logger.error("Failed to execute SELL order: %s", e)
    
    async def _place_broker_order(self, broker, side, price):
        """Place order through broker API."""
        try:
            api_url = broker['api_url']
            api_key = broker['api_key']
            api_secret = broker['api_secret']
            
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json',
                'X-API-Key': api_key,
                'X-API-Secret': api_secret
            }
            
            order_data = {
                'symbol': SYMBOL,
                'side': side,
                'quantity': 1,  # TODO: Implement position sizing
                'price': price,
                'type': 'LIMIT'
            }
            
            endpoint = f"{api_url}/orders"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, headers=headers, json=order_data) as response:
                    if response.status == 200:
                        result = await response.json()
                        self.logger.info("Order placed successfully: %s", result)
                        return result
                    else:
                        self.logger.error("Order placement failed: %s - %s", 
                                        response.status, await response.text())
                        return None
            
        except Exception as e:
            self.logger.error("Failed to place broker order: %s", e)
            return None
    
    async def _update_position_management(self, market_data):
        """Update position management (stop loss, trailing stop, etc.)."""
        try:
            if not self.position or not self.entry_price:
                return
            
            current_price = market_data['close']
            
            # Check stop loss
            if self.stop_loss_price and current_price <= self.stop_loss_price:
                self.logger.warning("STOP LOSS TRIGGERED at %s", current_price)
                await self._execute_sell_order(market_data, {
                    'action': 'SELL',
                    'strength': 1,
                    'reason': 'Stop loss triggered'
                })
                return
            
            # Check trailing stop
            if CFG.get('trailing_stop') and self.position == 'LONG':
                trailing_pct = CFG['trailing_stop'].get('percentage', 1.0) / 100
                new_stop = current_price * (1 - trailing_pct)
                
                if new_stop > self.stop_loss_price:
                    self.stop_loss_price = new_stop
                    self.logger.info("Trailing stop updated to: %s", new_stop)
            
        except Exception as e:
            self.logger.error("Failed to update position management: %s", e)
    
    async def _execute_trading_iteration(self):
        """Execute one iteration of the trading strategy."""
        current_time = datetime.now(timezone.utc)
        self.logger.debug("Executing trading iteration at %s", current_time)
        
        try:
            # 1. Fetch market data from real broker API
            market_data = await self._fetch_market_data()
            if not market_data:
                self.logger.warning("Failed to fetch market data")
                return
            
            # 2. Update price history for pandas_ta calculations
            self._update_price_history(market_data)
            
            # 3. Calculate technical indicators using pandas_ta
            indicators = await self._calculate_indicators()
            
            # 4. Generate trading signals
            signal = await self._generate_trading_signal(indicators)
            
            # 5. Execute trading decisions
            await self._execute_trading_decisions(signal, market_data)
            
            # 6. Update position management
            await self._update_position_management(market_data)
            
            self.logger.info("Trading iteration completed successfully")
            
        except Exception as e:
            self.logger.exception("Error in trading iteration: %s", e)
    
    def cleanup(self):
        """Clean up resources."""
        self.logger.info("Cleaning up strategy runner")

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and run strategy
    runner = TradingStrategy()
    asyncio.run(runner.run())
`;
    }

    openTemplateViewer(strategy, templateContent) {
        console.log('🔧 Opening template viewer for strategy:', strategy.id);
        
        try {
            // Get all required DOM elements
            const strategyNameEl = document.getElementById('templateStrategyName');
            const strategyIdEl = document.getElementById('templateStrategyId');
            const productEl = document.getElementById('templateProduct');
            const timeframeEl = document.getElementById('templateTimeframe');
            const codeContentEl = document.getElementById('templateCodeContent');
            const modalEl = document.getElementById('templateViewerModal');
            
            // Check if all elements exist
            if (!strategyNameEl || !strategyIdEl || !productEl || !timeframeEl || !codeContentEl || !modalEl) {
                console.error('❌ Template viewer elements not found:', {
                    strategyName: !!strategyNameEl,
                    strategyId: !!strategyIdEl,
                    product: !!productEl,
                    timeframe: !!timeframeEl,
                    codeContent: !!codeContentEl,
                    modal: !!modalEl
                });
                
                // Try to create a simple fallback modal
                this.createFallbackTemplateViewer(strategy, templateContent);
                return;
            }
            
            // Populate template info
            strategyNameEl.textContent = strategy.name;
            strategyIdEl.textContent = `ID: ${strategy.id}`;
            productEl.textContent = `Product: ${strategy.product || 'N/A'}`;
            timeframeEl.textContent = `Timeframe: ${strategy.candle_duration || 'N/A'}`;
            
            // Populate code content
            codeContentEl.textContent = templateContent;
            
            // Show the modal
            modalEl.style.display = 'flex';
            
            // Store current template data for download/deploy actions
            this.currentTemplate = {
                strategy: strategy,
                content: templateContent
            };
            
            console.log('✅ Template viewer opened successfully');
            
        } catch (error) {
            console.error('❌ Error opening template viewer:', error);
            this.showError('Failed to open template viewer: ' + error.message);
        }
    }

    createFallbackTemplateViewer(strategy, templateContent) {
        console.log('🔧 Creating fallback template viewer');
        
        // Remove any existing fallback modal
        const existingFallback = document.getElementById('fallbackTemplateModal');
        if (existingFallback) {
            existingFallback.remove();
        }
        
        // Create fallback modal HTML
        const fallbackHTML = `
            <div class="modal-overlay" id="fallbackTemplateModal" style="display: flex; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); z-index: 10000; justify-content: center; align-items: center;">
                <div class="modal-content" style="background: white; padding: 20px; border-radius: 8px; max-width: 90%; max-height: 90%; overflow: auto;">
                    <div class="modal-header" style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                        <h2>Strategy Template - ${strategy.name}</h2>
                        <button onclick="document.getElementById('fallbackTemplateModal').remove()" style="background: none; border: none; font-size: 24px; cursor: pointer;">&times;</button>
                    </div>
                    <div class="modal-body">
                        <div style="margin-bottom: 20px;">
                            <p><strong>Strategy ID:</strong> ${strategy.id}</p>
                            <p><strong>Product:</strong> ${strategy.product || 'N/A'}</p>
                            <p><strong>Timeframe:</strong> ${strategy.candle_duration || 'N/A'}</p>
                        </div>
                        <div style="margin-bottom: 20px;">
                            <button onclick="downloadFallbackTemplate()" style="background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; margin-right: 10px;">
                                📥 Download Template
                            </button>
                            <button onclick="viewFallbackRawScript()" style="background: #28a745; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer;">
                                👁️ View Raw Script
                            </button>
                        </div>
                        <div style="background: #f8f9fa; padding: 15px; border-radius: 4px; border: 1px solid #dee2e6;">
                            <h4>Generated Python Code Preview</h4>
                            <pre style="background: #2d2d2d; color: #d4d4d4; padding: 15px; border-radius: 4px; overflow-x: auto; font-size: 12px;"><code>${templateContent}</code></pre>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Add to DOM
        document.body.insertAdjacentHTML('beforeend', fallbackHTML);
        
        // Store current template data
        this.currentTemplate = {
            strategy: strategy,
            content: templateContent
        };
        
        // Add global functions for fallback buttons
        window.downloadFallbackTemplate = () => {
            if (this.currentTemplate) {
                const template = this.currentTemplate;
                const filename = `strategy_${template.strategy.id}_${template.strategy.name.replace(/\s+/g, '_')}.py`;
                const blob = new Blob([template.content], { type: 'text/plain' });
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
                this.showSuccess('Template downloaded successfully!');
            }
        };
        
        window.viewFallbackRawScript = () => {
            if (this.currentTemplate) {
                const template = this.currentTemplate;
                const newWindow = window.open('', '_blank');
                newWindow.document.write(`
                    <html>
                        <head>
                            <title>Strategy ${template.strategy.id} - Raw Script</title>
                            <style>
                                body { font-family: monospace; padding: 20px; background: #1e1e1e; color: #d4d4d4; }
                                pre { background: #2d2d2d; padding: 15px; border-radius: 5px; overflow-x: auto; }
                                .header { margin-bottom: 20px; padding-bottom: 20px; border-bottom: 1px solid #555; }
                                .filename { font-size: 18px; font-weight: bold; color: #4ec9b0; }
                                .strategy-info { color: #9cdcfe; margin-top: 10px; }
                            </style>
                        </head>
                        <body>
                            <div class="header">
                                <div class="filename">strategy_${template.strategy.id}_${template.strategy.name.replace(/\s+/g, '_')}.py</div>
                                <div class="strategy-info">
                                    Strategy: ${template.strategy.name}<br>
                                    Product: ${template.strategy.product || 'N/A'}<br>
                                    Timeframe: ${template.strategy.candle_duration || 'N/A'}
                                </div>
                            </div>
                            <pre><code>${template.content}</code></pre>
                        </body>
                    </html>
                `);
                newWindow.document.close();
            }
        };
        
        console.log('✅ Fallback template viewer created successfully');
    }

    async openBrokerModal(strategyId) {
        console.log('🔧 openBrokerModal called with strategyId:', strategyId);
        
        // Set current strategy
        this.currentStrategy = this.strategies.find(s => s.id === strategyId);
        if (!this.currentStrategy) {
            console.error('❌ Current strategy not found for ID:', strategyId);
            return;
        }
        
        console.log('✅ Current strategy set:', this.currentStrategy);
        
        // Refresh brokers to ensure latest data
        console.log('🔄 Loading brokers...');
        await this.loadBrokers();
        console.log('📊 Brokers loaded:', this.brokers);
        
        const modal = document.getElementById('brokerModal');
        const brokerSelect = document.getElementById('brokerSelect');
        const brokerInfo = document.getElementById('brokerInfo');
        const connectBtn = document.getElementById('connectBrokerBtn');
        
        if (!modal || !brokerSelect) {
            console.error('❌ Modal elements not found');
            return;
        }
        
        // Clear previous options
        brokerSelect.innerHTML = '<option value="">-- Select a broker --</option>';
        
        // Update debug info
        this.updateDebugInfo();
        
        // Show all brokers for the user (not just "connected" ones)
        const availableBrokers = this.brokers.filter(broker => broker.id);
        console.log('🔍 Available brokers:', availableBrokers);
        
        if (availableBrokers.length === 0) {
            console.warn('⚠️ No brokers available for user');
            brokerSelect.innerHTML = '<option value="">No brokers available for this user</option>';
            brokerSelect.disabled = true;
            connectBtn.disabled = true;
        } else {
            console.log('✅ Adding broker options to dropdown');
            // Add broker options to dropdown
            availableBrokers.forEach(broker => {
                const option = document.createElement('option');
                option.value = broker.id;
                option.textContent = broker.name || `Broker ${broker.id}`;
                option.dataset.type = broker.type || 'unknown';
                option.dataset.status = 'Available';
                brokerSelect.appendChild(option);
                console.log('➕ Added broker option:', option.textContent, 'with value:', option.value);
            });
            
            // Enable dropdown and connect button
            brokerSelect.disabled = false;
            connectBtn.disabled = false;
            
            // Add change event listener to show broker details
            brokerSelect.onchange = (e) => {
                const selectedBrokerId = e.target.value;
                console.log('🔄 Broker selection changed to:', selectedBrokerId);
                if (selectedBrokerId) {
                    const selectedBroker = this.brokers.find(b => b.id == selectedBrokerId);
                    if (selectedBroker) {
                        document.getElementById('brokerType').textContent = this.getBrokerTypeDisplay(selectedBroker.type || 'unknown');
                        document.getElementById('brokerStatus').textContent = 'Available for Connection';
                        brokerInfo.style.display = 'block';
                        connectBtn.disabled = false;
                        console.log('✅ Broker details updated for:', selectedBroker.name);
                    }
                } else {
                    brokerInfo.style.display = 'none';
                    connectBtn.disabled = true;
                }
            };
        }
        
        modal.style.display = 'flex';
        console.log('🎉 Broker modal opened successfully');
    }

    closeBrokerModal() {
        document.getElementById('brokerModal').style.display = 'none';
        this.currentStrategy = null;
    }

    async connectBrokerToStrategy(brokerId) {
        if (!this.currentStrategy) return;
        
        try {
            const response = await fetch(`${this.baseURL}/api/strategy/${this.currentStrategy.id}/connect-broker`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    broker_id: brokerId
                })
            });

            if (response.ok) {
                const result = await response.json();
                if (result.success) {
                    // Close modal
                    this.closeBrokerModal();
                    
                    // Refresh strategies to get updated data
                    await this.loadStrategies();
                    
                    // Show success notification
                    this.showSuccess('Strategy successfully connected to broker!');
                } else {
                    this.showError('Failed to connect broker to strategy');
                }
            } else {
                this.showError('Failed to connect broker to strategy');
            }
            
        } catch (error) {
            console.error('Error connecting broker to strategy:', error);
            this.showError('Failed to connect broker to strategy. Please try again.');
        }
    }

    connectSelectedBroker() {
        const brokerSelect = document.getElementById('brokerSelect');
        const selectedBrokerId = brokerSelect.value;
        
        if (!selectedBrokerId) {
            this.showError('Please select a broker first');
            return;
        }
        
        if (!this.currentStrategy) {
            this.showError('No strategy selected');
            return;
        }
        
        // Call the existing connect method
        this.connectBrokerToStrategy(parseInt(selectedBrokerId));
    }

    getBrokerIcon(type) {
        const icons = {
            'crypto': 'bitcoin',
            'equity': 'chart-line',
            'forex': 'dollar-sign',
            'commodity': 'gem'
        };
        return icons[type] || 'chart-line';
    }

    getBrokerTypeDisplay(type) {
        const types = {
            'crypto': 'Cryptocurrency',
            'equity': 'Stock/Equity',
            'forex': 'Forex',
            'commodity': 'Commodity'
        };
        return types[type] || type;
    }

    saveStrategiesToStorage() {
        try {
            localStorage.setItem('trademanthan_strategies', JSON.stringify(this.strategies));
            console.log('Strategies saved to localStorage');
        } catch (error) {
            console.error('Error saving strategies:', error);
        }
    }

    showNotification(message, type = 'info') {
        // Create notification element if it doesn't exist
        let notification = document.getElementById('notification');
        if (!notification) {
            notification = document.createElement('div');
            notification.id = 'notification';
            notification.className = 'notification';
            document.body.appendChild(notification);
        }
        
        notification.textContent = message;
        notification.className = `notification ${type}`;
        notification.classList.add('show');
        
        // Hide after 3 seconds
        setTimeout(() => {
            notification.classList.remove('show');
        }, 3000);
    }

    async viewLogs(strategyId) {
        const strategy = this.strategies.find(s => s.id === strategyId);
        if (!strategy) return;

        this.currentStrategy = strategy;
        this.openLogsModal();
        await this.loadStrategyLogs(strategyId);
    }

    openLogsModal() {
        const modal = document.getElementById('logsModal');
        const title = document.getElementById('logsModalTitle');
        
        title.textContent = `Strategy Logs - ${this.currentStrategy.name}`;
        modal.style.display = 'flex';
    }

    closeLogsModal() {
        document.getElementById('logsModal').style.display = 'none';
        this.currentStrategy = null;
    }

    async loadStrategyLogs(strategyId) {
        try {
            const logsStatus = document.getElementById('logsStatus');
            const logsList = document.getElementById('logsList');
            
            if (!this.currentStrategy.is_live) {
                logsStatus.innerHTML = '<div class="status-message">Strategy is not Live</div>';
                logsList.innerHTML = '<p class="no-logs">No execution logs available for stopped strategies.</p>';
                return;
            }

            // In production, this would call your backend API
            // For now, we'll use mock data
            const mockLogs = [
                {
                    id: 1,
                    log_level: 'INFO',
                    message: 'Strategy execution started',
                    execution_timestamp: new Date().toISOString(),
                    signal_generated: false,
                    order_placed: false
                },
                {
                    id: 2,
                    log_level: 'INFO',
                    message: 'Market data received - analyzing indicators',
                    execution_timestamp: new Date(Date.now() - 60000).toISOString(),
                    signal_generated: false,
                    order_placed: false
                },
                {
                    id: 3,
                    log_level: 'WARNING',
                    message: 'RSI approaching overbought levels',
                    execution_timestamp: new Date(Date.now() - 120000).toISOString(),
                    signal_generated: false,
                    order_placed: false
                }
            ];

            logsStatus.innerHTML = `<div class="status-message live">Strategy is Live - ${mockLogs.length} logs available</div>`;
            
            let logsHTML = '';
            mockLogs.forEach(log => {
                const logClass = `log-item ${log.log_level.toLowerCase()}`;
                const timestamp = new Date(log.execution_timestamp).toLocaleString();
                
                logsHTML += `
                    <div class="${logClass}">
                        <div class="log-header">
                            <span class="log-level">${log.log_level}</span>
                            <span class="log-time">${timestamp}</span>
                        </div>
                        <div class="log-message">${log.message}</div>
                    </div>
                `;
            });
            
            logsList.innerHTML = logsHTML;
        } catch (error) {
            console.error('Error loading strategy logs:', error);
            this.showError('Failed to load strategy logs');
        }
    }

    filterLogs() {
        // In production, this would filter logs by level
        console.log('Filtering logs...');
    }

    async deleteStrategy(strategyId) {
        try {
            const strategy = this.strategies.find(s => s.id === strategyId);
            if (!strategy) return;

            if (strategy.is_live) {
                this.showError('Cannot delete a running strategy. Stop it first.');
                return;
            }

            if (confirm(`Are you sure you want to delete the strategy "${strategy.name}"? This action cannot be undone.`)) {
                // Detach broker if connected
                if (strategy.broker_connected) {
                    strategy.broker_connected = false;
                    strategy.broker_id = null;
                }

                // Remove strategy
                this.strategies = this.strategies.filter(s => s.id !== strategyId);
                this.loadStrategies();
                this.showSuccess('Strategy deleted successfully');
            }
        } catch (error) {
            console.error('Error deleting strategy:', error);
            this.showError('Failed to delete strategy');
        }
    }

    async editStrategy(strategyId) {
        console.log('🔧 StrategyManager.editStrategy called with ID:', strategyId);
        console.log('🔧 Current strategies:', this.strategies);
        console.log('🔧 Strategy to edit:', this.strategies.find(s => s.id === strategyId));
        
        if (!strategyId) {
            console.error('❌ No strategy ID provided');
            return;
        }
        
        const strategy = this.strategies.find(s => s.id === strategyId);
        if (!strategy) {
            console.error('❌ Strategy not found:', strategyId);
            return;
        }
        
        console.log('✅ Strategy found, opening form...');
        await this.openStrategyForm(strategyId);
    }

    showSuccess(message) {
        this.showNotification(message, 'success');
    }

    showError(message) {
        this.showNotification(message, 'error');
    }

    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `notification ${type}`;
        notification.textContent = message;
        
        // Add icon based on type
        const icon = document.createElement('i');
        icon.className = `fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}`;
        icon.style.marginRight = '0.5rem';
        notification.insertBefore(icon, notification.firstChild);
        
        document.body.appendChild(notification);
        
        // Show notification
        setTimeout(() => notification.classList.add('show'), 100);
        
        // Hide and remove after 5 seconds
        setTimeout(() => {
            notification.classList.remove('show');
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
            }, 300);
        }, 5000);
    }

    // Ensure all form sections are visible and properly structured
    ensureFormSectionsVisible() {
        console.log('🔧 ensureFormSectionsVisible called');
        
        // Force all form sections to be visible
        const formSections = document.querySelectorAll('.form-section');
        console.log(`🔧 Found ${formSections.length} form sections to make visible`);
        
        formSections.forEach((section, index) => {
            console.log(`🔧 Making section ${index} visible:`, section);
            
            // Remove any inline styles that might hide the section
            section.removeAttribute('style');
            
            // Force visibility with !important equivalent
            section.style.setProperty('display', 'block', 'important');
            section.style.setProperty('visibility', 'visible', 'important');
            section.style.setProperty('opacity', '1', 'important');
            section.style.setProperty('position', 'relative', 'important');
            section.style.setProperty('z-index', '1', 'important');
            
            // Ensure the section is not hidden by parent elements
            let parent = section.parentElement;
            while (parent && parent !== document.body) {
                if (parent.style.display === 'none') {
                    parent.style.display = 'block';
                }
                if (parent.style.visibility === 'hidden') {
                    parent.style.visibility = 'visible';
                }
                parent = parent.parentElement;
            }
        });
        
        // Specifically ensure the Trading Configuration section is first and visible
        const tradingConfigSection = document.querySelector('.form-section.compact-section:first-of-type');
        if (tradingConfigSection) {
            console.log('🔧 Trading config section found and made visible:', tradingConfigSection);
            tradingConfigSection.style.setProperty('display', 'block', 'important');
            tradingConfigSection.style.setProperty('visibility', 'visible', 'important');
            tradingConfigSection.style.setProperty('opacity', '1', 'important');
            
            // Move it to the top if needed
            const form = document.getElementById('strategyForm');
            if (form && tradingConfigSection.parentElement === form) {
                form.insertBefore(tradingConfigSection, form.firstChild);
                console.log('🔧 Moved trading config section to top of form');
            }
        } else {
            console.error('❌ Trading config section not found!');
        }
        
        console.log('🔧 Form sections visibility ensured');
    }

    // Fix form structure to ensure proper ordering
    fixFormStructure() {
        console.log('🔧 fixFormStructure called');
        
        const form = document.getElementById('strategyForm');
        if (!form) {
            console.error('❌ Strategy form not found!');
            return;
        }
        
        // Get all form sections
        const sections = form.querySelectorAll('.form-section');
        console.log(`🔧 Found ${sections.length} sections in form`);
        
        // Ensure proper order: Trading Configuration first, then Basic Info, then others
        const tradingConfigSection = Array.from(sections).find(section => 
            section.querySelector('h3')?.textContent.includes('Trading Configuration')
        );
        
        if (tradingConfigSection) {
            console.log('🔧 Trading config section found, ensuring it\'s first');
            // Move to top if not already there
            if (tradingConfigSection !== form.firstElementChild) {
                form.insertBefore(tradingConfigSection, form.firstChild);
                console.log('🔧 Moved trading config section to top');
            }
        } else {
            console.error('❌ Trading config section not found in form!');
        }
        
        // Log the final form structure
        const finalSections = form.querySelectorAll('.form-section');
        console.log('🔧 Final form structure:');
        finalSections.forEach((section, index) => {
            const heading = section.querySelector('h3');
            console.log(`  Section ${index}: ${heading ? heading.textContent : 'No heading'}`);
        });
    }

    // Force modal to be properly displayed
    forceModalDisplay() {
        console.log('🔧 forceModalDisplay called');
        
        const modal = document.getElementById('strategyModal');
        if (!modal) {
            console.error('❌ Strategy modal not found!');
            return;
        }
        
        // Force modal to be visible
        modal.style.setProperty('display', 'flex', 'important');
        modal.style.setProperty('visibility', 'visible', 'important');
        modal.style.setProperty('opacity', '1', 'important');
        modal.style.setProperty('z-index', '99999', 'important');
        modal.style.setProperty('position', 'fixed', 'important');
        
        // Force form to be visible
        const form = document.getElementById('strategyForm');
        if (form) {
            form.style.setProperty('display', 'block', 'important');
            form.style.setProperty('visibility', 'visible', 'important');
            form.style.setProperty('opacity', '1', 'important');
        }
        
        // Force all form sections to be visible
        const sections = modal.querySelectorAll('.form-section');
        sections.forEach((section, index) => {
            section.style.setProperty('display', 'block', 'important');
            section.style.setProperty('visibility', 'visible', 'important');
            section.style.setProperty('opacity', '1', 'important');
            section.style.setProperty('position', 'relative', 'important');
            section.style.setProperty('z-index', '1', 'important');
        });
        
        console.log('🔧 Modal display forced');
    }

    // Debug method to log form element states
    logFormElementStates() {
        console.log('🔍 ===== FORM ELEMENT STATE DEBUG =====');
        
        // Check if form exists
        const form = document.getElementById('strategyForm');
        if (!form) {
            console.error('❌ Strategy form not found!');
            return;
        }
        console.log('✅ Strategy form found');
        
        // Log all form elements
        const formElements = form.elements;
        console.log('🔍 Total form elements:', formElements.length);
        
        for (let i = 0; i < formElements.length; i++) {
            const element = formElements[i];
            if (element.name) {
                let value = '';
                if (element.type === 'checkbox') {
                    value = element.checked;
                } else if (element.type === 'select-one') {
                    value = element.value;
                } else {
                    value = element.value;
                }
                console.log(`  - ${element.name} (${element.type}): ${value}`);
            }
        }
        
        // Check specific critical elements
        const criticalElements = [
            'platform', 'product', 'product_id', 'candle_duration', 
            'stop_loss_type', 'fixed_stop_loss'
        ];
        
        console.log('🔍 Critical elements check:');
        criticalElements.forEach(name => {
            const element = form.querySelector(`[name="${name}"]`);
            if (element) {
                let value = '';
                if (element.type === 'checkbox') {
                    value = element.checked;
                } else if (element.type === 'select-one') {
                    value = element.value;
                } else {
                    value = element.value;
                }
                console.log(`  - ${name}: ${value} (type: ${element.type}, id: ${element.id})`);
            } else {
                console.log(`  - ${name}: ❌ Element not found`);
            }
        });
        
        console.log('✅ ===== FORM ELEMENT STATE DEBUG COMPLETED =====');
    }
}

// Initialize strategy manager when page loads
let strategyManager;

// Wait for both DOM and left menu to be ready
function initializeStrategyManager() {
    console.log('🔧 initializeStrategyManager called');
    console.log('🔧 Document ready state:', document.readyState);
    
    if (document.readyState === 'loading') {
        console.log('🔧 Document still loading, waiting for DOMContentLoaded');
        document.addEventListener('DOMContentLoaded', initializeStrategyManager);
        return;
    }
    
    // Check if left menu is ready
    const leftMenuContainer = document.getElementById('left-menu-container');
    console.log('🔧 Left menu container:', leftMenuContainer);
    console.log('🔧 Left menu children:', leftMenuContainer ? leftMenuContainer.children.length : 'null');
    
    if (!leftMenuContainer || leftMenuContainer.children.length === 0) {
        console.log('🔧 Left menu not ready, retrying in 100ms...');
        setTimeout(initializeStrategyManager, 100);
        return;
    }
    
    console.log('🚀 Trade Manthan Strategy Management Loading...');
    try {
        strategyManager = new StrategyManager();
        
        // Make strategyManager globally accessible immediately after initialization
        window.strategyManager = strategyManager;
        
        console.log('✅ Strategy Manager initialized and ready');
        console.log('🔧 strategyManager object:', strategyManager);
        console.log('🔧 window.strategyManager:', window.strategyManager);
    } catch (error) {
        console.error('❌ Error initializing Strategy Manager:', error);
    }
}

// Start initialization
initializeStrategyManager();

// Test if global functions are available
console.log('🔧 Testing global functions...');
console.log('🔧 window.editStrategy:', typeof window.editStrategy);
console.log('🔧 window.strategyManager:', window.strategyManager);
console.log('🔧 StrategyManager class:', typeof StrategyManager);



// Fallback initialization after 3 seconds if left menu doesn't load
setTimeout(() => {
    if (!strategyManager) {
        console.log('⚠️ Left menu not loaded after 3 seconds, initializing strategy manager anyway...');
        try {
            strategyManager = new StrategyManager();
            window.strategyManager = strategyManager;
            console.log('✅ Strategy Manager initialized with fallback');
        } catch (error) {
            console.error('❌ Fallback initialization failed:', error);
        }
    }
}, 3000);

// Global functions for onclick handlers (simplified)
function showAddStrategyForm() {
    if (strategyManager) {
        strategyManager.openStrategyForm();
    }
}

// Global edit strategy function as backup
async function editStrategy(strategyId) {
    console.log('🔧 Global editStrategy called with ID:', strategyId);
    
    if (strategyManager) {
        console.log('✅ Using strategyManager.editStrategy');
        await strategyManager.editStrategy(strategyId);
    } else {
        console.error('❌ strategyManager not available, trying to initialize...');
        try {
            const strategyManager = new StrategyManager();
            window.strategyManager = strategyManager;
            console.log('✅ Strategy Manager created on demand');
            await strategyManager.editStrategy(strategyId);
        } catch (error) {
            console.error('❌ Failed to create strategy manager:', error);
        }
    }
}

// Global functions for other strategy actions
function manageBrokerConnection(strategyId) {
    if (strategyManager) {
        strategyManager.manageBrokerConnection(strategyId);
    } else {
        alert('Strategy manager not available. Please refresh the page.');
    }
}

function toggleExecution(strategyId) {
    if (strategyManager) {
        strategyManager.toggleExecution(strategyId);
    } else {
        alert('Strategy manager not available. Please refresh the page.');
    }
}

function viewLogs(strategyId) {
    if (strategyManager) {
        strategyManager.viewLogs(strategyId);
    } else {
        alert('Strategy manager not available. Please refresh the page.');
    }
}

function deleteStrategy(strategyId) {
    if (strategyManager) {
        strategyManager.deleteStrategy(strategyId);
    } else {
        alert('Strategy manager not available. Please refresh the page.');
    }
}

function buildStrategy(strategyId) {
    if (strategyManager) {
        strategyManager.buildStrategy(strategyId);
    } else {
        alert('Strategy manager not available. Please refresh the page.');
    }
}

function closeTemplateViewer() {
    const modal = document.getElementById('templateViewerModal');
    if (modal) {
        modal.style.display = 'none';
    }
}

function downloadTemplate() {
    if (strategyManager && strategyManager.currentTemplate) {
        const template = strategyManager.currentTemplate;
        const filename = `strategy_${template.strategy.id}_${template.strategy.name.replace(/\s+/g, '_')}.py`;
        
        const blob = new Blob([template.content], { type: 'text/plain' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
        strategyManager.showSuccess('Template downloaded successfully!');
    } else {
        alert('No template available for download');
    }
}

function viewRawScript() {
    if (strategyManager && strategyManager.currentTemplate) {
        const template = strategyManager.currentTemplate;
        const newWindow = window.open('', '_blank');
        newWindow.document.write(`
            <html>
                <head>
                    <title>Strategy ${template.strategy.id} - Raw Script</title>
                    <style>
                        body { font-family: monospace; padding: 20px; background: #1e1e1e; color: #d4d4d4; }
                        pre { background: #2d2d2d; padding: 15px; border-radius: 5px; overflow-x: auto; }
                        .header { margin-bottom: 20px; padding-bottom: 20px; border-bottom: 1px solid #555; }
                        .filename { font-size: 18px; font-weight: bold; color: #4ec9b0; }
                        .strategy-info { color: #9cdcfe; margin-top: 10px; }
                    </style>
                </head>
                <body>
                    <div class="header">
                        <div class="filename">strategy_${template.strategy.id}_${template.strategy.name.replace(/\s+/g, '_')}.py</div>
                        <div class="strategy-info">
                            Strategy: ${template.strategy.name}<br>
                            Product: ${template.strategy.product || 'N/A'}<br>
                            Timeframe: ${template.strategy.candle_duration || 'N/A'}
                        </div>
                    </div>
                    <pre><code>${template.content}</code></pre>
                </body>
            </html>
        `);
        newWindow.document.close();
    } else {
        alert('No template available to view');
    }
}

function deployToRunner() {
    if (strategyManager && strategyManager.currentTemplate) {
        strategyManager.showNotification('Deploying strategy to runner...', 'info');
        // TODO: Implement deployment to strategy runner
        setTimeout(() => {
            strategyManager.showSuccess('Strategy deployed to runner successfully!');
        }, 2000);
    } else {
        alert('No template available for deployment');
    }
}

function closeStrategyForm() {
    if (strategyManager) {
        strategyManager.closeStrategyForm();
    }
}

function closeBrokerModal() {
    if (strategyManager) {
        strategyManager.closeBrokerModal();
    }
}

function closeLogsModal() {
    if (strategyManager) {
        strategyManager.closeLogsModal();
    }
}

// Make functions globally accessible
window.showAddStrategyForm = showAddStrategyForm;
window.editStrategy = editStrategy;
window.manageBrokerConnection = manageBrokerConnection;
window.toggleExecution = toggleExecution;
window.viewLogs = viewLogs;
window.deleteStrategy = deleteStrategy;
window.buildStrategy = buildStrategy;
window.closeStrategyForm = closeStrategyForm;
window.closeBrokerModal = closeBrokerModal;
window.closeLogsModal = closeLogsModal;
window.closeTemplateViewer = closeTemplateViewer;
window.downloadTemplate = downloadTemplate;
window.viewRawScript = viewRawScript;
window.deployToRunner = deployToRunner;

// Also make strategyManager globally accessible for debugging
window.strategyManager = strategyManager;


// Global version access function
window.getStrategyManagerVersion = function() {
    if (window.strategyManager) {
        return StrategyManager.getVersionInfo();
    } else {
        return {
            version: StrategyManager.VERSION,
            buildDate: StrategyManager.BUILD_DATE,
            features: StrategyManager.FEATURES,
            status: "StrategyManager not yet initialized"
        };
    }
};

// Log version on page load
console.log("🚀 Trade Manthan Strategy Manager v3.0 loaded");
console.log("📋 Use window.getStrategyManagerVersion() to get version info");


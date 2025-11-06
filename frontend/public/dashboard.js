// Dashboard functionality
class Dashboard {
    constructor() {
        this.currentSection = 'dashboard';
        this.userData = null;
        this.brokers = [];
        this.strategies = [];
        this.cryptoPrices = [];

        this.init();
    }

    init() {
        // Wait for left menu to load and authenticate before proceeding
        this.waitForLeftMenu().then(() => {
            console.log('Dashboard: Left menu ready, initializing dashboard functionality...');
            this.setupNavigation();
            this.loadUserData();
            this.loadDashboardData();
            this.setupEventListeners();
            this.setupMobileMenu();
        });
    }

    waitForLeftMenu() {
        return new Promise((resolve) => {
            const checkLeftMenu = () => {
                const leftPanel = document.querySelector('.left-panel');
                const userAvatar = document.getElementById('userAvatar');
                
                // Check if both the left panel exists AND user data is loaded (indicating auth is complete)
                if (leftPanel && userAvatar) {
                    console.log('Dashboard: Left panel and user data ready, proceeding with initialization');
                    resolve();
                } else {
                    console.log('Dashboard: Waiting for left menu to complete authentication...');
                    setTimeout(checkLeftMenu, 100);
                }
            };
            checkLeftMenu();
        });
    }

    updateUserInfo() {
        if (this.userData) {
            document.getElementById('userName').textContent = this.userData.name;
            document.getElementById('userEmail').textContent = this.userData.email;
            if (this.userData.picture) {
                document.getElementById('userAvatar').src = this.userData.picture;
            }
        }
    }

    setupNavigation() {
        const navItems = document.querySelectorAll('.nav-item');
        navItems.forEach(item => {
            item.addEventListener('click', () => {
                const section = item.dataset.section;
                this.showSection(section);

                // Update active state
                navItems.forEach(nav => nav.classList.remove('active'));
                item.classList.add('active');
            });
        });
    }

    showSection(sectionName) {
        // Hide all sections
        const sections = document.querySelectorAll('.content-section');
        sections.forEach(section => section.classList.remove('active'));

        // Show selected section
        const targetSection = document.getElementById(sectionName);
        if (targetSection) {
            targetSection.classList.add('active');
            this.currentSection = sectionName;

            // Load section-specific data
            this.loadSectionData(sectionName);
        }
    }

    loadSectionData(sectionName) {
        switch (sectionName) {
            case 'dashboard':
                this.loadDashboardData();
                break;
            case 'broker':
                this.loadBrokerData();
                break;
            case 'strategy':
                this.loadStrategyData();
                break;
        }
    }

    async loadDashboardData() {
        // Load crypto prices
        await this.loadCryptoPrices();

        // Load broker summary
        this.loadBrokerSummary();

        // Load strategy summary
        this.loadStrategySummary();
    }

    async loadCryptoPrices() {
        try {
            // In production, this would call your backend API
            // For now, we'll use mock data
            this.cryptoPrices = [
                { symbol: 'BTCUSD', name: 'Bitcoin', price: 45000, icon: '₿' },
                { symbol: 'ETHUSD', name: 'Ethereum', price: 3200, icon: 'Ξ' },
                { symbol: 'XRPUSD', name: 'Ripple', price: 0.85, icon: 'XRP' },
                { symbol: 'SOLUSD', name: 'Solana', price: 95, icon: '◎' }
            ];

            this.renderCryptoPrices();
        } catch (error) {
            console.error('Error loading crypto prices:', error);
        }
    }

    renderCryptoPrices() {
        const cryptoGrid = document.getElementById('cryptoGrid');
        if (!cryptoGrid) return;

        cryptoGrid.innerHTML = this.cryptoPrices.map(crypto => `
            <div class="crypto-item">
                <div class="crypto-icon">${crypto.icon}</div>
                <div class="crypto-name">${crypto.name}</div>
                <div class="crypto-price">$${crypto.price.toLocaleString()}</div>
            </div>
        `).join('');
    }

    loadBrokerSummary() {
        const brokerCardsContainer = document.getElementById('brokerCardsContainer');
        if (!brokerCardsContainer) return;

        if (this.brokers.length === 0) {
            brokerCardsContainer.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-university"></i>
                    <h4>No Brokers Connected</h4>
                    <p>Connect your first broker to start trading</p>
                    <button class="btn btn-primary" onclick="showAddBrokerForm()">
                        <i class="fas fa-plus"></i> Add Broker
                    </button>
                </div>
            `;
        } else {
            brokerCardsContainer.innerHTML = this.brokers.map(broker => `
                <div class="broker-mini-card" onclick="dashboard.openBrokerManagement(${broker.id})">
                    <div class="broker-mini-card-header">
                        <h4 class="broker-mini-card-name">${broker.name}</h4>
                        <span class="broker-mini-card-status ${broker.is_connected ? 'connected' : 'disconnected'}">
                            ${broker.is_connected ? 'Connected' : 'Disconnected'}
                        </span>
                    </div>
                    <div class="broker-mini-card-balance">
                        $${(broker.wallet_balance || 0).toLocaleString()}
                    </div>
                    <div class="broker-mini-card-balance-label">Wallet Balance</div>
                    <div class="broker-mini-card-footer">
                        <span class="broker-mini-card-type">${broker.type || 'Unknown'}</span>
                        <span class="broker-mini-card-last-sync">
                            ${broker.last_connection ? new Date(broker.last_connection).toLocaleDateString() : 'Never'}
                        </span>
                    </div>
                </div>
            `).join('');
        }
    }

    loadStrategySummary() {
        const strategyCardsContainer = document.getElementById('strategyCardsContainer');
        if (!strategyCardsContainer) return;

        if (this.strategies.length === 0) {
            strategyCardsContainer.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-robot"></i>
                    <h4>No Strategies Created</h4>
                    <p>Create your first trading strategy to start automated trading</p>
                    <div class="strategy-actions">
                        <button class="btn btn-primary" onclick="showAddStrategyForm()">
                            <i class="fas fa-plus"></i> Create Strategy
                        </button>
                        <a href="strategy.html" class="btn btn-secondary">
                            <i class="fas fa-robot"></i> Strategy Management
                        </a>
                    </div>
                </div>
            `;
        } else {
            strategyCardsContainer.innerHTML = this.strategies.map(strategy => `
                <div class="strategy-mini-card" onclick="dashboard.openStrategyManagement(${strategy.id})">
                    <div class="strategy-mini-card-header">
                        <h4 class="strategy-mini-card-name">${strategy.name}</h4>
                        <span class="strategy-mini-card-status ${strategy.is_active ? (strategy.is_live ? 'live' : 'active') : 'inactive'}">
                            ${strategy.is_active ? (strategy.is_live ? 'Live' : 'Active') : 'Inactive'}
                        </span>
                    </div>
                    <div class="strategy-mini-card-pnl ${strategy.total_pnl >= 0 ? 'positive' : 'negative'}">
                        ${strategy.total_pnl >= 0 ? '+' : ''}$${Math.abs(strategy.total_pnl).toFixed(2)}
                    </div>
                    <div class="strategy-mini-card-pnl-label">Total P&L</div>
                    <div class="strategy-mini-card-footer">
                        <span class="strategy-mini-card-broker">
                            ${strategy.broker ? strategy.broker.name : 'No Broker'}
                        </span>
                        <span class="strategy-mini-card-execution">
                            ${strategy.execution_status || 'STOPPED'}
                        </span>
                    </div>
                </div>
            `).join('');
        }
    }

    async loadBrokerData() {
        try {
            // In production, this would call your backend API
            // For now, we'll use mock data
            this.brokers = [
                {
                    id: 1,
                    name: 'Delta Exchange',
                    type: 'Crypto',
                    wallet_balance: 12500.75,
                    is_connected: true,
                    last_connection: new Date().toISOString(),
                    connection_status: 'connected'
                },
                {
                    id: 2,
                    name: 'Zerodha',
                    type: 'Equity',
                    wallet_balance: 8750.50,
                    is_connected: true,
                    last_connection: new Date(Date.now() - 3600000).toISOString(), // 1 hour ago
                    connection_status: 'connected'
                },
                {
                    id: 3,
                    name: 'Angel One',
                    type: 'Equity',
                    wallet_balance: 0,
                    is_connected: false,
                    last_connection: new Date(Date.now() - 86400000).toISOString(), // 1 day ago
                    connection_status: 'disconnected'
                }
            ];

            this.renderBrokerList();
        } catch (error) {
            console.error('Error loading broker data:', error);
        }
    }

    renderBrokerList() {
        const brokerList = document.getElementById('brokerList');
        if (!brokerList) return;

        if (this.brokers.length === 0) {
            brokerList.innerHTML = '<p style="text-align: center; color: #666;">No brokers added yet.</p>';
        } else {
            brokerList.innerHTML = this.brokers.map(broker => `
                <div class="broker-item" style="background: white; padding: 1rem; border-radius: 8px; margin-bottom: 1rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>${broker.name}</strong>
                            <div style="color: #666; font-size: 0.9rem;">
                                Balance: $${broker.wallet_balance.toLocaleString()}
                            </div>
                        </div>
                        <div style="display: flex; gap: 0.5rem;">
                            <span class="status-badge ${broker.is_connected ? 'connected' : 'disconnected'}">
                                ${broker.is_connected ? 'Connected' : 'Disconnected'}
                            </span>
                            <button class="btn btn-secondary" onclick="editBroker(${broker.id})">Edit</button>
                        </div>
                    </div>
                </div>
            `).join('');
        }
    }

    async loadStrategyData() {
        try {
            // In production, this would call your backend API
            // For now, we'll use mock data
            this.strategies = [
                {
                    id: 1,
                    name: 'Supertrend RSI Combo',
                    description: 'Supertrend trend direction with RSI momentum confirmation',
                    total_pnl: 1250.50,
                    is_active: true,
                    is_live: true,
                    execution_status: 'RUNNING',
                    broker: { name: 'Delta Exchange' }
                },
                {
                    id: 2,
                    name: 'BB Squeeze Momentum',
                    description: 'Bollinger Band Squeeze breakout strategy',
                    total_pnl: -320.75,
                    is_active: true,
                    is_live: false,
                    execution_status: 'STOPPED',
                    broker: { name: 'Delta Exchange' }
                },
                {
                    id: 3,
                    name: 'Triple EMA Trend',
                    description: 'Triple EMA crossover trend following strategy',
                    total_pnl: 875.25,
                    is_active: true,
                    is_live: false,
                    execution_status: 'PAUSED',
                    broker: { name: 'Zerodha' }
                },
                {
                    id: 4,
                    name: 'MACD Divergence',
                    description: 'MACD divergence detection strategy',
                    total_pnl: -150.00,
                    is_active: false,
                    is_live: false,
                    execution_status: 'STOPPED',
                    broker: null
                }
            ];

            this.renderStrategyList();
        } catch (error) {
            console.error('Error loading strategy data:', error);
        }
    }

    renderStrategyList() {
        const strategyList = document.getElementById('strategyList');
        if (!strategyList) return;

        if (this.strategies.length === 0) {
            strategyList.innerHTML = `
                <div style="text-align: center; color: #666; padding: 2rem;">
                    <p style="margin-bottom: 1rem;">No strategies created yet.</p>
                    <a href="strategy.html" class="btn btn-primary">
                        <i class="fas fa-plus"></i> Create Your First Strategy
                    </a>
                </div>
            `;
        } else {
            strategyList.innerHTML = this.strategies.map(strategy => `
                <div class="strategy-item" style="background: white; padding: 1rem; border-radius: 8px; margin-bottom: 1rem;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>${strategy.name}</strong>
                            <div style="color: #666; font-size: 0.9rem;">${strategy.description}</div>
                            <div style="color: #666; font-size: 0.9rem;">Broker: ${strategy.broker_name}</div>
                        </div>
                        <div style="display: flex; gap: 0.5rem; align-items: center;">
                            <div class="pnl ${strategy.total_pnl >= 0 ? 'positive' : 'negative'}">
                                ${strategy.total_pnl >= 0 ? '+' : ''}$${strategy.total_pnl.toLocaleString()}
                            </div>
                            <div class="strategy-actions">
                                <button class="btn btn-secondary" onclick="editStrategy(${strategy.id})">Edit</button>
                                <a href="strategy.html" class="btn btn-primary">Manage</a>
                            </div>
                        </div>
                    </div>
                </div>
            `).join('');
        }
    }

    setupEventListeners() {
        // Add any additional event listeners here
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

    // Open broker management page
    openBrokerManagement(brokerId) {
        console.log('Opening broker management for broker ID:', brokerId);
        // Navigate to broker management section
        this.showSection('broker');
        
        // Highlight the specific broker if needed
        setTimeout(() => {
            const brokerElement = document.querySelector(`[data-broker-id="${brokerId}"]`);
            if (brokerElement) {
                brokerElement.scrollIntoView({ behavior: 'smooth' });
                brokerElement.style.border = '2px solid #1976d2';
                setTimeout(() => {
                    brokerElement.style.border = '';
                }, 2000);
            }
        }, 100);
    }

    // Open strategy management page
    openStrategyManagement(strategyId) {
        console.log('Opening strategy management for strategy ID:', strategyId);
        // Navigate to strategy management section
        this.showSection('strategy');
        
        // Highlight the specific strategy if needed
        setTimeout(() => {
            const strategyElement = document.querySelector(`[data-strategy-id="${strategyId}"]`);
            if (strategyElement) {
                strategyElement.scrollIntoView({ behavior: 'smooth' });
                strategyElement.style.border = '2px solid #9c27b0';
                setTimeout(() => {
                    strategyElement.style.border = '';
                }, 2000);
            }
        }, 100);
    }

    // Refresh broker balances from API
    async refreshBrokerBalances() {
        console.log('🔄 Refreshing broker balances...');
        
        try {
            // Show loading state
            const refreshBtn = document.querySelector('.broker-card .btn-icon');
            if (refreshBtn) {
                const originalIcon = refreshBtn.innerHTML;
                refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
                refreshBtn.disabled = true;
            }

            // Fetch updated broker data with wallet balances
            await this.loadBrokerData();
            
            // Update the broker summary display
            this.loadBrokerSummary();
            
            // Show success message
            this.showNotification('Broker balances refreshed successfully!', 'success');
            
        } catch (error) {
            console.error('Error refreshing broker balances:', error);
            this.showNotification('Failed to refresh broker balances', 'error');
        } finally {
            // Restore refresh button
            if (refreshBtn) {
                refreshBtn.innerHTML = '<i class="fas fa-sync-alt"></i>';
                refreshBtn.disabled = false;
            }
        }
    }

    // Refresh strategy data
    async refreshStrategyData() {
        console.log('🔄 Refreshing strategy data...');
        
        try {
            // Show loading state
            const refreshBtn = document.querySelector('.strategy-card .btn-icon');
            if (refreshBtn) {
                const originalIcon = refreshBtn.innerHTML;
                refreshBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
                refreshBtn.disabled = true;
            }

            // Fetch updated strategy data
            await this.loadStrategyData();
            
            // Update the strategy summary display
            this.loadStrategySummary();
            
            // Show success message
            this.showNotification('Strategy data refreshed successfully!', 'success');
            
        } catch (error) {
            console.error('Error refreshing strategy data:', error);
            this.showNotification('Failed to refresh strategy data', 'error');
        } finally {
            // Restore refresh button
            if (refreshBtn) {
                refreshBtn.innerHTML = '<i class="fas fa-sync-alt"></i>';
                refreshBtn.disabled = false;
            }
        }
    }

    // Show notification
    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
                <span>${message}</span>
            </div>
            <button class="notification-close" onclick="this.parentElement.remove()">&times;</button>
        `;
        
        // Add to page
        document.body.appendChild(notification);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 5000);
    }
}

// Global functions for forms
function showAddBrokerForm() {
    // Create and show broker form
    const formHTML = `
        <div class="form-overlay" id="brokerFormOverlay">
            <div class="form-popup">
                <button class="close-form-btn" onclick="closeForm('brokerFormOverlay')">&times;</button>
                <h3>Add New Broker</h3>
                <form id="addBrokerForm">
                    <div class="form-group">
                        <label for="brokerName">Broker Name</label>
                        <input type="text" id="brokerName" required>
                    </div>
                    <div class="form-group">
                        <label for="apiKey">API Key</label>
                        <input type="text" id="apiKey" required>
                    </div>
                    <div class="form-group">
                        <label for="apiSecret">API Secret</label>
                        <input type="password" id="apiSecret" required>
                    </div>
                    <div class="form-actions">
                        <button type="button" class="btn btn-secondary" onclick="closeForm('brokerFormOverlay')">Cancel</button>
                        <button type="submit" class="btn btn-primary">Add Broker</button>
                    </div>
                </form>
            </div>
        </div>
    `;

    document.body.insertAdjacentHTML('beforeend', formHTML);
    document.getElementById('brokerFormOverlay').style.display = 'flex';

    // Handle form submission
    document.getElementById('addBrokerForm').addEventListener('submit', function(e) {
        e.preventDefault();
        // Handle broker creation
        console.log('Adding broker...');
        closeForm('brokerFormOverlay');
    });
}

function showAddStrategyForm() {
    // Redirect to strategy management page
    window.location.href = 'strategy.html';
}

function editBroker(brokerId) {
    console.log('Editing broker:', brokerId);
    // Implement broker editing
}

function editStrategy(strategyId) {
    // Redirect to strategy management page with edit mode
    window.location.href = `strategy.html?edit=${strategyId}`;
}

function closeForm(formId) {
    const form = document.getElementById(formId);
    if (form) {
        form.remove();
    }
}

function logout() {
    console.log("Logging out...");
    
    // Clear user data and token
    localStorage.removeItem('trademanthan_user');
    localStorage.removeItem('trademanthan_token');

    // Redirect to main page
    window.location.href = 'index.html';
}

// Initialize dashboard when page loads
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Trade Manthan Dashboard Loading...');
    new Dashboard();
});

// Add some CSS for status badges and PnL
const additionalStyles = `
    <style>
        .status-badge {
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-size: 0.8rem;
            font-weight: 500;
            white-space: nowrap;
            display: inline-block;
        }

        .status-badge.connected {
            background: rgba(76, 175, 80, 0.1);
            color: #4caf50;
            border: 1px solid rgba(76, 175, 80, 0.3);
        }

        .status-badge.disconnected {
            background: rgba(244, 67, 54, 0.1);
            color: #f44336;
            border: 1px solid rgba(244, 67, 54, 0.3);
        }

        .pnl {
            font-weight: 600;
            font-size: 1.1rem;
        }

        .pnl.positive {
            color: #4caf50;
        }

        .pnl.negative {
            color: #f44336;
        }

        .strategy-summary {
            text-align: center;
        }

        .strategy-stats {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
            margin-bottom: 1.5rem;
        }

        .stat-item {
            background: rgba(25, 118, 210, 0.1);
            padding: 1rem;
            border-radius: 8px;
            border: 1px solid rgba(25, 118, 210, 0.2);
        }

        .stat-label {
            font-size: 0.8rem;
            color: #666;
            margin-bottom: 0.5rem;
        }

        .stat-value {
            font-size: 1.25rem;
            font-weight: 600;
            color: #1976d2;
        }

        .stat-value.positive {
            color: #4caf50;
        }

        .stat-value.negative {
            color: #f44336;
        }

        .strategy-actions {
            display: flex;
            gap: 1rem;
            justify-content: center;
            flex-wrap: wrap;
        }

        .add-strategy-placeholder .strategy-actions {
            margin-top: 1rem;
        }
    </style>
`;

document.head.insertAdjacentHTML('beforeend', additionalStyles);

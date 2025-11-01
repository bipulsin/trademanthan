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
        this.checkAuth();
        this.setupNavigation();
        this.loadUserData();
        this.loadDashboardData();
        this.setupEventListeners();
    }
    
    checkAuth() {
        const token = localStorage.getItem('trademanthan_token');
        if (!token) {
            window.location.href = 'login.html';
            return;
        }
        
        // Load user data from localStorage
        const userData = localStorage.getItem('trademanthan_user');
        if (userData) {
            this.userData = JSON.parse(userData);
            this.updateUserInfo();
        }
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
        const brokerContent = document.getElementById('brokerContent');
        if (!brokerContent) return;
        
        if (this.brokers.length === 0) {
            brokerContent.innerHTML = `
                <div class="add-broker-placeholder">
                    <i class="fas fa-plus-circle" style="font-size: 3rem; color: #ddd; margin-bottom: 1rem;"></i>
                    <p style="color: #666; margin-bottom: 1rem;">No brokers connected</p>
                    <button class="btn btn-primary" onclick="showAddBrokerForm()">
                        <i class="fas fa-plus"></i> Add Broker
                    </button>
                </div>
            `;
        } else {
            const connectedBrokers = this.brokers.filter(b => b.is_connected);
            brokerContent.innerHTML = `
                <div class="broker-summary">
                    <div class="broker-item">
                        <strong>${connectedBrokers[0].name}</strong>
                        <div class="wallet-balance">$${connectedBrokers[0].wallet_balance.toLocaleString()}</div>
                    </div>
                </div>
            `;
        }
    }
    
    loadStrategySummary() {
        const strategyContent = document.getElementById('strategyContent');
        if (!strategyContent) return;
        
        if (this.strategies.length === 0) {
            strategyContent.innerHTML = `
                <div class="add-strategy-placeholder">
                    <i class="fas fa-plus-circle" style="font-size: 3rem; color: #ddd; margin-bottom: 1rem;"></i>
                    <p style="color: #666; margin-bottom: 1rem;">No strategies created</p>
                    <button class="btn btn-primary" onclick="showAddStrategyForm()">
                        <i class="fas fa-plus"></i> Create Strategy
                    </button>
                </div>
            `;
        } else {
            const totalPnl = this.strategies.reduce((sum, s) => sum + s.total_pnl, 0);
            strategyContent.innerHTML = `
                <div class="strategy-summary">
                    <div class="strategy-count">${this.strategies.length} Active Strategies</div>
                    <div class="total-pnl ${totalPnl >= 0 ? 'positive' : 'negative'}">
                        ${totalPnl >= 0 ? '+' : ''}$${totalPnl.toLocaleString()}
                    </div>
                </div>
            `;
        }
    }
    
    async loadBrokerData() {
        try {
            // In production, this would call your backend API
            // For now, we'll use mock data
            this.brokers = [
                {
                    id: 1,
                    name: 'Demo Broker',
                    wallet_balance: 25000,
                    is_connected: true,
                    last_sync: new Date().toISOString()
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
                    name: 'Moving Average Crossover',
                    description: 'Simple moving average strategy',
                    total_pnl: 1250.50,
                    is_active: true,
                    broker_name: 'Demo Broker'
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
            strategyList.innerHTML = '<p style="text-align: center; color: #666;">No strategies created yet.</p>';
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
                            <button class="btn btn-secondary" onclick="editStrategy(${strategy.id})">Edit</button>
                        </div>
                    </div>
                </div>
            `).join('');
        }
    }
    
    setupEventListeners() {
        // Add any additional event listeners here
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
    // Create and show strategy form
    const formHTML = `
        <div class="form-overlay" id="strategyFormOverlay">
            <div class="form-popup">
                <button class="close-form-btn" onclick="closeForm('strategyFormOverlay')">&times;</button>
                <h3>Create New Strategy</h3>
                <form id="addStrategyForm">
                    <div class="form-group">
                        <label for="strategyName">Strategy Name</label>
                        <input type="text" id="strategyName" required>
                    </div>
                    <div class="form-group">
                        <label for="strategyDescription">Description</label>
                        <textarea id="strategyDescription" rows="3"></textarea>
                    </div>
                    <div class="form-actions">
                        <button type="button" class="btn btn-secondary" onclick="closeForm('strategyFormOverlay')">Cancel</button>
                        <button type="submit" class="btn btn-primary">Create Strategy</button>
                    </div>
                </form>
            </div>
        </div>
    `;
    
    document.body.insertAdjacentHTML('beforeend', formHTML);
    document.getElementById('strategyFormOverlay').style.display = 'flex';
    
    // Handle form submission
    document.getElementById('addStrategyForm').addEventListener('submit', function(e) {
        e.preventDefault();
        // Handle strategy creation
        console.log('Creating strategy...');
        closeForm('strategyFormOverlay');
    });
}

function closeForm(formId) {
    const form = document.getElementById(formId);
    if (form) {
        form.remove();
    }
}

function logout() {
    // Clear user data
    localStorage.removeItem('trademanthan_user');
    localStorage.removeItem('trademanthan_token');
    
    // Redirect to login
    window.location.href = 'login.html';
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
            font-weight: 700;
            font-size: 1.1rem;
        }
        
        .pnl.positive {
            color: #4caf50;
        }
        
        .pnl.negative {
            color: #f44336;
        }
        
        .add-broker-placeholder,
        .add-strategy-placeholder {
            text-align: center;
            padding: 2rem;
        }
        
        .broker-summary,
        .strategy-summary {
            text-align: center;
        }
        
        .wallet-balance {
            font-size: 1.5rem;
            font-weight: 700;
            color: #4caf50;
            margin-top: 0.5rem;
        }
        
        .strategy-count {
            color: #666;
            margin-bottom: 0.5rem;
        }
        
        .total-pnl {
            font-size: 1.5rem;
            font-weight: 700;
        }
        
        .total-pnl.positive {
            color: #4caf50;
        }
        
        .total-pnl.negative {
            color: #f44336;
        }
    </style>
`;

document.head.insertAdjacentHTML('beforeend', additionalStyles);

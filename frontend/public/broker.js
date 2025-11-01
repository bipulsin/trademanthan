class BrokerManager {
    constructor() {
        this.brokers = [];
        this.currentUser = null;
        this.editingBrokerId = null;
        this.deletingBrokerId = null;
        this.init();
    }

    async init() {
        await this.waitForLeftMenu();
        this.loadUserData();
        
        // Clear old demo data from localStorage
        this.clearOldDemoData();
        
        // Test localStorage functionality
        this.testLocalStorage();
        
        this.loadBrokers();
        this.setupEventListeners();
        this.setupMobileMenu();
    }

    async waitForLeftMenu() {
        return new Promise((resolve) => {
            const checkLeftMenu = () => {
                const leftPanel = document.querySelector('.left-panel');
                const userAvatar = document.querySelector('#userAvatar');
                
                if (leftPanel && userAvatar) {
                    resolve();
                } else {
                    setTimeout(checkLeftMenu, 100);
                }
            };
            checkLeftMenu();
        });
    }

    loadUserData() {
        const userData = localStorage.getItem('trademanthan_user');
        if (userData) {
            try {
                this.currentUser = JSON.parse(userData);
            } catch (error) {
                console.error('Error parsing user data:', error);
            }
        }
    }

    setupEventListeners() {
        // Form submission
        document.getElementById('brokerForm').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleBrokerSubmit();
        });

        // Close modals on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.closeBrokerModal();
                this.closeDeleteModal();
            }
        });

        // Close modals on outside click
        document.getElementById('brokerModal').addEventListener('click', (e) => {
            if (e.target.id === 'brokerModal') {
                this.closeBrokerModal();
            }
        });

        document.getElementById('deleteModal').addEventListener('click', (e) => {
            if (e.target.id === 'deleteModal') {
                this.closeDeleteModal();
            }
        });
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

    async loadBrokers() {
        console.log('BrokerManager: Loading brokers...');
        
        if (!this.currentUser || !this.currentUser.id) {
            console.error('BrokerManager: No current user found');
            this.brokers = [];
            this.renderBrokers();
            return;
        }
        
        console.log('BrokerManager: Current user ID:', this.currentUser.id);
        
        try {
            // Fetch brokers from backend API
            const apiUrl = `/broker/?user_id=${this.currentUser.id}`;
            console.log('BrokerManager: Calling API:', apiUrl);
            
            const response = await fetch(apiUrl);
            console.log('BrokerManager: API response status:', response.status);
            
            if (response.ok) {
                const data = await response.json();
                console.log('BrokerManager: API response data:', data);
                
                if (data.success) {
                    this.brokers = data.brokers;
                    console.log('BrokerManager: Brokers loaded from API:', this.brokers);
                    console.log('BrokerManager: Number of brokers from API:', this.brokers.length);
                } else {
                    console.error('BrokerManager: API returned error:', data);
                    this.brokers = [];
                }
            } else {
                console.error('BrokerManager: Failed to fetch brokers from API. Status:', response.status);
                const errorText = await response.text();
                console.error('BrokerManager: API error response:', errorText);
                
                // Only fallback to localStorage for server errors (5xx), not client errors (4xx)
                if (response.status >= 500) {
                    console.log('BrokerManager: Server error, falling back to localStorage');
                    this.loadBrokersFromLocalStorage();
                } else {
                    console.log('BrokerManager: Client error, showing empty state');
                    this.brokers = [];
                }
            }
        } catch (error) {
            console.error('BrokerManager: Network error fetching brokers from API:', error);
            // Only fallback to localStorage for network errors
            this.loadBrokersFromLocalStorage();
        }
        
        console.log('BrokerManager: Final brokers array:', this.brokers);
        this.renderBrokers();
    }
    
    loadBrokersFromLocalStorage() {
        console.log('BrokerManager: Falling back to localStorage...');
        const savedBrokers = localStorage.getItem('trademanthan_brokers');
        console.log('BrokerManager: Raw data from localStorage:', savedBrokers);
        
        if (savedBrokers) {
            try {
                const allBrokers = JSON.parse(savedBrokers);
                // Filter brokers for current user (if they have user_id field)
                this.brokers = allBrokers.filter(broker => 
                    !broker.user_id || broker.user_id === this.currentUser.id
                );
                console.log('BrokerManager: Brokers loaded from storage:', this.brokers);
                console.log('BrokerManager: Number of brokers:', this.brokers.length);
            } catch (error) {
                console.error('BrokerManager: Error parsing saved brokers:', error);
                this.brokers = [];
            }
        } else {
            console.log('BrokerManager: No brokers found in storage, showing empty state');
            // Don't create demo data - just show empty state
            this.brokers = [];
        }
    }

    saveBrokersToStorage() {
        try {
            console.log('BrokerManager: Saving brokers to storage:', this.brokers);
            localStorage.setItem('trademanthan_brokers', JSON.stringify(this.brokers));
            console.log('BrokerManager: Brokers saved to storage successfully');
            
            // Verify the save
            const saved = localStorage.getItem('trademanthan_brokers');
            console.log('BrokerManager: Verification - data in storage:', saved);
            
        } catch (error) {
            console.error('BrokerManager: Error saving brokers to storage:', error);
        }
    }

    renderBrokers() {
        const brokerGrid = document.getElementById('brokerGrid');
        
        if (this.brokers.length === 0) {
            brokerGrid.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-university"></i>
                    <h3>No Brokers Connected</h3>
                    <p>Get started by connecting your first trading broker</p>
                    <button class="btn btn-primary" onclick="showAddBrokerForm()">
                        <i class="fas fa-plus"></i> Connect Broker
                    </button>
                </div>
            `;
            return;
        }

        brokerGrid.innerHTML = this.brokers.map(broker => this.createBrokerCard(broker)).join('');
    }

    createBrokerCard(broker) {
        const maskedApiKey = this.maskApiKey(broker.api_key || '');
        const maskedApiSecret = this.maskApiSecret(broker.api_secret || '');
        const statusClass = `status-${broker.connection_status || 'disconnected'}`;
        const statusText = (broker.connection_status || 'disconnected').charAt(0).toUpperCase() + (broker.connection_status || 'disconnected').slice(1);
        const createdAt = broker.created_at ? new Date(broker.created_at).toLocaleDateString() : 'Unknown';

        return `
            <div class="broker-card" data-broker-id="${broker.id}">
                <div class="broker-card-header">
                    <h3 class="broker-name">${broker.name}</h3>
                    <div class="broker-actions">
                        <button class="action-btn btn-edit" onclick="editBroker(${broker.id})" title="Edit Broker">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="action-btn btn-delete" onclick="deleteBroker(${broker.id})" title="Delete Broker">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                </div>
                
                <div class="broker-card-body">
                    <div class="broker-info">
                        <div class="info-item">
                            <span class="info-label">Type:</span>
                            <span class="info-value">${this.formatBrokerType(broker.type)}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">API URL:</span>
                            <span class="info-value">${broker.api_url || 'Not set'}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">API Key:</span>
                            <span class="info-value">${maskedApiKey}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">API Secret:</span>
                            <span class="info-value">${maskedApiSecret}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">Test Mode:</span>
                            <span class="info-value">${broker.test_mode ? 'Yes' : 'No'}</span>
                        </div>
                        <div class="info-item">
                            <span class="info-label">Created:</span>
                            <span class="info-value">${createdAt}</span>
                        </div>
                    </div>
                    
                    <div class="broker-status">
                        <div class="status-indicator ${statusClass}">
                            ${statusText}
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    maskApiKey(apiKey) {
        if (!apiKey || apiKey.trim() === '') return 'Not set';
        // Show only first 8 characters to fit in single row
        if (apiKey.length <= 8) return apiKey;
        return apiKey.substring(0, 8) + '...';
    }

    maskApiSecret(apiSecret) {
        if (!apiSecret || apiSecret.trim() === '') return 'Not set';
        // Show only first 8 characters to fit in single row
        if (apiSecret.length <= 8) return apiSecret;
        return apiSecret.substring(0, 8) + '...';
    }

    formatBrokerType(type) {
        const types = {
            'crypto': 'Cryptocurrency',
            'equity': 'Stock/Equity',
            'forex': 'Forex',
            'commodity': 'Commodity'
        };
        return types[type] || type;
    }

    showAddBrokerForm() {
        this.editingBrokerId = null;
        document.getElementById('modalTitle').textContent = 'Add New Broker';
        document.getElementById('brokerForm').reset();
        document.getElementById('brokerModal').style.display = 'flex';
    }

    editBroker(brokerId) {
        console.log('BrokerManager: Editing broker ID:', brokerId);
        const broker = this.brokers.find(b => b.id === brokerId);
        
        if (!broker) {
            console.error('BrokerManager: Broker not found for editing');
            return;
        }
        
        this.editingBrokerId = brokerId;
        
        // Populate form with broker data
        document.getElementById('brokerName').value = broker.name || '';
        document.getElementById('brokerType').value = broker.type || 'crypto';
        document.getElementById('apiUrl').value = broker.api_url || '';
        document.getElementById('apiKey').value = broker.api_key || '';
        document.getElementById('apiSecret').value = broker.api_secret || '';
        document.getElementById('testMode').checked = broker.test_mode || false;
        
        // Update modal title
        document.getElementById('modalTitle').textContent = 'Edit Broker';
        
        // Show modal
        document.getElementById('brokerModal').style.display = 'flex';
    }

    closeBrokerModal() {
        document.getElementById('brokerModal').style.display = 'none';
        this.editingBrokerId = null;
    }

    async handleBrokerSubmit() {
        const form = document.getElementById('brokerForm');
        const formData = new FormData(form);
        
        const brokerData = {
            user_id: this.currentUser.id,
            name: formData.get('brokerName'),
            type: formData.get('brokerType'),
            api_url: formData.get('apiUrl'),
            api_key: formData.get('apiKey'),
            api_secret: formData.get('apiSecret'),
            test_mode: formData.get('testMode') === 'on',
            is_connected: false,
            connection_status: 'disconnected'
        };
        
        console.log('BrokerManager: Submitting broker data:', brokerData);
        
        try {
            let response;
            if (this.editingBrokerId) {
                // Update existing broker
                console.log('BrokerManager: Updating broker ID:', this.editingBrokerId);
                response = await fetch(`/broker/${this.editingBrokerId}`, {
                    method: 'PUT',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(brokerData)
                });
            } else {
                // Create new broker
                console.log('BrokerManager: Creating new broker');
                response = await fetch('/broker/', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(brokerData)
                });
            }
            
            if (response.ok) {
                const result = await response.json();
                if (result.success) {
                    console.log('BrokerManager: Broker operation successful:', result);
                    this.showNotification('Broker saved successfully!', 'success');
                    
                    // Refresh brokers from API
                    await this.loadBrokers();
                    
                    // Close modal and reset form
                    this.closeBrokerModal();
                    form.reset();
                    this.editingBrokerId = null;
                } else {
                    console.error('BrokerManager: API returned error:', result);
                    this.showNotification('Failed to save broker: ' + result.message, 'error');
                }
            } else {
                console.error('BrokerManager: HTTP error:', response.status);
                this.showNotification('Failed to save broker. Please try again.', 'error');
            }
        } catch (error) {
            console.error('BrokerManager: Error saving broker:', error);
            this.showNotification('Error saving broker. Please try again.', 'error');
        }
    }

    deleteBroker(brokerId) {
        this.deletingBrokerId = brokerId;
        document.getElementById('deleteModal').style.display = 'flex';
    }

    closeDeleteModal() {
        document.getElementById('deleteModal').style.display = 'none';
        this.deletingBrokerId = null;
    }

    async confirmDeleteBroker() {
        if (!this.deletingBrokerId) return;
        
        console.log('BrokerManager: Confirming deletion of broker ID:', this.deletingBrokerId);
        
        try {
            const response = await fetch(`/broker/${this.deletingBrokerId}`, {
                method: 'DELETE'
            });
            
            if (response.ok) {
                const result = await response.json();
                if (result.success) {
                    console.log('BrokerManager: Broker deleted successfully');
                    this.showNotification('Broker deleted successfully!', 'success');
                    
                    // Refresh brokers from API
                    await this.loadBrokers();
                } else {
                    console.error('BrokerManager: API returned error:', result);
                    this.showNotification('Failed to delete broker: ' + result.message, 'error');
                }
            } else {
                console.error('BrokerManager: HTTP error:', response.status);
                this.showNotification('Failed to delete broker. Please try again.', 'error');
            }
        } catch (error) {
            console.error('BrokerManager: Error deleting broker:', error);
            this.showNotification('Error deleting broker. Please try again.', 'error');
        }
        
        this.closeDeleteModal();
        this.deletingBrokerId = null;
    }

    togglePassword(fieldId) {
        const input = document.getElementById(fieldId);
        const button = input.nextElementSibling;
        const icon = button.querySelector('i');
        
        if (input.type === 'password') {
            input.type = 'text';
            icon.className = 'fas fa-eye-slash';
        } else {
            input.type = 'password';
            icon.className = 'fas fa-eye';
        }
    }

    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <i class="fas fa-${type === 'success' ? 'check-circle' : 'info-circle'}"></i>
            <span>${message}</span>
        `;
        
        // Add to page
        document.body.appendChild(notification);
        
        // Show notification
        setTimeout(() => notification.classList.add('show'), 100);
        
        // Remove after 3 seconds
        setTimeout(() => {
            notification.classList.remove('show');
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }

    // Test localStorage functionality
    testLocalStorage() {
        console.log('BrokerManager: Testing localStorage functionality...');
        
        try {
            // Test write
            const testKey = 'trademanthan_test';
            const testData = { test: 'data', timestamp: Date.now() };
            localStorage.setItem(testKey, JSON.stringify(testData));
            console.log('BrokerManager: Test write successful');
            
            // Test read
            const readData = localStorage.getItem(testKey);
            console.log('BrokerManager: Test read successful:', readData);
            
            // Test delete
            localStorage.removeItem(testKey);
            console.log('BrokerManager: Test delete successful');
            
            // Test broker storage specifically
            const brokerTestData = [{ id: 999, name: 'Test Broker' }];
            localStorage.setItem('trademanthan_brokers', JSON.stringify(brokerTestData));
            const readBrokers = localStorage.getItem('trademanthan_brokers');
            console.log('BrokerManager: Broker storage test successful:', readBrokers);
            
            // Clean up test data
            localStorage.removeItem('trademanthan_brokers');
            
            console.log('BrokerManager: All localStorage tests passed!');
            return true;
        } catch (error) {
            console.error('BrokerManager: localStorage test failed:', error);
            return false;
        }
    }
    
    clearOldDemoData() {
        console.log('BrokerManager: Clearing old demo data from localStorage...');
        try {
            // Remove old demo brokers that don't have user_id
            const savedBrokers = localStorage.getItem('trademanthan_brokers');
            if (savedBrokers) {
                const allBrokers = JSON.parse(savedBrokers);
                // Keep only brokers that have a user_id (real user data)
                const realBrokers = allBrokers.filter(broker => broker.user_id);
                
                if (realBrokers.length !== allBrokers.length) {
                    console.log('BrokerManager: Removed demo brokers, keeping real user brokers');
                    localStorage.setItem('trademanthan_brokers', JSON.stringify(realBrokers));
                }
            }
        } catch (error) {
            console.error('BrokerManager: Error clearing old demo data:', error);
        }
    }
}

// Global functions for onclick handlers
function showAddBrokerForm() {
    window.brokerManager.showAddBrokerForm();
}

function editBroker(brokerId) {
    window.brokerManager.editBroker(brokerId);
}

function deleteBroker(brokerId) {
    window.brokerManager.deleteBroker(brokerId);
}

function closeBrokerModal() {
    window.brokerManager.closeBrokerModal();
}

function closeDeleteModal() {
    window.brokerManager.closeDeleteModal();
}

function confirmDeleteBroker() {
    window.brokerManager.confirmDeleteBroker();
}

function togglePassword(fieldId) {
    window.brokerManager.togglePassword(fieldId);
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.brokerManager = new BrokerManager();
});

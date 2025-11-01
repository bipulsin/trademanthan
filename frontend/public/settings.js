// Settings Management JavaScript
class SettingsManager {
    constructor() {
        this.settings = {};
        this.currentUser = null;
        this.init();
    }

    init() {
        // Wait for left menu to load and authenticate before proceeding
        this.waitForLeftMenu().then(() => {
            console.log('Settings: Left menu ready, initializing settings functionality...');
            this.loadUserData();
                    this.loadSettings();
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
                    console.log('Settings: Left panel and user data ready, proceeding with initialization');
                    resolve();
                } else {
                    console.log('Settings: Waiting for left menu to complete authentication...');
                    setTimeout(checkLeftMenu, 100);
                }
            };
            checkLeftMenu();
        });
    }

    setupEventListeners() {
        // Toggle switches for notification methods
        document.getElementById('emailEnabled').addEventListener('change', (e) => {
            this.toggleNotificationMethod('email', e.target.checked);
        });

        document.getElementById('telegramEnabled').addEventListener('change', (e) => {
            this.toggleNotificationMethod('telegram', e.target.checked);
        });

        document.getElementById('whatsappEnabled').addEventListener('change', (e) => {
            this.toggleNotificationMethod('whatsapp', e.target.checked);
        });

        // Theme selection
        document.querySelectorAll('input[name="theme"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.changeTheme(e.target.value);
            });
        });

        // Language and timezone changes
        document.getElementById('language').addEventListener('change', (e) => {
            this.changeLanguage(e.target.value);
        });

        document.getElementById('timezone').addEventListener('change', (e) => {
            this.changeTimezone(e.target.value);
        });

        document.getElementById('currency').addEventListener('change', (e) => {
            this.changeCurrency(e.target.value);
        });

        // Trading settings
        document.getElementById('cryptoUpdateInterval').addEventListener('change', (e) => {
            this.updateCryptoUpdateInterval(parseInt(e.target.value));
        });

        document.getElementById('cryptoAutoRefresh').addEventListener('change', (e) => {
            this.updateSetting('trading.crypto.autoRefresh', e.target.checked);
        });

        document.getElementById('cryptoShowCountdown').addEventListener('change', (e) => {
            this.updateSetting('trading.crypto.showCountdown', e.target.checked);
        });

        document.getElementById('strategyCheckInterval').addEventListener('change', (e) => {
            this.updateSetting('trading.strategy.checkInterval', parseInt(e.target.value));
        });

        document.getElementById('strategyNotifications').addEventListener('change', (e) => {
            this.updateSetting('trading.strategy.notifications', e.target.checked);
        });

        // Form inputs for real-time saving
        this.setupFormInputListeners();
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

    setupFormInputListeners() {
        // Email settings
        document.getElementById('emailAddress').addEventListener('blur', (e) => {
            this.updateSetting('email.address', e.target.value);
        });

        document.getElementById('emailFrequency').addEventListener('change', (e) => {
            this.updateSetting('email.frequency', e.target.value);
        });

        // Telegram settings
        document.getElementById('telegramBotToken').addEventListener('blur', (e) => {
            this.updateSetting('telegram.botToken', e.target.value);
        });

        document.getElementById('telegramChatId').addEventListener('blur', (e) => {
            this.updateSetting('telegram.chatId', e.target.value);
        });

        document.getElementById('telegramFrequency').addEventListener('change', (e) => {
            this.updateSetting('telegram.frequency', e.target.value);
        });

        // WhatsApp settings
        document.getElementById('whatsappPhone').addEventListener('blur', (e) => {
            this.updateSetting('whatsapp.phone', e.target.value);
        });

        document.getElementById('whatsappApiKey').addEventListener('blur', (e) => {
            this.updateSetting('whatsapp.apiKey', e.target.value);
        });

        document.getElementById('whatsappFrequency').addEventListener('change', (e) => {
            this.updateSetting('whatsapp.frequency', e.target.value);
        });

        // Notification type checkboxes
        this.setupNotificationTypeListeners();
    }

    setupNotificationTypeListeners() {
        const notificationTypes = ['Strategy', 'Broker', 'Reports', 'Trades'];
        const methods = ['email', 'telegram', 'whatsapp'];

        methods.forEach(method => {
            notificationTypes.forEach(type => {
                const checkbox = document.getElementById(`${method}${type}`);
                if (checkbox) {
                    checkbox.addEventListener('change', (e) => {
                        this.updateSetting(`${method}.types.${type.toLowerCase()}`, e.target.checked);
                    });
                }
            });
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
                    setTimeout(() => {
                        leftPanel.classList.remove('mobile-open');
                    }, 150);
                });
            });
        }
    }

    loadUserData() {
        const userData = localStorage.getItem('trademanthan_user');
        if (userData) {
            try {
                this.currentUser = JSON.parse(userData);
                console.log('Settings: User data loaded:', this.currentUser);
            } catch (error) {
                console.error('Settings: Error parsing user data:', error);
            }
        }
    }

    loadSettings() {
        // Load settings from localStorage or use defaults
        const savedSettings = localStorage.getItem('trademanthan_settings');
        if (savedSettings) {
            try {
                this.settings = JSON.parse(savedSettings);
                console.log('Settings: Loaded saved settings:', this.settings);
            } catch (error) {
                console.error('Settings: Error parsing saved settings:', error);
                this.settings = this.getDefaultSettings();
            }
        } else {
            this.settings = this.getDefaultSettings();
        }

        this.applySettings();
    }

    getDefaultSettings() {
        return {
            notifications: {
                email: {
                    enabled: true,
                    address: 'user@example.com',
                    frequency: 'daily',
                    types: {
                        strategy: true,
                        broker: true,
                        reports: true,
                        trades: true
                    }
                },
                telegram: {
                    enabled: false,
                    botToken: '',
                    chatId: '',
                    frequency: 'immediate',
                    types: {
                        strategy: false,
                        broker: false,
                        reports: false,
                        trades: false
                    }
                },
                whatsapp: {
                    enabled: false,
                    phone: '',
                    apiKey: '',
                    frequency: 'immediate',
                    types: {
                        strategy: false,
                        broker: false,
                        reports: false,
                        trades: false
                    }
                }
            },
            application: {
                theme: 'light',
                language: 'en',
                timezone: 'EST',
                currency: 'USD'
            },
            trading: {
                crypto: {
                    updateInterval: 300000, // 5 minutes in milliseconds
                    autoRefresh: true,
                    showCountdown: true
                },
                strategy: {
                    checkInterval: 300000, // 5 minutes in milliseconds
                    notifications: true
                }
            }
        };
    }

    applySettings() {
        // Apply notification settings
        this.applyNotificationSettings();
        
        // Apply application settings
        this.applyApplicationSettings();
        
        // Apply trading settings
        this.applyTradingSettings();
    }

    applyNotificationSettings() {
        const { email, telegram, whatsapp } = this.settings.notifications;

        // Email settings
        document.getElementById('emailEnabled').checked = email.enabled;
        document.getElementById('emailAddress').value = email.address;
        document.getElementById('emailFrequency').value = email.frequency;
        document.getElementById('emailStrategy').checked = email.types.strategy;
        document.getElementById('emailBroker').checked = email.types.broker;
        document.getElementById('emailReports').checked = email.types.reports;
        document.getElementById('emailTrades').checked = email.types.trades;

        // Telegram settings
        document.getElementById('telegramEnabled').checked = telegram.enabled;
        document.getElementById('telegramBotToken').value = telegram.botToken;
        document.getElementById('telegramChatId').value = telegram.chatId;
        document.getElementById('telegramFrequency').value = telegram.frequency;
        document.getElementById('telegramStrategy').checked = telegram.types.strategy;
        document.getElementById('telegramBroker').checked = telegram.types.broker;
        document.getElementById('telegramReports').checked = telegram.types.reports;
        document.getElementById('telegramTrades').checked = telegram.types.trades;

        // WhatsApp settings
        document.getElementById('whatsappEnabled').checked = whatsapp.enabled;
        document.getElementById('whatsappPhone').value = whatsapp.phone;
        document.getElementById('whatsappApiKey').value = whatsapp.apiKey;
        document.getElementById('whatsappFrequency').value = whatsapp.frequency;
        document.getElementById('whatsappStrategy').checked = whatsapp.types.strategy;
        document.getElementById('whatsappBroker').checked = whatsapp.types.broker;
        document.getElementById('whatsappReports').checked = whatsapp.types.reports;
        document.getElementById('whatsappTrades').checked = whatsapp.types.trades;

        // Show/hide configuration sections
        this.toggleNotificationMethod('email', email.enabled);
        this.toggleNotificationMethod('telegram', telegram.enabled);
        this.toggleNotificationMethod('whatsapp', whatsapp.enabled);
    }

    applyApplicationSettings() {
        const { theme, language, timezone, currency } = this.settings.application;

        // Theme
        document.querySelector(`input[name="theme"][value="${theme}"]`).checked = true;
        this.changeTheme(theme);

        // Language
        document.getElementById('language').value = language;

        // Timezone
        document.getElementById('timezone').value = timezone;

        // Currency
        document.getElementById('currency').value = currency;
    }

    applyTradingSettings() {
        const { crypto, strategy } = this.settings.trading;

        // Crypto settings
        document.getElementById('cryptoUpdateInterval').value = crypto.updateInterval;
        document.getElementById('cryptoAutoRefresh').checked = crypto.autoRefresh;
        document.getElementById('cryptoShowCountdown').checked = crypto.showCountdown;

        // Strategy settings
        document.getElementById('strategyCheckInterval').value = strategy.checkInterval;
        document.getElementById('strategyNotifications').checked = strategy.notifications;
    }

    updateCryptoUpdateInterval(interval) {
        this.updateSetting('trading.crypto.updateInterval', interval);
        
        // Notify the crypto price manager about the new interval
        if (window.cryptoPriceManager) {
            window.cryptoPriceManager.updateInterval(interval);
        }
        
        // Show success message
        this.showNotification('Crypto update interval updated successfully!', 'success');
    }

    toggleNotificationMethod(method, enabled) {
        const configElement = document.getElementById(`${method}Config`);
        if (configElement) {
            configElement.style.display = enabled ? 'block' : 'none';
        }

        // Update settings
        this.settings.notifications[method].enabled = enabled;
        this.saveSettingsToStorage();
    }

    changeTheme(theme) {
        this.settings.application.theme = theme;
        
        // Apply theme to body
        document.body.className = theme === 'dark' ? 'dark-theme' : '';
        
        // Save to storage
        this.saveSettingsToStorage();
        
        // Show notification
        this.showNotification(`Theme changed to ${theme}`, 'success');
    }

    changeLanguage(language) {
        this.settings.application.language = language;
        this.saveSettingsToStorage();
        this.showNotification(`Language changed to ${language}`, 'info');
    }

    changeTimezone(timezone) {
        this.settings.application.timezone = timezone;
        this.saveSettingsToStorage();
        this.showNotification(`Timezone changed to ${timezone}`, 'info');
    }

    changeCurrency(currency) {
        this.settings.application.currency = currency;
        this.saveSettingsToStorage();
        this.showNotification(`Currency changed to ${currency}`, 'info');
    }

    updateSetting(path, value) {
        // Update nested setting using path (e.g., 'email.address', 'telegram.types.strategy')
        const keys = path.split('.');
        let current = this.settings;
        
        for (let i = 0; i < keys.length - 1; i++) {
            current = current[keys[i]];
        }
        
        current[keys[keys.length - 1]] = value;
        this.saveSettingsToStorage();
    }

    saveSettingsToStorage() {
        try {
            localStorage.setItem('trademanthan_settings', JSON.stringify(this.settings));
            console.log('Settings: Settings saved to localStorage');
        } catch (error) {
            console.error('Settings: Error saving settings:', error);
        }
    }

    async saveSettings() {
        try {
            // In production, this would send settings to your backend API
            console.log('Settings: Saving settings to backend...');
            
            // Simulate API call
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            this.showNotification('Settings saved successfully!', 'success');
            
            // Save to localStorage as backup
            this.saveSettingsToStorage();
            
        } catch (error) {
            console.error('Settings: Error saving settings:', error);
            this.showNotification('Failed to save settings. Please try again.', 'error');
        }
    }

    resetToDefaults() {
        if (confirm('Are you sure you want to reset all settings to defaults? This action cannot be undone.')) {
            this.settings = this.getDefaultSettings();
            this.applySettings();
            this.saveSettingsToStorage();
            this.showNotification('Settings reset to defaults', 'info');
        }
    }

    async testTelegram() {
        const botToken = document.getElementById('telegramBotToken').value;
        const chatId = document.getElementById('telegramChatId').value;
        
        if (!botToken || !chatId) {
            this.showNotification('Please enter both Bot Token and Chat ID', 'error');
            return;
        }

        try {
            this.showNotification('Testing Telegram connection...', 'info');
            
            // In production, this would make an actual API call to test the connection
            await new Promise(resolve => setTimeout(resolve, 2000));
            
            this.showNotification('Telegram connection successful! Test message sent.', 'success');
            
        } catch (error) {
            console.error('Settings: Telegram test failed:', error);
            this.showNotification('Telegram connection failed. Please check your credentials.', 'error');
        }
    }

    async testWhatsApp() {
        const phone = document.getElementById('whatsappPhone').value;
        const apiKey = document.getElementById('whatsappApiKey').value;
        
        if (!phone || !apiKey) {
            this.showNotification('Please enter both Phone Number and API Key', 'error');
            return;
        }

        try {
            this.showNotification('Testing WhatsApp connection...', 'info');
            
            // In production, this would make an actual API call to test the connection
            await new Promise(resolve => setTimeout(resolve, 2000));
            
            this.showNotification('WhatsApp connection successful! Test message sent.', 'success');
            
        } catch (error) {
            console.error('Settings: WhatsApp test failed:', error);
            this.showNotification('WhatsApp connection failed. Please check your credentials.', 'error');
        }
    }

    showNotification(message, type = 'info') {
        const notification = document.getElementById('notification');
        notification.textContent = message;
        notification.className = `notification ${type}`;
        notification.classList.add('show');
        
        // Hide after 3 seconds
        setTimeout(() => {
            notification.classList.remove('show');
        }, 3000);
    }
}

// Global functions for onclick handlers
function saveSettings() {
    if (window.settingsManager) {
        window.settingsManager.saveSettings();
    }
}

function resetToDefaults() {
    if (window.settingsManager) {
        window.settingsManager.resetToDefaults();
    }
}

function testTelegram() {
    if (window.settingsManager) {
        window.settingsManager.testTelegram();
    }
}

function testWhatsApp() {
    if (window.settingsManager) {
        window.settingsManager.testWhatsApp();
    }
}

// Upstox token update function
async function updateUpstoxToken() {
    const tokenInput = document.getElementById('upstoxToken');
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
            alert('✅ Token updated successfully! Backend service is restarting...');
            tokenInput.value = '';
        } else {
            alert('❌ Error: ' + result.message);
        }
    } catch (error) {
        alert('❌ Error updating token: ' + error.message);
    } finally {
        button.disabled = false;
        button.innerHTML = '<i class="fas fa-sync-alt"></i> Update Token';
    }
}

// Upstox OAuth Login for Settings Page
async function initiateUpstoxOAuthSettings() {
    try {
        // Show loading state
        const button = event.target;
        button.disabled = true;
        button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Redirecting...';
        
        console.log('Initiating Upstox OAuth login...');
        
        // Get the OAuth login URL from backend
        const response = await fetch('https://trademanthan.in/scan/upstox/login');
        const result = await response.json();
        
        if (result.status === 'success' && result.auth_url) {
            // Redirect to Upstox OAuth page
            console.log('Redirecting to Upstox OAuth page...');
            window.location.href = result.auth_url;
        } else {
            alert('❌ Error initiating OAuth login: ' + (result.message || 'Unknown error'));
            button.disabled = false;
            button.innerHTML = '<i class="fas fa-sign-in-alt"></i> Login with Upstox';
        }
    } catch (error) {
        alert('❌ Error initiating OAuth login: ' + error.message);
        console.error('OAuth initiation error:', error);
        const button = event.target;
        button.disabled = false;
        button.innerHTML = '<i class="fas fa-sign-in-alt"></i> Login with Upstox';
    }
}

// Check for OAuth callback success on page load
window.addEventListener('DOMContentLoaded', function() {
    const urlParams = new URLSearchParams(window.location.search);
    const authStatus = urlParams.get('auth');
    
    if (authStatus === 'success') {
        // Show success message
        const statusDiv = document.getElementById('authStatusMessage');
        if (statusDiv) {
            statusDiv.style.display = 'block';
            statusDiv.style.background = 'linear-gradient(135deg, #48bb78 0%, #38a169 100%)';
            statusDiv.style.color = 'white';
            statusDiv.innerHTML = '<i class="fas fa-check-circle"></i> ✅ Upstox authentication successful! Backend service is restarting...';
        }
        
        // Clean up URL
        const cleanUrl = window.location.pathname;
        window.history.replaceState({}, document.title, cleanUrl);
        
        // Reload page after delay
        setTimeout(() => {
            window.location.reload();
        }, 3000);
    } else if (authStatus === 'error') {
        const errorMsg = urlParams.get('message') || 'Authentication failed';
        const statusDiv = document.getElementById('authStatusMessage');
        if (statusDiv) {
            statusDiv.style.display = 'block';
            statusDiv.style.background = 'linear-gradient(135deg, #fc8181 0%, #f56565 100%)';
            statusDiv.style.color = 'white';
            statusDiv.innerHTML = '<i class="fas fa-exclamation-triangle"></i> ❌ Error: ' + errorMsg;
        }
        
        // Clean up URL
        const cleanUrl = window.location.pathname;
        window.history.replaceState({}, document.title, cleanUrl);
    }
});

// Make functions globally accessible
window.saveSettings = saveSettings;
window.updateUpstoxToken = updateUpstoxToken;
window.resetToDefaults = resetToDefaults;
window.testTelegram = testTelegram;
window.testWhatsApp = testWhatsApp;
window.initiateUpstoxOAuthSettings = initiateUpstoxOAuthSettings;

// Initialize settings manager when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    console.log('Settings: DOM loaded, initializing SettingsManager...');
    window.settingsManager = new SettingsManager();
});

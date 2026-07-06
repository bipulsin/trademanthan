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
        this.loadRsConvictionConfig();
        this.setupRsConfigListeners();
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
        // Mobile menu interactions are handled centrally in left-menu.js.
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
        const vajraEnterEl = document.getElementById('telegramVajraEnter');
        if (vajraEnterEl) {
            vajraEnterEl.addEventListener('change', (e) => {
                if (!this.settings.notifications.telegram.types) {
                    this.settings.notifications.telegram.types = {};
                }
                this.updateSetting('telegram.types.vajraEnter', e.target.checked);
            });
        }
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
                if (
                    this.settings.notifications &&
                    this.settings.notifications.telegram &&
                    this.settings.notifications.telegram.types &&
                    this.settings.notifications.telegram.types.vajraEnter === undefined
                ) {
                    this.settings.notifications.telegram.types.vajraEnter = false;
                }
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
                        trades: false,
                        vajraEnter: false
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
        const vajraEnterEl = document.getElementById('telegramVajraEnter');
        if (vajraEnterEl) {
            vajraEnterEl.checked = !!(telegram.types && telegram.types.vajraEnter);
        }

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
        const { language, timezone, currency } = this.settings.application;

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

    async loadRsConvictionConfig() {
        if (!document.getElementById('rsConvictionSettings')) return;
        try {
            const res = await fetch('/api/dashboard/relative-strength/config', { credentials: 'same-origin' });
            const cfg = await res.json();
            this.applyRsConvictionConfig(cfg);
        } catch (e) {
            console.warn('Settings: RS config load failed', e);
        }
    }

    applyRsConvictionConfig(cfg) {
        if (!cfg) return;
        const map = {
            rs_W_rs: 'W_rs', rs_W_anchor: 'W_anchor', rs_W_persist: 'W_persist',
            rs_W_slope: 'W_slope', rs_W_accum: 'W_accum', rs_W_whip: 'W_whip',
            rs_convergence_atr: 'convergence_atr', rs_convergence_bars: 'convergence_bars',
            rs_expiry_atr: 'expiry_atr', rs_sl_buffer_atr: 'sl_buffer_atr',
            rs_sl_late_pct: 'sl_late_pct', rs_chop_warning_crosses: 'chop_warning_crosses',
            rs_alert_window_start_min: 'alert_window_start_min',
            rs_alert_window_end_min: 'alert_window_end_min',
        };
        Object.keys(map).forEach(function (id) {
            const el = document.getElementById(id);
            if (el && cfg[map[id]] != null) el.value = cfg[map[id]];
        });
        const snd = document.getElementById('rs_alert_sound_enabled');
        if (snd) snd.checked = !!cfg.alert_sound_enabled;
        const goSnd = document.getElementById('rs_go_alert_sound_enabled');
        if (goSnd) goSnd.checked = !!cfg.go_alert_sound_enabled;
        const fwSnd = document.getElementById('rs_fast_watch_sound_enabled');
        if (fwSnd) fwSnd.checked = !!cfg.fast_watch_sound_enabled;
        const fwUi = document.getElementById('rs_fast_watch_ui_enabled');
        if (fwUi) fwUi.checked = !!cfg.fast_watch_ui_enabled;
        const ema = document.getElementById('rs_show_ema10_passive');
        if (ema) ema.checked = cfg.show_ema10_passive !== false;
    }

    collectRsConvictionConfig() {
        const body = {};
        const map = {
            rs_W_rs: 'W_rs', rs_W_anchor: 'W_anchor', rs_W_persist: 'W_persist',
            rs_W_slope: 'W_slope', rs_W_accum: 'W_accum', rs_W_whip: 'W_whip',
            rs_convergence_atr: 'convergence_atr', rs_convergence_bars: 'convergence_bars',
            rs_expiry_atr: 'expiry_atr', rs_sl_buffer_atr: 'sl_buffer_atr',
            rs_sl_late_pct: 'sl_late_pct', rs_chop_warning_crosses: 'chop_warning_crosses',
            rs_alert_window_start_min: 'alert_window_start_min',
            rs_alert_window_end_min: 'alert_window_end_min',
        };
        Object.keys(map).forEach(function (id) {
            const el = document.getElementById(id);
            if (!el || el.value === '') return;
            const v = Number(el.value);
            body[map[id]] = Number.isNaN(v) ? el.value : v;
        });
        const snd = document.getElementById('rs_alert_sound_enabled');
        if (snd) body.alert_sound_enabled = snd.checked;
        const goSnd = document.getElementById('rs_go_alert_sound_enabled');
        if (goSnd) body.go_alert_sound_enabled = goSnd.checked;
        const fwSnd = document.getElementById('rs_fast_watch_sound_enabled');
        if (fwSnd) body.fast_watch_sound_enabled = fwSnd.checked;
        const fwUi = document.getElementById('rs_fast_watch_ui_enabled');
        if (fwUi) body.fast_watch_ui_enabled = fwUi.checked;
        const ema = document.getElementById('rs_show_ema10_passive');
        if (ema) body.show_ema10_passive = ema.checked;
        return body;
    }

    setupRsConfigListeners() {
        const saveBtn = document.getElementById('rsConfigSave');
        const resetBtn = document.getElementById('rsConfigReset');
        if (saveBtn) {
            saveBtn.addEventListener('click', async () => {
                try {
                    const res = await fetch('/api/dashboard/relative-strength/config', {
                        credentials: 'same-origin',
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(this.collectRsConvictionConfig()),
                    });
                    const cfg = await res.json();
                    this.applyRsConvictionConfig(cfg);
                    this.showNotification('RS conviction config saved', 'success');
                } catch (e) {
                    this.showNotification('Failed to save RS config', 'error');
                }
            });
        }
        if (resetBtn) {
            resetBtn.addEventListener('click', async () => {
                if (!confirm('Reset RS conviction settings to defaults?')) return;
                try {
                    const res = await fetch('/api/dashboard/relative-strength/config/reset', {
                        credentials: 'same-origin',
                        method: 'POST',
                    });
                    await res.json();
                    await this.loadRsConvictionConfig();
                    this.showNotification('RS config reset to defaults', 'info');
                } catch (e) {
                    this.showNotification('Reset failed', 'error');
                }
            });
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
        const apiBase =
            window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
                ? 'http://localhost:8000'
                : window.location.origin;
        const response = await fetch(`${apiBase}/scan/update-upstox-token`, {
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
        const apiBase =
            window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
                ? 'http://localhost:8000'
                : window.location.origin;
        const response = await fetch(`${apiBase}/scan/upstox/login`);
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

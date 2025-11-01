/**
 * Cache Management Utility for Trade Manthan
 * Provides manual cache management and refresh options
 */

class CacheManager {
    constructor() {
        this.init();
    }
    
    init() {
        // Add cache management UI to the page
        this.addCacheManagementUI();
        
        // Set up cache monitoring
        this.setupCacheMonitoring();
        
        console.log('🔧 Cache Manager initialized');
    }
    
    addCacheManagementUI() {
        // Create cache management button
        const cacheButton = document.createElement('div');
        cacheButton.className = 'cache-management-button';
        cacheButton.innerHTML = `
            <button onclick="cacheManager.showCacheMenu()" title="Cache Management">
                <i class="fas fa-sync-alt"></i>
            </button>
        `;
        
        // Add styles
        const style = document.createElement('style');
        style.textContent = `
            .cache-management-button {
                position: fixed;
                bottom: 20px;
                right: 20px;
                z-index: 9999;
            }
            
            .cache-management-button button {
                background: linear-gradient(135deg, #2196f3, #1976d2);
                color: white;
                border: none;
                border-radius: 50%;
                width: 50px;
                height: 50px;
                cursor: pointer;
                box-shadow: 0 4px 15px rgba(0,0,0,0.3);
                transition: all 0.3s ease;
                font-size: 18px;
            }
            
            .cache-management-button button:hover {
                transform: scale(1.1);
                box-shadow: 0 6px 20px rgba(0,0,0,0.4);
            }
            
            .cache-menu {
                position: fixed;
                bottom: 80px;
                right: 20px;
                background: white;
                border-radius: 10px;
                box-shadow: 0 8px 25px rgba(0,0,0,0.2);
                padding: 15px;
                min-width: 250px;
                z-index: 9998;
                display: none;
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            }
            
            .cache-menu.show {
                display: block;
                animation: slideUp 0.3s ease-out;
            }
            
            @keyframes slideUp {
                from { transform: translateY(20px); opacity: 0; }
                to { transform: translateY(0); opacity: 1; }
            }
            
            .cache-menu h3 {
                margin: 0 0 15px 0;
                color: #1976d2;
                font-size: 16px;
                border-bottom: 2px solid #e3f2fd;
                padding-bottom: 8px;
            }
            
            .cache-menu button {
                background: #f5f5f5;
                border: 1px solid #ddd;
                padding: 8px 12px;
                margin: 5px;
                border-radius: 5px;
                cursor: pointer;
                font-size: 12px;
                transition: all 0.2s ease;
            }
            
            .cache-menu button:hover {
                background: #e3f2fd;
                border-color: #2196f3;
            }
            
            .cache-menu button.danger {
                background: #ffebee;
                border-color: #f44336;
                color: #d32f2f;
            }
            
            .cache-menu button.danger:hover {
                background: #ffcdd2;
            }
            
            .cache-status {
                font-size: 11px;
                color: #666;
                margin-top: 10px;
                padding-top: 10px;
                border-top: 1px solid #eee;
            }
        `;
        document.head.appendChild(style);
        
        // Add to page
        document.body.appendChild(cacheButton);
        
        // Create cache menu
        this.createCacheMenu();
    }
    
    createCacheMenu() {
        const menu = document.createElement('div');
        menu.className = 'cache-menu';
        menu.id = 'cacheMenu';
        
        const versionInfo = window.VERSION_INFO || { version: 'Unknown', cacheBuster: 'Unknown' };
        
        menu.innerHTML = `
            <h3><i class="fas fa-cog"></i> Cache Management</h3>
            
            <button onclick="cacheManager.refreshCurrentPage()">
                <i class="fas fa-sync-alt"></i> Refresh Page
            </button>
            
            <button onclick="cacheManager.refreshAllResources()">
                <i class="fas fa-redo"></i> Refresh All Resources
            </button>
            
            <button onclick="cacheManager.clearBrowserCache()" class="danger">
                <i class="fas fa-trash"></i> Clear Browser Cache
            </button>
            
            <button onclick="cacheManager.forceHardRefresh()" class="danger">
                <i class="fas fa-exclamation-triangle"></i> Force Hard Refresh
            </button>
            
            <div class="cache-status">
                <div>Version: ${versionInfo.version}</div>
                <div>Cache: ${versionInfo.cacheBuster}</div>
                <div>Last: ${new Date().toLocaleTimeString()}</div>
            </div>
        `;
        
        document.body.appendChild(menu);
    }
    
    showCacheMenu() {
        const menu = document.getElementById('cacheMenu');
        if (menu) {
            menu.classList.toggle('show');
            
            // Auto-hide after 10 seconds
            setTimeout(() => {
                menu.classList.remove('show');
            }, 10000);
        }
    }
    
    refreshCurrentPage() {
        console.log('🔄 Refreshing current page...');
        location.reload();
    }
    
    refreshAllResources() {
        console.log('🔄 Refreshing all resources...');
        
        if (window.versionManager) {
            window.versionManager.autoVersionResources();
        }
        
        // Show success message
        this.showNotification('All resources refreshed!', 'success');
    }
    
    clearBrowserCache() {
        console.log('🗑️ Clearing browser cache...');
        
        if ('caches' in window) {
            caches.keys().then(names => {
                names.forEach(name => {
                    caches.delete(name);
                });
                console.log('✅ Browser cache cleared');
                this.showNotification('Browser cache cleared!', 'success');
            });
        } else {
            this.showNotification('Cache API not supported', 'warning');
        }
    }
    
    forceHardRefresh() {
        console.log('💥 Force hard refresh...');
        
        // Clear all possible caches
        if ('caches' in window) {
            caches.keys().then(names => {
                names.forEach(name => {
                    caches.delete(name);
                });
            });
        }
        
        // Clear localStorage and sessionStorage
        localStorage.clear();
        sessionStorage.clear();
        
        // Force reload with cache busting
        window.location.reload(true);
    }
    
    setupCacheMonitoring() {
        // Monitor for cache-related errors
        window.addEventListener('error', (event) => {
            if (event.target.tagName === 'SCRIPT' || event.target.tagName === 'LINK') {
                console.warn('🔄 Resource loading error detected, suggesting cache refresh');
                this.suggestCacheRefresh();
            }
        });
        
        // Monitor for 404 errors on resources
        window.addEventListener('unhandledrejection', (event) => {
            if (event.reason && event.reason.message && event.reason.message.includes('404')) {
                console.warn('🔄 404 error detected, suggesting cache refresh');
                this.suggestCacheRefresh();
            }
        });
    }
    
    suggestCacheRefresh() {
        // Show a subtle suggestion to refresh cache
        const suggestion = document.createElement('div');
        suggestion.className = 'cache-suggestion';
        suggestion.innerHTML = `
            <i class="fas fa-info-circle"></i>
            Resource loading issue detected. 
            <button onclick="cacheManager.refreshAllResources()">Refresh Cache</button>
        `;
        
        // Add styles
        suggestion.style.cssText = `
            position: fixed;
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            background: #fff3cd;
            color: #856404;
            padding: 10px 15px;
            border-radius: 5px;
            border: 1px solid #ffeaa7;
            z-index: 10000;
            font-size: 14px;
            display: flex;
            align-items: center;
            gap: 10px;
        `;
        
        document.body.appendChild(suggestion);
        
        // Auto-remove after 8 seconds
        setTimeout(() => {
            if (suggestion.parentElement) {
                suggestion.remove();
            }
        }, 8000);
    }
    
    showNotification(message, type = 'info') {
        const notification = document.createElement('div');
        notification.className = `cache-notification ${type}`;
        notification.textContent = message;
        
        // Add styles
        notification.style.cssText = `
            position: fixed;
            top: 80px;
            right: 20px;
            background: ${type === 'success' ? '#4caf50' : type === 'warning' ? '#ff9800' : '#2196f3'};
            color: white;
            padding: 10px 15px;
            border-radius: 5px;
            z-index: 10000;
            font-size: 14px;
            animation: slideIn 0.3s ease-out;
        `;
        
        document.body.appendChild(notification);
        
        // Auto-remove after 3 seconds
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 3000);
    }
}

// Initialize cache manager when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        window.cacheManager = new CacheManager();
    });
} else {
    window.cacheManager = new CacheManager();
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CacheManager;
}


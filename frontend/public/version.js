/**
 * Version Management System for Trade Manthan
 * Automatically handles cache busting and version tracking
 */

class VersionManager {
    constructor() {
        this.version = '2025.08.22';
        this.buildDate = new Date().toISOString();
        this.cacheBuster = 20250822190156;
        
        this.init();
    }
    
    init() {
        // Make version info globally accessible
        window.VERSION_INFO = {
            version: this.version,
            buildDate: this.buildDate,
            cacheBuster: this.cacheBuster
        };
        
        // Add version info to page title
        this.updatePageTitle();
        
        // Add version info to console
        this.logVersionInfo();
        
        // Log strategy manager version
        this.logStrategyManagerVersion();
        
        // Set up automatic cache refresh mechanism
        this.setupCacheRefresh();
    }
    
    updatePageTitle() {
        const title = document.title;
        if (!title.includes('v' + this.version)) {
            document.title = title + ' v' + this.version;
        }
    }
    
    logVersionInfo() {
        console.log('🚀 Trade Manthan v' + this.version);
        console.log('📅 Build Date:', this.buildDate);
        console.log('🔄 Cache Buster:', this.cacheBuster);
        console.log('🌐 Environment:', window.location.hostname);
    }
    
    logStrategyManagerVersion() {
        // Wait for StrategyManager to be available
        setTimeout(() => {
            if (window.StrategyManager) {
                console.log('📋 Strategy Manager v' + StrategyManager.VERSION);
                console.log('✨ Features:', StrategyManager.FEATURES.join(', '));
                console.log('📅 Build Date:', StrategyManager.BUILD_DATE);
            } else if (window.strategyManager) {
                console.log('📋 Strategy Manager v' + strategyManager.constructor.VERSION);
            }
        }, 1000);
    }
    
    setupCacheRefresh() {
        // Check for version mismatch every 5 minutes
        setInterval(() => {
            this.checkForUpdates();
        }, 5 * 60 * 1000);
        
        // Listen for visibility change to check for updates when user returns to tab
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden) {
                this.checkForUpdates();
            }
        });
    }
    
    checkForUpdates() {
        // Fetch version info from server to check for updates
        fetch('/version.json?t=' + this.cacheBuster)
            .then(response => response.json())
            .then(data => {
                if (data.version !== this.version) {
                    this.notifyUpdate(data.version);
                }
            })
            .catch(error => {
                // Silently fail - version check is not critical
                console.debug('Version check failed:', error.message);
            });
    }
    
    notifyUpdate(newVersion) {
        console.log('🔄 New version available:', newVersion);
        
        // Show update notification
        this.showUpdateNotification(newVersion);
    }
    
    showUpdateNotification(newVersion) {
        // Create update notification
        const notification = document.createElement('div');
        notification.className = 'update-notification';
        notification.innerHTML = `
            <div class="update-content">
                <i class="fas fa-sync-alt"></i>
                <span>New version ${newVersion} available!</span>
                <button onclick="location.reload(true)">Update Now</button>
                <button onclick="this.parentElement.parentElement.remove()">Later</button>
            </div>
        `;
        
        // Add styles
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: linear-gradient(135deg, #4caf50, #45a049);
            color: white;
            padding: 15px 20px;
            border-radius: 10px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            z-index: 10000;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            animation: slideIn 0.3s ease-out;
        `;
        
        // Add animation styles
        const style = document.createElement('style');
        style.textContent = `
            @keyframes slideIn {
                from { transform: translateX(100%); opacity: 0; }
                to { transform: translateX(0); opacity: 1; }
            }
            .update-content {
                display: flex;
                align-items: center;
                gap: 10px;
            }
            .update-content button {
                background: rgba(255,255,255,0.2);
                border: 1px solid rgba(255,255,255,0.3);
                color: white;
                padding: 5px 10px;
                border-radius: 5px;
                cursor: pointer;
                font-size: 12px;
            }
            .update-content button:hover {
                background: rgba(255,255,255,0.3);
            }
        `;
        document.head.appendChild(style);
        
        // Add to page
        document.body.appendChild(notification);
        
        // Auto-remove after 10 seconds
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 10000);
    }
    
    // Get cache-busted URL for any resource
    getCacheBustedUrl(url) {
        if (url.includes('?')) {
            return url + '&v=' + this.cacheBuster;
        } else {
            return url + '?v=' + this.cacheBuster;
        }
    }
    
    // Force refresh all resources
    forceRefresh() {
        console.log('🔄 Force refreshing all resources...');
        
        // Clear any cached data
        if ('caches' in window) {
            caches.keys().then(names => {
                names.forEach(name => {
                    caches.delete(name);
                });
            });
        }
        
        // Reload page with cache busting
        window.location.reload(true);
    }
    
    // Get current version info
    getVersionInfo() {
        return {
            version: this.version,
            buildDate: this.buildDate,
            cacheBuster: this.cacheBuster
        };
    }
    
    // Auto-version all resources
    autoVersionResources() {
        console.log('🔄 Auto-versioning resources...');
        
        // Version all script tags
        const scripts = document.querySelectorAll('script[src]');
        scripts.forEach(script => {
            if (script.src && !script.src.includes('?v=') && !script.src.includes('cdnjs.cloudflare.com')) {
                const newSrc = this.getCacheBustedUrl(script.src);
                console.log('📝 Versioning script:', script.src, '→', newSrc);
                script.src = newSrc;
            }
        });
        
        // Version all link tags (CSS)
        const links = document.querySelectorAll('link[href]');
        links.forEach(link => {
            if (link.href && !link.href.includes('?v=') && !link.href.includes('cdnjs.cloudflare.com')) {
                const newHref = this.getCacheBustedUrl(link.href);
                console.log('📝 Versioning stylesheet:', link.href, '→', newHref);
                link.href = newHref;
            }
        });
        
        // Version all img tags
        const images = document.querySelectorAll('img[src]');
        images.forEach(img => {
            if (img.src && !img.src.includes('?v=') && !img.src.includes('logo.jpeg')) {
                const newSrc = this.getCacheBustedUrl(img.src);
                console.log('📝 Versioning image:', img.src, '→', newSrc);
                img.src = newSrc;
            }
        });
        
        console.log('✅ Resource versioning complete');
    }
    
    // Force refresh specific resource
    refreshResource(resourcePath) {
        const resource = document.querySelector(`[src="${resourcePath}"], [href="${resourcePath}"]`);
        if (resource) {
            const newUrl = this.getCacheBustedUrl(resourcePath);
            if (resource.src) {
                resource.src = newUrl;
            } else if (resource.href) {
                resource.href = newUrl;
            }
            console.log('🔄 Refreshed resource:', resourcePath);
        }
    }
}

// Initialize version manager when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        window.versionManager = new VersionManager();
    });
} else {
    window.versionManager = new VersionManager();
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = VersionManager;
}

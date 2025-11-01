// Global authentication state
let isAuthenticating = false;
let hasRedirected = false;
let isAuthenticated = false;

// Left Menu Module
class LeftMenu {
    constructor() {
        this.currentPage = this.getCurrentPage();
        this.isAuthenticated = false;
        this.init();
    }

    getCurrentPage() {
        const path = window.location.pathname;
        if (path.includes('dashboard')) return 'dashboard';
        if (path.includes('strategy')) return 'strategy';
        if (path.includes('broker')) return 'broker';
        if (path.includes('algo')) return 'algo';
        if (path.includes('scan')) return 'scan';
        if (path.includes('settings')) return 'settings';
        if (path.includes('reports')) return 'reports';
        return 'dashboard';
    }

    init() {
        console.log('LeftMenu: Initializing...');
        
        // Prevent multiple authentication checks
        if (isAuthenticating) {
            console.log('LeftMenu: Authentication already in progress, skipping...');
            return;
        }
        
        isAuthenticating = true;
        
        // Add a small delay to ensure DOM is fully ready
        setTimeout(() => {
            // First check authentication before doing anything
            if (this.checkAuthentication()) {
                console.log('LeftMenu: Authentication successful, loading menu...');
                this.isAuthenticated = true;
                isAuthenticated = true; // Set global flag
                this.loadLeftMenuHTML();
                this.setupMobileMenu();
                this.loadUserData();
                this.setupNavigation();
                this.setActiveNavigation();
                console.log('LeftMenu: Initialization complete');
            } else {
                console.log('LeftMenu: Authentication failed');
                console.log('LeftMenu: Current path:', window.location.pathname);
                console.log('LeftMenu: Stored token:', localStorage.getItem('trademanthan_token'));
                console.log('LeftMenu: Stored user:', localStorage.getItem('trademanthan_user'));
                
                // Only redirect if we're on a protected page and haven't redirected yet
                const currentPath = window.location.pathname;
                const isProtectedPage = currentPath.includes('dashboard') || 
                                      currentPath.includes('strategy') || 
                                      currentPath.includes('broker') || 
                                      currentPath.includes('algo') ||
                                      currentPath.includes('scan') ||
                                      currentPath.includes('reports') ||
                                      currentPath.includes('settings');
                
                if (!hasRedirected && isProtectedPage) {
                    hasRedirected = true;
                    console.log('LeftMenu: Redirecting to login page');
                    window.location.replace('index.html');
                }
            }
            isAuthenticating = false;
        }, 100);
    }

    checkAuthentication() {
        console.log('LeftMenu: Checking authentication...');
        
        // If we're already authenticated globally, skip the check
        if (isAuthenticated) {
            console.log('LeftMenu: Already authenticated globally, skipping check');
            return true;
        }
        
        try {
            const token = localStorage.getItem('trademanthan_token');
            console.log('LeftMenu: Token found:', token);
            
            if (!token) {
                console.log('LeftMenu: No token found');
                return false;
            }

            // Check if token is valid (JWT from backend, Google OAuth fallback, or email token)
            if (!token.startsWith('google_token_') && !token.startsWith('email_token_') && !token.includes('.')) {
                console.log('LeftMenu: Invalid token format');
                localStorage.removeItem('trademanthan_token');
                return false;
            }

            // Check if user data exists
            const userData = localStorage.getItem('trademanthan_user');
            if (!userData) {
                console.log('LeftMenu: No user data found');
                return false;
            }

            try {
                const user = JSON.parse(userData);
                console.log('LeftMenu: User data valid:', user);
                
                // Additional validation: ensure user has required fields
                if (!user.email || !user.name) {
                    console.log('LeftMenu: User data missing required fields');
                    return false;
                }
                
                return true;
            } catch (error) {
                console.error('LeftMenu: Error parsing user data:', error);
                localStorage.removeItem('trademanthan_user');
                localStorage.removeItem('trademanthan_token');
                return false;
            }
        } catch (error) {
            console.error('LeftMenu: Unexpected error during authentication check:', error);
            return false;
        }
    }

    loadLeftMenuHTML() {
        const container = document.getElementById('left-menu-container');
        if (container) {
            container.innerHTML = `
                <!-- Left Panel Menu -->
                <aside class="left-panel">
                    <div class="panel-header">
                        <img src="./logo.jpg" alt="Trade Manthan Logo" class="logo-image">
                        <h2 class="logo-title">
                            <span class="title-trade">Trade</span>
                            <span class="title-manthan">Manthan</span>
                        </h2>
                    </div>
                    
                    <nav class="panel-nav">
                        <ul class="nav-list">
                            <li class="nav-item" data-section="dashboard">
                                <i class="fas fa-chart-line"></i>
                                <span>Dashboard</span>
                            </li>
                            <li class="nav-item" data-section="algo">
                                <i class="fas fa-brain"></i>
                                <span>Algorithmic Trading</span>
                            </li>
                            <li class="nav-item" data-section="broker">
                                <i class="fas fa-university"></i>
                                <span>Broker Management</span>
                            </li>
                            <li class="nav-item" data-section="strategy">
                                <i class="fas fa-robot"></i>
                                <span>Strategy Management</span>
                            </li>
                            <li class="nav-item" data-section="scan">
                                <i class="fas fa-radar"></i>
                                <span>Chartink Scans</span>
                            </li>
                            <li class="nav-item" data-section="settings">
                                <i class="fas fa-cog"></i>
                                <span>Settings</span>
                            </li>
                            <li class="nav-item" data-section="reports">
                                <i class="fas fa-chart-bar"></i>
                                <span>Reports</span>
                            </li>
                        </ul>
                    </nav>
                    
                    <div class="panel-footer">
                        <div class="user-info">
                            <img src="https://via.placeholder.com/40" alt="User Avatar" class="user-avatar" id="userAvatar">
                            <div class="user-details">
                                <span class="user-name" id="userName">Demo User</span>
                                <span class="user-email" id="userEmail">demo@trademanthan.com</span>
                            </div>
                        </div>
                        <button class="logout-btn" onclick="logout()">
                            <i class="fas fa-sign-out-alt"></i>
                            <span>Logout</span>
                        </button>
                    </div>
                </aside>

                <!-- Mobile Hamburger Menu -->
                <div class="mobile-menu-toggle" id="mobileMenuToggle">
                    <i class="fas fa-bars"></i>
                </div>
            `;
            console.log('LeftMenu: HTML loaded successfully');
        } else {
            console.error('LeftMenu: Container not found');
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

    loadUserData() {
        const userData = localStorage.getItem('trademanthan_user');
        if (userData) {
            try {
                const user = JSON.parse(userData);
                const userNameElement = document.getElementById('userName');
                const userEmailElement = document.getElementById('userEmail');
                const userAvatarElement = document.getElementById('userAvatar');
                
                if (userNameElement) userNameElement.textContent = user.name || 'User';
                if (userEmailElement) userEmailElement.textContent = user.email || 'user@example.com';
                if (userAvatarElement && user.picture) userAvatarElement.src = user.picture;
            } catch (error) {
                console.error('Error loading user data:', error);
            }
        }
    }

    setupNavigation() {
        const navItems = document.querySelectorAll('.nav-item');
        navItems.forEach(item => {
            item.addEventListener('click', function() {
                const section = this.dataset.section;
                console.log('LeftMenu: Navigation clicked for section:', section);
                
                // Store current authentication state before navigation
                const currentUser = localStorage.getItem('trademanthan_user');
                const currentToken = localStorage.getItem('trademanthan_token');
                
                if (!currentUser || !currentToken) {
                    console.error('LeftMenu: No authentication data found, cannot navigate');
                    return;
                }
                
                // Navigate to the appropriate page
                let targetPage = '';
                switch (section) {
                    case 'dashboard':
                        targetPage = 'dashboard.html';
                        break;
                    case 'strategy':
                        targetPage = 'strategy.html';
                        break;
                    case 'broker':
                        targetPage = 'broker.html';
                        break;
                    case 'algo':
                        targetPage = 'algo.html';
                        break;
                    case 'scan':
                        targetPage = 'scan.html';
                        break;
                    case 'settings':
                        targetPage = 'settings.html';
                        break;
                    case 'reports':
                        targetPage = 'reports.html';
                        break;
                    default:
                        console.error('LeftMenu: Unknown section:', section);
                        return;
                }
                
                console.log('LeftMenu: Navigating to:', targetPage);
                
                // Use replace to avoid adding to browser history
                try {
                    window.location.replace(targetPage);
                } catch (e) {
                    console.error('LeftMenu: Navigation error:', e);
                    // Fallback to href
                    window.location.href = targetPage;
                }
            });
        });
    }

    setActiveNavigation() {
        const navItems = document.querySelectorAll('.nav-item');
        navItems.forEach(item => {
            if (item.dataset.section === this.currentPage) {
                item.classList.add('active');
            } else {
                item.classList.remove('active');
            }
        });
    }

    static logout() {
        console.log("Logging out...");
        
        // Clear user data and token
        localStorage.removeItem('trademanthan_user');
        localStorage.removeItem('trademanthan_token');

        // Redirect to main page
        window.location.href = 'index.html';
    }
}

// Global logout function
function logout() {
    LeftMenu.logout();
}

// Initialize left menu when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded, initializing LeftMenu...');
    new LeftMenu();
});

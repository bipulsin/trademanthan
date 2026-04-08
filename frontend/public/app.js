// Email/Password login form handler
document.addEventListener('DOMContentLoaded', function() {
    // Add console welcome message
    console.log('🚀 Trade Manthan Platform Loaded Successfully!');
    console.log('📊 Professional Algo Trading Platform');
    console.log('🔗 Frontend: HTML/CSS/JS with blue-black gradient theme');
    console.log('⚡ Backend: FastAPI with Python');
    console.log('🗄️ Database: PostgreSQL (production)');
    console.log('🌐 Domain: https://www.tradewithcto.com (production)');
    
    // Initialize feature card animations
    initializeFeatureCards();
    
    // Initialize email login form
    initializeEmailLogin();
});

// Initialize email login form
function initializeEmailLogin() {
    const emailLoginForm = document.getElementById('emailLoginForm');
    if (emailLoginForm) {
        emailLoginForm.addEventListener('submit', handleEmailLogin);
    }
}

// Handle email/password login
function handleEmailLogin(event) {
    event.preventDefault();
    
    const email = document.getElementById('loginEmail').value;
    const password = document.getElementById('loginPassword').value;
    
    if (!email || !password) {
        alert('Please enter both email and password');
        return;
    }
    
    // Email/password flow is not enabled in production for secured session
    alert('Email/password login is currently disabled. Please use Google login.');
    return;

    // Show loading state
    const submitBtn = event.target.querySelector('button[type="submit"]');
    const originalText = submitBtn.textContent;
    submitBtn.textContent = "🔄 Signing in...";
    submitBtn.disabled = true;
    
    // Simulate API call delay
    setTimeout(() => {
        // Here you would typically make an API call to your backend
        console.log('Attempting login with:', { email, password });
        
        // For now, simulate successful login
        simulateLogin({
            email: email,
            name: email.split('@')[0], // Use email prefix as name
            picture: "https://via.placeholder.com/150"
        });
        
        // Reset button
        submitBtn.textContent = originalText;
        submitBtn.disabled = false;
        
    }, 1500);
}

// Feature card animations
function initializeFeatureCards() {
    const featureCards = document.querySelectorAll('.feature-card');
    
    featureCards.forEach((card, index) => {
        // Add loading animation
        card.style.opacity = '0';
        card.style.transform = 'translateY(20px)';
        
        // Staggered animation
        setTimeout(() => {
            card.style.transition = 'all 0.6s ease';
            card.style.opacity = '1';
            card.style.transform = 'translateY(0)';
        }, index * 200);
        
        // Interactive hover effects
        card.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-10px) scale(1.02)';
            this.style.boxShadow = '0 20px 40px rgba(25, 118, 210, 0.2)';
        });
        
        card.addEventListener('mouseleave', function() {
            this.style.transform = 'translateY(0) scale(1)';
            this.style.boxShadow = '0 8px 32px rgba(0, 0, 0, 0.1)';
        });
    });
}

// Google OAuth client ID
const GOOGLE_CLIENT_ID = '428560418671-t59riis4gqkhavnevt9ve6km54ltsba7.apps.googleusercontent.com';
// API: same origin as the page (canonical www.tradewithcto.com).
const API_BASE_URL = (function () {
    const h = window.location.hostname;
    if (h === 'localhost' || h === '127.0.0.1') return 'http://localhost:8000';
    return window.location.origin;
})();

/** Do not JSON-parse HTML error pages (nginx 502 returns <!DOCTYPE...>). */
async function parseLoginApiJson(res) {
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (!res.ok) {
        const text = await res.text().catch(() => '');
        let msg = 'HTTP ' + res.status;
        if (res.status === 502 || res.status === 503 || res.status === 504) {
            msg +=
                ' — login API temporarily unavailable (server busy or restarting). Try https://www.tradewithcto.com/login.html or retry in a minute.';
        } else if (text && text.trim().charAt(0) !== '<') {
            try {
                const j = JSON.parse(text);
                msg = (j.detail || j.message || msg);
            } catch (e) {
                msg += ': ' + text.trim().slice(0, 200);
            }
        }
        throw new Error(msg);
    }
    if (!ct.includes('application/json')) {
        await res.text();
        throw new Error(
            'Server returned non-JSON. Try https://www.tradewithcto.com/login.html if this persists.'
        );
    }
    return res.json();
}

// Detect mobile/touch device (Google button in popup often fails on mobile - popup blocking, touch issues)
function isMobileView() {
    return window.matchMedia('(max-width: 768px)').matches ||
           /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) ||
           ('ontouchstart' in window);
}

// Render Google Sign-In button when popup is visible (fixes mobile - button in hidden div gets 0 height)
function renderGoogleButton() {
    const container = document.getElementById('googleSigninContainer');
    const fallback = document.getElementById('mobileGoogleFallback');
    if (!container) return;

    // On mobile: skip Google button in popup, show link to full-page login (works reliably)
    if (isMobileView() && fallback) {
        container.innerHTML = '';
        container.style.display = 'none';
        fallback.style.display = 'flex';
        return;
    }

    function doRender() {
        if (typeof google === 'undefined' || !google.accounts || !google.accounts.id) {
            setTimeout(doRender, 100);
            return;
        }
        try {
            google.accounts.id.initialize({
                client_id: GOOGLE_CLIENT_ID,
                callback: handleCredentialResponse,
                auto_select: false,
                cancel_on_tap_outside: false
            });
            container.innerHTML = '';
            container.style.display = 'block';
            const w = Math.max(container.offsetWidth || 0, 280);
            google.accounts.id.renderButton(container, {
                type: 'standard',
                size: 'large',
                theme: 'outline',
                text: 'sign_in_with',
                shape: 'rectangular',
                logo_alignment: 'left',
                width: w
            });
            if (fallback) fallback.style.display = 'none';
        } catch (e) {
            console.warn('Google button render failed, showing fallback:', e);
            if (fallback) {
                container.style.display = 'none';
                fallback.style.display = 'flex';
            }
        }
    }
    doRender();
}

// Login popup functionality
function openLoginPopup() {
    const overlay = document.getElementById('loginOverlay');
    overlay.style.display = 'flex';
    overlay.style.animation = 'slideIn 0.3s ease-out';

    // Render Google button when popup is visible (critical for mobile - fixes 0-height iframe)
    setTimeout(renderGoogleButton, 50);
}

function closeLoginPopup() {
    const overlay = document.getElementById('loginOverlay');
    overlay.style.animation = 'slideOut 0.3s ease-in forwards';
    
    setTimeout(() => {
        overlay.style.display = 'none';
    }, 300);
}

// Google OAuth callback function
async function handleCredentialResponse(response) {
    console.log("=== GOOGLE OAUTH CALLBACK EXECUTED ===");
    console.log("Google OAuth response received");
    console.log("Response object:", response);
    console.log("Function called at:", new Date().toISOString());
    
    if (!response || !response.credential) {
        console.error("Invalid Google OAuth response");
        alert("Login failed. Please try again.");
        return;
    }
    
    // Decode the JWT token to get user information
    try {
        const payload = JSON.parse(atob(response.credential.split('.')[1]));
        console.log("Decoded user info:", payload);
        
        // Extract user information from the JWT payload
        const userData = {
            email: payload.email,
            name: payload.name || (payload.given_name && payload.family_name ? payload.given_name + ' ' + payload.family_name : 'User'),
            picture: payload.picture || 'https://via.placeholder.com/150',
            sub: payload.sub // Google's unique user ID
        };
        
        console.log("User data extracted:", userData);
        
        // Call backend API to create/authenticate user
        try {
            console.log("Calling backend API to authenticate user...");
            
            // For now, we'll use a simplified approach since the backend expects a different flow
            // In production, you'd want to implement the full OAuth flow with the backend
            async function doAuthRequest(path) {
                return fetch(`${API_BASE_URL}${path}`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        google_id: userData.sub,
                        email: userData.email,
                        name: userData.name,
                        picture: userData.picture,
                        credential: response.credential
                    })
                });
            }

            let apiResponse = await doAuthRequest('/api/auth/google-verify');
            if (!apiResponse.ok && [502, 503, 504].includes(apiResponse.status)) {
                apiResponse = await doAuthRequest('/auth/google-verify');
            }

            const authResult = await parseLoginApiJson(apiResponse);
            console.log("Backend authentication successful:", authResult);

            const token = authResult && authResult.access_token;
            if (!token || !String(token).includes('.')) {
                throw new Error(
                    (authResult && authResult.detail) ||
                        'Backend did not return a valid session token. Check server logs and GOOGLE_CLIENT_ID / database.'
                );
            }
            localStorage.setItem('trademanthan_user', JSON.stringify(authResult.user || {}));
            localStorage.setItem('trademanthan_token', token);

            console.log("User authenticated and data stored");
        } catch (apiError) {
            console.error("Backend API authentication failed:", apiError);
            const msg = (apiError && apiError.message) ? apiError.message : String(apiError);
            alert('Login failed: ' + msg);
            return;
        }
        
        // Close the login popup
        closeLoginPopup();
        
        // Show success message briefly
        showLoginSuccess();
        
        // Redirect to dashboard after a short delay
        setTimeout(() => {
            console.log("Redirecting to dashboard...");
            console.log("Current location:", window.location.href);
            console.log("Stored user data:", localStorage.getItem('trademanthan_user'));
            console.log("Stored token:", localStorage.getItem('trademanthan_token'));
            
            // Force a hard redirect to dashboard
            try {
                window.location.replace('dashboard.html');
            } catch (redirectError) {
                console.error("Redirect error:", redirectError);
                // Fallback to href if replace fails
                window.location.href = 'dashboard.html';
            }
        }, 1500);
        
    } catch (error) {
        console.error("Error processing Google OAuth response:", error);
        console.error("Response credential:", response.credential);
        alert("Login failed. Please try again. Error: " + error.message);
    }
}

// Instagram login handler
function handleInstagramLogin() {
    // For now, show a simple alert. In production, you'd implement Instagram OAuth
    alert('Instagram login functionality will be implemented here. Please use Google login or email/password for now.');
}

// Ensure the function is globally accessible
window.handleCredentialResponse = handleCredentialResponse;
window.handleInstagramLogin = handleInstagramLogin;

// Sign up form handler
function showSignupForm() {
    // For now, show a simple alert. In production, you'd show a signup form
    alert('Sign up functionality will be implemented here. Please contact support for account creation.');
}

// Forgot password handler
function showForgotPassword() {
    // For now, show a simple alert. In production, you'd show a password reset form
    alert('Password reset functionality will be implemented here. Please contact support for password assistance.');
}

// Simulate login process
function simulateLogin(userData) {
        // Store user data in localStorage (in production, use secure tokens)
        localStorage.setItem('trademanthan_user', JSON.stringify(userData));
    localStorage.setItem('trademanthan_token', 'email_token_' + Date.now());
        
        // Show success message
    const submitBtn = document.querySelector('#emailLoginForm button[type="submit"]');
    if (submitBtn) {
        submitBtn.textContent = "✅ Login Successful!";
        submitBtn.style.background = "linear-gradient(135deg, #4caf50 0%, #45a049 100%)";
    }
        
        // Redirect to dashboard after a short delay
        setTimeout(() => {
        // Force a hard redirect to dashboard
        window.location.replace('dashboard.html');
        }, 1000);
}

// Show login success message
function showLoginSuccess() {
    // Create a temporary success message
    const successDiv = document.createElement('div');
    successDiv.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: linear-gradient(135deg, #4caf50 0%, #45a049 100%);
        color: white;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        z-index: 10000;
        font-weight: 500;
    `;
    successDiv.textContent = "✅ Login Successful! Redirecting to dashboard...";
    
    document.body.appendChild(successDiv);
    
    // Remove the message after 3 seconds
    setTimeout(() => {
        if (successDiv.parentNode) {
            successDiv.parentNode.removeChild(successDiv);
        }
    }, 3000);
}

// Utility functions
// scrollToFeatures function removed - no longer needed

// Add slideOut animation to CSS
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateY(-20px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes slideOut {
        from {
            opacity: 1;
            transform: translateY(0);
        }
        to {
            opacity: 0;
            transform: translateY(-20px);
        }
    }
    
    .btn-large {
        padding: 1rem 2rem;
        font-size: 1.1rem;
    }
    
    .footer {
        background: rgba(0, 0, 0, 0.3);
        padding: 2rem 0;
        text-align: center;
        margin-top: 4rem;
    }
    
    .footer p {
        color: rgba(255, 255, 255, 0.7);
    }
`;
document.head.appendChild(style);

// Handle escape key to close popup
document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
        closeLoginPopup();
    }
});

// Close popup when clicking outside
document.addEventListener('click', function(event) {
    const overlay = document.getElementById('loginOverlay');
    const popup = document.querySelector('.login-popup');
    
    if (event.target === overlay) {
        closeLoginPopup();
    }
});

// Google button styling - use CSS instead of JavaScript
function styleGoogleButton() {
    const googleButton = document.querySelector('.g_id_signin');
    if (googleButton) {
        // Add custom CSS class for styling
        googleButton.classList.add('google-signin-custom');
        console.log('Google button styled with custom CSS');
    }
}

// Call this function after Google button loads
setTimeout(styleGoogleButton, 1000);
setTimeout(styleGoogleButton, 2000);

// openLoginPopup is defined above - duplicate removed

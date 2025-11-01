// Google OAuth callback function
function handleCredentialResponse(response) {
    console.log("Google OAuth response received");
    console.log("Response:", response);
    
    // Check if response is valid
    if (!response || !response.credential) {
        console.error('Invalid OAuth response:', response);
        alert('OAuth login failed: Invalid response from Google');
        return;
    }
    
    // Send the JWT credential to our backend
    const credential = response.credential;
    
    // Show loading state
    const googleButton = document.querySelector('.g_id_signin');
    if (googleButton) {
        googleButton.style.opacity = '0.6';
        googleButton.style.pointerEvents = 'none';
    }
    
    // Send to backend for verification
    fetch('/api/auth/google', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ credential: credential })
    })
    .then(response => {
        console.log('Backend response status:', response.status);
        return response.json();
    })
    .then(data => {
        console.log('Backend response data:', data);
        if (data.access_token) {
            // Store the access token
            localStorage.setItem('trademanthan_token', data.access_token);
            localStorage.setItem('trademanthan_user', JSON.stringify(data.user));
            
            // Show success message
            alert('Login successful! Redirecting...');
            
            // Redirect to dashboard
            window.location.href = '/dashboard.html';
        } else {
            console.error('Login failed:', data.detail);
            alert('Login failed: ' + (data.detail || 'Unknown error'));
        }
    })
    .catch(error => {
        console.error('Error:', error);
        alert('Login failed: ' + error.message);
    })
    .finally(() => {
        // Reset button state
        if (googleButton) {
            googleButton.style.opacity = '1';
            googleButton.style.pointerEvents = 'auto';
        }
    });
}

// Demo login function for development
function demoLogin() {
    console.log("Demo login clicked");
    
    // Simulate successful login with demo data
    simulateLogin({
        email: "demo@trademanthan.com",
        name: "Demo Trader",
        picture: "https://via.placeholder.com/150"
    });
}

// Simulate login process
function simulateLogin(userData) {
    // Show loading state
    const demoBtn = document.querySelector('.btn-demo');
    const originalText = demoBtn.textContent;
    demoBtn.textContent = "🔄 Logging in...";
    demoBtn.disabled = true;
    
    // Simulate API call delay
    setTimeout(() => {
        // Store user data in localStorage (in production, use secure tokens)
        localStorage.setItem('trademanthan_user', JSON.stringify(userData));
        localStorage.setItem('trademanthan_token', 'demo_token_' + Date.now());
        
        // Show success message
        demoBtn.textContent = "✅ Login Successful!";
        demoBtn.style.background = "linear-gradient(135deg, #4caf50 0%, #45a049 100%)";
        
        // Redirect to dashboard after a short delay
        setTimeout(() => {
            window.location.href = 'dashboard.html';
        }, 1000);
        
    }, 1500);
}

// Close login popup
function closeLogin() {
    const overlay = document.getElementById('loginOverlay');
    overlay.style.animation = 'slideOut 0.3s ease-in forwards';
    
    setTimeout(() => {
        // In a real app, you might want to redirect back to the landing page
        // For now, we'll just hide the popup
        overlay.style.display = 'none';
    }, 300);
}

// Add slideOut animation to CSS
const style = document.createElement('style');
style.textContent = `
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
`;
document.head.appendChild(style);

// Mobile Chrome OAuth fallback function
function initiateGoogleAuth() {
    console.log('🔄 Initiating Google Auth via fallback method');
    
    // Try to trigger the Google OAuth popup manually
    if (typeof google !== 'undefined' && google.accounts) {
        try {
            google.accounts.oauth2.initCodeClient({
                client_id: "822255471884-ihvqhttvtnqjfqtukq1c9msi4n3qjad5.apps.googleusercontent.com",
                scope: 'email profile',
                ux_mode: 'popup',
                callback: handleGoogleAuthResponse
            }).requestCode();
        } catch (error) {
            console.error('Google OAuth error:', error);
            alert('Google OAuth failed. Please try the demo login or refresh the page.');
        }
    } else {
        console.error('Google OAuth not loaded');
        alert('Google OAuth not available. Please try the demo login.');
    }
}

// Handle Google OAuth response from popup method
function handleGoogleAuthResponse(response) {
    console.log('Google OAuth popup response:', response);
    
    if (response.code) {
        // Exchange code for credential
        fetch('/api/auth/google-code', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ code: response.code })
        })
        .then(response => response.json())
        .then(data => {
            if (data.access_token) {
                localStorage.setItem('trademanthan_token', data.access_token);
                localStorage.setItem('trademanthan_user', JSON.stringify(data.user));
                alert('Login successful! Redirecting...');
                window.location.href = '/dashboard.html';
            } else {
                console.error('Login failed:', data.detail);
                alert('Login failed: ' + (data.detail || 'Unknown error'));
            }
        })
        .catch(error => {
            console.error('Error:', error);
            alert('Login failed: ' + error.message);
        });
    } else {
        console.error('No authorization code received');
        alert('Google OAuth failed. Please try again.');
    }
}

// Initialize login popup
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Trade Manthan Login Page Loaded');
    console.log('📱 Google OAuth integration ready');
    console.log('🔑 Demo login available for development');
    
    // Mobile-specific initialization
    const isMobile = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
    const isChromeMobile = /Chrome/i.test(navigator.userAgent) && isMobile;
    console.log('📱 Mobile device detected:', isMobile);
    console.log('🌐 Chrome Mobile detected:', isChromeMobile);
    
    // Check if user is already logged in
    const userToken = localStorage.getItem('trademanthan_token');
    if (userToken) {
        console.log('User already logged in, redirecting to dashboard...');
        setTimeout(() => {
            window.location.href = 'dashboard.html';
        }, 1000);
    }
    
    // Wait for Google OAuth to load
    const checkGoogleOAuth = () => {
        if (typeof google !== 'undefined' && google.accounts) {
            console.log('✅ Google OAuth loaded successfully');
            
            // Initialize with mobile-specific settings
            google.accounts.id.initialize({
                client_id: "822255471884-ihvqhttvtnqjfqtukq1c9msi4n3qjad5.apps.googleusercontent.com",
                callback: handleCredentialResponse,
                auto_select: false,
                cancel_on_tap_outside: false
            });
            
            // Render the button with mobile-optimized settings
            google.accounts.id.renderButton(
                document.querySelector('.g_id_signin'),
                {
                    type: 'standard',
                    size: 'large',
                    theme: 'outline',
                    text: 'sign_in_with',
                    shape: 'rectangular',
                    logo_alignment: 'left',
                    width: '100%'
                }
            );
            
            // Check if button is clickable after a delay
            setTimeout(() => {
                const googleButton = document.querySelector('.g_id_signin iframe');
                if (googleButton) {
                    console.log('🔍 Checking Google OAuth button clickability...');
                    
                    // Test if button is clickable
                    const testClick = () => {
                        try {
                            googleButton.click();
                            console.log('✅ Google OAuth button is clickable');
                        } catch (error) {
                            console.warn('⚠️ Google OAuth button not clickable, showing fallback');
                            showMobileFallback();
                        }
                    };
                    
                    // Test after a short delay
                    setTimeout(testClick, 1000);
                } else {
                    console.warn('⚠️ Google OAuth button not found, showing fallback');
                    showMobileFallback();
                }
            }, 2000);
            
        } else {
            console.log('⏳ Waiting for Google OAuth to load...');
            setTimeout(checkGoogleOAuth, 100);
        }
    };
    
    // Show mobile fallback button
    function showMobileFallback() {
        const mobileBtn = document.getElementById('mobile-google-btn');
        const googleContainer = document.getElementById('google-signin-container');
        
        if (mobileBtn && googleContainer) {
            mobileBtn.style.display = 'block';
            googleContainer.style.display = 'none';
            console.log('📱 Mobile fallback button activated');
        }
    }
    
    // Initialize OAuth
    checkGoogleOAuth();
    
    // For Chrome Mobile, show fallback immediately if OAuth fails
    if (isChromeMobile) {
        setTimeout(() => {
            const googleButton = document.querySelector('.g_id_signin iframe');
            if (!googleButton || googleButton.offsetHeight === 0) {
                console.log('🔧 Chrome Mobile: Activating fallback button');
                showMobileFallback();
            }
        }, 3000);
    }
});

// Handle escape key to close popup
document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape') {
        closeLogin();
    }
});

// Close popup when clicking outside
document.addEventListener('click', function(event) {
    const overlay = document.getElementById('loginOverlay');
    const popup = document.querySelector('.login-popup');
    
    if (event.target === overlay) {
        closeLogin();
    }
});

/** Same backend as app.js — mirror domains must not use relative /api (nginx 502). */
function trademanthanApiBase() {
    const h = window.location.hostname;
    if (h === 'localhost' || h === '127.0.0.1') return 'http://localhost:8000';
    if (
        h === 'www.tradewithcto.com' ||
        h === 'tradewithcto.com' ||
        h.endsWith('.tradewithcto.com') ||
        h === 'www.tradentical.com' ||
        h === 'tradentical.com' ||
        h.endsWith('.tradentical.com')
    ) {
        return 'https://trademanthan.in';
    }
    return window.location.origin;
}

/** Avoid JSON.parse on HTML error pages (502/504 return <!DOCTYPE...>). */
function readAuthResponseJson(res) {
    const ct = (res.headers.get('content-type') || '').toLowerCase();
    if (!res.ok) {
        return res.text().then(function (text) {
            var msg = 'HTTP ' + res.status;
            if (res.status === 502 || res.status === 503 || res.status === 504) {
                msg +=
                    ' — the login API is temporarily unavailable (server restarting or overloaded). Try again in one minute, or sign in at https://trademanthan.in/login.html';
            } else if (text && text.length > 0 && text.trim().charAt(0) !== '<') {
                msg += ': ' + text.trim().slice(0, 200);
            }
            throw new Error(msg);
        });
    }
    if (ct.indexOf('application/json') === -1) {
        return res.text().then(function (text) {
            throw new Error(
                'Login server returned non-JSON. If this continues, the API may be down. Try https://trademanthan.in/login.html'
            );
        });
    }
    return res.json();
}

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
    
    // Send to backend for verification (/api/auth first, then /auth — same FastAPI mounts)
    const base = trademanthanApiBase();
    const tryUrls = [base + '/api/auth/google', base + '/auth/google'];
    const tryPost = function (idx) {
        if (idx >= tryUrls.length) {
            return Promise.reject(new Error('Login API unreachable'));
        }
        return fetch(tryUrls[idx], {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ credential: credential }),
        }).then(function (response) {
            console.log('Backend response status:', response.status, tryUrls[idx]);
            var retry =
                !response.ok &&
                (response.status === 502 || response.status === 503 || response.status === 504) &&
                idx + 1 < tryUrls.length;
            if (retry) {
                return tryPost(idx + 1);
            }
            return readAuthResponseJson(response);
        });
    };

    tryPost(0)
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
    console.log('🔄 Initiating Google Auth via mobile fallback token flow');

    // Prefer token flow for mobile fallback (does not require backend client_secret/code exchange)
    if (typeof google !== 'undefined' && google.accounts && google.accounts.oauth2) {
        try {
            const tokenClient = google.accounts.oauth2.initTokenClient({
                client_id: "428560418671-t59riis4gqkhavnevt9ve6km54ltsba7.apps.googleusercontent.com",
                scope: 'openid email profile',
                callback: handleGoogleTokenResponse
            });
            tokenClient.requestAccessToken({ prompt: 'consent' });
        } catch (error) {
            console.error('Google OAuth token flow error:', error);
            alert('Google OAuth failed. Please try again.');
        }
    } else {
        console.error('Google OAuth not loaded');
        alert('Google OAuth not available. Please refresh the page and try again.');
    }
}

// Handle Google OAuth response from token flow fallback
function handleGoogleTokenResponse(response) {
    console.log('Google OAuth token response:', response);
    if (!response || !response.access_token) {
        alert('Google OAuth failed. No access token received.');
        return;
    }

    fetch('https://www.googleapis.com/oauth2/v3/userinfo', {
        headers: {
            'Authorization': `Bearer ${response.access_token}`
        }
    })
        .then(res => res.json())
        .then(userinfo => {
            if (!userinfo || !userinfo.sub || !userinfo.email || !userinfo.name) {
                throw new Error('Incomplete user profile from Google');
            }

            const base = trademanthanApiBase();
            return fetch(base + '/api/auth/google-verify', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    google_id: userinfo.sub,
                    email: userinfo.email,
                    name: userinfo.name,
                    picture: userinfo.picture || null
                })
            }).then(function (res) {
                if (
                    !res.ok &&
                    (res.status === 502 || res.status === 503 || res.status === 504)
                ) {
                    return fetch(base + '/auth/google-verify', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            google_id: userinfo.sub,
                            email: userinfo.email,
                            name: userinfo.name,
                            picture: userinfo.picture || null
                        })
                    });
                }
                return res;
            });
        })
        .then(function (res) {
            return readAuthResponseJson(res);
        })
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
                client_id: "428560418671-t59riis4gqkhavnevt9ve6km54ltsba7.apps.googleusercontent.com",
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

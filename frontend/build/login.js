// Google OAuth callback function
function handleCredentialResponse(response) {
    console.log("Google OAuth response received");
    
    // Send the authorization code to our backend
    const code = response.credential;
    
    // For now, we'll use a demo approach
    // In production, you'd send this to your backend
    console.log("Authorization code:", code);
    
    // Simulate successful login
    simulateLogin({
        email: "user@example.com",
        name: "Demo User",
        picture: "https://via.placeholder.com/150"
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

// Initialize login popup
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Trade Manthan Login Page Loaded');
    console.log('📱 Google OAuth integration ready');
    console.log('🔑 Demo login available for development');
    
    // Check if user is already logged in
    const userToken = localStorage.getItem('trademanthan_token');
    if (userToken) {
        console.log('User already logged in, redirecting to dashboard...');
        setTimeout(() => {
            window.location.href = 'dashboard.html';
        }, 1000);
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

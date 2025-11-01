document.addEventListener('DOMContentLoaded', function() {
    // Add console welcome message
    console.log('🚀 Trade Manthan Platform Loaded Successfully!');
    console.log('📊 Professional Algo Trading Platform');
    console.log('🔗 Frontend: HTML/CSS/JS with blue-black gradient theme');
    console.log('⚡ Backend: FastAPI with Python');
    console.log('🗄️ Database: SQLite for local development');
    console.log('🌐 Domain: https://trademanthan.in (production)');
    
    // Initialize feature card animations
    initializeFeatureCards();
});

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

// Login popup functionality
function openLoginPopup() {
    const overlay = document.getElementById('loginOverlay');
    overlay.style.display = 'flex';
    overlay.style.animation = 'slideIn 0.3s ease-out';
}

function closeLoginPopup() {
    const overlay = document.getElementById('loginOverlay');
    overlay.style.animation = 'slideOut 0.3s ease-in forwards';
    
    setTimeout(() => {
        overlay.style.display = 'none';
    }, 300);
}

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

// Utility functions
function scrollToFeatures() {
    const featuresSection = document.getElementById('features');
    featuresSection.scrollIntoView({ behavior: 'smooth' });
}

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
    
    .hero-buttons {
        display: flex;
        gap: 1rem;
        justify-content: center;
        margin-top: 2rem;
        flex-wrap: wrap;
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

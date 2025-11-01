// Delta API Guide JavaScript

document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Delta API Guide loaded');
    
    // Initialize tabs
    initializeTabs();
    
    // Initialize smooth scrolling for navigation links
    initializeSmoothScrolling();
    
    // Initialize code highlighting
    initializeCodeHighlighting();
    
    // Add active state to current section in navigation
    initializeSectionTracking();
});

/**
 * Initialize tab functionality for code examples
 */
function initializeTabs() {
    const tabButtons = document.querySelectorAll('.tab-btn');
    const tabPanes = document.querySelectorAll('.tab-pane');
    
    tabButtons.forEach(button => {
        button.addEventListener('click', function() {
            const targetTab = this.getAttribute('onclick').match(/'([^']+)'/)[1];
            
            // Remove active class from all buttons and panes
            tabButtons.forEach(btn => btn.classList.remove('active'));
            tabPanes.forEach(pane => pane.classList.remove('active'));
            
            // Add active class to clicked button and target pane
            this.classList.add('active');
            document.getElementById(targetTab).classList.add('active');
        });
    });
    
    console.log('✅ Tabs initialized');
}

/**
 * Initialize smooth scrolling for navigation links
 */
function initializeSmoothScrolling() {
    const navLinks = document.querySelectorAll('.nav-link');
    
    navLinks.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            
            const targetId = this.getAttribute('href').substring(1);
            const targetElement = document.getElementById(targetId);
            
            if (targetElement) {
                // Smooth scroll to target section
                targetElement.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
                
                // Update URL hash
                history.pushState(null, null, '#' + targetId);
                
                // Add visual feedback
                this.style.transform = 'scale(0.95)';
                setTimeout(() => {
                    this.style.transform = '';
                }, 150);
            }
        });
    });
    
    console.log('✅ Smooth scrolling initialized');
}

/**
 * Initialize code highlighting with Prism.js
 */
function initializeCodeHighlighting() {
    // Check if Prism is loaded
    if (typeof Prism !== 'undefined') {
        // Highlight all code blocks
        Prism.highlightAll();
        console.log('✅ Code highlighting initialized');
    } else {
        console.log('⚠️ Prism.js not loaded, code highlighting disabled');
    }
}

/**
 * Initialize section tracking for navigation highlighting
 */
function initializeSectionTracking() {
    const sections = document.querySelectorAll('.guide-section');
    const navLinks = document.querySelectorAll('.nav-link');
    
    // Function to update active navigation link
    function updateActiveNavLink() {
        const scrollPosition = window.scrollY + 100; // Offset for header
        
        sections.forEach((section, index) => {
            const sectionTop = section.offsetTop;
            const sectionHeight = section.offsetHeight;
            
            if (scrollPosition >= sectionTop && scrollPosition < sectionTop + sectionHeight) {
                // Remove active class from all nav links
                navLinks.forEach(link => link.classList.remove('active-nav'));
                
                // Add active class to corresponding nav link
                const sectionId = section.getAttribute('id');
                const correspondingLink = document.querySelector(`.nav-link[href="#${sectionId}"]`);
                if (correspondingLink) {
                    correspondingLink.classList.add('active-nav');
                }
            }
        });
    }
    
    // Add scroll event listener
    window.addEventListener('scroll', updateActiveNavLink);
    
    // Initial call
    updateActiveNavLink();
    
    console.log('✅ Section tracking initialized');
}

/**
 * Show specific tab content
 */
function showTab(tabName) {
    // Hide all tab panes
    const tabPanes = document.querySelectorAll('.tab-pane');
    tabPanes.forEach(pane => pane.classList.remove('active'));
    
    // Remove active class from all tab buttons
    const tabButtons = document.querySelectorAll('.tab-btn');
    tabButtons.forEach(btn => btn.classList.remove('active'));
    
    // Show target tab pane
    const targetPane = document.getElementById(tabName);
    if (targetPane) {
        targetPane.classList.add('active');
    }
    
    // Add active class to clicked button
    const clickedButton = event.target;
    if (clickedButton) {
        clickedButton.classList.add('active');
    }
    
    // Re-highlight code if Prism is available
    if (typeof Prism !== 'undefined') {
        Prism.highlightAll();
    }
}

/**
 * Copy code to clipboard
 */
function copyCodeToClipboard(codeElement) {
    const text = codeElement.textContent;
    
    if (navigator.clipboard && window.isSecureContext) {
        // Use modern clipboard API
        navigator.clipboard.writeText(text).then(() => {
            showCopySuccess(codeElement);
        }).catch(err => {
            console.error('Failed to copy: ', err);
            fallbackCopyTextToClipboard(text, codeElement);
        });
    } else {
        // Fallback for older browsers
        fallbackCopyTextToClipboard(text, codeElement);
    }
}

/**
 * Fallback copy method for older browsers
 */
function fallbackCopyTextToClipboard(text, codeElement) {
    const textArea = document.createElement('textarea');
    textArea.value = text;
    textArea.style.position = 'fixed';
    textArea.style.left = '-999999px';
    textArea.style.top = '-999999px';
    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();
    
    try {
        document.execCommand('copy');
        showCopySuccess(codeElement);
    } catch (err) {
        console.error('Fallback copy failed: ', err);
    }
    
    document.body.removeChild(textArea);
}

/**
 * Show copy success feedback
 */
function showCopySuccess(codeElement) {
    // Create success message
    const successMsg = document.createElement('div');
    successMsg.textContent = '✅ Copied!';
    successMsg.style.cssText = `
        position: absolute;
        top: 10px;
        right: 10px;
        background: #28a745;
        color: white;
        padding: 4px 8px;
        border-radius: 4px;
        font-size: 12px;
        z-index: 1000;
        animation: fadeInOut 2s ease-in-out;
    `;
    
    // Add animation styles
    if (!document.getElementById('copy-animation-styles')) {
        const style = document.createElement('style');
        style.id = 'copy-animation-styles';
        style.textContent = `
            @keyframes fadeInOut {
                0% { opacity: 0; transform: translateY(-10px); }
                20% { opacity: 1; transform: translateY(0); }
                80% { opacity: 1; transform: translateY(0); }
                100% { opacity: 0; transform: translateY(-10px); }
            }
        `;
        document.head.appendChild(style);
    }
    
    // Position the code element relatively if not already
    if (getComputedStyle(codeElement).position === 'static') {
        codeElement.style.position = 'relative';
    }
    
    // Add success message
    codeElement.appendChild(successMsg);
    
    // Remove after animation
    setTimeout(() => {
        if (successMsg.parentNode) {
            successMsg.parentNode.removeChild(successMsg);
        }
    }, 2000);
}

/**
 * Add copy buttons to code blocks
 */
function addCopyButtonsToCodeBlocks() {
    const codeBlocks = document.querySelectorAll('pre code');
    
    codeBlocks.forEach(codeBlock => {
        const preElement = codeBlock.parentElement;
        
        // Check if copy button already exists
        if (preElement.querySelector('.copy-button')) {
            return;
        }
        
        // Create copy button
        const copyButton = document.createElement('button');
        copyButton.className = 'copy-button';
        copyButton.innerHTML = '<i class="fas fa-copy"></i>';
        copyButton.style.cssText = `
            position: absolute;
            top: 10px;
            right: 10px;
            background: rgba(255, 255, 255, 0.9);
            border: 1px solid #e9ecef;
            border-radius: 4px;
            padding: 6px 8px;
            cursor: pointer;
            font-size: 12px;
            color: #6c757d;
            transition: all 0.3s ease;
            z-index: 100;
        `;
        
        // Add hover effects
        copyButton.addEventListener('mouseenter', function() {
            this.style.background = '#1976d2';
            this.style.color = 'white';
            this.style.borderColor = '#1976d2';
        });
        
        copyButton.addEventListener('mouseleave', function() {
            this.style.background = 'rgba(255, 255, 255, 0.9)';
            this.style.color = '#6c757d';
            this.style.borderColor = '#e9ecef';
        });
        
        // Add click handler
        copyButton.addEventListener('click', function() {
            copyCodeToClipboard(codeBlock);
        });
        
        // Position the pre element relatively
        preElement.style.position = 'relative';
        
        // Add copy button
        preElement.appendChild(copyButton);
    });
}

/**
 * Initialize copy buttons after a short delay
 */
setTimeout(() => {
    addCopyButtonsToCodeBlocks();
}, 500);

/**
 * Utility function to scroll to top
 */
function scrollToTop() {
    window.scrollTo({
        top: 0,
        behavior: 'smooth'
    });
}

/**
 * Add scroll to top button
 */
function addScrollToTopButton() {
    const scrollButton = document.createElement('button');
    scrollButton.innerHTML = '<i class="fas fa-arrow-up"></i>';
    scrollButton.className = 'scroll-to-top';
    scrollButton.style.cssText = `
        position: fixed;
        bottom: 30px;
        right: 30px;
        width: 50px;
        height: 50px;
        background: #1976d2;
        color: white;
        border: none;
        border-radius: 50%;
        cursor: pointer;
        font-size: 18px;
        box-shadow: 0 4px 12px rgba(25, 118, 210, 0.3);
        transition: all 0.3s ease;
        z-index: 1000;
        opacity: 0;
        visibility: hidden;
    `;
    
    // Add hover effects
    scrollButton.addEventListener('mouseenter', function() {
        this.style.transform = 'scale(1.1)';
        this.style.boxShadow = '0 6px 20px rgba(25, 118, 210, 0.4)';
    });
    
    scrollButton.addEventListener('mouseleave', function() {
        this.style.transform = 'scale(1)';
        this.style.boxShadow = '0 4px 12px rgba(25, 118, 210, 0.3)';
    });
    
    // Add click handler
    scrollButton.addEventListener('click', scrollToTop);
    
    // Add to body
    document.body.appendChild(scrollButton);
    
    // Show/hide based on scroll position
    window.addEventListener('scroll', function() {
        if (window.scrollY > 300) {
            scrollButton.style.opacity = '1';
            scrollButton.style.visibility = 'visible';
        } else {
            scrollButton.style.opacity = '0';
            scrollButton.style.visibility = 'hidden';
        }
    });
}

// Add scroll to top button after page loads
setTimeout(() => {
    addScrollToTopButton();
}, 1000);

// Add CSS for active navigation state
const activeNavStyles = `
    .nav-link.active-nav {
        background: #1976d2 !important;
        color: white !important;
        border-color: #1976d2 !important;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(25, 118, 210, 0.3);
    }
`;

if (!document.getElementById('active-nav-styles')) {
    const style = document.createElement('style');
    style.id = 'active-nav-styles';
    style.textContent = activeNavStyles;
    document.head.appendChild(style);
}

console.log('🎯 Delta API Guide JavaScript loaded successfully');

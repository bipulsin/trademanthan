class ReportsManager {
    constructor() {
        this.trades = [];
        this.filteredTrades = [];
        this.currentUser = null;
        this.currentPage = 1;
        this.tradesPerPage = 20;
        this.init();
    }

    async init() {
        await this.waitForLeftMenu();
        this.loadUserData();
        this.loadTrades();
        this.setupEventListeners();
        this.setupMobileMenu();
        this.updateSummary();
    }

    async waitForLeftMenu() {
        return new Promise((resolve) => {
            const checkLeftMenu = () => {
                const leftPanel = document.querySelector('.left-panel');
                const userAvatar = document.querySelector('#userAvatar');
                
                if (leftPanel && userAvatar) {
                    resolve();
                } else {
                    setTimeout(checkLeftMenu, 100);
                }
            };
            checkLeftMenu();
        });
    }

    loadUserData() {
        const userData = localStorage.getItem('trademanthan_user');
        if (userData) {
            try {
                this.currentUser = JSON.parse(userData);
            } catch (error) {
                console.error('Error parsing user data:', error);
            }
        }
    }

    setupEventListeners() {
        // Filter changes
        document.getElementById('dateRange').addEventListener('change', () => this.filterReports());
        document.getElementById('symbolFilter').addEventListener('change', () => this.filterReports());
        document.getElementById('pnlFilter').addEventListener('change', () => this.filterReports());
        document.getElementById('searchTrades').addEventListener('input', () => this.filterReports());
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

    loadTrades() {
        // Mock data for demonstration
        this.trades = [
            {
                id: 1,
                dateTime: '2024-01-15T10:30:00Z',
                symbol: 'BTC',
                type: 'buy',
                entryPrice: 43250.50,
                exitPrice: 43800.25,
                quantity: 0.5,
                pnl: 274.88,
                fees: 12.50,
                status: 'completed'
            },
            {
                id: 2,
                dateTime: '2024-01-15T09:15:00Z',
                symbol: 'ETH',
                type: 'sell',
                entryPrice: 2650.75,
                exitPrice: 2620.50,
                quantity: 2.0,
                pnl: -60.50,
                fees: 8.75,
                status: 'completed'
            },
            {
                id: 3,
                dateTime: '2024-01-14T16:45:00Z',
                symbol: 'AAPL',
                type: 'buy',
                entryPrice: 185.25,
                exitPrice: 188.75,
                quantity: 100,
                pnl: 350.00,
                fees: 5.25,
                status: 'completed'
            },
            {
                id: 4,
                dateTime: '2024-01-14T14:20:00Z',
                symbol: 'GOOGL',
                type: 'sell',
                entryPrice: 142.80,
                exitPrice: 141.20,
                quantity: 50,
                pnl: -80.00,
                fees: 3.50,
                status: 'completed'
            },
            {
                id: 5,
                dateTime: '2024-01-13T11:30:00Z',
                symbol: 'BTC',
                type: 'buy',
                entryPrice: 42800.00,
                exitPrice: 43250.50,
                quantity: 0.25,
                pnl: 112.63,
                fees: 6.25,
                status: 'completed'
            },
            {
                id: 6,
                dateTime: '2024-01-13T10:15:00Z',
                symbol: 'ETH',
                type: 'buy',
                entryPrice: 2600.00,
                exitPrice: 2650.75,
                quantity: 1.5,
                pnl: 76.13,
                fees: 6.50,
                status: 'completed'
            },
            {
                id: 7,
                dateTime: '2024-01-12T15:45:00Z',
                symbol: 'AAPL',
                type: 'sell',
                entryPrice: 182.50,
                exitPrice: 185.25,
                quantity: 75,
                pnl: 206.25,
                fees: 4.00,
                status: 'completed'
            },
            {
                id: 8,
                dateTime: '2024-01-12T13:20:00Z',
                symbol: 'GOOGL',
                type: 'buy',
                entryPrice: 140.00,
                exitPrice: 142.80,
                quantity: 25,
                pnl: 70.00,
                fees: 2.00,
                status: 'completed'
            }
        ];

        this.filteredTrades = [...this.trades];
        this.renderTrades();
    }

    filterReports() {
        const dateRange = document.getElementById('dateRange').value;
        const symbolFilter = document.getElementById('symbolFilter').value;
        const pnlFilter = document.getElementById('pnlFilter').value;
        const searchQuery = document.getElementById('searchTrades').value.toLowerCase();

        let filtered = [...this.trades];

        // Date range filter
        if (dateRange !== 'custom') {
            const days = parseInt(dateRange);
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - days);
            
            filtered = filtered.filter(trade => {
                const tradeDate = new Date(trade.dateTime);
                return tradeDate >= cutoffDate;
            });
        }

        // Symbol filter
        if (symbolFilter) {
            filtered = filtered.filter(trade => trade.symbol === symbolFilter);
        }

        // P&L filter
        if (pnlFilter === 'profit') {
            filtered = filtered.filter(trade => trade.pnl > 0);
        } else if (pnlFilter === 'loss') {
            filtered = filtered.filter(trade => trade.pnl < 0);
        }

        // Search filter
        if (searchQuery) {
            filtered = filtered.filter(trade => 
                trade.symbol.toLowerCase().includes(searchQuery) ||
                trade.status.toLowerCase().includes(searchQuery)
            );
        }

        this.filteredTrades = filtered;
        this.currentPage = 1;
        this.renderTrades();
        this.updateSummary();
    }

    renderTrades() {
        const tableBody = document.getElementById('tradesTableBody');
        const tradesCount = document.getElementById('tradesCount');
        
        if (this.filteredTrades.length === 0) {
            tableBody.innerHTML = `
                <tr>
                    <td colspan="9" class="empty-state">
                        <i class="fas fa-chart-line"></i>
                        <h3>No Trades Found</h3>
                        <p>No trades match your current filters</p>
                    </td>
                </tr>
            `;
            tradesCount.textContent = '0 trades found';
            this.renderPagination();
            return;
        }

        const startIndex = (this.currentPage - 1) * this.tradesPerPage;
        const endIndex = startIndex + this.tradesPerPage;
        const pageTrades = this.filteredTrades.slice(startIndex, endIndex);

        tableBody.innerHTML = pageTrades.map(trade => this.createTradeRow(trade)).join('');
        tradesCount.textContent = `${this.filteredTrades.length} trades found`;
        
        this.renderPagination();
    }

    createTradeRow(trade) {
        const dateTime = new Date(trade.dateTime).toLocaleString();
        const entryPrice = this.formatCurrency(trade.entryPrice);
        const exitPrice = this.formatCurrency(trade.exitPrice);
        const quantity = this.formatQuantity(trade.quantity, trade.symbol);
        const pnl = this.formatCurrency(trade.pnl);
        const fees = this.formatCurrency(trade.fees);
        const pnlClass = trade.pnl >= 0 ? 'positive' : 'negative';
        const pnlSign = trade.pnl >= 0 ? '+' : '';

        return `
            <tr>
                <td>${dateTime}</td>
                <td><strong>${trade.symbol}</strong></td>
                <td><span class="trade-type ${trade.type}">${trade.type.toUpperCase()}</span></td>
                <td>${entryPrice}</td>
                <td>${exitPrice}</td>
                <td>${quantity}</td>
                <td class="pnl-value ${pnlClass}">${pnlSign}${pnl}</td>
                <td>${fees}</td>
                <td><span class="status-badge status-${trade.status}">${trade.status}</span></td>
            </tr>
        `;
    }

    renderPagination() {
        const pagination = document.getElementById('pagination');
        const totalPages = Math.ceil(this.filteredTrades.length / this.tradesPerPage);
        
        if (totalPages <= 1) {
            pagination.innerHTML = '';
            return;
        }

        let paginationHTML = '';
        
        // Previous button
        paginationHTML += `
            <button onclick="changePage(${this.currentPage - 1})" ${this.currentPage === 1 ? 'disabled' : ''}>
                <i class="fas fa-chevron-left"></i> Previous
            </button>
        `;

        // Page numbers
        for (let i = 1; i <= totalPages; i++) {
            if (i === 1 || i === totalPages || (i >= this.currentPage - 2 && i <= this.currentPage + 2)) {
                paginationHTML += `
                    <button onclick="changePage(${i})" class="${i === this.currentPage ? 'active' : ''}">
                        ${i}
                    </button>
                `;
            } else if (i === this.currentPage - 3 || i === this.currentPage + 3) {
                paginationHTML += '<span>...</span>';
            }
        }

        // Next button
        paginationHTML += `
            <button onclick="changePage(${this.currentPage + 1})" ${this.currentPage === totalPages ? 'disabled' : ''}>
                Next <i class="fas fa-chevron-right"></i>
            </button>
        `;

        // Page info
        paginationHTML += `
            <span class="pagination-info">
                Page ${this.currentPage} of ${totalPages}
            </span>
        `;

        pagination.innerHTML = paginationHTML;
    }

    changePage(page) {
        const totalPages = Math.ceil(this.filteredTrades.length / this.tradesPerPage);
        if (page >= 1 && page <= totalPages) {
            this.currentPage = page;
            this.renderTrades();
        }
    }

    updateSummary() {
        const totalTrades = this.filteredTrades.length;
        const totalPnL = this.filteredTrades.reduce((sum, trade) => sum + trade.pnl, 0);
        const totalFees = this.filteredTrades.reduce((sum, trade) => sum + trade.fees, 0);
        const profitableTrades = this.filteredTrades.filter(trade => trade.pnl > 0).length;
        const winRate = totalTrades > 0 ? (profitableTrades / totalTrades) * 100 : 0;

        document.getElementById('totalTrades').textContent = totalTrades;
        document.getElementById('totalPnL').textContent = this.formatCurrency(totalPnL);
        document.getElementById('winRate').textContent = `${winRate.toFixed(1)}%`;
        document.getElementById('totalFees').textContent = this.formatCurrency(totalFees);
    }

    formatCurrency(amount) {
        return new Intl.NumberFormat('en-US', {
            style: 'currency',
            currency: 'USD',
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }).format(amount);
    }

    formatQuantity(quantity, symbol) {
        if (symbol === 'BTC' || symbol === 'ETH') {
            return quantity.toFixed(4);
        } else {
            return Math.round(quantity).toLocaleString();
        }
    }

    refreshReports() {
        this.loadTrades();
        this.filterReports();
        this.showNotification('Reports refreshed successfully!', 'success');
    }

    exportToCSV() {
        if (this.filteredTrades.length === 0) {
            this.showNotification('No trades to export!', 'warning');
            return;
        }

        const headers = ['Date/Time', 'Symbol', 'Type', 'Entry Price', 'Exit Price', 'Quantity', 'P&L', 'Fees', 'Status'];
        const csvContent = [
            headers.join(','),
            ...this.filteredTrades.map(trade => [
                new Date(trade.dateTime).toLocaleString(),
                trade.symbol,
                trade.type.toUpperCase(),
                trade.entryPrice,
                trade.exitPrice,
                trade.quantity,
                trade.pnl,
                trade.fees,
                trade.status
            ].join(','))
        ].join('\n');

        this.downloadFile(csvContent, 'trades_report.csv', 'text/csv');
        this.showNotification('CSV export completed!', 'success');
    }

    exportToExcel() {
        if (this.filteredTrades.length === 0) {
            this.showNotification('No trades to export!', 'warning');
            return;
        }

        // For Excel export, we'll create a CSV with .xlsx extension
        // In a real implementation, you'd use a library like SheetJS
        const headers = ['Date/Time', 'Symbol', 'Type', 'Entry Price', 'Exit Price', 'Quantity', 'P&L', 'Fees', 'Status'];
        const csvContent = [
            headers.join(','),
            ...this.filteredTrades.map(trade => [
                new Date(trade.dateTime).toLocaleString(),
                trade.symbol,
                trade.type.toUpperCase(),
                trade.entryPrice,
                trade.exitPrice,
                trade.quantity,
                trade.pnl,
                trade.fees,
                trade.status
            ].join(','))
        ].join('\n');

        this.downloadFile(csvContent, 'trades_report.xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        this.showNotification('Excel export completed!', 'success');
    }

    downloadFile(content, filename, mimeType) {
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }

    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'warning' ? 'exclamation-triangle' : 'info-circle'}"></i>
            <span>${message}</span>
        `;
        
        // Add to page
        document.body.appendChild(notification);
        
        // Show notification
        setTimeout(() => notification.classList.add('show'), 100);
        
        // Remove after 3 seconds
        setTimeout(() => {
            notification.classList.remove('show');
            setTimeout(() => notification.remove(), 300);
        }, 3000);
    }
}

// Global functions for onclick handlers
function refreshReports() {
    window.reportsManager.refreshReports();
}

function exportToCSV() {
    window.reportsManager.exportToCSV();
}

function exportToExcel() {
    window.reportsManager.exportToExcel();
}

function changePage(page) {
    window.reportsManager.changePage(page);
}

function filterReports() {
    window.reportsManager.filterReports();
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.reportsManager = new ReportsManager();
});

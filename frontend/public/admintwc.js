(() => {
    let userActivityExpanded = false;
    let userActivityLoadedOnce = false;

    const API_BASE_URL = (function () {
        const h = window.location.hostname;
        if (h === 'localhost' || h === '127.0.0.1') return 'http://localhost:8000';
        return window.location.origin;
    })();

    function getToken() {
        return localStorage.getItem('trademanthan_token') || '';
    }

    function formatDateTime(value) {
        if (!value) return '-';
        const d = new Date(value);
        if (Number.isNaN(d.getTime())) return '-';
        return d.toLocaleString('en-IN', {
            year: 'numeric',
            month: 'short',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: true,
        });
    }

    function actionBtn(iconClass, title, activeClass, active, onClickJs) {
        const color = active ? '#2563eb' : '#6b7280';
        const iconHtml = iconClass === 'paid-coin'
            ? `<span class="paid-coin-icon ${active ? 'active' : ''}" aria-hidden="true">₹</span>`
            : `<i class="fas ${iconClass}" aria-hidden="true"></i>`;
        return `<button class="admin-flag-btn ${activeClass}" title="${title}" data-tooltip="${title}" aria-label="${title}" onclick="${onClickJs}" style="border:none;background:transparent;cursor:pointer;padding:6px;color:${color};font-size:16px;">
            ${iconHtml}
        </button>`;
    }

    async function apiFetch(paths, options) {
        let lastError = null;
        for (const path of paths) {
            try {
                const url = path.startsWith('http') ? path : `${API_BASE_URL}${path}`;
                const res = await fetch(url, options);
                if (res.ok) return res;
                const bodyText = await res.text().catch(() => '');
                lastError = new Error(`HTTP ${res.status} ${bodyText.slice(0, 180)}`);
            } catch (e) {
                lastError = e;
            }
        }
        throw lastError || new Error('API request failed');
    }

    async function loadUserActivity() {
        const tbody = document.getElementById('userActivityBody');
        const cards = document.getElementById('userActivityCards');
        if (!tbody) return;
        tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;">Loading user activity...</td></tr>`;
        if (cards) cards.innerHTML = `<div style="padding: 4px 2px;">Loading user activity...</div>`;

        const token = getToken();
        if (!token) {
            tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;color:#dc2626;">Session expired. Please login again.</td></tr>`;
            if (cards) cards.innerHTML = `<div style="padding: 4px 2px; color:#dc2626;">Session expired. Please login again.</div>`;
            return;
        }

        try {
            const res = await apiFetch(
                ['/api/auth/admin/user-activity', '/auth/admin/user-activity'],
                {
                    headers: { Authorization: `Bearer ${token}` },
                    cache: 'no-store',
                }
            );
            const contentType = (res.headers.get('content-type') || '').toLowerCase();
            if (!contentType.includes('application/json')) {
                throw new Error('Unexpected response from server');
            }
            const data = await res.json();
            const users = Array.isArray(data.users) ? data.users : [];
            if (!users.length) {
                tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;">No users found.</td></tr>`;
                if (cards) cards.innerHTML = `<div style="padding: 4px 2px;">No users found.</div>`;
                return;
            }
            tbody.innerHTML = users
                .map((u) => {
                    const safeName = (u.name || '-').replace(/"/g, '&quot;');
                    const safeEmail = (u.email || '-').replace(/"/g, '&quot;');
                    const safePage = (u.last_page_visited || '-').replace(/"/g, '&quot;');
                    const rowClass = u.is_blocked ? ' style="background: rgba(239,68,68,0.08);"' : '';
                    return `<tr${rowClass}>
                        <td style="padding:10px;">
                            <div style="font-weight:600;">${safeName}</div>
                            <div style="font-size:12px;color:#64748b;">${safeEmail}</div>
                        </td>
                        <td style="padding:10px;">${formatDateTime(u.last_login_at)}</td>
                        <td style="padding:10px;">${u.last_login_ip || '-'}</td>
                        <td style="padding:10px;">
                            <div>${safePage}</div>
                            <div style="font-size:12px;color:#64748b;">${formatDateTime(u.last_page_visited_at)}</div>
                        </td>
                        <td style="padding:10px;white-space:nowrap;">
                            ${actionBtn('fa-ban', `${u.is_blocked ? 'Unblock User' : 'Block User'}`, 'flag-block', !!u.is_blocked, `window.toggleUserFlag(${u.id}, 'is_blocked', ${!u.is_blocked})`)}
                            ${actionBtn('paid-coin', `${u.is_paid_user ? 'Remove Paid User' : 'Mark as Paid User'}`, 'flag-paid', !!u.is_paid_user, `window.toggleUserFlag(${u.id}, 'is_paid_user', ${!u.is_paid_user})`)}
                            ${actionBtn('fa-user-shield', `${u.is_admin ? 'Remove Admin Access' : 'Grant Admin Access'}`, 'flag-admin', !!u.is_admin, `window.toggleUserFlag(${u.id}, 'is_admin', ${!u.is_admin})`)}
                        </td>
                    </tr>`;
                })
                .join('');

            if (cards) {
                cards.innerHTML = users.map((u) => {
                    const safeName = (u.name || '-').replace(/"/g, '&quot;');
                    const safeEmail = (u.email || '-').replace(/"/g, '&quot;');
                    const safePage = (u.last_page_visited || '-').replace(/"/g, '&quot;');
                    return `<div class="user-card ${u.is_blocked ? 'blocked' : ''}">
                        <div class="user-card-title">${safeName}</div>
                        <div class="user-card-email">${safeEmail}</div>
                        <div class="user-card-row"><strong>Last Login:</strong> ${formatDateTime(u.last_login_at)}</div>
                        <div class="user-card-row"><strong>Last Login IP:</strong> ${u.last_login_ip || '-'}</div>
                        <div class="user-card-row"><strong>Last Page:</strong> ${safePage}</div>
                        <div class="user-card-row"><strong>Page Time:</strong> ${formatDateTime(u.last_page_visited_at)}</div>
                        <div class="user-card-actions">
                            ${actionBtn('fa-ban', `${u.is_blocked ? 'Unblock User' : 'Block User'}`, 'flag-block', !!u.is_blocked, `window.toggleUserFlag(${u.id}, 'is_blocked', ${!u.is_blocked})`)}
                            ${actionBtn('paid-coin', `${u.is_paid_user ? 'Remove Paid User' : 'Mark as Paid User'}`, 'flag-paid', !!u.is_paid_user, `window.toggleUserFlag(${u.id}, 'is_paid_user', ${!u.is_paid_user})`)}
                            ${actionBtn('fa-user-shield', `${u.is_admin ? 'Remove Admin Access' : 'Grant Admin Access'}`, 'flag-admin', !!u.is_admin, `window.toggleUserFlag(${u.id}, 'is_admin', ${!u.is_admin})`)}
                        </div>
                    </div>`;
                }).join('');
            }
        } catch (e) {
            console.error('User activity load failed:', e);
            tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;color:#dc2626;">Failed to load user activity.</td></tr>`;
            if (cards) cards.innerHTML = `<div style="padding: 4px 2px; color:#dc2626;">Failed to load user activity.</div>`;
        }
    }

    async function toggleUserFlag(userId, key, value) {
        const token = getToken();
        if (!token) return;
        try {
            await apiFetch(
                [`/api/auth/admin/users/${userId}/flags`, `/auth/admin/users/${userId}/flags`],
                {
                    method: 'PATCH',
                    headers: {
                        'Content-Type': 'application/json',
                        Authorization: `Bearer ${token}`,
                    },
                    body: JSON.stringify({ [key]: value }),
                }
            );
            await loadUserActivity();
        } catch (e) {
            alert('Failed to update user flag.');
        }
    }

    window.toggleUserFlag = toggleUserFlag;

    let smartFuturesExpanded = false;
    let smartFuturesLoadedOnce = false;

    async function loadSmartFuturesConfig() {
        const msg = document.getElementById('sfAdminMsg');
        const token = getToken();
        if (!token) {
            if (msg) msg.textContent = 'Not logged in';
            return;
        }
        try {
            const res = await apiFetch(['/api/smart-futures/config', '/smart-futures/config'], {
                headers: { Authorization: `Bearer ${token}` },
                cache: 'no-store',
            });
            const data = await res.json();
            const live = document.getElementById('sfAdminLive');
            const pos = document.getElementById('sfAdminPos');
            const part = document.getElementById('sfAdminPartial');
            if (live) live.value = data.live_enabled ? 'YES' : 'NO';
            if (pos) pos.value = String(data.position_size || 1);
            if (part) part.checked = !!data.partial_exit_enabled;
            const ap = document.getElementById('sfAdminAtrPeriod');
            const ao = document.getElementById('sfAdminAtrOverride');
            if (ap) ap.value = String(data.brick_atr_period != null ? data.brick_atr_period : 10);
            if (ao) ao.value = data.brick_atr_override != null && data.brick_atr_override !== '' ? String(data.brick_atr_override) : '';
            if (msg) msg.textContent = '';
        } catch (e) {
            console.error(e);
            if (msg) msg.textContent = 'Failed to load config';
        }
    }

    async function saveSmartFuturesConfig() {
        const msg = document.getElementById('sfAdminMsg');
        const token = getToken();
        if (!token) return;
        const liveEl = document.getElementById('sfAdminLive');
        const posEl = document.getElementById('sfAdminPos');
        const partEl = document.getElementById('sfAdminPartial');
        const apEl = document.getElementById('sfAdminAtrPeriod');
        const aoEl = document.getElementById('sfAdminAtrOverride');
        const rawOv = aoEl && aoEl.value != null ? String(aoEl.value).trim() : '';
        const body = {
            live_enabled: liveEl && liveEl.value === 'YES',
            position_size: posEl ? parseInt(posEl.value, 10) : 1,
            partial_exit_enabled: partEl ? !!partEl.checked : false,
            brick_atr_period: apEl ? parseInt(apEl.value, 10) || 10 : 10,
            brick_atr_override: rawOv === '' ? null : parseFloat(rawOv),
        };
        if (body.brick_atr_override != null && !Number.isFinite(body.brick_atr_override)) {
            if (msg) msg.textContent = 'Invalid ATR override number';
            return;
        }
        try {
            const res = await fetch(`${API_BASE_URL}/api/smart-futures/config`, {
                method: 'PUT',
                headers: {
                    Authorization: `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(body),
            });
            if (!res.ok) {
                const t = await res.text();
                throw new Error(t || res.status);
            }
            if (msg) msg.textContent = 'Saved.';
            setTimeout(() => {
                if (msg) msg.textContent = '';
            }, 3000);
        } catch (e) {
            console.error(e);
            if (msg) msg.textContent = 'Save failed';
        }
    }

    function setSmartFuturesExpanded(expanded) {
        smartFuturesExpanded = expanded;
        const content = document.getElementById('smartFuturesContent');
        const icon = document.getElementById('smartFuturesCollapseIcon');
        if (content) content.classList.toggle('expanded', expanded);
        if (icon) {
            icon.classList.toggle('fa-chevron-down', !expanded);
            icon.classList.toggle('fa-chevron-up', expanded);
        }
    }

    function setUserActivityExpanded(expanded) {
        userActivityExpanded = expanded;
        const content = document.getElementById('userActivityContent');
        const icon = document.getElementById('userActivityCollapseIcon');
        if (content) content.classList.toggle('expanded', expanded);
        if (icon) {
            icon.classList.toggle('fa-chevron-down', !expanded);
            icon.classList.toggle('fa-chevron-up', expanded);
        }
    }

    document.addEventListener('DOMContentLoaded', () => {
        setSmartFuturesExpanded(false);
        const sfToggle = document.getElementById('smartFuturesToggle');
        if (sfToggle) {
            sfToggle.addEventListener('click', async () => {
                const next = !smartFuturesExpanded;
                setSmartFuturesExpanded(next);
                if (next && !smartFuturesLoadedOnce) {
                    smartFuturesLoadedOnce = true;
                    await loadSmartFuturesConfig();
                }
            });
        }
        const sfSave = document.getElementById('sfAdminSave');
        if (sfSave) sfSave.addEventListener('click', () => saveSmartFuturesConfig());

        setUserActivityExpanded(false);
        const toggle = document.getElementById('userActivityToggle');
        if (toggle) {
            toggle.addEventListener('click', async () => {
                const next = !userActivityExpanded;
                setUserActivityExpanded(next);
                if (next && !userActivityLoadedOnce) {
                    userActivityLoadedOnce = true;
                    await loadUserActivity();
                }
            });
        }

        setInterval(() => {
            if (userActivityExpanded) {
                loadUserActivity();
            }
        }, 60000);
    });
})();

(() => {
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
        return `<button class="admin-flag-btn ${activeClass}" title="${title}" onclick="${onClickJs}" style="border:none;background:transparent;cursor:pointer;padding:6px;color:${color};font-size:16px;">
            <i class="fas ${iconClass}"></i>
        </button>`;
    }

    async function apiFetch(paths, options) {
        let lastError = null;
        for (const path of paths) {
            try {
                const res = await fetch(path, options);
                if (res.ok) return res;
                lastError = new Error(`HTTP ${res.status}`);
            } catch (e) {
                lastError = e;
            }
        }
        throw lastError || new Error('API request failed');
    }

    async function loadUserActivity() {
        const tbody = document.getElementById('userActivityBody');
        if (!tbody) return;
        tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;">Loading user activity...</td></tr>`;

        const token = getToken();
        if (!token) {
            tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;color:#dc2626;">Session expired. Please login again.</td></tr>`;
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
            const data = await res.json();
            const users = Array.isArray(data.users) ? data.users : [];
            if (!users.length) {
                tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;">No users found.</td></tr>`;
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
                            ${actionBtn('fa-ban', 'Block / Unblock User', 'flag-block', !!u.is_blocked, `window.toggleUserFlag(${u.id}, 'is_blocked', ${!u.is_blocked})`)}
                            ${actionBtn('fa-crown', 'Paid User On/Off', 'flag-paid', !!u.is_paid_user, `window.toggleUserFlag(${u.id}, 'is_paid_user', ${!u.is_paid_user})`)}
                            ${actionBtn('fa-user-shield', 'Admin User On/Off', 'flag-admin', !!u.is_admin, `window.toggleUserFlag(${u.id}, 'is_admin', ${!u.is_admin})`)}
                        </td>
                    </tr>`;
                })
                .join('');
        } catch (e) {
            tbody.innerHTML = `<tr><td colspan="5" style="padding:12px;color:#dc2626;">Failed to load user activity.</td></tr>`;
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

    document.addEventListener('DOMContentLoaded', () => {
        loadUserActivity();
        setInterval(loadUserActivity, 60000);
    });
})();

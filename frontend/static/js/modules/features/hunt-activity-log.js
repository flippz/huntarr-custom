/**
 * Home dashboard feed for durable Huntarr activity events.
 */

window.HuntActivityLog = {
    pollInterval: null,
    page: 1,
    pageSize: 50,
    initialized: false,

    init: function () {
        if (this.initialized) return;
        this.initialized = true;

        ['status', 'type', 'app', 'time'].forEach((name) => {
            const select = document.getElementById(`hunt-activity-${name}-filter`);
            if (!select) return;
            select.addEventListener('change', () => {
                this.page = 1;
                this.refresh();
            });
        });

        this.refresh();
        this.startPolling();
    },

    cleanup: function () {
        this.stopPolling();
        this.initialized = false;
    },

    startPolling: function () {
        this.stopPolling();
        this.pollInterval = setInterval(() => {
            if (window.huntarrUI && window.huntarrUI.currentSection === 'home') {
                this.refresh();
            }
        }, 30000);
    },

    stopPolling: function () {
        if (this.pollInterval) {
            clearInterval(this.pollInterval);
            this.pollInterval = null;
        }
    },

    refresh: function () {
        const list = document.getElementById('hunt-activity-log-list');
        if (!list) return;

        const params = new URLSearchParams({
            page: String(this.page),
            page_size: String(this.pageSize),
            status: this.value('status'),
            type: this.value('type'),
            app: this.value('app'),
        });
        const since = this.sinceValue();
        if (since) params.set('since', String(since));

        HuntarrUtils.fetchWithTimeout(`./api/hunt-manager/activity?${params.toString()}`)
            .then((response) => {
                if (!response.ok) throw new Error(`Activity feed returned ${response.status}`);
                return response.json();
            })
            .then((data) => this.render(data.entries || []))
            .catch((error) => {
                console.error('[HuntActivityLog] Failed to load activity:', error);
                if (list) {
                    list.innerHTML = '<div class="hunt-activity-empty">Unable to load activity. It will retry shortly.</div>';
                }
            });
    },

    value: function (name) {
        const select = document.getElementById(`hunt-activity-${name}-filter`);
        return select ? (select.value || 'all') : 'all';
    },

    sinceValue: function () {
        const range = this.value('time');
        const seconds = { hour: 3600, day: 86400, week: 604800 };
        return seconds[range] ? Math.floor(Date.now() / 1000) - seconds[range] : '';
    },

    render: function (entries) {
        const list = document.getElementById('hunt-activity-log-list');
        if (!list) return;
        if (!entries.length) {
            list.innerHTML = '<div class="hunt-activity-empty">No matching Huntarr activity yet.</div>';
            return;
        }

        list.innerHTML = entries.map((entry) => {
            const status = entry.status || 'searching';
            const app = this.title(entry.app_type);
            const title = entry.processed_info || entry.media_id || 'Hunt activity';
            const detail = entry.detail || '';
            return `<div class="hunt-activity-row">
                <div class="hunt-activity-time">${this.escape(entry.occurred_at_readable || '')}</div>
                <div class="hunt-activity-main">
                    <div class="hunt-activity-line">
                        <span class="hunt-activity-badge hunt-activity-${this.css(status)}">${this.statusLabel(status)}</span>
                        <span class="hunt-activity-app">${this.escape(app)}</span>
                        <span class="hunt-activity-title">${this.escape(title)}</span>
                    </div>
                    ${detail ? `<div class="hunt-activity-detail">${this.escape(detail)}</div>` : ''}
                </div>
            </div>`;
        }).join('');
    },

    statusLabel: function (status) {
        return {
            searching: 'Searching',
            downloaded: 'Downloaded',
            no_results: 'No results',
            failed: 'Failed',
            deferred: 'Deferred',
        }[status] || this.title(status);
    },

    title: function (value) {
        return String(value || 'Unknown').split('_').map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join(' ');
    },

    css: function (value) {
        return String(value || 'searching').toLowerCase().replace(/[^a-z0-9_-]+/g, '-');
    },

    escape: function (value) {
        return String(value == null ? '' : value).replace(/[&<>"']/g, (char) => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;',
        })[char]);
    },
};

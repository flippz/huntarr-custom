/**
 * Swaparr Activity Module
 * Expandable panel on the home page showing what Swaparr is doing to Sonarr's queue:
 * live queue items (Sonarr status + Swaparr strikes + torrent client status) and a
 * rolling history of items that completed normally vs. were removed by Swaparr.
 */

window.HuntarrSwaparrActivity = {
    expanded: false,
    pollInterval: null,
    currentInstanceId: null,
    historyPage: 1,
    storageKey: 'huntarr-swaparr-activity-expanded',

    init: function () {
        const toggleBtn = document.getElementById('toggle-swaparr-activity');
        if (toggleBtn && !toggleBtn.dataset.activityBound) {
            toggleBtn.dataset.activityBound = 'true';
            toggleBtn.addEventListener('click', () => this.togglePanel());
        }

        if (!this.stateRestored) {
            this.stateRestored = true;
            const saved = localStorage.getItem(this.storageKey);
            const shouldExpand = saved === null ? true : saved === 'true';
            if (shouldExpand) {
                this.setExpanded(true);
            }
        }

        const instanceSelect = document.getElementById('swaparr-activity-instance-select');
        if (instanceSelect && !instanceSelect.dataset.activityBound) {
            instanceSelect.dataset.activityBound = 'true';
            instanceSelect.addEventListener('change', () => {
                this.currentInstanceId = instanceSelect.value;
                this.historyPage = 1;
                this.refresh();
            });
        }

        const prevBtn = document.getElementById('swaparr-activity-history-prev');
        const nextBtn = document.getElementById('swaparr-activity-history-next');
        if (prevBtn && !prevBtn.dataset.activityBound) {
            prevBtn.dataset.activityBound = 'true';
            prevBtn.addEventListener('click', () => {
                if (this.historyPage > 1) {
                    this.historyPage -= 1;
                    this.loadHistory();
                }
            });
        }
        if (nextBtn && !nextBtn.dataset.activityBound) {
            nextBtn.dataset.activityBound = 'true';
            nextBtn.addEventListener('click', () => {
                this.historyPage += 1;
                this.loadHistory();
            });
        }
    },

    setInstances: function (instances) {
        const select = document.getElementById('swaparr-activity-instance-select');
        if (!select) return;

        const previousValue = select.value;
        select.innerHTML = (instances || [])
            .map(inst => `<option value="${inst.instance_id}">${inst.instance_name}</option>`)
            .join('');

        if (previousValue && (instances || []).some(i => i.instance_id === previousValue)) {
            select.value = previousValue;
        }
        this.currentInstanceId = select.value || null;

        const row = document.querySelector('.swaparr-activity-instance-row');
        if (row) row.style.display = (instances || []).length > 1 ? 'flex' : 'none';
    },

    togglePanel: function () {
        this.setExpanded(!this.expanded);
        localStorage.setItem(this.storageKey, this.expanded ? 'true' : 'false');
    },

    setExpanded: function (expanded) {
        const panel = document.getElementById('swaparr-activity-panel');
        const chevron = document.getElementById('swaparr-activity-chevron');
        if (!panel) return;

        this.expanded = expanded;
        panel.style.display = this.expanded ? 'block' : 'none';
        if (chevron) chevron.className = this.expanded ? 'fas fa-chevron-up' : 'fas fa-chevron-down';

        if (this.expanded) {
            this.refresh();
            this.startPolling();
        } else {
            this.stopPolling();
        }
    },

    startPolling: function () {
        this.stopPolling();
        this.pollInterval = setInterval(() => {
            if (this.expanded && window.huntarrUI && window.huntarrUI.currentSection === 'home') {
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
        this.loadActivity();
        this.loadHistory();
    },

    loadActivity: function () {
        if (!this.currentInstanceId) return;
        HuntarrUtils.fetchWithTimeout(`./api/swaparr/activity/sonarr/${this.currentInstanceId}`)
            .then(response => response.json())
            .then(data => this.renderQueue(data))
            .catch(error => {
                console.error('[SwaparrActivity] Error loading activity:', error);
                this.renderQueue({ success: false });
            });
    },

    loadHistory: function () {
        if (!this.currentInstanceId) return;
        HuntarrUtils.fetchWithTimeout(`./api/swaparr/activity/sonarr/${this.currentInstanceId}/history?page=${this.historyPage}&page_size=20`)
            .then(response => response.json())
            .then(data => this.renderHistory(data))
            .catch(error => {
                console.error('[SwaparrActivity] Error loading history:', error);
                this.renderHistory({ success: false });
            });
    },

    formatBytes: function (bytes) {
        if (!bytes || bytes <= 0) return '-';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return parseFloat((bytes / Math.pow(1024, i)).toFixed(1)) + ' ' + units[i];
    },

    statusBadgeClass: function (label) {
        const lower = (label || '').toLowerCase();
        if (lower.includes('error')) return 'error';
        if (lower.includes('warning')) return 'warning';
        return '';
    },

    renderQueue: function (data) {
        const tbody = document.querySelector('#swaparr-activity-queue-table tbody');
        if (!tbody) return;

        const queue = (data && data.success && data.queue) ? data.queue : [];
        if (queue.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="swaparr-activity-empty">No active queue items.</td></tr>';
            return;
        }

        tbody.innerHTML = queue.map(item => {
            const status = item.sonarr_status || {};
            const badgeClass = this.statusBadgeClass(status.label);
            const statusHtml = badgeClass
                ? `<span class="swaparr-activity-badge ${badgeClass}">${status.label}</span>`
                : (status.label || 'Unknown');

            const torrent = item.torrent_status;
            const torrentHtml = torrent ? (torrent.state || '-') : '-';
            const nameHtml = item.episode_count > 1
                ? `${item.name} <span class="swaparr-activity-badge">${item.episode_count} episodes</span>`
                : item.name;

            return `<tr>
                <td>${nameHtml}</td>
                <td>${this.formatBytes(item.size)}</td>
                <td>${statusHtml}${status.detail ? `<div class="swaparr-activity-empty" style="text-align:left;padding:2px 0;">${status.detail}</div>` : ''}</td>
                <td>${item.strikes}/${item.max_strikes}</td>
                <td>${torrentHtml}</td>
            </tr>`;
        }).join('');
    },

    renderHistory: function (data) {
        const tbody = document.querySelector('#swaparr-activity-history-table tbody');
        const pageLabel = document.getElementById('swaparr-activity-history-page-label');
        if (!tbody) return;

        const entries = (data && data.success && data.entries) ? data.entries : [];
        if (pageLabel && data && data.success) {
            pageLabel.textContent = `Page ${data.current_page || 1} of ${data.total_pages || 1}`;
        }

        if (entries.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="swaparr-activity-empty">No history yet.</td></tr>';
            return;
        }

        tbody.innerHTML = entries.map((entry, idx) => {
            const badgeClass = entry.event_type === 'removed' ? 'removed' : (entry.event_type === 'failed' ? 'failed' : 'completed');
            const label = entry.event_type === 'removed' ? 'Removed' : (entry.event_type === 'failed' ? 'Failed' : 'Completed');
            const reasonHtml = entry.reason
                ? `<span class="swaparr-activity-reason-link" data-incident-idx="${idx}">${entry.reason}</span>`
                : '-';
            return `<tr>
                <td>${entry.occurred_at}</td>
                <td>${entry.item_name}</td>
                <td><span class="swaparr-activity-badge ${badgeClass}">${label}</span></td>
                <td>${reasonHtml}</td>
            </tr>`;
        }).join('');

        this._lastHistoryEntries = entries;
        tbody.querySelectorAll('[data-incident-idx]').forEach(el => {
            el.addEventListener('click', () => {
                const entry = this._lastHistoryEntries[parseInt(el.dataset.incidentIdx, 10)];
                if (entry) this.openIncident(entry);
            });
        });
    },

    openIncident: function (entry) {
        let overlay = document.getElementById('swaparr-incident-overlay');
        if (overlay) overlay.remove();

        overlay = document.createElement('div');
        overlay.id = 'swaparr-incident-overlay';
        overlay.className = 'swaparr-incident-overlay';
        overlay.innerHTML = `
            <div class="swaparr-incident-modal">
                <div class="swaparr-incident-header">
                    <h3>${entry.item_name}</h3>
                    <button type="button" class="swaparr-incident-close">&times;</button>
                </div>
                <div class="swaparr-incident-body">
                    <div class="swaparr-incident-loading"><i class="fas fa-spinner fa-spin"></i> Gathering details from Sonarr, Decypharr, NZBDav and Swaparr...</div>
                </div>
            </div>
        `;
        document.body.appendChild(overlay);

        const close = () => overlay.remove();
        overlay.querySelector('.swaparr-incident-close').addEventListener('click', close);
        overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

        if (!this.currentInstanceId) return;
        const params = new URLSearchParams({ name: entry.item_name, item_id: entry.item_id || '' });
        HuntarrUtils.fetchWithTimeout(`./api/swaparr/activity/sonarr/${this.currentInstanceId}/incident?${params.toString()}`)
            .then(response => response.json())
            .then(data => this.renderIncident(overlay, data))
            .catch(() => {
                const body = overlay.querySelector('.swaparr-incident-body');
                if (body) body.innerHTML = '<div class="swaparr-incident-empty">Failed to load details.</div>';
            });
    },

    renderIncident: function (overlay, data) {
        const body = overlay.querySelector('.swaparr-incident-body');
        if (!body) return;

        if (!data || !data.success) {
            body.innerHTML = '<div class="swaparr-incident-empty">Failed to load details.</div>';
            return;
        }

        const timeline = data.timeline || [];
        const extra = data.additional_context || [];

        let html = '';
        if (timeline.length === 0) {
            html += '<div class="swaparr-incident-empty">No timeline events found across Sonarr, Decypharr, NZBDav or Swaparr for this item.</div>';
        } else {
            html += '<ul class="swaparr-incident-timeline">' + timeline.map(t => `
                <li>
                    <span class="ts">${t.timestamp || ''}</span>
                    <span class="src ${(t.source || '').toLowerCase().replace(/\s+/g, '-')}">${t.source}</span>
                    <span class="msg">${t.message}</span>
                </li>`).join('') + '</ul>';
        }

        if (extra.length > 0) {
            html += '<div style="margin-top:16px; font-size:12.5px; color:rgba(160,168,184,0.9); font-weight:600;">Additional context (no timestamp available)</div>';
            html += '<ul class="swaparr-incident-timeline">' + extra.map(t => `
                <li>
                    <span class="ts">-</span>
                    <span class="src ${(t.source || '').toLowerCase().replace(/\s+/g, '-')}">${t.source}</span>
                    <span class="msg">${t.message}</span>
                </li>`).join('') + '</ul>';
        }

        body.innerHTML = html;
    }
};

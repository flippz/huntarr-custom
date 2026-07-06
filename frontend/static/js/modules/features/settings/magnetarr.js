(function () {
    window.SettingsForms = window.SettingsForms || {};

    function notify(message, type) {
        if (window.HuntarrNotifications && window.HuntarrNotifications.showNotification) {
            window.HuntarrNotifications.showNotification(message, type || 'info');
        }
    }

    function escapeHtml(value) {
        if (value === null || value === undefined) return '';
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function formatBytes(bytes) {
        if (!bytes || bytes <= 0) return '-';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return parseFloat((bytes / Math.pow(1024, i)).toFixed(1)) + ' ' + units[i];
    }

    function copyToClipboard(text, label) {
        const done = () => notify((label || 'Value') + ' copied to clipboard', 'success');
        const fail = () => notify('Failed to copy to clipboard', 'error');
        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(done).catch(fail);
        } else {
            try {
                const tmp = document.createElement('textarea');
                tmp.value = text;
                tmp.style.position = 'fixed';
                tmp.style.opacity = '0';
                document.body.appendChild(tmp);
                tmp.select();
                document.execCommand('copy');
                document.body.removeChild(tmp);
                done();
            } catch (e) {
                fail();
            }
        }
    }

    window.SettingsForms.generateMagnetarrForm = function (container, settings = {}) {
        if (!settings || typeof settings !== 'object') {
            settings = {};
        }

        container.setAttribute('data-app-type', 'magnetarr');

        const torznabUrl = window.location.origin + '/api/magnetarr/torznab';
        const apiKey = settings.torznab_api_key || '';

        let html = `
            <div style="margin-bottom: 25px;">
                <div style="display: flex; gap: 15px; flex-wrap: wrap;">
                    <button type="button" id="magnetarr-save-button" disabled style="
                        background: #6b7280;
                        color: #9ca3af;
                        border: 1px solid #4b5563;
                        padding: 8px 16px;
                        border-radius: 6px;
                        font-size: 14px;
                        font-weight: 500;
                        cursor: not-allowed;
                        display: flex;
                        align-items: center;
                        gap: 8px;
                        transition: all 0.2s ease;
                    ">
                        <i class="fas fa-save"></i>
                        Save Changes
                    </button>
                </div>
            </div>

            <div class="settings-group">
                <h3>Magnetarr Configuration</h3>
                <p class="setting-help" style="margin-bottom: 20px; color: #9ca3af;">
                    Magnetarr scrapes configured sources for magnet links and exposes them as a Torznab indexer so *arr apps (via Prowlarr) can search them.
                </p>

                <div class="setting-item">
                    <label for="magnetarr_enabled">
                        Enable Magnetarr:
                    </label>
                    <label class="toggle-switch">
                        <input type="checkbox" id="magnetarr_enabled" ${settings.enabled === true ? 'checked' : ''}>
                        <span class="toggle-slider"></span>
                    </label>
                    <p class="setting-help">Enable periodic scraping of configured sources for magnet links</p>
                </div>
            </div>

            <div class="settings-group">
                <h3>Torznab Indexer</h3>
                <p class="setting-help" style="margin-bottom: 20px; color: #9ca3af;">
                    Add this as a custom Torznab indexer in Prowlarr so Sonarr/Radarr/etc. can search magnets discovered by Magnetarr.
                </p>

                <div class="setting-item">
                    <label for="magnetarr_torznab_url">
                        Indexer URL:
                    </label>
                    <div style="display: flex; align-items: center; gap: 8px; flex: 1;">
                        <input type="text" id="magnetarr_torznab_url" value="${escapeHtml(torznabUrl)}" readonly style="flex: 1; min-width: 0;">
                        <button type="button" id="magnetarr-copy-url-btn" class="tag-add-btn" title="Copy URL">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                    <p class="setting-help">Use this as the Torznab URL when adding a custom indexer in Prowlarr</p>
                </div>

                <div class="setting-item">
                    <label for="magnetarr_torznab_apikey">
                        API Key:
                    </label>
                    <div style="display: flex; align-items: center; gap: 8px; flex: 1;">
                        <input type="text" id="magnetarr_torznab_apikey" value="${escapeHtml(apiKey)}" readonly placeholder="Generated on first use" style="flex: 1; min-width: 0;">
                        <button type="button" id="magnetarr-copy-key-btn" class="tag-add-btn" title="Copy API Key">
                            <i class="fas fa-copy"></i>
                        </button>
                    </div>
                    <p class="setting-help">Auto-generated API key for the Torznab endpoint. Provide this to Prowlarr along with the Indexer URL.</p>
                </div>
            </div>

            <div class="settings-group">
                <h3>Real-Debrid</h3>
                <p class="setting-help" style="margin-bottom: 20px; color: #9ca3af;">
                    Sources with "Send to Real-Debrid" enabled will have newly discovered magnets added to your
                    Real-Debrid account automatically. Get an API token from
                    <a href="https://real-debrid.com/apitoken" target="_blank" rel="noopener">real-debrid.com/apitoken</a>.
                </p>

                <div class="setting-item">
                    <label for="magnetarr_rd_api_token">API Token:</label>
                    <input type="password" id="magnetarr_rd_api_token" value="${escapeHtml(settings.realdebrid_api_token || '')}" placeholder="Real-Debrid API token">
                </div>

                <div class="setting-item">
                    <label for="magnetarr_rd_recheck_minutes">Recheck Interval (minutes):</label>
                    <input type="number" id="magnetarr_rd_recheck_minutes" min="5" value="${settings.realdebrid_recheck_minutes || 20}">
                    <p class="setting-help">How often to check a submitted post for content changes (e.g. a new session added).</p>
                </div>

                <div class="setting-item">
                    <label for="magnetarr_rd_watch_days">Watch Window (days):</label>
                    <input type="number" id="magnetarr_rd_watch_days" min="1" value="${settings.realdebrid_watch_days || 4}">
                    <p class="setting-help">Stop rechecking a post this many days after it was first discovered.</p>
                </div>
            </div>

            <div class="settings-group">
                <h3>Real-Debrid Submissions</h3>
                <p class="setting-help" style="margin-bottom: 15px; color: #9ca3af;">
                    Magnets sent to Real-Debrid and currently being watched for post content changes.
                </p>

                <div style="overflow-x: auto;">
                    <table class="magnetarr-table" id="magnetarr-rd-submissions-table" style="width: 100%; border-collapse: collapse; font-size: 13px;">
                        <thead>
                            <tr>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Title</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Status</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">First Seen</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Last Checked</th>
                            </tr>
                        </thead>
                        <tbody id="magnetarr-rd-submissions-tbody">
                            <tr><td colspan="4" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">Loading...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="settings-group">
                <h3>Sources</h3>
                <p class="setting-help" style="margin-bottom: 15px; color: #9ca3af;">
                    Configure the sites/feeds Magnetarr should scan for magnet links.
                </p>

                <div style="margin-bottom: 15px;">
                    <button type="button" id="magnetarr-add-source-btn" style="
                        background: #2563eb;
                        color: #fff;
                        border: 1px solid #1d4ed8;
                        padding: 8px 16px;
                        border-radius: 6px;
                        font-size: 14px;
                        font-weight: 500;
                        cursor: pointer;
                        display: inline-flex;
                        align-items: center;
                        gap: 8px;
                    ">
                        <i class="fas fa-plus"></i> Add Source
                    </button>
                </div>

                <div style="overflow-x: auto;">
                    <table class="magnetarr-table" id="magnetarr-sources-table" style="width: 100%; border-collapse: collapse; font-size: 13px;">
                        <thead>
                            <tr>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Name</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">URL</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Interval</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Enabled</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Last Scanned</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Status</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Actions</th>
                            </tr>
                        </thead>
                        <tbody id="magnetarr-sources-tbody">
                            <tr><td colspan="7" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">Loading sources...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="settings-group">
                <h3>Recent Discoveries</h3>
                <p class="setting-help" style="margin-bottom: 15px; color: #9ca3af;">
                    Magnets discovered across all sources.
                </p>

                <div style="margin-bottom: 15px; display: flex; gap: 10px; align-items: center;">
                    <input type="text" id="magnetarr-magnets-search" placeholder="Search discoveries..." style="max-width: 320px;">
                    <button type="button" id="magnetarr-magnets-search-btn" class="tag-add-btn" title="Search">
                        <i class="fas fa-search"></i>
                    </button>
                </div>

                <div style="overflow-x: auto;">
                    <table class="magnetarr-table" id="magnetarr-magnets-table" style="width: 100%; border-collapse: collapse; font-size: 13px;">
                        <thead>
                            <tr>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Title</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Source</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Category</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Size</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Discovered</th>
                                <th style="text-align:left; padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.2);">Actions</th>
                            </tr>
                        </thead>
                        <tbody id="magnetarr-magnets-tbody">
                            <tr><td colspan="6" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">Loading discoveries...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Add/Edit Source Modal -->
            <div id="magnetarr-source-modal" class="magnetarr-modal-overlay" style="display: none;">
                <div class="magnetarr-modal">
                    <div class="magnetarr-modal-header">
                        <h3 id="magnetarr-source-modal-title">Add Source</h3>
                        <button type="button" id="magnetarr-source-modal-close" class="magnetarr-modal-close">&times;</button>
                    </div>
                    <div class="magnetarr-modal-body">
                        <input type="hidden" id="magnetarr_source_id" value="">
                        <div class="setting-item">
                            <label for="magnetarr_source_name">Name:</label>
                            <input type="text" id="magnetarr_source_name" placeholder="e.g. My Magnet Feed">
                        </div>
                        <div class="setting-item">
                            <label for="magnetarr_source_url">URL:</label>
                            <input type="text" id="magnetarr_source_url" placeholder="https://example.com/feed">
                        </div>
                        <div class="setting-item">
                            <label for="magnetarr_source_type">Source Type:</label>
                            <input type="text" id="magnetarr_source_type" placeholder="e.g. rss, html">
                        </div>
                        <div class="setting-item">
                            <label for="magnetarr_source_interval">Interval (minutes):</label>
                            <input type="number" id="magnetarr_source_interval" min="1" value="60">
                        </div>
                        <div class="setting-item">
                            <label for="magnetarr_source_enabled">Enabled:</label>
                            <label class="toggle-switch">
                                <input type="checkbox" id="magnetarr_source_enabled" checked>
                                <span class="toggle-slider"></span>
                            </label>
                        </div>
                        <div class="setting-item">
                            <label for="magnetarr_source_rd_auto_add">Send to Real-Debrid:</label>
                            <label class="toggle-switch">
                                <input type="checkbox" id="magnetarr_source_rd_auto_add">
                                <span class="toggle-slider"></span>
                            </label>
                            <p class="setting-help">Automatically add newly discovered magnets from this source to Real-Debrid.</p>
                        </div>
                        <div class="setting-item">
                            <label for="magnetarr_source_rd_keywords">Real-Debrid Keywords:</label>
                            <input type="text" id="magnetarr_source_rd_keywords" placeholder="e.g. Formula 1, Grand Prix">
                            <p class="setting-help">Comma-separated keywords — only magnets whose title contains ALL of them are sent to Real-Debrid. Leave blank to send everything from this source. Discovery/storage/Torznab is unaffected either way.</p>
                        </div>
                    </div>
                    <div class="magnetarr-modal-footer">
                        <button type="button" id="magnetarr-source-modal-cancel" class="magnetarr-btn-secondary">Cancel</button>
                        <button type="button" id="magnetarr-source-modal-save" class="magnetarr-btn-primary">Save Source</button>
                    </div>
                </div>
            </div>
        `;

        container.innerHTML = html;

        window.SettingsForms.setupMagnetarrCopyButtons();
        window.SettingsForms.setupMagnetarrSourceModal();
        window.SettingsForms.loadMagnetarrSources();
        window.SettingsForms.loadMagnetarrMagnets();
        window.SettingsForms.loadMagnetarrTorznabKey();
        window.SettingsForms.loadMagnetarrRealdebridSubmissions();

        const searchBtn = document.getElementById('magnetarr-magnets-search-btn');
        const searchInput = document.getElementById('magnetarr-magnets-search');
        if (searchBtn) {
            searchBtn.addEventListener('click', () => {
                window.SettingsForms.loadMagnetarrMagnets(searchInput ? searchInput.value.trim() : '');
            });
        }
        if (searchInput) {
            searchInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    window.SettingsForms.loadMagnetarrMagnets(searchInput.value.trim());
                }
            });
        }

        const addSourceBtn = document.getElementById('magnetarr-add-source-btn');
        if (addSourceBtn) {
            addSourceBtn.addEventListener('click', () => {
                window.SettingsForms.openMagnetarrSourceModal(null);
            });
        }

        const magnetarrEnabledToggle = container.querySelector('#magnetarr_enabled');
        if (magnetarrEnabledToggle) {
            magnetarrEnabledToggle.addEventListener('change', () => {
                if (window.huntarrUI && window.huntarrUI.originalSettings && window.huntarrUI.originalSettings.magnetarr) {
                    window.huntarrUI.originalSettings.magnetarr.enabled = magnetarrEnabledToggle.checked;
                }

                try {
                    const cachedSettings = localStorage.getItem('huntarr-settings-cache');
                    if (cachedSettings) {
                        const parsed = JSON.parse(cachedSettings);
                        if (!parsed.magnetarr) parsed.magnetarr = {};
                        parsed.magnetarr.enabled = magnetarrEnabledToggle.checked;
                        localStorage.setItem('huntarr-settings-cache', JSON.stringify(parsed));
                    }
                } catch (e) {
                    console.warn('[SettingsForms] Failed to update cached magnetarr settings:', e);
                }
            });
        }

        if (window.SettingsForms.setupMagnetarrManualSave) {
            window.SettingsForms.setupMagnetarrManualSave(container, settings);
        }
    };

    window.SettingsForms.setupMagnetarrCopyButtons = function () {
        const copyUrlBtn = document.getElementById('magnetarr-copy-url-btn');
        const copyKeyBtn = document.getElementById('magnetarr-copy-key-btn');

        if (copyUrlBtn) {
            copyUrlBtn.addEventListener('click', () => {
                const input = document.getElementById('magnetarr_torznab_url');
                if (input) copyToClipboard(input.value, 'Indexer URL');
            });
        }
        if (copyKeyBtn) {
            copyKeyBtn.addEventListener('click', () => {
                const input = document.getElementById('magnetarr_torznab_apikey');
                if (input && input.value) {
                    copyToClipboard(input.value, 'API Key');
                } else {
                    notify('API key not loaded yet, try again in a moment.', 'warning');
                }
            });
        }
    };

    window.SettingsForms.loadMagnetarrTorznabKey = function () {
        HuntarrUtils.fetchWithTimeout('./api/magnetarr/torznab-key')
            .then(response => response.json())
            .then(data => {
                const input = document.getElementById('magnetarr_torznab_apikey');
                if (input && data && data.api_key) {
                    input.value = data.api_key;
                }
            })
            .catch(error => {
                console.error('[Magnetarr] Error loading Torznab API key:', error);
            });
    };

    // ---------------------------------------------------------------------
    // Real-Debrid submissions
    // ---------------------------------------------------------------------

    window.SettingsForms.loadMagnetarrRealdebridSubmissions = function () {
        const tbody = document.getElementById('magnetarr-rd-submissions-tbody');
        if (!tbody) return;

        HuntarrUtils.fetchWithTimeout('./api/magnetarr/realdebrid/submissions')
            .then(response => response.json())
            .then(data => window.SettingsForms.renderMagnetarrRealdebridSubmissions((data && data.submissions) || []))
            .catch(error => {
                console.error('[Magnetarr] Error loading Real-Debrid submissions:', error);
                tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">Failed to load submissions.</td></tr>';
            });
    };

    window.SettingsForms.renderMagnetarrRealdebridSubmissions = function (submissions) {
        const tbody = document.getElementById('magnetarr-rd-submissions-tbody');
        if (!tbody) return;

        if (!submissions || submissions.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">No Real-Debrid submissions yet.</td></tr>';
            return;
        }

        const statusColors = {
            submitted: '#3b82f6',
            downloaded: '#22c55e',
            error: '#ef4444',
            expired: 'rgba(160,168,184,0.7)',
            pending: 'rgba(160,168,184,0.7)',
        };

        tbody.innerHTML = submissions.map(sub => {
            const color = statusColors[sub.status] || 'rgba(160,168,184,0.7)';
            const statusLabel = sub.last_error
                ? `<span style="color:${color};" title="${escapeHtml(sub.last_error)}">${escapeHtml(sub.status)}</span>`
                : `<span style="color:${color};">${escapeHtml(sub.status)}</span>`;
            return `
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1); max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${escapeHtml(sub.title)}">${escapeHtml(sub.title)}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${statusLabel}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${escapeHtml(sub.first_seen_at)}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${escapeHtml(sub.last_checked_at || '-')}</td>
                </tr>
            `;
        }).join('');
    };

    // ---------------------------------------------------------------------
    // Sources
    // ---------------------------------------------------------------------

    window.SettingsForms.loadMagnetarrSources = function () {
        const tbody = document.getElementById('magnetarr-sources-tbody');
        if (!tbody) return;

        HuntarrUtils.fetchWithTimeout('./api/magnetarr/sources')
            .then(response => response.json())
            .then(data => window.SettingsForms.renderMagnetarrSources((data && data.sources) || []))
            .catch(error => {
                console.error('[Magnetarr] Error loading sources:', error);
                tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">Failed to load sources.</td></tr>';
            });
    };

    window.SettingsForms.renderMagnetarrSources = function (sources) {
        const tbody = document.getElementById('magnetarr-sources-tbody');
        if (!tbody) return;

        if (!sources || sources.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">No sources configured yet.</td></tr>';
            return;
        }

        window.SettingsForms._magnetarrSources = sources;

        tbody.innerHTML = sources.map(src => {
            const lastScanned = src.last_scanned_at ? escapeHtml(src.last_scanned_at) : 'Never';
            const status = src.last_error
                ? `<span style="color:#ef4444;" title="${escapeHtml(src.last_error)}"><i class="fas fa-exclamation-triangle"></i> Error</span>`
                : (src.last_scanned_at ? '<span style="color:#22c55e;"><i class="fas fa-check"></i> OK</span>' : '<span style="color:rgba(160,168,184,0.7);">-</span>');
            return `
                <tr data-source-id="${escapeHtml(src.id)}">
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${escapeHtml(src.name)}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1); max-width: 260px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${escapeHtml(src.url)}">${escapeHtml(src.url)}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${escapeHtml(src.interval_minutes)} min</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${src.enabled ? '<span style="color:#22c55e;">Yes</span>' : '<span style="color:#ef4444;">No</span>'}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${lastScanned}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${status}</td>
                    <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1); white-space: nowrap;">
                        <button type="button" class="magnetarr-row-btn" data-action="scan" data-id="${escapeHtml(src.id)}" title="Scan Now"><i class="fas fa-sync-alt"></i></button>
                        <button type="button" class="magnetarr-row-btn" data-action="edit" data-id="${escapeHtml(src.id)}" title="Edit"><i class="fas fa-edit"></i></button>
                        <button type="button" class="magnetarr-row-btn" data-action="delete" data-id="${escapeHtml(src.id)}" title="Delete"><i class="fas fa-trash"></i></button>
                    </td>
                </tr>
            `;
        }).join('');

        tbody.querySelectorAll('[data-action="scan"]').forEach(btn => {
            btn.addEventListener('click', () => window.SettingsForms.scanMagnetarrSource(btn.dataset.id));
        });
        tbody.querySelectorAll('[data-action="edit"]').forEach(btn => {
            btn.addEventListener('click', () => {
                const source = (window.SettingsForms._magnetarrSources || []).find(s => String(s.id) === String(btn.dataset.id));
                window.SettingsForms.openMagnetarrSourceModal(source || null);
            });
        });
        tbody.querySelectorAll('[data-action="delete"]').forEach(btn => {
            btn.addEventListener('click', () => window.SettingsForms.deleteMagnetarrSource(btn.dataset.id));
        });
    };

    window.SettingsForms.scanMagnetarrSource = function (id) {
        HuntarrUtils.fetchWithTimeout(`./api/magnetarr/sources/${id}/scan`, { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                if (data && data.success) {
                    notify(`Scan complete: ${data.found || 0} magnet(s) found`, 'success');
                    window.SettingsForms.loadMagnetarrSources();
                    window.SettingsForms.loadMagnetarrMagnets();
                } else {
                    notify((data && data.error) || 'Scan failed', 'error');
                }
            })
            .catch(error => {
                console.error('[Magnetarr] Error scanning source:', error);
                notify('Scan failed', 'error');
            });
    };

    window.SettingsForms.deleteMagnetarrSource = function (id) {
        const doDelete = () => {
            HuntarrUtils.fetchWithTimeout(`./api/magnetarr/sources/${id}`, { method: 'DELETE' })
                .then(response => response.json())
                .then(data => {
                    if (data && data.success) {
                        notify('Source deleted', 'success');
                        window.SettingsForms.loadMagnetarrSources();
                    } else {
                        notify((data && data.error) || 'Failed to delete source', 'error');
                    }
                })
                .catch(error => {
                    console.error('[Magnetarr] Error deleting source:', error);
                    notify('Failed to delete source', 'error');
                });
        };

        if (window.HuntarrConfirm && window.HuntarrConfirm.show) {
            window.HuntarrConfirm.show({ title: 'Delete Source', message: 'Are you sure you want to delete this source?', confirmLabel: 'Delete', onConfirm: doDelete });
        } else if (confirm('Are you sure you want to delete this source?')) {
            doDelete();
        }
    };

    window.SettingsForms.setupMagnetarrSourceModal = function () {
        const modal = document.getElementById('magnetarr-source-modal');
        if (!modal) return;

        const closeBtn = document.getElementById('magnetarr-source-modal-close');
        const cancelBtn = document.getElementById('magnetarr-source-modal-cancel');
        const saveBtn = document.getElementById('magnetarr-source-modal-save');

        const close = () => { modal.style.display = 'none'; };

        if (closeBtn) closeBtn.addEventListener('click', close);
        if (cancelBtn) cancelBtn.addEventListener('click', close);
        modal.addEventListener('click', (e) => { if (e.target === modal) close(); });

        if (saveBtn) {
            saveBtn.addEventListener('click', () => window.SettingsForms.saveMagnetarrSource());
        }
    };

    window.SettingsForms.openMagnetarrSourceModal = function (source) {
        const modal = document.getElementById('magnetarr-source-modal');
        if (!modal) return;

        const title = document.getElementById('magnetarr-source-modal-title');
        const idInput = document.getElementById('magnetarr_source_id');
        const nameInput = document.getElementById('magnetarr_source_name');
        const urlInput = document.getElementById('magnetarr_source_url');
        const typeInput = document.getElementById('magnetarr_source_type');
        const intervalInput = document.getElementById('magnetarr_source_interval');
        const enabledInput = document.getElementById('magnetarr_source_enabled');
        const rdAutoAddInput = document.getElementById('magnetarr_source_rd_auto_add');
        const rdKeywordsInput = document.getElementById('magnetarr_source_rd_keywords');

        if (source) {
            if (title) title.textContent = 'Edit Source';
            if (idInput) idInput.value = source.id;
            if (nameInput) nameInput.value = source.name || '';
            if (urlInput) urlInput.value = source.url || '';
            if (typeInput) typeInput.value = source.source_type || '';
            if (intervalInput) intervalInput.value = source.interval_minutes || 60;
            if (enabledInput) enabledInput.checked = source.enabled !== false;
            if (rdAutoAddInput) rdAutoAddInput.checked = !!source.realdebrid_auto_add;
            if (rdKeywordsInput) rdKeywordsInput.value = source.realdebrid_keywords || '';
        } else {
            if (title) title.textContent = 'Add Source';
            if (idInput) idInput.value = '';
            if (nameInput) nameInput.value = '';
            if (urlInput) urlInput.value = '';
            if (typeInput) typeInput.value = '';
            if (intervalInput) intervalInput.value = 60;
            if (enabledInput) enabledInput.checked = true;
            if (rdAutoAddInput) rdAutoAddInput.checked = false;
            if (rdKeywordsInput) rdKeywordsInput.value = '';
        }

        modal.style.display = 'flex';
    };

    window.SettingsForms.saveMagnetarrSource = function () {
        const idInput = document.getElementById('magnetarr_source_id');
        const nameInput = document.getElementById('magnetarr_source_name');
        const urlInput = document.getElementById('magnetarr_source_url');
        const typeInput = document.getElementById('magnetarr_source_type');
        const intervalInput = document.getElementById('magnetarr_source_interval');
        const enabledInput = document.getElementById('magnetarr_source_enabled');
        const rdAutoAddInput = document.getElementById('magnetarr_source_rd_auto_add');
        const rdKeywordsInput = document.getElementById('magnetarr_source_rd_keywords');

        const name = nameInput ? nameInput.value.trim() : '';
        const url = urlInput ? urlInput.value.trim() : '';

        if (!name || !url) {
            notify('Name and URL are required', 'error');
            return;
        }

        const payload = {
            name: name,
            url: url,
            source_type: typeInput ? typeInput.value.trim() : '',
            interval_minutes: intervalInput ? parseInt(intervalInput.value) || 60 : 60,
            enabled: enabledInput ? enabledInput.checked : true,
            realdebrid_auto_add: rdAutoAddInput ? rdAutoAddInput.checked : false,
            realdebrid_keywords: rdKeywordsInput ? rdKeywordsInput.value.trim() : '',
        };

        const id = idInput ? idInput.value : '';
        const isEdit = !!id;
        const endpoint = isEdit ? `./api/magnetarr/sources/${id}` : './api/magnetarr/sources';
        const method = isEdit ? 'PUT' : 'POST';

        HuntarrUtils.fetchWithTimeout(endpoint, {
            method: method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        })
            .then(response => response.json())
            .then(data => {
                if (data && data.success) {
                    notify(isEdit ? 'Source updated' : 'Source added', 'success');
                    const modal = document.getElementById('magnetarr-source-modal');
                    if (modal) modal.style.display = 'none';
                    window.SettingsForms.loadMagnetarrSources();
                } else {
                    notify((data && data.error) || 'Failed to save source', 'error');
                }
            })
            .catch(error => {
                console.error('[Magnetarr] Error saving source:', error);
                notify('Failed to save source', 'error');
            });
    };

    // ---------------------------------------------------------------------
    // Recent discoveries
    // ---------------------------------------------------------------------

    window.SettingsForms.loadMagnetarrMagnets = function (query) {
        const tbody = document.getElementById('magnetarr-magnets-tbody');
        if (!tbody) return;

        const params = new URLSearchParams({ page_size: '25' });
        if (query) params.set('q', query);

        HuntarrUtils.fetchWithTimeout(`./api/magnetarr/magnets?${params.toString()}`)
            .then(response => response.json())
            .then(data => window.SettingsForms.renderMagnetarrMagnets((data && data.items) || []))
            .catch(error => {
                console.error('[Magnetarr] Error loading discoveries:', error);
                tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">Failed to load discoveries.</td></tr>';
            });
    };

    window.SettingsForms.renderMagnetarrMagnets = function (items) {
        const tbody = document.getElementById('magnetarr-magnets-tbody');
        if (!tbody) return;

        if (!items || items.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; padding: 14px; color: rgba(160,168,184,0.7);">No discoveries yet.</td></tr>';
            return;
        }

        window.SettingsForms._magnetarrMagnets = items;

        tbody.innerHTML = items.map(item => `
            <tr data-magnet-id="${escapeHtml(item.id)}">
                <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1); max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${escapeHtml(item.title)}">${escapeHtml(item.title)}</td>
                <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${escapeHtml(item.source_name)}</td>
                <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${escapeHtml(item.category || '-')}</td>
                <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${formatBytes(item.size_bytes)}</td>
                <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">${escapeHtml(item.discovered_at)}</td>
                <td style="padding: 8px; border-bottom: 1px solid rgba(160,168,184,0.1);">
                    <button type="button" class="magnetarr-row-btn" data-action="copy-magnet" data-id="${escapeHtml(item.id)}" title="Copy magnet link"><i class="fas fa-copy"></i></button>
                </td>
            </tr>
        `).join('');

        tbody.querySelectorAll('[data-action="copy-magnet"]').forEach(btn => {
            btn.addEventListener('click', () => {
                const item = (window.SettingsForms._magnetarrMagnets || []).find(m => String(m.id) === String(btn.dataset.id));
                if (item && item.magnet_uri) {
                    copyToClipboard(item.magnet_uri, 'Magnet link');
                }
            });
        });
    };

    // ---------------------------------------------------------------------
    // Save button / dirty tracking (mirrors Swaparr's pattern)
    // ---------------------------------------------------------------------

    window.SettingsForms.setupMagnetarrManualSave = function (container, originalSettings = {}) {
        const saveButton = container.querySelector('#magnetarr-save-button');
        if (!saveButton) return;

        saveButton.disabled = true;
        saveButton.style.background = '#6b7280';
        saveButton.style.color = '#9ca3af';
        saveButton.style.borderColor = '#4b5563';
        saveButton.style.cursor = 'not-allowed';

        let hasChanges = false;

        const updateSaveButtonState = (changesDetected) => {
            hasChanges = changesDetected;
            const btn = container.querySelector('#magnetarr-save-button');
            if (!btn) return;

            if (hasChanges) {
                btn.disabled = false;
                btn.style.background = '#dc2626';
                btn.style.color = '#ffffff';
                btn.style.borderColor = '#dc2626';
                btn.style.cursor = 'pointer';
            } else {
                btn.disabled = true;
                btn.style.background = '#6b7280';
                btn.style.color = '#9ca3af';
                btn.style.borderColor = '#4b5563';
                btn.style.cursor = 'not-allowed';
            }
        };

        const enabledToggle = container.querySelector('#magnetarr_enabled');
        if (enabledToggle) {
            enabledToggle.addEventListener('change', () => updateSaveButtonState(true));
        }

        const rdInputs = [
            container.querySelector('#magnetarr_rd_api_token'),
            container.querySelector('#magnetarr_rd_recheck_minutes'),
            container.querySelector('#magnetarr_rd_watch_days'),
        ];
        rdInputs.forEach(input => {
            if (input) input.addEventListener('input', () => updateSaveButtonState(true));
        });

        const newSaveButton = saveButton.cloneNode(true);
        saveButton.parentNode.replaceChild(newSaveButton, saveButton);

        newSaveButton.addEventListener('click', () => {
            if (!hasChanges) return;

            newSaveButton.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Saving...';
            newSaveButton.disabled = true;

            const settings = { ...originalSettings };
            const enabled = document.getElementById('magnetarr_enabled');
            if (enabled) settings.enabled = enabled.checked;
            const rdToken = document.getElementById('magnetarr_rd_api_token');
            if (rdToken) settings.realdebrid_api_token = rdToken.value.trim();
            const rdRecheckMinutes = document.getElementById('magnetarr_rd_recheck_minutes');
            if (rdRecheckMinutes) settings.realdebrid_recheck_minutes = parseInt(rdRecheckMinutes.value) || 20;
            const rdWatchDays = document.getElementById('magnetarr_rd_watch_days');
            if (rdWatchDays) settings.realdebrid_watch_days = parseInt(rdWatchDays.value) || 4;

            window.SettingsForms.saveAppSettings('magnetarr', settings);

            newSaveButton.innerHTML = '<i class="fas fa-save"></i> Save Changes';
            updateSaveButtonState(false);
        });
    };
})();

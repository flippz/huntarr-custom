(function() {
    window.SettingsForms = window.SettingsForms || {};

    window.SettingsForms.generateNzbdavForm = function(container, settings = {}) {
        if (!settings || typeof settings !== "object") {
            settings = {};
        }

        container.setAttribute("data-app-type", "nzbdav");

        let nzbdavHtml = `
            <div class="settings-group">
                <h3>NZBDav Configuration</h3>
                <p class="setting-help" style="margin: 0 0 16px 0; color: #94a3b8; font-size: 0.85rem;">
                    NZBDav handles Usenet downloads alongside your torrent/debrid client. Connecting it here lets
                    Swaparr's Activity history show NZBDav's own failure reason (e.g. a missing Usenet article)
                    instead of just "Failed in Sonarr".
                </p>
                <div class="instance-card-grid" id="nzbdav-instances-grid">
        `;

        const nzbdavInstance = {
            name: 'NZBDav',
            api_url: settings.host ? `${settings.host}:${settings.port || 3123}` : '',
            api_key: settings.api_key || '',
            enabled: settings.enabled === true
        };

        nzbdavHtml += window.SettingsForms.renderInstanceCard('nzbdav', nzbdavInstance, 0, { hideDelete: true });

        nzbdavHtml += `
                </div>
            </div>
        `;

        container.innerHTML = nzbdavHtml;

        const grid = container.querySelector('#nzbdav-instances-grid');
        if (grid) {
            grid.addEventListener('click', (e) => {
                const editBtn = e.target.closest('.btn-card.edit');
                if (editBtn) {
                    window.SettingsForms.openNzbdavModal();
                }
            });
        }

        // Test connection after rendering (custom, since NZBDav uses host/port/api_key, not a single api_url)
        setTimeout(() => {
            window.SettingsForms.testNzbdavConnectionCard(settings);
        }, 100);
    };

    window.SettingsForms.testNzbdavConnectionCard = function(settings) {
        if (!settings.enabled) {
            window.SettingsForms.updateInstanceStatusIcon('nzbdav', 0, 'disabled');
            return;
        }
        if (!settings.host || !settings.api_key) {
            window.SettingsForms.updateInstanceStatusIcon('nzbdav', 0, 'error');
            return;
        }
        window.SettingsForms.updateInstanceStatusIcon('nzbdav', 0, 'loading');
        fetch('./api/nzbdav/test-connection', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                host: settings.host,
                port: settings.port || 3123,
                api_key: settings.api_key,
                use_ssl: !!settings.use_ssl
            })
        })
        .then(response => response.json())
        .then(data => {
            window.SettingsForms.updateInstanceStatusIcon('nzbdav', 0, data.success ? 'connected' : 'error');
        })
        .catch(() => {
            window.SettingsForms.updateInstanceStatusIcon('nzbdav', 0, 'error');
        });
    };

    window.SettingsForms.openNzbdavModal = function() {
        const settings = window.huntarrUI.originalSettings.nzbdav || {};

        const titleEl = document.getElementById('instance-editor-title');
        if (titleEl) {
            titleEl.textContent = `Edit NZBDav Configuration`;
        }

        const contentEl = document.getElementById('instance-editor-content');
        if (contentEl) {
            contentEl.innerHTML = `
                <div class="editor-grid">
                    <div class="editor-section">
                        <div class="editor-section-title">
                            Connection Details
                            <span id="nzbdav-connection-status" class="connection-status-badge status-unknown">
                                <i class="fas fa-question-circle"></i> Not Tested
                            </span>
                        </div>

                        <div class="editor-field-group">
                            <div class="editor-setting-item">
                                <label style="display: flex; align-items: center; color: #f8fafc; font-weight: 500;">
                                    <span>Enable Status </span>
                                    <i id="enable-status-icon" class="fas ${settings.enabled ? 'fa-check-circle' : 'fa-minus-circle'}" style="color: ${settings.enabled ? '#10b981' : '#ef4444'}; font-size: 1.1rem; margin-left: 6px;"></i>
                                </label>
                                <select id="editor-enabled" onchange="window.SettingsForms.updateEnableStatusIcon && window.SettingsForms.updateEnableStatusIcon();" style="width: 100%; padding: 10px; border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.2); background: rgba(15, 23, 42, 0.5); color: white;">
                                    <option value="true" ${settings.enabled ? 'selected' : ''}>Enabled</option>
                                    <option value="false" ${!settings.enabled ? 'selected' : ''}>Disabled</option>
                                </select>
                            </div>
                            <p class="setting-help" style="margin: 0 0 20px 0; color: #94a3b8; font-size: 0.85rem;">Enable or disable the NZBDav status integration</p>
                        </div>

                        <div class="setting-item" style="margin-bottom: 20px;">
                            <label style="display: block; color: #f8fafc; font-weight: 500; margin-bottom: 8px;">Host</label>
                            <input type="text" id="editor-nzbdav-host" value="${settings.host || ''}" placeholder="192.168.1.15 or localhost" style="width: 100%; padding: 10px; border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.2); background: rgba(15, 23, 42, 0.5); color: white;">
                            <p class="setting-help" style="margin-top: 5px; color: #94a3b8; font-size: 0.85rem;">The hostname or IP NZBDav is running on (no http:// prefix)</p>
                        </div>

                        <div class="setting-item" style="margin-bottom: 20px;">
                            <label style="display: block; color: #f8fafc; font-weight: 500; margin-bottom: 8px;">Port</label>
                            <input type="number" id="editor-nzbdav-port" value="${settings.port || 3123}" min="1" max="65535" style="width: 100%; padding: 10px; border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.2); background: rgba(15, 23, 42, 0.5); color: white;">
                        </div>

                        <div class="setting-item" style="margin-bottom: 20px;">
                            <label style="display: block; color: #f8fafc; font-weight: 500; margin-bottom: 8px;">API Key</label>
                            <input type="text" id="editor-nzbdav-apikey" value="${settings.api_key || ''}" placeholder="Your NZBDav API Key" style="width: 100%; padding: 10px; border-radius: 8px; border: 1px solid rgba(148, 163, 184, 0.2); background: rgba(15, 23, 42, 0.5); color: white;">
                            <p class="setting-help" style="margin-top: 5px; color: #94a3b8; font-size: 0.85rem;">Found in NZBDav's own Settings page (Sonarr's download client config always shows this masked, so it must be entered here separately)</p>
                        </div>

                        <div class="setting-item" style="margin-bottom: 0;">
                            <label style="display: flex; align-items: center; color: #f8fafc; font-weight: 500; margin-bottom: 8px;">
                                <input type="checkbox" id="editor-nzbdav-ssl" ${settings.use_ssl ? 'checked' : ''} style="margin-right: 8px;">
                                Use SSL/HTTPS
                            </label>
                        </div>
                    </div>
                </div>
            `;

            const hostInput = document.getElementById('editor-nzbdav-host');
            const portInput = document.getElementById('editor-nzbdav-port');
            const keyInput = document.getElementById('editor-nzbdav-apikey');
            const sslInput = document.getElementById('editor-nzbdav-ssl');

            const testConnection = () => {
                const statusBadge = document.getElementById('nzbdav-connection-status');
                if (!statusBadge) return;

                const enabledEl = document.getElementById('editor-enabled');
                if (enabledEl && enabledEl.value === 'false') {
                    statusBadge.className = 'connection-status-badge status-disabled';
                    statusBadge.innerHTML = '<i class="fas fa-ban"></i> Disabled';
                    statusBadge.style.color = '#94a3b8';
                    statusBadge.style.opacity = '0.9';
                    return;
                }

                const host = hostInput.value.trim();
                const port = parseInt(portInput.value, 10) || 3123;
                const apiKey = keyInput.value.trim();
                statusBadge.removeAttribute('style');

                if (!host || !apiKey) {
                    statusBadge.className = 'connection-status-badge status-error';
                    statusBadge.innerHTML = '<i class="fas fa-exclamation-circle"></i> Missing Host or API Key';
                    return;
                }

                statusBadge.className = 'connection-status-badge status-testing';
                statusBadge.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Testing Connection...';

                fetch('./api/nzbdav/test-connection', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ host, port, api_key: apiKey, use_ssl: sslInput.checked })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        const version = data.version ? ` (${data.version})` : '';
                        statusBadge.className = 'connection-status-badge status-success';
                        statusBadge.innerHTML = `<i class="fas fa-check-circle"></i> Connected${version}`;
                    } else {
                        statusBadge.className = 'connection-status-badge status-error';
                        statusBadge.innerHTML = `<i class="fas fa-times-circle"></i> Connection Failed${data.message ? ': ' + data.message : ''}`;
                    }
                })
                .catch(() => {
                    statusBadge.className = 'connection-status-badge status-error';
                    statusBadge.innerHTML = '<i class="fas fa-times-circle"></i> Connection Test Failed';
                });
            };

            if (hostInput) hostInput.addEventListener('blur', testConnection);
            if (portInput) portInput.addEventListener('blur', testConnection);
            if (keyInput) keyInput.addEventListener('blur', testConnection);
            if (sslInput) sslInput.addEventListener('change', testConnection);
            const enabledSelect = document.getElementById('editor-enabled');
            if (enabledSelect) enabledSelect.addEventListener('change', testConnection);

            setTimeout(() => testConnection(), 100);
        }

        const saveBtn = document.getElementById('instance-editor-save');
        const cancelBtn = document.getElementById('instance-editor-cancel');
        const backBtn = document.getElementById('instance-editor-back');

        if (saveBtn) {
            saveBtn.onclick = () => window.SettingsForms.saveNzbdavFromEditor();
        }
        const navigateBack = () => {
            if (window.SettingsForms.clearInstanceEditorDirty) {
                window.SettingsForms.clearInstanceEditorDirty();
            }
            window.huntarrUI.switchSection('nzbdav');
        };
        if (cancelBtn) {
            cancelBtn.onclick = () => {
                if (window.SettingsForms.isInstanceEditorDirty && window.SettingsForms.isInstanceEditorDirty()) {
                    window.SettingsForms.confirmLeaveInstanceEditor((result) => {
                        if (result === 'discard') navigateBack();
                    });
                } else {
                    navigateBack();
                }
            };
        }
        if (backBtn) {
            backBtn.onclick = () => {
                if (window.SettingsForms.isInstanceEditorDirty && window.SettingsForms.isInstanceEditorDirty()) {
                    window.SettingsForms.confirmLeaveInstanceEditor((result) => {
                        if (result === 'discard') navigateBack();
                    });
                } else {
                    navigateBack();
                }
            };
        }

        window.huntarrUI.switchSection('instance-editor');

        setTimeout(() => {
            if (window.SettingsForms.setupEditorChangeDetection) {
                window.SettingsForms.setupEditorChangeDetection();
            }
        }, 100);
    };

    window.SettingsForms.saveNzbdavFromEditor = function() {
        const settings = window.huntarrUI.originalSettings.nzbdav || {};
        const enabledEl = document.getElementById('editor-enabled');

        settings.enabled = enabledEl ? enabledEl.value === 'true' : settings.enabled;
        settings.host = document.getElementById('editor-nzbdav-host').value.trim();
        settings.port = parseInt(document.getElementById('editor-nzbdav-port').value, 10) || 3123;
        settings.api_key = document.getElementById('editor-nzbdav-apikey').value.trim();
        settings.use_ssl = document.getElementById('editor-nzbdav-ssl').checked;
        settings.name = 'NZBDav';

        window.huntarrUI.originalSettings.nzbdav = settings;
        window.SettingsForms.saveAppSettings('nzbdav', settings);

        if (window.SettingsForms.clearInstanceEditorDirty) {
            window.SettingsForms.clearInstanceEditorDirty();
        }

        const saveBtn = document.getElementById('instance-editor-save');
        if (saveBtn) {
            const originalText = saveBtn.innerHTML;
            saveBtn.innerHTML = '<i class="fas fa-check"></i> Saved!';
            saveBtn.style.opacity = '0.7';
            setTimeout(() => {
                saveBtn.innerHTML = originalText;
                saveBtn.style.opacity = '';
            }, 1500);
        }
    };

})();

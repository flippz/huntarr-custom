/**
 * Magnetarr Card Module
 * Home-page status card for Magnetarr: total magnets discovered, sources count,
 * last scan time, session stats, and a small recent-discoveries list.
 */

window.HuntarrMagnetarr = {

    loadMagnetarrStatus: function () {
        HuntarrUtils.fetchWithTimeout('./api/magnetarr/status')
            .then(response => response.json())
            .then(data => {
                const magnetarrCard = document.getElementById('magnetarrStatusCard');
                if (!magnetarrCard) return;

                if (data.enabled && data.configured) {
                    magnetarrCard.style.display = 'block';

                    const formatNumber = window.HuntarrStats ?
                        window.HuntarrStats.formatLargeNumber.bind(window.HuntarrStats) :
                        (n => (n || 0).toString());

                    const totalMagnetsEl = document.getElementById('magnetarr-total-magnets');
                    const sourcesCountEl = document.getElementById('magnetarr-sources-count');
                    if (totalMagnetsEl) totalMagnetsEl.textContent = formatNumber(data.total_magnets || 0);
                    if (sourcesCountEl) sourcesCountEl.textContent = formatNumber(data.sources_count || 0);

                    const sessionStats = data.session_stats || {};
                    const scannedEl = document.getElementById('magnetarr-session-scanned');
                    const foundEl = document.getElementById('magnetarr-session-found');
                    if (scannedEl) scannedEl.textContent = formatNumber(sessionStats.scanned || 0);
                    if (foundEl) foundEl.textContent = formatNumber(sessionStats.found || 0);

                    this.loadRecentDiscoveries();
                } else {
                    magnetarrCard.style.display = 'none';
                }
            })
            .catch(error => {
                console.error('Error loading Magnetarr status:', error);
                const magnetarrCard = document.getElementById('magnetarrStatusCard');
                if (magnetarrCard) magnetarrCard.style.display = 'none';
            });
    },

    loadRecentDiscoveries: function () {
        const list = document.getElementById('magnetarr-recent-discoveries');
        if (!list) return;

        HuntarrUtils.fetchWithTimeout('./api/magnetarr/magnets?page_size=5')
            .then(response => response.json())
            .then(data => {
                const items = (data && data.items) || [];
                if (items.length === 0) {
                    list.innerHTML = '<li class="magnetarr-card-empty">No discoveries yet.</li>';
                    return;
                }
                list.innerHTML = items.map(item => `
                    <li class="magnetarr-card-item">
                        <span class="magnetarr-card-item-title" title="${item.title}">${item.title}</span>
                        <span class="magnetarr-card-item-meta">${item.source_name || ''}</span>
                    </li>
                `).join('');
            })
            .catch(error => {
                console.error('Error loading Magnetarr discoveries:', error);
                list.innerHTML = '<li class="magnetarr-card-empty">Failed to load discoveries.</li>';
            });
    },

    setupMagnetarrStatusPolling: function () {
        // Load initial status
        this.loadMagnetarrStatus();

        // Set up polling to refresh Magnetarr status every 30 seconds
        setInterval(() => {
            if (window.huntarrUI && window.huntarrUI.currentSection === 'home') {
                this.loadMagnetarrStatus();
            }
        }, 30000);
    }
};

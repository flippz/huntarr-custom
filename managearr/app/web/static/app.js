const THEME_STORAGE_KEY = 'managearr-theme';

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const toggle = document.getElementById('theme-toggle');
  if (toggle) toggle.textContent = theme === 'light' ? 'Dark theme' : 'Light theme';
  const settingsLabel = document.getElementById('settings-theme-current');
  if (settingsLabel) settingsLabel.textContent = theme === 'light' ? 'Light' : 'Dark';
}

function initTheme() {
  const stored = localStorage.getItem(THEME_STORAGE_KEY);
  applyTheme(stored === 'light' ? 'light' : 'dark');
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
  const next = current === 'light' ? 'dark' : 'light';
  localStorage.setItem(THEME_STORAGE_KEY, next);
  applyTheme(next);
}

function initNavDrawer() {
  const openBtn = document.getElementById('nav-open');
  const closeBtn = document.getElementById('nav-close');
  const backdrop = document.getElementById('sidebar-backdrop');
  const close = () => document.body.classList.remove('nav-open');
  if (openBtn) openBtn.addEventListener('click', () => document.body.classList.add('nav-open'));
  if (closeBtn) closeBtn.addEventListener('click', close);
  if (backdrop) backdrop.addEventListener('click', close);
}

initTheme();
document.addEventListener('DOMContentLoaded', () => {
  initNavDrawer();
  const toggle = document.getElementById('theme-toggle');
  if (toggle) toggle.addEventListener('click', toggleTheme);
  const settingsToggle = document.getElementById('settings-theme-toggle');
  if (settingsToggle) settingsToggle.addEventListener('click', toggleTheme);
});

async function api(path, { method = 'GET', body } = {}) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }

  // A page can finish loading just as the preview container is replaced.
  // Retry idempotent reads briefly; never retry a write automatically.
  const attempts = method === 'GET' ? 3 : 1;
  let res;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      res = await fetch(path, opts);
      break;
    } catch (error) {
      if (attempt === attempts - 1) throw error;
      await new Promise(resolve => setTimeout(resolve, 250 * (attempt + 1)));
    }
  }
  if (res.status === 204) return null;
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const message = (data.errors && data.errors.join(', ')) || res.statusText;
    throw new Error(message);
  }
  return data;
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function escapeHtml(value) {
  const div = document.createElement('div');
  div.textContent = String(value);
  return div.innerHTML;
}

function setSelectOptions(select, items, labelOf, valueOf) {
  // Builds <option> elements via the DOM Option constructor/textContent,
  // never innerHTML string concatenation - operator-entered text (a
  // library name, a download client name) can contain arbitrary markup,
  // and the Option constructor's label argument is always plain text,
  // never parsed as HTML, so this is safe regardless of content.
  select.textContent = '';
  for (const item of items) {
    select.appendChild(new Option(String(labelOf(item)), String(valueOf(item))));
  }
}

function formatLocalTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value || '-';
  const pad = number => String(number).padStart(2, '0');
  return `${pad(date.getDate())}-${pad(date.getMonth() + 1)}-${String(date.getFullYear()).slice(-2)} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function resultClass(result) {
  return String(result || '').toLowerCase().replaceAll(' ', '-');
}

let toastTimer = null;
function toast(message) {
  const el = document.getElementById('toast');
  if (!el) return;
  el.textContent = message;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.hidden = true; }, 4000);
}

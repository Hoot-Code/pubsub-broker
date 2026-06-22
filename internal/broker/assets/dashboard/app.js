// app.js — Bootstrap: auth check, router, shared utils. ES modules, zero deps.
// Each section is a separate JS module imported via ES module imports.

const app = {
  session: null,
  activeSection: null,
  sectionModules: {},
};

// ── Session bootstrap ──────────────────────────────────────────────────

async function initSession() {
  try {
    const res = await fetch('/dashboard/session');
    if (res.status === 401) {
      window.location.replace('/dashboard?expired=1');
      return;
    }
    if (!res.ok) throw new Error('session ' + res.status);
    app.session = await res.json();
  } catch {
    window.location.replace('/dashboard?expired=1');
    return;
  }
  renderTopBar();
}

function renderTopBar() {
  const s = app.session;
  if (!s) return;
  document.getElementById('sessionInfo').textContent = s.client_id + ' · ' + s.role;
  document.getElementById('logoutBtn').style.display = '';
}

// ── Navigation ─────────────────────────────────────────────────────────

const NAV_ITEMS = [
  { id: 'overview',     label: 'Overview',         icon: '⊞' },
  { id: 'topics',       label: 'Topics',           icon: '◆' },
  { id: 'partitions',   label: 'Partitions',       icon: '▦' },
  { id: 'consumers',    label: 'Consumer Groups',  icon: '⧫' },
  { id: 'explorer',     label: 'Live Explorer',    icon: '◎' },
  { id: 'dlq',          label: 'DLQ',              icon: '⊘' },
  { id: 'cluster',      label: 'Cluster',          icon: '⬡' },
  { id: 'metrics',      label: 'Metrics',          icon: '⊿' },
  { id: 'audit',        label: 'Audit Logs',       icon: '☰' },
  { id: 'settings',     label: 'Settings',         icon: '⚙' },
];

function renderNav() {
  const nav = document.getElementById('nav');
  nav.innerHTML = '';
  for (const item of NAV_ITEMS) {
    const a = document.createElement('a');
    a.href = '#' + item.id;
    a.dataset.section = item.id;
    a.innerHTML = `<span class="nav-icon">${item.icon}</span>${item.label}`;
    a.addEventListener('click', (e) => {
      e.preventDefault();
      navigateTo(item.id);
    });
    nav.appendChild(a);
  }
}

function navigateTo(sectionId) {
  for (const [id, mod] of Object.entries(app.sectionModules)) {
    if (id !== sectionId && mod?.deactivate) {
      try { mod.deactivate(); } catch (e) { console.error(e); }
    }
  }

  app.activeSection = sectionId;
  // Use replaceState instead of assigning window.location.hash directly.
  // Assigning the hash fires a hashchange event, which can re-trigger
  // navigateTo for a stale hash if the user navigated quickly — causing
  // the dashboard to jump back to a previous section.
  history.replaceState(null, '', '#' + sectionId);

  document.querySelectorAll('#nav a').forEach(a => {
    a.classList.toggle('active', a.dataset.section === sectionId);
  });

  // Create a fresh container div inside #section for every navigation.
  // This isolates the new section from any in-flight async renders that
  // belong to the previous section: when those fetches complete they will
  // write to the old, now-detached div and the updates are invisible.
  const sectionHost = document.getElementById('section');
  sectionHost.innerHTML = '';
  const container = document.createElement('div');
  container.className = 'section-fullscreen';
  container.style.cssText = 'flex:1;min-height:0;display:flex;flex-direction:column';
  container.innerHTML = '<div class="loading">Loading…</div>';
  sectionHost.appendChild(container);

  const mod = app.sectionModules[sectionId];
  if (mod && mod.activate) {
    mod.activate(container, app.session);
  }
}

// ── RBAC helpers ───────────────────────────────────────────────────────

function isAdmin() { return app.session?.role === 'admin'; }
function isViewer() { return app.session?.role === 'viewer'; }
function canWrite() { return !isViewer(); }
function canAdmin() { return isAdmin(); }

// ── Shared fetch helper ────────────────────────────────────────────────

async function apiFetch(url, opts) {
  try {
    const res = await fetch(url, opts);
    if (res.status === 401) {
      window.location.replace('/dashboard?expired=1');
      return null;
    }
    return res;
  } catch {
    return null;
  }
}

// ── Shared UI helpers ──────────────────────────────────────────────────

function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (k === 'className') el.className = v;
      else if (k === 'onclick') el.addEventListener('click', v);
      else if (k === 'innerHTML') el.innerHTML = v;
      else if (k.startsWith('data-')) el.setAttribute(k, v);
      else el.setAttribute(k, v);
    }
  }
  for (const child of children) {
    if (child == null) continue;
    if (child instanceof Node) el.appendChild(child);
    else el.appendChild(document.createTextNode(String(child)));
  }
  return el;
}

function badge(text, color) {
  return h('span', { className: 'badge badge-' + color }, text);
}

function statusDot(ok) {
  return h('span', { className: 'status-dot ' + (ok ? 'green' : 'red') });
}

function formatTime(ts) {
  if (!ts) return '—';
  const d = new Date(ts);
  return d.toLocaleTimeString();
}

function formatBytes(b) {
  if (b === undefined || b === null) return '—';
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(1) + ' MB';
}

function formatNumber(n) {
  if (n === undefined || n === null) return '—';
  return n.toLocaleString();
}

function rbacGate(writeNeeded, adminNeeded) {
  if (adminNeeded && !canAdmin()) return 'disabled';
  if (writeNeeded && !canWrite()) return 'disabled';
  return '';
}

function unreachableBanner(msg) {
  return h('div', { className: 'unreachable' }, '⚠ ' + msg);
}

// ── Section error handler ───────────────────────────────────────────────

function formatSectionError(err, label) {
  return `${label}: ${err?.message || 'unknown error'} (see browser console)`;
}

function safeRun(fn, container, label) {
  Promise.resolve(fn()).catch(err => {
    console.error(`[${label}] section error:`, err);
    container.innerHTML = '';
    container.appendChild(unreachableBanner(formatSectionError(err, label)));
  });
}

// ── Section module registry ────────────────────────────────────────────

function registerSection(id, mod) {
  app.sectionModules[id] = mod;
}

// ── Init ───────────────────────────────────────────────────────────────

async function main() {
  await initSession();
  if (!app.session) return;

  renderNav();
  document.getElementById('logoutBtn').addEventListener('click', async function() {
    this.disabled = true;
    this.textContent = 'Signing out…';
    try {
      await fetch('/dashboard/logout', { method: 'POST' });
    } catch {
      // Proceed with redirect even if the request fails.
    }
    app.session = null;
    try { localStorage.removeItem('pubsub_remember_key'); } catch {}
    window.location.replace('/dashboard?logged_out=1');
  });

  // Import section modules
  const sectionImports = [
    import('/dashboard/overview.js'),
    import('/dashboard/topics.js'),
    import('/dashboard/partitions.js'),
    import('/dashboard/explorer.js'),
    import('/dashboard/consumers.js'),
    import('/dashboard/dlq.js'),
    import('/dashboard/cluster.js'),
    import('/dashboard/metrics.js'),
    import('/dashboard/audit.js'),
    import('/dashboard/settings.js'),
  ];
  await Promise.all(sectionImports);

  // Navigate to hash or default
  const hash = window.location.hash.slice(1);
  const valid = NAV_ITEMS.find(i => i.id === hash);
  navigateTo(valid ? hash : 'overview');

  window.addEventListener('hashchange', () => {
    const h = window.location.hash.slice(1);
    const v = NAV_ITEMS.find(i => i.id === h);
    if (v && v.id !== app.activeSection) navigateTo(v.id);
  });
}

// Expose to section modules
window.__app = app;
window.__apiFetch = apiFetch;
window.__h = h;
window.__badge = badge;
window.__statusDot = statusDot;
window.__formatTime = formatTime;
window.__formatBytes = formatBytes;
window.__formatNumber = formatNumber;
window.__rbacGate = rbacGate;
window.__isAdmin = isAdmin;
window.__isViewer = isViewer;
window.__canWrite = canWrite;
window.__canAdmin = canAdmin;
window.__unreachableBanner = unreachableBanner;
window.__safeRun = safeRun;
window.__formatSectionError = formatSectionError;
window.__registerSection = registerSection;
window.__navigateTo = navigateTo;

main();

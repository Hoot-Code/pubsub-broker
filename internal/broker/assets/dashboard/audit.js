// audit.js — Audit Logs section: table with client-side filtering.
const { h, badge, formatTime, apiFetch, registerSection, unreachableBanner, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, formatTime: g.__formatTime,
    apiFetch: g.__apiFetch, registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    safeRun: g.__safeRun };
})();

let pollTimer = null;
let allEvents = [];
let filterText = '';

function activate(container) {
  safeRun(() => fetchAudit(container), container, 'Audit');
  pollTimer = setInterval(
    () => safeRun(() => fetchAudit(container), container, 'Audit'),
    5000);
}

function deactivate() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

async function fetchAudit(container) {
  const res = await apiFetch('/audit/recent?n=100');
  if (!res?.ok) {
    container.innerHTML = '';
    container.appendChild(h('div', { className: 'section-header' }, h('h2', null, 'Audit Logs')));
    container.appendChild(unreachableBanner('Could not load audit events'));
    return;
  }
  allEvents = await res.json().catch(() => []);
  renderAudit(container);
}

function renderAudit(container) {
  container.innerHTML = '';
  container.classList.add('section-fullscreen');
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Audit Logs'));
  container.appendChild(hdr);

  // Search
  const searchRow = h('div', { className: 'form-row' });
  const searchInput = h('input', {
    type: 'text',
    className: 'search-box',
    id: 'auditSearch',
    placeholder: 'Search by client, type, or topic…',
  });
  searchInput.value = filterText;
  searchInput.addEventListener('input', (e) => {
    filterText = e.target.value.toLowerCase();
    renderAuditTable(container);
  });
  searchRow.appendChild(searchInput);
  searchRow.appendChild(h('span', { style: 'font-size:12px;color:var(--text3);margin-left:auto' },
    'Showing last 100 events (server ring buffer max: 1000)'));
  container.appendChild(searchRow);

  renderAuditTable(container);
}

function renderAuditTable(container) {
  const existingTbl = container.querySelector('.audit-table-wrap');
  if (existingTbl) existingTbl.remove();

  let events = allEvents;
  if (filterText) {
    events = events.filter(e =>
      (e.client_id || '').toLowerCase().includes(filterText) ||
      (e.type || '').toLowerCase().includes(filterText) ||
      (e.topic || '').toLowerCase().includes(filterText) ||
      (e.error || '').toLowerCase().includes(filterText)
    );
  }

  const wrap = h('div', { className: 'audit-table-wrap', style: 'flex:1;overflow:auto' });

  if (events.length === 0) {
    wrap.appendChild(h('div', { className: 'loading' }, 'No matching events'));
    container.appendChild(wrap);
    return;
  }

  const tbl = h('table', { className: 'tbl', style: 'width:100%' });
  tbl.appendChild(h('thead', null,
    h('tr', null,
      h('th', null, 'Time'),
      h('th', null, 'Type'),
      h('th', null, 'Client'),
      h('th', null, 'Topic'),
      h('th', null, 'Result'),
      h('th', null, 'Source'),
      h('th', null, 'Error'),
    )
  ));

  const tbody = h('tbody');
  for (const e of events) {
    const source = e.details?.source || 'api';
    const result = e.success
      ? h('td', null, h('span', { className: 'badge badge-green' }, '✓ Success'))
      : h('td', null, h('span', { className: 'badge badge-red' }, '✗ Fail'));

    tbody.appendChild(h('tr', null,
      h('td', { className: 'mono' }, formatTime(e.time)),
      h('td', null, badge(e.type || '—', typeColor(e.type))),
      h('td', { className: 'mono' }, e.client_id || '—'),
      h('td', { className: 'mono' }, e.topic || '—'),
      result,
      h('td', null, badge(source, source === 'dashboard' ? 'blue' : 'gray')),
      h('td', { style: 'color:var(--text3);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap' },
        e.error || '—'),
    ));
  }
  tbl.appendChild(tbody);
  wrap.appendChild(tbl);
  container.appendChild(wrap);
}

function typeColor(type) {
  switch (type) {
    case 'auth': return 'blue';
    case 'publish': return 'green';
    case 'subscribe': return 'cyan';
    case 'create_topic': return 'violet';
    case 'delete_topic': return 'red';
    case 'forbidden': return 'red';
    case 'seek': return 'amber';
    default: return 'gray';
  }
}

registerSection('audit', { activate, deactivate });

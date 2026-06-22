// partitions.js — Partitions detail section: topic selector + partition table.
const { h, badge, formatNumber, formatBytes, apiFetch, registerSection, unreachableBanner, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, formatNumber: g.__formatNumber, formatBytes: g.__formatBytes,
    apiFetch: g.__apiFetch, registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    safeRun: g.__safeRun };
})();

let pollTimer = null;
let selectedTopic = '';
let fetchInFlight = false;

async function fetchPartitions(container) {
  if (fetchInFlight) return;
  fetchInFlight = true;
  try {
    const topicsRes = await apiFetch('/topics');
    if (!topicsRes?.ok) {
      container.innerHTML = '';
      container.appendChild(h('div', { className: 'section-header' }, h('h2', null, 'Partitions')));
      container.appendChild(unreachableBanner('Could not load topics'));
      return;
    }
    const topics = await topicsRes.json().catch(() => []);

    if (!selectedTopic && topics.length > 0) {
      selectedTopic = topics[0].Config?.Name || '';
    }
    renderPartitions(container, topics);
  } finally {
    fetchInFlight = false;
  }
}

async function loadPartitionData(topic) {
  if (!topic) return [];
  const res = await apiFetch('/topics/' + encodeURIComponent(topic) + '/partitions');
  if (!res?.ok) return [];
  return res.json().catch(() => []);
}

async function renderPartitions(container, topics) {
  container.innerHTML = '';
  container.classList.add('section-full');
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Partitions'));
  container.appendChild(hdr);

  if (topics.length === 0) {
    container.appendChild(h('div', { className: 'loading' }, 'No topics found'));
    return;
  }

  // Topic selector
  const formRow = h('div', { className: 'form-row' });
  const sel = h('select', { id: 'topicSelect' });
  for (const t of topics) {
    const name = t.Config?.Name || '';
    const opt = h('option', { value: name }, name);
    if (name === selectedTopic) opt.selected = true;
    sel.appendChild(opt);
  }
  sel.addEventListener('change', async () => {
    selectedTopic = sel.value;
    await renderPartitionTable(container, topics);
  });
  formRow.appendChild(h('label', { style: 'margin-right:8px' }, 'Topic:'));
  formRow.appendChild(sel);
  container.appendChild(formRow);

  await renderPartitionTable(container, topics);
}

async function renderPartitionTable(container, topics) {
  // Remove old table if present
  const oldTbl = container.querySelector('.partition-table-wrap');
  if (oldTbl) oldTbl.remove();

  if (!selectedTopic) return;

  const parts = await loadPartitionData(selectedTopic);

  const wrap = h('div', { className: 'partition-table-wrap' });
  if (parts.length === 0) {
    wrap.appendChild(h('div', { className: 'loading' }, 'No partition data for "' + selectedTopic + '"'));
    container.appendChild(wrap);
    return;
  }

  const tbl = h('table', { className: 'tbl' });
  tbl.appendChild(h('thead', null,
    h('tr', null,
      h('th', null, 'Partition'),
      h('th', null, 'Leader'),
      h('th', null, 'ISR'),
      h('th', null, 'Replicas'),
      h('th', null, 'Segments'),
      h('th', null, 'Size'),
      h('th', null, 'WAL'),
      h('th', null, 'Status'),
    )
  ));

  const tbody = h('tbody');
  for (const p of parts) {
    const status = p.under_replicated
      ? badge('Under-replicated', 'red')
      : badge('Healthy', 'green');
    const walBadge = p.wal_status === 'synced'
      ? badge('Synced', 'green')
      : badge('Pending', 'amber');

    const row = h('tr', null,
      h('td', { className: 'mono' }, String(p.partition)),
      h('td', { className: 'mono' }, p.leader_node_id || '—'),
      h('td', { className: 'mono' }, Array.isArray(p.isr) ? p.isr.join(', ') : '—'),
      h('td', { className: 'mono' }, Array.isArray(p.replicas) ? p.replicas.join(', ') : '—'),
      h('td', { className: 'mono' }, String(p.segment_count || 0)),
      h('td', { className: 'mono' }, formatBytes(p.segment_total_bytes)),
      h('td', null, walBadge),
      h('td', null, status),
    );
    tbody.appendChild(row);
  }
  tbl.appendChild(tbody);
  wrap.appendChild(tbl);
  container.appendChild(wrap);
}

function activate(container) {
  safeRun(() => fetchPartitions(container), container, 'Partitions');
  pollTimer = setInterval(
    () => safeRun(() => fetchPartitions(container), container, 'Partitions'),
    3000);
}

function deactivate() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

registerSection('partitions', { activate, deactivate });

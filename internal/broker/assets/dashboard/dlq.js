// dlq.js — DLQ section: group/topic selector, table, per-row admin actions.
const { h, badge, formatTime, apiFetch, registerSection, unreachableBanner, rbacGate, isAdmin, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, formatTime: g.__formatTime,
    apiFetch: g.__apiFetch, registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    rbacGate: g.__rbacGate, isAdmin: g.__isAdmin, safeRun: g.__safeRun };
})();

let selectedGroup = '';
let selectedTopic = '';

function activate(container) {
  safeRun(() => renderDLQ(container), container, 'DLQ');
}

function deactivate() {}

function renderDLQ(container) {
  container.innerHTML = '';
  container.classList.add('section-fullscreen');
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Dead Letter Queue'));
  container.appendChild(hdr);

  const form = h('div', { className: 'form-row', style: 'width:100%;gap:16px' });

  const groupWrap = h('div', { className: 'form-group', style: 'flex:1;min-width:150px' });
  groupWrap.appendChild(h('label', null, 'Group:'));
  const groupInput = h('input', { type: 'text', id: 'dlqGroup', placeholder: 'consumer-group', style: 'width:100%' });
  groupWrap.appendChild(groupInput);
  form.appendChild(groupWrap);

  const topicWrap = h('div', { className: 'form-group', style: 'flex:1;min-width:150px' });
  topicWrap.appendChild(h('label', null, 'Topic:'));
  const topicInput = h('input', { type: 'text', id: 'dlqTopic', placeholder: 'topic', style: 'width:100%' });
  topicWrap.appendChild(topicInput);
  form.appendChild(topicWrap);

  const loadBtn = h('button', { className: 'btn btn-primary', id: 'dlqLoad', style: 'align-self:flex-end' }, 'Load');
  loadBtn.addEventListener('click', () => loadDLQ(container));

  const purgeBtn = h('button', {
    className: 'btn btn-danger ' + rbacGate(false, true),
    style: 'align-self:flex-end',
  }, 'Purge All');
  if (!isAdmin()) {
    purgeBtn.setAttribute('title', 'admin role required');
  }
  purgeBtn.addEventListener('click', async () => {
    if (!confirm('Delete ALL DLQ entries for this group/topic?')) return;
    const g = groupInput.value.trim();
    const t = topicInput.value.trim();
    const url = '/dlq' + (g || t ? '?' + (g ? 'group=' + encodeURIComponent(g) : '') + (g && t ? '&' : '') + (t ? 'topic=' + encodeURIComponent(t) : '') : '');
    await apiFetch(url, { method: 'DELETE' });
    loadDLQ(container);
  });

  form.appendChild(loadBtn);
  form.appendChild(purgeBtn);
  container.appendChild(form);

  container.appendChild(h('div', { id: 'dlqTable', style: 'flex:1;overflow-y:auto;min-height:0' }));

  const g = groupInput.value.trim();
  const t = topicInput.value.trim();
  if (g || t) loadDLQ(container);
  else {
    document.getElementById('dlqTable').appendChild(
      h('div', { className: 'loading' }, 'Enter a group and/or topic to load DLQ entries'));
  }
}

async function loadDLQ(container) {
  const g = document.getElementById('dlqGroup')?.value.trim() || '';
  const t = document.getElementById('dlqTopic')?.value.trim() || '';
  selectedGroup = g;
  selectedTopic = t;

  const tableDiv = document.getElementById('dlqTable');
  if (!tableDiv) return;

  if (!g && !t) {
    tableDiv.innerHTML = '';
    tableDiv.appendChild(h('div', { className: 'loading' }, 'Enter a group and/or topic to load DLQ entries'));
    return;
  }

  const params = new URLSearchParams();
  if (g) params.set('group', g);
  if (t) params.set('topic', t);
  const res = await apiFetch('/dlq?' + params.toString());
  if (!res?.ok) {
    tableDiv.innerHTML = '';
    tableDiv.appendChild(unreachableBanner('No DLQ entries found (or endpoint unreachable)'));
    return;
  }
  const entries = await res.json().catch(() => []);

  tableDiv.innerHTML = '';
  const tbl = h('table', { className: 'tbl', style: 'width:100%' });
  tbl.appendChild(h('thead', null,
    h('tr', null,
      h('th', null, 'ID'),
      h('th', null, 'Partition'),
      h('th', null, 'Offset'),
      h('th', null, 'Key'),
      h('th', null, 'Attempts'),
      h('th', null, 'Enqueued'),
      h('th', null, ''),
    )
  ));

  const tbody = h('tbody');
  for (const e of entries) {
    const row = h('tr', null,
      h('td', { className: 'mono', style: 'font-size:11px;max-width:120px;overflow:hidden;text-overflow:ellipsis' }, e.id || '—'),
      h('td', { className: 'mono' }, String(e.partition ?? '—')),
      h('td', { className: 'mono' }, String(e.offset ?? '—')),
      h('td', { className: 'mono' }, e.key || '—'),
      h('td', { className: 'mono' }, String(e.attempts ?? 0)),
      h('td', { className: 'mono' }, formatTime(e.enqueued_at)),
    );

    if (isAdmin()) {
      const actions = h('td', { style: 'white-space:nowrap' });
      // Replay
      actions.appendChild(h('button', {
        className: 'btn btn-sm',
        onclick: async () => {
          const params = new URLSearchParams({ group: selectedGroup, topic: selectedTopic, limit: '1' });
          await apiFetch('/dlq/replay?' + params.toString(), { method: 'POST' });
          loadDLQ(container);
        }
      }, 'Replay'));
      // Delete
      actions.appendChild(h('button', {
        className: 'btn btn-sm btn-danger',
        style: 'margin-left:4px',
        onclick: async () => {
          const params = new URLSearchParams({ group: selectedGroup, topic: selectedTopic });
          await apiFetch('/dlq/' + encodeURIComponent(e.id) + '?' + params.toString(), { method: 'DELETE' });
          loadDLQ(container);
        }
      }, 'Delete'));
      // Export
      const exportLink = h('a', {
        href: '/dlq/' + encodeURIComponent(e.id) + '/export?group=' + encodeURIComponent(selectedGroup) + '&topic=' + encodeURIComponent(selectedTopic),
        className: 'btn btn-sm',
        style: 'margin-left:4px;text-decoration:none',
        target: '_blank',
      }, 'Export');
      actions.appendChild(exportLink);
      row.appendChild(actions);
    }

    tbody.appendChild(row);
  }
  tbl.appendChild(tbody);
  tableDiv.appendChild(tbl);
}

registerSection('dlq', { activate, deactivate });

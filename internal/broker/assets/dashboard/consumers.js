// consumers.js — Consumer Groups section: group/topic pairs, expandable details.
const { h, badge, formatNumber, apiFetch, registerSection, unreachableBanner, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, formatNumber: g.__formatNumber,
    apiFetch: g.__apiFetch, registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    safeRun: g.__safeRun };
})();

let pollTimer = null;
let expandedRows = new Set();
let fetchInFlight = false;

async function fetchConsumers(container) {
  if (fetchInFlight) return;
  fetchInFlight = true;
  try {
    const res = await apiFetch('/consumers');
    if (!res?.ok) {
      container.innerHTML = '';
      container.appendChild(h('div', { className: 'section-header' }, h('h2', null, 'Consumer Groups')));
      container.appendChild(unreachableBanner('Could not load consumer groups'));
      return;
    }
    const data = await res.json().catch(() => ({}));
    renderConsumers(container, data);
  } finally {
    fetchInFlight = false;
  }
}

function renderConsumers(container, data) {
  container.innerHTML = '';
  container.classList.add('section-full');
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Consumer Groups'));
  container.appendChild(hdr);

  const groups = data.groups || [];
  if (groups.length === 0) {
    container.appendChild(h('div', { className: 'loading' }, 'No consumer groups'));
    return;
  }

  // Group by group+topic
  const pairMap = {};
  for (const g of groups) {
    const key = g.group + '||' + g.topic;
    if (!pairMap[key]) pairMap[key] = { group: g.group, topic: g.topic, totalLag: 0, partitions: [] };
    pairMap[key].totalLag += g.lag || 0;
    pairMap[key].partitions.push({ partition: g.partition, lag: g.lag });
  }
  const pairs = Object.values(pairMap);

  const tbl = h('table', { className: 'tbl' });
  tbl.appendChild(h('thead', null,
    h('tr', null,
      h('th', null, ''),
      h('th', null, 'Group'),
      h('th', null, 'Topic'),
      h('th', null, 'Partitions'),
      h('th', null, 'Total Lag'),
      h('th', null, ''),
    )
  ));

  const tbody = h('tbody');
  for (const pair of pairs) {
    const expanded = expandedRows.has(pair.group + pair.topic);
    const toggleBtn = h('button', {
      className: 'btn btn-sm',
      onclick: async () => {
        const key = pair.group + pair.topic;
        if (expandedRows.has(key)) {
          expandedRows.delete(key);
        } else {
          expandedRows.add(key);
        }
        await fetchConsumers(container);
      }
    }, expanded ? '▼' : '▶');

    const row = h('tr', null,
      h('td', null, toggleBtn),
      h('td', { className: 'mono' }, pair.group),
      h('td', { className: 'mono' }, pair.topic),
      h('td', { className: 'mono' }, String(pair.partitions.length)),
      h('td', { className: 'mono' }, formatNumber(pair.totalLag)),
      h('td', null,
        h('button', {
          className: 'btn btn-sm',
          onclick: () => window.__navigateTo('dlq')
        }, 'DLQ')
      ),
    );
    tbody.appendChild(row);

    if (expanded) {
      tbody.appendChild(h('tr', null, h('td', { colspan: '6' }, h('div', { className: 'loading' }, 'Loading details…'))));
      loadGroupDetail(tbody, pair.group, pair.topic);
    }
  }
  tbl.appendChild(tbody);
  const wrap = h('div', { className: 'table-scroll' });
  wrap.appendChild(tbl);
  container.appendChild(wrap);
}

async function loadGroupDetail(tbody, group, topic) {
  const res = await apiFetch('/consumers/' + encodeURIComponent(group) + '/' + encodeURIComponent(topic));
  if (!res?.ok) return;
  const detail = await res.json().catch(() => null);
  if (!detail) return;

  // Remove loading row
  const rows = tbody.querySelectorAll('tr');
  for (const r of rows) {
    if (r.querySelector('.loading') && r.previousElementSibling) {
      const prev = r.previousElementSibling;
      if (prev.querySelector('.mono')?.textContent === group) {
        r.remove();
        break;
      }
    }
  }

  // Members
  const membersDiv = h('div', { style: 'padding:8px 0' });
  membersDiv.appendChild(h('div', { style: 'font-size:12px;color:var(--text3);margin-bottom:4px' }, 'Members:'));
  for (const m of (detail.members || [])) {
    membersDiv.appendChild(h('div', { className: 'mono', style: 'font-size:12px;padding:2px 0' },
      `${m.consumer_id} — partitions: [${(m.partitions || []).join(', ')}] — push: ${m.push_mode ? 'yes' : 'no'}`
    ));
  }

  if (detail.rebalancing) {
    membersDiv.appendChild(badge('Rebalancing', 'amber'));
  }
  if (detail.failed_message_count > 0) {
    membersDiv.appendChild(badge(detail.failed_message_count + ' failed', 'red'));
  }
  membersDiv.appendChild(h('div', { className: 'mono', style: 'font-size:11px;color:var(--text3);margin-top:4px' },
    `max_retries: ${detail.max_retries ?? '—'} | retry_delay: ${detail.retry_delay_ms ?? '—'}ms`
  ));

  // Partition offsets
  if (detail.partitions && detail.partitions.length > 0) {
    membersDiv.appendChild(h('div', { style: 'font-size:12px;color:var(--text3);margin-top:8px;margin-bottom:4px' }, 'Partition Offsets:'));
    const ptbl = h('table', { className: 'tbl' });
    ptbl.appendChild(h('thead', null,
      h('tr', null, h('th', null, 'Partition'), h('th', null, 'Committed'), h('th', null, 'Current'), h('th', null, 'Lag'))
    ));
    const ptbody = h('tbody');
    for (const p of detail.partitions) {
      ptbody.appendChild(h('tr', null,
        h('td', { className: 'mono' }, String(p.partition)),
        h('td', { className: 'mono' }, formatNumber(p.committed_offset)),
        h('td', { className: 'mono' }, formatNumber(p.current_offset)),
        h('td', { className: 'mono' }, formatNumber(p.lag)),
      ));
    }
    ptbl.appendChild(ptbody);
    membersDiv.appendChild(ptbl);
  }

  // Insert after the group row
  const newRow = h('tr', null, h('td', { colspan: '6' }, membersDiv));
  // Find the correct position
  let insertAfter = null;
  const allRows = Array.from(tbody.children);
  for (let i = 0; i < allRows.length; i++) {
    if (allRows[i].querySelector('.mono')?.textContent === group) {
      insertAfter = allRows[i];
      break;
    }
  }
  if (insertAfter?.nextSibling) {
    tbody.insertBefore(newRow, insertAfter.nextSibling);
  } else {
    tbody.appendChild(newRow);
  }
}

function activate(container) {
  safeRun(() => fetchConsumers(container), container, 'Consumer Groups');
  pollTimer = setInterval(
    () => safeRun(() => fetchConsumers(container), container, 'Consumer Groups'),
    3000);
}

function deactivate() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

registerSection('consumers', { activate, deactivate });

// topics.js — Topics table + partition detail navigation.
const { h, badge, formatNumber, formatBytes, apiFetch, registerSection, unreachableBanner, rbacGate, canWrite, isAdmin, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, formatNumber: g.__formatNumber, formatBytes: g.__formatBytes,
    apiFetch: g.__apiFetch, registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    rbacGate: g.__rbacGate, canWrite: g.__canWrite, isAdmin: g.__isAdmin, safeRun: g.__safeRun };
})();

let pollTimer = null;
let fetchInFlight = false;

async function fetchTopics(container) {
  if (fetchInFlight) return;
  fetchInFlight = true;
  try {
    const res = await apiFetch('/topics');
    if (!res?.ok) {
      container.innerHTML = '';
      container.appendChild(h('div', { className: 'section-header' }, h('h2', null, 'Topics')));
      container.appendChild(unreachableBanner('Could not load topics'));
      return;
    }
    const topics = await res.json().catch(() => []);

    const consumerRes = await apiFetch('/consumers');
    let groupTopicMap = {};
    if (consumerRes?.ok) {
      const data = await consumerRes.json().catch(() => ({}));
      for (const g of (data.groups || [])) {
        groupTopicMap[g.topic] = (groupTopicMap[g.topic] || 0) + 1;
      }
    }

    renderTopics(container, topics, groupTopicMap);
  } finally {
    fetchInFlight = false;
  }
}

function renderTopics(container, topics, groupTopicMap) {
  container.innerHTML = '';
  container.classList.add('section-full');
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Topics'));

  if (canWrite()) {
    const btn = h('button', {
      className: 'btn btn-primary',
      onclick: () => showCreateTopic(container)
    }, '+ Create Topic');
    if (!isAdmin()) {
      btn.classList.add('disabled');
      btn.setAttribute('title', 'admin role required');
    }
    hdr.appendChild(btn);
  }
  container.appendChild(hdr);

  if (topics.length === 0) {
    container.appendChild(h('div', { className: 'loading' }, 'No topics found'));
    return;
  }

  const tbl = h('table', { className: 'tbl' });
  const thead = h('thead', null,
    h('tr', null,
      h('th', null, 'Name'),
      h('th', null, 'Partitions'),
      h('th', null, 'Messages'),
      h('th', null, 'Groups'),
      h('th', null, 'Retention'),
      h('th', null, ''),
    )
  );
  tbl.appendChild(thead);

  const tbody = h('tbody');
  for (const t of topics) {
    const name = t.Config?.Name || '—';
    const parts = t.Config?.Partitions || 0;
    const msgs = t.MessageCount || 0;
    const groups = groupTopicMap[name] || 0;
    const retHours = t.Config?.RetentionHours || 0;
    const retBytes = t.Config?.RetentionBytes || 0;
    let retStr = '—';
    if (retHours > 0 || retBytes > 0) {
      retStr = [];
      if (retHours > 0) retStr.push(retHours + 'h');
      if (retBytes > 0) retStr.push(formatBytes(retBytes));
      retStr = retStr.join(' / ');
    }

    const row = h('tr', null,
      h('td', { className: 'mono' }, name),
      h('td', { className: 'mono' }, String(parts)),
      h('td', { className: 'mono' }, formatNumber(msgs)),
      h('td', { className: 'mono' }, String(groups)),
      h('td', { className: 'mono' }, retStr),
      h('td', null,
        h('button', {
          className: 'btn btn-sm',
          onclick: () => window.__navigateTo('partitions')
        }, 'Partitions')
      ),
    );
    tbody.appendChild(row);
  }
  tbl.appendChild(tbody);
  const wrap = h('div', { className: 'table-scroll' });
  wrap.appendChild(tbl);
  container.appendChild(wrap);
}

function showCreateTopic(container) {
  const overlay = h('div', {
    style: 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.5);z-index:100;display:flex;align-items:center;justify-content:center'
  });
  const card = h('div', { className: 'card', style: 'width:400px;max-width:90vw' });
  card.appendChild(h('h3', { style: 'margin-bottom:12px' }, 'Create Topic'));

  const nameInput = h('input', { type: 'text', id: 'topicName', placeholder: 'topic-name' });
  const partInput = h('input', { type: 'number', id: 'topicParts', value: '1', min: '1' });

  card.appendChild(h('div', { className: 'form-group' }, h('label', null, 'Name'), nameInput));
  card.appendChild(h('div', { className: 'form-group' }, h('label', null, 'Partitions'), partInput));

  const actions = h('div', { style: 'display:flex;gap:8px;margin-top:16px;justify-content:flex-end' });
  actions.appendChild(h('button', { className: 'btn', onclick: () => overlay.remove() }, 'Cancel'));
  actions.appendChild(h('button', {
    className: 'btn btn-primary',
    onclick: async () => {
      const name = nameInput.value.trim();
      const parts = parseInt(partInput.value) || 1;
      if (!name) return;
      overlay.remove();
      // Note: topic creation requires the binary protocol; HTTP-only creation
      // is not supported in this phase. Show a message.
      alert('Topic creation via the dashboard requires the binary protocol.\nUse brokectl or the Go SDK to create topics.');
    }
  }, 'Create'));
  card.appendChild(actions);
  overlay.appendChild(card);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

function activate(container) {
  safeRun(() => fetchTopics(container), container, 'Topics');
  pollTimer = setInterval(
    () => safeRun(() => fetchTopics(container), container, 'Topics'),
    3000);
}

function deactivate() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

registerSection('topics', { activate, deactivate });

// cluster.js — Cluster View: node cards, Raft internals, consensus display.
const { h, badge, formatNumber, formatTime, apiFetch, registerSection, unreachableBanner, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, formatNumber: g.__formatNumber, formatTime: g.__formatTime,
    apiFetch: g.__apiFetch, registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    safeRun: g.__safeRun };
})();

let pollTimer = null;
let fetchInFlight = false;

async function fetchCluster(container) {
  if (fetchInFlight) return;
  fetchInFlight = true;
  try {
    const [membersRes, raftRes, isrRes] = await Promise.all([
      apiFetch('/cluster/members'),
      apiFetch('/cluster/raft'),
      apiFetch('/cluster/isr'),
    ]);

    let members = [], raft = null, isr = [];
    let errors = [];

    if (membersRes?.ok) members = await membersRes.json().catch(() => []);
    else errors.push('Members');

    if (raftRes?.ok) raft = await raftRes.json().catch(() => null);
    if (raftRes?.status === 404) raft = null;

    if (isrRes?.ok) isr = await isrRes.json().catch(() => []);
    else errors.push('ISR');

    renderCluster(container, members, raft, isr, errors);
  } finally {
    fetchInFlight = false;
  }
}

function renderCluster(container, members, raft, isr, errors) {
  container.innerHTML = '';
  container.classList.add('section-fullscreen');

  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Cluster'));
  container.appendChild(hdr);

  if (errors.length > 0) {
    container.appendChild(unreachableBanner('Could not load: ' + errors.join(', ')));
  }

  const isSingleNode = members.length <= 1;
  const consensusAlgo = raft ? 'Raft' : (isSingleNode ? 'Single-node' : 'Bully');

  const algoInfo = h('div', { style: 'margin-bottom:16px;flex-shrink:0' });
  algoInfo.appendChild(h('span', { style: 'color:var(--text3);margin-right:8px' }, 'Consensus:'));
  algoInfo.appendChild(badge(consensusAlgo, raft ? 'blue' : 'gray'));
  container.appendChild(algoInfo);

  const split = h('div', { className: 'split-horizontal' });

  // Left: Nodes
  const leftPane = h('div', { className: 'split-pane' });
  const nodesCard = h('div', { className: 'card', style: 'margin-bottom:0' });
  nodesCard.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:12px' }, 'Nodes'));
  const nodesRow = h('div', { className: 'cluster-nodes', style: 'margin-bottom:0' });
  for (const m of members) {
    const isLeader = raft ? (raft.leader_id === m.node_id) : (members.indexOf(m) === 0);
    const card = h('div', { className: 'node-card' + (isLeader ? ' leader' : '') });
    card.appendChild(h('h3', null, m.node_id || '—'));
    if (isLeader) card.appendChild(badge('Leader', 'green'));
    else if (!isSingleNode) card.appendChild(badge('Follower', 'blue'));
    const fields = h('div');
    fields.appendChild(makeField('Addr', m.addr || '—'));
    fields.appendChild(makeField('HTTP', m.http_addr || '—'));
    if (m.joined_at) fields.appendChild(makeField('Joined', formatTime(m.joined_at)));
    card.appendChild(fields);
    nodesRow.appendChild(card);
  }
  nodesCard.appendChild(nodesRow);
  leftPane.appendChild(nodesCard);
  split.appendChild(leftPane);

  // Right: Raft + ISR
  const rightPane = h('div', { className: 'split-pane' });

  if (raft) {
    const raftCard = h('div', { className: 'card' });
    raftCard.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:12px' }, 'Raft Internals'));
    const grid = h('div', { className: 'card-grid' });
    grid.appendChild(makeStatCard('Term', formatNumber(raft.term)));
    grid.appendChild(makeStatCard('Leader', raft.leader_id || '—'));
    grid.appendChild(makeStatCard('Commit Index', formatNumber(raft.commit_index)));
    grid.appendChild(makeStatCard('Last Applied', formatNumber(raft.last_applied)));
    grid.appendChild(makeStatCard('Log Length', formatNumber(raft.log_length)));
    raftCard.appendChild(grid);

    const matchIdx = raft.match_index || {};
    const nextIdx = raft.next_index || {};
    const peerKeys = Object.keys(matchIdx).length > 0 ? Object.keys(matchIdx) : Object.keys(nextIdx);
    if (peerKeys.length > 0) {
      const ptbl = h('table', { className: 'tbl', style: 'margin-top:12px;width:100%' });
      ptbl.appendChild(h('thead', null,
        h('tr', null, h('th', null, 'Peer'), h('th', null, 'Match Index'), h('th', null, 'Next Index'))
      ));
      const ptbody = h('tbody');
      for (const pk of peerKeys) {
        ptbody.appendChild(h('tr', null,
          h('td', { className: 'mono' }, pk),
          h('td', { className: 'mono' }, formatNumber(matchIdx[pk] || 0)),
          h('td', { className: 'mono' }, formatNumber(nextIdx[pk] || 0)),
        ));
      }
      ptbl.appendChild(ptbody);
      raftCard.appendChild(ptbl);
    }
    rightPane.appendChild(raftCard);
  }

  if (isr.length > 0) {
    const isrCard = h('div', { className: 'card' });
    isrCard.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:12px' }, 'ISR State'));
    const isrTbl = h('table', { className: 'tbl', style: 'width:100%' });
    isrTbl.appendChild(h('thead', null,
      h('tr', null, h('th', null, 'Topic'), h('th', null, 'Partition'), h('th', null, 'ISR'), h('th', null, 'Leader'), h('th', null, 'Status'))
    ));
    const isrBody = h('tbody');
    for (const e of isr) {
      isrBody.appendChild(h('tr', null,
        h('td', { className: 'mono' }, e.topic || '—'),
        h('td', { className: 'mono' }, String(e.partition ?? '—')),
        h('td', { className: 'mono' }, Array.isArray(e.isr) ? e.isr.join(', ') : '—'),
        h('td', { className: 'mono' }, e.leader || '—'),
        h('td', null, e.under_replicated ? badge('Under-replicated', 'red') : badge('OK', 'green')),
      ));
    }
    isrTbl.appendChild(isrBody);
    isrCard.appendChild(isrTbl);
    rightPane.appendChild(isrCard);
  }

  split.appendChild(rightPane);
  container.appendChild(split);
}

function makeField(label, value) {
  return h('div', { className: 'field' },
    h('span', { className: 'label' }, label),
    h('span', { className: 'mono' }, value)
  );
}

function makeStatCard(label, value) {
  const card = h('div', { className: 'card-stat' });
  card.appendChild(h('div', { className: 'label' }, label));
  card.appendChild(h('div', { className: 'value' }, value));
  return card;
}

function activate(container) {
  safeRun(() => fetchCluster(container), container, 'Cluster');
  pollTimer = setInterval(
    () => safeRun(() => fetchCluster(container), container, 'Cluster'),
    3000);
}

function deactivate() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

registerSection('cluster', { activate, deactivate });

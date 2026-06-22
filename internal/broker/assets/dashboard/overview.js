// overview.js — Overview section: summary stats, cluster health, uptime.
const { h, formatNumber, apiFetch, registerSection, unreachableBanner, safeRun } = (() => {
  const g = window;
  return { h: g.__h, formatNumber: g.__formatNumber, apiFetch: g.__apiFetch,
    registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner, safeRun: g.__safeRun };
})();

let pollTimer = null;
let startTime = Date.now();

async function fetchOverview(container) {
  const errors = [];
  let topics = [], consumers = {}, members = [], raft = null, health = null, history = null;

  const [topicsRes, consumersRes, membersRes, raftRes, healthRes] = await Promise.all([
    apiFetch('/topics'),
    apiFetch('/consumers'),
    apiFetch('/cluster/members'),
    apiFetch('/cluster/raft'),
    apiFetch('/healthz/ready'),
  ]);

  if (topicsRes?.ok) topics = await topicsRes.json().catch(() => []);
  else errors.push('Topics');

  if (consumersRes?.ok) consumers = await consumersRes.json().catch(() => ({}));
  else errors.push('Consumers');

  if (membersRes?.ok) members = await membersRes.json().catch(() => []);
  else errors.push('Cluster');

  if (raftRes?.ok) raft = await raftRes.json().catch(() => null);
  if (raftRes?.status === 404) raft = null;

  if (healthRes?.ok) health = await healthRes.json().catch(() => null);
  else errors.push('Health');

  renderOverview(container, topics, consumers, members, raft, health, errors);
}

function renderOverview(container, topics, consumers, members, raft, health, errors) {
  container.innerHTML = '';

  const hdr = h('div', { className: 'section-header' }, h('h2', null, 'Overview'));
  container.appendChild(hdr);

  if (errors.length > 0) {
    container.appendChild(unreachableBanner('Could not reach: ' + errors.join(', ')));
  }

  const topicCount = topics.length;
  let partCount = 0;
  let msgTotal = 0;
  for (const t of topics) {
    partCount += t.Config?.Partitions || 0;
    msgTotal += t.MessageCount || 0;
  }

  const groups = consumers.groups || [];
  const distinctGroups = new Set(groups.map(g => g.group)).size;
  const activeConns = consumers.active_connections || 0;

  const uptimeMs = Date.now() - startTime;
  const uptimeStr = formatUptime(uptimeMs);

  const healthOk = health?.status === 'ready';

  const grid = h('div', { className: 'card-row' });

  grid.appendChild(makeCard('Topics', formatNumber(topicCount), 'blue'));
  grid.appendChild(makeCard('Partitions', formatNumber(partCount), 'violet'));
  grid.appendChild(makeCard('Messages', formatNumber(msgTotal), 'cyan'));
  grid.appendChild(makeCard('Active Connections', formatNumber(activeConns), 'green'));
  grid.appendChild(makeCard('Consumer Groups', formatNumber(distinctGroups), 'amber'));
  grid.appendChild(makeCard('Cluster Nodes', formatNumber(members.length), 'blue'));
  grid.appendChild(makeCard('Uptime', uptimeStr, ''));
  grid.appendChild(makeCard('Health', '', healthOk ? 'green' : 'red', healthOk ? 'Healthy' : 'Unhealthy'));

  container.appendChild(grid);

  // Cluster info
  const clusterCard = h('div', { className: 'card' });
  clusterCard.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:12px' }, 'Cluster Status'));

  const leader = members.length > 1 ? members.find(m => {
    // Try to determine leader from raft or default to first
    return raft && raft.leader_id === m.node_id;
  }) : null;

  const infoGrid = h('div', { className: 'card-grid' });
  infoGrid.appendChild(makeCard('Consensus', raft ? 'Raft' : (members.length > 1 ? 'Bully' : 'Single-node'), 'blue'));
  infoGrid.appendChild(makeCard('Leader', leader ? leader.node_id : (members.length === 1 ? members[0]?.node_id : '—'), 'green'));
  if (raft) {
    infoGrid.appendChild(makeCard('Raft Term', formatNumber(raft.term), ''));
    infoGrid.appendChild(makeCard('Commit Index', formatNumber(raft.commit_index), ''));
  }
  clusterCard.appendChild(infoGrid);
  container.appendChild(clusterCard);
}

function makeCard(label, value, color, overrideText) {
  const card = h('div', { className: 'card-stat' });
  card.appendChild(h('div', { className: 'label' }, label));
  const valEl = h('div', { className: 'value' + (color ? ' ' + color : '') }, overrideText || String(value));
  card.appendChild(valEl);
  return card;
}

function formatUptime(ms) {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  const d = Math.floor(h / 24);
  if (d > 0) return d + 'd ' + (h % 24) + 'h';
  if (h > 0) return h + 'h ' + (m % 60) + 'm';
  if (m > 0) return m + 'm ' + (s % 60) + 's';
  return s + 's';
}

function activate(container) {
  startTime = Date.now();
  safeRun(() => fetchOverview(container), container, 'Overview');
  pollTimer = setInterval(
    () => safeRun(() => fetchOverview(container), container, 'Overview'),
    3000);
}

function deactivate() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
}

registerSection('overview', { activate, deactivate });

// explorer.js — Live Message Explorer: WebSocket streaming with filters.
const { h, badge, apiFetch, registerSection, unreachableBanner, rbacGate, canWrite, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, apiFetch: g.__apiFetch,
    registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    rbacGate: g.__rbacGate, canWrite: g.__canWrite, safeRun: g.__safeRun };
})();

const MAX_MESSAGES = 500;
let ws = null;
let messages = [];
let paused = false;
let droppedCount = 0;
let selectedTopic = '';
let containerRef = null;

function activate(container) {
  containerRef = container;
  messages = [];
  droppedCount = 0;
  paused = false;
  safeRun(() => {
    renderExplorer(container);
    return loadTopics();
  }, container, 'Explorer');
}

function deactivate() {
  if (ws) { ws.close(); ws = null; }
  messages = [];
}

async function loadTopics() {
  const res = await apiFetch('/topics');
  if (!res?.ok) return;
  const topics = await res.json().catch(() => []);
  const sel = containerRef?.querySelector('#explorerTopic');
  if (!sel) return;
  const prev = sel.value;
  sel.innerHTML = '';
  for (const t of topics) {
    const opt = h('option', { value: t.Config?.Name || '' }, t.Config?.Name || '');
    if (t.Config?.Name === prev || t.Config?.Name === selectedTopic) opt.selected = true;
    sel.appendChild(opt);
  }
  if (!sel.value && topics.length > 0) sel.value = topics[0].Config?.Name || '';
  if (sel.value !== selectedTopic) {
    selectedTopic = sel.value;
    connectWS();
  }
}

function renderExplorer(container) {
  container.innerHTML = '';
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Live Message Explorer'));
  container.appendChild(hdr);

  // Controls
  const controls = h('div', { className: 'form-row', style: 'flex-wrap:wrap;gap:8px;margin-bottom:12px' });

  controls.appendChild(h('label', { style: 'margin-right:4px' }, 'Topic:'));
  const topicSel = h('select', { id: 'explorerTopic' });
  topicSel.addEventListener('change', () => {
    selectedTopic = topicSel.value;
    messages = [];
    droppedCount = 0;
    connectWS();
  });
  controls.appendChild(topicSel);

  const partSel = h('select', { id: 'explorerPartition' });
  partSel.appendChild(h('option', { value: '-1' }, 'All'));
  for (let i = 0; i < 32; i++) {
    partSel.appendChild(h('option', { value: String(i) }, String(i)));
  }
  partSel.addEventListener('change', () => sendFilterUpdate());
  controls.appendChild(h('label', { style: 'margin-right:4px' }, 'Partition:'));
  controls.appendChild(partSel);

  const keyInput = h('input', { type: 'text', id: 'explorerKey', placeholder: 'Key filter', style: 'width:120px' });
  controls.appendChild(h('label', { style: 'margin-right:4px' }, 'Key:'));
  controls.appendChild(keyInput);

  const prodInput = h('input', { type: 'text', id: 'explorerProducer', placeholder: 'Producer', style: 'width:120px' });
  controls.appendChild(h('label', { style: 'margin-right:4px' }, 'Producer:'));
  controls.appendChild(prodInput);

  const searchInput = h('input', { type: 'text', id: 'explorerSearch', placeholder: 'Payload search', style: 'width:140px' });
  controls.appendChild(h('label', { style: 'margin-right:4px' }, 'Search:'));
  controls.appendChild(searchInput);

  const pauseBtn = h('button', { className: 'btn', id: 'explorerPause' }, 'Pause');
  pauseBtn.addEventListener('click', () => {
    if (!ws) return;
    paused = !paused;
    ws.send(JSON.stringify({ action: paused ? 'pause' : 'resume' }));
    pauseBtn.textContent = paused ? 'Resume' : 'Pause';
    pauseBtn.classList.toggle('btn-primary', paused);
  });
  controls.appendChild(pauseBtn);

  const connectBtn = h('button', { className: 'btn btn-primary', id: 'explorerConnect' }, 'Connect');
  connectBtn.addEventListener('click', () => connectWS());
  controls.appendChild(connectBtn);

  container.appendChild(controls);

  // Status
  const statusBar = h('div', { className: 'status-bar', id: 'explorerStatus' });
  statusBar.appendChild(h('span', { id: 'explorerDropIndicator' }, ''));
  statusBar.appendChild(badge('Showing last 500 messages', 'gray'));
  container.appendChild(statusBar);

  // Message list
  const msgList = h('div', { className: 'msg-list', id: 'explorerMsgList' });
  container.appendChild(msgList);

  // Apply filters on enter
  [keyInput, prodInput, searchInput].forEach(inp => {
    inp.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') sendFilterUpdate();
    });
  });
}

function connectWS() {
  if (ws) { ws.close(); ws = null; }
  if (!selectedTopic) return;

  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = `${proto}//${location.host}/explorer/stream?topic=${encodeURIComponent(selectedTopic)}`;
  ws = new WebSocket(url);

  ws.onopen = () => {
    const connBtn = document.getElementById('explorerConnect');
    if (connBtn) connBtn.textContent = 'Connected';
    sendFilterUpdate();
  };

  ws.onmessage = (evt) => {
    const data = JSON.parse(evt.data);
    if (data.status) {
      droppedCount += data.status.dropped_since_last || 0;
      updateDropIndicator();
      return;
    }
    messages.unshift(data);
    if (messages.length > MAX_MESSAGES) messages.length = MAX_MESSAGES;
    appendMessage(data);
  };

  ws.onerror = () => {};
  ws.onclose = () => {
    const connBtn = document.getElementById('explorerConnect');
    if (connBtn) connBtn.textContent = 'Connect';
  };
}

function sendFilterUpdate() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({
    action: 'update_filter',
    filter: {
      partition: parseInt(document.getElementById('explorerPartition')?.value || '-1'),
      key: document.getElementById('explorerKey')?.value || '',
      producer: document.getElementById('explorerProducer')?.value || '',
      search: document.getElementById('explorerSearch')?.value || '',
    }
  }));
}

function appendMessage(msg) {
  const list = document.getElementById('explorerMsgList');
  if (!list) return;

  let payload;
  try {
    const decoded = atob(msg.payload);
    const bytes = new Uint8Array(decoded.length);
    for (let i = 0; i < decoded.length; i++) bytes[i] = decoded.charCodeAt(i);
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
    payload = text;
  } catch {
    const decoded = atob(msg.payload);
    payload = `[binary payload, ${decoded.length} bytes]`;
  }

  const ts = msg.timestamp_ns ? new Date(msg.timestamp_ns / 1e6).toISOString() : '';
  const item = h('div', { className: 'msg-item', innerHTML:
    `<span class="ts">${ts}</span> <span class="topic-tag">[${msg.topic}:${msg.partition}]</span> ` +
    `<span class="key-tag">key=${msg.key || '—'}</span> ` +
    `<span style="color:var(--text3)">producer=${msg.producer || '—'}</span> ` +
    `<span class="payload">${escapeHtml(payload)}</span>`
  });

  if (list.firstChild) {
    list.insertBefore(item, list.firstChild);
  } else {
    list.appendChild(item);
  }

  // Cap DOM
  while (list.children.length > MAX_MESSAGES) {
    list.removeChild(list.lastChild);
  }
}

function updateDropIndicator() {
  const el = document.getElementById('explorerDropIndicator');
  if (el) {
    el.textContent = droppedCount > 0 ? `${droppedCount} messages dropped (client too slow)` : '';
    el.className = droppedCount > 0 ? 'badge badge-amber' : '';
  }
}

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

registerSection('explorer', { activate, deactivate });

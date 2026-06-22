// settings.js — Settings section: display and edit hot-reloadable config.
const { h, apiFetch, registerSection, unreachableBanner, isAdmin, safeRun } = (() => {
  const g = window;
  return { h: g.__h, apiFetch: g.__apiFetch,
    registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner, isAdmin: g.__isAdmin,
    safeRun: g.__safeRun };
})();

function activate(container) {
  safeRun(() => fetchSettings(container), container, 'Settings');
}

function deactivate() {}

async function fetchSettings(container) {
  container.innerHTML = '';
  container.classList.add('section-full');
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Settings'));
  container.appendChild(hdr);

  if (!isAdmin()) {
    container.appendChild(h('div', { className: 'readonly-note' }, 'Admin role required to view settings'));
    return;
  }

  container.appendChild(h('div', { className: 'readonly-note' },
    'Hot-reloadable settings can be edited below. Settings requiring a restart are shown locked.'));

  const res = await apiFetch('/config/effective');
  if (!res?.ok) {
    container.appendChild(unreachableBanner('Could not load configuration'));
    return;
  }
  const cfg = await res.json().catch(() => null);
  if (!cfg) {
    container.appendChild(unreachableBanner('Invalid configuration response'));
    return;
  }

  renderConfig(container, cfg);
}

function renderConfig(container, cfg) {
  const grid = h('div', { className: 'settings-grid' });

  // Retention
  if (cfg.retention) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Retention'));
    appendField(card, 'Max Age Hours', cfg.retention.max_age_hours, 'retention.max_age_hours');
    appendField(card, 'Max Size MB', cfg.retention.max_size_mb, 'retention.max_size_mb');
    grid.appendChild(card);
  }

  // TLS
  if (cfg.network) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'TLS'));
    const tlsEnabled = !!(cfg.network.tls_cert_file);
    appendField(card, 'Enabled', { value: tlsEnabled ? 'Yes' : 'No' }, '');
    if (tlsEnabled) {
      appendField(card, 'Certificate', { value: cfg.network.tls_cert_file || '—' }, '');
      appendField(card, 'Min Version', { value: cfg.network.tls_min_version ? '0x' + cfg.network.tls_min_version.toString(16) : 'default (1.3)' }, '');
    }
    grid.appendChild(card);
  }

  // Rate Limits
  if (cfg.rate_limit) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Rate Limits'));
    appendField(card, 'Enabled', cfg.rate_limit.enabled, 'rate_limit.enabled');
    appendField(card, 'Per-Client RPS', cfg.rate_limit.per_client_rps, 'rate_limit.per_client_rps');
    appendField(card, 'Per-Topic RPS', cfg.rate_limit.per_topic_rps, 'rate_limit.per_topic_rps');
    appendField(card, 'Burst Multiplier', cfg.rate_limit.burst_multiplier, '');
    grid.appendChild(card);
  }

  // Replication
  if (cfg.replication) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Replication'));
    appendField(card, 'Factor', { value: String(cfg.replication.factor ?? '—') }, '');
    appendField(card, 'Sync Interval', { value: cfg.replication.sync_interval || '—' }, '');
    appendField(card, 'Ack Timeout', { value: cfg.replication.ack_timeout || '—' }, '');
    grid.appendChild(card);
  }

  // Cluster
  if (cfg.cluster) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Cluster'));
    appendField(card, 'Enabled', { value: cfg.cluster.enabled ? 'Yes' : 'No' }, '');
    appendField(card, 'Consensus', { value: cfg.cluster.consensus_algorithm || 'bully' }, '');
    appendField(card, 'Node ID', { value: cfg.cluster.node_id || '—' }, '');
    appendField(card, 'Heartbeat (ms)', { value: String(cfg.cluster.heartbeat_interval_ms ?? '—') }, '');
    grid.appendChild(card);
  }

  // Storage / WAL
  if (cfg.storage) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Storage & WAL'));
    appendField(card, 'Data Path', { value: cfg.storage.data_path || '—' }, '');
    appendField(card, 'WAL Path', { value: cfg.storage.wal_path || '—' }, '');
    appendField(card, 'Segment Max Bytes', { value: String(cfg.storage.segment_max_bytes ?? '—') }, '');
    appendField(card, 'Sync Policy', { value: cfg.storage.sync_policy || '—' }, '');
    grid.appendChild(card);
  }

  // Compaction
  if (cfg.compaction) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Compaction'));
    appendField(card, 'Interval (ms)', cfg.compaction.interval_ms, 'compaction.interval_ms');
    appendField(card, 'Tombstone Grace (ms)', cfg.compaction.tombstone_grace_ms, 'compaction.tombstone_grace_ms');
    grid.appendChild(card);
  }

  // Logging
  if (cfg.logging) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Logging'));
    appendField(card, 'Level', cfg.logging.level, 'logging.level');
    appendField(card, 'Format', cfg.logging.format, '');
    grid.appendChild(card);
  }

  // Drain / Flow Control
  {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Broker Timing'));
    appendField(card, 'Drain Timeout (ms)', cfg.drain_timeout_ms, 'drain_timeout_ms');
    appendField(card, 'Flow Control Pause (ms)', cfg.flow_control_pause_ms, 'flow_control_pause_ms');
    grid.appendChild(card);
  }

  // Dashboard / Explorer
  if (cfg.network) {
    const card = h('div', { className: 'card' });
    card.appendChild(h('h3', { style: 'font-size:14px;margin-bottom:8px' }, 'Dashboard'));
    appendField(card, 'Dashboard Enabled', { value: cfg.network.dashboard_enabled ? 'Yes' : 'No' }, '');
    appendField(card, 'Explorer Enabled', { value: cfg.network.explorer_enabled ? 'Yes' : 'No' }, '');
    appendField(card, 'Max Explorer Connections', { value: String(cfg.network.explorer_max_connections ?? '—') }, '');
    appendField(card, 'Session TTL', { value: cfg.network.dashboard_session_ttl || '12h' }, '');
    grid.appendChild(card);
  }

  container.appendChild(grid);
}

// appendField renders a config field row. `field` may be:
//   - A configField object { value, hot_reloadable } from the annotated response
//   - A plain value (for non-annotated fields)
// `fieldPath` is the dotted path for hot-reloadable fields; empty for read-only.
function appendField(card, label, field, fieldPath) {
  const isObj = field && typeof field === 'object' && 'value' in field;
  const value = isObj ? field.value : field;
  const hotReloadable = isObj ? !!field.hot_reloadable : false;
  const editable = hotReloadable && fieldPath !== '';

  const row = h('div', { style: 'display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid rgba(63,63,70,.3);font-size:13px' });
  row.appendChild(h('span', { style: 'color:var(--text3)' }, label));

  if (editable) {
    const valueSpan = h('span', { className: 'mono', style: 'display:inline-flex;align-items:center;gap:4px' });
    const valText = h('span', null, String(value ?? '—'));
    valueSpan.appendChild(valText);

    const editBtn = h('button', {
      style: 'background:none;border:none;color:var(--text3);cursor:pointer;font-size:12px;padding:2px 4px',
      title: 'Edit this setting',
      innerHTML: '&#9998;', // pencil icon
    });
    editBtn.addEventListener('click', () => startEdit(row, valText, editBtn, fieldPath, value, card));
    valueSpan.appendChild(editBtn);
    row.appendChild(valueSpan);
  } else if (fieldPath !== '' && !hotReloadable) {
    // Non-hot-reloadable field with a known path — show lock.
    const lockSpan = h('span', { className: 'mono', style: 'display:inline-flex;align-items:center;gap:4px' });
    lockSpan.appendChild(h('span', null, String(value ?? '—')));
    const lock = h('span', {
      style: 'color:var(--text3);font-size:12px;cursor:help',
      title: 'Requires a broker restart to change — edit configs/broker.json directly.',
      innerHTML: '&#128274;', // lock icon
    });
    lockSpan.appendChild(lock);
    row.appendChild(lockSpan);
  } else {
    row.appendChild(h('span', { className: 'mono' }, String(value ?? '—')));
  }

  card.appendChild(row);
}

function startEdit(row, valText, editBtn, fieldPath, oldVal, card) {
  editBtn.style.display = 'none';
  valText.style.display = 'none';

  const input = h('input', {
    type: typeof oldVal === 'boolean' ? 'checkbox' : (typeof oldVal === 'number' ? 'number' : 'text'),
    value: String(oldVal),
    style: 'background:var(--bg2);color:var(--text1);border:1px solid var(--border);border-radius:4px;padding:2px 6px;font-size:13px;width:120px;font-family:monospace',
  });
  if (typeof oldVal === 'boolean') {
    input.checked = oldVal;
  }

  const saveBtn = h('button', {
    style: 'background:#2563eb;color:#fff;border:none;border-radius:4px;padding:2px 8px;font-size:12px;cursor:pointer',
  }, 'Save');
  const cancelBtn = h('button', {
    style: 'background:var(--bg2);color:var(--text3);border:1px solid var(--border);border-radius:4px;padding:2px 8px;font-size:12px;cursor:pointer;margin-left:4px',
  }, 'Cancel');

  const wrapper = h('span', { style: 'display:inline-flex;align-items:center;gap:4px' });
  wrapper.appendChild(input);
  wrapper.appendChild(saveBtn);
  wrapper.appendChild(cancelBtn);

  // Insert after valText's position.
  valText.parentNode.insertBefore(wrapper, valText.nextSibling);

  const restore = () => {
    wrapper.remove();
    valText.style.display = '';
    editBtn.style.display = '';
  };

  cancelBtn.addEventListener('click', restore);

  saveBtn.addEventListener('click', async () => {
    let newVal;
    if (typeof oldVal === 'boolean') {
      newVal = input.checked;
    } else if (typeof oldVal === 'number') {
      newVal = Number(input.value);
    } else {
      newVal = input.value;
    }

    // Confirmation dialog.
    if (!confirm(`Apply this change to the running broker?\n\n${fieldPath}: ${oldVal} → ${newVal}`)) {
      return;
    }

    saveBtn.disabled = true;
    saveBtn.textContent = '...';

    const res = await apiFetch('/config', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ changes: { [fieldPath]: newVal } }),
    });

    if (res?.ok) {
      // Re-fetch the effective config to show confirmed state.
      const fresh = await apiFetch('/config/effective');
      if (fresh?.ok) {
        const cfg = await fresh.json().catch(() => null);
        if (cfg) {
          renderConfig(card.parentNode, cfg);
          return;
        }
      }
      // Fallback: show saved message inline.
      valText.textContent = String(newVal);
      restore();
    } else {
      const errBody = await res?.json().catch(() => null);
      const errMsg = errBody?.error || 'Unknown error';
      // Show error inline.
      const errSpan = h('span', { style: 'color:#ef4444;font-size:12px;margin-left:8px' }, errMsg);
      wrapper.appendChild(errSpan);
      setTimeout(() => { errSpan.remove(); }, 5000);
      saveBtn.disabled = false;
      saveBtn.textContent = 'Save';
    }
  });

  input.focus();
  input.select();
}

registerSection('settings', { activate, deactivate });

// metrics.js — Metrics charts with time-range tabs (5m/15m/1h/24h).
import { renderLineChart } from '/dashboard/charts.js';

const { h, badge, apiFetch, registerSection, unreachableBanner, safeRun } = (() => {
  const g = window;
  return { h: g.__h, badge: g.__badge, apiFetch: g.__apiFetch,
    registerSection: g.__registerSection, unreachableBanner: g.__unreachableBanner,
    safeRun: g.__safeRun };
})();

let pollTimer = null;
let currentRange = '15m';
let fetchInFlight = false;

const CHART_DEFS = [
  { key: 'messages_published_total', title: 'Publish Rate', color: '#22c55e', unit: 'msg' },
  { key: 'messages_consumed_total', title: 'Consume Rate', color: '#3b82f6', unit: 'msg' },
  { key: 'messages_errored_total', title: 'Error Rate', color: '#ef4444', unit: 'msg' },
  { key: 'active_connections', title: 'Active Connections', color: '#06b6d4', unit: '' },
  { key: 'active_consumer_groups', title: 'Consumer Groups', color: '#eab308', unit: '' },
  { key: 'process_resident_memory_bytes', title: 'Memory Usage', color: '#8b5cf6', unit: 'B' },
  { key: 'process_cpu_seconds_total', title: 'CPU Time', color: '#f97316', unit: 's' },
  { key: 'consumer_lag_total', title: 'Consumer Lag', color: '#ef4444', unit: 'msg' },
  { key: 'wal_bytes_total', title: 'WAL Throughput', color: '#22c55e', unit: 'B' },
  { key: 'topic_count', title: 'Topic Count', color: '#3b82f6', unit: '' },
];

function activate(container) {
  renderMetrics(container);
  safeRun(() => fetchAndRender(container), container, 'Metrics');
  const interval = (currentRange === '5m' || currentRange === '15m') ? 10000 : 30000;
  pollTimer = setInterval(
    () => safeRun(() => fetchAndRender(container), container, 'Metrics'),
    interval);
}

function deactivate() {
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  // Reset the in-flight guard so that re-visiting this section doesn't
  // get stuck: if a fetch was mid-flight when deactivate was called,
  // fetchInFlight would remain true and the next activate would skip
  // the first fetch call entirely, leaving the charts blank indefinitely.
  fetchInFlight = false;
}

function renderMetrics(container) {
  container.innerHTML = '';
  container.classList.add('section-full');
  const hdr = h('div', { className: 'section-header' });
  hdr.appendChild(h('h2', null, 'Metrics & Monitoring'));
  container.appendChild(hdr);

  const tabs = h('div', { className: 'tabs' });
  for (const r of ['5m', '15m', '1h', '24h']) {
    const tab = h('button', {
      className: 'tab' + (r === currentRange ? ' active' : ''),
      onclick: () => {
        currentRange = r;
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        safeRun(() => fetchAndRender(container), container, 'Metrics');
        const interval = (r === '5m' || r === '15m') ? 10000 : 30000;
        pollTimer = setInterval(
          () => safeRun(() => fetchAndRender(container), container, 'Metrics'),
          interval);
        tabs.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
      }
    }, r);
    tabs.appendChild(tab);
  }
  container.appendChild(tabs);

  container.appendChild(h('div', { id: 'metricsCharts', className: 'chart-row' }));
}

async function fetchAndRender(container) {
  if (fetchInFlight) return;
  fetchInFlight = true;
  try {
    const res = await apiFetch('/metrics/history?range=' + currentRange);
    if (!res?.ok) {
      const chartsDiv = document.getElementById('metricsCharts');
      if (chartsDiv) {
        chartsDiv.innerHTML = '';
        chartsDiv.appendChild(unreachableBanner('Could not load metrics history'));
      }
      return;
    }
    const data = await res.json().catch(() => ({ series: {} }));
    renderCharts(data.series || {});
  } finally {
    fetchInFlight = false;
  }
}

function renderCharts(series) {
  const chartsDiv = document.getElementById('metricsCharts');
  if (!chartsDiv) return;
  chartsDiv.innerHTML = '';

  // Compute rates (delta between consecutive points) for counter metrics
  const counters = new Set(['messages_published_total', 'messages_consumed_total', 'messages_errored_total',
    'messages_acked_total', 'messages_nacked_total', 'wal_bytes_total', 'wal_entries_total',
    'bytes_published_total', 'bytes_consumed_total', 'go_gc_duration_seconds_count']);

  for (const def of CHART_DEFS) {
    const raw = series[def.key];
    if (!raw || raw.length < 2) continue;

    let data;
    if (counters.has(def.key)) {
      // Compute per-second rate
      data = [];
      for (let i = 1; i < raw.length; i++) {
        const dt = (new Date(raw[i].t) - new Date(raw[i-1].t)) / 1000;
        if (dt <= 0) continue;
        const dv = raw[i].v - raw[i-1].v;
        data.push({ t: raw[i].t, v: dv / dt });
      }
    } else {
      data = raw;
    }

    if (data.length < 2) continue;

    const wrap = h('div');
    renderLineChart(wrap, data, {
      color: def.color,
      title: def.title,
      unit: counters.has(def.key) ? def.unit + '/s' : def.unit,
      height: 120,
    });
    chartsDiv.appendChild(wrap);
  }

  if (chartsDiv.children.length === 0) {
    chartsDiv.appendChild(h('div', { className: 'loading' }, 'Waiting for metric data… (collects every 10s)'));
  }
}

registerSection('metrics', { activate, deactivate });

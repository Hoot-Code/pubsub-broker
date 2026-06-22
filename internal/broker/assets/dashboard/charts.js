// charts.js — Hand-rolled SVG line-chart helper. Reused by metrics.js. Zero deps.
// Each chart renders a polyline with axis labels (min/max value, time range start/end).

const h = window.__h;

/**
 * renderLineChart(container, data, opts)
 * container: DOM element to render into
 * data: array of {t: string|number|Date, v: number}
 * opts: { color, height, title, unit, decimals }
 * Returns the SVG element.
 */
export function renderLineChart(container, data, opts = {}) {
  const color = opts.color || '#22c55e';
  const height = opts.height || 120;
  const pad = { top: 8, right: 40, bottom: 20, left: 6 };
  const decimals = opts.decimals ?? 1;

  const wrap = h('div', { className: 'chart-container' });
  if (opts.title) {
    wrap.appendChild(h('div', { className: 'chart-title' }, opts.title));
  }

  if (!data || data.length < 2) {
    wrap.appendChild(h('div', { className: 'loading' }, 'Not enough data yet'));
    container.appendChild(wrap);
    return null;
  }

  const svgW = 360;
  const svgH = height;
  const w = svgW - pad.left - pad.right;
  const hh = svgH - pad.top - pad.bottom;

  let vMin = Infinity, vMax = -Infinity;
  for (const p of data) {
    if (p.v < vMin) vMin = p.v;
    if (p.v > vMax) vMax = p.v;
  }
  if (vMin === vMax) { vMin -= 1; vMax += 1; }
  const vRange = vMax - vMin;

  const toX = (i) => pad.left + (i / (data.length - 1)) * w;
  const toY = (v) => pad.top + (1 - (v - vMin) / vRange) * hh;

  let points = '';
  for (let i = 0; i < data.length; i++) {
    points += toX(i).toFixed(1) + ',' + toY(data[i].v).toFixed(1) + ' ';
  }

  const tStart = data[0].t;
  const tEnd = data[data.length - 1].t;
  const fmtShort = (t) => {
    const d = new Date(t);
    return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0');
  };

  const unit = opts.unit || '';
  const formatVal = (v) => v.toFixed(decimals) + (unit ? ' ' + unit : '');

  const svgNS = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(svgNS, 'svg');
  svg.setAttribute('viewBox', `0 0 ${svgW} ${svgH}`);
  svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');

  // Grid lines
  for (let i = 0; i <= 4; i++) {
    const y = pad.top + (i / 4) * hh;
    const line = document.createElementNS(svgNS, 'line');
    line.setAttribute('x1', pad.left);
    line.setAttribute('y1', y);
    line.setAttribute('x2', svgW - pad.right);
    line.setAttribute('y2', y);
    line.setAttribute('stroke', '#27272a');
    line.setAttribute('stroke-width', '1');
    svg.appendChild(line);

    const val = vMax - (i / 4) * vRange;
    const txt = document.createElementNS(svgNS, 'text');
    txt.setAttribute('x', svgW - pad.right + 2);
    txt.setAttribute('y', y + 3);
    txt.setAttribute('fill', '#71717a');
    txt.setAttribute('font-size', '9');
    txt.setAttribute('font-family', 'monospace');
    txt.textContent = formatVal(val);
    svg.appendChild(txt);
  }

  // Time labels
  const tLabels = [tStart, tEnd];
  for (let i = 0; i < 2; i++) {
    const txt = document.createElementNS(svgNS, 'text');
    txt.setAttribute('x', toX(i === 0 ? 0 : data.length - 1));
    txt.setAttribute('y', svgH - 2);
    txt.setAttribute('fill', '#71717a');
    txt.setAttribute('font-size', '9');
    txt.setAttribute('font-family', 'monospace');
    txt.setAttribute('text-anchor', i === 0 ? 'start' : 'end');
    txt.textContent = fmtShort(tLabels[i]);
    svg.appendChild(txt);
  }

  // Fill area
  const fillPath = document.createElementNS(svgNS, 'polygon');
  const fillPoints = `${toX(0).toFixed(1)},${(pad.top + hh).toFixed(1)} ` + points +
    `${toX(data.length - 1).toFixed(1)},${(pad.top + hh).toFixed(1)}`;
  fillPath.setAttribute('points', fillPoints);
  fillPath.setAttribute('fill', color);
  fillPath.setAttribute('fill-opacity', '0.1');
  svg.appendChild(fillPath);

  // Polyline
  const polyline = document.createElementNS(svgNS, 'polyline');
  polyline.setAttribute('points', points);
  polyline.setAttribute('fill', 'none');
  polyline.setAttribute('stroke', color);
  polyline.setAttribute('stroke-width', '1.5');
  polyline.setAttribute('stroke-linejoin', 'round');
  svg.appendChild(polyline);

  // Latest dot
  const dot = document.createElementNS(svgNS, 'circle');
  dot.setAttribute('cx', toX(data.length - 1).toFixed(1));
  dot.setAttribute('cy', toY(data[data.length - 1].v).toFixed(1));
  dot.setAttribute('r', '3');
  dot.setAttribute('fill', color);
  svg.appendChild(dot);

  wrap.appendChild(svg);
  container.appendChild(wrap);
  return svg;
}

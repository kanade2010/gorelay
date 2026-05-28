package main

import "net/http"

func (r *Relay) handleWeakNetLossPage(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(`<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>WeakNet Loss Monitor</title>
  <style>
    body{margin:0;background:#0b1220;color:#e2e8f0;font-family:Arial,sans-serif}
    .wrap{max-width:1400px;margin:0 auto;padding:16px}
    .toolbar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;background:#111827;border:1px solid #334155;border-radius:10px;padding:10px 12px}
    .toolbar select,.toolbar button{background:#0b1220;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:6px 10px}
    .toolbar button{cursor:pointer}
    .status{font-size:12px;color:#94a3b8}
    .status.err{color:#ef4444}
    .legend{display:flex;gap:14px;align-items:center;flex-wrap:wrap;font-size:12px;color:#cbd5e1}
    .dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px}
    .dot.txe{background:#38bdf8}
    .dot.rxe{background:#22c55e}
    .dot.txa{background:#f59e0b}
    .dot.rxa{background:#ef4444}
    .line-toggles{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:8px}
    .line-btn{border:1px solid #334155;border-radius:8px;padding:4px 10px;font-size:12px;background:#0b1220;color:#e2e8f0;cursor:pointer}
    .line-btn.off{opacity:.45;text-decoration:line-through}
    .line-btn.txe{border-color:#38bdf8;color:#7dd3fc}
    .line-btn.rxe{border-color:#22c55e;color:#86efac}
    .line-btn.txa{border-color:#f59e0b;color:#fcd34d}
    .line-btn.rxa{border-color:#ef4444;color:#fca5a5}
    .chart-box{position:relative;height:460px;margin-top:12px;background:#111827;border:1px solid #334155;border-radius:10px;padding:8px}
    .chart-box canvas{display:block;width:100%;height:100%}
    .detail{margin-top:10px;background:#111827;border:1px solid #334155;border-radius:10px;padding:10px;font-size:12px;color:#cbd5e1}
    .note{font-size:12px;color:#94a3b8;margin-top:8px}
    .mono{font-family:Consolas,monospace}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="toolbar">
      <strong>WeakNetEmulation Loss Monitor</strong>
      <label>
        Window
        <select id="lastSelect">
          <option value="60">60s</option>
          <option value="120">120s</option>
          <option value="300" selected>300s</option>
        </select>
      </label>
      <label>
        Peer IP
        <select id="peerSelect">
          <option value="all" selected>all</option>
        </select>
      </label>
      <button id="refreshBtn">Refresh</button>
      <button id="toggleBtn">Pause</button>
      <span id="updatedAt" class="status">updated: -</span>
      <span id="errorText" class="status"></span>
    </div>
    <div class="line-toggles">
      <button id="btnTXE" class="line-btn txe">TX Emulation Loss</button>
      <button id="btnRXE" class="line-btn rxe">RX Emulation Loss</button>
      <button id="btnTXA" class="line-btn txa">TX Actual Loss</button>
      <button id="btnRXA" class="line-btn rxa">RX Actual Loss</button>
    </div>
    <div class="legend" style="margin-top:8px">
      <span><i class="dot txe"></i>TX Emulation Loss</span>
      <span><i class="dot rxe"></i>RX Emulation Loss</span>
      <span><i class="dot txa"></i>TX Actual Loss</span>
      <span><i class="dot rxa"></i>RX Actual Loss</span>
    </div>
    <div class="chart-box">
      <canvas id="chartCanvas"></canvas>
    </div>
    <div id="detail" class="detail">hover chart to inspect one-second bucket details.</div>
    <div class="note">tx_actual_loss_rate = tx_emulation_loss + tx_real_loss; tx_real_loss depends on RTCP RR (often 0 without RR).</div>
  </div>

  <script>
    (function () {
      const state = {
        last: 300,
        paused: false,
        timer: null,
        points: [],
        peerIP: 'all',
        hoverIndex: -1,
        hoverYPercent: null,
        visible: {
          txe: true,
          rxe: true,
          txa: true,
          rxa: true,
        },
      };

      const colors = {
        txe: '#38bdf8',
        rxe: '#22c55e',
        txa: '#f59e0b',
        rxa: '#ef4444',
        axis: '#64748b',
        grid: '#1f2937',
        text: '#cbd5e1',
        cross: '#94a3b8',
      };

      const lineDefs = [
        { id: 'txe', field: 'tx_emulation_loss_rate', btnId: 'btnTXE', color: colors.txe },
        { id: 'rxe', field: 'rx_emulation_loss_rate', btnId: 'btnRXE', color: colors.rxe },
        { id: 'txa', field: 'tx_actual_loss_rate', btnId: 'btnTXA', color: colors.txa },
        { id: 'rxa', field: 'rx_actual_loss_rate', btnId: 'btnRXA', color: colors.rxa },
      ];

      const lastSelect = document.getElementById('lastSelect');
      const peerSelect = document.getElementById('peerSelect');
      const refreshBtn = document.getElementById('refreshBtn');
      const toggleBtn = document.getElementById('toggleBtn');
      const updatedAt = document.getElementById('updatedAt');
      const errorText = document.getElementById('errorText');
      const detail = document.getElementById('detail');
      const canvas = document.getElementById('chartCanvas');
      const ctx = canvas.getContext('2d');

      function fmtPercent(v) {
        return (v * 100).toFixed(2) + '%';
      }

      function fmtTime(tsSec) {
        return new Date(tsSec * 1000).toLocaleTimeString();
      }

      function resizeCanvas() {
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        canvas.width = Math.max(1, Math.floor(rect.width * dpr));
        canvas.height = Math.max(1, Math.floor(rect.height * dpr));
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      }

      function getPlotRect(w, h) {
        const left = 52;
        const right = Math.max(left + 40, w - 12);
        const top = 10;
        const bottom = Math.max(top + 40, h - 30);
        return { left, right, top, bottom };
      }

      function xOf(i, n, left, right) {
        if (n <= 1) return left;
        return left + (i * (right - left)) / (n - 1);
      }

      function yOf(percent, top, bottom) {
        return bottom - (percent / 100) * (bottom - top);
      }

      function percentOfY(y, top, bottom) {
        const p = ((bottom - y) / Math.max(1, (bottom - top))) * 100;
        return Math.max(0, Math.min(100, p));
      }

      function drawAxes(left, right, top, bottom) {
        ctx.strokeStyle = colors.grid;
        ctx.lineWidth = 1;
        for (let i = 0; i <= 4; i++) {
          const y = top + (i * (bottom - top)) / 4;
          ctx.beginPath();
          ctx.moveTo(left, y);
          ctx.lineTo(right, y);
          ctx.stroke();
        }
        ctx.strokeStyle = colors.axis;
        ctx.beginPath();
        ctx.moveTo(left, top);
        ctx.lineTo(left, bottom);
        ctx.lineTo(right, bottom);
        ctx.stroke();

        ctx.fillStyle = colors.text;
        ctx.font = '12px Arial';
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        for (let i = 0; i <= 4; i++) {
          const p = 100 - i * 25;
          const y = yOf(p, top, bottom);
          ctx.fillText(p + '%', left - 6, y);
        }
      }

      function drawXLabels(points, left, right, bottom) {
        if (!points.length) return;
        const ticks = Math.min(6, points.length);
        ctx.fillStyle = colors.text;
        ctx.font = '11px Arial';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        for (let i = 0; i < ticks; i++) {
          const idx = Math.floor((i * (points.length - 1)) / Math.max(1, ticks - 1));
          const x = xOf(idx, points.length, left, right);
          ctx.fillText(fmtTime(points[idx].ts_sec), x, bottom + 6);
        }
      }

      function drawLine(points, field, color, left, right, top, bottom) {
        if (points.length === 0) return;
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;
        ctx.beginPath();
        for (let i = 0; i < points.length; i++) {
          const x = xOf(i, points.length, left, right);
          const y = yOf((points[i][field] || 0) * 100, top, bottom);
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.stroke();
      }

      function drawYCursorLabel(y, p, left, top, bottom) {
        const text = p.toFixed(2) + '%';
        ctx.font = '11px Arial';
        const tw = ctx.measureText(text).width;
        const bh = 16;
        const bw = tw + 10;
        const x = left + 4;
        const clampedY = Math.max(top + bh / 2, Math.min(bottom - bh / 2, y));
        const by = clampedY - bh / 2;

        ctx.fillStyle = 'rgba(2,6,23,0.95)';
        ctx.fillRect(x, by, bw, bh);
        ctx.strokeStyle = colors.cross;
        ctx.strokeRect(x, by, bw, bh);
        ctx.fillStyle = '#e2e8f0';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        ctx.fillText(text, x + 5, clampedY);
      }

      function drawHover(points, i, left, right, top, bottom) {
        if (i < 0 || i >= points.length) return;
        const x = xOf(i, points.length, left, right);

        ctx.strokeStyle = colors.cross;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, top);
        ctx.lineTo(x, bottom);
        ctx.stroke();

        if (state.hoverYPercent !== null) {
          const y = yOf(state.hoverYPercent, top, bottom);
          ctx.beginPath();
          ctx.moveTo(left, y);
          ctx.lineTo(right, y);
          ctx.stroke();
          drawYCursorLabel(y, state.hoverYPercent, left, top, bottom);
        }

        const p = points[i];
        for (const d of lineDefs) {
          if (!state.visible[d.id]) continue;
          const y = yOf((p[d.field] || 0) * 100, top, bottom);
          ctx.fillStyle = d.color;
          ctx.beginPath();
          ctx.arc(x, y, 3, 0, Math.PI * 2);
          ctx.fill();
        }
      }

      function renderDetail() {
        const i = state.hoverIndex;
        if (i < 0 || i >= state.points.length) {
          detail.textContent = 'hover chart to inspect one-second bucket details.';
          return;
        }
        const p = state.points[i];
        let lines = [];
        for (const d of lineDefs) {
          if (!state.visible[d.id]) continue;
          lines.push(d.field + '=' + fmtPercent(p[d.field] || 0));
        }
        const yText = state.hoverYPercent === null ? '-' : state.hoverYPercent.toFixed(2) + '%';

        detail.innerHTML =
          '<div><strong>' + fmtTime(p.ts_sec) + '</strong> <span class="mono">cursor_y=' + yText + '</span></div>' +
          '<div class="mono">' + lines.join('  ') + '</div>' +
          '<div class="mono">tx_input=' + (p.tx_input_packets || 0) +
          ' tx_emu_drop=' + (p.tx_emu_dropped_packets || 0) +
          ' tx_real_lost=' + (p.tx_real_lost_packets || 0) + '</div>' +
          '<div class="mono">rx_input=' + (p.rx_input_packets || 0) +
          ' rx_emu_drop=' + (p.rx_emu_dropped_packets || 0) +
          ' rx_real_lost=' + (p.rx_real_lost_packets || 0) + '</div>';
      }

      function draw() {
        resizeCanvas();
        const rect = canvas.getBoundingClientRect();
        const w = rect.width;
        const h = rect.height;
        ctx.clearRect(0, 0, w, h);

        const plot = getPlotRect(w, h);
        drawAxes(plot.left, plot.right, plot.top, plot.bottom);
        drawXLabels(state.points, plot.left, plot.right, plot.bottom);

        for (const d of lineDefs) {
          if (!state.visible[d.id]) continue;
          drawLine(state.points, d.field, d.color, plot.left, plot.right, plot.top, plot.bottom);
        }
        drawHover(state.points, state.hoverIndex, plot.left, plot.right, plot.top, plot.bottom);
      }

      function findHoverIndex(clientX) {
        if (!state.points.length) return -1;
        const rect = canvas.getBoundingClientRect();
        const plot = getPlotRect(rect.width, rect.height);
        const x = Math.max(plot.left, Math.min(plot.right, clientX - rect.left));
        if (state.points.length <= 1) return 0;
        const ratio = (x - plot.left) / Math.max(1, plot.right - plot.left);
        return Math.max(0, Math.min(state.points.length - 1, Math.round(ratio * (state.points.length - 1))));
      }

      function findHoverYPercent(clientY) {
        const rect = canvas.getBoundingClientRect();
        const plot = getPlotRect(rect.width, rect.height);
        const y = Math.max(plot.top, Math.min(plot.bottom, clientY - rect.top));
        return percentOfY(y, plot.top, plot.bottom);
      }

      function syncLineButtons() {
        for (const d of lineDefs) {
          const btn = document.getElementById(d.btnId);
          if (!btn) continue;
          if (state.visible[d.id]) btn.classList.remove('off');
          else btn.classList.add('off');
        }
      }

      function syncPeerOptions(ips, selected) {
        const cur = selected || state.peerIP || 'all';
        const safe = Array.isArray(ips) ? ips : [];
        const options = ['<option value=\"all\">all</option>']
          .concat(safe.map(ip => '<option value=\"' + ip + '\">' + ip + '</option>'));
        peerSelect.innerHTML = options.join('');
        let next = cur;
        if (next !== 'all' && safe.indexOf(next) < 0) {
          next = 'all';
        }
        state.peerIP = next;
        peerSelect.value = next;
      }

      async function fetchData() {
        try {
          let url = '/api/weaknet/loss?last=' + encodeURIComponent(state.last);
          if (state.peerIP && state.peerIP !== 'all') {
            url += '&peer_ip=' + encodeURIComponent(state.peerIP);
          }
          const res = await fetch(url, { cache: 'no-store' });
          if (!res.ok) throw new Error('HTTP ' + res.status);
          const json = await res.json();
          syncPeerOptions(json.available_peer_ips || [], json.selected_peer_ip || state.peerIP);
          state.points = Array.isArray(json.points) ? json.points : [];
          if (state.hoverIndex >= state.points.length) state.hoverIndex = state.points.length - 1;
          if (state.hoverIndex < -1) state.hoverIndex = -1;
          updatedAt.textContent = 'updated: ' + new Date().toLocaleTimeString();
          errorText.textContent = '';
          errorText.classList.remove('err');
          draw();
          renderDetail();
        } catch (err) {
          errorText.textContent = 'error: ' + String(err);
          errorText.classList.add('err');
        }
      }

      function restartTimer() {
        if (state.timer) clearInterval(state.timer);
        if (state.paused) return;
        state.timer = setInterval(fetchData, 1000);
      }

      function toggleLine(id) {
        state.visible[id] = !state.visible[id];
        syncLineButtons();
        draw();
        renderDetail();
      }

      lastSelect.addEventListener('change', function () {
        state.last = Number(lastSelect.value) || 300;
        state.hoverIndex = -1;
        state.hoverYPercent = null;
        fetchData();
      });
      peerSelect.addEventListener('change', function () {
        state.peerIP = peerSelect.value || 'all';
        state.hoverIndex = -1;
        state.hoverYPercent = null;
        fetchData();
      });
      refreshBtn.addEventListener('click', fetchData);
      toggleBtn.addEventListener('click', function () {
        state.paused = !state.paused;
        toggleBtn.textContent = state.paused ? 'Resume' : 'Pause';
        restartTimer();
      });

      document.getElementById('btnTXE').addEventListener('click', function () { toggleLine('txe'); });
      document.getElementById('btnRXE').addEventListener('click', function () { toggleLine('rxe'); });
      document.getElementById('btnTXA').addEventListener('click', function () { toggleLine('txa'); });
      document.getElementById('btnRXA').addEventListener('click', function () { toggleLine('rxa'); });

      canvas.addEventListener('mousemove', function (ev) {
        state.hoverIndex = findHoverIndex(ev.clientX);
        state.hoverYPercent = findHoverYPercent(ev.clientY);
        draw();
        renderDetail();
      });
      canvas.addEventListener('mouseleave', function () {
        state.hoverIndex = -1;
        state.hoverYPercent = null;
        draw();
        renderDetail();
      });
      window.addEventListener('resize', draw);

      syncLineButtons();
      fetchData();
      restartTimer();
    })();
  </script>
</body>
</html>`))
}

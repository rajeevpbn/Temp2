import asyncio
import base64
from collections import deque
import gzip
import json
import os
import socket
import subprocess
import threading
import time
import webbrowser
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import httpx
import uvicorn
import websockets

app = FastAPI()
active_websockets: List[WebSocket] = []
main_async_loop = None

# Historical cache buffer (keeps last 500 calls in memory)
HISTORY_CACHE = deque(maxlen=500)
broadcast_queue: asyncio.Queue = None

CAPTURE_MEDIA_FILES = False

# ==============================================================================
# DYNAMIC PORT ALLOCATION (Collision-free)
# ==============================================================================
def find_available_port(start_range=9000, end_range=9999) -> int:
    for port in range(start_range, end_range):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]

DASHBOARD_PORT = find_available_port(8080, 8180)
CDP_PORT = find_available_port(9400, 9999)

# ==============================================================================
# UI DASHBOARD
# ==============================================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>API Call Tracer Pro</title>
  <style>
    :root {
      --bg-base: #090d16;
      --bg-surface: #111827;
      --bg-elevated: #1e293b;
      --bg-card: #182234;
      --border-subtle: #243247;
      --border-focus: #3b82f6;
      --text-primary: #f1f5f9;
      --text-secondary: #94a3b8;
      --text-muted: #64748b;
      --accent: #38bdf8;
      --status-ok: #34d399;
      --status-warn: #fbbf24;
      --status-err: #f87171;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0; padding: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg-base); color: var(--text-primary);
      height: 100vh; display: flex; flex-direction: column; overflow: hidden;
      -webkit-font-smoothing: antialiased;
    }

    header {
      background: var(--bg-surface);
      padding: 10px 18px;
      border-bottom: 1px solid var(--border-subtle);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
    }
    .brand { display: flex; align-items: center; gap: 10px; }
    h1 {
      margin: 0; font-size: 1rem; font-weight: 600;
      color: var(--text-primary); display: flex; align-items: center; gap: 8px;
    }
    h1::before {
      content: ""; width: 8px; height: 8px; border-radius: 50%;
      background: var(--accent); display: inline-block;
    }

    .badge {
      padding: 2px 8px; border-radius: 4px; font-size: 0.72rem; font-weight: 500;
      background: rgba(52, 211, 153, 0.15); color: var(--status-ok);
      border: 1px solid rgba(52, 211, 153, 0.3);
    }

    .header-actions { display: flex; align-items: center; gap: 8px; }

    button, select, input[type="text"] {
      background: var(--bg-elevated); color: var(--text-primary);
      border: 1px solid var(--border-subtle); border-radius: 5px;
      padding: 5px 11px; font-size: 0.8rem; font-weight: 500;
      cursor: pointer; outline: none; transition: all 0.12s ease-in-out;
    }
    button:hover { background: #27354a; border-color: #3b4d66; }
    .btn-danger {
      background: rgba(239, 68, 68, 0.1); color: #fca5a5;
      border-color: rgba(239, 68, 68, 0.25);
    }
    .btn-danger:hover { background: rgba(239, 68, 68, 0.2); }

    .dropdown { position: relative; display: inline-block; }
    .dropdown-content {
      display: none; position: absolute; right: 0; top: calc(100% + 4px);
      background: var(--bg-surface); min-width: 220px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.5), 0 0 0 1px var(--border-subtle);
      border-radius: 6px; z-index: 100; overflow: hidden; padding: 4px;
    }
    .dropdown-content a {
      color: var(--text-secondary); padding: 7px 12px; text-decoration: none;
      display: block; font-size: 0.8rem; border-radius: 4px;
    }
    .dropdown-content a:hover { background: var(--bg-elevated); color: var(--text-primary); }
    .dropdown-divider { height: 1px; background: var(--border-subtle); margin: 4px 6px; }
    .dropdown:hover .dropdown-content { display: block; }

    .toolbar {
      background: var(--bg-surface); padding: 7px 18px; border-bottom: 1px solid var(--border-subtle);
      display: flex; align-items: center; gap: 10px; font-size: 0.8rem; flex-wrap: wrap;
    }
    .search-box { flex: 1; min-width: 180px; background: var(--bg-base); border: 1px solid var(--border-subtle); color: #fff; }

    .workspace { display: grid; grid-template-columns: 460px 1fr; flex: 1; overflow: hidden; }
    .sidebar { background: var(--bg-surface); border-right: 1px solid var(--border-subtle); display: flex; flex-direction: column; overflow: hidden; }
    .list-header {
      padding: 8px 14px; background: var(--bg-surface); border-bottom: 1px solid var(--border-subtle);
      display: grid; grid-template-columns: 74px 58px 1fr 64px; font-size: 0.72rem; font-weight: 600; color: var(--text-muted); text-transform: uppercase;
    }
    #log-list { flex: 1; overflow-y: auto; }
    .log-row {
      display: grid; grid-template-columns: 74px 58px 1fr 64px; padding: 9px 14px;
      border-bottom: 1px solid rgba(36, 50, 71, 0.4); cursor: pointer; align-items: center; gap: 6px; font-size: 0.78rem;
    }
    .log-row:hover { background: rgba(30, 41, 59, 0.5); }
    .log-row.selected { background: var(--bg-card); border-left: 2px solid var(--accent); }

    .badge-method {
      padding: 2px 5px; border-radius: 3px; font-weight: 700; font-family: monospace; font-size: 0.7rem; text-align: center; width: fit-content;
    }
    .badge-method.GET { background: rgba(52, 211, 153, 0.12); color: #34d399; }
    .badge-method.POST { background: rgba(56, 189, 248, 0.12); color: #38bdf8; }
    .badge-method.PUT { background: rgba(251, 191, 36, 0.12); color: #fbbf24; }
    .badge-method.DELETE { background: rgba(248, 113, 113, 0.12); color: #f87171; }
    .badge-method.PATCH { background: rgba(192, 132, 252, 0.12); color: #c084fc; }
    .badge-method.OPTIONS { background: rgba(148, 163, 184, 0.15); color: #94a3b8; }
    .badge-method.WS { background: rgba(245, 158, 11, 0.2); color: #fbbf24; }
    .badge-method.MEDIA { background: rgba(192, 132, 252, 0.15); color: #c084fc; }

    .status-pill { font-family: monospace; font-weight: 600; }
    .status-2xx { color: var(--status-ok); }
    .status-3xx { color: var(--status-warn); }
    .status-4xx, .status-5xx { color: var(--status-err); }
    .url-cell { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--text-secondary); }
    .time-cell { text-align: right; font-family: monospace; color: var(--text-muted); font-size: 0.72rem; }

    .detail-panel { background: var(--bg-base); display: flex; flex-direction: column; overflow: hidden; }
    .detail-nav {
      background: var(--bg-surface); border-bottom: 1px solid var(--border-subtle);
      display: flex; justify-content: space-between; align-items: center; padding: 0 16px;
    }
    .tabs { display: flex; gap: 2px; }
    .tab-btn {
      background: transparent; border: none; padding: 10px 14px; font-size: 0.8rem;
      font-weight: 500; color: var(--text-muted); border-bottom: 2px solid transparent; cursor: pointer;
    }
    .tab-btn.active { color: #fff; font-weight: 600; border-bottom-color: var(--accent); }
    .detail-content { flex: 1; padding: 16px 18px; overflow-y: auto; }
    .card { background: var(--bg-surface); border: 1px solid var(--border-subtle); border-radius: 6px; padding: 14px; margin-bottom: 14px; }
    .card-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; font-weight: 600; font-size: 0.78rem; color: var(--text-secondary); }
    pre {
      background: var(--bg-base); padding: 12px; border-radius: 5px; overflow-x: auto;
      border: 1px solid var(--border-subtle); font-family: monospace; font-size: 0.8rem; line-height: 1.5; margin: 0; color: #cbd5e1;
      white-space: pre-wrap; word-break: break-all;
    }
    .json-key { color: #60a5fa; }
    .json-str { color: #a5f3fc; }
    .json-num { color: #fde047; }
    .json-bool { color: #f472b6; font-weight: 600; }
    .json-null { color: #64748b; font-style: italic; }

    table.headers-table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }
    table.headers-table th, table.headers-table td { text-align: left; padding: 6px 8px; border-bottom: 1px solid rgba(36, 50, 71, 0.5); }
    table.headers-table th { color: var(--text-muted); width: 28%; font-weight: 500; }
    table.headers-table td { font-family: monospace; word-break: break-all; color: var(--text-primary); }
    .empty-state { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 100%; color: var(--text-muted); font-size: 0.85rem; }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <h1>API Call Tracer Pro</h1>
      <span class="badge" id="status-badge">Connecting...</span>
      <span style="font-size: 0.78rem; color: var(--text-muted);" id="counter-label">(0 calls)</span>
    </div>

    <div class="header-actions">
      <select id="media-capture-mode" onchange="updateTrackerCaptureMode()" style="border-color: rgba(56, 189, 248, 0.4); background: #132238;">
        <option value="ignore_media" selected>Ignore Big Media (Videos, Images, Streams)</option>
        <option value="capture_media">Capture Everything (Include Videos & Images)</option>
      </select>

      <button id="pause-btn" onclick="togglePause()">Pause</button>

      <div class="dropdown">
        <button>Export &#9662;</button>
        <div class="dropdown-content">
          <a href="#" onclick="exportData('postman_all')">Postman Collection (All Calls)</a>
          <a href="#" onclick="exportData('postman_selected')">Postman Collection (Selected)</a>
          <div class="dropdown-divider"></div>
          <a href="#" onclick="exportData('selected')">Export Selected (JSON)</a>
          <a href="#" onclick="exportData('filtered')">Export Filtered (JSON)</a>
          <a href="#" onclick="exportData('all')">Export All (JSON)</a>
          <div class="dropdown-divider"></div>
          <a href="#" onclick="exportData('har')">Export HAR (HTTP Archive)</a>
          <a href="#" onclick="exportData('csv')">Export Summary (CSV)</a>
        </div>
      </div>

      <button class="btn-danger" onclick="clearLogs()">Clear</button>
    </div>
  </header>

  <div class="toolbar">
    <input type="text" class="search-box" id="search-filter" placeholder="Filter endpoint, pyActivity, or PRServlet..." oninput="applyFilters()" />
    <label style="display:flex; align-items:center; gap:5px; cursor:pointer; color:var(--text-secondary); user-select:none;">
      <input type="checkbox" id="regex-toggle" onchange="applyFilters()"> Regex
    </label>

    <select id="method-filter" onchange="applyFilters()">
      <option value="">All Methods</option>
      <option value="GET">GET</option>
      <option value="POST">POST</option>
      <option value="PUT">PUT</option>
      <option value="DELETE">DELETE</option>
      <option value="PATCH">PATCH</option>
      <option value="OPTIONS">OPTIONS</option>
      <option value="WS">WS (Push/WebSocket)</option>
      <option value="MEDIA">MEDIA</option>
    </select>

    <select id="status-filter" onchange="applyFilters()">
      <option value="">All Status Codes</option>
      <option value="2xx">2xx Success</option>
      <option value="3xx">3xx Redirect</option>
      <option value="4xx">4xx Client Error</option>
      <option value="5xx">5xx Server Error</option>
    </select>

    <label style="display:flex; align-items:center; gap:5px; margin-left:auto; cursor:pointer; color:var(--text-secondary); user-select:none;">
      <input type="checkbox" id="autoscroll-toggle" checked> Auto-Scroll
    </label>
  </div>

  <div class="workspace">
    <div class="sidebar">
      <div class="list-header">
        <span>Method</span>
        <span>Status</span>
        <span>URL Path</span>
        <span style="text-align: right;">Time</span>
      </div>
      <div id="log-list"></div>
    </div>

    <div class="detail-panel">
      <div class="detail-nav" id="detail-nav" style="display: none;">
        <div class="tabs">
          <button class="tab-btn active" onclick="switchTab('response')">Response</button>
          <button class="tab-btn" onclick="switchTab('request')">Request</button>
          <button class="tab-btn" onclick="switchTab('query')">Query / Pega Params</button>
          <button class="tab-btn" onclick="switchTab('headers')">Headers</button>
          <button class="tab-btn" onclick="switchTab('overview')">Overview</button>
        </div>
        <div style="display: flex; gap: 6px;">
          <button onclick="exportData('postman_selected')">To Postman</button>
          <button onclick="copyCurrentCurl()">Copy as cURL</button>
        </div>
      </div>

      <div class="detail-content" id="detail-content">
        <div class="empty-state">
          <div>Interact with your Pega portal in Chrome. All calls stream here live.</div>
        </div>
      </div>
    </div>
  </div>

  <script>
    let logs = [];
    let filteredLogs = [];
    let selectedIdx = null;
    let isPaused = false;
    let isRawMode = false;
    let activeTab = 'response';
    let socket = null;
    let pingInterval = null;

    const logListEl = document.getElementById('log-list');
    const detailContentEl = document.getElementById('detail-content');
    const detailNavEl = document.getElementById('detail-nav');
    const counterLabel = document.getElementById('counter-label');
    const statusBadge = document.getElementById('status-badge');
    const autoScrollEl = document.getElementById('autoscroll-toggle');

    function initWebSocket() {
      if (socket) {
        try { socket.close(); } catch(e) {}
      }
      clearInterval(pingInterval);

      socket = new WebSocket(`ws://${location.host}/ws`);

      socket.onopen = () => {
        statusBadge.textContent = "Live Capturing";
        statusBadge.style.color = "var(--status-ok)";
        statusBadge.style.background = "rgba(52, 211, 153, 0.15)";

        pingInterval = setInterval(() => {
          if (socket.readyState === WebSocket.OPEN) {
            socket.send(JSON.stringify({ action: "ping" }));
          }
        }, 4000);
      };

      socket.onclose = () => {
        statusBadge.textContent = "Reconnecting...";
        statusBadge.style.color = "var(--status-warn)";
        statusBadge.style.background = "rgba(251, 191, 36, 0.15)";
        clearInterval(pingInterval);
        setTimeout(initWebSocket, 1500);
      };

      socket.onerror = () => {
        try { socket.close(); } catch(e) {}
      };

      socket.onmessage = (event) => {
        if (isPaused) return;
        try {
          const payload = JSON.parse(event.data);
          
          if (payload.type === "history_batch") {
            logs = payload.items;
            applyFilters();
            return;
          }

          const item = payload;
          if (!item._id) {
            item._id = Date.now() + Math.random().toString(36).substr(2, 4);
          }
          logs.unshift(item);
          applyFilters();
          if (autoScrollEl.checked) logListEl.scrollTop = 0;
        } catch(e) {}
      };
    }
    initWebSocket();

    function updateTrackerCaptureMode() {
      const mode = document.getElementById('media-capture-mode').value;
      if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({
          action: "set_media_mode",
          capture_media: (mode === "capture_media")
        }));
      }
    }

    function applyFilters() {
      const search = document.getElementById('search-filter').value.trim();
      const isRegex = document.getElementById('regex-toggle').checked;
      const method = document.getElementById('method-filter').value;
      const statusFilter = document.getElementById('status-filter').value;

      let regexMatcher = null;
      if (search && isRegex) {
        try { regexMatcher = new RegExp(search, 'i'); } catch (e) {}
      }

      filteredLogs = logs.filter(log => {
        let matchSearch = true;
        if (search) {
          if (regexMatcher) matchSearch = regexMatcher.test(log.url);
          else matchSearch = log.url.toLowerCase().includes(search.toLowerCase());
        }

        const effectiveMethod = log.is_media ? 'MEDIA' : log.request.method;
        const matchMethod = !method || effectiveMethod === method;

        const code = Number(log.response.status) || 0;
        let matchStatus = true;
        if (statusFilter === '2xx') matchStatus = code >= 200 && code < 300;
        else if (statusFilter === '3xx') matchStatus = code >= 300 && code < 400;
        else if (statusFilter === '4xx') matchStatus = code >= 400 && code < 500;
        else if (statusFilter === '5xx') matchStatus = code >= 500 && code < 600;

        return matchSearch && matchMethod && matchStatus;
      });

      counterLabel.textContent = `(${filteredLogs.length} shown / ${logs.length} total)`;
      renderList();
    }

    function renderList() {
      if (filteredLogs.length === 0) {
        logListEl.innerHTML = '<div style="padding: 24px; text-align: center; color: var(--text-muted); font-size: 0.78rem;">No matching calls</div>';
        return;
      }

      logListEl.innerHTML = filteredLogs.map((log) => {
        const isSelected = selectedIdx !== null && filteredLogs[selectedIdx]?._id === log._id;
        const statusClass = log.response.status < 300 ? 'status-2xx' : (log.response.status < 400 ? 'status-3xx' : 'status-4xx');
        
        let pathOnly = log.url;
        try { 
          const u = new URL(log.url);
          pathOnly = u.pathname + (u.search.length > 50 ? u.search.substring(0, 50) + "..." : u.search);
        } catch (e) {}

        const displayMethod = log.is_media ? 'MEDIA' : log.request.method;

        return `
          <div class="log-row ${isSelected ? 'selected' : ''}" onclick="selectItem('${log._id}')">
            <div><span class="badge-method ${displayMethod}">${displayMethod}</span></div>
            <div><span class="status-pill ${statusClass}">${log.response.status || 'ERR'}</span></div>
            <div class="url-cell" title="${log.url}">${pathOnly}</div>
            <div class="time-cell">${log.duration_ms}ms</div>
          </div>
        `;
      }).join('');
    }

    function selectItem(logId) {
      selectedIdx = filteredLogs.findIndex(l => l._id === logId);
      renderList();
      renderDetails();
    }

    window.addEventListener('keydown', (e) => {
      if (!filteredLogs.length) return;
      if (e.key === 'ArrowDown') {
        if (selectedIdx === null || selectedIdx >= filteredLogs.length - 1) selectedIdx = 0;
        else selectedIdx++;
        renderList();
        renderDetails();
        e.preventDefault();
      } else if (e.key === 'ArrowUp') {
        if (selectedIdx === null || selectedIdx <= 0) selectedIdx = filteredLogs.length - 1;
        else selectedIdx--;
        renderList();
        renderDetails();
        e.preventDefault();
      }
    });

    function switchTab(tab) {
      activeTab = tab;
      document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.toggle('active', btn.textContent.toLowerCase() === tab));
      renderDetails();
    }

    function toggleRawMode() {
      isRawMode = !isRawMode;
      renderDetails();
    }

    function renderDetails() {
      if (selectedIdx === null || !filteredLogs[selectedIdx]) {
        detailNavEl.style.display = 'none';
        detailContentEl.innerHTML = '<div class="empty-state">Select an API call on the left.</div>';
        return;
      }
      detailNavEl.style.display = 'flex';
      const log = filteredLogs[selectedIdx];

      if (activeTab === 'response') {
        detailContentEl.innerHTML = `
          <div class="card">
            <div class="card-header">
              <span>Status: ${log.response.status} | Latency: ${log.duration_ms}ms</span>
              <div style="display:flex; gap:6px;">
                <button onclick="toggleRawMode()">${isRawMode ? 'Format View' : 'Raw View'}</button>
                <button onclick="copyToClip(filteredLogs[${selectedIdx}].response.body)">Copy Body</button>
              </div>
            </div>
            <pre>${isRawMode ? escapeHtml(String(log.response.body || '')) : colorizeJson(log.response.body)}</pre>
          </div>`;
      } else if (activeTab === 'request') {
        detailContentEl.innerHTML = `
          <div class="card">
            <div class="card-header">
              <span>Payload (${log.request.method})</span>
              <div style="display:flex; gap:6px;">
                <button onclick="toggleRawMode()">${isRawMode ? 'Format View' : 'Raw View'}</button>
                <button onclick="copyToClip(filteredLogs[${selectedIdx}].request.post_data)">Copy Payload</button>
              </div>
            </div>
            <pre>${isRawMode ? escapeHtml(String(log.request.post_data || '')) : colorizeJson(log.request.post_data)}</pre>
          </div>`;
      } else if (activeTab === 'query') {
        let queryParams = {};
        try {
          const u = new URL(log.url);
          u.searchParams.forEach((v, k) => { queryParams[k] = v; });
        } catch(e) {}
        detailContentEl.innerHTML = `
          <div class="card">
            <div class="card-header"><span>URL Query Parameters (e.g., pyActivity, PZP)</span></div>
            ${renderTable(queryParams, '(No query parameters)')}
          </div>`;
      } else if (activeTab === 'headers') {
        detailContentEl.innerHTML = `
          <div class="card"><div class="card-header"><span>Response Headers</span></div>${renderTable(log.response.headers)}</div>
          <div class="card"><div class="card-header"><span>Request Headers</span></div>${renderTable(log.request.headers)}</div>`;
      } else if (activeTab === 'overview') {
        detailContentEl.innerHTML = `
          <div class="card">
            <div class="card-header"><span>Request Overview</span></div>
            <table class="headers-table">
              <tr><th>Full URL:</th><td>${log.url}</td></tr>
              <tr><th>HTTP Method:</th><td>${log.request.method}</td></tr>
              <tr><th>Status Code:</th><td>${log.response.status}</td></tr>
              <tr><th>Duration:</th><td>${log.duration_ms} ms</td></tr>
            </table>
          </div>`;
      }
    }

    function renderTable(obj, emptyMsg = '(No headers)') {
      if (!obj || Object.keys(obj).length === 0) return `<div style="color:var(--text-muted);font-size:0.78rem;">${emptyMsg}</div>`;
      let html = '<table class="headers-table">';
      for (const [k, v] of Object.entries(obj)) html += `<tr><th>${k}</th><td>${v}</td></tr>`;
      return html + '</table>';
    }

    function buildPostmanCollection(items, name = "API Trace") {
      return {
        info: {
          name: `${name} (${new Date().toLocaleTimeString()})`,
          schema: "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
        },
        item: items.map(entry => {
          let urlObj;
          try {
            urlObj = new URL(entry.url);
          } catch(e) {
            urlObj = { protocol: 'https:', host: 'unknown', pathname: entry.url, searchParams: new URLSearchParams() };
          }
          const headerList = Object.entries(entry.request.headers || {})
            .filter(([k]) => !['content-length', 'host'].includes(k.toLowerCase()))
            .map(([k, v]) => ({ key: k, value: String(v), type: "text" }));
          const queryList = [];
          if (urlObj.searchParams) {
            urlObj.searchParams.forEach((v, k) => queryList.push({ key: k, value: v }));
          }
          let bodyObj = undefined;
          if (entry.request.post_data) {
            bodyObj = {
              mode: "raw",
              raw: entry.request.post_data,
              options: { raw: { language: entry.request.headers?.['content-type']?.includes('json') ? 'json' : 'text' } }
            };
          }
          return {
            name: `${entry.request.method} ${urlObj.pathname || entry.url}`,
            request: {
              method: entry.request.method,
              header: headerList,
              body: bodyObj,
              url: {
                raw: entry.url,
                protocol: urlObj.protocol ? urlObj.protocol.replace(':', '') : 'https',
                host: urlObj.host ? urlObj.host.split('.') : [],
                path: urlObj.pathname ? urlObj.pathname.split('/').filter(Boolean) : [],
                query: queryList.length ? queryList : undefined
              }
            }
          };
        })
      };
    }

    function exportData(type) {
      if (!logs.length) return alert("No traces captured yet!");
      if (type === 'postman_all') {
        downloadFile(JSON.stringify(buildPostmanCollection(logs, "Complete API Trace"), null, 2), `postman_all_${Date.now()}.json`, 'application/json');
      } else if (type === 'postman_selected') {
        if (selectedIdx === null || !filteredLogs[selectedIdx]) return alert("Select a call first!");
        const single = filteredLogs[selectedIdx];
        downloadFile(JSON.stringify(buildPostmanCollection([single], `Call - ${single.request.method}`), null, 2), `postman_${single.request.method}_${Date.now()}.json`, 'application/json');
      } else if (type === 'selected') {
        if (selectedIdx === null || !filteredLogs[selectedIdx]) return alert("Select a call first!");
        downloadFile(JSON.stringify(filteredLogs[selectedIdx], null, 2), `trace_selected_${Date.now()}.json`, 'application/json');
      } else if (type === 'filtered') {
        downloadFile(JSON.stringify(filteredLogs, null, 2), `trace_filtered_${Date.now()}.json`, 'application/json');
      } else if (type === 'all') {
        downloadFile(JSON.stringify(logs, null, 2), `trace_all_${Date.now()}.json`, 'application/json');
      } else if (type === 'har') {
        const har = {
          log: {
            version: "1.2",
            creator: { name: "Pure CDP Tracer", version: "1.0" },
            entries: logs.map(i => ({
              startedDateTime: new Date().toISOString(),
              time: i.duration_ms,
              request: {
                method: i.request.method,
                url: i.url,
                headers: Object.entries(i.request.headers || {}).map(([k, v]) => ({ name: k, value: String(v) })),
                postData: i.request.post_data ? { mimeType: "application/json", text: i.request.post_data } : undefined
              },
              response: {
                status: i.response.status,
                headers: Object.entries(i.response.headers || {}).map(([k, v]) => ({ name: k, value: String(v) })),
                content: { mimeType: "application/json", text: i.response.body }
              }
            }))
          }
        };
        downloadFile(JSON.stringify(har, null, 2), `session_${Date.now()}.har`, 'application/json');
      } else if (type === 'csv') {
        let csv = "Method,Status,Duration_ms,URL\n";
        logs.forEach(l => {
          csv += `"${l.request.method}","${l.response.status}","${l.duration_ms}","${l.url.replace(/"/g, '""')}"\n`;
        });
        downloadFile(csv, `summary_${Date.now()}.csv`, 'text/csv');
      }
    }

    function downloadFile(content, filename, mimeType) {
      const blob = new Blob([content], { type: mimeType });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
      URL.revokeObjectURL(url);
    }

    function copyCurrentCurl() {
      if (selectedIdx === null || !filteredLogs[selectedIdx]) return;
      const log = filteredLogs[selectedIdx];
      let curl = `curl -X ${log.request.method} "${log.url}"`;
      for (const [k, v] of Object.entries(log.request.headers || {})) {
        if (!['content-length', 'host'].includes(k.toLowerCase())) curl += ` \\\n  -H "${k}: ${v.replace(/"/g, '\\"')}"`;
      }
      if (log.request.post_data) curl += ` \\\n  --data '${log.request.post_data.replace(/'/g, "'\\''")}'`;
      navigator.clipboard.writeText(curl);
      alert("cURL copied to clipboard!");
    }

    function copyToClip(text) {
      navigator.clipboard.writeText(typeof text === 'object' ? JSON.stringify(text, null, 2) : (text || ''));
      alert("Copied!");
    }

    function colorizeJson(raw) {
      if (!raw) return '<span style="color:var(--text-muted);">(Empty Body)</span>';
      try {
        const obj = typeof raw === 'string' ? JSON.parse(raw) : raw;
        const json = JSON.stringify(obj, null, 2);
        return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, match => {
          let cls = 'json-num';
          if (/^"/.test(match)) cls = /:$/.test(match) ? 'json-key' : 'json-str';
          else if (/true|false/.test(match)) cls = 'json-bool';
          else if (/null/.test(match)) cls = 'json-null';
          return `<span class="${cls}">${match}</span>`;
        });
      } catch (e) {
        return escapeHtml(String(raw));
      }
    }

    function escapeHtml(str) {
      return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function clearLogs() { logs = []; filteredLogs = []; selectedIdx = null; renderList(); renderDetails(); }
    function togglePause() {
      isPaused = !isPaused;
      const btn = document.getElementById('pause-btn');
      btn.textContent = isPaused ? "Resume" : "Pause";
      statusBadge.textContent = isPaused ? "Paused" : "Live Capturing";
      statusBadge.style.color = isPaused ? "var(--status-warn)" : "var(--status-ok)";
      statusBadge.style.background = isPaused ? "rgba(251, 191, 36, 0.15)" : "rgba(52, 211, 153, 0.15)";
    }
  </script>
</body>
</html>
"""

# ==============================================================================
# FASTAPI & WEBSOCKET ENGINE
# ==============================================================================
@app.get("/", response_class=HTMLResponse)
def get_dashboard():
    return DASHBOARD_HTML

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    global CAPTURE_MEDIA_FILES
    await ws.accept()
    active_websockets.append(ws)

    # Immediately replay all recent logs to client on connect
    if HISTORY_CACHE:
        try:
            await ws.send_text(json.dumps({
                "type": "history_batch",
                "items": list(HISTORY_CACHE)
            }))
        except Exception:
            pass

    try:
        while True:
            text = await ws.receive_text()
            try:
                msg = json.loads(text)
                if msg.get("action") == "set_media_mode":
                    CAPTURE_MEDIA_FILES = bool(msg.get("capture_media", False))
                    state = "ENABLED" if CAPTURE_MEDIA_FILES else "DISABLED"
                    print(f"[*] Media capture mode: {state}")
                elif msg.get("action") == "ping":
                    pass
            except Exception:
                pass
    except WebSocketDisconnect:
        if ws in active_websockets:
            active_websockets.remove(ws)

async def start_broadcaster_worker():
    global broadcast_queue
    broadcast_queue = asyncio.Queue(maxsize=3000)
    while True:
        record = await broadcast_queue.get()
        HISTORY_CACHE.appendleft(record)
        msg = json.dumps(record)
        for ws in list(active_websockets):
            try:
                await ws.send_text(msg)
            except Exception:
                if ws in active_websockets:
                    active_websockets.remove(ws)
        broadcast_queue.task_done()

def queue_record(record: dict):
    if not main_async_loop or not broadcast_queue:
        return
    record["_id"] = str(time.time()) + os.urandom(2).hex()
    try:
        main_async_loop.call_soon_threadsafe(broadcast_queue.put_nowait, record)
    except Exception:
        pass

def start_uvicorn(port: int):
    global main_async_loop
    main_async_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(main_async_loop)
    main_async_loop.create_task(start_broadcaster_worker())
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    main_async_loop.run_until_complete(server.serve())

# ==============================================================================
# PEGA-SAFE ASSET FILTERING
# ==============================================================================
STRICT_BINARY_EXTS = (
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".woff", ".woff2", ".ttf", ".svg", ".ico",
    ".mp4", ".mp3", ".webm", ".ts", ".m3u8", ".pdf", ".zip"
)

def is_strictly_binary(url: str, headers: dict) -> bool:
    clean_url = url.lower().split("?")[0]
    if clean_url.endswith(STRICT_BINARY_EXTS):
        return True
    content_type = ""
    for k, v in headers.items():
        if k.lower() == "content-type":
            content_type = v.lower()
            break
    binary_content = ["image/", "font/", "video/", "audio/", "application/pdf"]
    return any(b in content_type for b in binary_content)

def clean_post_data(raw_text: str):
    if not raw_text:
        return None
    if len(raw_text) > 1024 * 1024:
        return "(Large payload > 1MB - truncated)"
    return raw_text

def decode_cdp_body(raw_body: str, is_base64: bool):
    if not raw_body:
        return ""
    if not is_base64:
        return raw_body
    try:
        data = base64.b64decode(raw_body)
        if len(data) > 3 * 1024 * 1024:
            return "(Binary payload > 3MB - skipped)"
        if len(data) > 2 and data[0] == 0x1f and data[1] == 0x8b:
            try:
                return gzip.decompress(data).decode("utf-8", errors="replace")
            except Exception:
                return "(Compressed GZIP Binary)"
        return data.decode("utf-8", errors="replace")
    except Exception:
        return "(Binary Data / Undecodable)"

# ==============================================================================
# CHROME LAUNCHER (With Enterprise & Pega Flags)
# ==============================================================================
def find_chrome_path():
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe"),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/usr/bin/google-chrome"
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None

def launch_chrome_process(chrome_path: str, port: int):
    user_data = os.path.join(os.environ.get("TEMP", "C:\\Temp"), f"cdp_tracer_{port}_{int(time.time())}")
    cmd = [
        chrome_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data}",
        "--start-maximized",
        "--no-first-run",
        "--no-default-browser-check",
        "--test-type",  # Suppresses unsupported flag warning bar
        "--ignore-certificate-errors",  # Supports internal corporate inspection certs
        "--auth-server-allowlist=*",  # Pass enterprise SSO/Kerberos/NTLM auth
        "--auth-schemes=basic,digest,ntlm,negotiate",
        "about:blank"
    ]
    print(f"[+] Launching Chrome on port {port}...")
    return subprocess.Popen(cmd)

# ==============================================================================
# PURE CDP ENGINE
# ==============================================================================
async def run_cdp_tracer(port: int):
    print(f"[*] Connecting to Chrome CDP port {port}...")
    browser_ws = None

    for _ in range(30):
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"http://127.0.0.1:{port}/json/version", timeout=1.0)
                data = res.json()
                if "webSocketDebuggerUrl" in data:
                    browser_ws = data["webSocketDebuggerUrl"]
                    break
        except Exception:
            pass
        await asyncio.sleep(0.5)

    if not browser_ws:
        print("[-] Failed to find Chrome debugging port.")
        return

    print(f"[+] Connected to Chrome master socket: {browser_ws}")

    async with websockets.connect(browser_ws, max_size=100 * 1024 * 1024) as ws:
        # Browser-level auto-attach across ALL pages, iframes, and sub-frames
        await ws.send(json.dumps({
            "id": 1,
            "method": "Target.setAutoAttach",
            "params": {
                "autoAttach": True,
                "waitForDebuggerOnStart": False,
                "flatten": True
            }
        }))
        await ws.send(json.dumps({
            "id": 2,
            "method": "Target.setDiscoverTargets",
            "params": {"discover": True}
        }))

        sessions = set()
        in_flight = {}
        request_body_map = {}
        msg_counter = 100

        # Scavenger to prevent stale references
        async def purge_stale_requests():
            while True:
                await asyncio.sleep(10)
                now = time.time()
                for req_id in list(in_flight.keys()):
                    if now - in_flight[req_id]["start_time"] > 35:
                        del in_flight[req_id]
                for mid in list(request_body_map.keys()):
                    if now - request_body_map[mid].get("created", now) > 20:
                        del request_body_map[mid]

        asyncio.create_task(purge_stale_requests())

        print("\n========================================================")
        print("API Call Tracer Pro ACTIVE! (All Pega IFrames & Calls Bound)")
        print(f"Dashboard:  http://127.0.0.1:{DASHBOARD_PORT}")
        print(f"Chrome CDP: port {port}")
        print("========================================================\n")

        while True:
            raw_msg = await ws.recv()
            event = json.loads(raw_msg)
            session_id = event.get("sessionId")
            method = event.get("method")
            params = event.get("params", {})
            event_id = event.get("id")

            # 1. Asynchronous response body delivery
            if event_id and event_id in request_body_map:
                req_info = request_body_map.pop(event_id)
                res_dict = event.get("result", {})
                raw_body = res_dict.get("body", "")
                is_b64 = res_dict.get("base64Encoded", False)

                if "error" in event:
                    body = "(Response body not available or served from cache)"
                else:
                    body = decode_cdp_body(raw_body, is_b64)

                record = {
                    "url": req_info["url"],
                    "duration_ms": req_info["duration_ms"],
                    "is_media": False,
                    "request": {
                        "method": req_info["method"],
                        "headers": req_info["headers"],
                        "post_data": req_info["post_data"],
                    },
                    "response": {
                        "status": req_info["status"],
                        "headers": req_info["headers_resp"],
                        "body": body
                    }
                }
                print(f"Captured: [{req_info['method']}] {req_info['status']} -> {req_info['url']}")
                queue_record(record)
                continue

            # 2. Target Attached: Capture top pages, IFrames (Pega Gadgets), and WebWorkers
            if method == "Target.attachedToTarget":
                target_info = params.get("targetInfo", {})
                target_session = params.get("sessionId")
                target_type = target_info.get("type")
                target_url = target_info.get("url", "")

                if "lenovo.com" in target_url or "vantage" in target_url:
                    continue

                if target_session not in sessions:
                    sessions.add(target_session)
                    print(f"[+] Hooked target [{target_type}]: {target_url or 'subframe'}")

                    # Prevent "Paused in debugger"
                    msg_counter += 1
                    await ws.send(json.dumps({
                        "id": msg_counter,
                        "sessionId": target_session,
                        "method": "Runtime.runIfWaitingForDebugger",
                        "params": {}
                    }))

                    # Enable network tracking on this specific target session
                    msg_counter += 1
                    await ws.send(json.dumps({
                        "id": msg_counter,
                        "sessionId": target_session,
                        "method": "Network.enable",
                        "params": {"maxTotalBufferSize": 20971520}
                    }))

            # 3. Clean up detached sessions
            elif method == "Target.detachedFromTarget":
                detached_session = params.get("sessionId")
                sessions.discard(detached_session)

            # 4. Outgoing HTTP Request
            elif method == "Network.requestWillBeSent":
                req_id = params.get("requestId")
                req = params.get("request", {})
                req_url = req.get("url", "")

                if not req_url.startswith("data:") and "lenovo.com" not in req_url:
                    in_flight[req_id] = {
                        "url": req_url,
                        "method": req.get("method"),
                        "headers": req.get("headers", {}),
                        "post_data": clean_post_data(req.get("postData", None)),
                        "start_time": time.time(),
                        "session_id": session_id,
                        "type": params.get("type", "")
                    }

            # 5. Response Headers & Status Received
            elif method == "Network.responseReceived":
                req_id = params.get("requestId")
                resp = params.get("response", {})
                req_type = params.get("type", "")

                if req_id in in_flight:
                    in_flight[req_id]["status"] = resp.get("status")
                    in_flight[req_id]["headers_resp"] = resp.get("headers", {})
                    if req_type:
                        in_flight[req_id]["type"] = req_type

            # 6. WebSocket frames (Pega push updates)
            elif method == "Network.webSocketFrameSent":
                ws_req_id = params.get("requestId")
                payload_data = params.get("response", {}).get("payloadData", "")
                record = {
                    "url": f"wss://push-channel/{ws_req_id}",
                    "duration_ms": 1.0,
                    "is_media": False,
                    "request": {
                        "method": "WS",
                        "headers": {"Connection": "Upgrade", "Upgrade": "websocket"},
                        "post_data": payload_data
                    },
                    "response": {
                        "status": 101,
                        "headers": {},
                        "body": "(Sent WebSocket Message)"
                    }
                }
                queue_record(record)

            elif method == "Network.webSocketFrameReceived":
                ws_req_id = params.get("requestId")
                payload_data = params.get("response", {}).get("payloadData", "")
                record = {
                    "url": f"wss://push-channel/{ws_req_id}",
                    "duration_ms": 1.0,
                    "is_media": False,
                    "request": {
                        "method": "WS",
                        "headers": {},
                        "post_data": None
                    },
                    "response": {
                        "status": 101,
                        "headers": {},
                        "body": payload_data
                    }
                }
                queue_record(record)

            # 7. Request Failed / Cancelled
            elif method == "Network.loadingFailed":
                req_id = params.get("requestId")
                in_flight.pop(req_id, None)

            # 8. Request Finished -> Dispatch
            elif method == "Network.loadingFinished":
                req_id = params.get("requestId")
                if req_id in in_flight and "status" in in_flight[req_id]:
                    item = in_flight.pop(req_id)
                    media_check = is_strictly_binary(item["url"], item.get("headers_resp", {}))

                    if media_check and not CAPTURE_MEDIA_FILES:
                        continue

                    duration_ms = round((time.time() - item["start_time"]) * 1000, 1)

                    if media_check and CAPTURE_MEDIA_FILES:
                        encoded_length = params.get("encodedDataLength", 0)
                        size_kb = round(encoded_length / 1024, 2)
                        record = {
                            "url": item["url"],
                            "duration_ms": duration_ms,
                            "is_media": True,
                            "request": {
                                "method": item["method"],
                                "headers": item["headers"],
                                "post_data": "(Binary Asset)",
                            },
                            "response": {
                                "status": item["status"],
                                "headers": item.get("headers_resp", {}),
                                "body": f"[Binary Asset - Transferred: {size_kb} KB]"
                            }
                        }
                        print(f"Captured: [MEDIA] {item['status']} -> {item['url']}")
                        queue_record(record)
                        continue

                    # Preflight OPTIONS or 204 No Content
                    if item["method"] == "OPTIONS" or item["status"] == 204:
                        record = {
                            "url": item["url"],
                            "duration_ms": duration_ms,
                            "is_media": False,
                            "request": {
                                "method": item["method"],
                                "headers": item["headers"],
                                "post_data": item["post_data"],
                            },
                            "response": {
                                "status": item["status"],
                                "headers": item["headers_resp"],
                                "body": "(No Content - Preflight/204)"
                            }
                        }
                        print(f"Captured: [{item['method']}] {item['status']} -> {item['url']}")
                        queue_record(record)
                        continue

                    # Fetch body via the session that finished the call
                    msg_counter += 1
                    target_sess = session_id or item.get("session_id")
                    request_body_map[msg_counter] = {
                        "url": item["url"],
                        "method": item["method"],
                        "headers": item["headers"],
                        "post_data": item["post_data"],
                        "status": item["status"],
                        "headers_resp": item["headers_resp"],
                        "duration_ms": duration_ms,
                        "created": time.time()
                    }

                    cmd = {
                        "id": msg_counter,
                        "method": "Network.getResponseBody",
                        "params": {"requestId": req_id}
                    }
                    if target_sess:
                        cmd["sessionId"] = target_sess

                    try:
                        await ws.send(json.dumps(cmd))
                    except Exception:
                        request_body_map.pop(msg_counter, None)

def main():
    chrome = find_chrome_path()
    if not chrome:
        print("[-] Google Chrome was not found in standard paths.")
        return

    launch_chrome_process(chrome, CDP_PORT)
    time.sleep(1.2)
    webbrowser.open(f"http://127.0.0.1:{DASHBOARD_PORT}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run_cdp_tracer(CDP_PORT))

if __name__ == "__main__":
    t = threading.Thread(target=start_uvicorn, args=(DASHBOARD_PORT,), daemon=True)
    t.start()
    main()
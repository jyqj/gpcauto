import { $, $$, esc, showToast, showConfirm, listenSSE } from './utils.js';

import * as api from './api.js';
import { loadDashboard } from './dashboard.js';
import { PROXY_STATUS } from './constants.js';

let allProxies = [];
let proxySelected = new Set();
let proxyHealth = {};
let proxyPage = 1;
let proxyPageSize = 50;
let proxyTotal = 0;
let proxySearchTerm = '';
let proxyFilter = 'all';

export function initProxies() {
  const drawer = $('#proxyImportDrawer');
  $('#btnOpenProxyImport').addEventListener('click', () => drawer.classList.add('open'));
  $('#btnCloseProxyImport').addEventListener('click', () => drawer.classList.remove('open'));
  drawer.addEventListener('click', (e) => { if (e.target === drawer) drawer.classList.remove('open'); });

  $$('#proxyInputTabs .sub-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('#proxyInputTabs .sub-tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      const m = btn.dataset.mode;
      $('#proxyModeBatch').style.display = m === 'batch' ? 'block' : 'none';
      $('#proxyModeManual').style.display = m === 'manual' ? 'block' : 'none';
    });
  });

  $('#btnAddProxy').addEventListener('click', addProxy);
  $('#btnImportProxies').addEventListener('click', batchImport);
  $('#btnRefreshProxies').addEventListener('click', loadProxies);
  $('#btnCheckProxies').addEventListener('click', () => checkProxies(null));
  $('#btnResetProxies').addEventListener('click', resetAllProxies);

  $('#proxyCheckAll').addEventListener('change', (e) => {
    if (e.target.checked) selectAll(); else selectNone();
  });

  // 搜索
  let proxySearchTimer;
  $('#proxySearch')?.addEventListener('input', (e) => {
    clearTimeout(proxySearchTimer);
    proxySearchTimer = setTimeout(() => {
      proxySearchTerm = e.target.value.trim();
      proxyPage = 1;
      loadProxies();
    }, 200);
  });

  // 筛选 tab
  $$('#proxyFilterTabs .filter-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('#proxyFilterTabs .filter-tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      proxyFilter = btn.dataset.filter;
      proxyPage = 1;
      loadProxies();
    });
  });

  window.proxySelectAll = selectAll;
  window.proxySelectNone = selectNone;
  window.proxyBatchDelete = batchDelete;
  window.proxyBatchCheck = () => { const ids = [...proxySelected]; if (ids.length) checkProxies(ids); };
  window.proxyBatchEnable = () => batchSetStatus(PROXY_STATUS.AVAILABLE);
  window.proxyBatchDisable = () => batchSetStatus(PROXY_STATUS.DISABLED);
  window.deleteProxy = deleteProxy;
  window.enableProxy = enableProxy;
}

async function resetAllProxies() {
  const disabled = allProxies.filter(p => p.status === PROXY_STATUS.DISABLED);
  if (!disabled.length) { showToast('没有被封禁的代理'); return; }
  if (!await showConfirm(`将 ${disabled.length} 个封禁代理全部解禁？`)) return;
  try {
    await api.proxies.resetAll();
    showToast(`已解禁 ${disabled.length} 个代理`, 'success');
    loadProxies();
  } catch (e) { showToast('解禁失败: ' + e.message, 'error'); }
}

export async function loadProxies() {
  try {
    const params = { page: proxyPage, page_size: proxyPageSize };
    if (proxySearchTerm) params.search = proxySearchTerm;
    if (proxyFilter !== 'all') params.status = proxyFilter;
    const data = await api.proxies.list(params);
    allProxies = data.proxies;
    proxyTotal = data.total ?? data.proxies.length;
    const validIds = new Set(allProxies.map(p => p.id));
    proxySelected.forEach(id => { if (!validIds.has(id)) proxySelected.delete(id); });
    renderTable(allProxies);
    renderProxyPagination();
    updateBatchBar();
  } catch (e) { showToast('加载代理列表失败', 'error'); }
}

function renderTable(proxies) {
  const body = $('#proxiesBody');
  const empty = $('#proxiesEmpty');
  if (!proxies.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';

  body.innerHTML = proxies.map(p => {
    const typeBadge = `<span class="badge proxy-${p.type}">${p.type}</span>`;
    const auth = p.username ? `${p.username}:${p.password || ''}` : '-';
    const sel = proxySelected.has(p.id) ? 'checked' : '';
    const time = (p.created_at || '').replace('T', ' ').replace('Z', '').substring(0, 16);
    const isDisabled = (p.status || PROXY_STATUS.AVAILABLE) === PROXY_STATUS.DISABLED;
    const dimRow = isDisabled ? ' row-dim' : '';

    const h = proxyHealth[p.id];
    let statusBadge;
    if (h) {
      statusBadge = h.alive
        ? `<span class="badge active" title="${h.latency_ms}ms">${h.latency_ms}ms</span>`
        : `<span class="badge dead" title="${esc(h.error || 'failed')}">失败</span>`;
    } else {
      statusBadge = isDisabled
        ? '<span class="badge dead">已封禁</span>'
        : '<span class="badge active">可用</span>';
    }

    const actions = isDisabled
      ? `<button class="btn-sm accent" onclick="enableProxy(${p.id})">启用</button> <button class="btn-sm danger" onclick="deleteProxy(${p.id})">删</button>`
      : `<button class="btn-sm danger" onclick="deleteProxy(${p.id})">删</button>`;

    return `<tr class="${sel ? 'row-selected' : ''}${dimRow}" data-id="${p.id}">
      <td><input type="checkbox" class="row-check proxy-check" data-id="${p.id}" ${sel}></td>
      <td class="cell-id">${p.id}</td>
      <td>${typeBadge}</td>
      <td>${esc(p.host)}</td>
      <td>${esc(p.port)}</td>
      <td>${esc(auth)}</td>
      <td>${esc(p.label || '-')}</td>
      <td>${statusBadge}</td>
      <td>${esc(time)}</td>
      <td>${actions}</td>
    </tr>`;
  }).join('');

  body.querySelectorAll('.proxy-check').forEach(cb => {
    cb.addEventListener('change', () => {
      const id = +cb.dataset.id;
      if (cb.checked) proxySelected.add(id); else proxySelected.delete(id);
      cb.closest('tr').classList.toggle('row-selected', cb.checked);
      updateBatchBar();
    });
  });
}

function renderProxyPagination() {
  let el = document.getElementById('proxyPagination');
  if (!el) {
    el = document.createElement('div');
    el.id = 'proxyPagination';
    el.className = 'pagination-bar';
    const tableWrap = document.querySelector('#page-proxies .table-wrap');
    if (tableWrap) tableWrap.after(el);
  }

  const totalPages = Math.max(1, Math.ceil(proxyTotal / proxyPageSize));

  let html = `<span class="page-info">共 ${proxyTotal} 条，第 ${proxyPage}/${totalPages} 页</span>`;
  html += `<div class="page-btns">`;
  html += `<button class="btn btn-sm" ${proxyPage <= 1 ? 'disabled' : ''} onclick="window._proxyPage(1)">首页</button>`;
  html += `<button class="btn btn-sm" ${proxyPage <= 1 ? 'disabled' : ''} onclick="window._proxyPage(${proxyPage - 1})">上一页</button>`;
  html += `<button class="btn btn-sm" ${proxyPage >= totalPages ? 'disabled' : ''} onclick="window._proxyPage(${proxyPage + 1})">下一页</button>`;
  html += `<button class="btn btn-sm" ${proxyPage >= totalPages ? 'disabled' : ''} onclick="window._proxyPage(${totalPages})">末页</button>`;
  html += `</div>`;
  el.innerHTML = html;
}

function proxyGoToPage(p) {
  proxyPage = p;
  loadProxies();
}
window._proxyPage = proxyGoToPage;

function updateBatchBar() {
  const bar = $('#proxyBatchBar');
  const n = proxySelected.size;
  bar.style.display = n ? 'flex' : 'none';
  $('#proxySelCount').textContent = `已选 ${n} 项`;
  $('#proxyCheckAll').checked = n > 0 && allProxies.every(p => proxySelected.has(p.id));
}

function selectAll() { allProxies.forEach(p => proxySelected.add(p.id)); renderTable(allProxies); updateBatchBar(); }
function selectNone() { proxySelected.clear(); renderTable(allProxies); updateBatchBar(); }

async function batchSetStatus(status) {
  const ids = [...proxySelected];
  if (!ids.length) return;
  const label = status === PROXY_STATUS.AVAILABLE ? '启用' : '封禁';
  await api.proxies.batchStatus(ids, status);
  showToast(`已${label} ${ids.length} 个代理`, 'success');
  loadProxies();
  loadDashboard();
}

async function enableProxy(id) {
  await api.proxies.batchStatus([id], PROXY_STATUS.AVAILABLE);
  showToast(`代理 #${id} 已启用`, 'success');
  proxyHealth[id] = undefined;
  loadProxies();
}

async function batchDelete() {
  const ids = [...proxySelected];
  if (!ids.length) return;
  if (!await showConfirm(`确认删除 ${ids.length} 个代理？`)) return;
  await api.proxies.batchDelete(ids);
  showToast(`已删除 ${ids.length} 个代理`, 'success');
  proxySelected.clear();
  loadProxies();
  loadDashboard();
}

async function deleteProxy(id) {
  await api.proxies.remove(id);
  showToast('代理已删除', 'success');
  loadProxies();
}

async function checkProxies(ids) {
  const btn = $('#btnCheckProxies');
  btn.disabled = true;
  btn.textContent = '检测中...';
  try {
    const body = ids ? { ids } : {};
    const { task_id } = await api.proxies.check(body);
    const pw = $('#proxyCheckWrap');
    const pb = $('#proxyCheckBar');
    const pt = $('#proxyCheckText');
    pw.style.display = 'block';

    listenSSE(`/api/proxies/check/${task_id}/stream`, {
      onProxyChecked(d) {
        pb.style.width = Math.round((d.current / d.total) * 100) + '%';
        pt.textContent = `${d.current}/${d.total}`;
        proxyHealth[d.proxy_id] = { alive: d.alive, latency_ms: d.latency_ms, error: d.error };
        renderTable(allProxies);
      },
      onDone(d) {
        pw.style.display = 'none';
        btn.disabled = false;
        btn.textContent = '测活';
        const r = d.result || {};
        showToast(`检测完成: ${r.alive || 0} 存活, ${r.dead || 0} 已封禁`, r.dead ? 'warning' : 'success');
        loadProxies();
        loadDashboard();
      },
      onError(d) {
        pw.style.display = 'none';
        btn.disabled = false;
        btn.textContent = '测活';
        showToast(`检测失败: ${d.message}`, 'error');
      },
    });
  } catch (e) {
    btn.disabled = false;
    btn.textContent = '测活';
    showToast(`启动检测失败: ${e.message}`, 'error');
  }
}

async function addProxy() {
  const type = $('#newProxyType').value;
  const host = $('#newProxyHost').value.trim();
  const port = $('#newProxyPort').value.trim();
  const username = $('#newProxyUser').value.trim();
  const password = $('#newProxyPass').value.trim();
  if (!host || !port) { showToast('请填写主机和端口', 'warning'); return; }
  await api.proxies.add({ type, host, port, username, password, label: '' });
  $('#newProxyHost').value = ''; $('#newProxyPort').value = ''; $('#newProxyUser').value = ''; $('#newProxyPass').value = '';
  showToast('代理已添加', 'success');
  $('#proxyImportDrawer').classList.remove('open');
  loadProxies();
  loadDashboard();
}

async function batchImport() {
  const raw = $('#proxyBatchInput').value.trim();
  if (!raw) { showToast('请输入代理数据', 'warning'); return; }
  const defaultType = $('#proxyDefaultType').value;
  const btn = $('#btnImportProxies');
  btn.disabled = true;
  btn.textContent = '导入中...';
  try {
    const result = await api.proxies.batchImport(raw, defaultType);
    showToast(`导入完成: ${result.imported} 成功, ${result.failed} 失败`, result.failed ? 'warning' : 'success');
    $('#proxyBatchInput').value = '';
    $('#proxyImportDrawer').classList.remove('open');
    loadProxies();
    loadDashboard();
  } catch (e) {
    showToast(`导入失败: ${e.message}`, 'error');
  }
  btn.disabled = false;
  btn.textContent = '批量导入';
}

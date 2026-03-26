import { $, $$, esc, escAttr, showToast, showConfirm, listenSSE, fmtTime, API } from './utils.js';
import * as api from './api.js';
import { loadDashboard } from './dashboard.js';
import { ACCOUNT_STATUS, SALE_STATUS } from './constants.js';

let allAccounts = [];
let acctSelected = new Set();
let acctFilter = 'all';
let acctSaleFilter = 'all';
let acctSearchTerm = '';
let lastClickedIdx = null;
let acctSortField = null;
let acctSortDir = 'asc';
let acctPage = 1;
let acctPageSize = 50;
let acctTotal = 0;

export function initAccounts() {
  $$('#acctFilterTabs .filter-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('#acctFilterTabs .filter-tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      acctFilter = btn.dataset.filter;
      acctPage = 1;
      loadAccounts();
    });
  });

  $$('#acctSaleTabs .sale-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('#acctSaleTabs .sale-tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      acctSaleFilter = btn.dataset.sale;
      acctPage = 1;
      loadAccounts();
    });
  });

  let timer;
  $('#acctSearch').addEventListener('input', (e) => {
    clearTimeout(timer);
    timer = setTimeout(() => { acctSearchTerm = e.target.value.trim(); acctPage = 1; loadAccounts(); }, 200);
  });

  $('#acctCheckAll').addEventListener('change', (e) => {
    if (e.target.checked) selectAll(); else selectNone();
  });

  $$('#page-accounts thead .sortable').forEach(th => {
    th.addEventListener('click', () => {
      const field = th.dataset.sort;
      if (acctSortField === field) {
        acctSortDir = acctSortDir === 'asc' ? 'desc' : 'asc';
      } else {
        acctSortField = field;
        acctSortDir = 'asc';
      }
      $$('#page-accounts thead .sortable').forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
      th.classList.add(acctSortDir === 'asc' ? 'sort-asc' : 'sort-desc');
      acctPage = 1;
      loadAccounts();
    });
  });

  $('#btnCheckAll').addEventListener('click', () => startKeyCheck(null));
  $('#btnRefreshAccounts').addEventListener('click', loadAccounts);
  $('#btnExportCSV').addEventListener('click', () => window.open(`${API}/api/accounts/export?format=csv`, '_blank'));
  $('#btnExportKeys').addEventListener('click', () => window.open(`${API}/api/accounts/export-keys`, '_blank'));
  $('#btnCleanupDead').addEventListener('click', cleanupDead);
  $('#btnSellExport')?.addEventListener('click', sellExport);

  window.acctBatchDelete = batchDelete;
  window.acctBatchStatus = batchStatus;
  window.acctBatchCheck = () => { const ids = [...acctSelected]; if (ids.length) startKeyCheck(ids); };
  window.acctBatchExport = batchExport;
  window.acctSelectAll = selectAll;
  window.acctSelectNone = selectNone;
  window.acctSelectInvert = selectInvert;
  window.acctSelectByCondition = selectByCondition;
}

function showLoading(bodyId, cols) {
  const body = $(bodyId);
  const rows = Array.from({ length: 5 }, () =>
    `<tr class="table-loading">${Array.from({ length: cols }, () =>
      `<td><div class="skeleton" style="width:${40 + Math.random() * 60}%"></div></td>`
    ).join('')}</tr>`
  ).join('');
  body.innerHTML = rows;
}

export async function loadAccounts() {
  showLoading('#accountsBody', 12);
  $('#accountsEmpty').style.display = 'none';
  try {
    const params = {
      page: acctPage,
      page_size: acctPageSize,
    };
    if (acctSearchTerm) params.search = acctSearchTerm;
    if (acctFilter !== 'all') params.status = acctFilter;
    if (acctSaleFilter !== 'all') params.sale_status = acctSaleFilter;
    if (acctSortField) {
      params.sort = acctSortField;
      params.order = acctSortDir;
    }
    const data = await api.accounts.list(params);
    allAccounts = data.accounts;
    acctTotal = data.total ?? data.accounts.length;
    const validIds = new Set(allAccounts.map(a => a.id));
    acctSelected.forEach(id => { if (!validIds.has(id)) acctSelected.delete(id); });
    render();
    renderPagination();
  } catch (e) { showToast('加载账号列表失败', 'error'); }
}

function getFiltered() {
  return allAccounts;
}

function renderPagination() {
  let el = document.getElementById('acctPagination');
  if (!el) {
    el = document.createElement('div');
    el.id = 'acctPagination';
    el.className = 'pagination-bar';
    const tableWrap = document.querySelector('#page-accounts .table-wrap');
    if (tableWrap) tableWrap.after(el);
  }

  const totalPages = Math.max(1, Math.ceil(acctTotal / acctPageSize));

  let html = `<span class="page-info">共 ${acctTotal} 条，第 ${acctPage}/${totalPages} 页</span>`;
  html += `<div class="page-btns">`;
  html += `<button class="btn btn-sm" ${acctPage <= 1 ? 'disabled' : ''} onclick="window._acctPage(1)">首页</button>`;
  html += `<button class="btn btn-sm" ${acctPage <= 1 ? 'disabled' : ''} onclick="window._acctPage(${acctPage - 1})">上一页</button>`;
  html += `<button class="btn btn-sm" ${acctPage >= totalPages ? 'disabled' : ''} onclick="window._acctPage(${acctPage + 1})">下一页</button>`;
  html += `<button class="btn btn-sm" ${acctPage >= totalPages ? 'disabled' : ''} onclick="window._acctPage(${totalPages})">末页</button>`;
  html += `</div>`;
  el.innerHTML = html;
}

function goToPage(p) {
  acctPage = p;
  loadAccounts();
}
window._acctPage = goToPage;

function render() {
  const filtered = getFiltered();
  renderTable(filtered);
  updateBatchBar();
  updateSaleCounts();
}

function updateSaleCounts() {
  const counts = { [SALE_STATUS.UNSOLD]: 0, [SALE_STATUS.SOLD]: 0, [SALE_STATUS.RECYCLED]: 0 };
  allAccounts.forEach(a => {
    const s = a.sale_status || SALE_STATUS.UNSOLD;
    if (counts[s] !== undefined) counts[s]++;
  });
  const el = (id) => document.querySelector(id);
  if (el('#saleCountUnsold')) el('#saleCountUnsold').textContent = counts[SALE_STATUS.UNSOLD];
  if (el('#saleCountSold')) el('#saleCountSold').textContent = counts[SALE_STATUS.SOLD];
  if (el('#saleCountRecycled')) el('#saleCountRecycled').textContent = counts[SALE_STATUS.RECYCLED];
}

function renderTable(accounts) {
  const body = $('#accountsBody');
  const empty = $('#accountsEmpty');
  if (!accounts.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';

  body.innerHTML = accounts.map(a => {
    const keyShort = a.api_key || '-';
    const time = fmtTime(a.registered_at);
    const checked = fmtTime(a.checked_at) || '-';
    const status = a.status || ACCOUNT_STATUS.UNKNOWN;
    const statusBadge = `<span class="badge ${status}">${status}</span>`;
    const saleStatus = a.sale_status || SALE_STATUS.UNSOLD;
    const saleBadge = `<span class="badge sale-${saleStatus}">${saleStatus}</span>`;
    const cardInfo = a.card_id
      ? `<span class="badge yes" title="Card #${a.card_id}">#${a.card_id}${a.card_number ? ' ' + a.card_number : ''}</span>`
      : '<span class="badge no">未绑</span>';
    const credit = a.credit_loaded || 0;
    const creditDisplay = credit > 0 ? `$${credit}` : '-';
    const sel = acctSelected.has(a.id) ? 'checked' : '';
    return `<tr class="${sel ? 'row-selected' : ''}" data-id="${a.id}">
      <td><input type="checkbox" class="row-check acct-check" data-id="${a.id}" ${sel}></td>
      <td class="cell-id">#${a.id}</td>
      <td title="${esc(a.email)}">${esc(a.email)}</td>
      <td title="${esc(a.password || '')}">${esc(a.password || '-')}</td>
      <td title="${esc(a.api_key || '')}">${esc(keyShort)}</td>
      <td>${statusBadge}</td>
      <td>${saleBadge}</td>
      <td>${cardInfo}</td>
      <td class="cell-credit" data-id="${a.id}" title="点击编辑">${creditDisplay}</td>
      <td>${esc(time)}</td>
      <td>${esc(checked)}</td>
      <td>
        <button class="btn-sm accent" onclick="copyText('${escAttr(a.api_key || '')}')">Key</button>
        <button class="btn-sm" onclick="copyText('${escAttr(a.password || '')}')">密码</button>
      </td>
    </tr>`;
  }).join('');

  const checkboxes = [...body.querySelectorAll('.acct-check')];
  checkboxes.forEach((cb, idx) => {
    cb.addEventListener('click', (e) => {
      const id = +cb.dataset.id;
      if (e.shiftKey && lastClickedIdx !== null) {
        const from = Math.min(lastClickedIdx, idx);
        const to = Math.max(lastClickedIdx, idx);
        for (let i = from; i <= to; i++) {
          const cbi = checkboxes[i];
          const iid = +cbi.dataset.id;
          acctSelected.add(iid);
          cbi.checked = true;
          cbi.closest('tr').classList.add('row-selected');
        }
      } else {
        if (cb.checked) acctSelected.add(id); else acctSelected.delete(id);
        cb.closest('tr').classList.toggle('row-selected', cb.checked);
      }
      lastClickedIdx = idx;
      updateBatchBar();
    });
  });

  body.querySelectorAll('.cell-credit').forEach(cell => {
    cell.addEventListener('click', () => {
      const aid = +cell.dataset.id;
      const acct = allAccounts.find(a => a.id === aid);
      if (!acct) return;
      const cur = acct.credit_loaded || 0;
      const inp = document.createElement('input');
      inp.type = 'number'; inp.min = '0'; inp.step = '1';
      inp.value = cur; inp.className = 'inline-edit';
      inp.style.width = '60px';
      cell.textContent = '';
      cell.appendChild(inp);
      inp.focus(); inp.select();
      const save = async () => {
        const val = parseFloat(inp.value) || 0;
        try {
          await api.accounts.update(aid, { credit_loaded: val });
          acct.credit_loaded = val;
        } catch (e) { showToast('保存失败', 'error'); }
        cell.textContent = val > 0 ? `$${val}` : '-';
      };
      inp.addEventListener('blur', save);
      inp.addEventListener('keydown', e => { if (e.key === 'Enter') inp.blur(); if (e.key === 'Escape') { cell.textContent = cur > 0 ? `$${cur}` : '-'; } });
    });
  });
}

function updateBatchBar() {
  const bar = $('#acctBatchBar');
  const n = acctSelected.size;
  bar.style.display = n ? 'flex' : 'none';
  const filtered = getFiltered();
  $('#acctSelCount').textContent = `已选 ${n} / ${acctTotal} 项`;
  $('#acctCheckAll').checked = filtered.length > 0 && filtered.every(a => acctSelected.has(a.id));

  const hasUnsold = [...acctSelected].some(id => {
    const a = allAccounts.find(x => x.id === id);
    return a && (a.sale_status || SALE_STATUS.UNSOLD) === SALE_STATUS.UNSOLD;
  });
  const sellBtn = $('#btnSellExport');
  if (sellBtn) sellBtn.style.display = hasUnsold ? 'inline-flex' : 'none';
}

function selectAll() { getFiltered().forEach(a => acctSelected.add(a.id)); render(); }
function selectNone() { acctSelected.clear(); render(); }
function selectInvert() { getFiltered().forEach(a => { if (acctSelected.has(a.id)) acctSelected.delete(a.id); else acctSelected.add(a.id); }); render(); }
function selectByCondition(cond) {
  if (!cond) return;
  acctSelected.clear();
  allAccounts.forEach(a => {
    if (cond === 'unbound' && !a.card_id) acctSelected.add(a.id);
    else if (cond === SALE_STATUS.UNSOLD && (a.sale_status || SALE_STATUS.UNSOLD) === SALE_STATUS.UNSOLD) acctSelected.add(a.id);
    else if (cond !== 'unbound' && cond !== SALE_STATUS.UNSOLD && a.status === cond) acctSelected.add(a.id);
  });
  render();
}

async function batchDelete() {
  const ids = [...acctSelected];
  if (!ids.length) return;
  if (!await showConfirm(`确认删除 ${ids.length} 个账号？此操作不可撤销。`)) return;
  await api.accounts.batchDelete(ids);
  showToast(`已删除 ${ids.length} 个账号`, 'success');
  acctSelected.clear();
  loadAccounts();
  loadDashboard();
}

async function batchStatus(status) {
  const ids = [...acctSelected];
  if (!ids.length) return;
  await api.accounts.batchUpdate(ids, { status });
  showToast(`已将 ${ids.length} 个账号标记为 ${status}`, 'success');
  loadAccounts();
}

async function sellExport() {
  const ids = [...acctSelected].filter(id => {
    const a = allAccounts.find(x => x.id === id);
    return a && (a.sale_status || SALE_STATUS.UNSOLD) === SALE_STATUS.UNSOLD;
  });
  if (!ids.length) { showToast('没有可售卖的账号', 'warning'); return; }
  const totalCredit = ids.reduce((sum, id) => {
    const a = allAccounts.find(x => x.id === id);
    return sum + (a?.credit_loaded || 0);
  }, 0);
  if (!await showConfirm(`导出 ${ids.length} 个账号并标记为已售卖？\n总额度: $${totalCredit}\n格式: 账号----密码----key----$金额`)) return;

  try {
    const resp = await api.accounts.sell(ids, 'txt');
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      showToast(err.detail || err.error || '操作失败', 'error');
      return;
    }
    const token = resp.headers.get('X-Sale-Token');
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = `accounts_sold_${ids.length}.txt`;
    anchor.click();
    URL.revokeObjectURL(url);

    // blob 已完整获取，下载已触发；现在让用户确认是否标记为已售卖
    if (token) {
      const confirmed = await showConfirm(`文件已下载，确认将 ${ids.length} 个账号标记为已售卖？\n取消则账号状态将在 5 分钟后自动回滚。`);
      if (confirmed) {
        try {
          await api.accounts.sellConfirm(token);
          showToast(`已将 ${ids.length} 个账号标记为已售卖`, 'success');
        } catch (ce) {
          showToast(`确认售卖失败: ${ce.message}，账号将在 5 分钟后自动回滚`, 'warning');
        }
      } else {
        showToast('已取消售卖确认，账号状态将在 5 分钟后自动回滚', 'info');
      }
    }
    acctSelected.clear();
    loadAccounts();
    loadDashboard();
  } catch (e) {
    showToast(`导出失败: ${e.message}`, 'error');
  }
}

function batchExport() {
  if (!acctSelected.size) return;
  const selected = allAccounts.filter(a => acctSelected.has(a.id));
  const lines = ['email,password,api_key,status,sale_status'];
  selected.forEach(a => {
    lines.push([a.email, a.password, a.api_key, a.status, a.sale_status].map(v => `"${(v || '').replace(/"/g, '""')}"`).join(','));
  });
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = `accounts_selected_${selected.length}.csv`; a.click();
  URL.revokeObjectURL(url);
  showToast(`已导出 ${selected.length} 个账号`, 'success');
}

const checkPW = () => $('#checkProgressWrap');
const checkPB = () => $('#checkProgressBar');
const checkPT = () => $('#checkProgressText');


async function startKeyCheck(accountIds) {
  $('#btnCheckAll').disabled = true;
  try {
    const body = accountIds ? { account_ids: accountIds } : {};
    const { task_id } = await api.keys.check(body);
    listenSSE(`/api/keys/check/${task_id}/stream`, {
      onCheckProgress(d) {
        const pw = checkPW(), pb = checkPB(), pt = checkPT();
        pw.style.display = 'block';
        pb.style.width = Math.round((d.current / d.total) * 100) + '%';
        pt.textContent = `${d.current}/${d.total}`;
      },
      onDone() {
        checkPW().style.display = 'none';
        $('#btnCheckAll').disabled = false;
        loadAccounts();
        loadDashboard();
        showToast('测活完成', 'success');
      },
      onError() {
        checkPW().style.display = 'none';
        $('#btnCheckAll').disabled = false;
        showToast('测活失败', 'error');
      },
    });
  } catch (e) { $('#btnCheckAll').disabled = false; showToast('启动测活失败', 'error'); }
}

async function cleanupDead() {
  if (!await showConfirm('确认清理所有 dead 账号的 AdsPower 环境？此操作不可撤销。')) return;
  const btn = $('#btnCleanupDead');
  btn.disabled = true;
  btn.textContent = '清理中...';
  try {
    const res = await api.accounts.cleanupProfiles();
    showToast(`清理完成: ${res.cleaned}/${res.total_dead} 个环境已删除`, 'success');
  } catch (e) {
    showToast(`清理失败: ${e.message}`, 'error');
  }
  btn.disabled = false;
  btn.textContent = '清理失效';
}

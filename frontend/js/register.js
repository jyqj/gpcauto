import { $, $$, esc, escAttr, showToast, listenSSE } from './utils.js';
import * as api from './api.js';
import { loadDashboard } from './dashboard.js';
import { loadAccounts } from './accounts.js';
import { CARD_FAIL_TAG, CARD_STATUS, PROXY_STATUS } from './constants.js';

let running = 0;
let currentTaskId = null;
let isPaused = false;
let pausePending = false;
let pauseStats = null;

let csCards = [];
let csSelected = new Set();
let csLastClickedIdx = null;
let csSearchTerm = '';

export function initRegister() {
  const proxyType = $('#proxyType');
  const proxyStr = $('#proxyStr');
  const btnStart = $('#btnStart');
  const btnPause = $('#btnPause');
  const btnResume = $('#btnResume');
  const btnStop = $('#btnStop');

  btnPause.addEventListener('click', async () => {
    if (!currentTaskId) return;
    btnPause.disabled = true;
    try {
      await api.register.pause(currentTaskId);
      pausePending = true;
      appendLog('已发送暂停请求，等待所有并发 flow 收敛...', 'step');
    } catch (e) { appendLog(`暂停失败: ${e.message}`, 'error'); }
    if (!pausePending) btnPause.disabled = false;
  });

  btnResume.addEventListener('click', async () => {
    if (!currentTaskId) return;
    btnResume.disabled = true;
    try {
      await api.register.resume(currentTaskId);
      appendLog('已发送恢复请求...', 'step');
    } catch (e) { appendLog(`恢复失败: ${e.message}`, 'error'); }
    btnResume.disabled = false;
  });

  btnStop.addEventListener('click', async () => {
    if (!currentTaskId) return;
    btnStop.disabled = true;
    btnStop.textContent = '停止中...';
    try {
      await api.register.cancel(currentTaskId);
      appendLog('正在停止任务...', 'error');
    } catch (e) { appendLog(`停止请求失败: ${e.message}`, 'error'); }
    btnStop.disabled = false;
    btnStop.innerHTML = '<span class="btn-icon">■</span> 停止';
  });

  $$('input[name="bindPath"]').forEach(r => {
    r.addEventListener('change', () => {
      const isBilling = $('input[name="bindPath"]:checked').value === 'billing';
      $('#creditOnboarding').style.display = isBilling ? 'none' : 'block';
      $('#creditBilling').style.display = isBilling ? 'block' : 'none';
      updateTierHint();
    });
  });

  $$('input[name="rechargeStrategy"]').forEach(r => {
    r.addEventListener('change', () => {
      const strategy = $('input[name="rechargeStrategy"]:checked').value;
      $('#rechargeAutoConfig').style.display = strategy === 'auto' ? 'block' : 'none';
      $('#rechargeManualHint').style.display = strategy === 'manual' ? 'block' : 'none';
      updateTierHint();
    });
  });

  $('#rechargeUpper').addEventListener('change', () => { syncLowerOptions(); updateTierHint(); });
  $('#rechargeLower').addEventListener('change', updateTierHint);
  $$('input[name="creditAmount"]').forEach(r => r.addEventListener('change', updateTierHint));
  $('#creditAmountInput')?.addEventListener('input', updateTierHint);

  proxyType.addEventListener('change', () => {
    const isDirect = proxyType.value === 'direct';
    const isPool = proxyType.value === 'pool';
    proxyStr.style.display = isDirect || isPool ? 'none' : 'block';
    $('#proxyPoolSelect').style.display = isPool ? 'block' : 'none';
    if (isPool) loadProxyOptions();
  });

  $('#proxyPoolSelect').addEventListener('change', () => {
    $('#proxyPoolId').value = $('#proxyPoolSelect').value;
  });

  const csDrawer = $('#cardSelectDrawer');
  $('#btnOpenCardSelect').addEventListener('click', () => { loadCardPool(); csDrawer.classList.add('open'); });
  $('#btnCloseCardSelect').addEventListener('click', () => csDrawer.classList.remove('open'));
  csDrawer.addEventListener('click', (e) => { if (e.target === csDrawer) csDrawer.classList.remove('open'); });
  $('#csConfirm').addEventListener('click', () => { csDrawer.classList.remove('open'); updateTriggerCount(); });

  $('#csSelectAll').addEventListener('click', () => {
    csCards.filter(c => c.status === CARD_STATUS.AVAILABLE && !c.fail_tag).forEach(c => csSelected.add(c.id));
    renderCardPool();
  });
  $('#csInvert').addEventListener('click', () => {
    const available = csCards.filter(c => c.status === CARD_STATUS.AVAILABLE && !c.fail_tag);
    available.forEach(c => { if (csSelected.has(c.id)) csSelected.delete(c.id); else csSelected.add(c.id); });
    renderCardPool();
  });
  $('#csClear').addEventListener('click', () => { csSelected.clear(); renderCardPool(); });
  $('#csCheckAll').addEventListener('change', (e) => {
    const visible = getFilteredCsCards();
    if (e.target.checked) visible.forEach(c => csSelected.add(c.id));
    else visible.forEach(c => csSelected.delete(c.id));
    renderCardPool();
  });
  let csTimer;
  $('#csSearch').addEventListener('input', (e) => {
    clearTimeout(csTimer);
    csTimer = setTimeout(() => { csSearchTerm = e.target.value.trim(); renderCardPool(); }, 150);
  });

  btnStart.addEventListener('click', startRegister);

  $('#btnClearLog').addEventListener('click', () => {
    $('#logContainer').innerHTML = '<div class="log-empty">等待启动...</div>';
    $('#progressWrap').style.display = 'none';
    $('#subtaskPanel').style.display = 'none';
    $('#batchSummaryCard').style.display = 'none';
  });

  loadCardPool();
  loadAccountStats();
  if ($('#proxyType').value === 'pool') {
    $('#proxyPoolSelect').style.display = 'block';
    loadProxyOptions();
  }
}

const TIER_LABELS = { '0': '充满', '20': '$20', '10': '$10', '5': '$5' };
const TIER_ORDER = ['0', '20', '10', '5'];

function syncLowerOptions() {
  const upperVal = $('#rechargeUpper').value;
  const upperIdx = TIER_ORDER.indexOf(upperVal);
  const lowerSel = $('#rechargeLower');
  const prevLower = lowerSel.value;
  lowerSel.innerHTML = '';
  for (let i = Math.max(1, upperIdx); i < TIER_ORDER.length; i++) {
    const v = TIER_ORDER[i];
    const opt = document.createElement('option');
    opt.value = v; opt.textContent = TIER_LABELS[v];
    lowerSel.appendChild(opt);
  }
  if (lowerSel.querySelector(`option[value="${prevLower}"]`)) {
    lowerSel.value = prevLower;
  }
}

function updateTierHint() {
  const strategy = $('input[name="rechargeStrategy"]:checked')?.value;
  const hint = $('#rechargeTierHint');
  if (!hint || strategy !== 'auto') return;
  const base = getFirstChargeAmount();
  const upper = $('#rechargeUpper').value;
  const lower = $('#rechargeLower').value;
  const upperLabel = TIER_LABELS[upper] || upper;
  const lowerLabel = TIER_LABELS[lower] || lower;
  hint.textContent = `首充 $${base} → 从 ${upperLabel}/次 追充至 $100，遇 3DS 降至 ${lowerLabel}，遇余额不足停止`;
}

function getFirstChargeAmount() {
  const isBilling = $('input[name="bindPath"]:checked')?.value === 'billing';
  return isBilling
    ? Math.max(5, Math.min(100, parseInt($('#creditAmountInput').value) || 10))
    : parseInt($('input[name="creditAmount"]:checked')?.value) || 5;
}

// ─── Card pool multi-select ────────────────────────────────

async function loadCardPool() {
  try {
    const data = await api.cards.list();
    csCards = (data.cards || []).filter(c => c.status === CARD_STATUS.AVAILABLE && !c.fail_tag);
    const validIds = new Set(csCards.map(c => c.id));
    csSelected.forEach(id => { if (!validIds.has(id)) csSelected.delete(id); });
    renderCardPool();
    updateTriggerCount();
  } catch (e) {
    showToast('加载卡片列表失败', 'error');
  }
}

function getFilteredCsCards() {
  if (!csSearchTerm) return csCards;
  const q = csSearchTerm.toLowerCase();
  return csCards.filter(c =>
    (c.number || '').toLowerCase().includes(q) ||
    (c.holder_name || '').toLowerCase().includes(q)
  );
}

function renderCardPool() {
  const body = $('#csCardBody');
  const emptyEl = $('#csEmpty');
  const visible = getFilteredCsCards();
  if (!visible.length) {
    body.innerHTML = '';
    emptyEl.style.display = 'block';
    emptyEl.textContent = csCards.length ? '无匹配卡片' : '无可用卡片';
    updateCardSelectCount();
    return;
  }
  emptyEl.style.display = 'none';
  body.innerHTML = visible.map(c => {
    const checked = csSelected.has(c.id);
    const exp = `${c.exp_month}/${c.exp_year}`;
    const holder = c.holder_name || '-';
    const addrParts = [c.city, c.state, c.zip].filter(Boolean);
    const addrShort = addrParts.length ? addrParts.join(', ') : '-';
    const failTag = c.fail_tag ? `<span class="badge fail-${c.fail_tag}">${c.fail_tag}</span>` : '';
    const statusBadge = `<span class="badge ${c.status}">${c.status}</span>`;
    return `<tr class="${checked ? 'row-selected' : ''}" data-id="${c.id}">
      <td><input type="checkbox" class="row-check cs-check" data-id="${c.id}" ${checked ? 'checked' : ''}></td>
      <td class="cell-id">${c.id}</td>
      <td style="font-family:var(--font-mono);font-size:11px">${esc(c.number)}</td>
      <td>${esc(exp)}</td>
      <td>${esc(holder)}</td>
      <td title="${esc(c.address_line1 || '')}">${esc(addrShort)}</td>
      <td>${statusBadge} ${failTag}</td>
    </tr>`;
  }).join('');

  const csCbs = [...body.querySelectorAll('.cs-check')];
  csCbs.forEach((cb, idx) => {
    cb.addEventListener('click', (e) => {
      const id = +cb.dataset.id;
      if (e.shiftKey && csLastClickedIdx !== null) {
        const from = Math.min(csLastClickedIdx, idx);
        const to = Math.max(csLastClickedIdx, idx);
        for (let i = from; i <= to; i++) {
          const cbi = csCbs[i];
          const iid = +cbi.dataset.id;
          csSelected.add(iid);
          cbi.checked = true;
          cbi.closest('tr').classList.add('row-selected');
        }
      } else {
        if (cb.checked) csSelected.add(id); else csSelected.delete(id);
        cb.closest('tr').classList.toggle('row-selected', cb.checked);
      }
      csLastClickedIdx = idx;
      updateCardSelectCount();
    });
  });
  updateCardSelectCount();
}

function updateCardSelectCount() {
  const n = csSelected.size;
  const label = n ? `已选 ${n} 张` : '未选择';
  $('#csCount').textContent = label;
  $('#csFooterCount').textContent = `已选 ${n} 张`;
  $('#csCheckAll').checked = n > 0 && getFilteredCsCards().every(c => csSelected.has(c.id));
}

function updateTriggerCount() {
  const n = csSelected.size;
  $('#csCount').textContent = n ? `已选 ${n} 张` : '未选择';
  $('#csCount').classList.toggle('has-cards', n > 0);
}

// ─── Proxy loader ──────────────────────────────────────────

async function loadProxyOptions() {
  try {
    const data = await api.proxies.list();
    const sel = $('#proxyPoolSelect');
    sel.innerHTML = '<option value="">随机</option>';
    const available = (data.proxies || []).filter(p => p.status !== PROXY_STATUS.DISABLED);
    available.forEach(p => {
      const label = p.label || `${p.type}://${p.host}:${p.port}`;
      sel.innerHTML += `<option value="${p.id}">${esc(label)}</option>`;
    });
    if (!available.length) {
      $('#proxyPoolId').value = '';
    }
  } catch (e) {
    showToast('加载代理列表失败', 'error');
  }
}

// ─── Log / Progress ────────────────────────────────────────

function appendLog(msg, cls = '') {
  const logBox = $('#logContainer');
  const empty = logBox.querySelector('.log-empty');
  if (empty) empty.remove();
  const div = document.createElement('div');
  div.className = `log-line ${cls}`;
  const t = new Date().toLocaleTimeString('zh-CN', { hour12: false });
  div.innerHTML = `<span class="ts">${t}</span>${esc(msg)}`;
  logBox.appendChild(div);
  logBox.scrollTop = logBox.scrollHeight;
}

function setProgress(el, bar, text, cur, tot) {
  el.style.display = 'block';
  const pct = tot > 0 ? Math.round((cur / tot) * 100) : 0;
  bar.style.width = pct + '%';
  text.textContent = `${cur}/${tot}`;
}

function initSubtaskPanel() {
  const panel = $('#subtaskPanel');
  const list = $('#subtaskList');
  panel.style.display = 'block';
  list.innerHTML = '';
  $('#batchSummaryCard').style.display = 'none';
  $('#subtaskSummary').textContent = '';
}

function updateSubtask(idx, status, info, cardActionHtml = '') {
  let row = $(`#sub-${idx}`);
  if (!row) {
    const list = $('#subtaskList');
    row = document.createElement('div');
    row.className = 'subtask-row';
    row.id = `sub-${idx}`;
    row.innerHTML = `<span class="subtask-idx">#${idx}</span><span class="subtask-badge pending">等待中</span><span class="subtask-info">-</span>`;
    list.appendChild(row);
    row.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
  const badge = row.querySelector('.subtask-badge');
  const infoEl = row.querySelector('.subtask-info');
  badge.className = `subtask-badge ${status}`;
  const labels = { success: '成功', failed: '失败', running: '运行中', pending: '等待中' };
  badge.textContent = labels[status] || status;
  infoEl.textContent = info || '-';
  infoEl.title = info || '';
  let actionEl = row.querySelector('.subtask-card-action-wrap');
  if (cardActionHtml) {
    if (!actionEl) {
      actionEl = document.createElement('span');
      actionEl.className = 'subtask-card-action-wrap';
      row.appendChild(actionEl);
    }
    actionEl.innerHTML = cardActionHtml;
    bindCardActionEvents(row);
  }
}

function showBatchSummary(success, failed, total) {
  const card = $('#batchSummaryCard');
  card.style.display = 'flex';
  $('#summarySuccess').textContent = success;
  $('#summaryFailed').textContent = failed;
  $('#summaryTotal').textContent = total;
}

function renderRegisterControlState() {
  const btnStart = $('#btnStart');
  const btnPause = $('#btnPause');
  const btnResume = $('#btnResume');
  if (pausePending && pauseStats) {
    const parked = pauseStats.parked_flows || 0;
    const active = pauseStats.active_flows || 0;
    btnStart.innerHTML = `<span class="btn-icon">⏸</span> 暂停中 ${parked}/${active}`;
    $('#subtaskSummary').textContent = `暂停收敛中 ${parked}/${active} · 手动等待 ${pauseStats.manual_wait_flows || 0}`;
    btnPause.disabled = true;
    btnPause.style.display = 'inline-flex';
    btnResume.style.display = 'none';
    return;
  }
  if (isPaused && pauseStats) {
    const parked = pauseStats.parked_flows || 0;
    const active = pauseStats.active_flows || 0;
    btnStart.innerHTML = `<span class="btn-icon">⏸</span> 已暂停 ${parked}/${active}`;
    $('#subtaskSummary').textContent = `已暂停 ${parked}/${active} · 手动等待 ${pauseStats.manual_wait_flows || 0}`;
    btnPause.style.display = 'none';
    btnResume.style.display = 'inline-flex';
    return;
  }
}

// ─── Start registration ────────────────────────────────────

async function startRegister() {
  const concurrency = Math.max(1, Math.min(parseInt($('#regConcurrency').value) || 1, 10));

  const selectedCardIds = [...csSelected];
  if (!selectedCardIds.length) {
    showToast('请至少选择卡片', 'warning');
    return;
  }

  const proxyType = $('#proxyType').value;
  if (proxyType === 'pool') {
    const availableProxyCount = [...$('#proxyPoolSelect').options].filter(o => o.value).length;
    if (!availableProxyCount) {
      showToast('当前没有可用代理，请先启用代理或切换为直连', 'warning');
      return;
    }
  }

  const btnStart = $('#btnStart');
  const btnPause = $('#btnPause');
  const btnResume = $('#btnResume');
  const btnStop = $('#btnStop');
  btnStart.disabled = true;
  btnStart.classList.add('running');
  isPaused = false;
  btnPause.style.display = 'inline-flex';
  btnResume.style.display = 'none';
  btnStop.style.display = 'inline-flex';

  btnStart.innerHTML = '<span class="btn-icon">⟳</span> 连续注册中...';
  running++;
  pauseStats = null;
  appendLog(`启动连续注册: 卡池 ${selectedCardIds.length} 张, 并发 ${concurrency}`, 'step');
  initSubtaskPanel();
  $('#statRunning').textContent = running;

  const body = {
    proxy_type: proxyType,
    proxy_str: proxyType === 'direct' || proxyType === 'pool' ? '' : $('#proxyStr').value,
    proxy_id: proxyType === 'pool' && $('#proxyPoolId').value ? parseInt($('#proxyPoolId').value) : null,
    concurrency,
    card_ids: selectedCardIds,
    bind_path: $('input[name="bindPath"]:checked').value,
    credit_amount: getFirstChargeAmount(),
    recharge_strategy: $('input[name="rechargeStrategy"]:checked')?.value || 'none',
    recharge_upper: parseInt($('#rechargeUpper').value) || 20,
    recharge_lower: parseInt($('#rechargeLower').value) || 5,
  };

  try {
    const data = await api.register.start(body);
    currentTaskId = data.task_id;
    const poolSize = data.pool_size;
    appendLog(`任务 ${data.task_id} 已创建, 卡池 ${poolSize} 张`, 'step');
    updatePoolStatus(poolSize, 0, 0);

    const pw = $('#progressWrap'), pb = $('#progressBar'), pt = $('#progressText');
    pw.style.display = 'block';

    listenSSE(`/api/register/${data.task_id}/stream`, {
      onStep(d) {
        appendLog(d.message, 'step');
        if (d.sub_task) updateSubtask(d.sub_task, 'running', d.message);
      },
      onPauseRequested(d) {
        pausePending = true;
        pauseStats = d;
        btnPause.disabled = true;
        renderRegisterControlState();
        appendLog(`暂停请求已受理，已停住 ${d.parked_flows || 0}/${d.active_flows || 0} 个 flow`, 'step');
      },
      onPauseProgress(d) {
        pauseStats = d;
        renderRegisterControlState();
        appendLog(`暂停收敛中：已停住 ${d.parked_flows || 0}/${d.active_flows || 0}，手动等待 ${d.manual_wait_flows || 0}`, 'step');
      },
      onSubDone(d) {
        const pr = d.pool_remaining ?? 0;
        const used = poolSize - pr;
        setProgress(pw, pb, pt, used, poolSize);
        updatePoolStatus(pr, d.success, d.failed);
        btnStart.innerHTML = `<span class="btn-icon">⟳</span> 成功${d.success} · 失败${d.failed} · 剩余${pr}卡`;
        const creditInfo = d.credit_loaded ? ` · $${d.credit_loaded}` : '';
        const cardAction = buildCardActionHtml(d.card_id);
        updateSubtask(d.sub_task, 'success', `${d.email || '成功'}${creditInfo}`, cardAction);
        appendLog(`[#${d.sub_task}] ✓ ${d.email || '成功'}${creditInfo} · 卡#${d.card_id} · 剩余${pr}`, 'done');
      },
      onSubError(d) {
        const pr = d.pool_remaining ?? 0;
        const used = poolSize - pr;
        setProgress(pw, pb, pt, used, poolSize);
        updatePoolStatus(pr, d.success, d.failed);
        btnStart.innerHTML = `<span class="btn-icon">⟳</span> 成功${d.success} · 失败${d.failed} · 剩余${pr}卡`;
        const cardAction = buildCardActionHtml(d.card_id);
        updateSubtask(d.sub_task, 'failed', d.message, cardAction);
        appendLog(`[#${d.sub_task}] ✗ ${d.message} · 剩余${pr}卡`, 'error');
      },
      onDone(d) {
        const r = d.result || {};
        appendLog(`全部完成! 成功 ${r.success || 0}, 失败 ${r.failed || 0}, 卡池已耗尽`, 'done');
        showBatchSummary(r.success || 0, r.failed || 0, (r.success || 0) + (r.failed || 0));
        finishReg();
        loadAccounts();
        loadCardPool();
      },
      onAwaitingManual(d) {
        const label = d.sub_task ? `[#${d.sub_task}] ` : '';
        appendLog(`${label}等待手动续充 (关闭浏览器触发记录)...`, 'step');
        if (d.sub_task) updateSubtask(d.sub_task, 'running', '等待手动续充...');
      },
      onBrowserClosed(d) {
        const label = d.sub_task ? `[#${d.sub_task}] ` : '';
        appendLog(`${label}浏览器已关闭，等待记录...`, 'step');
        showManualResultDialog(d, currentTaskId);
      },
      onCancelled(d) {
        const r = d.result || {};
        appendLog(`任务已停止: 成功 ${r.success || 0}, 失败 ${r.failed || 0}`, 'error');
        showBatchSummary(r.success || 0, r.failed || 0, (r.success || 0) + (r.failed || 0));
        finishReg();
      },
      onPaused(d) {
        isPaused = true;
        pausePending = false;
        pauseStats = d;
        btnPause.disabled = false;
        renderRegisterControlState();
        appendLog(`任务已暂停：已停住 ${d.parked_flows || 0}/${d.active_flows || 0} 个 flow`, 'step');
      },
      onResumed() {
        isPaused = false;
        pausePending = false;
        pauseStats = null;
        btnResume.style.display = 'none';
        btnPause.style.display = 'inline-flex';
        btnPause.disabled = false;
        btnStart.innerHTML = '<span class="btn-icon">⟳</span> 连续注册中...';
        appendLog('任务已恢复', 'step');
      },
      onError(d) {
        appendLog(`错误: ${d.message}`, 'error');
        finishReg();
      },
    });
  } catch (e) {
    appendLog(`请求失败: ${e.message}`, 'error');
    finishReg();
  }
}

function updatePoolStatus(remaining, success, failed) {
  $('#subtaskSummary').textContent = `成功 ${success} · 失败 ${failed} · 剩余 ${remaining} 卡`;
}

function finishReg() {
  running = Math.max(0, running - 1);
  $('#statRunning').textContent = running;
  const btnStart = $('#btnStart');
  btnStart.disabled = false;
  btnStart.classList.remove('running');
  btnStart.innerHTML = '<span class="btn-icon">▶</span> 启动注册';
  $('#btnPause').style.display = 'none';
  $('#btnResume').style.display = 'none';
  $('#btnStop').style.display = 'none';
  currentTaskId = null;
  isPaused = false;
  pausePending = false;
  pauseStats = null;
  $('#progressBar').style.width = '0%';
  $('#progressWrap').style.display = 'none';
  loadAccountStats();
  loadDashboard();
}

async function loadAccountStats() {
  try {
    const d = await api.accounts.list();
    const accounts = d.accounts || [];
    const today = new Date().toISOString().slice(0, 10);
    $('#statTotal').textContent = accounts.length;
    $('#statToday').textContent = accounts.filter(a => (a.registered_at || '').startsWith(today)).length;
  } catch (e) {
    console.error(e);
  }
}

function buildCardActionHtml(cardId) {
  if (!cardId) return '';
  return `<select class="subtask-card-action" data-card-id="${cardId}" title="卡片操作">
    <option value="">卡 #${cardId}</option>
    <option value="keep">保持可用</option>
    <option value="${CARD_FAIL_TAG.DECLINE}">禁用: decline</option>
    <option value="${CARD_FAIL_TAG.THREE_DS}">禁用: 3ds</option>
    <option value="${CARD_FAIL_TAG.INSUFFICIENT}">禁用: insufficient</option>
  </select>`;
}

function bindCardActionEvents(container) {
  container.querySelectorAll('.subtask-card-action').forEach(sel => {
    if (sel._bound) return;
    sel._bound = true;
    sel.addEventListener('change', async () => {
      const cardId = +sel.dataset.cardId;
      const action = sel.value;
      if (!action) return;
      try {
        if (action === 'keep') {
          await api.cards.batchFailTag([cardId], '');
          await api.cards.batchStatus([cardId], CARD_STATUS.AVAILABLE);
          showToast(`卡 #${cardId} 保持可用`, 'success');
        } else {
          await api.cards.batchFailTag([cardId], action);
          await api.cards.batchStatus([cardId], CARD_STATUS.DISABLED);
          showToast(`卡 #${cardId} 已禁用 [${action}]`, 'warning');
        }
      } catch (e) { showToast(`操作失败: ${e.message}`, 'error'); }
      sel.value = '';
    });
  });
}

function showManualResultDialog(data, taskId) {
  document.querySelectorAll('.manual-result-overlay').forEach(el => el.remove());
  const credit = data.credit_loaded || 0;
  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay manual-result-overlay';
  overlay.innerHTML = `
    <div class="modal-card" style="max-width:440px">
      <div class="modal-title">手动续充记录</div>
      <div class="reg-result-body">
        <div class="reg-result-row"><span class="reg-result-label">账号</span><span class="reg-result-val">${esc(data.email || '#' + data.account_id)}</span></div>
        <div class="reg-result-row"><span class="reg-result-label">使用卡</span><span class="reg-result-val">#${data.card_id}</span></div>
        <div class="reg-result-row"><span class="reg-result-label">首充额度</span><span class="reg-result-val">$${credit}</span></div>
        <div class="reg-result-row">
          <span class="reg-result-label">最终总额</span>
          <div style="display:flex;align-items:center;gap:4px">
            <input type="number" class="input-text" id="manualCreditInput" min="0" step="1" value="${credit}" style="width:80px;margin:0">
            <span class="hint-text">$</span>
          </div>
        </div>
        <div class="reg-result-card-actions">
          <span class="reg-result-label">卡片操作</span>
          <div class="radio-group" style="flex-wrap:wrap;gap:10px">
            <label class="radio"><input type="radio" name="manualCardAction" value="keep" checked> 保持</label>
            <label class="radio"><input type="radio" name="manualCardAction" value="${CARD_FAIL_TAG.DECLINE}"> decline</label>
            <label class="radio"><input type="radio" name="manualCardAction" value="${CARD_FAIL_TAG.THREE_DS}"> 3ds</label>
            <label class="radio"><input type="radio" name="manualCardAction" value="${CARD_FAIL_TAG.INSUFFICIENT}"> insufficient</label>
          </div>
        </div>
      </div>
      <div class="modal-actions" style="margin-top:16px">
        <button class="modal-btn modal-btn-confirm accent" id="manualSubmitBtn">确认</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  requestAnimationFrame(() => requestAnimationFrame(() => overlay.classList.add('show')));

  const submitBtn = overlay.querySelector('#manualSubmitBtn');
  submitBtn.addEventListener('click', async () => {
    const creditLoaded = parseFloat(overlay.querySelector('#manualCreditInput').value) || 0;
    const cardAction = overlay.querySelector('input[name="manualCardAction"]:checked').value;
    submitBtn.disabled = true;
    submitBtn.textContent = '提交中...';
    try {
      await api.register.manualResult(taskId, { credit_loaded: creditLoaded, card_action: cardAction });
      overlay.classList.remove('show');
      setTimeout(() => overlay.remove(), 220);
      showToast('手动续充记录已保存', 'success');
      loadAccounts();
    } catch (e) {
      showToast(`提交失败: ${e.message}`, 'error');
      submitBtn.disabled = false;
      submitBtn.textContent = '确认';
    }
  });
}

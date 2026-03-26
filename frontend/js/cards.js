import { $, $$, esc, escAttr, showToast, showConfirm, listenSSE } from './utils.js';
import * as api from './api.js';
import { loadDashboard } from './dashboard.js';
import { CARD_FAIL_TAG, CARD_STATUS, PROXY_STATUS } from './constants.js';

const TAX_FREE = new Set(['AK', 'DE', 'MT', 'NH', 'OR']);
let currentAddress = null;
let allCards = [];
let cardSelected = new Set();
let cardFilter = CARD_STATUS.AVAILABLE;
let cardSearchTerm = '';
let pvRows = [];
let cardLastClickedIdx = null;
let cardSortField = null;
let cardSortDir = 'asc';
let cardPage = 1;
let cardPageSize = 50;
let cardTotal = 0;
let retryBindTaskId = null;
let retryBindPaused = false;
let retryBindPausePending = false;

export function initCards() {
  $('#addrTaxFreeOnly').addEventListener('change', loadStates);
  $('#btnRandomAddr').addEventListener('click', rollFullAddress);
  $('#btnRerollStreet').addEventListener('click', rerollStreet);
  $('#btnRerollLocation').addEventListener('click', rerollLocation);
  ['addrLine1', 'addrCity', 'addrState', 'addrZip'].forEach(id => {
    $(`#${id}`).addEventListener('input', () => { syncAddrFromFields(); if (id === 'addrState') updateAddrBadge(); });
  });

  const drawer = $('#cardImportDrawer');
  $('#btnOpenCardImport').addEventListener('click', () => drawer.classList.add('open'));
  $('#btnCloseCardImport').addEventListener('click', () => drawer.classList.remove('open'));
  drawer.addEventListener('click', (e) => { if (e.target === drawer) drawer.classList.remove('open'); });

  $$('#cardInputTabs .sub-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('#cardInputTabs .sub-tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      const m = btn.dataset.mode;
      $('#cardModeBatch').style.display = m === 'batch' ? 'block' : 'none';
      $('#cardModeManual').style.display = m === 'manual' ? 'block' : 'none';
    });
  });

  $('#btnParseCards').addEventListener('click', parsePreview);
  $('#btnConfirmImport').addEventListener('click', confirmImport);
  $('#btnClearPreview').addEventListener('click', () => {
    $('#previewWrap').style.display = 'none';
    $('#previewBody').innerHTML = '';
    $('#parseStatus').style.display = 'none';
    pvRows = [];
  });
  $('#btnManualAdd').addEventListener('click', manualAddCard);
  $('#btnRefreshCards').addEventListener('click', loadCards);

  $$('#cardFilterTabs .filter-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      $$('#cardFilterTabs .filter-tab').forEach(t => t.classList.remove('active'));
      btn.classList.add('active');
      cardFilter = btn.dataset.filter;
      cardPage = 1;
      loadCards();
    });
  });

  let timer;
  $('#cardSearch').addEventListener('input', (e) => {
    clearTimeout(timer);
    timer = setTimeout(() => { cardSearchTerm = e.target.value.trim(); cardPage = 1; loadCards(); }, 200);
  });

  $('#cardCheckAll').addEventListener('change', (e) => {
    if (e.target.checked) cardSelectAll(); else cardSelectNone();
  });

  $$('#page-cards thead .sortable').forEach(th => {
    th.addEventListener('click', () => {
      const field = th.dataset.sort;
      if (cardSortField === field) {
        cardSortDir = cardSortDir === 'asc' ? 'desc' : 'asc';
      } else {
        cardSortField = field;
        cardSortDir = 'asc';
      }
      $$('#page-cards thead .sortable').forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
      th.classList.add(cardSortDir === 'asc' ? 'sort-asc' : 'sort-desc');
      cardPage = 1;
      loadCards();
    });
  });

  $('#pvCheckAll').addEventListener('change', (e) => {
    pvRows.forEach(r => r.checked = e.target.checked);
    renderPreviewTable();
  });

  $('#btnRetryBind')?.addEventListener('click', retryBind);

  window.cardSelectAll = cardSelectAll;
  window.cardSelectNone = cardSelectNone;
  window.cardSelectInvert = cardSelectInvert;
  window.cardBatchAction = cardBatchAction;
  window.cardBatchDelete = cardBatchDelete;
  window.cardBatchAddress = cardBatchAddress;
  window.deleteCard = deleteCard;
  window.pvRemoveRow = pvRemoveRow;
  window.pvSelectValid = pvSelectValid;
  window.pvRetryFailed = pvRetryFailed;
  window.cardSelectByTag = cardSelectByTag;
  window.cardClearFailTag = cardClearFailTag;
  window._cardPage = cardGoToPage;

  loadStates();
}

function showCardLoading() {
  const body = $('#cardsBody');
  const rows = Array.from({ length: 5 }, () =>
    `<tr class="table-loading">${Array.from({ length: 9 }, () =>
      `<td><div class="skeleton" style="width:${40 + Math.random() * 60}%"></div></td>`
    ).join('')}</tr>`
  ).join('');
  body.innerHTML = rows;
  $('#cardsEmpty').style.display = 'none';
}

export async function loadCards() {
  showCardLoading();
  try {
    const params = { page: cardPage, page_size: cardPageSize };
    if (cardSearchTerm) params.search = cardSearchTerm;
    if (cardFilter === CARD_STATUS.AVAILABLE) {
      params.status = 'available';
    } else if (cardFilter === CARD_STATUS.DISABLED) {
      params.status = 'disabled';
    } else if ([CARD_FAIL_TAG.DECLINE, CARD_FAIL_TAG.THREE_DS, CARD_FAIL_TAG.INSUFFICIENT].includes(cardFilter)) {
      params.fail_tag = cardFilter;
    }
    // 'all' sends no filter
    if (cardSortField) {
      params.sort = cardSortField;
      params.order = cardSortDir;
    }
    const data = await api.cards.list(params);
    allCards = data.cards;
    cardTotal = data.total ?? data.cards.length;
    const validIds = new Set(allCards.map(c => c.id));
    cardSelected.forEach(id => { if (!validIds.has(id)) cardSelected.delete(id); });
    renderCardsTable(allCards);
    updateCardBatchBar();
    renderCardsPagination();
  } catch (e) { showToast('加载卡片列表失败', 'error'); }
}

// ─── Address ─────────────────────────────────────────────

async function loadStates() {
  const taxFreeOnly = $('#addrTaxFreeOnly').checked;
  try {
    const { states } = await api.addresses.states(taxFreeOnly);
    const sel = $('#addrStateSelect');
    const prev = sel.value;
    sel.innerHTML = '<option value="">随机</option>';
    states.forEach(s => {
      const tag = s.tax_free ? ' ★' : '';
      sel.innerHTML += `<option value="${s.code}">${s.code} ${s.name}${tag}</option>`;
    });
    if (prev && sel.querySelector(`option[value="${prev}"]`)) sel.value = prev;
  } catch (e) { showToast('加载州列表失败', 'error'); }
}

async function rollFullAddress() {
  const state = $('#addrStateSelect').value || undefined;
  const zip = $('#addrZipFilter').value.trim() || undefined;
  const taxFreeOnly = $('#addrTaxFreeOnly').checked;
  try {
    const { address } = await api.addresses.random({ state, zip, tax_free_only: taxFreeOnly && !state && !zip });
    if (!address) { showAddrResult(null); return; }
    currentAddress = address;
    showAddrResult(address);
  } catch (e) { showToast('生成地址失败', 'error'); }
}

async function rerollStreet() {
  try {
    const { address_line1 } = await api.addresses.rerollStreet();
    if (!address_line1) return;
    $('#addrLine1').value = address_line1;
    syncAddrFromFields();
    flashField($('#addrLine1'));
  } catch (e) { showToast('生成街道失败', 'error'); }
}

async function rerollLocation() {
  const fieldState = $('#addrState').value.trim().toUpperCase() || undefined;
  const fieldZip = $('#addrZip').value.trim() || undefined;
  const taxFreeOnly = $('#addrTaxFreeOnly').checked;
  try {
    const { address } = await api.addresses.random({ state: fieldState, zip: fieldZip, tax_free_only: taxFreeOnly && !fieldState && !fieldZip });
    if (!address) return;
    $('#addrCity').value = address.city;
    $('#addrState').value = address.state;
    $('#addrZip').value = address.zip;
    syncAddrFromFields();
    updateAddrBadge();
    flashField($('#addrCity')); flashField($('#addrState')); flashField($('#addrZip'));
  } catch (e) { showToast('生成地址失败', 'error'); }
}

function showAddrResult(addr) {
  const result = $('#addrResult');
  const hint = $('#addrEmptyHint');
  if (!addr) { result.style.display = 'none'; hint.style.display = 'block'; hint.textContent = '无匹配地址，请调整筛选条件'; return; }
  hint.style.display = 'none';
  result.style.display = 'block';
  $('#addrLine1').value = addr.address_line1 || '';
  $('#addrCity').value = addr.city || '';
  $('#addrState').value = addr.state || '';
  $('#addrZip').value = addr.zip || '';
  updateAddrBadge();
}

function updateAddrBadge() {
  const st = $('#addrState').value.trim().toUpperCase();
  const badge = $('#addrBadge');
  if (TAX_FREE.has(st)) { badge.textContent = '★ 免税州'; badge.className = 'addr-meta-badge tax-free'; }
  else if (st) { badge.textContent = st; badge.className = 'addr-meta-badge normal'; }
  else { badge.textContent = ''; badge.className = 'addr-meta-badge'; }
}

function syncAddrFromFields() {
  const line1 = $('#addrLine1').value.trim();
  const city = $('#addrCity').value.trim();
  const state = $('#addrState').value.trim();
  const zip = $('#addrZip').value.trim();
  if (!line1 && !city && !state && !zip) { currentAddress = null; return; }
  currentAddress = { address_line1: line1, city, state, zip, country: 'US' };
}

function flashField(el) { el.classList.add('field-flash'); setTimeout(() => el.classList.remove('field-flash'), 500); }

// ─── Card list ───────────────────────────────────────────

function cardStatusRank(c) {
  if (c.status === CARD_STATUS.AVAILABLE && !c.fail_tag) return 0;
  if (c.status === CARD_STATUS.AVAILABLE && c.fail_tag) return 1;
  if (c.status === CARD_STATUS.DISABLED) return 2;
  return 3;
}

function getFilteredCards() {
  return allCards;
}

function renderCardsPagination() {
  let el = document.getElementById('cardsPagination');
  if (!el) {
    el = document.createElement('div');
    el.id = 'cardsPagination';
    el.className = 'pagination-bar';
    const tableWrap = document.querySelector('#page-cards .table-wrap');
    if (tableWrap) tableWrap.after(el);
  }

  const totalPages = Math.max(1, Math.ceil(cardTotal / cardPageSize));

  let html = `<span class="page-info">共 ${cardTotal} 条，第 ${cardPage}/${totalPages} 页</span>`;
  html += `<div class="page-btns">`;
  html += `<button class="btn btn-sm" ${cardPage <= 1 ? 'disabled' : ''} onclick="window._cardPage(1)">首页</button>`;
  html += `<button class="btn btn-sm" ${cardPage <= 1 ? 'disabled' : ''} onclick="window._cardPage(${cardPage - 1})">上一页</button>`;
  html += `<button class="btn btn-sm" ${cardPage >= totalPages ? 'disabled' : ''} onclick="window._cardPage(${cardPage + 1})">下一页</button>`;
  html += `<button class="btn btn-sm" ${cardPage >= totalPages ? 'disabled' : ''} onclick="window._cardPage(${totalPages})">末页</button>`;
  html += `</div>`;
  el.innerHTML = html;
}

function cardGoToPage(p) {
  cardPage = p;
  loadCards();
}

function renderCardsTable(cards) {
  const body = $('#cardsBody');
  const empty = $('#cardsEmpty');
  if (!cards.length) { body.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';
  body.innerHTML = cards.map(c => {
    const exp = `${c.exp_month}/${c.exp_year}`;
    const holder = c.holder_name || '-';
    const addrParts = [c.city, c.state, c.zip].filter(Boolean);
    const addrShort = addrParts.length ? addrParts.join(', ') : '-';
    const addrFull = c.address_line1 ? `${c.address_line1}, ${addrShort}` : addrShort;
    const taxFree = TAX_FREE.has(c.state);
    const addrBadge = c.state ? (taxFree ? '<span class="badge-sm tax-free">免税</span> ' : '') : '';
    const statusCls = c.status;
    const statusBadge = `<span class="badge ${statusCls}">${c.status}</span>`;
    const failTag = c.fail_tag ? `<span class="badge fail-${c.fail_tag}">${c.fail_tag}</span>` : '';
    const useInfo = c.bound_count > 0 ? `${c.bound_count}号` : `${c.use_count || 0}次`;
    const sel = cardSelected.has(c.id) ? 'checked' : '';
    const dimRow = c.status === CARD_STATUS.DISABLED || c.fail_tag ? ' row-dim' : '';
    return `<tr class="${sel ? 'row-selected' : ''}${dimRow}" data-id="${c.id}">
      <td><input type="checkbox" class="row-check card-check" data-id="${c.id}" ${sel}></td>
      <td class="cell-id">${c.id}</td>
      <td>${esc(c.number)}</td>
      <td>${esc(exp)}</td>
      <td>${esc(holder)}</td>
      <td title="${escAttr(addrFull)}">${addrBadge}${esc(addrShort)}</td>
      <td>${statusBadge} ${failTag}</td>
      <td>${esc(useInfo)}</td>
      <td><button class="btn-sm danger" onclick="deleteCard(${c.id})">删</button></td>
    </tr>`;
  }).join('');

  const checkboxes = [...body.querySelectorAll('.card-check')];
  checkboxes.forEach((cb, idx) => {
    cb.addEventListener('click', (e) => {
      const id = +cb.dataset.id;
      if (e.shiftKey && cardLastClickedIdx !== null) {
        const from = Math.min(cardLastClickedIdx, idx);
        const to = Math.max(cardLastClickedIdx, idx);
        for (let i = from; i <= to; i++) {
          const cbi = checkboxes[i];
          const iid = +cbi.dataset.id;
          cardSelected.add(iid);
          cbi.checked = true;
          cbi.closest('tr').classList.add('row-selected');
        }
      } else {
        if (cb.checked) cardSelected.add(id); else cardSelected.delete(id);
        cb.closest('tr').classList.toggle('row-selected', cb.checked);
      }
      cardLastClickedIdx = idx;
      updateCardBatchBar();
    });
  });
}

function updateCardBatchBar() {
  const bar = $('#cardBatchBar');
  const n = cardSelected.size;
  bar.style.display = n ? 'flex' : 'none';
  const filtered = getFilteredCards();
  $('#cardSelCount').textContent = `已选 ${n} / ${filtered.length} 项`;
  $('#cardCheckAll').checked = n > 0 && filtered.every(c => cardSelected.has(c.id));

  const hasFailTag = [...cardSelected].some(id => {
    const c = allCards.find(x => x.id === id);
    return c && c.fail_tag;
  });
  const retryBtn = $('#btnRetryBind');
  if (retryBtn) retryBtn.style.display = hasFailTag ? 'inline-flex' : 'none';
}

function cardSelectAll() { getFilteredCards().forEach(c => cardSelected.add(c.id)); renderCardsTable(getFilteredCards()); updateCardBatchBar(); }
function cardSelectNone() { cardSelected.clear(); renderCardsTable(getFilteredCards()); updateCardBatchBar(); }
function cardSelectInvert() { getFilteredCards().forEach(c => { if (cardSelected.has(c.id)) cardSelected.delete(c.id); else cardSelected.add(c.id); }); renderCardsTable(getFilteredCards()); updateCardBatchBar(); }

function cardSelectByTag(tag) {
  if (!tag) return;
  cardSelected.clear();
  allCards.filter(c => c.fail_tag === tag).forEach(c => cardSelected.add(c.id));
  renderCardsTable(getFilteredCards());
  updateCardBatchBar();
}

async function cardClearFailTag() {
  const ids = [...cardSelected];
  if (!ids.length) return;
  try {
    await api.cards.batchFailTag(ids, '');
    showToast(`已清除 ${ids.length} 张卡的失败标记`, 'success');
    loadCards();
  } catch (e) {
    showToast(`清除标记失败: ${e.message}`, 'error');
  }
}

async function cardBatchAction(status) {
  const ids = [...cardSelected];
  if (!ids.length) return;
  await api.cards.batchStatus(ids, status);
  showToast(`已更新 ${ids.length} 张卡状态`, 'success');
  loadCards();
}

async function cardBatchDelete() {
  const ids = [...cardSelected];
  if (!ids.length) return;
  if (!await showConfirm(`确认删除 ${ids.length} 张卡？此操作不可撤销。`)) return;
  await api.cards.batchDelete(ids);
  showToast(`已删除 ${ids.length} 张卡`, 'success');
  cardSelected.clear();
  loadCards();
  loadDashboard();
}

async function deleteCard(id) {
  await api.cards.remove(id);
  showToast('卡片已删除', 'success');
  loadCards();
}

async function cardBatchAddress() {
  const ids = [...cardSelected];
  if (!ids.length) return;
  if (!currentAddress || !currentAddress.address_line1) { showToast('请先在「账单地址」区域 Roll 一个地址', 'warning'); return; }
  if (!await showConfirm(`将当前地址 (${currentAddress.state} ${currentAddress.city}) 分配给 ${ids.length} 张卡？`)) return;
  await api.cards.batchAddress(ids, { ...currentAddress, country: 'US' });
  showToast(`已为 ${ids.length} 张卡分配地址`, 'success');
  loadCards();
}

// ─── Card retry bind ─────────────────────────────────────

function removeRetryBindControl() {
  document.getElementById('retryBindTaskControl')?.remove();
  retryBindTaskId = null;
  retryBindPaused = false;
  retryBindPausePending = false;
}

function renderRetryBindControl() {
  if (!retryBindTaskId) return;
  let box = document.getElementById('retryBindTaskControl');
  if (!box) {
    box = document.createElement('div');
    box.id = 'retryBindTaskControl';
    box.className = 'modal-overlay';
    box.innerHTML = `
      <div class="modal-card" style="max-width:360px">
        <div class="modal-title">重绑任务控制</div>
        <div class="modal-message" id="retryBindTaskStatus"></div>
        <div class="modal-actions">
          <button class="modal-btn" id="retryBindPauseBtn">暂停</button>
          <button class="modal-btn" id="retryBindResumeBtn" style="display:none">继续</button>
          <button class="modal-btn modal-btn-confirm" id="retryBindCancelBtn">停止</button>
        </div>
      </div>`;
    document.body.appendChild(box);
    requestAnimationFrame(() => requestAnimationFrame(() => box.classList.add('show')));
    box.addEventListener('click', (e) => { if (e.target === box) e.stopPropagation(); });
  }
  $('#retryBindTaskStatus').textContent = retryBindPaused
    ? `任务 ${retryBindTaskId} 已暂停`
    : retryBindPausePending
      ? `任务 ${retryBindTaskId} 暂停中...`
      : `任务 ${retryBindTaskId} 运行中`;
  $('#retryBindPauseBtn').style.display = (retryBindPaused || retryBindPausePending) ? 'none' : 'inline-flex';
  $('#retryBindResumeBtn').style.display = retryBindPaused ? 'inline-flex' : 'none';
  $('#retryBindPauseBtn').onclick = async () => {
    try {
      await api.cards.retryBindPause(retryBindTaskId);
    } catch (e) {
      showToast(`暂停失败: ${e.message}`, 'error');
    }
  };
  $('#retryBindResumeBtn').onclick = async () => {
    try {
      await api.cards.retryBindResume(retryBindTaskId);
    } catch (e) {
      showToast(`恢复失败: ${e.message}`, 'error');
    }
  };
  $('#retryBindCancelBtn').onclick = async () => {
    try {
      await api.cards.retryBindCancel(retryBindTaskId);
    } catch (e) {
      showToast(`停止失败: ${e.message}`, 'error');
    }
  };
}

async function retryBind() {
  const ids = [...cardSelected].filter(id => {
    const c = allCards.find(x => x.id === id);
    return c && c.fail_tag;
  });
  if (!ids.length) { showToast('没有选中带失败标记的卡', 'warning'); return; }

  const overlay = document.createElement('div');
  overlay.className = 'modal-overlay';
  overlay.innerHTML = `
    <div class="modal-card" style="max-width:460px">
      <div class="modal-title">重试绑定 (${ids.length} 张卡)</div>
      <div class="reg-result-body">
        <div class="hint-text" style="margin-bottom:12px">每张卡将创建一个新账号进行注册+绑卡。</div>
        <div class="reg-result-row">
          <span class="reg-result-label">绑卡路径</span>
          <div class="radio-group">
            <label class="radio"><input type="radio" name="retryBindPath" value="onboarding" checked> Onboarding</label>
            <label class="radio"><input type="radio" name="retryBindPath" value="billing"> Billing</label>
          </div>
        </div>
        <div class="reg-result-row">
          <span class="reg-result-label">首充金额</span>
          <div id="retryOnboardingAmt">
            <div class="radio-group">
              <label class="radio"><input type="radio" name="retryCreditAmt" value="5" checked> $5</label>
              <label class="radio"><input type="radio" name="retryCreditAmt" value="10"> $10</label>
              <label class="radio"><input type="radio" name="retryCreditAmt" value="20"> $20</label>
            </div>
          </div>
          <div id="retryBillingAmt" style="display:none">
            <div style="display:flex;align-items:center;gap:4px">
              <input type="number" class="input-text" id="retryCreditInput" min="5" max="100" value="10" style="width:80px;margin:0">
              <span class="hint-text">$ (5~100)</span>
            </div>
          </div>
        </div>
        <div class="reg-result-row">
          <span class="reg-result-label">追充策略</span>
          <div class="radio-group">
            <label class="radio"><input type="radio" name="retryRechargeStrategy" value="none" checked> 不追充</label>
            <label class="radio"><input type="radio" name="retryRechargeStrategy" value="auto"> 自动追充</label>
          </div>
        </div>
        <div class="reg-result-row" id="retryTierRow" style="display:none">
          <span class="reg-result-label">追充档位</span>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            <span class="hint-text">起始</span>
            <select class="input-select" id="retryRechargeUpper" style="width:85px;margin:0">
              <option value="0">充满</option>
              <option value="20" selected>$20</option>
              <option value="10">$10</option>
              <option value="5">$5</option>
            </select>
            <span class="hint-text">最低</span>
            <select class="input-select" id="retryRechargeLower" style="width:75px;margin:0">
              <option value="20">$20</option>
              <option value="10">$10</option>
              <option value="5" selected>$5</option>
            </select>
          </div>
        </div>
        <div class="reg-result-row">
          <span class="reg-result-label">代理</span>
          <select class="input-select" id="retryProxyType" style="width:120px;margin:0">
            <option value="pool">代理池</option>
            <option value="direct">直连</option>
          </select>
        </div>
        <div class="reg-result-row">
          <span class="reg-result-label">并发数</span>
          <input type="number" class="input-text" id="retryConcurrency" min="1" max="10" value="1" style="width:60px;margin:0">
        </div>
        <div class="reg-result-row">
          <span class="reg-result-label">预估总额</span>
          <span class="reg-result-val accent" id="retryTotalPreview">$5/卡</span>
        </div>
      </div>
      <div class="modal-actions" style="margin-top:16px">
        <button class="modal-btn modal-btn-cancel" id="retryCancelBtn">取消</button>
        <button class="modal-btn modal-btn-confirm accent" id="retryConfirmBtn">开始重试</button>
      </div>
    </div>`;
  document.body.appendChild(overlay);
  requestAnimationFrame(() => requestAnimationFrame(() => overlay.classList.add('show')));

  const bindPathRadios = overlay.querySelectorAll('input[name="retryBindPath"]');
  bindPathRadios.forEach(r => r.addEventListener('change', () => {
    const isBilling = overlay.querySelector('input[name="retryBindPath"]:checked').value === 'billing';
    overlay.querySelector('#retryOnboardingAmt').style.display = isBilling ? 'none' : 'block';
    overlay.querySelector('#retryBillingAmt').style.display = isBilling ? 'block' : 'none';
    updateRetryPreview();
  }));

  overlay.querySelectorAll('input[name="retryRechargeStrategy"]').forEach(r => {
    r.addEventListener('change', () => {
      const isAuto = overlay.querySelector('input[name="retryRechargeStrategy"]:checked').value === 'auto';
      overlay.querySelector('#retryTierRow').style.display = isAuto ? 'flex' : 'none';
      updateRetryPreview();
    });
  });

  const updateRetryPreview = () => {
    const isBilling = overlay.querySelector('input[name="retryBindPath"]:checked').value === 'billing';
    const base = isBilling
      ? Math.max(5, parseInt(overlay.querySelector('#retryCreditInput').value) || 10)
      : parseInt(overlay.querySelector('input[name="retryCreditAmt"]:checked')?.value) || 5;
    const strategy = overlay.querySelector('input[name="retryRechargeStrategy"]:checked')?.value || 'none';
    if (strategy === 'auto') {
      overlay.querySelector('#retryTotalPreview').textContent = `首充 $${base} → 追充至 $100/卡`;
    } else {
      overlay.querySelector('#retryTotalPreview').textContent = `$${base}/卡`;
    }
  };
  overlay.querySelectorAll('input[name="retryCreditAmt"]').forEach(r => r.addEventListener('change', updateRetryPreview));
  overlay.querySelector('#retryCreditInput')?.addEventListener('input', updateRetryPreview);
  updateRetryPreview();

  const close = () => { overlay.classList.remove('show'); setTimeout(() => overlay.remove(), 220); };
  overlay.querySelector('#retryCancelBtn').onclick = close;
  overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

  overlay.querySelector('#retryConfirmBtn').addEventListener('click', async () => {
    const bindPath = overlay.querySelector('input[name="retryBindPath"]:checked').value;
    const isBilling = bindPath === 'billing';
    const creditAmount = isBilling
      ? Math.max(5, Math.min(100, parseInt(overlay.querySelector('#retryCreditInput').value) || 10))
      : parseInt(overlay.querySelector('input[name="retryCreditAmt"]:checked')?.value) || 5;
    const rechargeStrategy = overlay.querySelector('input[name="retryRechargeStrategy"]:checked')?.value || 'none';
    const rechargeUpper = parseInt(overlay.querySelector('#retryRechargeUpper').value) || 20;
    const rechargeLower = parseInt(overlay.querySelector('#retryRechargeLower').value) || 5;
    const proxyType = overlay.querySelector('#retryProxyType').value;
    const concurrency = Math.max(1, Math.min(10, parseInt(overlay.querySelector('#retryConcurrency').value) || 1));

    const btn = $('#btnRetryBind');
    btn.disabled = true;
    btn.textContent = '重试中...';

    try {
      if (proxyType === 'pool') {
        const { proxies } = await api.proxies.list();
        const available = (proxies || []).some(p => p.status !== PROXY_STATUS.DISABLED);
        if (!available) {
          showToast('当前没有可用代理，请先启用代理或切换为直连', 'warning');
          btn.disabled = false;
          btn.textContent = '重试绑定';
          return;
        }
      }

      close();
      const { task_id, count } = await api.cards.retryBind({
        card_ids: ids, proxy_type: proxyType, bind_path: bindPath,
        credit_amount: creditAmount, recharge_strategy: rechargeStrategy,
        recharge_upper: rechargeUpper, recharge_lower: rechargeLower, concurrency,
      });

      showToast(`已启动重试任务 (${count} 张卡)`, 'info');
      retryBindTaskId = task_id;
      retryBindPaused = false;
      retryBindPausePending = false;
      renderRetryBindControl();

      listenSSE(`/api/cards/retry-bind/${task_id}/stream`, {
        onSubDone(d) {
          const creditInfo = d.credit_loaded ? ` · $${d.credit_loaded}` : '';
          showToast(`卡 #${d.card_id} 绑定成功: ${d.email}${creditInfo}`, 'success');
        },
        onSubError(d) {
          const tagInfo = d.fail_tag ? ` [${d.fail_tag}]` : '';
          showToast(`卡 #${d.card_id} 重试失败${tagInfo}: ${d.message}`, 'error');
        },
        onPauseRequested(d) {
          retryBindPausePending = true;
          renderRetryBindControl();
          showToast(`重绑暂停中：${d.parked_flows || 0}/${d.active_flows || 0}`, 'info');
        },
        onPauseProgress(d) {
          retryBindPausePending = true;
          renderRetryBindControl();
        },
        onPaused() {
          retryBindPaused = true;
          retryBindPausePending = false;
          renderRetryBindControl();
          showToast('重绑任务已暂停', 'info');
        },
        onResumed() {
          retryBindPaused = false;
          retryBindPausePending = false;
          renderRetryBindControl();
          showToast('重绑任务已恢复', 'success');
        },
        onCancelled() {
          removeRetryBindControl();
          btn.disabled = false;
          btn.textContent = '重试绑定';
          loadCards();
          loadDashboard();
          showToast('重绑任务已停止', 'warning');
        },
        onDone(d) {
          removeRetryBindControl();
          const r = d.result || {};
          showToast(`重试完成: 成功 ${r.success || 0}, 失败 ${r.failed || 0}`, r.failed ? 'warning' : 'success', 5000);
          btn.disabled = false;
          btn.textContent = '重试绑定';
          loadCards();
          loadDashboard();
        },
        onError(d) {
          removeRetryBindControl();
          showToast(`重试任务失败: ${d.message}`, 'error');
          btn.disabled = false;
          btn.textContent = '重试绑定';
        },
      });
    } catch (e) {
      showToast(`启动重试失败: ${e.message}`, 'error');
      btn.disabled = false;
      btn.textContent = '重试绑定';
    }
  });
}

// ─── Preview table ───────────────────────────────────────

function pvValidateNumber(num) {
  const d = num.replace(/\D/g, '');
  if (d.length < 13 || d.length > 19) return 'invalid';
  if (allCards.find(c => c.number === d)) return 'duplicate';
  return 'ok';
}
function pvValidateExp(exp) {
  const m = exp.match(/^(\d{1,2})\s*\/\s*(\d{2,4})$/);
  if (!m) return exp.trim() ? 'invalid' : 'empty';
  const month = parseInt(m[1]);
  return (month < 1 || month > 12) ? 'invalid' : 'ok';
}
function pvValidateCvv(cvv) { return /^\d{3,4}$/.test(cvv.trim()) ? 'ok' : 'invalid'; }

function pvRowStatus(row) {
  const numSt = pvValidateNumber(row.number);
  if (numSt === 'duplicate') return 'duplicate';
  if (numSt === 'invalid') return 'invalid';
  if (pvValidateCvv(row.cvv) !== 'ok') return 'invalid';
  if (pvValidateExp(row.exp) === 'invalid') return 'warn';
  return 'ok';
}

function pvStatusBadge(status) {
  const map = { ok: '<span class="pv-badge pv-ok">有效</span>', warn: '<span class="pv-badge pv-warn">待确认</span>', invalid: '<span class="pv-badge pv-invalid">无效</span>', duplicate: '<span class="pv-badge pv-dup">重复</span>', fail: '<span class="pv-badge pv-invalid">解析失败</span>' };
  return map[status] || '';
}

async function parsePreview() {
  const raw = $('#cardBatch').value.trim();
  if (!raw) return;
  const { results } = await api.cards.preview(raw);
  pvRows = results.map((r, i) => {
    if (r.status === 'ok') {
      const exp = r.exp_month && r.exp_year ? `${r.exp_month}/${r.exp_year}` : '';
      return { idx: i, checked: true, number: r.number, exp, cvv: r.cvv, holder: r.holder_name || '', raw: null };
    }
    return { idx: i, checked: false, number: '', exp: '', cvv: '', holder: '', raw: r.raw || '' };
  });
  renderPreviewTable();
}

function renderPreviewTable() {
  const body = $('#previewBody');
  const wrap = $('#previewWrap');
  const status = $('#parseStatus');
  if (!pvRows.length) { wrap.style.display = 'none'; return; }
  wrap.style.display = 'block';
  let okCount = 0, failCount = 0, dupCount = 0;

  body.innerHTML = pvRows.map((r, i) => {
    const isFail = r.raw !== null && !r.number;
    const rowStatus = isFail ? 'fail' : pvRowStatus(r);
    if (rowStatus === 'ok' || rowStatus === 'warn') okCount++;
    else if (rowStatus === 'duplicate') dupCount++;
    else failCount++;
    const checked = r.checked ? 'checked' : '';
    const dimClass = !r.checked ? ' pv-row-dim' : '';
    const numClass = pvValidateNumber(r.number);
    const expClass = pvValidateExp(r.exp);
    const cvvClass = pvValidateCvv(r.cvv);

    if (isFail) {
      return `<tr data-pv-idx="${i}" class="pv-fail-editable${dimClass}">
        <td><input type="checkbox" class="row-check pv-check" data-pv-idx="${i}" ${checked}></td>
        <td class="pv-idx">${i + 1}</td>
        <td><input class="pv-input pv-v-invalid" data-field="number" data-pv-idx="${i}" value="" placeholder="卡号"></td>
        <td><input class="pv-input" data-field="exp" data-pv-idx="${i}" value="" placeholder="MM/YY"></td>
        <td><input class="pv-input" data-field="cvv" data-pv-idx="${i}" value="" placeholder="CVV"></td>
        <td><input class="pv-input" data-field="holder" data-pv-idx="${i}" value="" placeholder="姓名"></td>
        <td>${pvStatusBadge('fail')}</td>
        <td><button class="btn-sm danger" onclick="pvRemoveRow(${i})">×</button></td>
      </tr>`;
    }
    return `<tr data-pv-idx="${i}" class="${dimClass}">
      <td><input type="checkbox" class="row-check pv-check" data-pv-idx="${i}" ${checked}></td>
      <td class="pv-idx">${i + 1}</td>
      <td><input class="pv-input ${numClass === 'ok' ? '' : numClass === 'duplicate' ? 'pv-v-dup' : 'pv-v-invalid'}" data-field="number" data-pv-idx="${i}" value="${escAttr(r.number)}"></td>
      <td><input class="pv-input ${expClass === 'invalid' ? 'pv-v-invalid' : ''}" data-field="exp" data-pv-idx="${i}" value="${escAttr(r.exp)}"></td>
      <td><input class="pv-input ${cvvClass === 'invalid' ? 'pv-v-invalid' : ''}" data-field="cvv" data-pv-idx="${i}" value="${escAttr(r.cvv)}"></td>
      <td><input class="pv-input" data-field="holder" data-pv-idx="${i}" value="${escAttr(r.holder)}"></td>
      <td>${pvStatusBadge(rowStatus)}</td>
      <td><button class="btn-sm danger" onclick="pvRemoveRow(${i})">×</button></td>
    </tr>`;
  }).join('');

  body.querySelectorAll('.pv-check').forEach(cb => {
    cb.addEventListener('change', () => { pvRows[+cb.dataset.pvIdx].checked = cb.checked; cb.closest('tr').classList.toggle('pv-row-dim', !cb.checked); updatePvSummary(); });
  });
  body.querySelectorAll('.pv-input').forEach(inp => {
    inp.addEventListener('input', () => {
      const idx = +inp.dataset.pvIdx;
      const field = inp.dataset.field;
      const val = inp.value.trim();
      if (field === 'number') { pvRows[idx].number = val; pvRows[idx].raw = null; const st = pvValidateNumber(val); inp.className = `pv-input ${st === 'ok' ? '' : st === 'duplicate' ? 'pv-v-dup' : 'pv-v-invalid'}`; }
      else if (field === 'exp') { pvRows[idx].exp = val; const st = pvValidateExp(val); inp.className = `pv-input ${st === 'invalid' ? 'pv-v-invalid' : ''}`; }
      else if (field === 'cvv') { pvRows[idx].cvv = val; const st = pvValidateCvv(val); inp.className = `pv-input ${st === 'invalid' ? 'pv-v-invalid' : ''}`; }
      else if (field === 'holder') { pvRows[idx].holder = val; }
      const tr = inp.closest('tr');
      const statusCell = tr.querySelectorAll('td')[6];
      const isFail = pvRows[idx].raw !== null && !pvRows[idx].number;
      statusCell.innerHTML = pvStatusBadge(isFail ? 'fail' : pvRowStatus(pvRows[idx]));
      updatePvSummary();
    });
  });
  updatePvSummary();
  let msg = `解析 ${okCount} 张有效`;
  if (dupCount) msg += `，${dupCount} 张重复`;
  if (failCount) msg += `，${failCount} 行需修正`;
  status.textContent = msg;
  status.className = 'import-result ' + (failCount || dupCount ? 'warn' : 'ok');
  status.style.display = 'inline';
}

function updatePvSummary() {
  const checkedValid = pvRows.filter(r => { if (!r.checked) return false; const isFail = r.raw !== null && !r.number; if (isFail) return false; const st = pvRowStatus(r); return st === 'ok' || st === 'warn'; }).length;
  const checkedTotal = pvRows.filter(r => r.checked).length;
  $('#btnConfirmImport').textContent = `确认导入 (${checkedValid}张)`;
  $('#btnConfirmImport').disabled = checkedValid === 0;
  $('#pvToolbarInfo').textContent = `${checkedTotal} 选中 / ${pvRows.length} 总计`;
  $('#pvCheckAll').checked = pvRows.length > 0 && pvRows.every(r => r.checked);
}

function pvRemoveRow(idx) {
  pvRows.splice(idx, 1);
  if (!pvRows.length) { $('#previewWrap').style.display = 'none'; $('#parseStatus').style.display = 'none'; return; }
  renderPreviewTable();
}

function pvSelectValid() {
  pvRows.forEach(r => { const isFail = r.raw !== null && !r.number; const st = isFail ? 'fail' : pvRowStatus(r); r.checked = (st === 'ok' || st === 'warn'); });
  renderPreviewTable();
}

async function pvRetryFailed() {
  const failRaws = pvRows.filter(r => r.raw !== null && !r.number).map(r => r.raw);
  if (!failRaws.length) return;
  const { results } = await api.cards.preview(failRaws.join('\n'));
  let ri = 0;
  pvRows.forEach(r => {
    if (r.raw !== null && !r.number && ri < results.length) {
      const p = results[ri++];
      if (p.status === 'ok') { r.number = p.number; r.exp = p.exp_month && p.exp_year ? `${p.exp_month}/${p.exp_year}` : ''; r.cvv = p.cvv; r.holder = p.holder_name || ''; r.raw = null; r.checked = true; }
    }
  });
  renderPreviewTable();
}

async function confirmImport() {
  const autoAssign = $('#addrAutoAssign').checked;
  const addr = autoAssign ? currentAddress : null;
  const cardsArr = [];
  pvRows.forEach(r => {
    if (!r.checked) return;
    const isFail = r.raw !== null && !r.number;
    if (isFail) return;
    const st = pvRowStatus(r);
    if (st !== 'ok' && st !== 'warn') return;
    const number = r.number.replace(/\D/g, '');
    let exp_month = '', exp_year = '';
    const m = r.exp.match(/(\d{1,2})\s*\/\s*(\d{2,4})/);
    if (m) { exp_month = m[1].padStart(2, '0'); exp_year = m[2].length === 2 ? '20' + m[2] : m[2]; }
    const card = { number, exp_month, exp_year, cvv: r.cvv.trim(), holder_name: r.holder.trim() };
    if (addr) { card.address_line1 = addr.address_line1 || ''; card.city = addr.city || ''; card.state = addr.state || ''; card.zip = addr.zip || ''; card.country = 'US'; }
    if (number && card.cvv) cardsArr.push(card);
  });
  if (!cardsArr.length) return;
  const btn = $('#btnConfirmImport');
  btn.disabled = true;
  btn.textContent = '导入中...';
  try {
    const data = await api.cards.saveBatch(cardsArr);
    $('#previewWrap').style.display = 'none';
    pvRows = [];
    $('#cardBatch').value = '';
    $('#parseStatus').style.display = 'none';
    const skipMsg = data.skipped ? `，${data.skipped} 张重复已跳过` : '';
    showToast(`成功导入 ${data.imported} 张卡${skipMsg}`, data.skipped ? 'warning' : 'success');
    $('#cardImportDrawer').classList.remove('open');
    loadCards();
    loadDashboard();
  } catch (e) {
    btn.disabled = false;
    btn.textContent = '重试导入';
    showToast(`导入失败: ${e.message}`, 'error');
  }
}

async function manualAddCard() {
  const number = $('#manualNumber').value.trim();
  const expRaw = $('#manualExp').value.trim();
  const cvv = $('#manualCvv').value.trim();
  const holder = $('#manualHolder').value.trim();
  if (!number || !cvv) return;
  let exp_month = '', exp_year = '';
  const m = expRaw.match(/(\d{1,2})\s*\/\s*(\d{2,4})/);
  if (m) { exp_month = m[1].padStart(2, '0'); exp_year = m[2].length === 2 ? '20' + m[2] : m[2]; }
  const body = { number, exp_month, exp_year, cvv, holder_name: holder };
  const autoAssign = $('#addrAutoAssign').checked;
  if (autoAssign && currentAddress) { body.address_line1 = currentAddress.address_line1 || ''; body.city = currentAddress.city || ''; body.state = currentAddress.state || ''; body.zip = currentAddress.zip || ''; body.country = 'US'; }
  await api.cards.add(body);
  $('#manualNumber').value = ''; $('#manualExp').value = ''; $('#manualCvv').value = ''; $('#manualHolder').value = '';
  showToast('卡片已添加', 'success');
  $('#cardImportDrawer').classList.remove('open');
  loadCards();
}

import { $, showToast, fmtTime } from './utils.js';
import * as api from './api.js';
import { ACCOUNT_STATUS, CARD_FAIL_TAG, CARD_STATUS, PROXY_STATUS, SALE_STATUS } from './constants.js';

export function initDashboard() {
  loadDashboard();
  $('#btnRefreshDash')?.addEventListener('click', loadDashboard);
}

export async function loadDashboard() {
  try {
    const stats = await api.dashboard.stats();
    renderDashboard(stats);
  } catch (e) {
    showToast('看板数据加载失败', 'error');
  }
}

function pct(n, total) { return total ? Math.round((n / total) * 100) + '%' : '0%'; }

const TYPE_LABEL = {
  register: '注册',
  retry_bind: '重绑',
  key_check: '测活',
  proxy_check: '代理检测',
};

const STATUS_BADGE = {
  running: '<span class="badge badge-info">运行中</span>',
  success: '<span class="badge badge-ok">成功</span>',
  failed: '<span class="badge badge-fail">失败</span>',
  cancelled: '<span class="badge badge-warn">已取消</span>',
};

function renderDashboard(s) {
  const a = s.accounts || {};
  const c = s.cards || {};
  const p = s.proxies || {};
  const sale = a.by_sale_status || {};
  const st = a.by_status || {};
  const cst = c.by_status || {};
  const ctag = c.by_fail_tag || {};

  $('#dashTotal').textContent = a.total || 0;
  $('#dashToday').textContent = a.today || 0;
  $('#dashUnsold').textContent = sale[SALE_STATUS.UNSOLD] || 0;
  $('#dashSold').textContent = sale[SALE_STATUS.SOLD] || 0;
  $('#dashRecycled').textContent = sale[SALE_STATUS.RECYCLED] || 0;
  $('#dashActive').textContent = st[ACCOUNT_STATUS.ACTIVE] || 0;
  $('#dashDead').textContent = st[ACCOUNT_STATUS.DEAD] || 0;

  const cardTotal = c.total || 0;
  const cardOk = cst[CARD_STATUS.AVAILABLE] || 0;
  const failTotal = (ctag[CARD_FAIL_TAG.DECLINE] || 0) + (ctag[CARD_FAIL_TAG.THREE_DS] || 0) + (ctag[CARD_FAIL_TAG.INSUFFICIENT] || 0);
  $('#dashCards').textContent = cardTotal;
  $('#dashCardsOk').textContent = cardOk;
  $('#dashCardsFail').textContent = failTotal;
  $('#dashCardsBarOk').style.width = pct(cardOk, cardTotal);
  $('#dashCardsBarFail').style.width = pct(failTotal, cardTotal);

  const proxyTotal = p.total || 0;
  const ps = p.by_status || {};
  const proxyAvail = ps[PROXY_STATUS.AVAILABLE] || 0;
  const proxyDis = ps[PROXY_STATUS.DISABLED] || 0;
  $('#dashProxies').textContent = proxyTotal;
  $('#dashProxyAvail').textContent = proxyAvail;
  $('#dashProxyDisabled').textContent = proxyDis;
  $('#dashProxyBarOk').style.width = pct(proxyAvail, proxyTotal);
  $('#dashProxyBarFail').style.width = pct(proxyDis, proxyTotal);

  const grid = $('#dashFailDetail');
  if (grid) {
    grid.innerHTML = `
      <span class="dash-fail-tag decline">Decline: ${ctag[CARD_FAIL_TAG.DECLINE] || 0}</span>
      <span class="dash-fail-tag tds">3DS: ${ctag[CARD_FAIL_TAG.THREE_DS] || 0}</span>
      <span class="dash-fail-tag insufficient">Insufficient: ${ctag[CARD_FAIL_TAG.INSUFFICIENT] || 0}</span>
    `;
  }

  // ── 最近任务面板 ──
  const tasks = s.recent_tasks || [];
  const panel = $('#dashRecentTasks');
  if (panel) {
    if (!tasks.length) {
      panel.innerHTML = '<div class="empty-hint">暂无任务记录</div>';
      return;
    }
    let html = `<table class="table table-sm"><thead><tr>
      <th>类型</th><th>状态</th><th>成功</th><th>失败</th><th>开始时间</th><th>耗时</th>
    </tr></thead><tbody>`;
    for (const t of tasks) {
      const label = TYPE_LABEL[t.type] || t.type;
      const badge = STATUS_BADGE[t.status] || t.status;
      const dur = t.finished_at && t.started_at
        ? formatDuration(t.started_at, t.finished_at)
        : (t.status === 'running' ? '进行中' : '-');
      html += `<tr>
        <td>${label}</td>
        <td>${badge}</td>
        <td>${t.success_count}</td>
        <td>${t.failed_count}</td>
        <td>${fmtTime(t.started_at)}</td>
        <td>${dur}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    panel.innerHTML = html;
  }
}

function formatDuration(start, end) {
  try {
    const ms = new Date(end).getTime() - new Date(start).getTime();
    if (ms < 0) return '-';
    const s = Math.round(ms / 1000);
    if (s < 60) return `${s}秒`;
    const m = Math.floor(s / 60);
    const rs = s % 60;
    if (m < 60) return `${m}分${rs}秒`;
    return `${Math.floor(m / 60)}时${m % 60}分`;
  } catch {
    return '-';
  }
}

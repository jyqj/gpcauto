import { API } from './utils.js';

const json = "application/json";
const headers = { "Content-Type": json };

async function handleResponse(r) {
  if (!r.ok) {
    const body = await r.json().catch(() => ({}));
    throw new Error(body.detail || body.error || `HTTP ${r.status}`);
  }
  return r.json();
}

function post(url, body) {
  return fetch(`${API}${url}`, { method: "POST", headers, body: JSON.stringify(body) }).then(handleResponse);
}
function get(url) {
  return fetch(`${API}${url}`).then(handleResponse);
}
function del(url) {
  return fetch(`${API}${url}`, { method: "DELETE" }).then(handleResponse);
}
function put(url, body) {
  return fetch(`${API}${url}`, { method: "PUT", headers, body: JSON.stringify(body) }).then(handleResponse);
}

/** 构造查询字符串 */
function qs(params) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') p.set(k, v);
  }
  const s = p.toString();
  return s ? `?${s}` : '';
}

export const dashboard = {
  stats: () => get("/api/dashboard/stats"),
};

export const tasks = {
  history: (params = {}) => get(`/api/tasks/history${qs(params)}`),
};

export const auditLogs = {
  list: (params = {}) => get(`/api/audit-logs${qs(params)}`),
};

export const accounts = {
  list: (params = {}) => get(`/api/accounts${qs(params)}`),
  update: (id, fields) =>
    fetch(`${API}/api/accounts/${id}`, { method: "PATCH", headers, body: JSON.stringify(fields) }).then(handleResponse),
  batchDelete: (ids) => post("/api/accounts/batch-delete", { ids }),
  batchUpdate: (ids, fields) => post("/api/accounts/batch-update", { ids, ...fields }),
  sell: (ids, format = "txt") =>
    fetch(`${API}/api/accounts/sell`, { method: "POST", headers, body: JSON.stringify({ ids, format }) }),
  sellConfirm: (token) => post("/api/accounts/sell/confirm", { token }),
  cleanupProfiles: () => post("/api/accounts/cleanup-profiles", {}),
};

export const keys = {
  check: (body) => post("/api/keys/check", body),
};

export const cards = {
  list: (params = {}) => get(`/api/cards${qs(params)}`),
  add: (card) => post("/api/cards", card),
  remove: (id) => del(`/api/cards/${id}`),
  preview: (raw) => post("/api/cards/preview", { raw }),
  saveBatch: (cardsArr) => post("/api/cards/save-batch", { cards: cardsArr }),
  batchDelete: (ids) => post("/api/cards/batch-delete", { ids }),
  batchStatus: (ids, status) => post("/api/cards/batch-status", { ids, status }),
  batchFailTag: (ids, fail_tag) => post("/api/cards/batch-fail-tag", { ids, fail_tag }),
  batchAddress: (ids, addr) => post("/api/cards/batch-address", { ids, ...addr }),
  retryBind: (body) => post("/api/cards/retry-bind", body),
  retryBindCancel: (taskId) => post(`/api/cards/retry-bind/${taskId}/cancel`, {}),
  retryBindPause: (taskId) => post(`/api/cards/retry-bind/${taskId}/pause`, {}),
  retryBindResume: (taskId) => post(`/api/cards/retry-bind/${taskId}/resume`, {}),
};

export const addresses = {
  states: (taxFreeOnly) => get(`/api/addresses/states?tax_free_only=${taxFreeOnly}`),
  random: (params) => {
    const p = new URLSearchParams();
    if (params.state) p.set("state", params.state);
    if (params.zip) p.set("zip", params.zip);
    if (params.tax_free_only) p.set("tax_free_only", "true");
    return get(`/api/addresses/random?${p}`);
  },
  rerollStreet: () => get("/api/addresses/reroll-street"),
};

export const proxies = {
  list: (params = {}) => get(`/api/proxies${qs(params)}`),
  add: (proxy) => post("/api/proxies", proxy),
  remove: (id) => del(`/api/proxies/${id}`),
  batchImport: (raw, defaultType) => post("/api/proxies/batch", { raw, default_type: defaultType }),
  batchDelete: (ids) => post("/api/proxies/batch-delete", { ids }),
  batchStatus: (ids, status) => post("/api/proxies/batch-status", { ids, status }),
  check: (body) => post("/api/proxies/check", body),
  resetAll: () => post("/api/proxies/reset-all", {}),
};

export const settings = {
  get: () => get("/api/settings"),
  update: (body) => put("/api/settings", body),
};

export const register = {
  start: (body) => post("/api/register", body),
  cancel: (taskId) => post(`/api/register/${taskId}/cancel`, {}),
  pause: (taskId) => post(`/api/register/${taskId}/pause`, {}),
  resume: (taskId) => post(`/api/register/${taskId}/resume`, {}),
  manualResult: (taskId, body) => post(`/api/register/${taskId}/manual-result`, body),
};

export const API = window.location.origin;
export const $ = (s) => document.querySelector(s);
export const $$ = (s) => document.querySelectorAll(s);

export function esc(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

export function escAttr(s) {
  return String(s).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export function ts() {
  return new Date().toLocaleTimeString("zh-CN", { hour12: false });
}

export function fmtTime(t) {
  return (t || "").replace("T", " ").replace("Z", "").substring(0, 16);
}

// ─── Toast ───────────────────────────────────────────────

const TOAST_ICONS = { success: "✓", error: "✕", warning: "!", info: "i" };

export function showToast(message, type = "info", duration = 3000) {
  const container = document.getElementById("toastContainer");
  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `<span class="toast-icon">${TOAST_ICONS[type] || "i"}</span><span>${esc(message)}</span>`;
  container.appendChild(toast);
  requestAnimationFrame(() => requestAnimationFrame(() => toast.classList.add("show")));
  setTimeout(() => {
    toast.classList.remove("show");
    setTimeout(() => toast.remove(), 350);
  }, duration);
}

// ─── Confirm dialog ──────────────────────────────────────

export function showConfirm(message, title = "确认操作") {
  return new Promise((resolve) => {
    const overlay = document.createElement("div");
    overlay.className = "modal-overlay";
    overlay.innerHTML = `
      <div class="modal-card">
        <div class="modal-title">${esc(title)}</div>
        <div class="modal-message">${esc(message)}</div>
        <div class="modal-actions">
          <button class="modal-btn modal-btn-cancel">取消</button>
          <button class="modal-btn modal-btn-confirm">确认</button>
        </div>
      </div>`;
    document.body.appendChild(overlay);
    requestAnimationFrame(() => requestAnimationFrame(() => overlay.classList.add("show")));
    const close = (result) => {
      overlay.classList.remove("show");
      setTimeout(() => overlay.remove(), 220);
      resolve(result);
    };
    overlay.querySelector(".modal-btn-cancel").onclick = () => close(false);
    overlay.querySelector(".modal-btn-confirm").onclick = () => close(true);
    overlay.addEventListener("click", (e) => { if (e.target === overlay) close(false); });
  });
}

// ─── SSE with auto-reconnect ─────────────────────────────

export function listenSSE(url, h, { maxRetries = 3, retryDelay = 2000 } = {}) {
  let retries = 0;

  function connect() {
    const es = new EventSource(`${API}${url}`);
    es.onmessage = (e) => {
      retries = 0;
      const d = JSON.parse(e.data);
      if (d.type === "step" && h.onStep) h.onStep(d);
      else if (d.type === "check_progress" && h.onCheckProgress) h.onCheckProgress(d);
      else if (d.type === "proxy_checked" && h.onProxyChecked) h.onProxyChecked(d);
      else if (d.type === "sub_done" && h.onSubDone) h.onSubDone(d);
      else if (d.type === "sub_error" && h.onSubError) h.onSubError(d);
      else if (d.type === "pause_requested" && h.onPauseRequested) h.onPauseRequested(d);
      else if (d.type === "pause_progress" && h.onPauseProgress) h.onPauseProgress(d);
      else if (d.type === "paused" && h.onPaused) h.onPaused(d);
      else if (d.type === "resumed" && h.onResumed) h.onResumed(d);
      else if (d.type === "awaiting_manual" && h.onAwaitingManual) h.onAwaitingManual(d);
      else if (d.type === "browser_closed" && h.onBrowserClosed) h.onBrowserClosed(d);
      else if (d.type === "cancelled") { if (h.onCancelled) h.onCancelled(d); es.close(); }
      else if (d.type === "done") { if (h.onDone) h.onDone(d); es.close(); }
      else if (d.type === "error") { if (h.onError) h.onError(d); es.close(); }
    };
    es.onerror = () => {
      es.close();
      if (retries < maxRetries) {
        retries++;
        setTimeout(connect, retryDelay * retries);
      } else if (h.onError) {
        h.onError({ message: "SSE 连接断开，重试失败" });
      }
    };
    return es;
  }

  return connect();
}

// ─── Copy to clipboard ──────────────────────────────────

export async function copyText(t) {
  if (!t) return;
  try {
    await navigator.clipboard.writeText(t);
    showToast("已复制到剪贴板", "success", 2000);
  } catch {
    showToast("复制失败", "error", 2000);
  }
}
window.copyText = copyText;

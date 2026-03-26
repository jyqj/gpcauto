"""FastAPI 服务 — 注册主流程 + 生命周期 + 路由装配"""

import json
import logging
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel, Field, field_validator, model_validator

from . import db, adspower, key_checker
from . import register as reg_module
from .constants import (
    CARD_STATUS_DISABLED,
    ACCOUNT_STATUS_ACTIVE, ACCOUNT_STATUS_DEAD, ACCOUNT_STATUS_INVALID,
)

from .services.task_manager import (
    TaskState, tasks, tasks_lock,
    TASK_FINAL_STATES, TASK_TTL, TASK_MAX_AGE,
    _check_task_limit, _get_task, _register_task, _finish_task,
    _flow_check, _sse_generator,
    start_managed_thread, wait_managed_threads,
)
from .services.proxy_service import (
    _resolve_proxy, _cleanup_proxy_caches,
)
from .common import JSONDict, dump_model

from .routes import dashboard, settings, addresses, accounts, cards, proxies

def _setup_logging() -> None:
    """配置日志：stdout + 文件轮转。"""
    from logging.handlers import RotatingFileHandler
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # stdout
    if not root.handlers:
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)

    # 文件轮转
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)


_setup_logging()
log = logging.getLogger("server")


# ─── Resource-level locks ─────────────────────────────────

class _ResourceLockManager:
    """Per-resource locks to prevent concurrent operations on the same entity.

    Prevents conflicts such as two tasks recharging the same account,
    or a recycle loop deleting an account that is being used by a task.
    """

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._meta = threading.Lock()

    def _get(self, key: str) -> threading.Lock:
        with self._meta:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    @contextmanager
    def lock(self, resource_type: str, resource_id, timeout: float = 300):
        key = f"{resource_type}:{resource_id}"
        lk = self._get(key)
        if not lk.acquire(timeout=timeout):
            raise RuntimeError(f"资源 {key} 被其他操作占用")
        try:
            yield
        finally:
            lk.release()

    @contextmanager
    def try_lock(self, resource_type: str, resource_id):
        """Non-blocking acquire. Yields True if acquired, False if busy."""
        key = f"{resource_type}:{resource_id}"
        lk = self._get(key)
        acquired = lk.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                lk.release()

    def try_acquire(self, resource_type: str, resource_id) -> bool:
        """Non-blocking acquire. Returns True if acquired (caller must call release)."""
        return self._get(f"{resource_type}:{resource_id}").acquire(blocking=False)

    def release(self, resource_type: str, resource_id) -> None:
        try:
            self._get(f"{resource_type}:{resource_id}").release()
        except RuntimeError:
            pass

    def cleanup_idle(self) -> int:
        """Remove locks that are not currently held."""
        with self._meta:
            idle = [k for k, lk in self._locks.items() if not lk.locked()]
            for k in idle:
                del self._locks[k]
            return len(idle)


_resource_locks = _ResourceLockManager()
_shutdown_event = threading.Event()


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    db.init_db()
    bg_threads = [
        threading.Thread(target=_recycle_loop, daemon=True, name="recycle"),
        threading.Thread(target=_task_cleanup_loop, daemon=True, name="task_cleanup"),
        threading.Thread(target=_pending_sale_rollback_loop, daemon=True, name="pending_sale_rollback"),
        threading.Thread(target=_profile_cleanup_loop, daemon=True, name="profile_cleanup"),
    ]
    for t in bg_threads:
        t.start()

    dashboard.set_start_time(time.monotonic())

    yield

    # ── 优雅关停 ──
    log.info("服务关停中...")
    _shutdown_event.set()

    # 1. 取消所有运行中任务
    with tasks_lock:
        running = [t for t in tasks.values() if t.status not in TASK_FINAL_STATES]
    cancelled_count = 0
    for t in running:
        try:
            t.cancel()
            cancelled_count += 1
        except Exception:
            pass
    if cancelled_count:
        log.info(f"已取消 {cancelled_count} 个运行中任务")

    # 2. 等待业务任务线程退出（给 15 秒）
    remaining_workers = wait_managed_threads(timeout=15)
    if remaining_workers:
        log.warning(f"仍有 {remaining_workers} 个业务任务线程未在超时内退出")
    else:
        log.info("业务任务线程已全部退出")

    # 3. 等待后台维护线程退出（给 15 秒）
    deadline = time.time() + 15
    for t in bg_threads:
        remaining = max(0.1, deadline - time.time())
        t.join(timeout=remaining)

    # 4. 执行一轮 profile cleanup queue
    try:
        pending = db.list_pending_cleanups(limit=50)
        cleaned = 0
        for item in pending:
            try:
                adspower.stop_browser(item["profile_id"])
            except Exception:
                pass
            try:
                adspower.delete_profile(item["profile_id"])
                db.mark_cleanup_done(item["id"])
                if item.get("account_id"):
                    db.update_account(item["account_id"], ads_profile_id="")
                cleaned += 1
            except Exception:
                pass
        if cleaned:
            log.info(f"关停清理: 成功清理 {cleaned} 个残留 profile")
    except Exception as e:
        log.warning(f"关停清理异常: {e}")

    log.info("服务已关停")


app = FastAPI(title="GPT Platform", lifespan=_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── 路由装配 ──
accounts.set_resource_locks(_resource_locks)
app.include_router(dashboard.router)
app.include_router(settings.router)
app.include_router(addresses.router)
app.include_router(accounts.router)
app.include_router(cards.router)
app.include_router(proxies.router)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.exception(f"Unhandled error on {request.method} {request.url.path}")
    return JSONResponse(
        status_code=500,
        content={"error": "internal server error"},
    )


# ─── Auto-recycle scheduler ──────────────────────────────

def _recycle_loop() -> None:
    """Every hour, check sold accounts older than 3 days → test key → keep/delete."""
    _shutdown_event.wait(30)
    while not _shutdown_event.is_set():
        try:
            stale = db.get_stale_sold_accounts(days=3)
            for acct in stale:
                with _resource_locks.try_lock("account", acct["id"]) as acquired:
                    if not acquired:
                        log.info(f"Recycle: skipping {acct['email']} (locked by another operation)")
                        continue

                    key = acct.get("api_key", "")
                    status = key_checker.check_key(key) if key.startswith("sk-") else ACCOUNT_STATUS_DEAD

                    if status == ACCOUNT_STATUS_ACTIVE:
                        db.mark_account_recycled(acct["id"])
                        log.info(f"Recycled (alive): {acct['email']}")
                    elif status in (ACCOUNT_STATUS_DEAD, ACCOUNT_STATUS_INVALID):
                        pid = acct.get("ads_profile_id")
                        if pid:
                            if not _cleanup_ads_profile(pid, account_id=acct["id"], reason="recycle"):
                                log.warning(f"Recycle: profile 删除失败已入队，跳过 DB 删除 {acct['email']}")
                                continue
                        db.delete_accounts([acct["id"]])
                        log.info(f"Recycled ({status}, deleted): {acct['email']}")
                    else:
                        log.warning(f"Recycle: skipping {acct['email']} "
                                    f"(status={status}), will retry next cycle")
        except Exception as e:
            log.error(f"Recycle worker error: {e}")
        _shutdown_event.wait(3600)




def _task_cleanup_loop() -> None:
    """Periodically remove completed tasks older than TASK_TTL,
    and force-clean any task older than TASK_MAX_AGE regardless of state."""
    while not _shutdown_event.is_set():
        _shutdown_event.wait(300)
        if _shutdown_event.is_set():
            break
        now = time.time()
        with tasks_lock:
            stale = [tid for tid, t in tasks.items()
                     if (t.status in TASK_FINAL_STATES and now - t.created_at > TASK_TTL)
                     or (now - t.created_at > TASK_MAX_AGE)]
            for tid in stale:
                t = tasks[tid]
                if t.status not in TASK_FINAL_STATES:
                    t.cancel()
                    log.warning(f"Force-cancelled stuck task {tid} (age={int(now - t.created_at)}s)")
                del tasks[tid]
        if stale:
            log.info(f"Cleaned up {len(stale)} stale task(s)")
        _resource_locks.cleanup_idle()
        _cleanup_proxy_caches()

        # WAL checkpoint — 每个清理周期执行一次 PASSIVE checkpoint
        try:
            cp = db.wal_checkpoint()
            if cp.get("log_pages", 0) > 0:
                log.debug(f"WAL checkpoint: {cp}")
        except Exception as e:
            log.warning(f"WAL checkpoint error: {e}")


_CLEANUP_INTERVAL = 120  # 清理队列扫描间隔（秒）
_CLEANUP_MAX_ATTEMPTS = 10  # 超过此次数标记为永久失败


def _profile_cleanup_loop() -> None:
    """定期扫描 profile_cleanup_queue，执行 stop + delete。"""
    _shutdown_event.wait(15)
    while not _shutdown_event.is_set():
        try:
            pending = db.list_pending_cleanups(limit=20)
            for item in pending:
                pid = item["profile_id"]
                aid = item.get("account_id")
                # 注意：这里直接调用底层 API 而非 cleanup_profile()，
                # 因为 cleanup_profile() 失败时会 re-enqueue，在 queue worker 里会死循环。
                try:
                    adspower.stop_browser(pid)
                except Exception:
                    pass
                try:
                    adspower.delete_profile(pid)
                    db.mark_cleanup_done(item["id"])
                    if aid:
                        db.update_account(aid, ads_profile_id="")
                    log.info(f"Cleanup queue: 成功删除 profile {pid}")
                except Exception as exc:
                    if item["attempts"] + 1 >= _CLEANUP_MAX_ATTEMPTS:
                        db.mark_cleanup_permanently_failed(item["id"], str(exc))
                        log.warning(f"Cleanup queue: profile {pid} 重试耗尽，标记为永久失败")
                    else:
                        db.mark_cleanup_failed(item["id"], str(exc))
                        log.warning(f"Cleanup queue: profile {pid} 删除失败 (第 {item['attempts'] + 1} 次): {exc}")
        except Exception as e:
            log.error(f"Profile cleanup worker error: {e}")
        _shutdown_event.wait(_CLEANUP_INTERVAL)


_PENDING_SALE_TTL = 300  # 5 分钟未确认的 pending_sale 自动回滚


def _pending_sale_rollback_loop() -> None:
    """Periodically roll back unconfirmed pending_sale → unsold."""
    _shutdown_event.wait(60)
    while not _shutdown_event.is_set():
        try:
            rolled = db.rollback_pending_sales(older_than_seconds=_PENDING_SALE_TTL)
            if rolled:
                log.info(f"Rolled back {rolled} stale pending_sale(s) to unsold")
        except Exception as e:
            log.error(f"Pending sale rollback error: {e}")
        _shutdown_event.wait(60)




# ═══════════════════════════════════════════════════════════
# Registration API
# ═══════════════════════════════════════════════════════════

class RegisterRequest(BaseModel):
    proxy_type: str = Field("pool", pattern=r"^(direct|pool|socks5|http)$")
    proxy_str: str = ""
    proxy_id: Optional[int] = None
    card_ids: Optional[list[int]] = None
    credit_amount: int = Field(5, ge=5, le=100)
    bind_path: str = Field("onboarding", pattern=r"^(billing|onboarding)$")
    recharge_strategy: str = Field("none", pattern=r"^(none|auto|manual)$")
    recharge_upper: int = Field(20, description="0=fill, 20, 10, 5")
    recharge_lower: int = Field(5, description="20, 10, 5")
    concurrency: int = Field(1, ge=1, le=10)

    @field_validator("recharge_upper", "recharge_lower")
    @classmethod
    def _valid_tier(cls, v: int) -> int:
        if v not in (0, 5, 10, 20):
            raise ValueError(f"充值档位必须为 0/5/10/20, 实际: {v}")
        return v

    @model_validator(mode="after")
    def _check_combinations(self):
        if self.recharge_strategy == "manual" and self.concurrency > 1:
            raise ValueError("手动续充模式下并发必须为 1")
        return self


def _cleanup_ads_profile(profile_id: str, account_id: Optional[int] = None, reason: str = "") -> bool:
    """统一清理入口的内部别名。"""
    return adspower.cleanup_profile(profile_id, account_id=account_id, reason=reason)


def _handle_manual_recharge(
    state: TaskState,
    result: JSONDict,
    sub_task: Optional[int] = None,
    flow_id: Optional[str] = None,
) -> None:
    """After auto bind+charge, monitor browser and wait for user's manual result.
    Manual result 提交后联动清理 Ads profile。"""
    profile_id = result.get("_profile_id", "") or result.get("ads_profile_id", "")
    account_id = result.get("id")
    card_id = result.get("card_id")
    credit = result.get("credit_loaded", 0)
    evt_base = {"sub_task": sub_task} if sub_task is not None else {}

    state.push({
        "type": "awaiting_manual", **evt_base,
        "account_id": account_id, "card_id": card_id,
        "credit_loaded": credit, "email": result.get("email", ""),
    })
    if flow_id:
        state.set_flow_manual_wait(flow_id, True)

    inactive_streak = 0
    while not state.cancelled:
        status = adspower.check_browser_active(profile_id)
        if status is True:
            inactive_streak = 0
        elif status is False:
            inactive_streak += 1
        # status is None (error/unknown): 不计入，保持现状
        if inactive_streak >= 3:
            break
        time.sleep(0.5)

    if state.cancelled:
        if flow_id:
            state.set_flow_manual_wait(flow_id, False)
        # 被取消也要清理 profile
        if profile_id:
            _cleanup_ads_profile(profile_id, account_id=account_id, reason="manual_cancelled")
        return

    state.push({
        "type": "browser_closed", **evt_base,
        "account_id": account_id, "card_id": card_id,
        "credit_loaded": credit, "email": result.get("email", ""),
    })

    manual = state.wait_manual_result(timeout=600)
    if flow_id:
        state.set_flow_manual_wait(flow_id, False)

    if not manual or state.cancelled:
        # 超时或取消 — 也要清理 profile
        if profile_id:
            _cleanup_ads_profile(profile_id, account_id=account_id, reason="manual_timeout")
        return

    if flow_id and state.flow_control(flow_id):
        if profile_id:
            _cleanup_ads_profile(profile_id, account_id=account_id, reason="manual_flow_stopped")
        return

    # 更新额度
    new_credit = manual.get("credit_loaded")
    if new_credit is not None:
        db.update_account(account_id, credit_loaded=float(new_credit))
        result["credit_loaded"] = new_credit

    # 更新卡状态
    card_action = manual.get("card_action", "keep")
    if card_action != "keep" and card_id:
        db.set_card_fail_tag(card_id, card_action)
        db.batch_update_cards([card_id], status=CARD_STATUS_DISABLED)

    # 手动确认后清理 Ads profile
    if profile_id:
        _cleanup_ads_profile(profile_id, account_id=account_id, reason="manual_done")


@app.post("/api/register")
def start_register(req: RegisterRequest) -> JSONDict:
    _check_task_limit()
    task_id = uuid.uuid4().hex[:12]
    state = TaskState(task_id)
    _register_task(state, task_type="register", params_json=json.dumps({
        "proxy_type": req.proxy_type, "bind_path": req.bind_path,
        "credit_amount": req.credit_amount, "concurrency": req.concurrency,
        "recharge_strategy": req.recharge_strategy,
        "card_count": len(req.card_ids) if req.card_ids else 0,
    }, ensure_ascii=False))

    mode = "register_and_bind"
    credit_amount = max(5, min(100, req.credit_amount))
    concurrency = max(1, min(req.concurrency, 10))
    is_manual = req.recharge_strategy == "manual"
    is_auto = req.recharge_strategy == "auto"
    if is_manual:
        concurrency = 1

    recharge_upper = req.recharge_upper if is_auto else -1
    recharge_lower = req.recharge_lower if is_auto else 5

    use_card_ids: list[int] = list(req.card_ids) if req.card_ids else []
    if not use_card_ids:
        state.status = "failed"
        state.push({"type": "error", "message": "没有选择卡片"})
        return {"task_id": task_id, "pool_size": 0, "concurrency": concurrency}

    pool_size = len(use_card_ids)

    def batch_worker() -> None:
        counter = {"success": 0, "failed": 0, "done": 0}
        lock = threading.Lock()
        infra_failures = {"count": 0}
        fatal_error = {"message": ""}
        max_infra_failures = 3

        pool = list(use_card_ids)
        pool_lock = threading.Lock()
        task_seq = [0]
        card_usage: dict[int, int] = {cid: 0 for cid in pool}
        card_in_use: set = set()  # 正在被某个 flow 使用的卡

        def pick_card() -> Optional[int]:
            with pool_lock:
                if not pool:
                    return None
                # 优先选不在使用中的卡，用量最少，同量随机打散
                available = [c for c in pool if c not in card_in_use]
                if not available:
                    # 所有卡都在使用中，允许复用用量最少的
                    available = pool
                cid = min(available, key=lambda c: (card_usage.get(c, 0), random.random()))
                card_usage[cid] = card_usage.get(cid, 0) + 1
                card_in_use.add(cid)
                return cid

        def release_card(card_id: int) -> None:
            with pool_lock:
                card_in_use.discard(card_id)

        def remove_card(card_id: int) -> int:
            with pool_lock:
                try:
                    pool.remove(card_id)
                except ValueError:
                    pass
                return len(pool)

        def pool_remaining() -> int:
            with pool_lock:
                return len(pool)

        def worker_loop() -> None:
            while True:
                if state.wait_until_runnable():
                    return

                card_id = pick_card()
                if card_id is None:
                    return

                if not _resource_locks.try_acquire("card", card_id):
                    release_card(card_id)
                    continue

                with lock:
                    task_seq[0] += 1
                    idx = task_seq[0]
                flow_id = f"register:{idx}"
                state.register_flow(flow_id)

                def on_step(cur: int, tot: int, msg: str, _idx: int = idx, _cid: int = card_id) -> None:
                    state.push({
                        "type": "step", "sub_task": _idx,
                        "current": cur, "total": tot,
                        "message": f"[卡#{_cid}] {msg}",
                    })

                def _is_flow_stopped() -> bool:
                    return _flow_check(state, flow_id)

                try:
                    proxy = _resolve_proxy(
                        req.proxy_type, req.proxy_str, req.proxy_id,
                        on_log=lambda msg, _s=on_step: _s(0, 0, msg),
                        check_abort=_is_flow_stopped,
                    )
                    result = reg_module.run(
                        proxy=proxy, mode=mode, card_id=card_id,
                        credit_amount=credit_amount, bind_path=req.bind_path,
                        recharge_upper=recharge_upper, recharge_lower=recharge_lower,
                        on_step=on_step,
                        check_abort=_is_flow_stopped,
                        cleanup_mode="defer_manual" if is_manual else "always",
                    )

                    if is_manual and result.get("_profile_id") and result.get("card_id"):
                        _handle_manual_recharge(state, result, sub_task=idx, flow_id=flow_id)

                    with lock:
                        infra_failures["count"] = 0
                        counter["success"] += 1
                        counter["done"] += 1
                        snap = dict(counter)
                    state.push({
                        "type": "sub_done", "sub_task": idx,
                        **snap, "pool_remaining": pool_remaining(),
                        "email": result.get("email", ""),
                        "card_id": result.get("card_id"),
                        "credit_loaded": result.get("credit_loaded", 0),
                    })
                except reg_module.TaskAborted:
                    return
                except reg_module.BindFailure as e:
                    remaining = remove_card(e.card_id or card_id)
                    with lock:
                        infra_failures["count"] = 0
                        counter["failed"] += 1
                        counter["done"] += 1
                        snap = dict(counter)
                    state.push({
                        "type": "sub_error", "sub_task": idx,
                        **snap, "pool_remaining": remaining,
                        "message": str(e),
                        "card_id": e.card_id, "fail_tag": e.fail_tag,
                    })
                except Exception as e:
                    stop_now = False
                    err_msg = str(e)
                    with lock:
                        counter["failed"] += 1
                        counter["done"] += 1
                        infra_failures["count"] += 1
                        fail_streak = infra_failures["count"]
                        if fail_streak >= max_infra_failures and not fatal_error["message"]:
                            fatal_error["message"] = (
                                f"连续 {fail_streak} 次基础流程失败，任务已自动停止。"
                                f"最后错误: {err_msg}"
                            )
                            stop_now = True
                        snap = dict(counter)
                    state.push({
                        "type": "sub_error", "sub_task": idx,
                        **snap, "pool_remaining": pool_remaining(),
                        "message": err_msg,
                    })
                    if stop_now:
                        state.cancel()
                        return
                finally:
                    _resource_locks.release("card", card_id)
                    release_card(card_id)
                    state.unregister_flow(flow_id)

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(worker_loop) for _ in range(concurrency)]
            for f in as_completed(futures):
                pass

        if fatal_error["message"]:
            state.result = counter
            state.error = fatal_error["message"]
            state.status = "failed"
            state.push({
                "type": "error",
                "result": counter,
                "message": fatal_error["message"],
            })
        elif state.cancelled:
            state.result = counter
            state.status = "cancelled"
            state.push({"type": "cancelled", "result": counter,
                        "message": f"已停止 (成功 {counter['success']}, 失败 {counter['failed']})"})
        else:
            state.result = counter
            state.status = "success"
            state.push({"type": "done", "result": counter})
        _finish_task(state)

    start_managed_thread(batch_worker, name=f"register-{task_id}")

    return {"task_id": task_id, "pool_size": pool_size, "concurrency": concurrency}


@app.get("/api/register/{task_id}/stream")
async def stream_register(task_id: str):
    return EventSourceResponse(_sse_generator(_get_task(task_id)))


@app.get("/api/register/{task_id}")
def get_register_task(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    return {"task_id": task_id, "status": state.status, "steps": state.steps,
            "result": state.result, "error": state.error}


@app.post("/api/register/{task_id}/cancel")
def cancel_register_task(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    if state.status not in ("running", "pause_requested", "paused"):
        raise HTTPException(status_code=409, detail="task not running")
    state.cancel()
    return {"ok": True}


@app.post("/api/register/{task_id}/pause")
def pause_register_task(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    if state.status != "running":
        raise HTTPException(status_code=409, detail="task not running")
    state.pause()
    return {"ok": True}


@app.post("/api/register/{task_id}/resume")
def resume_register_task(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    if state.status not in ("pause_requested", "paused"):
        raise HTTPException(status_code=409, detail="task not paused")
    state.resume()
    return {"ok": True}


class ManualResultRequest(BaseModel):
    credit_loaded: Optional[float] = None
    card_action: str = "keep"


@app.post("/api/register/{task_id}/manual-result")
def submit_manual_result(task_id: str, body: ManualResultRequest) -> JSONDict:
    state = _get_task(task_id)
    state.set_manual_result(dump_model(body))
    return {"ok": True}




class RetryBindRequest(BaseModel):
    card_ids: list[int] = Field(..., min_length=1)
    proxy_type: str = Field("direct", pattern=r"^(direct|pool|socks5|http)$")
    proxy_str: str = ""
    proxy_id: Optional[int] = None
    bind_path: str = Field("onboarding", pattern=r"^(billing|onboarding)$")
    credit_amount: int = Field(5, ge=5, le=100)
    recharge_strategy: str = Field("none", pattern=r"^(none|auto)$")
    recharge_upper: int = Field(20)
    recharge_lower: int = Field(5)
    concurrency: int = Field(1, ge=1, le=10)

    @field_validator("recharge_upper", "recharge_lower")
    @classmethod
    def _valid_tier(cls, v: int) -> int:
        if v not in (0, 5, 10, 20):
            raise ValueError(f"充值档位必须为 0/5/10/20, 实际: {v}")
        return v


@app.post("/api/cards/retry-bind")
def retry_card_bind(req: RetryBindRequest) -> JSONDict:
    """Retry binding for cards with fail tags: register new account + bind each card."""
    _check_task_limit()
    task_id = uuid.uuid4().hex[:12]
    state = TaskState(task_id)
    _register_task(state, task_type="retry_bind", params_json=json.dumps({
        "card_count": len(req.card_ids), "proxy_type": req.proxy_type,
        "bind_path": req.bind_path, "credit_amount": req.credit_amount,
        "concurrency": req.concurrency,
    }, ensure_ascii=False))

    card_ids = req.card_ids[:50]
    concurrency = max(1, min(req.concurrency, 10))
    credit_amount = max(5, min(100, req.credit_amount))
    is_auto = req.recharge_strategy == "auto"
    recharge_upper = req.recharge_upper if is_auto else -1
    recharge_lower = req.recharge_lower if is_auto else 5

    def batch_worker() -> None:
        counter = {"success": 0, "failed": 0, "done": 0}
        lock = threading.Lock()

        def single(idx: int, cid: int) -> None:
            flow_id = f"retry:{idx + 1}"
            state.register_flow(flow_id)
            if not _resource_locks.try_acquire("card", cid):
                with lock:
                    counter["failed"] += 1
                    counter["done"] += 1
                    snap = dict(counter)
                state.push({
                    "type": "sub_error", "sub_task": idx + 1,
                    **snap, "count": len(card_ids),
                    "message": f"卡 #{cid} 被其他操作占用，已跳过",
                })
                state.unregister_flow(flow_id)
                return
            def on_step(cur: int, tot: int, msg: str) -> None:
                state.push({
                    "type": "step", "sub_task": idx + 1,
                    "current": cur, "total": tot,
                    "message": f"[卡#{cid}] {msg}",
                })
            def _is_flow_stopped() -> bool:
                return _flow_check(state, flow_id)
            try:
                proxy = _resolve_proxy(
                    req.proxy_type, req.proxy_str, req.proxy_id,
                    on_log=lambda msg: on_step(0, 0, msg),
                    check_abort=_is_flow_stopped,
                )
                result = reg_module.run(
                    proxy=proxy, mode="register_and_bind", card_id=cid,
                    credit_amount=credit_amount, bind_path=req.bind_path,
                    recharge_upper=recharge_upper, recharge_lower=recharge_lower,
                    on_step=on_step,
                    check_abort=_is_flow_stopped,
                )
                with lock:
                    counter["success"] += 1
                    counter["done"] += 1
                    snap = dict(counter)
                state.push({
                    "type": "sub_done", "sub_task": idx + 1,
                    **snap, "count": len(card_ids),
                    "email": result.get("email", ""),
                    "card_id": cid,
                    "credit_loaded": result.get("credit_loaded", 0),
                })
            except reg_module.TaskAborted:
                return
            except reg_module.BindFailure as e:
                with lock:
                    counter["failed"] += 1
                    counter["done"] += 1
                    snap = dict(counter)
                state.push({
                    "type": "sub_error", "sub_task": idx + 1,
                    **snap, "count": len(card_ids),
                    "message": str(e),
                    "card_id": e.card_id or cid, "fail_tag": e.fail_tag,
                })
            except Exception as e:
                with lock:
                    counter["failed"] += 1
                    counter["done"] += 1
                    snap = dict(counter)
                state.push({
                    "type": "sub_error", "sub_task": idx + 1,
                    **snap, "count": len(card_ids),
                    "message": str(e),
                    "card_id": cid,
                })
            finally:
                _resource_locks.release("card", cid)
                state.unregister_flow(flow_id)

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for i, cid in enumerate(card_ids):
                if state.wait_until_runnable():
                    break
                futures.append(executor.submit(single, i, cid))
            for f in as_completed(futures):
                pass

        if state.cancelled:
            state.result = counter
            state.status = "cancelled"
            state.push({"type": "cancelled", "result": counter})
        else:
            state.result = counter
            state.status = "success"
            state.push({"type": "done", "result": counter})
        _finish_task(state)

    start_managed_thread(batch_worker, name=f"retry-bind-{task_id}")
    return {"task_id": task_id, "count": len(card_ids)}


@app.get("/api/cards/retry-bind/{task_id}/stream")
async def stream_retry_bind(task_id: str):
    return EventSourceResponse(_sse_generator(_get_task(task_id)))


@app.post("/api/cards/retry-bind/{task_id}/cancel")
def cancel_retry_bind(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    if state.status not in ("running", "pause_requested", "paused"):
        raise HTTPException(status_code=409, detail="task not running")
    state.cancel()
    return {"ok": True}


@app.post("/api/cards/retry-bind/{task_id}/pause")
def pause_retry_bind(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    if state.status != "running":
        raise HTTPException(status_code=409, detail="task not running")
    state.pause()
    return {"ok": True}


@app.post("/api/cards/retry-bind/{task_id}/resume")
def resume_retry_bind(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    if state.status not in ("pause_requested", "paused"):
        raise HTTPException(status_code=409, detail="task not paused")
    state.resume()
    return {"ok": True}



# ═══════════════════════════════════════════════════════════
# Static files & frontend
# ═══════════════════════════════════════════════════════════

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/")
    def serve_index() -> FileResponse:
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

"""SSE task state management — TaskState, task registry, and helpers.

Extracted from server.py to keep task lifecycle logic self-contained.
"""

import asyncio
import functools
import json
import queue
import threading
import time
from typing import Any, Callable, Optional

from fastapi import HTTPException

from .. import db as _db
from ..common import JSONDict

# ─── Constants ────────────────────────────────────────────

TASK_TTL = 1800  # 30 min
TASK_MAX_AGE = 7200  # 2h — 无论状态如何，超龄任务一律清理
TASK_FINAL_STATES = {"success", "failed", "cancelled"}

MAX_RUNNING_TASKS = 5  # 全局最大同时运行任务数


# ─── TaskState ────────────────────────────────────────────

class TaskState:
    _MAX_STEPS = 500  # 保留最近事件，限制长任务内存占用

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        self.status = "running"
        self.steps: list[JSONDict] = []
        self.result: Optional[JSONDict] = None
        self.error: Optional[str] = None
        self._listeners: list[queue.Queue] = []
        self._listeners_lock = threading.Lock()
        self._cancel = threading.Event()
        self._pause = threading.Event()
        self.created_at = time.time()
        self._manual_result: Optional[JSONDict] = None
        self._manual_event = threading.Event()
        self._control_lock = threading.Lock()
        self._control_cv = threading.Condition(self._control_lock)
        self._pause_requested = False
        self._flows: dict[str, str] = {}

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    @property
    def paused(self) -> bool:
        return self._pause.is_set()

    @property
    def pause_requested(self) -> bool:
        return self._pause_requested

    def _control_snapshot_locked(self) -> JSONDict:
        active = {fid: st for fid, st in self._flows.items() if st != "done"}
        active_total = len(active)
        paused_total = sum(1 for st in active.values() if st == "pause_ack")
        manual_total = sum(1 for st in active.values() if st == "manual_wait")
        parked_total = paused_total + manual_total
        running_total = max(0, active_total - parked_total)
        return {
            "active_flows": active_total,
            "paused_flows": paused_total,
            "manual_wait_flows": manual_total,
            "parked_flows": parked_total,
            "running_flows": running_total,
        }

    def _push_pause_progress_locked(self) -> None:
        if not self._pause_requested and not self.paused:
            return
        snap = self._control_snapshot_locked()
        self.push({
            "type": "pause_progress",
            "message": f"暂停收敛中 {snap['parked_flows']}/{snap['active_flows']}",
            **snap,
        })

    def _maybe_mark_paused_locked(self) -> None:
        if not self._pause_requested:
            return
        snap = self._control_snapshot_locked()
        if snap["active_flows"] == 0 or snap["parked_flows"] >= snap["active_flows"]:
            if not self._pause.is_set():
                self._pause.set()
                self.status = "paused"
                self.push({
                    "type": "paused",
                    "message": "任务已暂停",
                    **snap,
                })

    def register_flow(self, flow_id: str) -> None:
        with self._control_cv:
            self._flows[flow_id] = "running"
            if self._pause_requested:
                self._push_pause_progress_locked()
                self._maybe_mark_paused_locked()

    def unregister_flow(self, flow_id: str) -> None:
        with self._control_cv:
            if flow_id in self._flows:
                self._flows[flow_id] = "done"
            if self._pause_requested:
                self._push_pause_progress_locked()
                self._maybe_mark_paused_locked()
            self._control_cv.notify_all()

    def set_flow_manual_wait(self, flow_id: str, waiting: bool) -> None:
        with self._control_cv:
            if flow_id not in self._flows:
                return
            self._flows[flow_id] = "manual_wait" if waiting else "running"
            if self._pause_requested:
                self._push_pause_progress_locked()
                self._maybe_mark_paused_locked()
            self._control_cv.notify_all()

    def wait_until_runnable(self) -> bool:
        with self._control_cv:
            while self._pause_requested and not self.cancelled:
                self._control_cv.wait(timeout=0.2)
            return self.cancelled

    def cancel(self) -> None:
        self._cancel.set()
        with self._control_cv:
            self._pause_requested = False
            self._pause.clear()
            self.status = "cancel_requested"
            self._control_cv.notify_all()
        self._manual_event.set()

    def pause(self) -> None:
        with self._control_cv:
            self._pause_requested = True
            self._pause.clear()
            self.status = "pause_requested"
            snap = self._control_snapshot_locked()
            self.push({
                "type": "pause_requested",
                "message": "已发送暂停请求，等待各 flow 收敛",
                **snap,
            })
            self._push_pause_progress_locked()
            self._maybe_mark_paused_locked()
            self._control_cv.notify_all()

    def resume(self) -> None:
        with self._control_cv:
            self._pause_requested = False
            self._pause.clear()
            for fid, flow_state in list(self._flows.items()):
                if flow_state == "pause_ack":
                    self._flows[fid] = "running"
            self.status = "running"
            snap = self._control_snapshot_locked()
            self.push({"type": "resumed", "message": "任务已恢复", **snap})
            self._control_cv.notify_all()

    def flow_control(self, flow_id: str) -> bool:
        with self._control_cv:
            if flow_id not in self._flows:
                self._flows[flow_id] = "running"
            while True:
                if self.cancelled:
                    self._control_cv.notify_all()
                    return True
                if not self._pause_requested:
                    if self._flows.get(flow_id) == "pause_ack":
                        self._flows[flow_id] = "running"
                    return False
                if self._flows.get(flow_id) != "manual_wait":
                    if self._flows.get(flow_id) != "pause_ack":
                        self._flows[flow_id] = "pause_ack"
                        self._push_pause_progress_locked()
                        self._maybe_mark_paused_locked()
                else:
                    self._push_pause_progress_locked()
                    self._maybe_mark_paused_locked()
                self._control_cv.wait(timeout=0.2)

    def push(self, event: JSONDict) -> None:
        self.steps.append(event)
        if len(self.steps) > self._MAX_STEPS:
            self.steps = self.steps[-self._MAX_STEPS:]
        with self._listeners_lock:
            for q in self._listeners:
                q.put_nowait(event)

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue()
        with self._listeners_lock:
            recent = self.steps[-self._MAX_STEPS:] if len(self.steps) > self._MAX_STEPS else self.steps
            for s in recent:
                q.put_nowait(s)
            self._listeners.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._listeners_lock:
            try:
                self._listeners.remove(q)
            except ValueError:
                pass

    def set_manual_result(self, result: JSONDict) -> None:
        self._manual_result = result
        self._manual_event.set()

    def wait_manual_result(self, timeout: float = 600) -> Optional[JSONDict]:
        self._manual_event.wait(timeout)
        result = self._manual_result
        self._manual_result = None
        self._manual_event.clear()
        return result


# ─── Task registry ────────────────────────────────────────

tasks: dict[str, TaskState] = {}
tasks_lock = threading.Lock()
worker_threads: set[threading.Thread] = set()
worker_threads_lock = threading.Lock()


# ─── Helper functions ─────────────────────────────────────

def _count_running_tasks() -> int:
    """Count tasks that are still actively running (not final)."""
    with tasks_lock:
        return sum(1 for t in tasks.values() if t.status not in TASK_FINAL_STATES)


def _check_task_limit() -> None:
    if _count_running_tasks() >= MAX_RUNNING_TASKS:
        raise HTTPException(status_code=429, detail=f"已达最大并发任务上限 ({MAX_RUNNING_TASKS})")


def _get_task(task_id: str) -> TaskState:
    with tasks_lock:
        state = tasks.get(task_id)
    if not state:
        raise HTTPException(status_code=404, detail="task not found")
    return state


def _register_task(state: TaskState, *, task_type: str = "", params_json: str = "") -> None:
    with tasks_lock:
        tasks[state.task_id] = state
    if task_type:
        state._task_type = task_type
        try:
            _db.insert_task_run(state.task_id, task_type, params_json)
        except Exception:
            pass


def _extract_counts(result: dict) -> tuple[int, int]:
    """从不同任务类型的 result dict 中提取 (success, failed) 计数。

    - register / retry_bind: {success, failed}
    - proxy_check: {alive, dead}
    - key_check: {active, dead, invalid, unknown, cleaned}
    """
    # 优先匹配 register / retry_bind
    if "success" in result:
        return result.get("success", 0), result.get("failed", 0)
    # proxy_check
    if "alive" in result:
        return result.get("alive", 0), result.get("dead", 0)
    # key_check
    if "active" in result:
        return result.get("active", 0), result.get("dead", 0) + result.get("invalid", 0)
    return 0, 0


def _finish_task(state: TaskState) -> None:
    """持久化任务最终状态到 task_runs 表。"""
    task_type = getattr(state, "_task_type", "")
    if not task_type:
        return
    result = state.result or {}
    success_count, failed_count = _extract_counts(result)
    try:
        _db.finish_task_run(
            state.task_id,
            status=state.status,
            success_count=success_count,
            failed_count=failed_count,
            error_summary=state.error or "",
        )
    except Exception:
        pass


def start_managed_thread(
    target: Callable[..., Any],
    *args: Any,
    name: Optional[str] = None,
    daemon: bool = True,
    **kwargs: Any,
) -> threading.Thread:
    """Start a worker thread and keep it in the lifecycle registry."""

    def _runner() -> None:
        try:
            target(*args, **kwargs)
        finally:
            with worker_threads_lock:
                worker_threads.discard(threading.current_thread())

    t = threading.Thread(target=_runner, name=name, daemon=daemon)
    with worker_threads_lock:
        worker_threads.add(t)
    t.start()
    return t


def managed_worker_count() -> int:
    with worker_threads_lock:
        worker_threads_copy = list(worker_threads)
    return sum(1 for t in worker_threads_copy if t.is_alive())


def wait_managed_threads(timeout: float = 15.0) -> int:
    """Wait for managed task threads to exit. Returns remaining alive count."""
    deadline = time.time() + timeout
    while True:
        with worker_threads_lock:
            alive = [t for t in worker_threads if t.is_alive()]
        if not alive:
            return 0
        remaining = deadline - time.time()
        if remaining <= 0:
            return len(alive)
        slice_timeout = min(0.2, remaining)
        for t in alive:
            t.join(timeout=slice_timeout)


def _flow_check(state: TaskState, flow_id: str) -> bool:
    """Flow-level cooperative control: wait on pause barrier, return True on cancel."""
    return state.flow_control(flow_id)


async def _sse_generator(state: TaskState):
    q = state.subscribe()
    loop = asyncio.get_event_loop()
    heartbeat_interval = 15
    last_send = time.time()
    try:
        while True:
            try:
                event = await loop.run_in_executor(
                    None, functools.partial(q.get, timeout=0.3)
                )
            except queue.Empty:
                if state.status not in ("running", "pause_requested", "paused", "cancel_requested"):
                    break
                if time.time() - last_send > heartbeat_interval:
                    yield {"comment": "keepalive"}
                    last_send = time.time()
                continue
            yield {"data": json.dumps(event, ensure_ascii=False)}
            last_send = time.time()
            if event.get("type") in ("done", "error", "cancelled"):
                break
    finally:
        state.unsubscribe(q)

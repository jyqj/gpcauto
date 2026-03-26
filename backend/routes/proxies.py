"""Proxies 路由。"""

import logging
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator
from sse_starlette.sse import EventSourceResponse

from .. import db
from ..common import JSONDict, dump_model, BatchIds, validate_pagination
from ..constants import PROXY_STATUS_AVAILABLE, PROXY_STATUS_DISABLED
from ..services.task_manager import (
    TaskState, _check_task_limit, _get_task, _register_task, _finish_task, _sse_generator,
    start_managed_thread,
)
from ..services.proxy_service import (
    _invalidate_proxy_cache, _invalidate_proxy_cache_all,
    _temp_ban_proxy, _build_proxy_url, _test_proxy,
)

router = APIRouter(prefix="/api", tags=["proxies"])
log = logging.getLogger("routes.proxies")


@router.get("/proxies")
def list_proxies(
    page: int = Query(0, ge=0),
    page_size: int = Query(0, ge=0, le=200),
    search: str = "",
    status: str = "",
    proxy_type: str = Query("", alias="type"),
    sort: str = "",
    order: str = "desc",
) -> JSONDict:
    validate_pagination(page, page_size)
    qkw = dict(search=search, status=status, proxy_type=proxy_type, sort=sort, order=order)
    result: JSONDict = {"proxies": db.list_proxies(page=page, page_size=page_size, **qkw)}
    if page > 0 and page_size > 0:
        result["total"] = db.count_proxies(search=search, status=status, proxy_type=proxy_type)
        result["page"] = page
        result["page_size"] = page_size
    return result


class ProxyCreate(BaseModel):
    label: str = ""
    type: str = Field("socks5", pattern=r"^(socks5|http|https)$")
    host: str = Field(..., min_length=1)
    port: str = Field(..., pattern=r"^\d{1,5}$")
    username: str = ""
    password: str = ""

    @field_validator("port")
    @classmethod
    def _valid_port(cls, v: str) -> str:
        if v and not (1 <= int(v) <= 65535):
            raise ValueError(f"端口范围 1-65535, 实际: {v}")
        return v


@router.post("/proxies")
def add_proxy(body: ProxyCreate) -> JSONDict:
    try:
        pid = db.insert_proxy(dump_model(body))
    except db.DuplicateRecordError:
        raise HTTPException(status_code=409, detail="代理已存在")
    return {"id": pid}


class ProxyBatchImport(BaseModel):
    raw: str
    default_type: str = "socks5"


def _parse_proxy_line(line: str, default_type: str = "socks5") -> Optional[dict]:
    line = line.strip()
    if not line:
        return None

    m = re.match(r'^(socks5|http|https)://(?:([^:@]+):([^@]+)@)?([^:]+):(\d+)$', line, re.I)
    if m:
        return {
            "type": m.group(1).lower().replace("https", "http"),
            "host": m.group(4), "port": m.group(5),
            "username": m.group(2) or "", "password": m.group(3) or "",
        }

    m = re.match(r'^([^:]+):(\d+):([^:]+):(.+)$', line)
    if m:
        return {
            "type": default_type, "host": m.group(1), "port": m.group(2),
            "username": m.group(3), "password": m.group(4),
        }

    m = re.match(r'^([^:]+):(\d+)$', line)
    if m:
        return {"type": default_type, "host": m.group(1), "port": m.group(2), "username": "", "password": ""}

    return None


@router.post("/proxies/batch")
def batch_import_proxies(body: ProxyBatchImport) -> JSONDict:
    results = []
    for line in body.raw.strip().splitlines():
        parsed = _parse_proxy_line(line, body.default_type)
        if parsed:
            results.append({"status": "ok", **parsed})
        else:
            results.append({"status": "fail", "raw": line.strip()[:120]})
    ok_items = [r for r in results if r["status"] == "ok"]
    if ok_items:
        proxies = [{"label": "", **{k: v for k, v in r.items() if k != "status"}} for r in ok_items]
        ids, dup_skipped = db.batch_insert_proxies(proxies)
        return {"imported": len(ids), "duplicated": dup_skipped,
                "failed": len(results) - len(ok_items), "results": results}
    return {"imported": 0, "failed": len(results), "results": results}


@router.delete("/proxies/{proxy_id}")
def remove_proxy(proxy_id: int) -> JSONDict:
    _invalidate_proxy_cache(proxy_id)
    db.delete_proxy(proxy_id)
    return {"ok": True}


@router.post("/proxies/batch-delete")
def batch_delete_proxies(body: BatchIds) -> JSONDict:
    for pid in body.ids:
        _invalidate_proxy_cache(pid)
    db.delete_proxies(body.ids)
    db.audit_log("proxy", None, "batch_delete", f"删除 {len(body.ids)} 个代理")
    return {"ok": True, "deleted": len(body.ids)}


class ProxyStatusUpdate(BaseModel):
    ids: list[int]
    status: str


@router.post("/proxies/batch-status")
def batch_update_proxy_status(body: ProxyStatusUpdate) -> JSONDict:
    if body.status not in (PROXY_STATUS_AVAILABLE, PROXY_STATUS_DISABLED):
        raise HTTPException(status_code=400, detail="invalid status")
    db.batch_update_proxy_status(body.ids, body.status)
    _invalidate_proxy_cache_all()
    db.audit_log("proxy", None, "batch_status", f"{len(body.ids)} 个代理 → {body.status}")
    return {"ok": True, "updated": len(body.ids)}


@router.post("/proxies/reset-all")
def reset_all_proxies() -> JSONDict:
    all_proxies = db.list_proxies()
    disabled_ids = [p["id"] for p in all_proxies if p.get("status") == PROXY_STATUS_DISABLED]
    if disabled_ids:
        db.batch_update_proxy_status(disabled_ids, PROXY_STATUS_AVAILABLE)
        _invalidate_proxy_cache_all()
    return {"ok": True, "reset": len(disabled_ids)}


class ProxyCheckRequest(BaseModel):
    ids: Optional[list[int]] = None
    concurrency: int = Field(5, ge=1, le=20)


@router.post("/proxies/check")
def start_proxy_check(req: ProxyCheckRequest) -> JSONDict:
    _check_task_limit()
    task_id = uuid.uuid4().hex[:12]
    state = TaskState(task_id)
    import json as _json
    _register_task(state, task_type="proxy_check", params_json=_json.dumps({
        "concurrency": req.concurrency,
        "ids_count": len(req.ids) if req.ids else None,
    }))
    initial_proxies = db.list_proxies()
    initial_total = len(initial_proxies if not req.ids else [p for p in initial_proxies if p["id"] in set(req.ids)])

    def worker() -> None:
        proxies_list = list(initial_proxies)
        if req.ids:
            id_set = set(req.ids)
            proxies_list = [p for p in proxies_list if p["id"] in id_set]

        total = len(proxies_list)
        if total == 0:
            state.result = {"alive": 0, "dead": 0, "recovered": 0, "total": 0}
            state.status = "success"
            state.push({"type": "done", "result": state.result})
            return

        result = {"alive": 0, "dead": 0, "recovered": 0, "total": total}
        completed = [0]
        lock = threading.Lock()

        def check_one(p: JSONDict) -> None:
            if state.cancelled:
                return
            proxy_url = _build_proxy_url(p)
            alive, latency, error = _test_proxy(proxy_url)
            recovered = False
            if alive:
                if p.get("status") == PROXY_STATUS_DISABLED:
                    db.batch_update_proxy_status([p["id"]], PROXY_STATUS_AVAILABLE)
                    _invalidate_proxy_cache(p["id"])
                    recovered = True
            if not alive:
                _temp_ban_proxy(p["id"])
                _invalidate_proxy_cache(p["id"])
                db.disable_proxy(p["id"])
            with lock:
                if alive:
                    result["alive"] += 1
                    if recovered:
                        result["recovered"] += 1
                else:
                    result["dead"] += 1
                completed[0] += 1
                idx = completed[0]
            state.push({
                "type": "proxy_checked", "current": idx, "total": total,
                "proxy_id": p["id"], "alive": alive,
                "recovered": recovered,
                "latency_ms": latency, "error": error or "",
            })

        workers = max(1, min(req.concurrency, total))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(check_one, p) for p in proxies_list]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    log.warning(f"Proxy check error: {e}")

        if state.cancelled:
            state.result = result
            state.status = "cancelled"
            state.push({"type": "cancelled", "result": result})
        else:
            state.result = result
            state.status = "success"
            state.push({"type": "done", "result": result})
        _finish_task(state)

    start_managed_thread(worker, name=f"proxy-check-{task_id}")
    return {"task_id": task_id, "total": initial_total}


@router.get("/proxies/check/{task_id}/stream")
async def stream_proxy_check(task_id: str):
    return EventSourceResponse(_sse_generator(_get_task(task_id)))


@router.post("/proxies/check/{task_id}/cancel")
def cancel_proxy_check(task_id: str) -> JSONDict:
    state = _get_task(task_id)
    if state.status not in ("running",):
        raise HTTPException(status_code=409, detail="task not running")
    state.cancel()
    return {"ok": True}

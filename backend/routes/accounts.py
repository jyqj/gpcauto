"""Accounts 路由。"""

import csv
import io
import logging
import threading
import time
import uuid
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from .. import db, adspower, key_checker
from ..common import JSONDict, dump_model, BatchIds, validate_pagination
from ..constants import ACCOUNT_STATUS_DEAD, CARD_STATUS_DISABLED
from ..services.task_manager import (
    TaskState, _check_task_limit, _get_task, _register_task, _finish_task, _sse_generator,
    TASK_FINAL_STATES, tasks, tasks_lock,
    start_managed_thread,
)

router = APIRouter(prefix="/api", tags=["accounts"])
log = logging.getLogger("routes.accounts")


def _cleanup_ads_profile(profile_id: str, account_id: Optional[int] = None, reason: str = "") -> bool:
    return adspower.cleanup_profile(profile_id, account_id=account_id, reason=reason)


# 从 server.py 传入的资源锁引用（在 include_router 后由 server.py 设置）
_resource_locks = None


def set_resource_locks(locks) -> None:
    global _resource_locks
    _resource_locks = locks


def _get_resource_locks():
    if _resource_locks is None:
        raise RuntimeError("resource_locks 未初始化，请确保 server.py 已调用 set_resource_locks()")
    return _resource_locks


@router.get("/accounts")
def list_accounts(
    page: int = Query(0, ge=0),
    page_size: int = Query(0, ge=0, le=200),
    search: str = "",
    status: str = "",
    sale_status: str = "",
    sort: str = "",
    order: str = "desc",
) -> JSONDict:
    validate_pagination(page, page_size)
    qkw = dict(search=search, status=status, sale_status=sale_status, sort=sort, order=order)
    result: JSONDict = {"accounts": db.list_accounts(page=page, page_size=page_size, **qkw)}
    if page > 0 and page_size > 0:
        result["total"] = db.count_accounts(search=search, status=status, sale_status=sale_status)
        result["page"] = page
        result["page_size"] = page_size
    return result


class AccountUpdate(BaseModel):
    card_id: Optional[int] = None
    status: Optional[str] = None
    notes: Optional[str] = None
    credit_loaded: Optional[float] = None


@router.patch("/accounts/{account_id}")
def patch_account(account_id: int, body: AccountUpdate) -> JSONDict:
    fields = {k: v for k, v in dump_model(body).items() if v is not None}
    if fields:
        db.update_account(account_id, **fields)
    return {"ok": True}


@router.post("/accounts/batch-delete")
def batch_delete_accounts(body: BatchIds) -> JSONDict:
    deleted_ids: list[int] = []
    profiles_cleaned = 0
    skipped_locked = 0
    skipped_cleanup = 0

    for aid in body.ids:
        with _get_resource_locks().try_lock("account", aid) as acquired:
            if not acquired:
                skipped_locked += 1
                continue
            acct = db.get_account(aid)
            pid = acct.get("ads_profile_id", "") if acct else ""
            if pid:
                if _cleanup_ads_profile(pid, account_id=aid, reason="batch_delete"):
                    profiles_cleaned += 1
                else:
                    skipped_cleanup += 1
                    continue
            deleted_ids.append(aid)

    if deleted_ids:
        db.delete_accounts(deleted_ids)
        db.audit_log("account", None, "batch_delete", f"删除 {len(deleted_ids)} 个账号, 清理 {profiles_cleaned} 个 profile")

    result: JSONDict = {"ok": True, "deleted": len(deleted_ids), "profiles_cleaned": profiles_cleaned}
    if skipped_locked:
        result["skipped_locked"] = skipped_locked
    if skipped_cleanup:
        result["skipped_cleanup_failed"] = skipped_cleanup
    return result


class BatchAccountUpdate(BaseModel):
    ids: list[int]
    status: Optional[str] = None
    card_id: Optional[int] = None
    sale_status: Optional[str] = None


@router.post("/accounts/batch-update")
def batch_update_accounts(body: BatchAccountUpdate) -> JSONDict:
    fields = {}
    if body.status is not None:
        fields["status"] = body.status
    if body.card_id is not None:
        fields["card_id"] = body.card_id
    if body.sale_status is not None:
        fields["sale_status"] = body.sale_status
    if fields:
        db.batch_update_accounts(body.ids, **fields)
        db.audit_log("account", None, "batch_update", f"{len(body.ids)} 个账号: {fields}")
    return {"ok": True}


class SellRequest(BaseModel):
    ids: list[int]
    format: str = "txt"


@router.post("/accounts/sell")
def sell_accounts(body: SellRequest) -> Response:
    token = f"ps_{time.time():.0f}_{uuid.uuid4().hex[:8]}"
    selected = db.lock_for_sale(body.ids, token)
    if not selected:
        raise HTTPException(status_code=404, detail="no unsold accounts matched")

    sep = "|"
    if body.format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["email", "password", "api_key", "credit_loaded"])
        for a in selected:
            writer.writerow([a.get("email", ""), a.get("password", ""), a.get("api_key", ""), a.get("credit_loaded", 0)])
        content = output.getvalue()
        media = "text/csv"
        fname = f"accounts_sold_{len(selected)}.csv"
    else:
        lines = [f"{a.get('email','')}{sep}{a.get('password','')}{sep}{a.get('api_key','')}{sep}${a.get('credit_loaded', 0)}" for a in selected]
        content = "\n".join(lines)
        media = "text/plain"
        fname = f"accounts_sold_{len(selected)}.txt"

    return Response(
        content=content, media_type=media,
        headers={"Content-Disposition": f"attachment; filename={fname}", "X-Sale-Token": token},
    )


class SellConfirmRequest(BaseModel):
    token: str


@router.post("/accounts/sell/confirm")
def confirm_sell(body: SellConfirmRequest) -> JSONDict:
    confirmed = db.confirm_sale(body.token)
    if confirmed == 0:
        raise HTTPException(status_code=404, detail="no pending sale found for this token")
    db.audit_log("account", None, "sell_confirm", f"确认售卖 {confirmed} 个账号, token={body.token}")
    return {"ok": True, "confirmed": confirmed}


@router.get("/accounts/export", response_model=None)
def export_accounts(format: str = "csv") -> Any:
    accounts = db.list_accounts()
    if format == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["email", "password", "api_key", "status", "sale_status",
                         "device_id", "name", "org_name", "ads_profile_id", "registered_at"])
        for a in accounts:
            writer.writerow([
                a.get("email", ""), a.get("password", ""), a.get("api_key", ""),
                a.get("status", ""), a.get("sale_status", ""),
                a.get("device_id", ""), a.get("name", ""),
                a.get("org_name", ""), a.get("ads_profile_id", ""),
                a.get("registered_at", ""),
            ])
        return Response(
            content=output.getvalue(), media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=accounts.csv"},
        )
    return {"accounts": accounts}


@router.get("/accounts/export-keys")
def export_keys() -> Response:
    accounts = db.list_accounts()
    keys = [a["api_key"] for a in accounts if a.get("api_key") and a["api_key"].startswith("sk-")]
    return Response(
        content="\n".join(keys), media_type="text/plain",
        headers={"Content-Disposition": "attachment; filename=api_keys.txt"},
    )


@router.post("/accounts/cleanup-profiles")
def cleanup_dead_profiles() -> JSONDict:
    accounts = db.list_accounts()
    dead = [a for a in accounts if a.get("status") == ACCOUNT_STATUS_DEAD and a.get("ads_profile_id")]
    cleaned = 0
    for a in dead:
        if _cleanup_ads_profile(a["ads_profile_id"], account_id=a["id"], reason="cleanup_dead"):
            cleaned += 1
    return {"cleaned": cleaned, "total_dead": len(dead)}


# ── Key Check ──

class CheckRequest(BaseModel):
    account_ids: Optional[list[int]] = None
    concurrency: int = Field(5, ge=1, le=10)
    cleanup_dead: bool = True


@router.post("/keys/check")
def start_key_check(req: CheckRequest) -> JSONDict:
    _check_task_limit()
    task_id = uuid.uuid4().hex[:12]
    state = TaskState(task_id)
    import json as _json
    _register_task(state, task_type="key_check", params_json=_json.dumps({
        "concurrency": req.concurrency,
        "cleanup_dead": req.cleanup_dead,
        "account_ids_count": len(req.account_ids) if req.account_ids else None,
    }))

    def worker() -> None:
        def on_progress(idx: int, total: int, aid: int, status: str) -> None:
            state.push({
                "type": "check_progress",
                "current": idx, "total": total,
                "account_id": aid, "status": status,
            })
        try:
            result = key_checker.batch_check(
                account_ids=req.account_ids,
                on_progress=on_progress,
                concurrency=req.concurrency,
                cleanup_dead=req.cleanup_dead,
                check_abort=lambda: state.cancelled,
            )
            if state.cancelled:
                state.result = result
                state.status = "cancelled"
                state.push({"type": "cancelled", "result": result})
                _finish_task(state)
                return
            state.result = result
            state.status = "success"
            state.push({"type": "done", "result": result})
        except Exception as e:
            if state.cancelled:
                state.result = {}
                state.status = "cancelled"
                state.push({"type": "cancelled", "result": {}})
                _finish_task(state)
                return
            state.error = str(e)
            state.status = "failed"
            state.push({"type": "error", "message": str(e)})
        _finish_task(state)

    start_managed_thread(worker, name=f"key-check-{task_id}")
    return {"task_id": task_id}


@router.get("/keys/check/{task_id}/stream")
async def stream_key_check(task_id: str):
    return EventSourceResponse(_sse_generator(_get_task(task_id)))

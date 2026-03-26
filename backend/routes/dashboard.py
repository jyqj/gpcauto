"""Dashboard & Health 路由。"""

import threading
import time

from typing import Optional

from fastapi import APIRouter, Query

from .. import db
from ..common import JSONDict
from ..services.task_manager import _count_running_tasks, managed_worker_count

router = APIRouter(prefix="/api", tags=["dashboard"])

# 由 server.py lifespan 在 app 启动时设置
_start_time: float = 0.0


def set_start_time(t: float) -> None:
    global _start_time
    _start_time = t


@router.get("/health")
def health_check() -> JSONDict:
    """健康检查：DB 可用性、文件大小、WAL、线程、配置、uptime。"""
    from ..config import ADS_API, TABMAIL_URL, TABMAIL_ADMIN_KEY, TABMAIL_ZONE_ID

    checks: JSONDict = {"status": "ok"}

    # DB 可用性
    try:
        db.get_all_settings()
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"
        checks["status"] = "degraded"

    # DB / WAL 文件大小
    checks["db_sizes"] = db.db_file_sizes()

    # 配置完整性
    config_missing = []
    if not db.get_setting("ads_api", ADS_API):
        config_missing.append("ads_api")
    if not db.get_setting("tabmail_url", TABMAIL_URL):
        config_missing.append("tabmail_url")
    if not db.get_setting("tabmail_admin_key", TABMAIL_ADMIN_KEY):
        config_missing.append("tabmail_admin_key")
    if not db.get_setting("tabmail_zone_id", TABMAIL_ZONE_ID):
        config_missing.append("tabmail_zone_id")
    checks["config_missing"] = config_missing

    # 运行状态
    checks["running_tasks"] = _count_running_tasks()
    checks["worker_threads"] = managed_worker_count()
    checks["total_threads"] = threading.active_count()
    checks["uptime_seconds"] = round(time.monotonic() - _start_time)

    return checks


@router.get("/dashboard/stats")
def get_dashboard_stats() -> JSONDict:
    stats = db.dashboard_stats()
    # 附加最近任务概览
    stats["recent_tasks"] = db.list_task_runs(limit=10)
    return stats


# ─── Task History ─────────────────────────────────────

@router.get("/tasks/history")
def list_task_history(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    type: str = "",
) -> JSONDict:
    runs = db.list_task_runs(limit=limit, offset=offset, task_type=type)
    total = db.count_task_runs(task_type=type)
    return {"runs": runs, "total": total}


# ─── Audit Logs ───────────────────────────────────────

@router.get("/audit-logs")
def list_audit(
    entity_type: str = "",
    entity_id: Optional[int] = None,
    action: str = "",
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> JSONDict:
    logs = db.list_audit_logs(
        entity_type=entity_type, entity_id=entity_id,
        action=action, limit=limit, offset=offset,
    )
    total = db.count_audit_logs(
        entity_type=entity_type, entity_id=entity_id, action=action,
    )
    return {"logs": logs, "total": total}

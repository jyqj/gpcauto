"""API Key 批量测活（并发 + 状态细分 + 可选环境清理）"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Optional

import requests

from . import db, adspower
from .constants import (
    ACCOUNT_STATUS_ACTIVE,
    ACCOUNT_STATUS_DEAD,
    ACCOUNT_STATUS_INVALID,
    ACCOUNT_STATUS_UNKNOWN,
)

log = logging.getLogger("reg.checker")


def check_key(api_key: str) -> str:
    """调用 /v1/models 检测单个 key，返回细分状态。
    只有 401/403 才判 dead；5xx / 网络异常 / 未知状态码一律 unknown，
    避免临时故障误删可用资源。
    """
    if not api_key or not api_key.startswith("sk-"):
        return ACCOUNT_STATUS_INVALID
    try:
        r = requests.get(
            "https://api.openai.com/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15,
        )
        if r.status_code == 200:
            return ACCOUNT_STATUS_ACTIVE
        if r.status_code == 429:
            # 429 仅说明限速，不代表可用（可能欠费限速或配额耗尽）
            return ACCOUNT_STATUS_UNKNOWN
        if r.status_code in (401, 403):
            return ACCOUNT_STATUS_DEAD
        return ACCOUNT_STATUS_UNKNOWN
    except requests.exceptions.Timeout:
        return ACCOUNT_STATUS_UNKNOWN
    except requests.exceptions.ConnectionError:
        return ACCOUNT_STATUS_UNKNOWN
    except Exception:
        return ACCOUNT_STATUS_UNKNOWN


def batch_check(
    account_ids: Optional[list[int]] = None,
    on_progress: Optional[Callable[[int, int, int, str], None]] = None,
    concurrency: int = 5,
    cleanup_dead: bool = True,
    check_abort: Optional[Callable[[], bool]] = None,
) -> dict[str, int]:
    """
    批量检测 key 状态。
    account_ids=None 表示检测全部。
    concurrency: 最大并发数
    cleanup_dead: 自动清理 dead 账号的 AdsPower 环境
    """
    accounts = db.list_accounts()
    if account_ids:
        id_set = set(account_ids)
        accounts = [a for a in accounts if a["id"] in id_set]

    total = len(accounts)
    if total == 0:
        return {
            ACCOUNT_STATUS_ACTIVE: 0,
            ACCOUNT_STATUS_DEAD: 0,
            ACCOUNT_STATUS_INVALID: 0,
            ACCOUNT_STATUS_UNKNOWN: 0,
            "cleaned": 0,
        }

    result = {
        ACCOUNT_STATUS_ACTIVE: 0,
        ACCOUNT_STATUS_DEAD: 0,
        ACCOUNT_STATUS_INVALID: 0,
        ACCOUNT_STATUS_UNKNOWN: 0,
        "cleaned": 0,
    }
    completed = [0]
    lock = threading.Lock()

    def check_one(acc: dict[str, Any]) -> None:
        if check_abort and check_abort():
            return
        status = check_key(acc.get("api_key", ""))
        if check_abort and check_abort():
            return
        db.update_account(acc["id"], status=status, checked_at=db._now())

        if cleanup_dead and status == ACCOUNT_STATUS_DEAD and acc.get("ads_profile_id"):
            ok = adspower.cleanup_profile(acc["ads_profile_id"], account_id=acc["id"], reason="key_check_dead")
            if ok:
                with lock:
                    result["cleaned"] += 1

        with lock:
            result[status] = result.get(status, 0) + 1
            completed[0] += 1
            idx = completed[0]

        if on_progress:
            on_progress(idx, total, acc["id"], status)

        log.info(f"[{idx}/{total}] {acc['email']}: {status}")

    workers = min(concurrency, total)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(check_one, acc) for acc in accounts]
        for f in as_completed(futures):
            if check_abort and check_abort():
                break
            try:
                f.result()
            except Exception as e:
                log.warning(f"检测异常: {e}")

    return result

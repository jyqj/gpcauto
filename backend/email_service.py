"""TabMail 临时邮箱服务"""

import logging
import re
import time
from typing import Callable, Optional

import requests

from .config import (
    TABMAIL_URL as _DEFAULT_URL,
    TABMAIL_ADMIN_KEY as _DEFAULT_ADMIN_KEY,
    TABMAIL_TENANT_ID as _DEFAULT_TENANT_ID,
    TABMAIL_ZONE_ID as _DEFAULT_ZONE_ID,
)

log = logging.getLogger("reg.email")


def _get_cfg(key: str, default: str) -> str:
    try:
        from . import db
        val = db.get_setting(key)
        return val if val is not None else default
    except Exception:
        return default


def _url() -> str:
    return _get_cfg("tabmail_url", _DEFAULT_URL).rstrip("/")


def _zone_id() -> str:
    return _get_cfg("tabmail_zone_id", _DEFAULT_ZONE_ID)


def _headers() -> dict[str, str]:
    return {
        "X-Admin-Key": _get_cfg("tabmail_admin_key", _DEFAULT_ADMIN_KEY),
        "X-Tenant-ID": _get_cfg("tabmail_tenant_id", _DEFAULT_TENANT_ID),
    }


def create_email() -> str:
    r = requests.get(
        f"{_url()}/api/v1/domains/{_zone_id()}/suggest-address",
        params={"subdomain": "true"},
        headers=_headers(),
        timeout=15,
    )
    r.raise_for_status()
    address = r.json()["data"]["address"]
    log.info(f"邮箱: {address}")
    return address


def poll_otp(address: str, timeout: int = 120, interval: float = 4,
            check_abort: Optional[Callable[[], bool]] = None) -> Optional[str]:
    from .register import TaskAborted
    start = time.time()
    while time.time() - start < timeout:
        if check_abort and check_abort():
            raise TaskAborted("任务已中断")
        try:
            r = requests.get(f"{_url()}/api/v1/mailbox/{address}", headers=_headers(), timeout=15)
            if r.status_code == 200:
                for msg in r.json().get("data") or []:
                    # 先查列表里的 subject
                    m = re.search(r"code is (\d{6})", msg.get("subject", ""))
                    if m:
                        return m.group(1)
                    # subject 没匹配到，拉邮件详情查 body
                    msg_id = msg.get("id")
                    if msg_id:
                        try:
                            detail = requests.get(
                                f"{_url()}/api/v1/mailbox/{address}/{msg_id}",
                                headers=_headers(), timeout=15,
                            )
                            if detail.status_code == 200:
                                payload = detail.json()
                                body_data = payload.get("data") or payload
                                for field in ("text_body", "html_body"):
                                    m = re.search(r"code is (\d{6})", body_data.get(field, ""))
                                    if m:
                                        return m.group(1)
                        except Exception as de:
                            log.warning(f"拉取邮件详情异常 ({address}/{msg_id}): {de}")
        except Exception as e:
            log.warning(f"邮箱轮询异常 ({address}): {e}")
        elapsed = int(time.time() - start)
        if elapsed % 12 < interval:
            log.info(f"等待 OTP... ({elapsed}s)")
        end = time.time() + interval
        while time.time() < end:
            if check_abort and check_abort():
                raise TaskAborted("任务已中断")
            time.sleep(min(0.5, end - time.time()))
    return None

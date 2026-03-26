"""AdsPower Local API 封装"""

import logging
import re
from typing import Any, Optional

import requests

from .config import ADS_API as _DEFAULT_API, ADS_API_KEY as _DEFAULT_KEY

log = logging.getLogger("reg.ads")
ADS_CREATE_TIMEOUT = 15
ADS_START_TIMEOUT = 20
ADS_STOP_TIMEOUT = 8
ADS_ACTIVE_TIMEOUT = 5
ADS_DELETE_TIMEOUT = 10


def _get_cfg(key: str, default: str) -> str:
    try:
        from . import db
        val = db.get_setting(key)
        return val if val is not None else default
    except Exception:
        return default


def _api_base() -> str:
    return _get_cfg("ads_api", _DEFAULT_API).rstrip("/")


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_get_cfg('ads_api_key', _DEFAULT_KEY)}"}


# ─── Proxy 解析 ──────────────────────────────────────────

def parse_proxy(proxy_type: str, proxy_str: str) -> Optional[dict[str, str]]:
    """
    将前端传入的代理信息标准化为 dict。
    支持格式:
      - socks5://user:pass@host:port
      - http://host:port
      - host:port:user:pass
      - host:port
    proxy_type: 'direct' / 'socks5' / 'http'
    返回 None 表示直连。
    """
    if proxy_type == "direct" or not proxy_str.strip():
        return None

    s = proxy_str.strip()
    host = port = user = pwd = ""

    url_match = re.match(
        r"^(?:socks5|http|https)://(?:([^:@]+):([^@]+)@)?([^:]+):(\d+)$", s
    )
    if url_match:
        user = url_match.group(1) or ""
        pwd = url_match.group(2) or ""
        host = url_match.group(3)
        port = url_match.group(4)
    else:
        parts = s.replace("@", ":").split(":")
        if len(parts) >= 4:
            host, port, user, pwd = parts[0], parts[1], parts[2], parts[3]
        elif len(parts) >= 2:
            host, port = parts[0], parts[1]
        else:
            host = parts[0]

    return {"type": proxy_type, "host": host, "port": port, "user": user, "pass": pwd}


def _to_ads_proxy(proxy: Optional[dict[str, str]]) -> dict[str, str]:
    """标准化 proxy dict → AdsPower user_proxy_config 格式"""
    if not proxy:
        return {"proxy_soft": "no_proxy"}
    return {
        "proxy_soft": "other",
        "proxy_type": proxy["type"],
        "proxy_host": proxy["host"],
        "proxy_port": proxy["port"],
        "proxy_user": proxy.get("user", ""),
        "proxy_password": proxy.get("pass", ""),
    }


# ─── Profile & Browser ──────────────────────────────────

def create_profile(
    group_id: str = "0",
    proxy: Optional[dict[str, str]] = None,
) -> str:
    body = {
        "group_id": group_id,
        "fingerprint_config": {
            "automatic_timezone": "1",
            "language_switch": "1",
            "webrtc": "disabled",
            "browser_kernel_config": {"type": "chrome", "version": "ua_auto"},
            "random_ua": {
                "ua_system_version": ["Windows 10", "Windows 11",
                                      "Mac OS X 13", "Mac OS X 14", "Mac OS X 15"],
            },
            "screen_resolution": "random",
            "hardware_concurrency": "4",
            "device_memory": "8",
            "canvas": "1",
            "webgl_image": "1",
            "audio": "1",
            "scan_port_type": "1",
        },
        "user_proxy_config": _to_ads_proxy(proxy),
    }
    r = requests.post(
        f"{_api_base()}/api/v1/user/create",
        json=body,
        headers=_headers(),
        timeout=ADS_CREATE_TIMEOUT,
    )
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"AdsPower 创建环境失败: {data}")
    pid = data["data"]["id"]
    log.info(f"profile 创建成功: {pid}")
    return pid


def start_browser(profile_id: str) -> dict[str, Any]:
    r = requests.get(
        f"{_api_base()}/api/v1/browser/start",
        params={"user_id": profile_id, "ip_tab": "0", "cdp_mask": "1"},
        headers=_headers(),
        timeout=ADS_START_TIMEOUT,
    )
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"AdsPower 启动浏览器失败: {data}")
    ws_url = data["data"]["ws"]["puppeteer"]
    debug_port = data["data"]["debug_port"]
    log.info(f"浏览器已启动 debug_port={debug_port}")
    return {"ws_url": ws_url, "debug_port": debug_port, "profile_id": profile_id}


def stop_browser(profile_id: str) -> None:
    try:
        requests.get(
            f"{_api_base()}/api/v1/browser/stop",
            params={"user_id": profile_id},
            headers=_headers(),
            timeout=ADS_STOP_TIMEOUT,
        )
    except Exception as e:
        log.warning(f"stop_browser 异常 ({profile_id}): {e}")


def check_browser_active(profile_id: str) -> Optional[bool]:
    """Poll AdsPower to check if browser is still running.

    Returns True (active), False (confirmed inactive), or None (unknown/error).
    """
    try:
        r = requests.get(
            f"{_api_base()}/api/v1/browser/active",
            params={"user_id": profile_id},
            headers=_headers(),
            timeout=ADS_ACTIVE_TIMEOUT,
        )
        data = r.json()
        if data.get("code") == 0:
            return data.get("data", {}).get("status") == "Active"
        return False
    except Exception:
        return None


def delete_profile(profile_id: str) -> bool:
    """删除 AdsPower 环境。成功或已不存在返回 True，失败抛出异常。"""
    r = requests.post(
        f"{_api_base()}/api/v1/user/delete",
        json={"user_ids": [profile_id]},
        headers=_headers(),
        timeout=ADS_DELETE_TIMEOUT,
    )
    data = r.json()
    code = data.get("code")
    if code == 0:
        log.info(f"profile 已删除: {profile_id}")
        return True
    # AdsPower 返回特定错误码表示 profile 不存在，视为幂等成功
    msg = str(data.get("msg", ""))
    if "not exist" in msg.lower() or "不存在" in msg:
        log.info(f"profile 已不存在，视为删除成功: {profile_id}")
        return True
    raise RuntimeError(f"AdsPower 删除 profile 失败: {data}")


def cleanup_profile(profile_id: str, account_id: Optional[int] = None, reason: str = "") -> bool:
    """统一清理入口：stop + delete + 清空 DB 字段。失败自动入 cleanup queue。

    成功返回 True（DB 中 ads_profile_id 已清空）。
    失败返回 False（已入队，DB 中保留 ads_profile_id 作为追踪锚点）。
    """
    from . import db

    try:
        stop_browser(profile_id)
    except Exception:
        pass
    try:
        delete_profile(profile_id)
        if account_id:
            db.update_account(account_id, ads_profile_id="")
        return True
    except Exception as exc:
        log.warning(f"清理 profile 失败，入队重试: {exc}")
        try:
            db.enqueue_profile_cleanup(profile_id, reason=reason or "cleanup_failed", account_id=account_id)
        except Exception as eq:
            log.error(f"入队也失败，但 DB 仍保留 ads_profile_id={profile_id} 可追踪: {eq}")
        return False

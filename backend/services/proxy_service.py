"""代理解析、测活、缓存、封禁、使用计数等服务。

从 server.py 中拆分出来的代理相关逻辑。
"""

import logging
import random
import re
import socket
import threading
import time
from typing import Any, Callable, Optional

import requests

from .. import db, adspower
from ..common import JSONDict

log = logging.getLogger("proxy_service")

# ─── Proxy resolution helper ─────────────────────────────


def _proxy_from_db(p: JSONDict) -> dict[str, str]:
    return adspower.parse_proxy(
        p["type"],
        f"{p['host']}:{p['port']}:{p['username']}:{p['password']}",
    )


_proxy_cache: dict[int, float] = {}          # {proxy_id: verified_at_timestamp}
_proxy_cache_lock = threading.Lock()
_PROXY_CACHE_TTL = 120           # reuse without re-testing for 2 min

# ── 临时封禁 (替代永久 disable，防止网络抖动导致雪崩) ──
_proxy_temp_ban: dict[int, float] = {}   # {proxy_id: ban_until_timestamp}
_PROXY_BAN_TTL = 300                      # 5 分钟自动解封

# ── 使用计数 (均匀分配) ──
_proxy_usage: dict[int, int] = {}         # {proxy_id: usage_count}


def _cache_proxy(proxy_id: int) -> None:
    with _proxy_cache_lock:
        _proxy_cache[proxy_id] = time.time()


def _is_proxy_cached(proxy_id: int) -> bool:
    with _proxy_cache_lock:
        t = _proxy_cache.get(proxy_id)
        if t and (time.time() - t) < _PROXY_CACHE_TTL:
            return True
        _proxy_cache.pop(proxy_id, None)
        return False


def _invalidate_proxy_cache(proxy_id: int) -> None:
    with _proxy_cache_lock:
        _proxy_cache.pop(proxy_id, None)
        _proxy_usage.pop(proxy_id, None)
        _proxy_temp_ban.pop(proxy_id, None)


def _invalidate_proxy_cache_all() -> None:
    with _proxy_cache_lock:
        _proxy_cache.clear()
        _proxy_usage.clear()
        _proxy_temp_ban.clear()


def _temp_ban_proxy(proxy_id: int) -> None:
    """临时封禁代理（内存级，自动过期）。"""
    with _proxy_cache_lock:
        _proxy_temp_ban[proxy_id] = time.time() + _PROXY_BAN_TTL
        _proxy_cache.pop(proxy_id, None)


def _is_temp_banned(proxy_id: int) -> bool:
    with _proxy_cache_lock:
        ban_until = _proxy_temp_ban.get(proxy_id)
        if ban_until and time.time() < ban_until:
            return True
        _proxy_temp_ban.pop(proxy_id, None)
        return False


def _track_proxy_usage(proxy_id: int) -> None:
    with _proxy_cache_lock:
        _proxy_usage[proxy_id] = _proxy_usage.get(proxy_id, 0) + 1


def _get_proxy_usage(proxy_id: int) -> int:
    with _proxy_cache_lock:
        return _proxy_usage.get(proxy_id, 0)


def _cleanup_proxy_caches() -> None:
    """Remove expired entries from proxy caches."""
    with _proxy_cache_lock:
        now = time.time()
        for pid in [k for k, t in _proxy_cache.items() if now - t >= _PROXY_CACHE_TTL]:
            del _proxy_cache[pid]
        for pid in [k for k, t in _proxy_temp_ban.items() if now >= t]:
            del _proxy_temp_ban[pid]


def _resolve_proxy(
    proxy_type: str,
    proxy_str: str = "",
    proxy_id: Optional[int] = None,
    on_log: Optional[Callable[[str], None]] = None,
    check_abort: Optional[Callable[[], bool]] = None,
) -> Optional[dict[str, str]]:
    """Resolve proxy with health-check + caching.
    Recently verified proxies are reused without re-testing."""

    def _log(msg: str) -> None:
        log.info(msg)
        if on_log:
            on_log(msg)

    def _abort() -> None:
        if check_abort and check_abort():
            from ..register import TaskAborted
            raise TaskAborted("任务已中断")

    if proxy_type == "direct":
        return None

    if proxy_id:
        p = db.get_proxy(proxy_id)
        if not p:
            raise RuntimeError(f"代理 #{proxy_id} 不存在")
        if _is_proxy_cached(p["id"]):
            _log(f"代理 #{p['id']} (缓存命中)")
            _track_proxy_usage(p["id"])
            return _proxy_from_db(p)
        _abort()
        _log(f"测试代理 #{p['id']} ({p['host']}:{p['port']})...")
        alive, latency, err = _test_proxy(_build_proxy_url(p))
        if not alive:
            _temp_ban_proxy(p["id"])
            raise RuntimeError(f"代理 #{p['id']} 不可用 (临时封禁 {_PROXY_BAN_TTL}s): {err}")
        _cache_proxy(p["id"])
        _track_proxy_usage(p["id"])
        _log(f"代理 #{p['id']} 可用 ({latency}ms)")
        return _proxy_from_db(p)

    if proxy_type == "pool":
        available = db.list_available_proxies()
        # 排除临时封禁的代理
        available = [p for p in available if not _is_temp_banned(p["id"])]
        if not available:
            raise RuntimeError("代理池为空或所有代理已封禁，请添加新的代理")

        # 优先返回已缓存的代理中使用次数最少的（均匀分配）
        cached = [p for p in available if _is_proxy_cached(p["id"])]
        if cached:
            pick = min(cached, key=lambda p: (_get_proxy_usage(p["id"]), random.random()))
            _log(f"代理 #{pick['id']} (缓存命中)")
            _track_proxy_usage(pick["id"])
            return _proxy_from_db(pick)

        # 按使用次数排序，优先测试用得少的
        available.sort(key=lambda p: (_get_proxy_usage(p["id"]), random.random()))
        for p in available:
            _abort()
            _log(f"测试代理 #{p['id']} ({p['host']}:{p['port']})...")
            alive, latency, err = _test_proxy(_build_proxy_url(p))
            if alive:
                _cache_proxy(p["id"])
                _track_proxy_usage(p["id"])
                _log(f"代理 #{p['id']} 可用 ({latency}ms)")
                return _proxy_from_db(p)
            _temp_ban_proxy(p["id"])
            _log(f"代理 #{p['id']} 不可用 (临时封禁 {_PROXY_BAN_TTL}s)")
        raise RuntimeError("所有代理均不可用，请添加新的代理")

    return adspower.parse_proxy(proxy_type, proxy_str)


def _build_proxy_url(p: JSONDict) -> str:
    scheme = "socks5h" if p["type"] == "socks5" else "http"
    user, pwd = p.get("username", ""), p.get("password", "")
    auth = f"{user}:{pwd}@" if user else ""
    return f"{scheme}://{auth}{p['host']}:{p['port']}"


_TEST_URLS = [
    "https://api.openai.com/v1/models",
    "https://httpbin.org/ip",
]


def _test_proxy(proxy_url: str) -> tuple[bool, int, Optional[str]]:
    """Test proxy connectivity. Returns (alive, latency_ms, error_msg).
    Any HTTP response (even 401/403) = alive. Only connection failures = dead."""
    proxies = {"https": proxy_url, "http": proxy_url}
    last_err = ""
    for url in _TEST_URLS:
        try:
            start = time.time()
            r = requests.get(url, proxies=proxies, timeout=15,
                             headers={"User-Agent": "Mozilla/5.0"})
            latency = int((time.time() - start) * 1000)
            return True, latency, None
        except (requests.exceptions.ProxyError, requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError) as e:
            err = str(e)
            if "Missing dependencies" in err or "pip install" in err:
                return _test_proxy_tcp(proxy_url)
            last_err = err[:120]
        except Exception as e:
            last_err = str(e)[:120]
    label = "超时" if "timeout" in last_err.lower() else "连接失败"
    return False, 0, f"{label}: {last_err}"


def _test_proxy_tcp(proxy_url: str) -> tuple[bool, int, Optional[str]]:
    """Fallback: TCP-level reachability test when PySocks is not installed."""
    m = re.search(r'@?([^:@/]+):(\d+)$', proxy_url.split('://')[-1].split('@')[-1])
    if not m:
        return False, 0, "无法解析代理地址"
    host, port = m.group(1), int(m.group(2))
    try:
        start = time.time()
        sock = socket.create_connection((host, port), timeout=10)
        latency = int((time.time() - start) * 1000)
        sock.close()
        return True, latency, None
    except socket.timeout:
        return False, 0, "TCP 连接超时"
    except OSError as e:
        return False, 0, f"TCP 连接失败: {e}"

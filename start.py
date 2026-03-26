#!/usr/bin/env python3
"""
GPT Platform 启动器

首次使用 (macOS):
  1. 先把整个文件夹移到「桌面」或「文稿」
  2. 打开「终端」，把 start.py 拖进终端窗口，按回车
  → 之后会生成「启动服务.command」，以后双击它即可

首次使用 (Windows):
  直接双击 start.bat
"""

import subprocess
import sys
import os
import time
import socket
import webbrowser
import platform

PORT = 9800
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
MIN_PYTHON = (3, 9)

PACKAGES = [
    "fastapi", "uvicorn", "sse-starlette",
    "requests[socks]", "playwright", "pydantic",
]

def color(text, code):
    if platform.system() == "Windows":
        return text
    return f"\033[{code}m{text}\033[0m"

def banner():
    os.system("cls" if platform.system() == "Windows" else "clear")
    print()
    print(color("  ┌──────────────────────────────────┐", "1;36"))
    print(color("  │      GPT Platform  v1.0          │", "1;36"))
    print(color(f"  │      http://localhost:{PORT}        │", "1;36"))
    print(color("  └──────────────────────────────────┘", "1;36"))
    print()

def is_translocated():
    return platform.system() == "Darwin" and "AppTranslocation" in PROJECT_DIR

def port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0

def check_deps():
    missing = []
    for mod in ["fastapi", "uvicorn", "sse_starlette", "requests", "pydantic", "playwright"]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    return missing

def install_deps():
    print(color("▸ 安装依赖...", "36"))
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install"] + PACKAGES + ["--quiet"]
    )

def open_browser():
    time.sleep(1.5)
    webbrowser.open(f"http://localhost:{PORT}")

def ensure_playwright_browser():
    """Ensure Playwright Chromium browser is downloaded (idempotent)."""
    print(color("▸ 检查 Playwright 浏览器...", "36"))
    try:
        result = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            timeout=180,
        )
        if result.returncode == 0:
            print(color("✓ Playwright 浏览器就绪", "32"))
            return True
        print(color("✕ Playwright 浏览器安装失败", "31"))
        print(color(f"  请手动运行: {sys.executable} -m playwright install chromium", "33"))
        return False
    except subprocess.TimeoutExpired:
        print(color("✕ Playwright 浏览器安装超时", "31"))
        print(color(f"  请手动运行: {sys.executable} -m playwright install chromium", "33"))
        return False


def preflight_check():
    """启动前检查外部依赖可达性和配置完整性（仅警告，不阻塞启动）。"""
    import importlib
    warnings = []

    # 检查 DB 目录可写
    data_dir = os.path.join(PROJECT_DIR, "data")
    os.makedirs(data_dir, exist_ok=True)
    test_file = os.path.join(data_dir, ".write_test")
    try:
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
    except OSError:
        warnings.append("数据目录不可写: " + data_dir)

    # 检查配置完整性
    try:
        config = importlib.import_module("backend.config")
        if not getattr(config, "ADS_API", ""):
            warnings.append("未配置 ADS_API")
        if not getattr(config, "TABMAIL_URL", ""):
            warnings.append("未配置 TABMAIL_URL")
        if not getattr(config, "TABMAIL_ADMIN_KEY", ""):
            warnings.append("未配置 TABMAIL_ADMIN_KEY")
        if not getattr(config, "TABMAIL_ZONE_ID", ""):
            warnings.append("未配置 TABMAIL_ZONE_ID")

        # 检查 AdsPower 连通性
        ads_api = getattr(config, "ADS_API", "")
        if ads_api:
            try:
                import requests as _req
                r = _req.get(f"{ads_api}/status", timeout=5)
                if r.status_code == 200:
                    print(color("✓ AdsPower API 可达", "32"))
                else:
                    warnings.append(f"AdsPower API 返回 HTTP {r.status_code}")
            except Exception:
                warnings.append(f"AdsPower API 不可达 ({ads_api})")

        # 检查 TabMail 连通性
        tabmail_url = getattr(config, "TABMAIL_URL", "")
        if tabmail_url:
            try:
                import requests as _req
                r = _req.get(f"{tabmail_url}/api/v1/health", timeout=5)
                if r.status_code in (200, 404):  # 404 说明服务在跑但没 health 端点
                    print(color("✓ TabMail API 可达", "32"))
                else:
                    warnings.append(f"TabMail API 返回 HTTP {r.status_code}")
            except Exception:
                warnings.append(f"TabMail API 不可达 ({tabmail_url})")
    except ImportError:
        warnings.append("无法导入 backend.config，跳过配置检查")

    if warnings:
        print(color("▸ 预检警告 (不影响启动):", "33"))
        for w in warnings:
            print(color(f"  ⚠ {w}", "33"))
        print()
    else:
        print(color("✓ 预检通过", "32"))


def ensure_launcher():
    if platform.system() != "Darwin":
        return
    launcher = os.path.join(PROJECT_DIR, "启动服务.command")
    if os.path.exists(launcher):
        return
    py_path = sys.executable
    script = f'#!/bin/bash\ncd "$(dirname "$0")"\n"{py_path}" start.py\n'
    with open(launcher, "w") as f:
        f.write(script)
    os.chmod(launcher, 0o755)
    print(color("✓ 已生成「启动服务.command」— 以后双击它即可启动", "1;32"))
    print()

def main():
    banner()

    if is_translocated():
        print(color("✕ 检测到 macOS 安全隔离 (App Translocation)", "1;31"))
        print()
        print(color("  请先将整个项目文件夹移动到「桌面」或「文稿」，", "33"))
        print(color("  然后重新运行 start.py", "33"))
        print()
        print(color("  原因: macOS 会将下载目录中的程序隔离到临时路径，", "90"))
        print(color("  移动后即可正常运行。", "90"))
        print()
        input("按回车退出...")
        return

    print(color(f"▸ Python {platform.python_version()}  ({sys.executable})", "36"))
    if sys.version_info < MIN_PYTHON:
        print(color(f"✕ 需要 Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ 才能运行", "31"))
        if sys.stdin and sys.stdin.isatty():
            input("\n按回车退出...")
        return

    missing = check_deps()
    if missing:
        print(color(f"▸ 缺少依赖: {', '.join(missing)}", "33"))
        try:
            install_deps()
        except subprocess.CalledProcessError:
            print(color("✕ 依赖安装失败，请手动运行:", "31"))
            print(color(f"  pip install {' '.join(PACKAGES)}", "33"))
            input("\n按回车退出...")
            return
        print(color("✓ 依赖安装完成", "32"))

    if not ensure_playwright_browser():
        if sys.stdin and sys.stdin.isatty():
            input("\n按回车退出...")
        return

    ensure_launcher()

    if port_in_use(PORT):
        print(color(f"▸ 端口 {PORT} 已被占用，尝试打开浏览器...", "33"))
        webbrowser.open(f"http://localhost:{PORT}")
        input("\n按回车退出...")
        return

    preflight_check()

    print(color(f"▸ 启动服务 (端口 {PORT})...", "32"))
    print(color("  Ctrl+C 停止服务\n", "36"))

    import threading
    threading.Thread(target=open_browser, daemon=True).start()

    try:
        subprocess.run(
            [sys.executable, "-m", "uvicorn", "backend.server:app",
             "--host", "0.0.0.0", "--port", str(PORT), "--log-level", "info"],
            cwd=PROJECT_DIR,
        )
    except KeyboardInterrupt:
        pass

    print(color("\n服务已停止", "31"))
    if sys.stdin and sys.stdin.isatty():
        input("按回车关闭窗口...")

if __name__ == "__main__":
    main()

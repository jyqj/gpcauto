"""Shared fixtures — 使用内存 SQLite，不影响生产数据。"""

import os
import sys
import tempfile

import pytest

# 确保项目根目录在 sys.path
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture(autouse=True)
def _isolate_db(monkeypatch, tmp_path):
    """每个测试用独立的临时 DB，隔离副作用。"""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setattr("backend.config.DB_PATH", db_path)
    monkeypatch.setattr("backend.db.DB_PATH", db_path)

    from backend import db
    db.init_db()
    yield


@pytest.fixture()
def client(_isolate_db):
    """FastAPI TestClient — 每个测试函数独享。"""
    from fastapi.testclient import TestClient
    from backend.server import app
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

"""API 冒烟测试 — 只验证核心只读端点可正常响应。"""


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["db"] == "ok"
    assert isinstance(data["running_tasks"], int)
    # P5: 增强字段
    assert "db_sizes" in data
    assert "db" in data["db_sizes"]
    assert isinstance(data["worker_threads"], int)
    assert isinstance(data["total_threads"], int)
    assert isinstance(data["uptime_seconds"], int)


def test_dashboard_stats(client):
    r = client.get("/api/dashboard/stats")
    assert r.status_code == 200
    data = r.json()
    assert "accounts" in data
    assert "cards" in data
    assert "proxies" in data


def test_list_accounts(client):
    r = client.get("/api/accounts")
    assert r.status_code == 200
    data = r.json()
    assert "accounts" in data
    # 不传分页参数时不应返回 total
    assert "total" not in data


def test_list_accounts_paginated(client):
    r = client.get("/api/accounts?page=1&page_size=10")
    assert r.status_code == 200
    data = r.json()
    assert "accounts" in data
    assert data["total"] == 0
    assert data["page"] == 1
    assert data["page_size"] == 10


def test_list_cards(client):
    r = client.get("/api/cards")
    assert r.status_code == 200
    data = r.json()
    assert "cards" in data
    assert "total" not in data


def test_list_cards_paginated(client):
    r = client.get("/api/cards?page=1&page_size=5")
    assert r.status_code == 200
    data = r.json()
    assert "total" in data
    assert data["page"] == 1


def test_list_proxies(client):
    r = client.get("/api/proxies")
    assert r.status_code == 200
    data = r.json()
    assert "proxies" in data
    assert "total" not in data


def test_list_proxies_paginated(client):
    r = client.get("/api/proxies?page=1&page_size=20")
    assert r.status_code == 200
    data = r.json()
    assert "total" in data


def test_proxy_check_revives_disabled_proxy(client, monkeypatch):
    from backend import db
    from backend.services import task_manager

    proxy_id = db.insert_proxy({
        "type": "socks5",
        "host": "127.0.0.1",
        "port": "1080",
        "username": "",
        "password": "",
        "label": "",
    })
    db.batch_update_proxy_status([proxy_id], "disabled")

    monkeypatch.setattr("backend.routes.proxies._test_proxy", lambda _proxy_url: (True, 123, None))
    monkeypatch.setattr(
        "backend.routes.proxies.start_managed_thread",
        lambda target, *args, **kwargs: target(*args),
    )

    r = client.post("/api/proxies/check", json={"ids": [proxy_id], "concurrency": 1})
    assert r.status_code == 200

    state = task_manager._get_task(r.json()["task_id"])
    assert state.result == {"alive": 1, "dead": 0, "recovered": 1, "total": 1}
    assert db.get_proxy(proxy_id)["status"] == "available"


def test_pagination_negative_values_rejected(client):
    """负数分页参数应返回 422。"""
    assert client.get("/api/accounts?page=-1&page_size=10").status_code == 422
    assert client.get("/api/cards?page=1&page_size=-5").status_code == 422
    assert client.get("/api/proxies?page=-1&page_size=-1").status_code == 422


def test_pagination_partial_params_rejected(client):
    """只传 page 不传 page_size（或反过来）应返回 422。"""
    assert client.get("/api/accounts?page=1").status_code == 422
    assert client.get("/api/cards?page_size=10").status_code == 422


def test_get_settings(client):
    r = client.get("/api/settings")
    assert r.status_code == 200
    data = r.json()
    assert "settings" in data
    assert "ads_api" in data["settings"]


def test_random_address(client):
    r = client.get("/api/addresses/random?tax_free_only=true")
    assert r.status_code == 200
    addr = r.json().get("address")
    assert addr is not None
    assert addr["state"] in ("AK", "DE", "MT", "NH", "OR")


def test_states_list(client):
    r = client.get("/api/addresses/states?tax_free_only=true")
    assert r.status_code == 200
    states = r.json()["states"]
    assert len(states) == 5


def test_card_preview(client):
    r = client.post("/api/cards/preview", json={
        "raw": "4111111111111111|12/2028|123|John Doe\nbadline"
    })
    assert r.status_code == 200
    results = r.json()["results"]
    assert results[0]["status"] == "ok"
    assert results[1]["status"] == "fail"


def test_reroll_street(client):
    r = client.get("/api/addresses/reroll-street")
    assert r.status_code == 200
    assert "address_line1" in r.json()


# ── 服务端搜索/过滤 ──

def test_accounts_search(client):
    r = client.get("/api/accounts?page=1&page_size=50&search=nonexistent")
    assert r.status_code == 200
    assert r.json()["total"] == 0


def test_cards_filter_by_status(client):
    r = client.get("/api/cards?page=1&page_size=50&status=disabled")
    assert r.status_code == 200
    assert r.json()["total"] == 0


def test_proxies_filter_by_type(client):
    r = client.get("/api/proxies?page=1&page_size=50&type=socks5")
    assert r.status_code == 200
    assert r.json()["total"] == 0


# ── 任务历史 & 审计日志 ──

def test_task_history(client):
    r = client.get("/api/tasks/history")
    assert r.status_code == 200
    data = r.json()
    assert "runs" in data
    assert "total" in data
    assert data["total"] == 0


def test_audit_logs(client):
    r = client.get("/api/audit-logs")
    assert r.status_code == 200
    data = r.json()
    assert "logs" in data
    assert "total" in data


def test_dashboard_includes_recent_tasks(client):
    r = client.get("/api/dashboard/stats")
    assert r.status_code == 200
    data = r.json()
    assert "recent_tasks" in data
    assert isinstance(data["recent_tasks"], list)


def test_audit_log_created_on_batch_delete(client):
    """批量删除卡片时应产生审计日志。"""
    # 先添加一张卡
    client.post("/api/cards", json={"number": "4111111111111111"})
    cards = client.get("/api/cards").json()["cards"]
    assert len(cards) > 0
    card_id = cards[0]["id"]
    # 批量删除
    client.post("/api/cards/batch-delete", json={"ids": [card_id]})
    # 检查审计日志
    r = client.get("/api/audit-logs?entity_type=card&action=batch_delete")
    assert r.status_code == 200
    assert r.json()["total"] >= 1

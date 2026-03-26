"""T2: 输入模型校验测试。"""


class TestRegisterValidation:

    def test_invalid_proxy_type(self, client):
        r = client.post("/api/register", json={"proxy_type": "invalid", "card_ids": [1]})
        assert r.status_code == 422

    def test_invalid_bind_path(self, client):
        r = client.post("/api/register", json={"bind_path": "invalid", "card_ids": [1]})
        assert r.status_code == 422

    def test_invalid_credit_amount_low(self, client):
        r = client.post("/api/register", json={"credit_amount": 1, "card_ids": [1]})
        assert r.status_code == 422

    def test_invalid_credit_amount_high(self, client):
        r = client.post("/api/register", json={"credit_amount": 999, "card_ids": [1]})
        assert r.status_code == 422

    def test_invalid_concurrency(self, client):
        r = client.post("/api/register", json={"concurrency": 0, "card_ids": [1]})
        assert r.status_code == 422

    def test_invalid_recharge_tier(self, client):
        r = client.post("/api/register", json={"recharge_upper": 15, "card_ids": [1]})
        assert r.status_code == 422

    def test_manual_with_high_concurrency(self, client):
        r = client.post("/api/register", json={
            "recharge_strategy": "manual", "concurrency": 3, "card_ids": [1],
        })
        assert r.status_code == 422


class TestCardValidation:

    def test_invalid_card_number_letters(self, client):
        r = client.post("/api/cards", json={"number": "4111abcd1111"})
        assert r.status_code == 422

    def test_card_number_too_short(self, client):
        r = client.post("/api/cards", json={"number": "411111"})
        assert r.status_code == 422

    def test_invalid_exp_month(self, client):
        r = client.post("/api/cards", json={"number": "4111111111111111", "exp_month": "13"})
        assert r.status_code == 422

    def test_invalid_cvv(self, client):
        r = client.post("/api/cards", json={"number": "4111111111111111", "cvv": "12"})
        assert r.status_code == 422

    def test_save_batch_expired_card_no_500(self, client):
        r = client.post("/api/cards/save-batch", json={
            "cards": [{"number": "4242424242424243", "exp_month": "01", "exp_year": "2020", "cvv": "123"}]
        })
        assert r.status_code == 200
        body = r.json()
        assert body["imported"] == 0
        assert body["expired"] == 1


class TestProxyValidation:

    def test_missing_host(self, client):
        r = client.post("/api/proxies", json={"port": "1080"})
        assert r.status_code == 422

    def test_invalid_port(self, client):
        r = client.post("/api/proxies", json={"host": "1.2.3.4", "port": "abc"})
        assert r.status_code == 422

    def test_invalid_type(self, client):
        r = client.post("/api/proxies", json={"host": "1.2.3.4", "port": "1080", "type": "ftp"})
        assert r.status_code == 422


class TestKeyCheckValidation:

    def test_invalid_concurrency_low(self, client):
        r = client.post("/api/keys/check", json={"concurrency": 0})
        assert r.status_code == 422

    def test_invalid_concurrency_high(self, client):
        r = client.post("/api/keys/check", json={"concurrency": 99})
        assert r.status_code == 422

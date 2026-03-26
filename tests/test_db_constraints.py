"""DB 唯一约束测试。"""

import sqlite3

import pytest

from backend import db


class TestCardUnique:

    def test_duplicate_card_number_rejected(self):
        """相同卡号第二次插入应抛出 DuplicateRecordError。"""
        db.insert_card({"number": "4111111111111111", "exp_month": "12", "exp_year": "2028", "cvv": "123"})
        with pytest.raises(db.DuplicateRecordError, match="cards"):
            db.insert_card({"number": "4111111111111111", "exp_month": "01", "exp_year": "2030", "cvv": "456"})

    def test_different_card_numbers_ok(self):
        id1 = db.insert_card({"number": "4111111111111111"})
        id2 = db.insert_card({"number": "5500000000000004"})
        assert id1 != id2


class TestProxyUnique:

    def test_duplicate_proxy_rejected(self):
        """相同 type+host+port+username 应抛出 DuplicateRecordError。"""
        db.insert_proxy({"type": "socks5", "host": "1.2.3.4", "port": "1080", "username": "u1", "password": "p1"})
        with pytest.raises(db.DuplicateRecordError, match="proxies"):
            db.insert_proxy({"type": "socks5", "host": "1.2.3.4", "port": "1080", "username": "u1", "password": "p2"})

    def test_different_port_ok(self):
        id1 = db.insert_proxy({"type": "socks5", "host": "1.2.3.4", "port": "1080", "username": "u1"})
        id2 = db.insert_proxy({"type": "socks5", "host": "1.2.3.4", "port": "1081", "username": "u1"})
        assert id1 != id2

    def test_batch_insert_skips_duplicates(self):
        db.insert_proxy({"type": "socks5", "host": "1.2.3.4", "port": "1080", "username": ""})
        ids, skipped = db.batch_insert_proxies([
            {"type": "socks5", "host": "1.2.3.4", "port": "1080", "username": ""},  # dup
            {"type": "http", "host": "5.6.7.8", "port": "8080", "username": ""},     # new
        ])
        assert len(ids) == 1
        assert skipped == 1


class TestAccountUnique:

    def test_duplicate_email_rejected(self):
        """相同 email 第二次插入应抛出 DuplicateRecordError。"""
        db.insert_account({"email": "test@example.com"})
        with pytest.raises(db.DuplicateRecordError, match="accounts"):
            db.insert_account({"email": "test@example.com"})

    def test_empty_api_key_allowed(self):
        """空 api_key 不应触发唯一约束。"""
        a1 = db.insert_account({"email": "a@test.com", "api_key": ""})
        a2 = db.insert_account({"email": "b@test.com", "api_key": ""})
        assert a1 != a2

    def test_duplicate_api_key_rejected(self):
        """相同非空 api_key 第二次插入应抛出。"""
        db.insert_account({"email": "c@test.com", "api_key": "sk-test123"})
        with pytest.raises(db.DuplicateRecordError):
            db.insert_account({"email": "d@test.com", "api_key": "sk-test123"})


class TestUniqueConstraintRepair:

    def test_migration_repairs_existing_duplicates(self):
        conn = sqlite3.connect(db.DB_PATH)
        conn.execute("DROP INDEX IF EXISTS idx_cards_number_unique")
        conn.execute("DROP INDEX IF EXISTS idx_accounts_email_unique")
        conn.execute("DROP INDEX IF EXISTS idx_accounts_apikey_unique")
        conn.execute("DROP INDEX IF EXISTS idx_proxies_unique")

        conn.execute("INSERT INTO cards (number, created_at) VALUES (?, ?)", ("4999999999999999", db._now()))
        conn.execute("INSERT INTO cards (number, created_at) VALUES (?, ?)", ("4999999999999999", db._now()))
        conn.execute("INSERT INTO accounts (email, api_key, notes, registered_at) VALUES (?, '', '', ?)", ("dup@test.com", db._now()))
        conn.execute("INSERT INTO accounts (email, api_key, notes, registered_at) VALUES (?, '', '', ?)", ("dup@test.com", db._now()))
        conn.execute("INSERT INTO accounts (email, api_key, notes, registered_at) VALUES (?, ?, '', ?)", ("k1@test.com", "sk-dup", db._now()))
        conn.execute("INSERT INTO accounts (email, api_key, notes, registered_at) VALUES (?, ?, '', ?)", ("k2@test.com", "sk-dup", db._now()))
        conn.execute(
            "INSERT INTO proxies (type, host, port, username, created_at) VALUES ('socks5', '9.9.9.9', '1080', 'u', ?)",
            (db._now(),),
        )
        conn.execute(
            "INSERT INTO proxies (type, host, port, username, created_at) VALUES ('socks5', '9.9.9.9', '1080', 'u', ?)",
            (db._now(),),
        )
        conn.commit()
        conn.close()

        db._migrate_unique_constraints()

        conn = sqlite3.connect(db.DB_PATH)
        assert conn.execute("SELECT COUNT(*) FROM cards WHERE number='4999999999999999'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM accounts WHERE email='dup@test.com'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM accounts WHERE api_key='sk-dup'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM proxies WHERE type='socks5' AND host='9.9.9.9' AND port='1080' AND username='u'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_cards_number_unique'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_accounts_email_unique'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_accounts_apikey_unique'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_proxies_unique'").fetchone()[0] == 1
        conn.close()

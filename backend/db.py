"""SQLite 数据层 — accounts / cards / proxies / settings"""

import json
import logging
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Generator, Optional

from .config import DB_PATH, ACCOUNTS_FILE
from .constants import (
    ACCOUNT_STATUS_UNKNOWN,
    CARD_STATUS_AVAILABLE,
    CARD_STATUS_DISABLED,
    PROXY_STATUS_AVAILABLE,
    PROXY_STATUS_DISABLED,
    SALE_STATUS_PENDING,
    SALE_STATUS_RECYCLED,
    SALE_STATUS_SOLD,
    SALE_STATUS_UNSOLD,
)

log = logging.getLogger("db")


class DuplicateRecordError(Exception):
    """唯一约束冲突时抛出的业务异常。"""
    def __init__(self, table: str, detail: str = "") -> None:
        self.table = table
        self.detail = detail
        super().__init__(f"{table}: 重复记录 — {detail}" if detail else f"{table}: 重复记录")


_CREATE_TABLES_SQL = f"""
CREATE TABLE IF NOT EXISTS cards (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    number          TEXT NOT NULL,
    exp_month       TEXT NOT NULL DEFAULT '',
    exp_year        TEXT NOT NULL DEFAULT '',
    cvv             TEXT NOT NULL DEFAULT '',
    holder_name     TEXT NOT NULL DEFAULT '',
    address_line1   TEXT NOT NULL DEFAULT '',
    city            TEXT NOT NULL DEFAULT '',
    state           TEXT NOT NULL DEFAULT '',
    zip             TEXT NOT NULL DEFAULT '',
    country         TEXT NOT NULL DEFAULT 'US',
    status          TEXT NOT NULL DEFAULT '{CARD_STATUS_AVAILABLE}',
    fail_tag        TEXT NOT NULL DEFAULT '',
    use_count       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS accounts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    email           TEXT NOT NULL,
    password        TEXT NOT NULL DEFAULT '',
    api_key         TEXT NOT NULL DEFAULT '',
    device_id       TEXT NOT NULL DEFAULT '',
    name            TEXT NOT NULL DEFAULT '',
    birthday        TEXT NOT NULL DEFAULT '',
    org_name        TEXT NOT NULL DEFAULT '',
    ads_profile_id  TEXT NOT NULL DEFAULT '',
    card_id         INTEGER REFERENCES cards(id) ON DELETE SET NULL,
    status          TEXT NOT NULL DEFAULT '{ACCOUNT_STATUS_UNKNOWN}',
    credit_loaded   REAL NOT NULL DEFAULT 0,
    sale_status     TEXT NOT NULL DEFAULT '{SALE_STATUS_UNSOLD}',
    sold_at         TEXT NOT NULL DEFAULT '',
    registered_at   TEXT NOT NULL DEFAULT '',
    checked_at      TEXT NOT NULL DEFAULT '',
    notes           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS proxies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    label           TEXT NOT NULL DEFAULT '',
    type            TEXT NOT NULL DEFAULT 'socks5',
    host            TEXT NOT NULL DEFAULT '',
    port            TEXT NOT NULL DEFAULT '',
    username        TEXT NOT NULL DEFAULT '',
    password        TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT '{PROXY_STATUS_AVAILABLE}',
    created_at      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS settings (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS profile_cleanup_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id      TEXT NOT NULL,
    account_id      INTEGER,
    reason          TEXT NOT NULL DEFAULT '',
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TEXT NOT NULL DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS task_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL,
    type            TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'running',
    success_count   INTEGER NOT NULL DEFAULT 0,
    failed_count    INTEGER NOT NULL DEFAULT 0,
    error_summary   TEXT NOT NULL DEFAULT '',
    params_json     TEXT NOT NULL DEFAULT '',
    started_at      TEXT NOT NULL DEFAULT '',
    finished_at     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type     TEXT NOT NULL,
    entity_id       INTEGER,
    action          TEXT NOT NULL,
    detail          TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_cards_status ON cards(status);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);
CREATE INDEX IF NOT EXISTS idx_accounts_card_id ON accounts(card_id);
CREATE INDEX IF NOT EXISTS idx_cleanup_queue_status ON profile_cleanup_queue(status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cleanup_queue_pending_profile
    ON profile_cleanup_queue(profile_id) WHERE status='pending';
CREATE INDEX IF NOT EXISTS idx_task_runs_started ON task_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_task_runs_type ON task_runs(type);
CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON audit_logs(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs(created_at);
"""

_CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_cards_fail_tag ON cards(fail_tag);
CREATE INDEX IF NOT EXISTS idx_accounts_sale_status ON accounts(sale_status);
"""


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@contextmanager
def _conn() -> Generator[sqlite3.Connection, None, None]:
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    c.execute("PRAGMA busy_timeout=15000")
    try:
        yield c
        c.commit()
    finally:
        c.close()


def wal_checkpoint() -> dict[str, Any]:
    """执行 WAL checkpoint（PASSIVE 模式，不阻塞写入）并返回状态。"""
    with _conn() as c:
        row = c.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        # row: (busy, log_pages, checkpointed_pages)
        return {"busy": row[0], "log_pages": row[1], "checkpointed": row[2]}


def db_file_sizes() -> dict[str, int]:
    """返回主库和 WAL 文件大小（字节）。"""
    sizes: dict[str, int] = {}
    for suffix, key in [("", "db"), ("-wal", "wal"), ("-shm", "shm")]:
        path = DB_PATH + suffix
        try:
            sizes[key] = os.path.getsize(path)
        except OSError:
            sizes[key] = 0
    return sizes


def init_db() -> None:
    with _conn() as c:
        c.executescript(_CREATE_TABLES_SQL)
    _migrate_schema()
    with _conn() as c:
        c.executescript(_CREATE_INDEXES_SQL)
    _migrate_jsonl()


def _migrate_schema() -> None:
    """Add columns introduced in v2 (sale lifecycle + card fail tags)."""
    with _conn() as c:
        acct_cols = {r[1] for r in c.execute("PRAGMA table_info(accounts)").fetchall()}
        if "sale_status" not in acct_cols:
            c.execute(f"ALTER TABLE accounts ADD COLUMN sale_status TEXT NOT NULL DEFAULT '{SALE_STATUS_UNSOLD}'")
        if "sold_at" not in acct_cols:
            c.execute("ALTER TABLE accounts ADD COLUMN sold_at TEXT NOT NULL DEFAULT ''")

        if "credit_loaded" not in acct_cols:
            c.execute("ALTER TABLE accounts ADD COLUMN credit_loaded REAL NOT NULL DEFAULT 0")

        card_cols = {r[1] for r in c.execute("PRAGMA table_info(cards)").fetchall()}
        if "fail_tag" not in card_cols:
            c.execute("ALTER TABLE cards ADD COLUMN fail_tag TEXT NOT NULL DEFAULT ''")

        proxy_cols = {r[1] for r in c.execute("PRAGMA table_info(proxies)").fetchall()}
        if "status" not in proxy_cols:
            c.execute(f"ALTER TABLE proxies ADD COLUMN status TEXT NOT NULL DEFAULT '{PROXY_STATUS_AVAILABLE}'")

        c.execute("CREATE INDEX IF NOT EXISTS idx_cards_fail_tag ON cards(fail_tag)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_accounts_sale_status ON accounts(sale_status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_proxies_status ON proxies(status)")

    # ── 唯一约束迁移（v3）──
    _migrate_unique_constraints()


def _has_duplicates(c: sqlite3.Connection, sql: str) -> bool:
    """检查 SQL 查询是否返回重复行，返回 True 表示有脏数据。"""
    rows = c.execute(sql).fetchall()
    return len(rows) > 0


def _try_create_unique_index(c: sqlite3.Connection, name: str, definition: str, dup_check_sql: str) -> None:
    """尝试创建唯一索引。如果存在重复数据，打日志并跳过。"""
    # 先检查索引是否已存在
    existing = c.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?", (name,)
    ).fetchone()
    if existing:
        return
    if _has_duplicates(c, dup_check_sql):
        log.warning(f"跳过唯一索引 {name}: 存在重复数据，请先清理。"
                    f"检查 SQL: {dup_check_sql}")
        return
    c.execute(f"CREATE UNIQUE INDEX {name} ON {definition}")
    log.info(f"唯一索引已创建: {name}")


def _migrate_unique_constraints() -> None:
    """添加唯一约束（v3）：cards.number / accounts.email / accounts.api_key / proxies 组合键。"""
    with _conn() as c:
        _repair_duplicates_for_unique_constraints(c)
        _try_create_unique_index(
            c, "idx_cards_number_unique", "cards(number)",
            "SELECT number FROM cards GROUP BY number HAVING COUNT(*) > 1",
        )
        _try_create_unique_index(
            c, "idx_accounts_email_unique", "accounts(email)",
            "SELECT email FROM accounts GROUP BY email HAVING COUNT(*) > 1",
        )
        # api_key: 只对非空值做唯一索引，空字符串不受限
        _try_create_unique_index(
            c, "idx_accounts_apikey_unique",
            "accounts(api_key) WHERE api_key != ''",
            "SELECT api_key FROM accounts WHERE api_key != '' GROUP BY api_key HAVING COUNT(*) > 1",
        )
        _try_create_unique_index(
            c, "idx_proxies_unique",
            "proxies(type, host, port, username)",
            "SELECT type, host, port, username FROM proxies GROUP BY type, host, port, username HAVING COUNT(*) > 1",
        )


def _append_note(existing: str, extra: str) -> str:
    existing = (existing or "").strip()
    extra = extra.strip()
    return f"{existing}\n{extra}".strip() if existing else extra


def _dedupe_email_variant(c: sqlite3.Connection, email: str, account_id: int) -> str:
    email = email or ""
    if "@" in email:
        local, domain = email.split("@", 1)
        base = f"{local}+dup{account_id}"
        candidate = f"{base}@{domain}"
        seq = 1
        while c.execute("SELECT 1 FROM accounts WHERE email=? AND id != ?", (candidate, account_id)).fetchone():
            candidate = f"{base}_{seq}@{domain}"
            seq += 1
        return candidate
    candidate = f"{email}#dup{account_id}"
    seq = 1
    while c.execute("SELECT 1 FROM accounts WHERE email=? AND id != ?", (candidate, account_id)).fetchone():
        candidate = f"{email}#dup{account_id}_{seq}"
        seq += 1
    return candidate


def _repair_duplicate_cards(c: sqlite3.Connection) -> int:
    rows = c.execute(
        "SELECT number FROM cards GROUP BY number HAVING COUNT(*) > 1"
    ).fetchall()
    repaired = 0
    for row in rows:
        number = row[0]
        dup_rows = c.execute(
            "SELECT * FROM cards WHERE number=? ORDER BY id DESC", (number,)
        ).fetchall()
        if len(dup_rows) <= 1:
            continue
        keep = dict(dup_rows[0])
        dup_ids = [dict(r)["id"] for r in dup_rows[1:]]
        merged = keep.copy()
        merged["use_count"] = sum(int(dict(r).get("use_count", 0) or 0) for r in dup_rows)
        if any(dict(r).get("status") == CARD_STATUS_DISABLED for r in dup_rows):
            merged["status"] = CARD_STATUS_DISABLED
        if not merged.get("fail_tag"):
            merged["fail_tag"] = next((dict(r).get("fail_tag", "") for r in dup_rows if dict(r).get("fail_tag")), "")
        for field in ("holder_name", "address_line1", "city", "state", "zip", "country", "exp_month", "exp_year", "cvv"):
            if not merged.get(field):
                merged[field] = next((dict(r).get(field, "") for r in dup_rows if dict(r).get(field)), "")
        c.execute(
            """UPDATE cards SET exp_month=?, exp_year=?, cvv=?, holder_name=?, address_line1=?,
               city=?, state=?, zip=?, country=?, status=?, fail_tag=?, use_count=? WHERE id=?""",
            (
                merged.get("exp_month", ""), merged.get("exp_year", ""), merged.get("cvv", ""),
                merged.get("holder_name", ""), merged.get("address_line1", ""), merged.get("city", ""),
                merged.get("state", ""), merged.get("zip", ""), merged.get("country", "US"),
                merged.get("status", CARD_STATUS_AVAILABLE), merged.get("fail_tag", ""),
                merged.get("use_count", 0), merged["id"],
            ),
        )
        placeholders = ",".join("?" * len(dup_ids))
        c.execute(
            f"UPDATE accounts SET card_id=? WHERE card_id IN ({placeholders})",
            [merged["id"], *dup_ids],
        )
        c.execute(f"DELETE FROM cards WHERE id IN ({placeholders})", dup_ids)
        repaired += len(dup_ids)
        log.warning(f"唯一约束修复: cards.number={number[-4:]} 合并 {len(dup_ids)} 条重复记录")
    return repaired


def _repair_duplicate_accounts_email(c: sqlite3.Connection) -> int:
    rows = c.execute(
        "SELECT email FROM accounts GROUP BY email HAVING COUNT(*) > 1"
    ).fetchall()
    repaired = 0
    for row in rows:
        email = row[0]
        dup_rows = c.execute(
            "SELECT id, email, notes FROM accounts WHERE email=? ORDER BY id DESC", (email,)
        ).fetchall()
        for dup in dup_rows[1:]:
            account_id = dup["id"]
            new_email = _dedupe_email_variant(c, email, account_id)
            notes = _append_note(dup["notes"], f"[dedupe] original_email={email}")
            c.execute("UPDATE accounts SET email=?, notes=? WHERE id=?", (new_email, notes, account_id))
            repaired += 1
            log.warning(f"唯一约束修复: accounts.email={email} -> {new_email} (id={account_id})")
    return repaired


def _repair_duplicate_accounts_api_keys(c: sqlite3.Connection) -> int:
    rows = c.execute(
        "SELECT api_key FROM accounts WHERE api_key != '' GROUP BY api_key HAVING COUNT(*) > 1"
    ).fetchall()
    repaired = 0
    for row in rows:
        api_key = row[0]
        dup_rows = c.execute(
            "SELECT id, notes FROM accounts WHERE api_key=? ORDER BY id DESC", (api_key,)
        ).fetchall()
        for dup in dup_rows[1:]:
            account_id = dup["id"]
            notes = _append_note(dup["notes"], f"[dedupe] original_api_key={api_key}")
            c.execute("UPDATE accounts SET api_key='', notes=? WHERE id=?", (notes, account_id))
            repaired += 1
            log.warning(f"唯一约束修复: accounts.api_key 已清空重复项 (id={account_id}, key={api_key[:12]}...)")
    return repaired


def _repair_duplicate_proxies(c: sqlite3.Connection) -> int:
    rows = c.execute(
        "SELECT type, host, port, username FROM proxies GROUP BY type, host, port, username HAVING COUNT(*) > 1"
    ).fetchall()
    repaired = 0
    for row in rows:
        p_type, host, port, username = row
        dup_rows = c.execute(
            "SELECT * FROM proxies WHERE type=? AND host=? AND port=? AND username=? ORDER BY id DESC",
            (p_type, host, port, username),
        ).fetchall()
        if len(dup_rows) <= 1:
            continue
        keep = dict(dup_rows[0])
        dup_ids = [dict(r)["id"] for r in dup_rows[1:]]
        merged = keep.copy()
        if not merged.get("label"):
            merged["label"] = next((dict(r).get("label", "") for r in dup_rows if dict(r).get("label")), "")
        if not merged.get("password"):
            merged["password"] = next((dict(r).get("password", "") for r in dup_rows if dict(r).get("password")), "")
        if any(dict(r).get("status") == PROXY_STATUS_AVAILABLE for r in dup_rows):
            merged["status"] = PROXY_STATUS_AVAILABLE
        c.execute(
            "UPDATE proxies SET label=?, password=?, status=? WHERE id=?",
            (merged.get("label", ""), merged.get("password", ""), merged.get("status", PROXY_STATUS_AVAILABLE), merged["id"]),
        )
        placeholders = ",".join("?" * len(dup_ids))
        c.execute(f"DELETE FROM proxies WHERE id IN ({placeholders})", dup_ids)
        repaired += len(dup_ids)
        log.warning(f"唯一约束修复: proxies={p_type}://{host}:{port} 合并 {len(dup_ids)} 条重复记录")
    return repaired


def _repair_duplicates_for_unique_constraints(c: sqlite3.Connection) -> None:
    repaired = 0
    repaired += _repair_duplicate_cards(c)
    repaired += _repair_duplicate_accounts_email(c)
    repaired += _repair_duplicate_accounts_api_keys(c)
    repaired += _repair_duplicate_proxies(c)
    if repaired:
        log.warning(f"唯一约束修复完成，共处理 {repaired} 条重复记录")


def _migrate_jsonl() -> None:
    if not os.path.exists(ACCOUNTS_FILE):
        return
    with _conn() as c:
        existing = c.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        if existing > 0:
            return
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                a = json.loads(line)
                c.execute(
                    """INSERT INTO accounts
                       (email, password, api_key, device_id, name, birthday,
                        org_name, ads_profile_id, registered_at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (
                        a.get("email", ""),
                        a.get("password", ""),
                        a.get("api_key", ""),
                        a.get("device_id", ""),
                        a.get("name", ""),
                        a.get("birthday", ""),
                        a.get("organization", ""),
                        a.get("ads_profile_id", ""),
                        a.get("registered_at", ""),
                    ),
                )


# ─── Accounts ─────────────────────────────────────────────

def insert_account(data: dict[str, Any]) -> int:
    try:
        with _conn() as c:
            cur = c.execute(
                """INSERT INTO accounts
                   (email, password, api_key, device_id, name, birthday,
                    org_name, ads_profile_id, card_id, credit_loaded,
                    sale_status, registered_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    data.get("email", ""),
                    data.get("password", ""),
                    data.get("api_key", ""),
                    data.get("device_id", ""),
                    data.get("name", ""),
                    data.get("birthday", ""),
                    data.get("organization", ""),
                    data.get("ads_profile_id", ""),
                    data.get("card_id"),
                    data.get("credit_loaded", 0),
                    data.get("sale_status", SALE_STATUS_UNSOLD),
                    data.get("registered_at", _now()),
                ),
            )
            return cur.lastrowid
    except sqlite3.IntegrityError as e:
        raise DuplicateRecordError("accounts", str(e))


def insert_account_with_card(data: dict[str, Any], card_id: int, increment_use: bool = True) -> int:
    """事务化：插入账号 + 更新卡片计数 + 清除卡片失败标签，保证原子性。"""
    try:
        return _insert_account_with_card_inner(data, card_id, increment_use)
    except sqlite3.IntegrityError as e:
        raise DuplicateRecordError("accounts", str(e))


def _insert_account_with_card_inner(data: dict[str, Any], card_id: int, increment_use: bool) -> int:
    with _conn() as c:
        cur = c.execute(
            """INSERT INTO accounts
               (email, password, api_key, device_id, name, birthday,
                org_name, ads_profile_id, card_id, credit_loaded,
                sale_status, registered_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                data.get("email", ""),
                data.get("password", ""),
                data.get("api_key", ""),
                data.get("device_id", ""),
                data.get("name", ""),
                data.get("birthday", ""),
                data.get("organization", ""),
                data.get("ads_profile_id", ""),
                data.get("card_id"),
                data.get("credit_loaded", 0),
                data.get("sale_status", SALE_STATUS_UNSOLD),
                data.get("registered_at", _now()),
            ),
        )
        account_id = cur.lastrowid
        if increment_use:
            c.execute("UPDATE cards SET use_count = use_count + 1 WHERE id=?", (card_id,))
        c.execute("UPDATE cards SET fail_tag='' WHERE id=?", (card_id,))
        return account_id


def update_account(account_id: int, **fields: Any) -> None:
    allowed = {
        "api_key", "card_id", "status", "checked_at", "notes",
        "email", "password", "device_id", "sale_status", "sold_at",
        "credit_loaded", "ads_profile_id",
    }
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets:
        return
    clause = ", ".join(f"{k}=?" for k in sets)
    with _conn() as c:
        c.execute(f"UPDATE accounts SET {clause} WHERE id=?", [*sets.values(), account_id])


_ACCOUNTS_SORT_ALLOW = {"id", "email", "status", "sale_status", "registered_at", "checked_at", "credit_loaded"}


def count_accounts(*, search: str = "", status: str = "", sale_status: str = "") -> int:
    where, params = _accounts_where(search, status, sale_status)
    sql = f"SELECT COUNT(*) FROM accounts a {where}"
    with _conn() as c:
        return c.execute(sql, params).fetchone()[0]


def _accounts_where(search: str, status: str, sale_status: str):
    clauses: list[str] = []
    params: list = []
    if search:
        clauses.append("(a.email LIKE ? OR a.api_key LIKE ? OR CAST(a.id AS TEXT) = ?)")
        like = f"%{search}%"
        params.extend([like, like, search])
    if status:
        clauses.append("a.status = ?")
        params.append(status)
    if sale_status:
        clauses.append("a.sale_status = ?")
        params.append(sale_status)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def list_accounts(*, page: int = 0, page_size: int = 0,
                  search: str = "", status: str = "", sale_status: str = "",
                  sort: str = "", order: str = "desc") -> list[dict[str, Any]]:
    where, params = _accounts_where(search, status, sale_status)
    order_clause = "a.id DESC"
    if sort and sort in _ACCOUNTS_SORT_ALLOW:
        direction = "ASC" if order.lower() == "asc" else "DESC"
        order_clause = f"a.{sort} {direction}"
    sql = f"""SELECT a.*, c.number AS card_number
              FROM accounts a
              LEFT JOIN cards c ON a.card_id = c.id
              {where}
              ORDER BY {order_clause}"""
    if page > 0 and page_size > 0:
        sql += " LIMIT ? OFFSET ?"
        params.extend([page_size, (page - 1) * page_size])
    with _conn() as c:
        rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_account(account_id: int) -> Optional[dict[str, Any]]:
    with _conn() as c:
        row = c.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        return dict(row) if row else None


def delete_accounts(ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        c.execute(f"DELETE FROM accounts WHERE id IN ({placeholders})", ids)


def batch_update_accounts(ids: list[int], **fields: Any) -> None:
    allowed = {"status", "card_id", "notes", "checked_at", "sale_status", "sold_at"}
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets or not ids:
        return
    clause = ", ".join(f"{k}=?" for k in sets)
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        c.execute(
            f"UPDATE accounts SET {clause} WHERE id IN ({placeholders})",
            [*sets.values(), *ids],
        )


def lock_for_sale(ids: list[int], token: str) -> list[dict[str, Any]]:
    """Atomically lock unsold accounts as pending_sale and return them.
    Uses single UPDATE…RETURNING to prevent concurrent requests from
    locking the same accounts (second caller gets 0 rows)."""
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        rows = c.execute(
            f"""UPDATE accounts
                SET sale_status='{SALE_STATUS_PENDING}', sold_at=?
                WHERE id IN ({placeholders}) AND sale_status='{SALE_STATUS_UNSOLD}'
                RETURNING *""",
            [token, *ids],
        ).fetchall()
        return [dict(r) for r in rows]


def confirm_sale(token: str) -> int:
    """Confirm a pending_sale → sold, using the token written by lock_for_sale."""
    now = _now()
    with _conn() as c:
        c.execute(
            f"UPDATE accounts SET sale_status='{SALE_STATUS_SOLD}', sold_at=? "
            f"WHERE sale_status='{SALE_STATUS_PENDING}' AND sold_at=?",
            (now, token),
        )
        return c.execute("SELECT changes()").fetchone()[0]


def rollback_pending_sales(older_than_seconds: int = 300) -> int:
    """Roll back stale pending_sale back to unsold (timeout guard)."""
    with _conn() as c:
        # sold_at 暂存 token，通过其时间前缀判断是否超时。
        rows = c.execute(
            f"SELECT id, sold_at FROM accounts WHERE sale_status='{SALE_STATUS_PENDING}'"
        ).fetchall()
        stale_ids = []
        now_ts = time.time()
        for row in rows:
            token = row[1]  # "ps_<timestamp>_<rand>"
            try:
                ts = float(token.split("_")[1])
                if now_ts - ts > older_than_seconds:
                    stale_ids.append(row[0])
            except (IndexError, ValueError):
                stale_ids.append(row[0])
        if stale_ids:
            placeholders = ",".join("?" * len(stale_ids))
            c.execute(
                f"UPDATE accounts SET sale_status='{SALE_STATUS_UNSOLD}', sold_at='' WHERE id IN ({placeholders})",
                stale_ids,
            )
        return len(stale_ids)


def get_profile_ids_by_account_ids(ids: list[int]) -> list[str]:
    """Get ads_profile_id values for the given account IDs."""
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        rows = c.execute(
            f"SELECT ads_profile_id FROM accounts WHERE id IN ({placeholders}) AND ads_profile_id != ''",
            ids,
        ).fetchall()
        return [r[0] for r in rows]


def get_stale_sold_accounts(days: int = 3) -> list[dict[str, Any]]:
    """Get sold accounts whose sold_at is older than N days."""
    import datetime
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with _conn() as c:
        rows = c.execute(
            f"SELECT * FROM accounts WHERE sale_status='{SALE_STATUS_SOLD}' AND sold_at != '' AND sold_at <= ?",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]


def add_credit(account_id: int, amount: float) -> None:
    with _conn() as c:
        c.execute("UPDATE accounts SET credit_loaded = credit_loaded + ? WHERE id=?",
                  (amount, account_id))


def mark_account_recycled(account_id: int) -> None:
    with _conn() as c:
        c.execute(f"UPDATE accounts SET sale_status='{SALE_STATUS_RECYCLED}' WHERE id=?", (account_id,))


# ─── Cards ────────────────────────────────────────────────

def insert_card(data: dict[str, Any]) -> int:
    try:
        with _conn() as c:
            cur = c.execute(
                """INSERT INTO cards
                   (number, exp_month, exp_year, cvv, holder_name,
                    address_line1, city, state, zip, country, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    data.get("number", ""),
                    data.get("exp_month", ""),
                    data.get("exp_year", ""),
                    data.get("cvv", ""),
                    data.get("holder_name", ""),
                    data.get("address_line1", ""),
                    data.get("city", ""),
                    data.get("state", ""),
                    data.get("zip", ""),
                    data.get("country", "US"),
                    _now(),
                ),
            )
            return cur.lastrowid
    except sqlite3.IntegrityError:
        raise DuplicateRecordError("cards", f"卡号 {data.get('number', '')[-4:]} 已存在")


_CARDS_SORT_ALLOW = {"id", "number", "status", "fail_tag", "use_count", "created_at"}


def _cards_where(search: str, status: str, fail_tag: str):
    clauses: list[str] = []
    params: list = []
    if search:
        clauses.append("(c.number LIKE ? OR c.holder_name LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like])
    if status:
        clauses.append("c.status = ?")
        params.append(status)
    if fail_tag:
        clauses.append("c.fail_tag = ?")
        params.append(fail_tag)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def count_cards(*, search: str = "", status: str = "", fail_tag: str = "") -> int:
    where, params = _cards_where(search, status, fail_tag)
    sql = f"SELECT COUNT(*) FROM cards c {where}"
    with _conn() as c:
        return c.execute(sql, params).fetchone()[0]


def list_cards(*, page: int = 0, page_size: int = 0,
               search: str = "", status: str = "", fail_tag: str = "",
               sort: str = "", order: str = "desc") -> list[dict[str, Any]]:
    where, params = _cards_where(search, status, fail_tag)
    order_clause = f"""CASE
                 WHEN c.status = '{CARD_STATUS_AVAILABLE}' AND c.fail_tag = '' THEN 0
                 WHEN c.status = '{CARD_STATUS_AVAILABLE}' AND c.fail_tag != '' THEN 1
                 WHEN c.status = '{CARD_STATUS_DISABLED}'  THEN 2
                 ELSE 3
               END, c.id DESC"""
    if sort and sort in _CARDS_SORT_ALLOW:
        direction = "ASC" if order.lower() == "asc" else "DESC"
        order_clause = f"c.{sort} {direction}"
    sql = f"""SELECT c.*,
                    (SELECT COUNT(*) FROM accounts a WHERE a.card_id = c.id) AS bound_count
             FROM cards c
             {where}
             ORDER BY {order_clause}"""
    if page > 0 and page_size > 0:
        sql += " LIMIT ? OFFSET ?"
        params.extend([page_size, (page - 1) * page_size])
    with _conn() as c:
        rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def delete_card(card_id: int) -> None:
    with _conn() as c:
        c.execute("UPDATE accounts SET card_id=NULL WHERE card_id=?", (card_id,))
        c.execute("DELETE FROM cards WHERE id=?", (card_id,))


def delete_cards(ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        c.execute(f"UPDATE accounts SET card_id=NULL WHERE card_id IN ({placeholders})", ids)
        c.execute(f"DELETE FROM cards WHERE id IN ({placeholders})", ids)


def batch_update_cards(ids: list[int], **fields: Any) -> None:
    allowed = {"status", "fail_tag"}
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets or not ids:
        return
    clause = ", ".join(f"{k}=?" for k in sets)
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        c.execute(
            f"UPDATE cards SET {clause} WHERE id IN ({placeholders})",
            [*sets.values(), *ids],
        )


def batch_update_card_address(ids: list[int], **fields: Any) -> None:
    allowed = {"address_line1", "city", "state", "zip", "country"}
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets or not ids:
        return
    clause = ", ".join(f"{k}=?" for k in sets)
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        c.execute(
            f"UPDATE cards SET {clause} WHERE id IN ({placeholders})",
            [*sets.values(), *ids],
        )


def set_card_fail_tag(card_id: int, tag: str) -> None:
    with _conn() as c:
        c.execute("UPDATE cards SET fail_tag=? WHERE id=?", (tag, card_id))


def clear_card_fail_tag(card_id: int) -> None:
    with _conn() as c:
        c.execute("UPDATE cards SET fail_tag='' WHERE id=?", (card_id,))


def get_card(card_id: int) -> Optional[dict[str, Any]]:
    with _conn() as c:
        row = c.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
        return dict(row) if row else None


def get_available_card() -> Optional[dict[str, Any]]:
    """Atomically pick and reserve the next available card (use_count+1).

    Uses a single UPDATE … RETURNING to prevent race conditions where
    concurrent threads select the same card.  RANDOM() ensures even
    distribution among cards with equal use_count.
    """
    with _conn() as c:
        row = c.execute(
            f"""UPDATE cards SET use_count = use_count + 1
               WHERE id = (
                   SELECT id FROM cards
                   WHERE status='{CARD_STATUS_AVAILABLE}' AND fail_tag=''
                   ORDER BY use_count ASC, RANDOM()
                   LIMIT 1
               )
               RETURNING *"""
        ).fetchone()
        return dict(row) if row else None


def card_exists(number: str) -> Optional[int]:
    """Return existing card ID if number already in DB, else None."""
    with _conn() as c:
        row = c.execute("SELECT id FROM cards WHERE number=?", (number,)).fetchone()
        return row[0] if row else None


def increment_card_use(card_id: int) -> None:
    with _conn() as c:
        c.execute("UPDATE cards SET use_count = use_count + 1 WHERE id=?", (card_id,))


def mark_card_used(card_id: int, account_id: int) -> None:
    with _conn() as c:
        c.execute("UPDATE cards SET use_count = use_count + 1 WHERE id=?", (card_id,))
        c.execute("UPDATE accounts SET card_id=? WHERE id=?", (card_id, account_id))


# ─── Proxies ─────────────────────────────────────────────

def insert_proxy(data: dict[str, Any]) -> int:
    try:
        with _conn() as c:
            cur = c.execute(
                """INSERT INTO proxies (label, type, host, port, username, password, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    data.get("label", ""),
                    data.get("type", "socks5"),
                    data.get("host", ""),
                    data.get("port", ""),
                    data.get("username", ""),
                    data.get("password", ""),
                    _now(),
                ),
            )
            return cur.lastrowid
    except sqlite3.IntegrityError:
        raise DuplicateRecordError("proxies", f"{data.get('host','')}:{data.get('port','')}")


def batch_insert_proxies(proxies: list[dict[str, Any]]) -> tuple[list[int], int]:
    """批量插入代理，返回 (成功ID列表, 跳过的重复数量)。"""
    ids = []
    skipped = 0
    with _conn() as c:
        for p in proxies:
            try:
                cur = c.execute(
                    """INSERT INTO proxies (label, type, host, port, username, password, created_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (
                        p.get("label", ""),
                        p.get("type", "socks5"),
                        p.get("host", ""),
                        p.get("port", ""),
                        p.get("username", ""),
                        p.get("password", ""),
                        _now(),
                    ),
                )
                ids.append(cur.lastrowid)
            except sqlite3.IntegrityError:
                skipped += 1
    return ids, skipped


def get_proxy(proxy_id: int) -> Optional[dict[str, Any]]:
    with _conn() as c:
        row = c.execute("SELECT * FROM proxies WHERE id=?", (proxy_id,)).fetchone()
        return dict(row) if row else None


_PROXIES_SORT_ALLOW = {"id", "host", "port", "type", "status"}


def _proxies_where(search: str, status: str, proxy_type: str):
    clauses: list[str] = []
    params: list = []
    if search:
        clauses.append("(host LIKE ? OR label LIKE ?)")
        like = f"%{search}%"
        params.extend([like, like])
    if status:
        clauses.append("status = ?")
        params.append(status)
    if proxy_type:
        clauses.append("type = ?")
        params.append(proxy_type)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def count_proxies(*, search: str = "", status: str = "", proxy_type: str = "") -> int:
    where, params = _proxies_where(search, status, proxy_type)
    sql = f"SELECT COUNT(*) FROM proxies {where}"
    with _conn() as c:
        return c.execute(sql, params).fetchone()[0]


def list_proxies(*, page: int = 0, page_size: int = 0,
                 search: str = "", status: str = "", proxy_type: str = "",
                 sort: str = "", order: str = "desc") -> list[dict[str, Any]]:
    where, params = _proxies_where(search, status, proxy_type)
    order_clause = f"CASE status WHEN '{PROXY_STATUS_AVAILABLE}' THEN 0 ELSE 1 END, id DESC"
    if sort and sort in _PROXIES_SORT_ALLOW:
        direction = "ASC" if order.lower() == "asc" else "DESC"
        order_clause = f"{sort} {direction}"
    sql = f"SELECT * FROM proxies {where} ORDER BY {order_clause}"
    if page > 0 and page_size > 0:
        sql += " LIMIT ? OFFSET ?"
        params.extend([page_size, (page - 1) * page_size])
    with _conn() as c:
        rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def list_available_proxies() -> list[dict[str, Any]]:
    with _conn() as c:
        rows = c.execute(
            f"SELECT * FROM proxies WHERE status='{PROXY_STATUS_AVAILABLE}' ORDER BY id DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def disable_proxy(proxy_id: int) -> None:
    with _conn() as c:
        c.execute(f"UPDATE proxies SET status='{PROXY_STATUS_DISABLED}' WHERE id=?", (proxy_id,))


def batch_update_proxy_status(ids: list[int], status: str) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        c.execute(
            f"UPDATE proxies SET status=? WHERE id IN ({placeholders})",
            [status, *ids],
        )


def delete_proxy(proxy_id: int) -> None:
    with _conn() as c:
        c.execute("DELETE FROM proxies WHERE id=?", (proxy_id,))


def delete_proxies(ids: list[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    with _conn() as c:
        c.execute(f"DELETE FROM proxies WHERE id IN ({placeholders})", ids)


# ─── Profile Cleanup Queue ────────────────────────────────

def enqueue_profile_cleanup(
    profile_id: str,
    reason: str,
    account_id: Optional[int] = None,
) -> int:
    """将 profile 加入待清理队列。利用部分唯一索引原子去重（同 profile_id + pending 仅一条）。"""
    now = _now()
    with _conn() as c:
        cur = c.execute(
            """INSERT OR IGNORE INTO profile_cleanup_queue
               (profile_id, account_id, reason, status, created_at, updated_at)
               VALUES (?,?,?,'pending',?,?)""",
            (profile_id, account_id, reason, now, now),
        )
        if cur.lastrowid:
            return cur.lastrowid
        # 已存在 pending 记录，返回其 id
        row = c.execute(
            "SELECT id FROM profile_cleanup_queue WHERE profile_id=? AND status='pending'",
            (profile_id,),
        ).fetchone()
        return row[0] if row else 0


def list_pending_cleanups(limit: int = 50) -> list[dict[str, Any]]:
    """获取待清理的 profile 列表，按创建时间升序。"""
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM profile_cleanup_queue WHERE status='pending' ORDER BY created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def mark_cleanup_done(cleanup_id: int) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE profile_cleanup_queue SET status='done', updated_at=? WHERE id=?",
            (_now(), cleanup_id),
        )


def mark_cleanup_failed(cleanup_id: int, error: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE profile_cleanup_queue SET status='pending', attempts=attempts+1, "
            "last_error=?, updated_at=? WHERE id=?",
            (error, _now(), cleanup_id),
        )


def mark_cleanup_permanently_failed(cleanup_id: int, error: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE profile_cleanup_queue SET status='failed', attempts=attempts+1, "
            "last_error=?, updated_at=? WHERE id=?",
            (error, _now(), cleanup_id),
        )


# ─── Settings ────────────────────────────────────────────

def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """Return the stored value, or *default* when the key has no row in DB.

    Returning None (the default) for missing keys lets callers distinguish
    'never configured' from 'explicitly set to empty string'.
    """
    try:
        with _conn() as c:
            row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            return row[0] if row else default
    except Exception:
        return default


def set_setting(key: str, value: str) -> None:
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )


def get_all_settings() -> dict[str, str]:
    try:
        with _conn() as c:
            rows = c.execute("SELECT key, value FROM settings").fetchall()
            return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


# ─── Dashboard Stats ─────────────────────────────────────

def dashboard_stats() -> dict[str, Any]:
    """单次连接、一条复合 SQL 聚合所有 dashboard 统计。"""
    today = time.strftime("%Y-%m-%d", time.gmtime())
    today_prefix = f"{today}%"
    sql = """
    SELECT 'acct_sale'  AS grp, sale_status AS key1, '' AS key2, COUNT(*) AS cnt FROM accounts GROUP BY sale_status
    UNION ALL
    SELECT 'acct_status', status, '', COUNT(*) FROM accounts GROUP BY status
    UNION ALL
    SELECT 'acct_today', '', '', COUNT(*) FROM accounts WHERE registered_at LIKE ?
    UNION ALL
    SELECT 'card_status', status, '', COUNT(*) FROM cards GROUP BY status
    UNION ALL
    SELECT 'card_tag', fail_tag, '', COUNT(*) FROM cards WHERE fail_tag != '' GROUP BY fail_tag
    UNION ALL
    SELECT 'proxy_type', type, '', COUNT(*) FROM proxies GROUP BY type
    UNION ALL
    SELECT 'proxy_status', status, '', COUNT(*) FROM proxies GROUP BY status
    """
    with _conn() as c:
        rows = c.execute(sql, (today_prefix,)).fetchall()

    by_sale: dict[str, int] = {}
    by_status: dict[str, int] = {}
    today_count = 0
    cards_by_status: dict[str, int] = {}
    cards_by_tag: dict[str, int] = {}
    proxies_by_type: dict[str, int] = {}
    proxies_by_status: dict[str, int] = {}

    for r in rows:
        grp, key1, _key2, cnt = r["grp"], r["key1"], r["key2"], r["cnt"]
        if grp == "acct_sale":
            by_sale[key1] = cnt
        elif grp == "acct_status":
            by_status[key1] = cnt
        elif grp == "acct_today":
            today_count = cnt
        elif grp == "card_status":
            cards_by_status[key1] = cnt
        elif grp == "card_tag":
            cards_by_tag[key1] = cnt
        elif grp == "proxy_type":
            proxies_by_type[key1] = cnt
        elif grp == "proxy_status":
            proxies_by_status[key1] = cnt

    total_accounts = sum(by_sale.values()) if by_sale else 0
    total_cards = sum(cards_by_status.values()) if cards_by_status else 0
    proxy_total = sum(proxies_by_type.values()) if proxies_by_type else 0

    return {
        "accounts": {
            "total": total_accounts,
            "today": today_count,
            "by_sale_status": by_sale,
            "by_status": by_status,
        },
        "cards": {
            "total": total_cards,
            "by_status": cards_by_status,
            "by_fail_tag": cards_by_tag,
        },
        "proxies": {
            "total": proxy_total,
            "by_type": proxies_by_type,
            "by_status": proxies_by_status,
        },
    }


# ─── Task Runs (持久化任务历史) ─────────────────────────

def insert_task_run(task_id: str, task_type: str, params_json: str = "") -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO task_runs (task_id, type, status, params_json, started_at) VALUES (?,?,?,?,?)",
            (task_id, task_type, "running", params_json, _now()),
        )
        return cur.lastrowid


def finish_task_run(task_id: str, status: str, success_count: int = 0,
                    failed_count: int = 0, error_summary: str = "") -> None:
    with _conn() as c:
        c.execute(
            """UPDATE task_runs SET status=?, success_count=?, failed_count=?,
               error_summary=?, finished_at=? WHERE task_id=?""",
            (status, success_count, failed_count, error_summary, _now(), task_id),
        )


def list_task_runs(*, limit: int = 20, offset: int = 0,
                   task_type: str = "") -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list = []
    if task_type:
        clauses.append("type = ?")
        params.append(task_type)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"SELECT * FROM task_runs {where} ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with _conn() as c:
        rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def count_task_runs(*, task_type: str = "") -> int:
    clauses: list[str] = []
    params: list = []
    if task_type:
        clauses.append("type = ?")
        params.append(task_type)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with _conn() as c:
        return c.execute(f"SELECT COUNT(*) FROM task_runs {where}", params).fetchone()[0]


# ─── Audit Logs (业务审计日志) ──────────────────────────

def audit_log(entity_type: str, entity_id: Optional[int], action: str, detail: str = "") -> None:
    """记录一条业务审计日志。"""
    try:
        with _conn() as c:
            c.execute(
                "INSERT INTO audit_logs (entity_type, entity_id, action, detail, created_at) VALUES (?,?,?,?,?)",
                (entity_type, entity_id, action, detail, _now()),
            )
    except Exception:
        pass  # 审计日志写入失败不应影响业务流程


def list_audit_logs(*, entity_type: str = "", entity_id: Optional[int] = None,
                    action: str = "", limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list = []
    if entity_type:
        clauses.append("entity_type = ?")
        params.append(entity_type)
    if entity_id is not None:
        clauses.append("entity_id = ?")
        params.append(entity_id)
    if action:
        clauses.append("action = ?")
        params.append(action)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"SELECT * FROM audit_logs {where} ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with _conn() as c:
        rows = c.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def count_audit_logs(*, entity_type: str = "", entity_id: Optional[int] = None,
                     action: str = "") -> int:
    clauses: list[str] = []
    params: list = []
    if entity_type:
        clauses.append("entity_type = ?")
        params.append(entity_type)
    if entity_id is not None:
        clauses.append("entity_id = ?")
        params.append(entity_id)
    if action:
        clauses.append("action = ?")
        params.append(action)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    with _conn() as c:
        return c.execute(f"SELECT COUNT(*) FROM audit_logs {where}", params).fetchone()[0]

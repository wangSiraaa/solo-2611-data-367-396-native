"""持久 token-vault：等值域映射的唯一事实来源。

设计要点
========
1. 映射以 (domain, value_sha256) 为业务键，原值以哈希方式存储用于比对，
   完整原值只存在于本专用目录，绝不写入公开回执/普通日志。
2. (domain, token) 与 (domain, value_sha256) 均有 UNIQUE 约束，数据库层
   保证“同域不同值不得同 token”和“同值不得重复分配”。
3. 候选代号由 sha256(domain 分隔符 + 原值) 取起始下标，随后线性探测，
   天然消化两位前缀碰撞；每次分配都从同一确定性起点开始，旧映射优先复用，
   因此绝不按批次重新编号，也不会覆盖既有映射。
4. 全部写操作在进程级 flock 排他锁 + SQLite IMMEDIATE 事务内进行，
   两个进程同时首次遇到同值时，后到者会拿到先到者已提交的映射。
5. 释放代号不会回收（见 release_token 说明），探测链保持稳定；
   代号耗尽抛 CapacityExhausted，属于可预期拒绝。
6. release 状态机 reserved -> intermediate -> complete，崩溃后由
   recover 依据 vault 记录与磁盘文件判定，失败包绝不允许标 complete。
"""

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import os
import sqlite3
from pathlib import Path

from .errors import CapacityExhausted
from .util import atomic_write, canonical_json, sha256_bytes

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS token_mapping (
    domain TEXT NOT NULL,
    value_sha256 TEXT NOT NULL,
    token TEXT NOT NULL,
    created_by_release TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (domain, value_sha256)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_token_per_domain
    ON token_mapping(domain, token);
CREATE TABLE IF NOT EXISTS release_record (
    release_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('reserved','intermediate','complete','abandoned')),
    policy_id TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    output_db_path TEXT,
    output_sha256 TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

STATE_RESERVED = "reserved"
STATE_INTERMEDIATE = "intermediate"
STATE_COMPLETE = "complete"
STATE_ABANDONED = "abandoned"


def value_hash(domain, value):
    """域隔离的原值哈希：同字符串不同域得到不同业务键。"""
    return hashlib.sha256(
        canonical_json([domain, value])
    ).hexdigest()


def encode_suffix(index: int, base: int, width: int) -> list[int]:
    """低位在前的定长 base 进制数字，宽度不足时抛容量错误（调用前已保证）。"""
    digits = []
    for _ in range(width):
        digits.append(index % base)
        index //= base
    if index != 0:
        raise CapacityExhausted("?")
    return digits


class VaultLock:
    """对 vault 目录内 lock 文件的进程排他锁。"""

    def __init__(self, vault_dir):
        self.vault_dir = Path(vault_dir)
        self._fh = None

    def acquire(self):
        self.vault_dir.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.vault_dir / "lock", "a+")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def release(self):
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
                self._fh.close()
            finally:
                self._fh = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc, tb):
        self.release()


@dataclass
class Allocation:
    token: str
    reused: bool


class TokenVault:
    def __init__(self, vault_dir):
        self.dir = Path(vault_dir)
        self.db_path = self.dir / "vault.sqlite"
        self.manifest_dir = self.dir / "manifests"
        self.lock = VaultLock(self.dir)

    # ---------- 初始化 ----------
    def initialize(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.dir, 0o700)
        conn = self._connect()
        try:
            conn.executescript(DDL)
            conn.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES('version', ?)",
                (str(SCHEMA_VERSION),),
            )
            conn.commit()
        finally:
            conn.close()
        if self.db_path.exists():
            os.chmod(self.db_path, 0o600)

    def _connect(self):
        conn = sqlite3.connect(
            self.db_path, timeout=30, isolation_level=None
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @contextmanager
    def locked(self):
        """在进程排他锁内打开连接；写事务由调用方用 IMMEDIATE 显式开启。"""
        with self.lock:
            conn = self._connect()
            try:
                yield conn
            finally:
                conn.close()

    # ---------- 代号空间 ----------
    @staticmethod
    def _start_index(domain, value, capacity):
        digest = hashlib.sha256(
            domain.encode("utf-8") + b"\x00"
            + canonical_json([value])
        ).digest()
        return int.from_bytes(digest, "big") % capacity

    @staticmethod
    def _format_token(spec, index):
        digits = encode_suffix(index, len(spec.alphabet), spec.width)
        suffix = "".join(spec.alphabet[d] for d in reversed(digits))
        return spec.prefix + suffix

    def allocate(self, conn, domain, value, spec, release_id):
        """返回 (token, reused)。必须在 locked() + IMMEDIATE 事务内调用。

        已存在映射（含历史发布遗留的预留 token）一律复用；
        新值从确定性起点线性探测首个未占用代号，耗尽即拒绝。
        """
        vhash = value_hash(domain, value)
        row = conn.execute(
            "SELECT token FROM token_mapping WHERE domain=? AND value_sha256=?",
            (domain, vhash),
        ).fetchone()
        if row is not None:
            return Allocation(token=row["token"], reused=True)

        capacity = spec.capacity
        start = self._start_index(domain, value, capacity)
        used = {
            r["token"]
            for r in conn.execute(
                "SELECT token FROM token_mapping WHERE domain=?", (domain,)
            )
        }
        if len(used) >= capacity:
            raise CapacityExhausted(domain)

        for step in range(capacity):
            idx = (start + step) % capacity
            token = self._format_token(spec, idx)
            if token not in used:
                conn.execute(
                    "INSERT INTO token_mapping"
                    "(domain, value_sha256, token, created_by_release) "
                    "VALUES(?,?,?,?)",
                    (domain, vhash, token, release_id),
                )
                return Allocation(token=token, reused=False)
        raise CapacityExhausted(domain)

    def used_count(self, conn, domain):
        return conn.execute(
            "SELECT COUNT(*) AS n FROM token_mapping WHERE domain=?",
            (domain,),
        ).fetchone()["n"]

    # ---------- 发布状态机 ----------
    def create_release(self, conn, release_id, policy_id,
                       policy_fp, source_fp, output_db_path=None):
        conn.execute(
            "INSERT INTO release_record"
            "(release_id, state, policy_id, policy_fingerprint, "
            "source_fingerprint, output_db_path) "
            "VALUES(?,?,?,?,?,?)",
            (release_id, STATE_RESERVED, policy_id, policy_fp, source_fp,
             str(output_db_path) if output_db_path else None),
        )

    def mark_intermediate(self, conn, release_id, output_db_path):
        conn.execute(
            "UPDATE release_record SET state=?, output_db_path=?, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE release_id=?",
            (STATE_INTERMEDIATE, str(output_db_path), release_id),
        )

    def mark_complete(self, conn, release_id, output_sha256):
        conn.execute(
            "UPDATE release_record SET state=?, output_sha256=?, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE release_id=?",
            (STATE_COMPLETE, output_sha256, release_id),
        )

    def mark_abandoned(self, conn, release_id):
        conn.execute(
            "UPDATE release_record SET state='abandoned', "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE release_id=?",
            (release_id,),
        )

    def get_release(self, conn, release_id):
        return conn.execute(
            "SELECT * FROM release_record WHERE release_id=?", (release_id,)
        ).fetchone()

    def list_releases(self, conn):
        return conn.execute(
            "SELECT * FROM release_record ORDER BY created_at"
        ).fetchall()

    # ---------- 私有清单（仅显式 private 目录） ----------
    def write_manifest(self, release_id, manifest: dict):
        """私有映射清单：含原值->令牌，权限 0600，绝不进入公开目录。"""
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        path = self.manifest_dir / f"{release_id}.json"
        atomic_write(path, canonical_json(manifest), mode=0o600)
        return path

    def read_manifest(self, release_id):
        import json

        path = self.manifest_dir / f"{release_id}.json"
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def token_for(self, conn, domain, value):
        """verify 用：按 (域, 原值) 查当前权威令牌，未分配返回 None。"""
        row = conn.execute(
            "SELECT token FROM token_mapping WHERE domain=? AND value_sha256=?",
            (domain, value_hash(domain, value)),
        ).fetchone()
        return row["token"] if row else None

    def mappings_for_domain(self, conn, domain):
        return conn.execute(
            "SELECT value_sha256, token FROM token_mapping WHERE domain=? "
            "ORDER BY token",
            (domain,),
        ).fetchall()

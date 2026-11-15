"""测试公共夹具：合成源库、policy 变体、临时工作目录。

所有数据均为合成字符串，不含任何真实个人信息。
"""

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from maskkit.vault import TokenVault
from maskkit.producer import run_release
from maskkit.policy import load_policy

BASE_POLICY = {
    "policy_id": "p-test",
    "sensitive_columns": [
        "customers.customer_id", "customers.email",
        "accounts.account_id", "accounts.owner_id",
        "invoices.account_ref", "invoices.bill_to_email",
    ],
    "public_keep_columns": ["invoices.invoice_id"],
    "domains": {
        "customer": {"prefix": "C", "alphabet": "0123456789abcdef", "width": 2},
        "account": {"prefix": "A", "alphabet": "0123456789abcdef", "width": 2},
        "email": {"prefix": "E", "alphabet": "0123456789abcdef", "width": 2},
    },
    "columns": {
        "customers.customer_id": "tokenize:customer",
        "customers.email": "tokenize:email",
        "accounts.account_id": "tokenize:account",
        "accounts.owner_id": "tokenize:customer",
        "invoices.invoice_id": "keep",
        "invoices.account_ref": "tokenize:account",
        "invoices.bill_to_email": "tokenize:email",
    },
}

SCHEMA_SQL = (
    "CREATE TABLE customers("
    "customer_id TEXT PRIMARY KEY, email TEXT UNIQUE);"
    "CREATE TABLE accounts("
    "account_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL "
    "REFERENCES customers(customer_id));"
    "CREATE TABLE invoices("
    "invoice_id TEXT PRIMARY KEY, "
    "account_ref TEXT REFERENCES accounts(account_id), "
    "bill_to_email TEXT REFERENCES customers(email));"
)


def write_source(path, customers, accounts, invoices):
    """customers/accounts/invoices 为 tuple 列表，列顺序与三表定义一致。"""
    if os.path.exists(path):
        os.chmod(path, 0o644)
    conn = sqlite3.connect(path)
    conn.executescript("DROP TABLE IF EXISTS invoices; DROP TABLE IF EXISTS accounts; "
                       "DROP TABLE IF EXISTS customers; " + SCHEMA_SQL)
    conn.executemany("INSERT INTO customers VALUES(?,?)", customers)
    conn.executemany("INSERT INTO accounts VALUES(?,?)", accounts)
    conn.executemany("INSERT INTO invoices VALUES(?,?,?)", invoices)
    conn.commit()
    conn.close()
    os.chmod(path, 0o444)  # 模拟只读源


def write_policy(path, policy_obj):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(policy_obj, fh, ensure_ascii=False, indent=2)


def standard_rows(n=3, shared_email=None):
    """生成标准三表行；shared_email 指定一个跨表/跨行复用的 email。"""
    customers, accounts, invoices = [], [], []
    for i in range(n):
        cid = f"SRC_C{i}"
        email = shared_email if (shared_email and i == 0) else f"SRC_EMAIL_{i}"
        aid = f"SRC_A{i}"
        customers.append((cid, email))
        accounts.append((aid, cid))
        invoices.append((f"I{i}", aid, email))
    return customers, accounts, invoices


class MaskKitTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source = self.root / "source.sqlite"
        self.policy = self.root / "policy.json"
        self.vault_dir = self.root / "vault"
        self.out = self.root / "out"
        write_policy(self.policy, BASE_POLICY)
        self.vault = TokenVault(self.vault_dir)
        self.vault.initialize()

    def tearDown(self):
        # 只读文件需要先恢复权限，TemporaryDirectory 才能清理。
        for p in self.root.rglob("source.sqlite"):
            try:
                os.chmod(p, 0o644)
            except OSError:
                pass
        self.tmp.cleanup()

    def make_source(self, customers, accounts, invoices):
        write_source(self.source, customers, accounts, invoices)

    def release(self, out=None):
        return run_release(
            source_path=self.source,
            policy_path=self.policy,
            vault=self.vault,
            output_dir=out or self.out,
        )

    def load_policy_obj(self):
        return load_policy(self.policy)

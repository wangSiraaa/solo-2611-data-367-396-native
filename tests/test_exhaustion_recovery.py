"""代号空间耗尽（可预期拒绝）与发布前中断恢复测试。"""

import json
import os
import sqlite3
import unittest
from unittest import mock

from tests.helpers import MaskKitTestCase
from maskkit.errors import CapacityExhausted, PlanRejected
from maskkit.recover import recover
from maskkit.vault import (
    TokenVault, STATE_RESERVED, STATE_INTERMEDIATE,
    STATE_COMPLETE, STATE_ABANDONED,
)


TINY_POLICY = {
    "policy_id": "p-tiny",
    "sensitive_columns": [
        "customers.customer_id", "customers.email",
        "accounts.account_id", "accounts.owner_id",
        "invoices.account_ref", "invoices.bill_to_email",
    ],
    "public_keep_columns": ["invoices.invoice_id"],
    # email 域容量 = 2^1 = 2 个代号
    "domains": {
        "customer": {"prefix": "C", "alphabet": "01", "width": 2},
        "account": {"prefix": "A", "alphabet": "01", "width": 2},
        "email": {"prefix": "E", "alphabet": "01", "width": 1},
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


def n_rows(n):
    customers = [(f"SRC_C{i}", f"EMAIL_{i}") for i in range(n)]
    accounts = [(f"SRC_A{i}", f"SRC_C{i}") for i in range(n)]
    invoices = [(f"I{i}", f"SRC_A{i}", f"EMAIL_{i}") for i in range(n)]
    return customers, accounts, invoices


class CapacityTests(MaskKitTestCase):
    def test_plan_rejects_when_distinct_values_exceed_capacity(self):
        from tests.helpers import write_policy
        write_policy(self.policy, TINY_POLICY)
        # 3 个不同 email，但 email 容量只有 2。
        self.make_source(*n_rows(3))
        with self.assertRaises(PlanRejected) as ctx:
            self.release()
        self.assertTrue(any("R015" in v and "email" in v
                            for v in ctx.exception.violations))

    def test_runtime_exhaustion_when_vault_almost_full(self):
        # policy 容量足够（hex width2=256），但 vault 中 email 域已被占满，
        # 模拟多次发布累积后代号耗尽的运行时场景。
        from tests.helpers import write_policy
        self.make_source(*n_rows(2))
        # 手工把 email 域 256 个代号全部占掉（不同哈希占位）。
        with self.vault.locked() as conn:
            conn.execute("BEGIN IMMEDIATE")
            for i in range(256):
                conn.execute(
                    "INSERT INTO token_mapping(domain, value_sha256, token) "
                    "VALUES('email', ?, ?)",
                    (f"phantom-{i:04x}", f"E{i:02x}"),
                )
            conn.execute("COMMIT")
        with self.assertRaises(CapacityExhausted) as ctx:
            self.release()
        self.assertEqual(ctx.exception.domain, "email")
        # 耗尽拒绝不得留下半成品输出。
        self.assertFalse((self.out / "release.sqlite").exists())

    def test_probe_finds_free_slot_around_collision(self):
        # 宽度 1、字母表 01 -> 容量 2，仅两个不同值，应成功并互不相同。
        from tests.helpers import write_policy
        write_policy(self.policy, TINY_POLICY)
        self.make_source(*n_rows(2))
        res = self.release()
        conn = sqlite3.connect(res.output_db)
        tokens = {r[0] for r in conn.execute(
            "SELECT DISTINCT email FROM customers")}
        self.assertEqual(tokens, {"E0", "E1"})
        conn.close()


class CrashRecoveryTests(MaskKitTestCase):
    def _rows(self):
        return n_rows(2)

    def test_reserved_release_is_abandoned_and_mappings_kept(self):
        self.make_source(*self._rows())
        # 先完成一个正常发布，制造稳定映射。
        ok = self.release(self.root / "out_ok")

        # 手工制造一个崩溃在 reserved 阶段的发布记录。
        rid = "deadbeef" * 4
        with self.vault.locked() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.vault.create_release(conn, rid, "p-test", "pfp", "sfp")
            conn.execute("COMMIT")

        actions = recover(self.vault)
        self.assertTrue(any(
            a.release_id == rid and a.new_state == STATE_ABANDONED
            for a in actions
        ))

        # 已完成发布的映射不丢失，且仍可 verify。
        from maskkit.verifier import verify_release
        report = verify_release(ok.output_db, ok.receipt_path, self.vault)
        self.assertTrue(report.ok, report.violations)

    def test_orphan_output_after_replace_crash_is_cleaned(self):
        # 成品已原子放到最终路径，但 vault 事务提交前崩溃：
        # 记录停在 reserved 且指向该路径，recover 必须删除孤儿并废弃。
        self.make_source(*self._rows())
        ok = self.release(self.root / "out_ok")
        orphan_dir = self.root / "out_orphan"
        orphan_dir.mkdir()
        orphan_db = orphan_dir / "release.sqlite"
        # 用一份完整成品充当“已 replace 但未提交”的孤儿。
        import shutil
        shutil.copy(ok.output_db, orphan_db)
        rid = "abcd1234" * 4
        with self.vault.locked() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.vault.create_release(
                conn, rid, "p-test", "pfp", "sfp",
                output_db_path=orphan_db,
            )
            conn.execute("COMMIT")

        actions = recover(self.vault)
        self.assertTrue(any(
            a.release_id == rid and a.new_state == STATE_ABANDONED
            for a in actions
        ))
        self.assertFalse(orphan_db.exists())
        # 原有完整发布不受影响。
        with self.vault.locked() as conn:
            self.assertEqual(
                self.vault.get_release(conn, ok.release_id)["state"],
                STATE_COMPLETE,
            )

    def test_intermediate_with_missing_file_is_abandoned(self):
        self.make_source(*self._rows())
        rid = "cafebabe" * 4
        ghost = self.root / "out_gone" / "release.sqlite"
        with self.vault.locked() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.vault.create_release(conn, rid, "p-test", "pfp", "sfp")
            self.vault.mark_intermediate(conn, rid, ghost)
            conn.execute("COMMIT")
        actions = recover(self.vault)
        self.assertTrue(any(
            a.release_id == rid and a.new_state == STATE_ABANDONED
            for a in actions
        ))

    def test_intermediate_with_healthy_file_completes(self):
        # 正常发布后，把状态回退到 intermediate，模拟最后状态更新前崩溃。
        self.make_source(*self._rows())
        res = self.release()
        with self.vault.locked() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE release_record SET state='intermediate' "
                "WHERE release_id=?", (res.release_id,)
            )
            conn.execute("COMMIT")
        actions = recover(self.vault)
        self.assertTrue(any(
            a.release_id == res.release_id
            and a.new_state == STATE_COMPLETE
            for a in actions
        ))
        with self.vault.locked() as conn:
            state = self.vault.get_release(conn, res.release_id)["state"]
        self.assertEqual(state, STATE_COMPLETE)

    def test_failed_release_never_marked_complete(self):
        self.make_source(*self._rows())
        # 在 mark_complete 处注入异常，模拟成品落盘后提交前崩溃。
        with mock.patch.object(
            TokenVault, "mark_complete",
            side_effect=RuntimeError("simulated crash"),
        ):
            with self.assertRaises(RuntimeError):
                self.release()
        with self.vault.locked() as conn:
            states = {
                r["release_id"]: r["state"]
                for r in self.vault.list_releases(conn)
            }
        self.assertNotIn(STATE_COMPLETE, set(states.values()))
        # 恢复后该失败包被废弃，且没有成品被误认完成。
        recover(self.vault)
        with self.vault.locked() as conn:
            states = {
                r["release_id"]: r["state"]
                for r in self.vault.list_releases(conn)
            }
        self.assertNotIn(STATE_COMPLETE, set(states.values()))


if __name__ == "__main__":
    unittest.main()

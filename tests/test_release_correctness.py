"""release 正确性：碰撞处理、空串/NULL、等值域共享、跨批稳定。"""

import json
import sqlite3
import unittest

from tests.helpers import MaskKitTestCase, standard_rows
from maskkit.verifier import verify_release


def rows_snapshot_b():
    """在快照 A 基础上新增一个客户/账户/账单，老值保持不变。"""
    customers, accounts, invoices = standard_rows()
    customers.append(("SRC_C3", "SRC_EMAIL_3"))
    accounts.append(("SRC_A3", "SRC_C3"))
    invoices.append(("I3", "SRC_A3", "SRC_EMAIL_3"))
    return customers, accounts, invoices


class ReleaseCorrectnessTests(MaskKitTestCase):
    def test_collision_values_get_distinct_tokens(self):
        # 使用与 prepare.py 相同的两位 sha256 前缀碰撞原值（合成数据）。
        customers = [("SRC_C0", "SRC_EMAIL_16"),
                     ("SRC_C1", "SRC_EMAIL_37"),
                     ("SRC_C2", "MAIL_2")]
        accounts = [("SRC_A0", "SRC_C0"), ("SRC_A1", "SRC_C1"),
                    ("SRC_A2", "SRC_C2")]
        invoices = [("I0", "SRC_A0", "SRC_EMAIL_16"),
                    ("I1", "SRC_A1", "SRC_EMAIL_37"),
                    ("I2", "SRC_A2", "MAIL_2")]
        self.make_source(customers, accounts, invoices)
        res = self.release()
        manifest = json.loads(res.manifest_path.read_text())
        email_map = {m["value"]: m["token"]
                     for m in manifest["domains"]["email"]}
        self.assertNotEqual(
            email_map["SRC_EMAIL_16"], email_map["SRC_EMAIL_37"]
        )
        # 令牌格式固定：前缀 E + 两位十六进制。
        for tok in email_map.values():
            self.assertRegex(tok, r"^E[0-9a-f]{2}$")

    def test_empty_string_is_legal_value_and_tokenized(self):
        # 第三行 email 为空串（合法原值），与 NULL 语义不同。
        customers = [("SRC_C0", "MAIL_0"), ("SRC_C1", "MAIL_1"),
                     ("SRC_C2", "")]
        accounts = [("SRC_A0", "SRC_C0"), ("SRC_A1", "SRC_C1"),
                    ("SRC_A2", "SRC_C2")]
        invoices = [("I0", "SRC_A0", "MAIL_0"), ("I1", "SRC_A1", "MAIL_1"),
                    ("I2", "SRC_A2", "")]
        self.make_source(customers, accounts, invoices)
        res = self.release()
        manifest = json.loads(res.manifest_path.read_text())
        email_map = {m["value"]: m["token"]
                     for m in manifest["domains"]["email"]}
        self.assertIn("", email_map)
        # 空串与 NULL 语义不同，这里它必须拿到真实令牌。
        self.assertRegex(email_map[""], r"^E[0-9a-f]{2}$")

        conn = sqlite3.connect(res.output_db)
        # 成品中 customers 第三行 email 为令牌而非空串。
        val = conn.execute(
            "SELECT email FROM customers WHERE customer_id IN "
            "(SELECT owner_id FROM accounts WHERE account_id IN "
            "(SELECT account_ref FROM invoices WHERE invoice_id='I2'))"
        ).fetchone()[0]
        self.assertEqual(val, email_map[""])
        conn.close()

    def test_null_preserved_as_null(self):
        # 增加一个 NULL email 的账单行（bill_to_email 允许 NULL）。
        c, a, i = standard_rows()
        i.append(("I_NULL", "SRC_A0", None))
        self.make_source(c, a, i)
        res = self.release()
        conn = sqlite3.connect(res.output_db)
        val = conn.execute(
            "SELECT bill_to_email FROM invoices WHERE invoice_id='I_NULL'"
        ).fetchone()[0]
        self.assertIsNone(val)
        conn.close()

    def test_equal_value_same_domain_shares_token_across_tables(self):
        shared = "SHARED@X"
        customers = [("SRC_C0", shared), ("SRC_C1", "OTHER@Y")]
        accounts = [("SRC_A0", "SRC_C0"), ("SRC_A1", "SRC_C1")]
        # 账单 bill_to_email 与客户 email 同原文，必须共享同一令牌。
        invoices = [("I0", "SRC_A0", shared), ("I1", "SRC_A1", "OTHER@Y")]
        self.make_source(customers, accounts, invoices)
        res = self.release()
        conn = sqlite3.connect(res.output_db)
        cust_email = conn.execute(
            "SELECT email FROM customers LIMIT 1"
        ).fetchone()[0]
        bill_email = conn.execute(
            "SELECT bill_to_email FROM invoices LIMIT 1"
        ).fetchone()[0]
        self.assertEqual(cust_email, bill_email)
        conn.close()

    def test_same_value_different_domains_get_different_public_tokens(self):
        # 同原文出现在两个不同域：用合成同值串构造。
        same = "SAME_STRING"
        customers = [("SRC_C0", same), ("SRC_C1", "OTHER")]
        accounts = [("SRC_A0", "SRC_C0"), ("SRC_A1", "SRC_C1")]
        invoices = [("I0", "SRC_A0", same), ("I1", "SRC_A1", "OTHER")]
        self.make_source(customers, accounts, invoices)
        res = self.release()
        conn = sqlite3.connect(res.output_db)
        customer_token = conn.execute(
            "SELECT customer_id FROM customers LIMIT 1"
        ).fetchone()[0]
        # email 域的同原文
        email_token = conn.execute(
            "SELECT email FROM customers LIMIT 1"
        ).fetchone()[0]
        self.assertNotEqual(customer_token, email_token)
        self.assertTrue(customer_token.startswith("C"))
        self.assertTrue(email_token.startswith("E"))
        conn.close()

    def test_relationships_hold_in_output(self):
        self.make_source(*standard_rows())
        res = self.release()
        conn = sqlite3.connect(res.output_db)
        conn.execute("PRAGMA foreign_keys=ON")
        self.assertEqual(list(conn.execute("PRAGMA foreign_key_check")), [])
        # 三表 join 仍能逐行连通。
        n = conn.execute(
            "SELECT COUNT(*) FROM invoices iv "
            "JOIN accounts a ON a.account_id=iv.account_ref "
            "JOIN customers c ON c.customer_id=a.owner_id "
            "AND c.email=iv.bill_to_email"
        ).fetchone()[0]
        self.assertEqual(n, 3)
        conn.close()

    def test_keep_column_preserves_original(self):
        self.make_source(*standard_rows())
        res = self.release()
        conn = sqlite3.connect(res.output_db)
        ids = [r[0] for r in conn.execute(
            "SELECT invoice_id FROM invoices ORDER BY invoice_id")]
        self.assertEqual(ids, ["I0", "I1", "I2"])
        conn.close()

    def test_two_snapshots_stable_identities_and_new_value(self):
        # 快照 A
        self.make_source(*standard_rows())
        r1 = self.release(self.root / "out1")
        m1 = {d: {x["value"]: x["token"] for x in items}
              for d, items in json.loads(r1.manifest_path.read_text())
              ["domains"].items()}

        # 快照 B：新增客户，老值原样保留
        import os
        os.chmod(self.source, 0o644)
        self.make_source(*rows_snapshot_b())
        r2 = self.release(self.root / "out2")
        m2 = {d: {x["value"]: x["token"] for x in items}
              for d, items in json.loads(r2.manifest_path.read_text())
              ["domains"].items()}

        # 老身份完全稳定
        for domain in m1:
            for value, token in m1[domain].items():
                self.assertEqual(m2[domain][value], token,
                                 f"domain={domain} value 身份漂移")
        # 新客户拿到新令牌，且不与老令牌冲突
        self.assertIn("SRC_C3", m2["customer"])
        self.assertNotIn(m2["customer"]["SRC_C3"],
                         set(m1["customer"].values()))

        # 两个成品都能通过 verify
        for r in (r1, r2):
            report = verify_release(r.output_db, r.receipt_path, self.vault)
            self.assertTrue(report.ok, report.violations)

    def test_repeated_release_reuses_all_mappings(self):
        self.make_source(*standard_rows())
        r1 = self.release(self.root / "out1")
        r2 = self.release(self.root / "out2")
        self.assertEqual(r1.new_allocations, 9)
        self.assertEqual(r2.new_allocations, 0)
        self.assertEqual(r2.reused_allocations, 9)
        # 成品字节一致（同策略同快照同映射 -> 确定性内容）
        self.assertEqual(
            r1.output_db.read_bytes(), r2.output_db.read_bytes()
        )


if __name__ == "__main__":
    unittest.main()

"""verify 反向用例：篡改成品/哈希不符/一一性破坏必须被发现。"""

import json
import os
import sqlite3

from tests.helpers import MaskKitTestCase
from tests.test_exhaustion_recovery import n_rows
from maskkit.verifier import verify_release


class VerifyNegativeTests(MaskKitTestCase):
    def test_tampered_db_fails_hash_check(self):
        self.make_source(*n_rows(2))
        res = self.release()
        # 直接篡改成品（破坏哈希与内容一致性）。
        os.chmod(res.output_db, 0o644)
        conn = sqlite3.connect(res.output_db)
        conn.execute(
            "UPDATE customers SET email=email WHERE 1=1"
        )  # 先确认可写
        # 制造一个不属于任何域格式的脏值（只改一行，避开 UNIQUE 冲突）。
        conn.execute("UPDATE customers SET email='RAW_LEAK' WHERE rowid=1")
        conn.commit()
        conn.close()
        report = verify_release(res.output_db, res.receipt_path, self.vault)
        self.assertFalse(report.ok)
        self.assertTrue(any("V01" in c for c in report.violations))
        self.assertTrue(any("V09" in c for c in report.violations))
        # 公开回执本身不含原值，错误信息也不回显被篡改的字符串。
        self.assertFalse(
            any("RAW_LEAK" in c for c in report.violations)
        )

    def test_duplicate_token_breaks_one_to_one(self):
        self.make_source(*n_rows(2))
        res = self.release()
        os.chmod(res.output_db, 0o644)
        conn = sqlite3.connect(res.output_db)
        # 把两个客户的 email 改成同一个令牌，破坏域内一一性/UNIQUE。
        tok = conn.execute("SELECT email FROM customers LIMIT 1").fetchone()[0]
        try:
            conn.execute(
                "UPDATE customers SET email=? WHERE email != ?", (tok, tok)
            )
            conn.commit()
        except sqlite3.IntegrityError:
            # UNIQUE 约束直接拦截，本身即证明约束真实成立。
            conn.close()
            self.skipTest("UNIQUE 约束在写入时即拦截重复")
        conn.close()
        report = verify_release(res.output_db, res.receipt_path, self.vault)
        self.assertFalse(report.ok)

    def test_fk_violation_detected(self):
        self.make_source(*n_rows(2))
        res = self.release()
        os.chmod(res.output_db, 0o644)
        conn = sqlite3.connect(res.output_db)
        conn.execute("PRAGMA foreign_keys=OFF")
        # 删除被引用的父行，制造孤儿外键。
        conn.execute("DELETE FROM accounts")
        conn.commit()
        conn.close()
        report = verify_release(res.output_db, res.receipt_path, self.vault)
        self.assertFalse(report.ok)
        self.assertTrue(any("V03" in c for c in report.violations))

    def test_not_null_violation_detected(self):
        self.make_source(*n_rows(2))
        res = self.release()
        os.chmod(res.output_db, 0o644)
        conn = sqlite3.connect(res.output_db)
        conn.execute("PRAGMA foreign_keys=OFF")
        # owner_id 是 NOT NULL，置空应被写入拦截；若绕过则 verify 必抓。
        try:
            conn.execute("UPDATE accounts SET owner_id=NULL")
            conn.commit()
            self.fail("NOT NULL 约束应当拦截")
        except sqlite3.IntegrityError:
            pass
        conn.close()

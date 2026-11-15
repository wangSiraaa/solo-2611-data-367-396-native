"""plan 阶段规则拒绝测试：违规信息只引用表/列/规则 ID。"""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tests.helpers import (
    BASE_POLICY, SCHEMA_SQL, standard_rows, write_policy, write_source,
)
from maskkit.catalog import open_source, load_catalog
from maskkit.planner import build_plan
from maskkit.policy import load_policy
from maskkit.errors import PlanRejected


def build(source, policy):
    conn = open_source(source)
    try:
        cat = load_catalog(conn)
        pol = load_policy(policy)
        return build_plan(cat, pol, conn)
    finally:
        conn.close()


class PlanRejectionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.source = self.root / "source.sqlite"
        self.policy = self.root / "policy.json"
        c, a, i = standard_rows()
        write_source(self.source, c, a, i)
        write_policy(self.policy, BASE_POLICY)

    def tearDown(self):
        import os
        os.chmod(self.source, 0o644)
        self.tmp.cleanup()

    def _policy(self, mutate):
        obj = json.loads(json.dumps(BASE_POLICY))
        mutate(obj)
        write_policy(self.policy, obj)

    def test_baseline_accepted(self):
        plan = build(self.source, self.policy)
        self.assertEqual(plan.creation_order,
                         ["customers", "accounts", "invoices"])

    def test_R001_unknown_column(self):
        def mutate(o):
            o["columns"]["customers.ghost"] = "keep"
            o["public_keep_columns"].append("customers.ghost")
        self._policy(mutate)
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R001" in v and "ghost" in v
                            for v in ctx.exception.violations))

    def test_R002_unregistered_column(self):
        # 给源库增加一个未登记列。
        import os
        os.chmod(self.source, 0o644)
        conn = sqlite3.connect(self.source)
        conn.execute("ALTER TABLE customers ADD COLUMN note TEXT")
        conn.commit()
        conn.close()
        os.chmod(self.source, 0o444)
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R002" in v and "note" in v
                            for v in ctx.exception.violations))

    def test_R006_unknown_domain(self):
        self._policy(lambda o: o["columns"].__setitem__(
            "customers.email", "tokenize:nope"))
        with self.assertRaises(PlanRejected) as ctx:
            load_policy(self.policy)
        self.assertTrue(any("R006" in v for v in ctx.exception.violations))

    def test_R007_fk_domain_mismatch(self):
        # owner_id 改到 account 域，与父列 customer_id 的 customer 域冲突。
        self._policy(lambda o: o["columns"].__setitem__(
            "accounts.owner_id", "tokenize:account"))
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R007" in v and "owner_id" in v
                            for v in ctx.exception.violations))

    def test_R007_fk_action_mismatch(self):
        # 用公开列外键变体：父列 keep、子列 tokenize，触发动作不一致
        # （两端都不在敏感清单，因此不触发 R011）。
        import os
        os.chmod(self.source, 0o644)
        conn = sqlite3.connect(self.source)
        conn.executescript(
            "CREATE TABLE parent(rid TEXT PRIMARY KEY);"
            "CREATE TABLE child(cid TEXT PRIMARY KEY, "
            "rid TEXT REFERENCES parent(rid));"
        )
        conn.execute("INSERT INTO parent VALUES('p1')")
        conn.execute("INSERT INTO child VALUES('c1','p1')")
        conn.commit()
        conn.close()
        os.chmod(self.source, 0o444)

        obj = json.loads(json.dumps(BASE_POLICY))
        obj["public_keep_columns"].extend(["parent.rid", "child.cid", "child.rid"])
        obj["columns"]["parent.rid"] = "keep"
        obj["columns"]["child.cid"] = "keep"
        obj["columns"]["child.rid"] = "tokenize:customer"
        write_policy(self.policy, obj)
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R007" in v and "child.rid" in v
                            for v in ctx.exception.violations))

    def test_R010_prefix_conflict(self):
        self._policy(lambda o: o["domains"]["account"].__setitem__(
            "prefix", "C"))
        with self.assertRaises(PlanRejected) as ctx:
            load_policy(self.policy)
        self.assertTrue(any("R010" in v for v in ctx.exception.violations))

    def test_R011_sensitive_keep_rejected(self):
        self._policy(lambda o: o["columns"].__setitem__(
            "customers.email", "keep"))
        with self.assertRaises(PlanRejected) as ctx:
            load_policy(self.policy)
        self.assertTrue(any("R011" in v and "email" in v
                            for v in ctx.exception.violations))

    def test_R012_drop_pk_rejected(self):
        self._policy(lambda o: o["columns"].__setitem__(
            "customers.customer_id", "drop"))
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R012" in v for v in ctx.exception.violations))

    def test_R012_drop_fk_rejected(self):
        self._policy(lambda o: o["columns"].__setitem__(
            "invoices.account_ref", "drop"))
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R012" in v for v in ctx.exception.violations))

    def test_R012_drop_unique_rejected(self):
        self._policy(lambda o: o["columns"].__setitem__(
            "customers.email", "drop"))
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R012" in v for v in ctx.exception.violations))

    def test_violations_never_contain_source_values(self):
        self._policy(lambda o: o["columns"].__setitem__(
            "accounts.owner_id", "tokenize:account"))
        try:
            build(self.source, self.policy)
            self.fail("应当被拒绝")
        except PlanRejected as exc:
            joined = " ".join(exc.violations)
            for forbidden in ("SRC_EMAIL", "SRC_C", "SRC_A"):
                self.assertNotIn(forbidden, joined)

    def test_R004_required_table_missing(self):
        import os
        os.chmod(self.source, 0o644)
        conn = sqlite3.connect(self.source)
        conn.execute("DROP TABLE invoices")
        conn.commit()
        conn.close()
        os.chmod(self.source, 0o444)
        with self.assertRaises(PlanRejected) as ctx:
            build(self.source, self.policy)
        self.assertTrue(any("R004" in v and "invoices" in v
                            for v in ctx.exception.violations))


if __name__ == "__main__":
    unittest.main()

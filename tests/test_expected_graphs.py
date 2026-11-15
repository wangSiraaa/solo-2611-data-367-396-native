"""Expected-graph checks for the contracted SQL subset.

Each assertion is an independent expected lineage graph built from the
semantics in the task statement, not from observed test data.
"""
from __future__ import annotations

import unittest
from pathlib import Path

from tests._helpers import build, impact, value_ids, control_ids, view_doc, write_project

EDGE_CATALOG = {
    "tables": {
        "orders": {"order_id": "TEXT", "customer_id": "TEXT", "amount": "INTEGER",
                   "status": "TEXT", "region": "TEXT"},
        "customers": {"customer_id": "TEXT", "name": "TEXT", "region": "TEXT",
                      "tier": "TEXT"},
        "payments": {"payment_id": "TEXT", "order_id": "TEXT", "paid": "INTEGER",
                     "method": "TEXT"},
    },
    "functions": {
        "abs": {"dependency": "argument_dependent"},
        "opaque_score": {"dependency": "unknown"},
    },
}

TMP = Path("/tmp/lineage_tests/graphs")


class ExpectedGraphTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        views = {
            "v_star": "SELECT o.*, c.name FROM orders o "
                      "LEFT JOIN customers c ON o.customer_id=c.customer_id",
            "v_ononly": "SELECT o.order_id, o.amount FROM orders o "
                        "INNER JOIN payments p ON o.order_id=p.order_id",
            "v_count": "SELECT region, COUNT(*) AS n, COUNT(status) AS n_status, "
                       "SUM(amount) AS s FROM orders GROUP BY region",
            "v_cte_shadow": (
                "WITH customers AS ("
                "SELECT o1.customer_id AS cid, o1.region AS label, o2.amount AS amt "
                "FROM orders o1 JOIN orders o2 ON o1.customer_id=o2.customer_id) "
                "SELECT cid, label, amt FROM customers"),
            "v_using": "SELECT customer_id, o.amount, c.tier FROM orders o "
                       "LEFT JOIN customers c USING (customer_id)",
            "v_case": "SELECT order_id, CASE WHEN status='ok' THEN amount "
                      "ELSE amount+1 END AS adj FROM orders",
            "v_selfalias": (
                "SELECT o1.order_id AS a, o2.order_id AS b, o1.amount-o2.amount AS diff "
                "FROM orders o1 JOIN orders o2 ON o1.customer_id=o2.customer_id"),
            "v_ambiguous": "SELECT customer_id FROM orders o "
                           "JOIN customers c ON o.customer_id=c.customer_id",
            "v_dupname": "SELECT o.amount, p.paid AS amount FROM orders o "
                         "JOIN payments p ON o.order_id=p.order_id",
            "v_unregistered": "SELECT mystery_fn(amount) AS x FROM orders",
            "v_unk_down1": "SELECT opaque_score(amount) AS score FROM orders",
            "v_unk_down2": "SELECT score+1 AS s2, order_id AS oid FROM v_unk_down1 "
                           "JOIN orders ON 1=1",
            "v_abs": "SELECT abs(amount-10) AS mag FROM orders",
            "v_recursive": (
                "WITH RECURSIVE r(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM r) "
                "SELECT x FROM r"),
            "v_missing": "SELECT * FROM ghost_table",
            "v_having": "SELECT region, SUM(amount) AS s FROM orders "
                        "GROUP BY region HAVING SUM(amount) > 1",
        }
        write_project(TMP, EDGE_CATALOG, views)
        cls.summary = build(TMP)
        cls.docs = {p.stem: __import__("json").loads(p.read_text())
                    for p in (TMP / "catalog_out" / "views").glob("*.json")}

    def col(self, view: str, name: str) -> dict:
        return next(c for c in self.docs[view]["columns"] if c["name"] == name)

    # -- star expansion ----------------------------------------------------- #
    def test_star_expands_every_binding_column_with_join_control(self):
        doc = self.docs["v_star"]
        self.assertEqual(
            [c["name"] for c in doc["columns"]],
            ["order_id", "customer_id", "amount", "status", "region", "name"],
        )
        for c in doc["columns"][:5]:
            self.assertIn("orders." + c["name"], value_ids(c))
            self.assertIn("orders.customer_id", control_ids(c, "join_pairing"))
        name_col = self.col("v_star", "name")
        self.assertEqual(value_ids(name_col), {"customers.name"})
        # LEFT JOIN null side recorded even though name is projected.
        self.assertIn("customers.name", control_ids(name_col, "left_null_side"))

    # -- unprojected ON column ---------------------------------------------- #
    def test_unprojected_on_column_is_control_source(self):
        order_id = self.col("v_ononly", "order_id")
        amount = self.col("v_ononly", "amount")
        # payments.order_id is never projected, yet decides pairing/multiplicity.
        self.assertIn("payments.order_id", control_ids(order_id, "join_pairing"))
        self.assertIn("payments.order_id", control_ids(amount, "join_pairing"))
        self.assertNotIn("payments.order_id", value_ids(amount))
        self.assertIn("orders.order_id", control_ids(amount, "join_pairing"))

    # -- COUNT(*) / COUNT(x) / SUM ------------------------------------------ #
    def test_count_star_has_no_value_source_but_full_control(self):
        n = self.col("v_count", "n")
        self.assertEqual(n["precision"], "no_value_source")
        self.assertEqual(value_ids(n), set())
        self.assertIn("orders.region", control_ids(n, "group_key"))

    def test_count_expr_is_control_sum_is_value(self):
        n_status = self.col("v_count", "n_status")
        self.assertEqual(value_ids(n_status), set())  # null/non-null only
        self.assertIn("orders.status", control_ids(n_status, "count_input"))
        self.assertIn("orders.region", control_ids(n_status, "group_key"))
        s = self.col("v_count", "s")
        self.assertEqual(value_ids(s), {"orders.amount"})
        self.assertIn("orders.region", control_ids(s, "group_key"))

    # -- CTE shadowing ------------------------------------------------------ #
    def test_cte_same_name_shadows_base_table(self):
        doc = self.docs["v_cte_shadow"]
        self.assertEqual({c["name"] for c in doc["columns"]},
                         {"cid", "label", "amt"})
        # Everything resolves inside the CTE; base customers table never used.
        all_ids = {d["base"]["id"] for c in doc["columns"]
                   for d in c["value_sources"] + c["control_sources"]}
        self.assertTrue(all_ids and all(x.startswith("orders.") for x in all_ids))
        cid = self.col("v_cte_shadow", "cid")
        self.assertEqual(value_ids(cid), {"orders.customer_id"})

    # -- USING -------------------------------------------------------------- #
    def test_using_merge_key_combines_both_sides_and_appears_once(self):
        doc = self.docs["v_using"]
        names = [c["name"] for c in doc["columns"]]
        self.assertEqual(names.count("customer_id"), 1)
        key = self.col("v_using", "customer_id")
        self.assertEqual(value_ids(key),
                         {"orders.customer_id", "customers.customer_id"})
        self.assertIn("orders.customer_id", control_ids(key, "join_pairing"))
        self.assertIn("customers.customer_id", control_ids(key, "join_pairing"))
        tier = self.col("v_using", "tier")
        self.assertIn("customers.tier", control_ids(tier, "left_null_side"))

    # -- CASE --------------------------------------------------------------- #
    def test_case_condition_is_control_and_branches_are_value(self):
        adj = self.col("v_case", "adj")
        self.assertEqual(value_ids(adj), {"orders.amount"})
        self.assertIn("orders.status", control_ids(adj, "case_condition"))
        # The condition must not be mislabeled as a value source.
        self.assertNotIn("orders.status", value_ids(adj))

    # -- same table, two alias roles ---------------------------------------- #
    def test_same_table_two_aliases_keep_distinct_bindings(self):
        a = self.col("v_selfalias", "a")
        b = self.col("v_selfalias", "b")
        self.assertEqual(value_ids(a), {"orders.order_id"})
        self.assertEqual(value_ids(b), {"orders.order_id"})
        alias_a = next(iter(a["value_sources"]))["base_alias"]
        alias_b = next(iter(b["value_sources"]))["base_alias"]
        self.assertEqual((alias_a, alias_b), ("o1", "o2"))
        diff = self.col("v_selfalias", "diff")
        aliases = {d["base_alias"] for d in diff["value_sources"]}
        self.assertEqual(aliases, {"o1", "o2"})

    # -- deterministic function --------------------------------------------- #
    def test_argument_dependent_function_tracks_arguments(self):
        mag = self.col("v_abs", "mag")
        self.assertFalse(mag["unknown"])
        self.assertEqual(value_ids(mag), {"orders.amount"})

    # -- unknown propagation ------------------------------------------------ #
    def test_unknown_taints_downstream_but_keeps_sibling_exact(self):
        s2 = self.col("v_unk_down2", "s2")
        self.assertTrue(s2["unknown"])
        self.assertEqual(value_ids(s2), {"orders.amount"})
        oid = self.col("v_unk_down2", "oid")
        self.assertFalse(oid["unknown"])
        self.assertEqual(value_ids(oid), {"orders.order_id"})

    # -- rejection semantics (never guessed, never silent no-dependency) ---- #
    def test_ambiguous_duplicate_unregistered_recursive_missing_rejected(self):
        for view, code in [
            ("v_ambiguous", "ambiguous_column"),
            ("v_dupname", "duplicate_output_column"),
            ("v_unregistered", "unregistered_function"),
            ("v_recursive", "recursive_cte_unsupported"),
            ("v_missing", "missing_base_object"),
            ("v_having", "unsupported_sql_construct"),
        ]:
            self.assertEqual(self.docs[view]["status"], "error", view)
            self.assertEqual(self.docs[view]["error"]["code"], code, view)
            self.assertIsNotNone(self.docs[view]["error"].get("message"))

    # -- impact bucket discipline ------------------------------------------- #
    def test_impact_buckets_and_unaffected(self):
        rep = impact(TMP, ["orders.amount"], "value_change")
        self.assertIn("v_count", rep["exact"])
        self.assertIn("v_unk_down1", rep["uncertain"])
        # unknown view is never called exactly unaffected
        self.assertNotIn("v_unk_down1", rep["unaffected"])
        rep2 = impact(TMP, ["customers.tier"], "value_change")
        self.assertIn("v_count", rep2["unaffected"])
        self.assertIn("v_unk_down1", rep2["uncertain"])
        # filter-like control column reported with explainable control path
        rep3 = impact(TMP, ["orders.status"], "value_change")
        case_view = next(c for c in rep3["exact"]["v_case"]["columns"]
                         if c["column"] == "adj")
        self.assertTrue(case_view["control_change_paths"])
        self.assertEqual(case_view["control_change_paths"][0]["reason"],
                         "case_condition")

    def test_drop_reports_binding_level(self):
        rep = impact(TMP, ["orders.amount"], "drop")
        levels = {x["view"]: x["level"] for x in rep["invalid"]}
        self.assertEqual(levels["v_count"], "column_binding")
        rep_t = impact(TMP, [], "drop", tables=["customers"])
        self.assertTrue(any(
            x["level"] == "from_binding" for x in rep_t["invalid"]))

    def test_type_change_uses_same_binding_levels(self):
        rep = impact(TMP, ["orders.amount"], "type_change")
        self.assertTrue(rep["invalid"])
        self.assertTrue(all(x["kind"] == "type_change" for x in rep["invalid"]))

    def test_unknown_in_predicates_taints_row_and_value_outputs(self):
        root = Path("/tmp/lineage_tests/unknown_pred")
        cat = {
            "tables": {"t": {"id": "TEXT", "a": "INTEGER", "b": "INTEGER"}},
            "functions": {"opq": {"dependency": "unknown"}},
        }
        views = {
            "v_case": "SELECT CASE WHEN opq(a)>1 THEN b ELSE 0 END AS x FROM t",
            "v_where": "SELECT id FROM t WHERE opq(b)=1",
        }
        write_project(root, cat, views)
        build(root)
        case_doc = view_doc(root, "v_case")
        where_doc = view_doc(root, "v_where")
        self.assertTrue(case_doc["columns"][0]["unknown"])
        self.assertIn("t.b", value_ids(case_doc["columns"][0]))
        self.assertIn("t.a", control_ids(case_doc["columns"][0], "case_condition"))
        where_col = where_doc["columns"][0]
        self.assertTrue(where_col["unknown"])
        self.assertIn("t.b", control_ids(where_col, "filter"))
        # Impact on an unrelated column still lands the tainted views in
        # uncertain, never in exactly unaffected.
        rep = impact(root, ["t.id"], "value_change")
        self.assertIn("v_case", rep["uncertain"])
        self.assertNotIn("v_case", rep["unaffected"])

    def test_source_count_limit_enforced(self):
        root = Path("/tmp/lineage_tests/limits")
        tables = {f"t{i}": {"k": "TEXT", "v": "INTEGER"} for i in range(6)}
        six = "SELECT t0.v FROM t0 " + " ".join(
            f"JOIN t{i} ON t0.k=t{i}.k" for i in range(1, 6))
        write_project(root, {"tables": tables, "functions": {}}, {"big": six})
        payload = build(root)
        self.assertEqual(payload["errors"], 1)
        self.assertEqual(payload["failures"][0]["code"], "catalog_limit_exceeded")


if __name__ == "__main__":
    unittest.main()

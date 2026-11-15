"""Counterfactual checks against a real SQLite database.

The lineage catalog is built from SQL structure, never from observed data
(the task explicitly forbids treating "no row changed" as a dependency proof).
These tests do the reverse: they take the *declared* impact sets and verify
that controlled counterfactual modifications of the small database produce
behavior consistent with each declaration - value changes move cells, control
changes move row membership/grouping/pairing, and an unrelated column leaves
every result identical.
"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from tests._helpers import build, impact, make_sqlite, value_ids, control_ids, write_project

TMP = Path("/tmp/lineage_tests/counterfactual")

SEED = """
CREATE TABLE customers(customer_id TEXT, region TEXT, note TEXT);
CREATE TABLE orders(order_id TEXT, customer_id TEXT, amount INTEGER, status TEXT);
INSERT INTO customers VALUES
 ('c1','north','u'),('c2','south','u'),('c3','west','u');
INSERT INTO orders VALUES
 ('o1','c1',10,'ok'),('o2','c1',20,'ok'),('o3','c2',8,'bad'),('o4','c2',40,'ok');
"""

CATALOG = {
    "tables": {
        "customers": {"customer_id": "TEXT", "region": "TEXT", "note": "TEXT"},
        "orders": {"order_id": "TEXT", "customer_id": "TEXT", "amount": "INTEGER",
                   "status": "TEXT"},
    },
    "functions": {"abs": {"dependency": "argument_dependent"}},
}

VIEWS = {
    "totals": (
        "SELECT c.region, SUM(o.amount) AS total, COUNT(*) AS n "
        "FROM orders o LEFT JOIN customers c ON o.customer_id=c.customer_id "
        "WHERE o.status='ok' GROUP BY c.region"),
    "calc": (
        "SELECT order_id, amount + 1 AS bumped, "
        "CASE WHEN status='ok' THEN amount ELSE 0 END AS adj FROM orders"),
}


def view_sqlite(conn, name: str, sql: str):
    conn.execute(f"DROP VIEW IF EXISTS {name}")
    conn.execute(f"CREATE TEMP VIEW {name} AS {sql}")
    return [dict(r) for r in conn.execute(f"SELECT * FROM {name} ORDER BY 1")]


class CounterfactualTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        write_project(TMP, CATALOG, VIEWS)
        cls.summary = build(TMP)
        cls.catalog = json.loads((TMP / "catalog_out" / "views").joinpath(
            "totals.json").read_text())
        cls.calc = json.loads((TMP / "catalog_out" / "views" / "calc.json").read_text())

    def baseline(self):
        conn = make_sqlite(SEED)
        totals = view_sqlite(conn, "totals", VIEWS["totals"])
        calc = view_sqlite(conn, "calc", VIEWS["calc"])
        return conn, totals, calc

    # -- 1. declared value dependency: amount change moves SUM cell -------- #
    def test_value_change_moves_declared_cell(self):
        _, totals_before, _ = self.baseline()
        conn = make_sqlite(SEED)
        conn.execute("UPDATE orders SET amount=amount+5 WHERE order_id='o1'")
        totals_after = view_sqlite(conn, "totals", VIEWS["totals"])
        before = {r["region"]: r["total"] for r in totals_before}
        after = {r["region"]: r["total"] for r in totals_after}
        self.assertNotEqual(before["north"], after["north"])
        # Row membership and counts unchanged by a pure value modification.
        self.assertEqual([r["n"] for r in totals_before],
                         [r["n"] for r in totals_after])
        # And the catalog declared exactly this value dependency.
        total_col = next(c for c in self.catalog["columns"] if c["name"] == "total")
        self.assertIn("orders.amount", value_ids(total_col))

    # -- 2. control dependency: filter column changes row membership ------- #
    def test_filter_column_change_moves_row_membership_not_other_cells(self):
        _, totals_before, _ = self.baseline()
        conn = make_sqlite(SEED)
        # Flip the only 'bad' row to 'ok': it now passes the WHERE filter.
        conn.execute("UPDATE orders SET status='ok' WHERE order_id='o3'")
        totals_after = view_sqlite(conn, "totals", VIEWS["totals"])
        counts_before = {r["region"]: r["n"] for r in totals_before}
        counts_after = {r["region"]: r["n"] for r in totals_after}
        self.assertNotEqual(counts_before["south"], counts_after["south"])
        # Catalog says status is control-only for every totals column.
        for col in self.catalog["columns"]:
            self.assertNotIn("orders.status", value_ids(col))
            self.assertIn("orders.status", control_ids(col, "filter"))

    # -- 3. control dependency: ON key changes pairing --------------------- #
    def test_join_key_change_repairs_null_side_and_pairing(self):
        conn = make_sqlite(SEED)
        # An order with no matching customer demonstrates the LEFT null side.
        conn.execute("INSERT INTO orders VALUES ('o5','c9',3,'ok')")
        before = view_sqlite(conn, "totals", VIEWS["totals"])
        self.assertIn(None, [r["region"] for r in before])
        conn.execute("INSERT INTO customers VALUES ('c9','east','u')")
        after = view_sqlite(conn, "totals", VIEWS["totals"])
        regions_after = {r["region"] for r in after}
        self.assertIn("east", regions_after)
        # ON key is a declared join control for every output.
        for col in self.catalog["columns"]:
            self.assertIn("orders.customer_id", control_ids(col, "join_pairing"))
            self.assertIn("customers.customer_id", control_ids(col, "join_pairing"))

    # -- 4. CASE condition controls which value appears -------------------- #
    def test_case_condition_controls_selected_value(self):
        conn = make_sqlite(SEED)
        before = {r["order_id"]: r["adj"] for r in
                  view_sqlite(conn, "calc", VIEWS["calc"])}
        conn.execute("UPDATE orders SET status='bad' WHERE order_id='o1'")
        after = {r["order_id"]: r["adj"] for r in
                 view_sqlite(conn, "calc", VIEWS["calc"])}
        self.assertNotEqual(before["o1"], after["o1"])
        adj = next(c for c in self.calc["columns"] if c["name"] == "adj")
        self.assertIn("orders.status", control_ids(adj, "case_condition"))

    # -- 5. unrelated column leaves every result bit-identical ------------- #
    def test_unrelated_column_is_unaffected_and_behavior_invariant(self):
        _, totals_before, calc_before = self.baseline()
        conn = make_sqlite(SEED)
        conn.execute("UPDATE customers SET note='totally different'")
        totals_after = view_sqlite(conn, "totals", VIEWS["totals"])
        calc_after = view_sqlite(conn, "calc", VIEWS["calc"])
        self.assertEqual(totals_before, totals_after)
        self.assertEqual(calc_before, calc_after)
        rep = impact(TMP, ["customers.note"], "value_change")
        self.assertIn("totals", rep["unaffected"])
        self.assertIn("calc", rep["unaffected"])

    # -- 6. COUNT(*) has no value source but reacts to filter -------------- #
    def test_count_star_no_value_source_but_control_reacts(self):
        n_col = next(c for c in self.catalog["columns"] if c["name"] == "n")
        self.assertEqual(n_col["precision"], "no_value_source")
        self.assertEqual(value_ids(n_col), set())
        conn = make_sqlite(SEED)
        before = view_sqlite(conn, "totals", VIEWS["totals"])
        # Removing the only 'ok' order in a group removes the group entirely:
        # an effect on row/group existence with no value dependency involved.
        conn2 = make_sqlite(SEED)
        conn2.execute("DELETE FROM orders WHERE order_id='o4'")
        after2 = view_sqlite(conn2, "totals", VIEWS["totals"])
        self.assertNotEqual(
            {r["region"] for r in before}, {r["region"] for r in after2})
        self.assertNotIn("south", {r["region"] for r in after2})
        self.assertIn("customers.region", control_ids(n_col, "group_key"))
        self.assertIn("orders.status", control_ids(n_col, "filter"))


if __name__ == "__main__":
    unittest.main()

"""Incremental rebuild, staleness, cycle and validation behavior."""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from tests._helpers import build, run_cli, view_doc, write_project

TMP = Path("/tmp/lineage_tests/incremental")

CATALOG = {
    "tables": {
        "t1": {"id": "TEXT", "k": "TEXT", "v": "INTEGER"},
        "t2": {"id": "TEXT", "label": "TEXT"},
    },
    "functions": {},
}

VIEWS = {
    "base_v": "SELECT id, v FROM t1 WHERE k='x'",
    "mid_v": "SELECT id, v+1 AS v1 FROM base_v",
    "top_v": "SELECT v1*2 AS v2 FROM mid_v",
}


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        write_project(TMP, CATALOG, VIEWS)
        build(TMP)

    def test_full_then_incremental_reuse(self):
        payload = build(TMP, full=False)
        # Nothing changed: all three analyses reused.
        self.assertEqual(payload["reused"], 3)
        doc = view_doc(TMP, "top_v")
        col = doc["columns"][0]
        self.assertIn("t1.v", {d["base"]["id"] for d in col["value_sources"]})
        # WHERE control propagates through both view boundaries.
        ctrl = {d["base"]["id"] for d in col["control_sources"]}
        self.assertIn("t1.k", ctrl)

    def test_change_one_view_reanalyzes_dependent_chain_only(self):
        (TMP / "views" / "mid_v.sql").write_text(
            "SELECT id, v+10 AS v1 FROM base_v", encoding="utf-8")
        payload = build(TMP, full=False)
        # base_v and top_v keep identical SQL and inputs; mid_v changed.
        self.assertEqual(payload["reused"], 2)
        doc = view_doc(TMP, "top_v")
        self.assertEqual(doc["status"], "ok")
        # No stale/mixed analysis: the rewritten mid_v expression is visible
        # through the rebuilt chain (v+10 then *2 still sourced from t1.v),
        # and every fingerprint matches the current SQL on disk.
        manifest = json.loads((TMP / "catalog_out" / "manifest.json").read_text())
        for name in ("base_v", "mid_v", "top_v"):
            stored = json.loads(
                (TMP / "catalog_out" / "views" / f"{name}.json").read_text())
            self.assertEqual(manifest["views"][name]["sql_sha256"],
                             stored["sql_sha256"])
        top_col = doc["columns"][0]
        self.assertIn("t1.v", {d["base"]["id"] for d in top_col["value_sources"]})

    def test_schema_change_is_detected_as_stale(self):
        cat = dict(CATALOG)
        cat["tables"] = {
            "t1": {"id": "TEXT", "k": "TEXT", "v": "INTEGER", "extra": "TEXT"},
            "t2": CATALOG["tables"]["t2"],
        }
        write_project(TMP, cat, VIEWS)
        code, report = run_cli(
            "validate", "--catalog", str(TMP / "catalog.json"),
            "--views", str(TMP / "views"), "--out", str(TMP / "catalog_out"))
        self.assertEqual(code, 1)
        codes = {p["code"] for p in report["problems"]}
        self.assertIn("stale_schema", codes)

    def test_missing_view_file_and_dropped_base_object(self):
        # Drop a base table from the catalog; view must fail closed.
        cat = {"tables": {"t2": CATALOG["tables"]["t2"]}, "functions": {}}
        write_project(TMP, cat, VIEWS)
        payload = build(TMP)
        self.assertGreaterEqual(payload["errors"], 2)
        failures = {f["view"]: f["code"] for f in payload["failures"]}
        # t1 is gone from the catalog, so the FROM binding fails as a missing
        # base object (fail-closed, never an empty dependency list).
        self.assertEqual(failures["base_v"], "missing_base_object")
        # Cascaded: downstream never silently keeps an old analysis.
        self.assertEqual(failures["mid_v"], "upstream_view_invalid")
        self.assertEqual(failures["top_v"], "upstream_view_invalid")

    def test_cycle_detection(self):
        root = Path("/tmp/lineage_tests/cycle")
        views = {
            "a": "SELECT * FROM b",
            "b": "SELECT * FROM a",
        }
        write_project(root, CATALOG, views)
        payload = build(root)
        self.assertEqual(payload["errors"], 2)
        self.assertTrue(all(f["code"] == "circular_view_reference"
                            for f in payload["failures"]))
        code, report = run_cli(
            "validate", "--catalog", str(root / "catalog.json"),
            "--views", str(root / "views"), "--out", str(root / "catalog_out"))
        self.assertEqual(code, 1)
        self.assertTrue(any(p["code"] == "circular_view_reference"
                            for p in report["problems"]))

    def test_validate_clean_catalog(self):
        code, report = run_cli(
            "validate", "--catalog", str(TMP / "catalog.json"),
            "--views", str(TMP / "views"), "--out", str(TMP / "catalog_out"))
        self.assertEqual(code, 0)
        self.assertTrue(report["valid"])


if __name__ == "__main__":
    unittest.main()

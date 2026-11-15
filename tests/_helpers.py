"""Shared helpers for the test suite: fixture projects and SQLite access."""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable


def write_project(root: Path, catalog: dict, views: dict[str, str]) -> None:
    (root / "views").mkdir(parents=True, exist_ok=True)
    (root / "catalog.json").write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False), encoding="utf-8")
    for name, sql in views.items():
        (root / "views" / f"{name}.sql").write_text(sql, encoding="utf-8")


def run_cli(*args: str) -> tuple[int, dict | str]:
    proc = subprocess.run(
        [PYTHON, "-m", "lineage_tool.cli", *args],
        cwd=ROOT, capture_output=True, text=True,
    )
    out = proc.stdout.strip()
    try:
        return proc.returncode, json.loads(out) if out else proc.stderr
    except json.JSONDecodeError:
        return proc.returncode, out + proc.stderr


def build(root: Path, out_name: str = "catalog_out", full: bool = True) -> dict:
    code, payload = run_cli(
        "build", "--catalog", str(root / "catalog.json"),
        "--views", str(root / "views"),
        "--out", str(root / out_name),
        *( ["--full"] if full else []),
    )
    assert code in (0, 1), payload
    return payload


def view_doc(root: Path, view: str, out_name: str = "catalog_out") -> dict:
    return json.loads((root / out_name / "views" / f"{view}.json").read_text())


def impact(root: Path, refs: list[str], kind: str,
           tables: list[str] | None = None, out_name: str = "catalog_out") -> dict:
    args = ["impact", "--out", str(root / out_name), "--change-kind", kind]
    for r in refs:
        args += ["--base-column", r]
    for t in tables or []:
        args += ["--base-table", t]
    code, payload = run_cli(*args)
    assert code == 0, payload
    return payload


def make_sqlite(seed_sql: str) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(seed_sql)
    return conn


def ids(cols_deps: list[dict], kind: str | None = None,
        reason: str | None = None) -> set[str]:
    out = set()
    for d in cols_deps:
        if kind and d["kind"] != kind:
            continue
        if reason and reason not in d["reason"].split("+"):
            continue
        out.add(d["base"]["id"])
    return out


def value_ids(col: dict) -> set[str]:
    return ids(col["value_sources"], "value")


def control_ids(col: dict, reason: str | None = None) -> set[str]:
    return ids(col["control_sources"], "control", reason)

"""端到端编排：plan -> generate -> verify，以及 manifest 与逐槽来源落盘。"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict
from datetime import datetime, timezone

from .catalog import read_catalog
from .csp import build_csp
from .declaration import load_declaration
from .errors import UnsatError
from .loader import load_database
from .parser import parse_schema
from .plan import build_plan
from .solver import build_unsat_conflict, enumerate_satisfiable, solve
from .verifier import verify_database


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _base_manifest(schema, raw_schema: str, raw_decl: str, schema_path: str,
                   generation_path: str, seed: int) -> dict:
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "schema_sql": {"path": os.path.abspath(schema_path),
                           "sha256": hashlib.sha256(raw_schema.encode()).hexdigest()},
            "generation_json": {"path": os.path.abspath(generation_path),
                                "sha256": hashlib.sha256(raw_decl.encode()).hexdigest()},
        },
        "schema_summary": {
            "tables": [
                {"name": t.name, "source_line": t.source_line,
                 "columns": list(t.column_names),
                 "primary_key": list(t.primary_key()) if t.primary_key() else None,
                 "unique_keys": [list(k) for k in t.unique_keys],
                 "foreign_keys": [
                     {"name": f.name, "columns": list(f.columns),
                      "ref_table": f.ref_table, "ref_columns": list(f.ref_columns),
                      "source": f.source.as_dict()} for f in t.foreign_keys],
                 "checks": [a.as_dict() for a in t.all_checks()]}
                for t in schema.tables
            ]
        },
        "seed": seed,
        "canonical_ordering": {
            "tables": "按表名字典序",
            "slots": "按行槽指纹（固定值+规范域）字典序，平局回退原槽号",
            "variables": "(表名, 规范槽号, 列名) 字典序",
            "domains": "整数升序，NULL 置尾；与声明枚举次序无关",
        },
    }


def run_plan(schema_path: str, generation_path: str) -> dict:
    raw_schema = _read_text(schema_path)
    schema = parse_schema(raw_schema)
    catalog = read_catalog(raw_schema, schema)
    decl = load_declaration(generation_path, schema, catalog)
    csp = build_csp(schema, catalog, decl)
    return build_plan(schema, catalog, csp)


def run_generate(schema_path: str, generation_path: str, out_dir: str) -> dict:
    os.makedirs(out_dir, exist_ok=True)
    raw_schema = _read_text(schema_path)
    raw_decl = _read_text(generation_path)

    schema = parse_schema(raw_schema)                 # 子集外约束在此拒绝
    catalog = read_catalog(raw_schema, schema)
    decl = load_declaration(generation_path, schema, catalog)

    plan_path = os.path.join(out_dir, "plan.json")
    conflict_path = os.path.join(out_dir, "unsat-conflict.json")
    manifest_path = os.path.join(out_dir, "manifest.json")
    db_path = os.path.join(out_dir, "synthetic.db")
    prov_path = os.path.join(out_dir, "assignments.jsonl")
    verify_path = os.path.join(out_dir, "verify-report.json")

    try:
        csp = build_csp(schema, catalog, decl)
    except UnsatError as ex:
        # 全固定/静态可定的无解：无变量可枚举，冲突直接来自被违反的真实约束
        conflict = dict(ex.conflict)
        conflict["independence_note"] = (
            "该子问题不含待分配变量：全部行槽为固定值，冲突可直接按原 schema "
            "约束与固定槽逐行核对；对成品库的独立 SQL 查询同样会拒绝该数据。")
        with open(conflict_path, "w", encoding="utf-8") as fh:
            json.dump(conflict, fh, ensure_ascii=False, indent=2)
        manifest = _base_manifest(schema, raw_schema, raw_decl,
                                  schema_path, generation_path, decl.seed)
        manifest.update(outcome="unsat_static",
                        artifacts={"conflict": os.path.abspath(conflict_path)})
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2)
        raise

    plan = build_plan(schema, catalog, csp)
    manifest = _base_manifest(schema, raw_schema, raw_decl,
                              schema_path, generation_path, decl.seed)
    manifest["plan_summary"] = {
        "cyclic_components": plan["reference_relationships"]
                                 ["cyclic_components_must_solve_together"],
        "loading_boundary": plan["loading_boundary"],
        "total_variables": len(csp.variables),
    }
    with open(plan_path, "w", encoding="utf-8") as fh:
        json.dump(plan, fh, ensure_ascii=False, indent=2)

    try:
        result = solve(csp)
    except UnsatError as ex:
        # 独立笛卡尔积枚举核对的冲突子问题；不发布任何数据库
        conflict = build_unsat_conflict(csp)
        conflict["solver_dead_end"] = ex.conflict
        with open(conflict_path, "w", encoding="utf-8") as fh:
            json.dump(conflict, fh, ensure_ascii=False, indent=2)
        manifest.update(outcome="unsat",
                        artifacts={"plan": os.path.abspath(plan_path),
                                   "conflict": os.path.abspath(conflict_path)})
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2)
        raise

    # 独立枚举与 DFS 交叉验证：枚举首个解，确认问题确实可满足
    enum_solutions = enumerate_satisfiable(csp, limit=1)
    if not enum_solutions:
        raise AssertionError("DFS 找到解但独立枚举判定无解，求解器缺陷")

    rows = load_database(db_path, raw_schema, csp, result)
    verify_report = verify_database(db_path, csp, catalog, rows, result)

    with open(prov_path, "w", encoding="utf-8") as fh:
        for row in rows:
            record = {
                "table": row.table,
                "orig_slot_index": row.orig_slot_index,
                "canon_slot": row.canon_slot,
                "row": row.values,
                "provenance": row.provenance,
            }
            fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    with open(verify_path, "w", encoding="utf-8") as fh:
        json.dump(verify_report, fh, ensure_ascii=False, indent=2)

    manifest["outcome"] = "published"
    manifest["artifacts"] = {
        "database": {"path": os.path.abspath(db_path), "sha256": _sha256(db_path)},
        "plan": os.path.abspath(plan_path),
        "assignments": os.path.abspath(prov_path),
        "verify_report": os.path.abspath(verify_path),
    }
    manifest["solver"] = {"search_nodes": result.search_nodes,
                          "backtracks": result.backtracks,
                          "independent_enumeration_confirms_satisfiable": True}
    manifest["row_counts"] = {t.name: len(csp.declaration.tables[t.name])
                              for t in schema.tables}
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    return {"manifest": manifest, "plan": plan, "rows": [asdict(r) for r in rows],
            "verify": verify_report, "paths": {
                "db": db_path, "plan": plan_path, "assignments": prov_path,
                "verify": verify_path, "manifest": manifest_path}}


def run_verify(db_path: str, schema_path: str, generation_path: str) -> dict:
    """对已发布成品库重新打开做独立复核（verify 子命令入口）。"""
    raw_schema = _read_text(schema_path)
    schema = parse_schema(raw_schema)
    catalog = read_catalog(raw_schema, schema)
    decl = load_declaration(generation_path, schema, catalog)
    csp = build_csp(schema, catalog, decl)
    result = solve(csp)
    from .loader import materialize_rows
    rows = materialize_rows(csp, result)
    return verify_database(db_path, csp, catalog, rows, result)

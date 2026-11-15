"""plan：在造库前展示约束来源、必须共同处理的引用关系与装载检查边界。

plan 不修改任何输入，也不产生成品库；它把解析/目录/声明编译成可审阅的
装配计划：
- 每张表的 PK/UNIQUE/NOT NULL/CHECK/FK 及原 schema 位置与原文字节；
- 外键图的强连通分量（SCC）：大小>1 或自引用的分量构成引用环，
  环内表无法用逐表立即检查装载，必须共同分配并在延迟事务中提交；
- 装载边界：foreign_keys=ON、单事务、DEFERRABLE INITIALLY DEFERRED、
  COMMIT 时由真实 SQLite 做最终裁决。
"""
from __future__ import annotations

from .catalog import Catalog
from .csp import CSP
from .model import SchemaModel


def _tarjan_scc(nodes: list[str], edges: dict[str, set[str]]) -> list[list[str]]:
    index = 0
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    low: dict[str, int] = {}
    result: list[list[str]] = []

    def strong(v: str) -> None:
        nonlocal index
        indices[v] = low[v] = index
        index += 1
        stack.append(v)
        on_stack.add(v)
        for w in edges.get(v, ()):
            if w not in indices:
                strong(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], indices[w])
        if low[v] == indices[v]:
            comp = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                comp.append(w)
                if w == v:
                    break
            result.append(sorted(comp))

    for v in nodes:
        if v not in indices:
            strong(v)
    return [sorted(c) for c in sorted(result, key=lambda c: c[0])]


def build_plan(schema: SchemaModel, catalog: Catalog, csp: CSP) -> dict:
    # 外键图：child -> parents
    edges: dict[str, set[str]] = {t.name: set() for t in schema.tables}
    fk_desc = []
    for t in schema.tables:
        for fk in t.foreign_keys:
            edges[t.name].add(fk.ref_table)
            fk_desc.append({
                "name": fk.name,
                "child_table": t.name,
                "child_columns": list(fk.columns),
                "parent_table": fk.ref_table,
                "parent_columns": list(fk.ref_columns),
                "composite": len(fk.columns) == 2,
                "deferrable": "DEFERRABLE INITIALLY DEFERRED",
                "source": fk.source.as_dict(),
            })

    sccs = _tarjan_scc(schema.table_names(), edges)
    cyclic = [c for c in sccs if len(c) > 1 or (len(c) == 1 and c[0] in edges.get(c[0], set()))]
    acyclic = [c for c in sccs if c not in cyclic]

    table_plans = {}
    constraint_sources = {}
    for t in schema.tables:
        cat = catalog.tables[t.name]
        table_plans[t.name] = {
            "source_line": t.source_line,
            "columns": [
                {"name": c.name, "not_null": c.name in cat.not_null,
                 "checks": [a.as_dict() for a in c.checks]}
                for c in t.columns
            ],
            "primary_key": list(t.primary_key()) if t.primary_key() else None,
            "unique_keys": [list(k) for k in t.unique_keys],
            "foreign_keys": [f.name for f in t.foreign_keys],
            "declared_slots": len(csp.declaration.tables[t.name]),
            "variables": [v.as_dict() for vid, v in csp.variables.items() if v.table == t.name],
        }
        srcs: list[dict] = [
            {"kind": "NOT NULL", "column": c.name, "source_line": t.source_line}
            for c in t.columns if c.name in cat.not_null
        ]
        srcs.extend({"kind": "CHECK", **a.as_dict()} for a in t.all_checks())
        if t.primary_key():
            srcs.append({"kind": "PRIMARY KEY", "columns": list(t.primary_key()),
                         "source_line": t.source_line})
        srcs.extend({"kind": "UNIQUE", "columns": list(k), "source_line": t.source_line}
                    for k in t.unique_keys)
        srcs.extend({"kind": "FOREIGN KEY", "name": f.name, "columns": list(f.columns),
                     "ref_table": f.ref_table, "ref_columns": list(f.ref_columns),
                     "source": f.source.as_dict()} for f in t.foreign_keys)
        constraint_sources[t.name] = srcs

    return {
        "inputs": {
            "tables": list(schema.table_names()),
            "total_declared_slots": sum(len(s) for s in csp.declaration.tables.values()),
            "total_variables": len(csp.variables),
            "seed": csp.declaration.seed,
        },
        "constraint_sources": constraint_sources,
        "reference_relationships": {
            "foreign_keys": fk_desc,
            "fk_graph": {k: sorted(v) for k, v in edges.items()},
            "strongly_connected_components": sccs,
            "cyclic_components_must_solve_together": cyclic,
            "acyclic_components": acyclic,
            "note": "环内表的外键无法用逐表立即检查满足；复合UNIQUE与有限域要求"
                    "跨表共同分配，复合外键按整元组匹配同一父键行。",
        },
        "loading_boundary": {
            "fresh_database": "输出路径若已存在则先删除，绝不从旧业务库取子集",
            "pragma_foreign_keys": "ON（连接级打开，不允许关闭后装脏库）",
            "transaction": "单事务 BEGIN; 全部外键 DEFERRABLE INITIALLY DEFERRED",
            "commit_time_checks": [
                "PRAGMA foreign_key_check 为空",
                "COMMIT 触发所有延迟外键约束",
                "UNIQUE/PK/CHECK/NOT NULL 在插入时即由真实 SQLite 强制执行",
            ],
            "on_rejection": "回滚、删除半成品文件、把错误映射回行槽/约束，整份不发布",
        },
        "tables": table_plans,
    }

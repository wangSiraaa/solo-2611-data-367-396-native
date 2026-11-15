"""verify：重新打开成品库做独立复核。

复核内容（任一项失败即 VerifyError）：
1. PRAGMA foreign_keys 可打开；foreign_key_check 为空；
   逐表 table_info 的 NOT NULL/PK、index_list/index_info 的 UNIQUE、
   foreign_key_list 的复合外键与原 schema 目录一致；
2. 精确行数 = 声明行槽数；逐槽按主键/全列多重集比对，固定值未被改动，
   one_of 单元格取值落在声明域内，类型为 INTEGER/NULL；
3. 独立业务约束查询（不依赖装载期 PRAGMA 是否报错）：
   - NOT NULL 计数、CHECK 谓词用 SQL 表达式重算（含 NULL 不拒绝语义）；
   - UNIQUE/PK 重复键用 GROUP BY 独立统计，含 NULL 语义说明；
   - 复合外键按整元组 NOT EXISTS 独立查询（MATCH SIMPLE 的部分 NULL 豁免）；
4. 逐槽赋值来源（provenance）与库内行完全一致。
"""
from __future__ import annotations

import sqlite3

from .catalog import Catalog
from .csp import CSP
from .errors import VerifyError
from .loader import LoadedRow
from .model import CheckKind, NULL
from .solver import SolveResult


def _check_sql_for_atom(atom) -> str:
    """把 CHECK 原子翻成独立 SQL 谓词（NULL 行用 OR col IS NULL 还原不拒绝语义）。"""
    col = atom.column
    if atom.kind is CheckKind.BETWEEN:
        return f"({col} BETWEEN {atom.lo} AND {atom.hi} OR {col} IS NULL)"
    if atom.kind is CheckKind.IN:
        in_list = ",".join(str(v) for v in atom.values)
        return f"({col} IN ({in_list}) OR {col} IS NULL)"
    if atom.kind is CheckKind.LE_CONST:
        return f"({col} <= {atom.hi} OR {col} IS NULL)"
    if atom.kind is CheckKind.GE_CONST:
        return f"({col} >= {atom.lo} OR {col} IS NULL)"
    if atom.kind is CheckKind.LE_COL:
        return f"({atom.left_col} <= {atom.right_col} OR {atom.left_col} IS NULL " \
               f"OR {atom.right_col} IS NULL)"
    raise VerifyError(f"未知 CHECK 类型 {atom.kind}")


def verify_database(db_path: str, csp: CSP, catalog: Catalog,
                    rows: list[LoadedRow], result: SolveResult) -> dict:
    report: dict = {"checks": [], "passed": True}

    def record(name: str, ok: bool, detail: str = "") -> None:
        report["checks"].append({"check": name, "ok": bool(ok), "detail": detail})
        if not ok:
            report["passed"] = False

    # 重新打开成品库：独立连接，与装载期连接完全分离
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error as ex:
        raise VerifyError(f"无法打开成品库 {db_path}: {ex}") from ex
    try:
        # ------------------------------------------------ PRAGMA 层
        conn.execute("PRAGMA foreign_keys=ON")
        record("pragma.foreign_keys_on",
               conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1)
        fkviol = conn.execute("PRAGMA foreign_key_check").fetchall()
        record("pragma.foreign_key_check_empty", not fkviol, repr(fkviol))

        actual_tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        record("schema.table_set", actual_tables == set(csp.schema.table_names()),
               f"actual={sorted(actual_tables)}")

        for t in csp.schema.tables:
            cat = catalog.tables[t.name]
            info = conn.execute(f'PRAGMA "table_info"("{t.name}")').fetchall()
            record(f"catalog[{t.name}].columns",
                   tuple(r[1] for r in info) == cat.column_names)
            record(f"catalog[{t.name}].not_null",
                   frozenset(r[1] for r in info if r[3]) == cat.not_null)
            pk = tuple(r[1] for r in sorted((r for r in info if r[5]),
                                            key=lambda r: r[5])) or None
            record(f"catalog[{t.name}].primary_key", pk == cat.pk, f"got={pk}")
            idx_rows = [r for r in conn.execute(
                f'PRAGMA "index_list"("{t.name}")').fetchall() if r[3] == "u"]
            uniq = []
            for ir in idx_rows:
                ii = conn.execute(f'PRAGMA "index_info"("{ir[1]}")').fetchall()
                uniq.append(tuple(r[2] for r in sorted(ii, key=lambda r: r[0])))
            record(f"catalog[{t.name}].unique_keys",
                   sorted(uniq) == sorted(cat.unique_keys), f"got={sorted(uniq)}")
            fks = []
            for r in conn.execute(f'PRAGMA "foreign_key_list"("{t.name}")').fetchall():
                fks.append((r[0], r[2], r[3], r[4]))
            expect = [(f.id, f.ref_table, f.columns, f.ref_columns) for f in cat.fks]
            got = [(fid, rt, (frm,), (to,)) for fid, rt, frm, to in fks]
            # 复合外键在 pragma 中按多行返回，按 id 聚合后再比
            agg: dict = {}
            for fid, rt, frm, to in fks:
                agg.setdefault((fid, rt), ([], []))
                agg[(fid, rt)][0].append(frm)
                agg[(fid, rt)][1].append(to)
            got_agg = sorted((fid, rt, tuple(cs), tuple(ps))
                             for (fid, rt), (cs, ps) in agg.items())
            expect_agg = sorted((f.id, f.ref_table, f.columns, f.ref_columns)
                                for f in cat.fks)
            record(f"catalog[{t.name}].foreign_keys", got_agg == expect_agg,
                   f"got={got_agg}")
            del got, expect

        # ------------------------------------------------ 行数与逐槽内容
        db_rows: dict[str, list[tuple]] = {}
        for t in csp.schema.tables:
            cols = t.column_names
            fetched = conn.execute(
                f'SELECT {",".join(cols)} FROM "{t.name}"').fetchall()
            db_rows[t.name] = [tuple(r) for r in fetched]
            expected_n = len(csp.declaration.tables[t.name])
            record(f"rows[{t.name}].exact_count", len(fetched) == expected_n,
                   f"db={len(fetched)} declared={expected_n}")

        # 逐槽：固定值/域/类型/provenance
        for row in rows:
            t = csp.schema.table(row.table)
            slot = csp.declaration.tables[row.table][row.orig_slot_index]
            for col in t.columns:
                cell = slot.cells[col.name]
                val = row.values[col.name]
                type_ok = val is NULL or isinstance(val, int) and not isinstance(val, bool)
                record(f"type[{row.table}#{row.orig_slot_index}.{col.name}]",
                       type_ok, f"value={val!r}")
                if cell.fixed:
                    record(f"fixed[{row.table}#{row.orig_slot_index}.{col.name}]",
                           val == cell.value, f"expected={cell.value!r} got={val!r}")
                else:
                    record(f"domain[{row.table}#{row.orig_slot_index}.{col.name}]",
                           val in cell.domain, f"value={val!r} domain={list(cell.domain)}")
                if row.provenance[col.name]["value"] != val:
                    record(f"provenance[{row.table}#{row.orig_slot_index}.{col.name}]",
                           False, "来源记录与实际赋值不一致")
            record(f"provenance[{row.table}#{row.orig_slot_index}].complete",
                   set(row.provenance) == set(t.column_names))

        # 库内多重集与物化行多重集完全一致（无复制/丢失/额外行）
        for t in csp.schema.tables:
            expected_multiset = sorted(
                tuple(row.values[c] for c in t.column_names)
                for row in rows if row.table == t.name)
            record(f"rows[{t.name}].multiset_match",
                   sorted(db_rows[t.name]) == expected_multiset)

        # ------------------------------------------------ 独立业务约束查询
        for t in csp.schema.tables:
            cat = catalog.tables[t.name]
            # NOT NULL
            for c in cat.not_null:
                n = conn.execute(
                    f'SELECT COUNT(*) FROM "{t.name}" WHERE "{c}" IS NULL').fetchone()[0]
                record(f"independent.not_null[{t.name}.{c}]", n == 0, f"nulls={n}")
            # CHECK（独立 SQL 重算）
            for atom in t.all_checks():
                pred = _check_sql_for_atom(atom)
                bad = conn.execute(
                    f'SELECT COUNT(*) FROM "{t.name}" WHERE NOT ({pred})').fetchone()[0]
                record(f"independent.check[{t.name}:{atom.source.text}]",
                       bad == 0, f"violating_rows={bad}")
            # PK/UNIQUE：含 NULL 的键按 SQLite 语义豁免，仅对全非 NULL 元组查重
            keys = ([("PK", t.primary_key())] if t.primary_key() else []) + \
                   [("UNIQUE", k) for k in t.unique_keys]
            for label, key in keys:
                kcols = ",".join(f'"{c}"' for c in key)
                null_guard = " AND ".join(f'"{c}" IS NOT NULL' for c in key)
                dup = conn.execute(
                    f'SELECT COUNT(*) FROM (SELECT {kcols} FROM "{t.name}" '
                    f'WHERE {null_guard} GROUP BY {kcols} HAVING COUNT(*) > 1)'
                ).fetchone()[0]
                record(f"independent.{label.lower()}_distinct[{t.name}({','.join(key)})]",
                       dup == 0, f"duplicate_groups={dup}")
            # 复合外键：整元组 NOT EXISTS；部分 NULL 按 MATCH SIMPLE 豁免
            for fk in t.foreign_keys:
                child = ",".join(f'c."{c}"' for c in fk.columns)
                parent_cols = ",".join(f'p."{c}"' for c in fk.ref_columns)
                null_guard = " AND ".join(f'c."{c}" IS NOT NULL' for c in fk.columns)
                on_match = " AND ".join(
                    f'c."{cc}" IS p."{pc}"'
                    for cc, pc in zip(fk.columns, fk.ref_columns))
                bad = conn.execute(
                    f'SELECT COUNT(*) FROM "{t.name}" c WHERE {null_guard} AND '
                    f'NOT EXISTS (SELECT 1 FROM "{fk.ref_table}" p WHERE {on_match})'
                ).fetchone()[0]
                record(f"independent.fk[{fk.name}]", bad == 0,
                       f"orphan_child_rows={bad}")
                del child, parent_cols

        # 求解器赋值与 provenance 中记录的变量值一致
        for row in rows:
            for col, prov in row.provenance.items():
                if prov["source"] == "one_of":
                    record(f"assignment[{row.table}#{row.orig_slot_index}.{col}]",
                           result.assignment[prov["vid"]] == prov["value"])
    finally:
        conn.close()

    if not report["passed"]:
        failed = [c for c in report["checks"] if not c["ok"]]
        raise VerifyError(f"独立复核失败 {len(failed)} 项: "
                          + "; ".join(f"{c['check']}({c['detail']})" for c in failed[:10]))
    report["total_checks"] = len(report["checks"])
    return report

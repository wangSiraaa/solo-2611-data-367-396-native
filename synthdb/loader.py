"""把求解得到的规范赋值物化为逐槽行，并在真实外键开启下装载新库。

装载纪律（不允许关闭外键后把脏库当完成）：
1. 输出路径若已存在，先删除；执行原始 schema.sql 建表；
2. 连接级 PRAGMA foreign_keys=ON，单事务内插入全部行；
   外键为 DEFERRABLE INITIALLY DEFERRED，循环引用可在事务内暂挂；
3. COMMIT 前执行 PRAGMA foreign_key_check；COMMIT 由 SQLite 做最终裁决；
4. 任一步失败：ROLLBACK、关闭并删除半成品文件，把数据库错误映射回
   原声明行槽与 schema 约束后抛 LoadRejected，整份输出不发布。
"""
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass

from .csp import CSP
from .errors import LoadRejected
from .model import NULL
from .solver import SolveResult


@dataclass
class LoadedRow:
    table: str
    orig_slot_index: int
    canon_slot: int
    values: dict[str, int | None]
    provenance: dict[str, dict]


def materialize_rows(csp: CSP, result: SolveResult) -> list[LoadedRow]:
    """把 (固定常量 + 变量赋值) 还原为按原始声明槽号排列的逐行结果。"""
    rows: list[LoadedRow] = []
    for t in csp.schema.tables:
        slots = csp.declaration.tables[t.name]
        by_orig: dict[int, LoadedRow] = {}
        for cs, (orig_idx, _fp) in enumerate(csp.slot_map[t.name]):
            slot = slots[orig_idx]
            values: dict[str, int | None] = {}
            prov: dict[str, dict] = {}
            for col in t.columns:
                cell = slot.cells[col.name]
                if cell.fixed:
                    values[col.name] = cell.value
                    prov[col.name] = {"source": "fixed", "value": cell.value}
                else:
                    vid = f"{t.name}.slot#{cs}.{col.name}"
                    val = result.assignment[vid]
                    values[col.name] = val
                    prov[col.name] = {
                        "source": "one_of",
                        "vid": vid,
                        "value": val,
                        "declared_domain": list(cell.domain),
                        "canonical_domain": list(csp.variables[vid].domain),
                    }
            by_orig[orig_idx] = LoadedRow(t.name, orig_idx, cs, values, prov)
        rows.extend(by_orig[i] for i in sorted(by_orig))
    return rows


def _map_integrity_error(exc: sqlite3.Error, csp: CSP) -> dict:
    msg = str(exc)
    mapping: dict = {"sqlite_error": msg, "candidates": []}
    low = msg.lower()
    for t in csp.schema.tables:
        if t.name.lower() in low:
            mapping["candidates"].append({"table": t.name})
        for fk in t.foreign_keys:
            if "foreign key" in low and fk.ref_table.lower() in low:
                mapping["candidates"].append(
                    {"constraint": fk.name, "source": fk.source.as_dict()})
        for kc in (list(t.unique_keys) + ([t.primary_key()] if t.primary_key() else [])):
            if "unique" in low or "constraint" in low:
                mapping["candidates"].append({"table": t.name, "key": list(kc)})
    mapping["hint"] = "求解器赋值已通过同语义 CSP 检查，真实数据库仍拒绝说明 " \
                      "CSP 语义与 SQLite 目录存在偏差，需人工核对映射"
    return mapping


def load_database(db_path: str, raw_schema: str, csp: CSP,
                  result: SolveResult) -> list[LoadedRow]:
    rows = materialize_rows(csp, result)
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    committed = False
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        fk_mode = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        if fk_mode != 1:
            raise LoadRejected("无法在连接上启用 foreign_keys=ON，拒绝装载")
        conn.executescript(raw_schema)
        # 建表脚本中的 PRAGMA 不会改变连接策略；再次确认
        if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
            raise LoadRejected("建表后 foreign_keys 被意外关闭，拒绝装载")

        conn.execute("BEGIN")
        try:
            for row in rows:
                cols = tuple(row.values.keys())
                placeholders = ",".join("?" for _ in cols)
                col_list = ",".join(cols)
                conn.execute(
                    f'INSERT INTO "{row.table}" ({col_list}) VALUES ({placeholders})',
                    tuple(None if v is NULL else v for v in row.values.values()),
                )
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise LoadRejected(
                    "COMMIT 前 foreign_key_check 发现违约: "
                    + repr(violations),
                    mapping={"foreign_key_check": [list(v) for v in violations]})
            conn.execute("COMMIT")
            committed = True
        except LoadRejected:
            conn.execute("ROLLBACK")
            raise
        except sqlite3.Error as ex:
            conn.execute("ROLLBACK")
            raise LoadRejected(
                f"真实 SQLite 拒绝了已生成的赋值，整份输出不发布: {ex}",
                mapping=_map_integrity_error(exc=ex, csp=csp)) from ex
    finally:
        conn.close()
        if not committed and os.path.exists(db_path):
            os.remove(db_path)

    if not committed:
        raise LoadRejected("装载未提交，半成品已删除")
    return rows

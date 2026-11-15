"""真实 SQLite 目录读取。

把原始 DDL 在一个一次性内存库中执行（仅用于读取目录，不装载任何业务数据），
通过 PRAGMA 取得真实数据库认定的主键、复合 UNIQUE、NOT NULL 与复合外键，
并与 sqlglot 解析模型交叉核对，避免解析器与真实数据库语义漂移。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .errors import SchemaParseError
from .model import SchemaModel, SourceRef, TableModel


@dataclass(frozen=True)
class CatalogFK:
    id: int
    columns: tuple[str, ...]
    ref_table: str
    ref_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableCatalog:
    name: str
    column_names: tuple[str, ...]
    not_null: frozenset[str]
    pk: tuple[str, ...] | None
    unique_keys: tuple[tuple[str, ...], ...]
    fks: tuple[CatalogFK, ...]


@dataclass(frozen=True)
class Catalog:
    tables: dict[str, TableCatalog]


def _read_table(conn: sqlite3.Connection, table: TableModel) -> TableCatalog:
    name = table.name
    info = conn.execute(f'PRAGMA "table_info"("{name}")').fetchall()
    # (cid, name, type, notnull, dflt_value, pk)
    col_names = tuple(r[1] for r in info)
    not_null = frozenset(r[1] for r in info if r[3])
    pk_cols = [r[1] for r in sorted((r for r in info if r[5]), key=lambda r: r[5])]
    pk = tuple(pk_cols) if pk_cols else None

    unique_keys: list[tuple[str, ...]] = []
    for idx in conn.execute(f'PRAGMA "index_list"("{name}")').fetchall():
        # (seq, name, unique, origin, partial)
        idx_name, unique, origin = idx[1], idx[2], idx[3]
        if origin != "u":        # 只取 UNIQUE 约束自动索引，忽略其他来源
            continue
        if unique != 1:
            raise SchemaParseError(f"表 {name} 自动唯一索引 {idx_name} 非唯一，目录异常")
        rows = conn.execute(f'PRAGMA "index_info"("{idx_name}")').fetchall()
        # (seqno, cid, name)
        key = tuple(r[2] for r in sorted(rows, key=lambda r: r[0]))
        unique_keys.append(key)

    fks: list[CatalogFK] = []
    for row in conn.execute(f'PRAGMA "foreign_key_list"("{name}")').fetchall():
        # (id, seq, table, from, to, on_update, on_delete, match)
        fk_id, _seq, ref_table, frm, to, on_upd, on_del, match = row
        existing = next((f for f in fks if f.id == fk_id), None)
        if existing is None:
            fks.append(CatalogFK(fk_id, (frm,), ref_table, (to,)))
        else:
            idx = next(i for i, f in enumerate(fks) if f.id == fk_id)
            fks[idx] = CatalogFK(
                fk_id,
                existing.columns + (frm,),
                ref_table,
                existing.ref_columns + (to,),
            )
    fks.sort(key=lambda f: f.id)
    return TableCatalog(name, col_names, not_null, pk, tuple(unique_keys), tuple(fks))


def read_catalog(raw_sql: str, schema: SchemaModel) -> Catalog:
    """在独立内存库执行 DDL 并读取目录，随后与解析模型逐项核对。"""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(raw_sql)
        tables: dict[str, TableCatalog] = {}
        names = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
        if names != set(schema.table_names()):
            raise SchemaParseError(
                f"真实数据库表集合 {sorted(names)} 与解析结果 {list(schema.table_names())} 不一致")
        for t in schema.tables:
            tables[t.name] = _read_table(conn, t)
    finally:
        conn.close()

    _cross_check(schema, tables)
    return Catalog(tables=tables)


def _cross_check(schema: SchemaModel, catalog: dict[str, TableCatalog]) -> None:
    for t in schema.tables:
        cat = catalog[t.name]
        if cat.column_names != t.column_names:
            raise SchemaParseError(f"表 {t.name} 列顺序与真实目录不一致")
        model_nn = frozenset(c.name for c in t.columns if c.not_null)
        if cat.not_null != model_nn:
            raise SchemaParseError(
                f"表 {t.name} NOT NULL 不一致: 解析 {sorted(model_nn)} / 目录 {sorted(cat.not_null)}")
        if cat.pk != t.primary_key():
            raise SchemaParseError(
                f"表 {t.name} 主键不一致: 解析 {t.primary_key()} / 目录 {cat.pk}")
        model_uniq = sorted(t.unique_keys)
        cat_uniq = sorted(cat.unique_keys)
        if model_uniq != cat_uniq:
            raise SchemaParseError(
                f"表 {t.name} UNIQUE 不一致: 解析 {model_uniq} / 目录 {cat_uniq}")
        model_fk = sorted((f.columns, f.ref_table, f.ref_columns) for f in t.foreign_keys)
        cat_fk = sorted((f.columns, f.ref_table, f.ref_columns) for f in cat.fks)
        if model_fk != cat_fk:
            raise SchemaParseError(
                f"表 {t.name} 外键不一致: 解析 {model_fk} / 目录 {cat_fk}")


def constraint_sources(schema: SchemaModel) -> dict:
    """汇总 plan/manifest 用的约束来源（行号 + 原文字节）。"""
    out: dict = {}
    for t in schema.tables:
        srcs = []
        if t.table_primary_key:
            srcs.append({"kind": "PRIMARY KEY", "columns": list(t.table_primary_key),
                         "source": SourceRef(t.source_line, "").as_dict() | {"text": None}})
        for uk in t.unique_keys:
            srcs.append({"kind": "UNIQUE", "columns": list(uk)})
        for fk in t.foreign_keys:
            srcs.append({"kind": "FOREIGN KEY", "name": fk.name,
                         "columns": list(fk.columns), "ref_table": fk.ref_table,
                         "ref_columns": list(fk.ref_columns), "source": fk.source.as_dict()})
        for atom in t.all_checks():
            srcs.append({"kind": "CHECK", **atom.as_dict()})
        for c in t.columns:
            if c.not_null:
                srcs.append({"kind": "NOT NULL", "column": c.name})
            if c.primary_key:
                srcs.append({"kind": "PRIMARY KEY", "columns": [c.name]})
        out[t.name] = srcs
    return out

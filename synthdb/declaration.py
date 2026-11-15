"""读取并校验 generation.json 的固定行槽/有限域声明。

规则：
- 每张 schema 表都必须出现（可空 rows），声明不得引入未知表/列；
- 每个单元格要么是固定 int/NULL，要么是 {"one_of": [...]} 有限域；
- 固定值在整个流程中不得改动；行槽不得丢弃、复制或额外增加；
- one_of 域为无重复的 int/NULL，域大小 1..4；总待分配单元格 <= 12；
- NULL 是否允许完全由真实目录决定（NOT NULL/PK 列拒绝 NULL）。
"""
from __future__ import annotations

import json
from typing import Any

from .catalog import Catalog
from .errors import DeclarationError
from .model import CellSpec, Declaration, NULL, SchemaModel, Slot


def _is_int_or_none(v: Any) -> bool:
    return v is NULL or isinstance(v, int) and not isinstance(v, bool)


def load_declaration(path_or_text: str, schema: SchemaModel, catalog: Catalog,
                     from_file: bool = True) -> Declaration:
    if from_file:
        with open(path_or_text, "r", encoding="utf-8") as fh:
            text = fh.read()
    else:
        text = path_or_text
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as ex:
        raise DeclarationError(f"generation 声明不是合法 JSON: {ex}") from ex

    if not isinstance(raw, dict) or set(raw.keys()) - {"tables", "seed"}:
        raise DeclarationError("顶层仅允许键 tables 与 seed")
    if "seed" not in raw or not isinstance(raw["seed"], int) or isinstance(raw["seed"], bool):
        raise DeclarationError("必须提供整数 seed")
    tables_decl = raw.get("tables")
    if not isinstance(tables_decl, dict):
        raise DeclarationError("tables 必须为对象")

    schema_names = set(schema.table_names())
    extra = set(tables_decl) - schema_names
    if extra:
        raise DeclarationError(f"声明包含 schema 中不存在的表: {sorted(extra)}")

    out: dict[str, list[Slot]] = {}
    variable_cells = 0
    for t in schema.tables:
        tname = t.name
        if tname not in tables_decl:
            raise DeclarationError(f"声明缺少表 {tname}（每张表的行槽都必须显式保留）")
        tdecl = tables_decl[tname]
        if not isinstance(tdecl, dict) or set(tdecl.keys()) - {"rows"}:
            raise DeclarationError(f"表 {tname} 下仅允许键 rows")
        rows = tdecl["rows"]
        if not isinstance(rows, list):
            raise DeclarationError(f"表 {tname}.rows 必须为列表")
        cat = catalog.tables[tname]
        slots: list[Slot] = []
        seen_pk: dict[tuple, int] = {}
        for slot_idx, row in enumerate(rows):
            where = f"{tname}.rows[{slot_idx}]"
            if not isinstance(row, dict):
                raise DeclarationError(f"{where} 必须为对象")
            if set(row.keys()) != set(t.column_names):
                raise DeclarationError(
                    f"{where} 必须且只能列出列 {list(t.column_names)}，实际 {sorted(row.keys())}")
            cells: dict[str, CellSpec] = {}
            for col in t.columns:
                spec = row[col.name]
                cwhere = f"{where}.{col.name}"
                not_null = col.name in cat.not_null
                if isinstance(spec, dict):
                    if set(spec.keys()) != {"one_of"}:
                        raise DeclarationError(f"{cwhere} 仅支持 {{\"one_of\": [...]}} 形式")
                    domain = spec["one_of"]
                    if not isinstance(domain, list) or not 1 <= len(domain) <= 4:
                        raise DeclarationError(f"{cwhere} one_of 域大小必须在 1..4 之间")
                    if not all(_is_int_or_none(v) for v in domain):
                        raise DeclarationError(f"{cwhere} 域只允许整数或 NULL")
                    if len(set(domain)) != len(domain):
                        raise DeclarationError(f"{cwhere} 域存在重复值")
                    if NULL in domain and not_null:
                        raise DeclarationError(
                            f"{cwhere} 位于 NOT NULL/PRIMARY KEY 列，域中不允许 NULL")
                    variable_cells += 1
                    cells[col.name] = CellSpec(tname, slot_idx, col.name,
                                               False, None, tuple(domain))
                elif _is_int_or_none(spec):
                    if spec is NULL and not_null:
                        raise DeclarationError(
                            f"{cwhere} 位于 NOT NULL/PRIMARY KEY 列，固定值不允许 NULL")
                    cells[col.name] = CellSpec(tname, slot_idx, col.name,
                                               True, spec, (spec,))
                else:
                    raise DeclarationError(
                        f"{cwhere} 必须是整数/NULL 固定值或 one_of 声明，实际类型 {type(spec).__name__}")
            slot = Slot(tname, slot_idx, cells)
            # 固定主键的早期重复检查（仅固定部分；变量部分交给求解器）
            if cat.pk:
                pk_vals = tuple(cells[c].value if cells[c].fixed else None for c in cat.pk)
                if all(v is not NULL for v in pk_vals):
                    if pk_vals in seen_pk:
                        raise DeclarationError(
                            f"{where} 的固定主键 {pk_vals} 与 rows[{seen_pk[pk_vals]}] 重复")
                    seen_pk[pk_vals] = slot_idx
            slots.append(slot)
        out[tname] = slots

    if variable_cells > 12:
        raise DeclarationError(f"待分配单元格共 {variable_cells} 个，超过上限 12")
    return Declaration(tables=out, seed=raw["seed"], raw=raw)

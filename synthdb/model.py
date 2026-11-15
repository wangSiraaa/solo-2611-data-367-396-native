"""解析后的结构化模型：schema 模型、声明模型、行槽与变量。

所有结构都带 source 来源（行号 + 原文字节），用于 plan 展示、
无解冲突子问题与失败映射。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# 域中的 NULL 哨兵（域里只允许 int 与 NULL）。
NULL = None


class CheckKind(str, Enum):
    BETWEEN = "between"          # col BETWEEN lo AND hi
    IN = "in"                    # col IN (v1, v2, ...)
    LE_CONST = "le_const"        # col <= int
    GE_CONST = "ge_const"        # col >= int
    LE_COL = "le_col"            # col1 <= col2


@dataclass(frozen=True)
class SourceRef:
    """约束在原 schema.sql 中的定位与原文字节。"""

    line: int          # 1 基行号
    text: str          # 原文（按字节保留的片段）

    def as_dict(self) -> dict:
        return {"line": self.line, "text": self.text}


@dataclass(frozen=True)
class CheckAtom:
    kind: CheckKind
    source: SourceRef
    column: str | None = None
    lo: int | None = None
    hi: int | None = None
    values: tuple[int, ...] | None = None
    left_col: str | None = None
    right_col: str | None = None

    def as_dict(self) -> dict:
        d: dict = {"kind": self.kind.value, "source": self.source.as_dict()}
        if self.kind is CheckKind.BETWEEN:
            d.update(column=self.column, lo=self.lo, hi=self.hi)
        elif self.kind is CheckKind.IN:
            d.update(column=self.column, values=list(self.values or ()))
        elif self.kind in (CheckKind.LE_CONST, CheckKind.GE_CONST):
            d.update(column=self.column, value=self.hi if self.kind is CheckKind.LE_CONST else self.lo)
        else:
            d.update(left_col=self.left_col, right_col=self.right_col)
        return d


@dataclass(frozen=True)
class ColumnModel:
    name: str
    not_null: bool
    primary_key: bool                 # 单列 INTEGER PRIMARY KEY（行内）
    checks: tuple[CheckAtom, ...]


@dataclass(frozen=True)
class ForeignKey:
    name: str                          # 合成稳定名 <table>__fk_<n>
    columns: tuple[str, ...]
    ref_table: str
    ref_columns: tuple[str, ...]
    source: SourceRef


@dataclass(frozen=True)
class TableModel:
    name: str
    source_line: int
    columns: tuple[ColumnModel, ...]
    table_primary_key: tuple[str, ...] | None     # 表级 PRIMARY KEY(col[,col])
    unique_keys: tuple[tuple[str, ...], ...]      # 表级 UNIQUE
    foreign_keys: tuple[ForeignKey, ...]
    checks: tuple[CheckAtom, ...]                 # 表级 CHECK 的原子

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def column(self, name: str) -> ColumnModel:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)

    def primary_key(self) -> tuple[str, ...] | None:
        if self.table_primary_key:
            return self.table_primary_key
        single = tuple(c.name for c in self.columns if c.primary_key)
        return single if single else None

    def all_checks(self) -> tuple[CheckAtom, ...]:
        return tuple(ch for c in self.columns for ch in c.checks) + self.checks


@dataclass(frozen=True)
class SchemaModel:
    tables: tuple[TableModel, ...]
    raw_sql: str

    def table(self, name: str) -> TableModel:
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(name)

    def table_names(self) -> tuple[str, ...]:
        return tuple(t.name for t in self.tables)


# ---------------------------------------------------------------- 声明模型

@dataclass(frozen=True)
class CellSpec:
    """一个单元格的声明：固定值或 one_of 有限域。"""

    table: str
    slot_index: int          # 声明中的原始槽号（0 基）
    column: str
    fixed: bool
    value: int | None        # fixed=True 时的固定值
    domain: tuple[int | None, ...]   # fixed=True 时为单元素元组

    @property
    def is_variable(self) -> bool:
        return not self.fixed


@dataclass(frozen=True)
class Slot:
    """一张表中的一个固定行槽，逐列给出 CellSpec。"""

    table: str
    slot_index: int
    cells: dict[str, CellSpec]

    def ordered_cells(self, columns: tuple[str, ...]) -> list[CellSpec]:
        return [self.cells[c] for c in columns]


@dataclass(frozen=True)
class Declaration:
    tables: dict[str, list[Slot]]
    seed: int
    raw: dict

    def all_cells(self) -> list[CellSpec]:
        out: list[CellSpec] = []
        for t in sorted(self.tables):
            for slot in self.tables[t]:
                out.extend(slot.cells.values())
        return out

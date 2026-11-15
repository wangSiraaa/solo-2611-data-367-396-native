"""严格按 schema-subset.md 解析 schema.sql。

策略：
1. sqlglot(sqlite 方言, RAISE) 做语法与 AST 校验；
2. 自带顶层逗号切分器从原文字节中切出每个列/表约束，保证 SourceRef 保留原文；
3. 任何子集之外的结构（AUTOINCREMENT、生成列、DEFAULT、内联 REFERENCES、
   自定义外键动作、OR/函数/算术 CHECK、非 INTEGER 类型、引号标识符……）
   一律抛 SubsetViolation，绝不悄悄忽略。
"""
from __future__ import annotations

import logging
import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

# 子集外特性由本模块显式抛 SubsetViolation；屏蔽 sqlglot 生成器自身的
# unsupported() 警告，避免拒绝信息里混入与决策无关的噪音。
logging.getLogger("sqlglot").setLevel(logging.ERROR)

from .errors import SchemaParseError, SubsetViolation
from .model import (
    CheckAtom,
    CheckKind,
    ColumnModel,
    ForeignKey,
    SchemaModel,
    SourceRef,
    TableModel,
)

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_ALLOWED_PRAGMAS = {"foreign_keys"}


# ---------------------------------------------------------- 原文切分

def _split_top_level(body: str) -> list[tuple[int, str]]:
    """按括号深度 1 的逗号切分 CREATE TABLE 的表体，返回 (相对偏移, 片段)。"""
    parts: list[tuple[int, str]] = []
    depth = 0
    start = 0
    for i, ch in enumerate(body):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                raise SchemaParseError("CREATE TABLE 括号不匹配")
        elif ch == "," and depth == 0:
            parts.append((start, body[start:i]))
            start = i + 1
    if depth != 0:
        raise SchemaParseError("CREATE TABLE 括号不匹配")
    parts.append((start, body[start:]))
    return [(off, seg.strip()) for off, seg in parts if seg.strip()]


def _extract_check_text(segment: str) -> str:
    """从列定义或表约束片段中取出 CHECK(...) 的原文字节（含配对括号）。"""
    m = re.search(r"CHECK\s*\(", segment, flags=re.IGNORECASE)
    if not m:
        return segment
    i = m.end() - 1  # 指向 '('
    depth = 0
    for j in range(i, len(segment)):
        if segment[j] == "(":
            depth += 1
        elif segment[j] == ")":
            depth -= 1
            if depth == 0:
                return segment[m.start():j + 1].strip()
    return segment[m.start():].strip()


# ---------------------------------------------------------- 字面量与原子

def _int_literal(node: exp.Expression, where: str) -> int:
    """子集内的整数字面量：Literal 或 负号 Neg(Literal)。"""
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        return -int(node.this.name)
    if isinstance(node, exp.Literal) and not node.is_string:
        try:
            return int(node.name)
        except ValueError as ex:
            raise SubsetViolation(f"{where} 含非整数字面量") from ex
    raise SubsetViolation(f"{where} 出现不支持的表达式: {node.sql(dialect='sqlite')!r}")


def _column_name(node: exp.Expression, where: str) -> str:
    # CHECK 中为 Column(Identifier)；FK/键列表中直接是 Identifier
    ident = node.this if isinstance(node, exp.Column) else node
    if isinstance(ident, exp.Identifier) and not ident.quoted and _IDENT_RE.match(ident.name):
        return ident.name
    raise SubsetViolation(f"{where} 只能引用未加引号的 ASCII 标识符列")


def _parse_check_tree(tree: exp.Expression, source: SourceRef, where: str) -> list[CheckAtom]:
    """展开 AND 链，逐原子按子集校验；NULL 语义由求解/装载层按 SQLite 处理。"""
    atoms: list[CheckAtom] = []
    stack = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, exp.And):
            stack.append(node.expression)
            stack.append(node.this)
        elif isinstance(node, exp.In):
            col = _column_name(node.this, where)
            if node.args.get("query") is not None or node.args.get("unnest"):
                raise SubsetViolation(f"{where} 的 IN 不支持子查询", f"line {source.line}", source.text)
            vals = tuple(_int_literal(e, where) for e in node.expressions)
            if len(set(vals)) != len(vals):
                raise SubsetViolation(f"{where} 的 IN 集合存在重复值", f"line {source.line}", source.text)
            atoms.append(CheckAtom(CheckKind.IN, source, column=col, values=vals))
        elif isinstance(node, exp.Between):
            col = _column_name(node.this, where)
            lo = _int_literal(node.args["low"], where)
            hi = _int_literal(node.args["high"], where)
            if lo > hi:
                raise SubsetViolation(f"{where} BETWEEN 下界大于上界", f"line {source.line}", source.text)
            atoms.append(CheckAtom(CheckKind.BETWEEN, source, column=col, lo=lo, hi=hi))
        elif isinstance(node, (exp.LTE, exp.GTE)):
            l_col = isinstance(node.this, exp.Column)
            r_col = isinstance(node.expression, exp.Column)
            if l_col and r_col:
                if isinstance(node, exp.GTE):
                    raise SubsetViolation(
                        f"{where} 两列比较仅支持 col <= col，不支持 col >= col",
                        f"line {source.line}", source.text,
                    )
                atoms.append(CheckAtom(
                    CheckKind.LE_COL, source,
                    left_col=_column_name(node.this, where),
                    right_col=_column_name(node.expression, where),
                ))
            elif l_col and not r_col:
                col = _column_name(node.this, where)
                val = _int_literal(node.expression, where)
                kind = CheckKind.LE_CONST if isinstance(node, exp.LTE) else CheckKind.GE_CONST
                atoms.append(CheckAtom(kind, source, column=col,
                                       lo=val if kind is CheckKind.GE_CONST else None,
                                       hi=val if kind is CheckKind.LE_CONST else None))
            else:
                raise SubsetViolation(
                    f"{where} 比较左侧必须是列: {node.sql(dialect='sqlite')!r}",
                    f"line {source.line}", source.text,
                )
        else:
            raise SubsetViolation(
                f"{where} 含子集外 CHECK 原子(仅支持 BETWEEN/IN/<=/>=/列间<=，"
                f"禁止 OR/函数/算术/其他比较): {node.sql(dialect='sqlite')!r}",
                f"line {source.line}", source.text,
            )
    return atoms


# ---------------------------------------------------------- 单表解析

def _parse_table(stmt: exp.Create, raw_stmt: str, base_line: int) -> TableModel:
    if not isinstance(stmt.this, exp.Schema) or not isinstance(stmt.this.this, exp.Table):
        raise SubsetViolation("仅支持普通 CREATE TABLE 表定义")
    table = stmt.this.this
    if table.args.get("catalog") or table.args.get("db"):
        raise SubsetViolation("不支持 schema 前缀的表名")
    tname = table.name
    if not (isinstance(table.this, exp.Identifier) and not table.this.quoted and _IDENT_RE.match(tname)):
        raise SubsetViolation(f"表名 {tname!r} 不是未加引号的 ASCII 标识符")

    # 从原文切分表体，拿到逐约束原文字节
    open_idx = raw_stmt.find("(")
    body_start_abs = open_idx + 1
    body = raw_stmt[body_start_abs:raw_stmt.rfind(")")]
    raw_parts = _split_top_level(body)
    entries = stmt.this.expressions
    if len(entries) != len(raw_parts):
        # 理论上不应发生，出现即说明解析与原文切分不一致
        raise SchemaParseError(f"表 {tname} 的 AST 条目数与原文切分数不一致")

    # 以整条 CREATE 语句的行起点表，把片段绝对偏移换算成文件内 1 基行号
    stmt_line_starts = [0]
    for i, ch in enumerate(raw_stmt):
        if ch == "\n":
            stmt_line_starts.append(i + 1)

    def abs_line(part_rel_offset: int) -> int:
        from bisect import bisect_left
        abs_off = body_start_abs + part_rel_offset
        return base_line + bisect_left(stmt_line_starts, abs_off)

    columns: dict[str, ColumnModel] = {}
    col_order: list[str] = []
    table_pk: tuple[str, ...] | None = None
    unique_keys: list[tuple[str, ...]] = []
    fks: list[ForeignKey] = []
    table_checks: list[CheckAtom] = []
    fk_seq = 0

    for entry, (rel_off, seg_text) in zip(entries, raw_parts):
        line = abs_line(rel_off)
        src = SourceRef(line=line, text=seg_text)

        if isinstance(entry, exp.ColumnDef):
            cname = entry.name
            if not (isinstance(entry.this, exp.Identifier) and not entry.this.quoted
                    and _IDENT_RE.match(cname)):
                raise SubsetViolation(f"表 {tname} 列名 {cname!r} 不是未加引号 ASCII 标识符",
                                      f"line {line}", seg_text)
            if cname in columns:
                raise SubsetViolation(f"表 {tname} 列 {cname} 重复定义", f"line {line}", seg_text)
            if entry.kind is None or entry.kind.sql(dialect="sqlite") != "INTEGER":
                raise SubsetViolation(f"表 {tname}.{cname} 类型必须恰为 INTEGER",
                                      f"line {line}", seg_text)
            not_null = False
            is_pk = False
            col_checks: list[CheckAtom] = []
            for con in entry.constraints:
                kind = con.kind
                if isinstance(kind, exp.NotNullColumnConstraint):
                    not_null = True
                elif isinstance(kind, exp.PrimaryKeyColumnConstraint):
                    is_pk = True
                elif isinstance(kind, exp.CheckColumnConstraint):
                    chk_src = SourceRef(line, _extract_check_text(seg_text))
                    col_checks.extend(_parse_check_tree(
                        kind.this, chk_src, f"表 {tname}.{cname} 的 CHECK"))
                else:
                    raise SubsetViolation(
                        f"表 {tname}.{cname} 含子集外列约束 "
                        f"{con.sql(dialect='sqlite')!r}（不支持 DEFAULT/内联REFERENCES/"
                        f"UNIQUE/AUTOINCREMENT/生成列等）",
                        f"line {line}", seg_text)
            columns[cname] = ColumnModel(cname, not_null, is_pk, tuple(col_checks))
            col_order.append(cname)

        elif isinstance(entry, exp.PrimaryKey):
            if table_pk is not None:
                raise SubsetViolation(f"表 {tname} 存在多个 PRIMARY KEY", f"line {line}", seg_text)
            cols = tuple(_column_name(e, f"表 {tname} PRIMARY KEY") for e in entry.expressions)
            if not 1 <= len(cols) <= 2:
                raise SubsetViolation(f"表 {tname} PRIMARY KEY 仅支持 1-2 列", f"line {line}", seg_text)
            table_pk = cols

        elif isinstance(entry, exp.UniqueColumnConstraint):
            schema_node = entry.args.get("this")
            if not isinstance(schema_node, exp.Schema):
                raise SubsetViolation(f"表 {tname} UNIQUE 结构非法", f"line {line}", seg_text)
            cols = tuple(_column_name(e, f"表 {tname} UNIQUE")
                         for e in schema_node.expressions)
            if not 1 <= len(cols) <= 2:
                raise SubsetViolation(f"表 {tname} UNIQUE 仅支持 1-2 列", f"line {line}", seg_text)
            unique_keys.append(cols)

        elif isinstance(entry, exp.ForeignKey):
            fk_seq += 1
            ref = entry.args.get("reference")
            if not isinstance(ref, exp.Reference):
                raise SubsetViolation(f"表 {tname} 外键缺少 REFERENCES", f"line {line}", seg_text)
            child_cols = tuple(_column_name(e, f"表 {tname} 外键") for e in entry.expressions)
            if not 1 <= len(child_cols) <= 2:
                raise SubsetViolation(f"表 {tname} 外键仅支持 1-2 列", f"line {line}", seg_text)
            parent_schema = ref.this
            if not isinstance(parent_schema, exp.Schema):
                raise SubsetViolation(f"表 {tname} 外键 REFERENCES 结构非法", f"line {line}", seg_text)
            pname = parent_schema.this.name
            if not _IDENT_RE.match(pname):
                raise SubsetViolation(f"表 {tname} 外键父表名非法", f"line {line}", seg_text)
            parent_cols = tuple(_column_name(e, f"表 {tname} 外键父列")
                                for e in parent_schema.expressions)
            if len(parent_cols) != len(child_cols):
                raise SubsetViolation(f"表 {tname} 外键父子列数不一致", f"line {line}", seg_text)
            # 仅允许 DEFERRABLE INITIALLY DEFERRED + 默认 MATCH SIMPLE/NO ACTION
            opt_sql = " ".join(str(o).upper() for o in (ref.args.get("options") or []))
            if opt_sql != "DEFERRABLE INITIALLY DEFERRED":
                raise SubsetViolation(
                    f"表 {tname} 外键必须声明为 DEFERRABLE INITIALLY DEFERRED，"
                    f"且不允许 MATCH/ON DELETE/ON UPDATE 自定义动作，实际: {opt_sql!r}",
                    f"line {line}", seg_text)
            fks.append(ForeignKey(
                name=f"{tname}__fk_{fk_seq}",
                columns=child_cols, ref_table=pname, ref_columns=parent_cols, source=src,
            ))

        elif isinstance(entry, exp.CheckColumnConstraint):
            # 表级 CHECK 在 sqlglot 中同样表示为 CheckColumnConstraint
            chk_src = SourceRef(line, _extract_check_text(seg_text))
            table_checks.extend(_parse_check_tree(entry.this, chk_src,
                                                  f"表 {tname} 表级 CHECK"))

        else:
            raise SubsetViolation(
                f"表 {tname} 含子集外表级条目 {entry.sql(dialect='sqlite')!r}",
                f"line {line}", seg_text)

    tmodel = TableModel(
        name=tname,
        source_line=base_line,
        columns=tuple(columns[c] for c in col_order),
        table_primary_key=table_pk,
        unique_keys=tuple(unique_keys),
        foreign_keys=tuple(fks),
        checks=tuple(table_checks),
    )
    _cross_validate(tmodel, src)
    return tmodel


def _cross_validate(t: TableModel, _src: SourceRef) -> None:
    """结构性交叉校验：约束引用的列必须存在；单列行内 PK 至多一个。"""
    known = set(t.column_names)
    inline_pk = [c.name for c in t.columns if c.primary_key]
    if len(inline_pk) > 1:
        raise SubsetViolation(f"表 {t.name} 存在多个单列 PRIMARY KEY")
    if t.table_primary_key and inline_pk:
        raise SubsetViolation(f"表 {t.name} 同时声明行内与表级 PRIMARY KEY")
    for cols in ([t.table_primary_key] if t.table_primary_key else []) + \
                [list(k) for k in t.unique_keys]:
        for c in cols:
            if c not in known:
                raise SubsetViolation(f"表 {t.name} 约束引用了不存在的列 {c}")
    for fk in t.foreign_keys:
        for c in fk.columns:
            if c not in known:
                raise SubsetViolation(f"表 {t.name} 外键引用了不存在的列 {c}",
                                      f"line {fk.source.line}", fk.source.text)
    for atom in t.all_checks():
        for c in (atom.column, atom.left_col, atom.right_col):
            if c is not None and c not in known:
                raise SubsetViolation(f"表 {t.name} CHECK 引用了不存在的列 {c}",
                                      f"line {atom.source.line}", atom.source.text)


# ---------------------------------------------------------- 入口

def _split_statements(raw_sql: str) -> list[tuple[int, str]]:
    """按顶层分号切分 SQL，返回 (1 基行号, 原文字节)。不剥离空白以外的任何字符。"""
    from bisect import bisect_left

    # 先完整收集每行起点（字符偏移），保证行号计算与扫描顺序无关
    line_starts = [0]
    for i, ch in enumerate(raw_sql):
        if ch == "\n":
            line_starts.append(i + 1)

    def line_of(offset: int) -> int:
        # 偏移落在某行起点上时，bisect_left 的插入位即该行的 1 基行号
        return bisect_left(line_starts, offset) + 1

    out: list[tuple[int, str]] = []
    depth = 0
    start = 0
    for i, ch in enumerate(raw_sql):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == ";" and depth == 0:
            seg = raw_sql[start:i].strip()
            if seg:
                out.append((line_of(start), seg))
            start = i + 1
    tail = raw_sql[start:].strip()
    if tail:
        out.append((line_of(start), tail))
    return out


def parse_schema(raw_sql: str) -> SchemaModel:
    try:
        statements = sqlglot.parse(raw_sql, read="sqlite", error_level=ErrorLevel.RAISE)
    except sqlglot.errors.ParseError as ex:
        raise SchemaParseError(f"schema.sql 不是合法 SQLite 语法: {ex}") from ex

    raw_stmts = _split_statements(raw_sql)

    non_empty = [s for s in statements if s is not None]
    if len(non_empty) != len(raw_stmts):
        raise SchemaParseError("SQL 语句数与原文切分数不一致")

    tables: list[TableModel] = []
    seen: set[str] = set()
    for stmt, (line, raw_stmt) in zip(non_empty, raw_stmts):
        if isinstance(stmt, exp.Pragma):
            eq = stmt.this
            pname = eq.this.name if isinstance(eq, exp.EQ) else (
                eq.name if isinstance(eq, exp.Var) else "")
            if pname.lower() not in _ALLOWED_PRAGMAS:
                raise SubsetViolation(f"仅允许 PRAGMA foreign_keys，发现 PRAGMA {pname}",
                                      f"line {line}", raw_stmt)
            if isinstance(eq, exp.EQ):
                pval = eq.expression.name.upper() if isinstance(eq.expression, exp.Var) else ""
                if pval not in ("ON", "1", "TRUE"):
                    raise SubsetViolation(
                        f"PRAGMA foreign_keys 只允许 ON/1/TRUE，实际 {pval!r}",
                        f"line {line}", raw_stmt)
            continue
        if not isinstance(stmt, exp.Create) or str(stmt.args.get("kind")).upper() != "TABLE" \
                or any(isinstance(p, exp.TemporaryProperty)
                       for p in (stmt.args.get("properties") or [])) \
                or stmt.args.get("exists"):
            raise SubsetViolation(
                f"仅支持普通 CREATE TABLE 语句（禁止 TEMP/IF NOT EXISTS/VIEW/INDEX 等）: "
                f"{stmt.sql(dialect='sqlite')!r}",
                f"line {line}", raw_stmt)
        tmodel = _parse_table(stmt, raw_stmt, line)
        if tmodel.name in seen:
            raise SubsetViolation(f"表 {tmodel.name} 重复定义", f"line {line}", raw_stmt)
        seen.add(tmodel.name)
        tables.append(tmodel)

    if not tables:
        raise SchemaParseError("schema 中没有任何 CREATE TABLE")

    # 外键父表/父键必须真实存在且为父表 PK/UNIQUE
    by_name = {t.name: t for t in tables}
    for t in tables:
        for fk in t.foreign_keys:
            if fk.ref_table not in by_name:
                raise SubsetViolation(
                    f"表 {t.name} 外键 {fk.name} 引用不存在的父表 {fk.ref_table}",
                    f"line {fk.source.line}", fk.source.text)
            parent = by_name[fk.ref_table]
            parent_keys = {parent.primary_key()} if parent.primary_key() else set()
            parent_keys.update(parent.unique_keys)
            if fk.ref_columns not in parent_keys:
                raise SubsetViolation(
                    f"表 {t.name} 外键 {fk.name} 的父键 {fk.ref_columns} "
                    f"不是 {fk.ref_table} 的 PRIMARY KEY/UNIQUE",
                    f"line {fk.source.line}", fk.source.text)
    return SchemaModel(tables=tuple(tables), raw_sql=raw_sql)

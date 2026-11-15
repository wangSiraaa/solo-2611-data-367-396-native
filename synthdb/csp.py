"""把 schema 目录 + 行槽声明编译成有限域约束问题（CSP）。

变量 = 待分配单元格（one_of）；固定单元格是常量，参与全部约束但不可改。
约束三类：
  1. 一元域过滤：NOT NULL 已在声明层处理，这里叠加单列 CHECK；
  2. 同行二元：col <= col（含常量参与的静态情形）；
  3. 全局：PK/复合 UNIQUE（按 SQLite NULL 语义）、复合外键（MATCH SIMPLE，
     分量必须命中同一父键元组）、跨行无其他约束。

Canonical 排序保证：输入的表名顺序、行槽顺序、域枚举次序均不影响规范结果——
变量按 (表名, 槽指纹序号, 列名) 排序，域按整数升序、NULL 置尾排序。
"""
from __future__ import annotations

from dataclasses import dataclass

from .catalog import Catalog
from .errors import UnsatError
from .model import CheckAtom, CheckKind, Declaration, ForeignKey, NULL, SchemaModel, Slot


# ------------------------------------------------------------ 变量与约束

@dataclass(frozen=True)
class Variable:
    vid: str                  # 规范标识 table.slot#n.column
    table: str
    slot_fingerprint: str
    canon_slot: int           # 槽按指纹排序后的规范序号
    column: str
    domain: tuple[int | None, ...]
    declared_domain: tuple[int | None, ...]
    orig_slot_index: int

    def as_dict(self) -> dict:
        return {
            "vid": self.vid,
            "table": self.table,
            "canon_slot": self.canon_slot,
            "column": self.column,
            "domain": list(self.domain),
            "declared_domain": list(self.declared_domain),
            "orig_slot_index": self.orig_slot_index,
        }


@dataclass(frozen=True)
class UnaryCheck:
    atom: CheckAtom

    @property
    def vid_selector(self):
        return None


@dataclass(frozen=True)
class SameRowCheck:
    table: str
    canon_slot: int
    atom: CheckAtom
    # 参与的单元格 vid；固定单元格为 None（常量内联到求值器）
    left_vid: str | None
    right_vid: str | None
    left_const: int | None
    right_const: int | None


@dataclass(frozen=True)
class KeyConstraint:
    table: str
    columns: tuple[str, ...]
    is_primary: bool
    strict_not_null: bool          # 单列 INTEGER PRIMARY KEY 隐式 NOT NULL
    source_text: str
    source_line: int | None


@dataclass(frozen=True)
class FKConstraint:
    fk: ForeignKey
    table: str
    # 每个分量在每个槽上的 vid（或 None 表示该槽该列固定）
    child_vids_by_slot: dict[int, tuple[str | None, ...]]
    child_const_by_slot: dict[int, tuple[int | None, ...]]


@dataclass
class CSP:
    schema: SchemaModel
    catalog: Catalog
    declaration: Declaration
    variables: dict[str, Variable]
    var_order: list[str]
    # 常量视图: (table, canon_slot, column) -> 固定值；变量单元格不在此表
    constants: dict[tuple[str, int, str], int | None]
    # 每表每槽: canon_slot -> (orig_slot_index, fingerprint)
    slot_map: dict[str, list[tuple[int, str]]]
    unary_checks: dict[str, list[CheckAtom]]          # vid -> 原子
    fixed_unary: list[tuple[str, int, int | None, CheckAtom]]
    same_row: list[SameRowCheck]
    keys: list[KeyConstraint]
    fks: list[FKConstraint]
    # 求值辅助：每个全局约束涉及的 vid 集合
    key_vids: dict[int, set[str]]
    fk_vids: dict[int, set[str]]

    def all_constraints_descriptive(self) -> list[dict]:
        """plan/冲突报告用的可读约束清单（附原文位置）。"""
        out: list[dict] = []
        for kc in self.keys:
            out.append({"kind": "PK" if kc.is_primary else "UNIQUE",
                        "table": kc.table, "columns": list(kc.columns),
                        "strict_not_null": kc.strict_not_null,
                        "source": {"line": kc.source_line, "text": kc.source_text}})
        for fkc in self.fks:
            fk = fkc.fk
            out.append({"kind": "FOREIGN KEY", "table": fk.columns and fkc.table,
                        "name": fk.name, "columns": list(fk.columns),
                        "ref_table": fk.ref_table, "ref_columns": list(fk.ref_columns),
                        "source": fk.source.as_dict()})
        for sr in self.same_row:
            out.append({"kind": "CHECK_SAME_ROW", **sr.atom.as_dict(),
                        "table": sr.table, "canon_slot": sr.canon_slot})
        for vid, atoms in self.unary_checks.items():
            for a in atoms:
                out.append({"kind": "CHECK_UNARY", "vid": vid, **a.as_dict()})
        for tname, slot_idx, val, a in self.fixed_unary:
            out.append({"kind": "CHECK_FIXED", "table": tname, "orig_slot_index": slot_idx,
                        "column": a.column, "value": val, **a.as_dict()})
        return out


# ------------------------------------------------------------ 工具

def _canon_domain(values: tuple[int | None, ...]) -> tuple[int | None, ...]:
    ints = sorted(v for v in values if v is not NULL)
    return tuple(ints) + ((NULL,) if NULL in values else ())


def _slot_fingerprint(slot: Slot) -> str:
    parts = []
    for col in sorted(slot.cells):
        cell = slot.cells[col]
        parts.append(f"{col}=" + ("F:" + repr(cell.value) if cell.fixed
                                  else "D:" + repr(list(_canon_domain(cell.domain)))))
    return "|".join(parts)


def _eval_unary(atom: CheckAtom, value) -> bool:
    """SQLite 三值逻辑：NULL 使 CHECK 结果为 NULL -> 不拒绝，返回 True。"""
    if value is NULL:
        return True
    if atom.kind is CheckKind.BETWEEN:
        return atom.lo <= value <= atom.hi
    if atom.kind is CheckKind.IN:
        return value in atom.values
    if atom.kind is CheckKind.LE_CONST:
        return value <= atom.hi
    if atom.kind is CheckKind.GE_CONST:
        return value >= atom.lo
    raise AssertionError("LE_COL 不是一元约束")


# ------------------------------------------------------------ 构建

def build_csp(schema: SchemaModel, catalog: Catalog, decl: Declaration) -> CSP:
    variables: dict[str, Variable] = {}
    var_order: list[str] = []
    constants: dict[tuple[str, int, str], int | None] = {}
    slot_map: dict[str, list[tuple[int, str]]] = {}
    unary_checks: dict[str, list[CheckAtom]] = {}
    fixed_unary: list[tuple[str, int, int | None, CheckAtom]] = []
    same_row: list[SameRowCheck] = []

    # 槽按指纹规范化排序，消除输入行槽顺序影响
    canon_index: dict[str, dict[int, int]] = {}
    for t in schema.tables:
        slots = decl.tables[t.name]
        fps = [_slot_fingerprint(s) for s in slots]
        if len(set(fps)) != len(fps):
            # 指纹相同意味着两个槽声明完全一致；仍可区分（用原序号打破平局），
            # 但 PK/UNIQUE 求解会处理；此处用稳定平局键。
            seen: dict[str, int] = {}
            deduped = []
            for i, fp in enumerate(fps):
                n = seen.get(fp, 0)
                seen[fp] = n + 1
                deduped.append((fp, n))
            order = sorted(range(len(slots)),
                           key=lambda i: (deduped[i][0], deduped[i][1], i))
        else:
            order = sorted(range(len(slots)), key=lambda i: (fps[i], i))
        canon_index[t.name] = {orig: canon for canon, orig in enumerate(order)}
        slot_map[t.name] = [(slots[orig].slot_index, fps[orig]) for orig in order]

    for t in schema.tables:
        tname = t.name
        slots = decl.tables[tname]
        unary_atoms = [a for a in t.all_checks() if a.kind is not CheckKind.LE_COL]
        lecol_atoms = [a for a in t.all_checks() if a.kind is CheckKind.LE_COL]

        for orig_idx, slot in enumerate(slots):
            cs = canon_index[tname][orig_idx]
            for col in t.columns:
                cell = slot.cells[col.name]
                key = (tname, cs, col.name)
                col_atoms = [a for a in unary_atoms if a.column == col.name]
                if cell.fixed:
                    constants[key] = cell.value
                    for a in col_atoms:
                        if not _eval_unary(a, cell.value):
                            conflict = {
                                "reason": "固定值违反 CHECK，固定值不得改动",
                                "table": tname, "orig_slot_index": orig_idx,
                                "column": col.name, "fixed_value": cell.value,
                                "constraint": a.as_dict(),
                            }
                            raise UnsatError(
                                f"无解：{tname}.rows[{orig_idx}].{col.name} 固定值 "
                                f"{cell.value!r} 不满足 {a.source.text!r}", conflict)
                        fixed_unary.append((tname, orig_idx, cell.value, a))
                else:
                    dom = _canon_domain(cell.domain)
                    filtered = tuple(v for v in dom if _eval_unary_any(v, col_atoms))
                    if not filtered:
                        conflict = {
                            "reason": "one_of 域被单列 CHECK 全部排除",
                            "table": tname, "orig_slot_index": orig_idx,
                            "column": col.name, "declared_domain": list(cell.domain),
                            "checks": [a.as_dict() for a in col_atoms],
                        }
                        raise UnsatError(
                            f"无解：{tname}.rows[{orig_idx}].{col.name} 的域 "
                            f"{list(cell.domain)} 被 CHECK 全部排除", conflict)
                    vid = f"{tname}.slot#{cs}.{col.name}"
                    variables[vid] = Variable(
                        vid=vid, table=tname, slot_fingerprint=_slot_fingerprint(slot),
                        canon_slot=cs, column=col.name, domain=filtered,
                        declared_domain=_canon_domain(cell.domain),
                        orig_slot_index=orig_idx)
                    var_order.append(vid)
                    unary_checks[vid] = col_atoms

            # 同行 col <= col（固定/变量任意组合）
            for a in lecol_atoms:
                lcell = slot.cells[a.left_col]
                rcell = slot.cells[a.right_col]
                left_vid = None if lcell.fixed else f"{tname}.slot#{cs}.{a.left_col}"
                right_vid = None if rcell.fixed else f"{tname}.slot#{cs}.{a.right_col}"
                same_row.append(SameRowCheck(
                    table=tname, canon_slot=cs, atom=a,
                    left_vid=left_vid, right_vid=right_vid,
                    left_const=lcell.value if lcell.fixed else None,
                    right_const=rcell.value if rcell.fixed else None))

    # 静态评估同行二元约束中两端皆常量的情形
    for sr in same_row:
        if sr.left_vid is None and sr.right_vid is None:
            lv, rv = sr.left_const, sr.right_const
            if lv is not NULL and rv is not NULL and not lv <= rv:
                raise UnsatError(
                    f"无解：{sr.table} 规范槽 {sr.canon_slot} 固定列 {sr.atom.left_col}<="
                    f"{sr.atom.right_col} 不成立（{lv} <= {rv} 为假）",
                    {"reason": "固定值违反同行 col<=col", "constraint": sr.atom.as_dict()})

    # 键约束
    keys: list[KeyConstraint] = []
    for t in schema.tables:
        pk = t.primary_key()
        if pk:
            single_pk = len(pk) == 1 and any(c.primary_key for c in t.columns if c.name == pk[0])
            keys.append(KeyConstraint(
                t.name, pk, True, strict_not_null=single_pk,
                source_text=f"PRIMARY KEY({','.join(pk)})", source_line=t.source_line))
        for uk in t.unique_keys:
            keys.append(KeyConstraint(
                t.name, uk, False, strict_not_null=False,
                source_text=f"UNIQUE({','.join(uk)})", source_line=t.source_line))

    # 外键：记录每槽各分量的 vid/常量
    fks: list[FKConstraint] = []
    for t in schema.tables:
        slots = decl.tables[t.name]
        for fk in t.foreign_keys:
            by_slot_vid: dict[int, tuple[str | None, ...]] = {}
            by_slot_const: dict[int, tuple[int | None, ...]] = {}
            for orig_idx, slot in enumerate(slots):
                cs = canon_index[t.name][orig_idx]
                vids, consts = [], []
                for c in fk.columns:
                    cell = slot.cells[c]
                    if cell.fixed:
                        vids.append(None)
                        consts.append(cell.value)
                    else:
                        vids.append(f"{t.name}.slot#{cs}.{c}")
                        consts.append(None)
                by_slot_vid[cs] = tuple(vids)
                by_slot_const[cs] = tuple(consts)
            fks.append(FKConstraint(fk, t.name, by_slot_vid, by_slot_const))

    csp = CSP(schema=schema, catalog=catalog, declaration=decl,
              variables=variables, var_order=sorted(
                  var_order, key=lambda v: (variables[v].table,
                                            variables[v].canon_slot,
                                            variables[v].column)),
              constants=constants, slot_map=slot_map,
              unary_checks=unary_checks, fixed_unary=fixed_unary,
              same_row=same_row, keys=keys, fks=fks,
              key_vids={}, fk_vids={})
    csp.key_vids = {i: _key_vids(csp, kc) for i, kc in enumerate(keys)}
    csp.fk_vids = {i: _fk_vids(csp, fkc) for i, fkc in enumerate(fks)}
    _static_full_check(csp)
    return csp


def _static_full_check(csp: "CSP") -> None:
    """空赋值下对全常量可定的约束立即求值，使全固定无解在构建期定位。

    延迟导入避免与 solver 形成模块级环。
    """
    from .solver import eval_fk, eval_key, eval_same_row

    def fixed_slot_values(table: str, columns) -> list[dict]:
        rows = []
        slots = csp.declaration.tables[table]
        for cs, (orig_idx, _fp) in enumerate(csp.slot_map[table]):
            slot = slots[orig_idx]
            rows.append({
                "orig_slot_index": orig_idx,
                "canon_slot": cs,
                "values": {c: slot.cells[c].value for c in columns},
                "domain": {c: [slot.cells[c].value] for c in columns},
            })
        return rows

    for kc in csp.keys:
        verdict = eval_key(csp, kc, {})
        if verdict is False:
            raise UnsatError(
                f"无解：表 {kc.table} 的 {'PRIMARY KEY' if kc.is_primary else 'UNIQUE'} "
                f"{kc.columns} 在固定行槽间已出现重复（含 SQLite NULL 语义判定）",
                {"reason": "固定行槽违反键唯一性", "table": kc.table,
                 "columns": list(kc.columns), "is_primary": kc.is_primary,
                 "source_line": kc.source_line, "source_text": kc.source_text,
                 "slots": fixed_slot_values(kc.table, kc.columns),
                 "sqlite_null_semantics": "含 NULL 的键互不冲突；本冲突全部为非 NULL 重复元组"})
    for fkc in csp.fks:
        verdict = eval_fk(csp, fkc, {})
        if verdict is False:
            raise UnsatError(
                f"无解：外键 {fkc.fk.name} 的固定子元组在父表固定键中无同一元组可命中",
                {"reason": "固定行槽违反复合外键", "fk": fkc.fk.name,
                 "source": fkc.fk.source.as_dict(),
                 "child_slots": fixed_slot_values(fkc.table, fkc.fk.columns),
                 "parent_slots": fixed_slot_values(fkc.fk.ref_table, fkc.fk.ref_columns)})
    for sr in csp.same_row:
        verdict = eval_same_row(csp, sr, {})
        if verdict is False:
            raise UnsatError(
                f"无解：{sr.table} 规范槽 {sr.canon_slot} 固定列间 "
                f"{sr.atom.source.text} 不成立",
                {"reason": "固定行槽违反同行 CHECK", "constraint": sr.atom.as_dict()})


def _eval_unary_any(value, atoms: list[CheckAtom]) -> bool:
    return all(_eval_unary(a, value) for a in atoms)


def _cell_value(csp: CSP, assignment: dict[str, int | None],
                table: str, canon_slot: int, column: str):
    for vid, v in csp.variables.items():
        if v.table == table and v.canon_slot == canon_slot and v.column == column:
            return assignment.get(vid)
    return csp.constants.get((table, canon_slot, column))


def _key_vids(csp: CSP, kc: KeyConstraint) -> set[str]:
    out = set()
    for vid, v in csp.variables.items():
        if v.table == kc.table and v.column in kc.columns:
            out.add(vid)
    return out


def _fk_vids(csp: CSP, fkc: FKConstraint) -> set[str]:
    out = set()
    for tup in fkc.child_vids_by_slot.values():
        out.update(v for v in tup if v is not None)
    # 父键变量也参与（父键值由父表槽决定）
    fk = fkc.fk
    for vid, v in csp.variables.items():
        if v.table == fk.ref_table and v.column in fk.ref_columns:
            out.add(vid)
    return out

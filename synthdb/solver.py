"""有限域求解器与独立枚举核对。

两条相互独立的判定路径：
- solve(): 前向检查 + MRV 回溯，产生确定性的规范赋值（域顺序固定，
  seed 只用于报告，不影响规范解的逻辑内容）；
- enumerate_satisfiable(): 对全部变量域做朴素笛卡尔积递归，独立判定
  子问题是否存在满足赋值，用于无解时核对冲突子问题。

部分赋值下三个全局求值器返回三态：True=满足、False=已违反、None=信息不足
（存在未赋值变量），避免把未赋值变量误当常量。

约束求值严格对齐 SQLite：
- UNIQUE/PK：NULL 互不相同；单列 INTEGER PRIMARY KEY 隐式 NOT NULL；
- 外键 MATCH SIMPLE：任一子列为 NULL 即豁免，否则整个元组必须命中
  同一个父键行的对应元组（复合外键不允许分量各自匹配不同父行）；
- CHECK：NULL 结果不拒绝；NOT NULL 已在声明/目录层处理。
"""
from __future__ import annotations

from dataclasses import dataclass

from .csp import CSP, FKConstraint, KeyConstraint, SameRowCheck
from .errors import UnsatError
from .model import NULL


# ------------------------------------------------------------ 取值（三态感知）

def _value(csp: CSP, assignment: dict, table: str, canon_slot: int, column: str):
    """返回 (状态, 值)；状态 missing 表示该单元格是尚未赋值的变量。"""
    for vid, v in csp.variables.items():
        if v.table == table and v.canon_slot == canon_slot and v.column == column:
            if vid in assignment:
                return "val", assignment[vid]
            return "missing", None
    return "val", csp.constants[(table, canon_slot, column)]


# ------------------------------------------------------------ 约束求值（三态）

def eval_same_row(csp: CSP, sr: SameRowCheck, assignment: dict) -> bool | None:
    if sr.left_vid:
        if sr.left_vid not in assignment:
            return None
        lv = assignment[sr.left_vid]
    else:
        lv = sr.left_const
    if sr.right_vid:
        if sr.right_vid not in assignment:
            return None
        rv = assignment[sr.right_vid]
    else:
        rv = sr.right_const
    if lv is NULL or rv is NULL:
        return True                       # CHECK NULL -> 不拒绝
    return lv <= rv


def eval_key(csp: CSP, kc: KeyConstraint, assignment: dict) -> bool | None:
    n_slots = len(csp.slot_map[kc.table])
    seen: set[tuple] = set()
    incomplete = False
    for cs in range(n_slots):
        tup = []
        for c in kc.columns:
            state, val = _value(csp, assignment, kc.table, cs, c)
            if state == "missing":
                incomplete = True
            tup.append(val)
        tup = tuple(tup)
        if incomplete:
            continue
        if kc.strict_not_null and any(v is NULL for v in tup):
            return False                   # 单列 INTEGER PK 隐式 NOT NULL
        if any(v is NULL for v in tup):
            continue                      # SQLite: 含 NULL 的键互不冲突
        if tup in seen:
            return False
        seen.add(tup)
    return None if incomplete else True


def eval_fk(csp: CSP, fkc: FKConstraint, assignment: dict) -> bool | None:
    """复合外键三态求值。

    - 子元组任一分量 NULL：MATCH SIMPLE 豁免；
    - 子元组完整且命中任一完整父键元组：满足；
    - 父表存在尚未赋全的槽（可能产生任意元组）：信息不足，返回 None；
    - 全部父槽完整仍未命中：明确违反 False。
    分量必须命中同一个父行的整元组，不允许跨父行拼凑。
    """
    fk = fkc.fk
    n_parent = len(csp.slot_map[fk.ref_table])
    parent_tuples: set[tuple] = set()
    parent_incomplete = False
    for cs in range(n_parent):
        tup, miss = [], False
        for c in fk.ref_columns:
            state, val = _value(csp, assignment, fk.ref_table, cs, c)
            if state == "missing":
                miss = True
            tup.append(val)
        if miss:
            parent_incomplete = True
        else:
            parent_tuples.add(tuple(tup))

    child_incomplete = False
    for cs, vids in fkc.child_vids_by_slot.items():
        child = []
        for i, vid in enumerate(vids):
            if vid is None:
                child.append(fkc.child_const_by_slot[cs][i])
            elif vid in assignment:
                child.append(assignment[vid])
            else:
                child_incomplete = True
                child.append(None)
        child = tuple(child)
        if child_incomplete:
            continue
        if any(v is NULL for v in child):
            continue                      # MATCH SIMPLE：部分 NULL 豁免
        if child in parent_tuples:
            continue                      # 命中同一父键元组
        if parent_incomplete:
            return None                   # 父槽未赋全，未来可能出现该元组
        return False                      # 父表已完整，确定孤儿
    return None if (child_incomplete or parent_incomplete) else True


# ------------------------------------------------------------ 前向检查

def _involved(csp: CSP, vid: str):
    srs = [sr for sr in csp.same_row
           if sr.left_vid == vid or sr.right_vid == vid]
    kidx = [i for i, vs in csp.key_vids.items() if vid in vs]
    fkidx = [i for i, vs in csp.fk_vids.items() if vid in vs]
    return srs, kidx, fkidx


def _violated(csp: CSP, assignment: dict, vid: str) -> bool:
    srs, kidx, fkidx = _involved(csp, vid)
    for sr in srs:
        if eval_same_row(csp, sr, assignment) is False:
            return True
    for i in kidx:
        if eval_key(csp, csp.keys[i], assignment) is False:
            return True
    for i in fkidx:
        if eval_fk(csp, csp.fks[i], assignment) is False:
            return True
    return False


def _forward_check(csp: CSP, assignment: dict, domains: dict[str, tuple]) -> dict[str, tuple] | None:
    """传播当前赋值，返回裁剪后的域副本；出现空域返回 None。"""
    domains = {v: tuple(d) for v, d in domains.items()}
    changed = True
    while changed:
        changed = False
        for vid in list(domains):
            kept = []
            for val in domains[vid]:
                trial = {**assignment, vid: val}
                if not _violated(csp, trial, vid):
                    kept.append(val)
            if not kept:
                return None
            if len(kept) != len(domains[vid]):
                domains[vid] = tuple(kept)
                changed = True
    return domains


# ------------------------------------------------------------ 回溯求解

@dataclass
class SolveResult:
    assignment: dict[str, int | None]
    search_nodes: int
    backtracks: int


def solve(csp: CSP) -> SolveResult:
    domains = {v: csp.variables[v].domain for v in csp.var_order}
    stats = {"nodes": 0, "backtracks": 0}
    deepest = {"depth": -1, "vid": None, "tried": None}

    def dfs(assignment: dict, dom: dict[str, tuple]) -> dict | None:
        unassigned = [v for v in csp.var_order if v not in assignment]
        if not unassigned:
            # 完整赋值下所有约束必须为 True
            if all(eval_same_row(csp, sr, assignment) is not False for sr in csp.same_row) \
                    and all(eval_key(csp, kc, assignment) is True for kc in csp.keys) \
                    and all(eval_fk(csp, f, assignment) is True for f in csp.fks):
                return dict(assignment)
            return None
        # MRV，规范平局键保证确定性
        vid = min(unassigned, key=lambda v: (len(dom[v]),
                                             csp.variables[v].table,
                                             csp.variables[v].canon_slot,
                                             csp.variables[v].column))
        stats["nodes"] += 1
        depth = len(assignment)
        if depth > deepest["depth"]:
            deepest.update(depth=depth, vid=vid, tried=list(dom[vid]))
        for val in dom[vid]:
            trial = {**assignment, vid: val}
            if _violated(csp, trial, vid):
                continue
            new_dom = _forward_check(csp, trial, dom)
            if new_dom is None:
                continue
            found = dfs(trial, new_dom)
            if found is not None:
                return found
        stats["backtracks"] += 1
        return None

    result = dfs({}, domains)
    if result is None:
        raise UnsatError(
            "回溯求解未找到满足赋值（最深失败点 "
            f"变量={deepest['vid']} 已试值域={deepest['tried']}）；"
            "最终无解结论由独立笛卡尔积枚举确认，而非尝试次数耗尽。",
            conflict={"deepest_dead_end": deepest})
    return SolveResult(result, stats["nodes"], stats["backtracks"])


# ------------------------------------------------------------ 独立枚举器

def enumerate_satisfiable(csp: CSP, restrict_vids: set[str] | None = None,
                          limit: int | None = None) -> list[dict]:
    """独立枚举：变量域笛卡尔积，逐个检查全部约束（代码路径与 DFS 完全独立）。

    完整问题枚举时所有变量都会赋值，求值器恒返回 True/False。
    restrict_vids 用于冲突子问题：未枚举变量保持无赋值，相关约束以 None
    表示未定，只有明确 False 才排除，保证子问题枚举是保守且诚实的。
    """
    vids = csp.var_order if restrict_vids is None else [
        v for v in csp.var_order if v in restrict_vids]
    solutions: list[dict] = []

    def rec(i: int, assignment: dict) -> None:
        if limit is not None and len(solutions) >= limit:
            return
        if i == len(vids):
            if _full_check(csp, assignment):
                solutions.append(dict(assignment))
            return
        vid = vids[i]
        for val in csp.variables[vid].domain:
            assignment[vid] = val
            rec(i + 1, assignment)
            if limit is not None and len(solutions) >= limit:
                assignment.pop(vid, None)
                return
        assignment.pop(vid, None)

    rec(0, {})
    return solutions


def _full_check(csp: CSP, assignment: dict) -> bool:
    for sr in csp.same_row:
        if eval_same_row(csp, sr, assignment) is False:
            return False
    for kc in csp.keys:
        if eval_key(csp, kc, assignment) is False:
            return False
    for fkc in csp.fks:
        if eval_fk(csp, fkc, assignment) is False:
            return False
    for vid, val in assignment.items():
        from .csp import _eval_unary_any
        if not _eval_unary_any(val, csp.unary_checks.get(vid, [])):
            return False
    return True


# ------------------------------------------------------------ 冲突子问题

def build_unsat_conflict(csp: CSP) -> dict:
    """构造可独立核对的冲突子问题并以独立枚举验证其确实无解。

    删除过滤：从全部约束出发逐个尝试移除，移除后独立枚举仍无解则永久移除，
    得到相对该过滤顺序不可约的约束子集；再对相关变量做笛卡尔积枚举确认零解。
    不声称最小核；固定键/被引用行槽不会被删除过滤移除（它们是常量不是约束）。
    """
    full_solutions = enumerate_satisfiable(csp, limit=1)
    if full_solutions:
        raise AssertionError("求解器报无解但独立枚举找到解，属于求解器缺陷")

    kept = _irreducible_subset(csp)
    related_vids = _vids_of_descriptive(csp, kept)
    sub_solutions = _enumerate_subproblem(csp, kept, related_vids, limit=1)
    if sub_solutions:
        raise AssertionError("冲突子问题竟然有解，冲突构造缺陷")

    return {
        "reason": "独立有限笛卡尔积枚举确认：相关变量不存在任何满足赋值",
        "slots": _slot_domains(csp, related_vids),
        "constraints": kept,
        "enumeration": {
            "variables": sorted(related_vids),
            "space_size": _space_size(csp, related_vids),
            "satisfying_assignments": 0,
            "method": "cartesian product over canonical domains, full constraint check",
        },
    }


def _irreducible_subset(csp: CSP) -> list[dict]:
    all_desc = csp.all_constraints_descriptive()
    kept = list(range(len(all_desc)))
    for idx in range(len(all_desc)):
        trial = [all_desc[j] for j in kept if j != idx]
        vids = _vids_of_descriptive(csp, trial)
        if not _enumerate_subproblem(csp, trial, vids, limit=1):
            kept = [j for j in kept if j != idx]
    return [all_desc[j] for j in kept]


def _vids_of_descriptive(csp: CSP, desc: list[dict]) -> set[str]:
    vids: set[str] = set()
    for d in desc:
        kind = d["kind"]
        if kind in ("PK", "UNIQUE"):
            for v in csp.var_order:
                var = csp.variables[v]
                if var.table == d["table"] and var.column in d["columns"]:
                    vids.add(v)
        elif kind == "FOREIGN KEY":
            for i, fkc in enumerate(csp.fks):
                if fkc.fk.name == d["name"]:
                    vids |= csp.fk_vids[i]
        elif kind == "CHECK_SAME_ROW":
            for sr in csp.same_row:
                if sr.atom.source.text == d["source"]["text"] and sr.table == d["table"] \
                        and sr.canon_slot == d["canon_slot"]:
                    vids.update(v for v in (sr.left_vid, sr.right_vid) if v)
        elif kind == "CHECK_UNARY":
            vids.add(d["vid"])
        # CHECK_FIXED 不涉及变量
    return vids


def _enumerate_subproblem(csp: CSP, desc: list[dict], vids: set[str], limit: int):
    ordered = [v for v in csp.var_order if v in vids]
    solutions = []

    def rec(i: int, assignment: dict) -> None:
        if limit is not None and len(solutions) >= limit:
            return
        if i == len(ordered):
            if _subproblem_check(csp, desc, assignment):
                solutions.append(dict(assignment))
            return
        vid = ordered[i]
        for val in csp.variables[vid].domain:
            assignment[vid] = val
            rec(i + 1, assignment)
            if limit is not None and len(solutions) >= limit:
                assignment.pop(vid, None)
                return
        assignment.pop(vid, None)

    rec(0, {})
    return solutions


def _subproblem_check(csp: CSP, desc: list[dict], assignment: dict) -> bool:
    for d in desc:
        kind = d["kind"]
        if kind in ("PK", "UNIQUE"):
            kc = next(k for k in csp.keys
                      if k.table == d["table"] and k.columns == tuple(d["columns"])
                      and k.is_primary == (kind == "PK"))
            if eval_key(csp, kc, assignment) is False:
                return False
        elif kind == "FOREIGN KEY":
            fkc = next(f for f in csp.fks if f.fk.name == d["name"])
            if eval_fk(csp, fkc, assignment) is False:
                return False
        elif kind == "CHECK_SAME_ROW":
            sr = next(s for s in csp.same_row
                      if s.atom.source.text == d["source"]["text"] and s.table == d["table"]
                      and s.canon_slot == d["canon_slot"])
            if eval_same_row(csp, sr, assignment) is False:
                return False
        elif kind == "CHECK_UNARY":
            vid = d["vid"]
            if vid in assignment:
                from .csp import _eval_unary_any
                if not _eval_unary_any(assignment[vid], csp.unary_checks[vid]):
                    return False
        elif kind == "CHECK_FIXED":
            continue                      # build_csp 阶段已静态保证
    return True


def _slot_domains(csp: CSP, vids: set[str]) -> list[dict]:
    """变量槽 + 参与约束的固定槽：全部给出域（固定即单值域）与原始槽号。"""
    out = []
    touched_tables: dict[str, set[str]] = {}
    for vid in vids:
        v = csp.variables[vid]
        touched_tables.setdefault(v.table, set()).add(v.column)
        out.append({
            "vid": vid, "table": v.table, "orig_slot_index": v.orig_slot_index,
            "canon_slot": v.canon_slot, "column": v.column,
            "declared_domain": list(v.declared_domain),
            "canonical_domain": list(v.domain),
            "fixed": False,
        })
    # 补入被约束引用的固定单元格，保证核对者无需回看其他文件即可重放
    for tname, cols in touched_tables.items():
        slots = csp.declaration.tables[tname]
        for cs, (orig_idx, _fp) in enumerate(csp.slot_map[tname]):
            slot = slots[orig_idx]
            for col_name in cols:
                cell = slot.cells[col_name]
                if cell.fixed:
                    out.append({
                        "vid": f"{tname}.slot#{cs}.{col_name}",
                        "table": tname, "orig_slot_index": orig_idx,
                        "canon_slot": cs, "column": col_name,
                        "declared_domain": [cell.value],
                        "canonical_domain": [cell.value],
                        "fixed": True,
                    })
    return sorted(out, key=lambda d: (d["table"], d["canon_slot"], d["column"]))


def _space_size(csp: CSP, vids: set[str]) -> int:
    n = 1
    for vid in vids:
        n *= len(csp.variables[vid].domain)
    return n

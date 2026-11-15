"""plan 阶段：把 catalog + policy 组合成可执行发布计划并执行全部静态校验。

规则编号（错误信息只含表/列/域/规则 ID，不含原值）：
  R001 策略引用了不存在的表/列
  R002 源表存在没有任何策略的列（未登记列策略）
  R003 敏感清单引用不存在的表/列
  R004 声明的必需表在源库缺失
  R005 列策略动作非法
  R006 引用未声明的域
  R007 外键两端动作或域不一致
  R008 域 alphabet/width 非法
  R009 域前缀非法
  R010 域前缀互相冲突
  R011 敏感列被原样保留
  R012 drop 将破坏 PK/UNIQUE/FK/NOT NULL 声明约束
  R013 必需表清单配置非法
  R014 列策略与敏感/公开清单登记不一致
  R015 当批去重后域取值数超过代号容量
"""

from dataclasses import dataclass, field

from .policy import ACTION_DROP, ACTION_KEEP


# policy 明确固定的三张业务表；合法变体允许额外用户表。
REQUIRED_TABLES = ("customers", "accounts", "invoices")


@dataclass
class ColumnPlan:
    table: str
    name: str
    action: str
    domain: str | None
    decl_type: str | None
    notnull: bool
    default: str | None


@dataclass
class TablePlan:
    table: str
    columns: list[ColumnPlan]
    pk_columns: list[str]
    fk_clauses: list[str]
    unique_indexes: list  # catalog.UniqueIndex
    fk_dependencies: list[str]

    @property
    def surviving_columns(self):
        return [c for c in self.columns if c.action != ACTION_DROP]

    def output_ddl(self):
        defs = []
        for cp in self.surviving_columns:
            parts = [f'"{cp.name}"', cp.decl_type or "TEXT"]
            if cp.notnull:
                parts.append("NOT NULL")
            if cp.default is not None:
                parts.append(f"DEFAULT {cp.default}")
            defs.append(" ".join(parts))
        if self.pk_columns:
            defs.append(
                "PRIMARY KEY (" + ",".join(f'"{c}"' for c in self.pk_columns) + ")"
            )
        defs.extend(self.fk_clauses)
        return (
            f'CREATE TABLE "{self.table}" (\n  ' + ",\n  ".join(defs) + "\n)"
        )


@dataclass
class Plan:
    policy_id: str
    table_plans: dict[str, TablePlan]
    creation_order: list[str]
    domain_specs: dict
    distinct_value_counts: dict[str, int] = field(default_factory=dict)

    def tokenize_columns(self):
        for tname in self.creation_order:
            for cp in self.table_plans[tname].columns:
                if cp.domain is not None:
                    yield cp

    def capacity_report(self):
        return {
            d: {
                "capacity": spec.capacity,
                "needed": self.distinct_value_counts.get(d, 0),
                "token_length": spec.token_length,
                "prefix": spec.prefix,
            }
            for d, spec in sorted(self.domain_specs.items())
        }


def _resolve_fk_target(catalog, ref_table, to_col):
    parent = catalog.get_table(ref_table)
    if parent is None:
        return None
    if to_col is None:
        pks = parent.pk_columns
        return pks[0] if len(pks) == 1 else None
    return to_col


def build_plan(catalog, policy, conn, required_tables=REQUIRED_TABLES):
    violations = []

    # R013：必需表清单本身合法。
    if not isinstance(required_tables, (tuple, list)) or not required_tables:
        violations.append("R013: 必需表声明非法")

    # R004：必需表存在。
    for t in required_tables or ():
        if catalog.get_table(t) is None:
            violations.append(f"R004: table={t!r} 在源库缺失")

    # R001 / R003：策略与敏感清单引用的表.列必须真实存在。
    for fq in policy.columns:
        if not catalog.has_column(fq):
            violations.append(f"R001: column={fq!r} 在源库不存在")
    for fq in policy.sensitive_columns | policy.public_keep_columns:
        if not catalog.has_column(fq):
            violations.append(f"R003: column={fq!r} 在源库不存在")

    # R002：源库每一列都必须登记列策略。
    for tname in catalog.table_names():
        for col in catalog.tables[tname].columns:
            fq = f"{tname}.{col.name}"
            if fq not in policy.columns:
                violations.append(f"R002: column={fq!r} 未登记列策略")

    # R014：策略中引用了但既不在敏感清单也不在公开清单的列，同样拒绝。
    for fq in policy.columns:
        if (fq not in policy.sensitive_columns
                and fq not in policy.public_keep_columns):
            violations.append(
                f"R014: column={fq!r} 有列策略但未登记在任何清单中"
            )

    # R007：每个外键两端必须同动作、同域。
    fk_targets = {}  # (table, col) -> (ref_table, ref_col)
    for tname, from_col, ref_table, raw_to in catalog.all_fk_pairs():
        target_col = _resolve_fk_target(catalog, ref_table, raw_to)
        if target_col is None:
            violations.append(
                f"R007: table={tname!r} column={from_col!r} 外键父列无法解析"
            )
            continue
        fk_targets[(tname, from_col)] = (ref_table, target_col)
        child_fq = f"{tname}.{from_col}"
        parent_fq = f"{ref_table}.{target_col}"
        child_rule = policy.rule(child_fq)
        parent_rule = policy.rule(parent_fq)
        if child_rule is None or parent_rule is None:
            violations.append(
                f"R007: 外键 {child_fq!r} -> {parent_fq!r} 两端缺少策略"
            )
            continue
        if child_rule.action != parent_rule.action:
            violations.append(
                f"R007: 外键 {child_fq!r} -> {parent_fq!r} 两端动作不一致"
            )
            continue
        if child_rule.domain != parent_rule.domain:
            violations.append(
                f"R007: 外键 {child_fq!r} -> {parent_fq!r} 两端域不一致"
            )

    # R012：drop 不允许落在任何 PK/UNIQUE/FK 列上。
    for tname in catalog.table_names():
        table = catalog.tables[tname]
        for col in table.columns:
            fq = f"{tname}.{col.name}"
            rule = policy.rule(fq)
            if rule is None:
                continue
            if rule.action == ACTION_DROP and table.participates_in_constraint(
                col.name
            ):
                violations.append(
                    f"R012: column={fq!r} 参与声明约束，禁止 drop"
                )

    if violations:
        from .errors import PlanRejected

        raise PlanRejected(sorted(set(violations)))

    # ---- 结构均合法，构建 TablePlan ----
    table_plans = {}
    for tname in catalog.table_names():
        table = catalog.tables[tname]
        col_plans = []
        pk_columns = []
        for col in table.columns:
            rule = policy.rule(f"{tname}.{col.name}")
            col_plans.append(
                ColumnPlan(
                    table=tname,
                    name=col.name,
                    action=rule.action,
                    domain=rule.domain,
                    decl_type=col.decl_type,
                    notnull=col.notnull,
                    default=col.default,
                )
            )
            if col.pk_pos > 0:
                pk_columns.append(col.name)

        fk_clauses = []
        deps = []
        for fk in table.fks:
            target_col = _resolve_fk_target(
                catalog, fk.ref_table, fk.to_col
            )
            fk_clauses.append(
                f'FOREIGN KEY ("{fk.from_col}") '
                f'REFERENCES "{fk.ref_table}" ("{target_col}")'
            )
            deps.append(fk.ref_table)
        table_plans[tname] = TablePlan(
            table=tname,
            columns=col_plans,
            pk_columns=pk_columns,
            fk_clauses=fk_clauses,
            unique_indexes=table.unique_indexes,
            fk_dependencies=sorted(set(deps)),
        )

    creation_order = _topological_order(table_plans)

    plan = Plan(
        policy_id=policy.policy_id,
        table_plans=table_plans,
        creation_order=creation_order,
        domain_specs=dict(policy.domains),
    )

    # R015：按域统计当批去重取值数（NULL 不占代号，空串是合法取值）。
    counts = count_distinct_values(conn, plan)
    plan.distinct_value_counts = counts
    for domain, spec in policy.domains.items():
        needed = counts.get(domain, 0)
        if needed > spec.capacity:
            violations.append(
                f"R015: domain={domain!r} 需要 {needed} 个代号，"
                f"容量仅 {spec.capacity}"
            )
    if violations:
        from .errors import PlanRejected

        raise PlanRejected(sorted(set(violations)))
    return plan


def _topological_order(table_plans):
    order = []
    visited = set()

    def visit(name, stack):
        if name in visited:
            return
        if name in stack:  # 理论上 SQLite 不允许循环 FK，防御性处理
            raise PlanRejectedCycle(name)
        stack.add(name)
        for dep in table_plans[name].fk_dependencies:
            visit(dep, stack)
        stack.discard(name)
        visited.add(name)
        order.append(name)

    for name in sorted(table_plans):
        visit(name, set())
    return order


class PlanRejectedCycle(Exception):
    def __init__(self, name):
        super().__init__(f"R007: table={name!r} 检测到循环外键")


def count_distinct_values(conn, plan):
    counts = {}
    domain_cols = {}
    for cp in plan.tokenize_columns():
        domain_cols.setdefault(cp.domain, []).append(f'"{cp.table}"."{cp.name}"')
    for domain, refs in domain_cols.items():
        values = set()
        for ref in refs:
            table = ref.split(".")[0].strip('"')
            col = ref.split(".")[1].strip('"')
            for (val,) in conn.execute(
                f'SELECT DISTINCT "{col}" FROM "{table}" '
                f'WHERE "{col}" IS NOT NULL'
            ):
                values.add(val)
        counts[domain] = len(values)
    return counts

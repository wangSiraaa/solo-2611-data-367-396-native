"""Scope / relational-operator graph analyzer.

Intermediate representation
----------------------------
A SELECT is lowered into a small tree of relational operators, each producing
a :class:`RelOutput`:

* table / view / CTE scan          (a binding in the current scope)
* join                             (INNER / LEFT, ON or USING)
* filter                           (WHERE)
* group/aggregate                  (GROUP BY with SUM/COUNT)
* projection                       (the SELECT list, including stars)

Every output column carries three pieces of information computed bottom-up:

* ``value_deps``    - base columns whose changed *value* may change this cell
* ``control_deps``  - base columns that may decide whether the row exists,
                      how rows pair in a join, how rows group, or which value
                      a CASE picks
* ``unknown``       - an opaque ``unknown`` function was reached, so no
                      completeness claim can be made for the column

Scopes are explicit bindings (FROM alias -> RelOutput). CTE names shadow outer
bindings per SQLite scoping rules; star expansion walks the *resolved*
bindings, so ``*`` / ``alias.*`` can never invent fields and unqualified names
are resolved against exactly the visible columns, with ambiguity rejected.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .errors import (
    E_AGGREGATE, E_AMBIGUOUS_COLUMN, E_DUPLICATE_OUTPUT, E_GROUPING,
    E_MISSING_COLUMN, E_RECURSIVE_CTE, E_SYNTAX,
    E_UNKNOWN_FUNCTION, E_UNSUPPORTED, LineageError,
)
from .models import (
    R_CASE_COND, R_COUNT_INPUT, R_DISTINCT, R_FILTER, R_GROUP, R_JOIN,
    R_LEFT_NULL, BaseRef, Dep, Hop, SourceLocation,
    union_deps, VALUE, CONTROL,
)

# Deterministic SQLite scalar functions whose result is a pure function of
# their scalar arguments. Anything not listed must be declared in
# catalog.functions (argument_dependent / unknown) or the view is rejected.
_DEFAULT_DETERMINISTIC_FUNCS = {
    "length", "lower", "upper", "trim", "ltrim", "rtrim", "substr", "substring",
    "coalesce", "ifnull", "nullif", "round", "abs", "replace", "instr",
    "printf", "hex", "quote", "typeof",
}

# --------------------------------------------------------------------------- #
# Locations
# --------------------------------------------------------------------------- #

def _node_span(node: exp.Expression) -> SourceLocation | None:
    """Tightest span covering all positioned identifiers under ``node``."""
    starts, ends, lines, cols = [], [], [], []
    for sub in node.walk():
        meta = getattr(sub, "meta", None)
        if meta and "start" in meta:
            starts.append(meta["start"])
            ends.append(meta["end"])
            lines.append(meta["line"])
            cols.append(meta["col"])
    if not starts:
        return None
    return SourceLocation(
        line=min(lines), col=min(cols), start=min(starts), end=max(ends) + 1
    )


def _ident_location(ident: exp.Identifier | None) -> SourceLocation | None:
    if ident is None:
        return None
    meta = getattr(ident, "meta", None)
    if not meta or "start" not in meta:
        return None
    return SourceLocation(
        line=meta["line"], col=meta["col"], start=meta["start"], end=meta["end"] + 1
    )


def parse_select(sql: str, view_name: str):
    """Parse a view body. Accepts a bare SELECT or CREATE VIEW ... AS SELECT."""
    try:
        statements = sqlglot.parse(sql, read="sqlite")
    except ParseError as exc:
        loc = SourceLocation(exc.line, exc.col, 0, 0) if getattr(exc, "line", None) else None
        raise LineageError(E_SYNTAX, f"SQLite parse error: {exc}", loc, view_name)
    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise LineageError(
            E_SYNTAX, "a view file must contain exactly one SELECT statement", None, view_name
        )
    stmt = statements[0]
    if isinstance(stmt, exp.Create):
        if not isinstance(stmt.this, exp.Select):
            raise LineageError(E_UNSUPPORTED, "only CREATE VIEW ... AS SELECT is supported",
                               _node_span(stmt), view_name)
        stmt = stmt.this
    if not isinstance(stmt, exp.Select):
        raise LineageError(E_UNSUPPORTED, f"unsupported statement: {type(stmt).__name__}",
                           _node_span(stmt), view_name)
    return stmt


# --------------------------------------------------------------------------- #
# Internal relational graph nodes
# --------------------------------------------------------------------------- #

@dataclass
class ExprResult:
    """Dependency result of analyzing a scalar expression."""

    value: list[Dep] = field(default_factory=list)
    control: list[Dep] = field(default_factory=list)
    unknown: bool = False
    unknown_reasons: list[str] = field(default_factory=list)
    nullable_sources: list[str] = field(default_factory=list)

    @staticmethod
    def empty() -> "ExprResult":
        return ExprResult()

    @staticmethod
    def merge(*parts: "ExprResult") -> "ExprResult":
        r = ExprResult()
        for p in parts:
            r.value = union_deps(r.value, p.value)
            r.control = union_deps(r.control, p.control)
            r.unknown = r.unknown or p.unknown
            r.unknown_reasons.extend(p.unknown_reasons)
            r.nullable_sources.extend(p.nullable_sources)
        r.unknown_reasons = sorted(set(r.unknown_reasons))
        r.nullable_sources = sorted(set(r.nullable_sources))
        return r


@dataclass
class ResolvedColumn:
    """One column produced by a relational operator."""

    name: str                       # lowercased logical name
    name_display: str               # original spelling at this scope
    value: list[Dep] = field(default_factory=list)
    control: list[Dep] = field(default_factory=list)
    unknown: bool = False
    unknown_reasons: list[str] = field(default_factory=list)
    nullable_sources: list[str] = field(default_factory=list)
    location: SourceLocation | None = None
    name_location: SourceLocation | None = None

    def expr_result(self) -> ExprResult:
        return ExprResult(
            value=list(self.value), control=list(self.control),
            unknown=self.unknown, unknown_reasons=list(self.unknown_reasons),
            nullable_sources=list(self.nullable_sources),
        )


@dataclass
class Binding:
    """A FROM-clause entry: alias name -> the relation it scans."""

    alias: str                      # lowercased
    alias_display: str
    kind: str                       # table | view | cte | subquery
    object_name: str                # lowercased object name ("" for subquery)
    object_display: str
    rel: "RelOutput"
    role_location: SourceLocation | None = None


@dataclass
class RelOutput:
    columns: list[ResolvedColumn]
    row_control: list[Dep] = field(default_factory=list)
    row_unknown: bool = False
    row_unknown_reasons: list[str] = field(default_factory=list)
    bindings: list[Binding] = field(default_factory=list)
    # For view/CTE scans: hop to prepend when propagating their deps.
    entry_hop: Hop | None = None
    using_merged: set[str] = field(default_factory=set)


# --------------------------------------------------------------------------- #
# Analyzer
# --------------------------------------------------------------------------- #

class Analyzer:
    def __init__(
        self,
        tables: dict[str, dict[str, str]],
        functions: dict[str, dict[str, Any]],
        resolve_view: Callable[[str, SourceLocation | None, str], RelOutput],
        view_name: str,
        sql_file: str,
    ):
        self.tables = tables            # lower table -> {lower col: display col}
        self.functions = functions      # lower fname -> declaration
        self.resolve_view = resolve_view
        self.view_name = view_name
        self.sql_file = sql_file

    # -- error helper ------------------------------------------------------- #
    def _err(self, code: str, message: str, node: exp.Expression | None = None) -> LineageError:
        return LineageError(code, message, _node_span(node) if node is not None else None,
                            self.view_name)

    # ====================================================================== #
    # SELECT entry point
    # ====================================================================== #
    def analyze_select(self, select: exp.Select) -> RelOutput:
        with_node = select.args.get("with_")
        cte_scope: dict[str, tuple[exp.CTE, dict[str, Any]]] = {}
        if with_node is not None:
            if with_node.recursive:
                raise self._err(E_RECURSIVE_CTE,
                                "recursive CTEs are outside the supported SQL subset",
                                with_node)
            for cte in with_node.expressions:
                cte_name = cte.alias
                if not cte_name:
                    raise self._err(E_UNSUPPORTED, "CTE requires an alias", cte)
                if cte_name.lower() in cte_scope:
                    raise self._err(E_UNSUPPORTED, "duplicate CTE name in same WITH clause", cte)
                cte_scope[cte_name.lower()] = (cte, {})

        # Reject top-level clauses outside the contracted subset early.
        self._reject_unsupported_clauses(select)

        rel = self._build_from(select, cte_scope)
        rel = self._apply_where(select, rel)
        rel = self._apply_group(select, rel)
        rel = self._project(select, rel)
        return rel

    def _reject_unsupported_clauses(self, select: exp.Select) -> None:
        if select.args.get("distinct") is not None:
            raise self._err(E_UNSUPPORTED,
                            "bare SELECT DISTINCT is not supported "
                            "(COUNT(DISTINCT ...) inside aggregates is)", select)
        for key, label in (
            ("having", "HAVING"), ("order", "ORDER BY"), ("limit", "LIMIT"),
            ("offset", "OFFSET"), ("window", "WINDOW"), ("qualify", "QUALIFY"),
            ("lateral", "LATERAL"), ("pivot", "PIVOT"),
        ):
            if select.args.get(key) is not None:
                raise self._err(E_UNSUPPORTED, f"{label} is outside the supported SQL subset",
                                select.args.get(key))

    # ====================================================================== #
    # FROM / JOIN
    # ====================================================================== #
    def _build_from(self, select: exp.Select,
                    cte_scope: dict[str, tuple[exp.CTE, dict[str, Any]]]) -> RelOutput:
        from_node = select.args.get("from_")
        if from_node is None or from_node.this is None:
            raise self._err(E_UNSUPPORTED, "SELECT without FROM is not supported", select)
        rel = self._scan_source(from_node.this, cte_scope)
        for join in select.args.get("joins") or []:
            rel = self._apply_join(join, rel, cte_scope)
        return rel

    def _scan_source(self, source: exp.Expression,
                     cte_scope: dict[str, tuple[exp.CTE, dict[str, Any]]]) -> RelOutput:
        if isinstance(source, exp.Subquery):
            alias = source.alias
            if not alias:
                raise self._err(E_UNSUPPORTED, "derived table in FROM requires an alias", source)
            if not isinstance(source.this, exp.Select):
                raise self._err(E_UNSUPPORTED,
                                f"unsupported derived table: {type(source.this).__name__}", source)
            inner = self.analyze_select(source.this)
            binding = Binding(alias.lower(), alias, "subquery", "", "", inner,
                              _node_span(source))
            inner.bindings = [binding]
            return inner

        if isinstance(source, exp.Table):
            tbl_alias_node = source.args.get("alias")
            alias_display = tbl_alias_node.this.name if tbl_alias_node else source.name
            alias = alias_display.lower()
            name_display = source.name
            name = name_display.lower()
            if source.args.get("db") or source.args.get("catalog"):
                raise self._err(E_UNSUPPORTED,
                                "schema-qualified table names are not supported", source)

            if name in cte_scope:
                cte, cache = cte_scope[name]
                if "rel" not in cache:
                    if not isinstance(cte.this, exp.Select):
                        raise self._err(E_UNSUPPORTED, "only SELECT CTE bodies are supported", cte)
                    cache["rel"] = self.analyze_select(cte.this)
                rel = self._entry_scan(cache["rel"], name, cte.alias,
                                       alias, alias_display, "cte", source)
                return rel

            if name in self.tables:
                rel = self._table_scan(name, name_display, alias, alias_display, source)
                rel.bindings = [Binding(alias, alias_display, "table", name, name_display, rel,
                                        _node_span(source))]
                return rel

            # View reference (resolved by the builder, which enforces cycles).
            upstream = self.resolve_view(name, _node_span(source), self.view_name)
            rel = self._entry_scan(upstream, name, name_display,
                                   alias, alias_display, "view", source)
            return rel

        raise self._err(E_UNSUPPORTED,
                        f"unsupported FROM source: {type(source).__name__}", source)

    def _entry_scan(self, upstream: RelOutput, object_name: str, object_display: str,
                    alias: str, alias_display: str, kind: str,
                    source: exp.Expression) -> RelOutput:
        """Copy an upstream view/CTE rel, prefixing one entry hop per column.

        A deep copy is mandatory: the same upstream relation may be scanned
        under several aliases, and mutating the shared object would prefix the
        same hop repeatedly and collapse distinct alias roles.
        """
        columns: list[ResolvedColumn] = []
        for col in upstream.columns:
            hop = Hop(
                from_view=self.view_name, from_view_display=self.view_name,
                from_column=col.name, from_column_display=col.name_display,
                to_object=object_name, to_object_display=object_display,
                alias=alias, alias_display=alias_display,
                to_column=col.name, to_column_display=col.name_display, kind=kind,
            )
            # Row-level control of the upstream relation becomes control of
            # every output column; it uses the same per-column entry hop so the
            # explanation chain stays identical regardless of which carrier
            # (column deps vs. row control) reached the boundary.
            columns.append(ResolvedColumn(
                name=col.name, name_display=col.name_display,
                value=self._prefix(col.value, hop),
                control=self._prefix(union_deps(col.control, upstream.row_control), hop),
                unknown=col.unknown or upstream.row_unknown,
                unknown_reasons=sorted(set(
                    col.unknown_reasons + upstream.row_unknown_reasons)),
                nullable_sources=list(col.nullable_sources),
                location=col.location, name_location=col.name_location,
            ))
        rel = RelOutput(
            columns=columns,
            # All upstream control was just folded into the columns with the
            # entry hop; keep row_control empty so no bare, unprefixed hop can
            # leak into the enclosing query's projection.
            row_control=[],
            row_unknown=upstream.row_unknown,
            row_unknown_reasons=list(upstream.row_unknown_reasons),
        )
        rel.bindings = [Binding(alias, alias_display, kind, object_name,
                                object_display, rel, _node_span(source))]
        return rel

    @staticmethod
    def _prefix(deps: list[Dep], hop: Hop) -> list[Dep]:
        return [
            Dep(kind=d.kind, base=d.base, reason=d.reason, path=(hop,) + d.path,
                base_alias=d.base_alias, base_alias_display=d.base_alias_display)
            for d in deps
        ]

    def _table_scan(self, name: str, name_display: str, alias: str,
                    alias_display: str, source: exp.Table) -> RelOutput:
        cols: list[ResolvedColumn] = []
        for col_display in self.tables[name].values():
            base = BaseRef(
                table=name, column=col_display.lower(),
                table_display=name_display, column_display=col_display,
            )
            hop = Hop(
                from_view=self.view_name, from_view_display=self.view_name,
                from_column="", from_column_display="",
                to_object=name, to_object_display=name_display,
                alias=alias, alias_display=alias_display,
                to_column=col_display, to_column_display=col_display, kind="table",
            )
            d = Dep(kind=VALUE, base=base, reason="base_column", path=(hop,),
                    base_alias=alias, base_alias_display=alias_display)
            ident = source.this
            cols.append(ResolvedColumn(
                name=col_display.lower(), name_display=col_display,
                value=[d], control=[], location=_ident_location(ident),
            ))
        return RelOutput(columns=cols, row_control=[], bindings=[])

    def _apply_join(self, join: exp.Join, left: RelOutput,
                    cte_scope: dict[str, tuple[exp.CTE, dict[str, Any]]]) -> RelOutput:
        method = (join.method or "").upper()
        side = (join.side or "").upper()
        kind = (join.kind or "").upper()
        if method == "CROSS" or kind == "CROSS":
            raise self._err(E_UNSUPPORTED,
                            "comma/CROSS joins are outside the supported SQL subset", join)
        if kind not in ("", "INNER", "OUTER") or side not in ("", "LEFT"):
            raise self._err(E_UNSUPPORTED,
                            f"unsupported join: side={side or 'INNER'} kind={kind or 'INNER'}",
                            join)
        is_left = side == "LEFT"

        right = self._scan_source(join.this, cte_scope)
        on_node = join.args.get("on")
        using_list = join.args.get("using")
        if on_node is None and not using_list:
            raise self._err(E_UNSUPPORTED, "joins require an ON or USING clause", join)
        if on_node is not None and using_list:
            raise self._err(E_UNSUPPORTED, "join cannot have both ON and USING", join)

        if using_list is not None:
            return self._apply_using(join, using_list, left, right, is_left)
        # In sqlglot 30 args["on"] is already the predicate expression itself.
        return self._apply_on(join, on_node, left, right, is_left)

    def _wrap_join_column(self, col: ResolvedColumn, binding: Binding,
                          join_control: list[Dep], left_row_control: list[Dep],
                          right_row_control: list[Dep], cond_unknown: bool,
                          is_left: bool, side: str) -> ResolvedColumn:
        """Rebind a scan column to the join output it participates in."""
        out = ResolvedColumn(
            name=col.name, name_display=col.name_display,
            value=list(col.value),
            control=union_deps(col.control, join_control,
                               left_row_control, right_row_control),
            unknown=col.unknown or cond_unknown,
            unknown_reasons=sorted(set(
                col.unknown_reasons +
                (["upstream unknown predicate/relation"] if cond_unknown else []))),
            nullable_sources=list(col.nullable_sources),
            location=col.location, name_location=col.name_location,
        )
        if is_left and side == "right":
            # The null-extended side: its value may become NULL and rows may
            # multiply through match cardinality - a control effect even when
            # the column is never projected.
            out.control = union_deps(
                out.control, self._as_control(out.value, R_LEFT_NULL))
            out.nullable_sources = sorted(
                set(out.nullable_sources) | {d.base.id for d in out.value})
        return out

    def _rebind(self, binding: Binding, columns: list[ResolvedColumn],
                using_merged: set[str] | None = None) -> Binding:
        wrapped = RelOutput(columns=columns, row_control=binding.rel.row_control,
                            bindings=[binding],
                            using_merged=set(using_merged or binding.rel.using_merged))
        return Binding(binding.alias, binding.alias_display, binding.kind,
                       binding.object_name, binding.object_display, wrapped,
                       binding.role_location)

    def _apply_on(self, join: exp.Join, predicate: exp.Expression, left: RelOutput,
                  right: RelOutput, is_left: bool) -> RelOutput:
        scope = self._join_scope(left, right)
        cond = self._analyze_expr(predicate, scope)
        # ON decides pairing/multiplicity for every output row.
        join_control = union_deps(
            self._with_reason(cond.control, R_JOIN),
            self._as_control(cond.value, R_JOIN),
        )

        out_cols: list[ResolvedColumn] = []
        wrapped_bindings: list[Binding] = []
        for binding in left.bindings:
            wrapped = [
                self._wrap_join_column(c, binding, join_control, left.row_control,
                                       right.row_control, cond.unknown, is_left, "left")
                for c in binding.rel.columns
            ]
            out_cols.extend(wrapped)
            wrapped_bindings.append(self._rebind(binding, wrapped))
        for binding in right.bindings:
            wrapped = [
                self._wrap_join_column(c, binding, join_control, left.row_control,
                                       right.row_control, cond.unknown, is_left, "right")
                for c in binding.rel.columns
            ]
            out_cols.extend(wrapped)
            wrapped_bindings.append(self._rebind(binding, wrapped))

        row_control = union_deps(left.row_control, right.row_control, join_control)
        row_unknown = left.row_unknown or right.row_unknown or cond.unknown
        reasons = list({*left.row_unknown_reasons, *right.row_unknown_reasons,
                        *cond.unknown_reasons})
        return RelOutput(
            columns=out_cols, row_control=row_control, row_unknown=row_unknown,
            row_unknown_reasons=reasons,
            bindings=wrapped_bindings,
        )

    def _apply_using(self, join: exp.Join, using_cols: list[exp.Expression],
                     left: RelOutput, right: RelOutput, is_left: bool) -> RelOutput:
        merged: set[str] = set()
        join_control: list[Dep] = []
        cond_unknown = False
        cond_reasons: list[str] = []
        for node in using_cols:
            key = node.name.lower()
            merged.add(key)
            # At this point the merge output does not exist yet: take the key
            # column straight from each side's own binding.
            lcol = self._column_from_binding(left.bindings[0], key, node)
            right_alias = right.bindings[0].alias
            rcol = self._column_from_binding(
                next(b for b in right.bindings if b.alias == right_alias), key, node)
            er = ExprResult.merge(lcol.expr_result(), rcol.expr_result())
            join_control = union_deps(
                join_control, self._with_reason(er.control, R_JOIN),
                self._as_control(er.value, R_JOIN),
            )
            cond_unknown = cond_unknown or er.unknown
            cond_reasons.extend(er.unknown_reasons)

        out_cols: list[ResolvedColumn] = []
        wrapped_bindings: list[Binding] = []
        right_columns = [c for b in right.bindings for c in b.rel.columns]

        for binding in left.bindings:
            wrapped: list[ResolvedColumn] = []
            for lcol in binding.rel.columns:
                if lcol.name in merged:
                    rcol = next(c for c in right_columns if c.name == lcol.name)
                    mc = ResolvedColumn(
                        name=lcol.name, name_display=lcol.name_display,
                        value=union_deps(lcol.value, rcol.value),
                        control=union_deps(lcol.control, rcol.control),
                        unknown=lcol.unknown or rcol.unknown,
                        unknown_reasons=sorted(set(
                            lcol.unknown_reasons + rcol.unknown_reasons)),
                        nullable_sources=sorted(
                            set(lcol.nullable_sources) | set(rcol.nullable_sources)),
                        location=lcol.location, name_location=lcol.name_location,
                    )
                    wrapped.append(self._wrap_join_column(
                        mc, binding, join_control, left.row_control,
                        right.row_control, cond_unknown, is_left, "left"))
                else:
                    wrapped.append(self._wrap_join_column(
                        lcol, binding, join_control, left.row_control,
                        right.row_control, cond_unknown, is_left, "left"))
            out_cols.extend(wrapped)
            wrapped_bindings.append(
                self._rebind(binding, wrapped,
                             using_merged=merged.intersection({c.name for c in wrapped})))

        for binding in right.bindings:
            wrapped = []
            for rcol in binding.rel.columns:
                if rcol.name in merged:
                    continue  # USING merge key appears once, owned by left side
                wrapped.append(self._wrap_join_column(
                    rcol, binding, join_control, left.row_control,
                    right.row_control, cond_unknown, is_left, "right"))
            out_cols.extend(wrapped)
            wrapped_bindings.append(self._rebind(binding, wrapped))

        return RelOutput(
            columns=out_cols,
            row_control=union_deps(left.row_control, right.row_control, join_control),
            row_unknown=left.row_unknown or right.row_unknown or cond_unknown,
            row_unknown_reasons=sorted(set(
                left.row_unknown_reasons + right.row_unknown_reasons + cond_reasons)),
            bindings=wrapped_bindings,
            using_merged=set(merged),
        )
    def _join_scope(self, left: RelOutput, right: RelOutput) -> list[Binding]:
        return left.bindings + right.bindings

    # ====================================================================== #
    # WHERE / GROUP / PROJECT
    # ====================================================================== #
    def _apply_where(self, select: exp.Select, rel: RelOutput) -> RelOutput:
        where = select.args.get("where")
        if where is None:
            return rel
        cond = self._analyze_expr(where.this, rel.bindings)
        # Predicate columns arrive in cond.control; normalize the reason to
        # row filtering. Any value deps surviving (e.g. arithmetic) join too.
        filter_control = union_deps(
            self._with_reason(cond.control, R_FILTER),
            self._as_control(cond.value, R_FILTER),
        )
        for col in rel.columns:
            col.control = union_deps(col.control, filter_control, rel.row_control)
            if cond.unknown:
                col.unknown = True
                col.unknown_reasons = sorted(set(
                    col.unknown_reasons + cond.unknown_reasons))
        rel.row_control = union_deps(rel.row_control, filter_control)
        rel.row_unknown = rel.row_unknown or cond.unknown
        rel.row_unknown_reasons = sorted(set(rel.row_unknown_reasons + cond.unknown_reasons))
        return rel

    def _apply_group(self, select: exp.Select, rel: RelOutput) -> RelOutput:
        group = select.args.get("group")
        has_agg = self._select_has_aggregate(select)
        if group is None and not has_agg:
            return rel
        if group is not None and group.args.get("cube") or group is not None and group.args.get("rollup"):
            raise self._err(E_UNSUPPORTED, "CUBE/ROLLUP are not supported", group)

        group_exprs: list[exp.Expression] = list(group.expressions) if group is not None else []
        group_results: list[ExprResult] = []
        group_sqls: set[str] = set()
        for g in group_exprs:
            self._reject_aggregate(g, "aggregate in GROUP BY")
            gr = self._analyze_expr(g, rel.bindings)
            group_results.append(gr)
            group_sqls.add(g.sql(dialect="sqlite").lower())

        # Validate SELECT against grouping rules and compute outputs.
        out_cols: list[ResolvedColumn] = []
        group_row_control = union_deps(
            rel.row_control,
            *[union_deps(r.control, self._as_control(r.value, R_GROUP)) for r in group_results],
        )
        group_row_unknown = rel.row_unknown or any(r.unknown for r in group_results)
        group_reasons = sorted({
            x for r in group_results for x in r.unknown_reasons
        } | set(rel.row_unknown_reasons))

        used_names: set[str] = set()
        for proj in select.expressions:
            expr = proj.this if isinstance(proj, exp.Alias) else proj
            if isinstance(expr, (exp.Star, exp.Column)) and self._is_star(expr):
                raise self._err(E_UNSUPPORTED, "SELECT * is not allowed together with GROUP BY",
                                proj)
            agg = self._single_outer_aggregate(expr)
            if agg is not None:
                out_cols.append(self._aggregate_column(
                    agg, proj, rel, group_row_control, group_row_unknown, group_reasons))
            else:
                self._reject_aggregate(expr, "nested aggregate in SELECT list")
                if expr.sql(dialect="sqlite").lower() not in group_sqls:
                    # SQLite also allows grouping by an output alias.
                    if not self._matches_group_alias(expr, group_exprs, select):
                        raise self._err(
                            E_GROUPING,
                            f"column/expression {expr.sql()!r} is neither grouped nor aggregated",
                            expr,
                        )
                er = self._analyze_expr(expr, rel.bindings)
                col = ResolvedColumn(
                    name=(proj.alias_or_name or expr.name or "expr").lower(),
                    name_display=self._output_spelling(proj, expr),
                    # Grouping keys keep their ordinary value provenance; the
                    # R_GROUP control effect lives in group_row_control.
                    value=er.value,
                    control=union_deps(er.control, group_row_control),
                    unknown=er.unknown or group_row_unknown,
                    unknown_reasons=sorted(set(er.unknown_reasons + group_reasons)),
                    nullable_sources=er.nullable_sources,
                    location=_node_span(proj),
                    name_location=self._alias_location(proj),
                )
                out_cols.append(self._check_dup(col, used_names))

        return RelOutput(
            columns=out_cols, row_control=group_row_control,
            row_unknown=group_row_unknown, row_unknown_reasons=group_reasons,
            bindings=rel.bindings,
        )

    def _matches_group_alias(self, expr: exp.Expression,
                             group_exprs: list[exp.Expression], select: exp.Select) -> bool:
        if not isinstance(expr, exp.Column):
            return False
        output_aliases = {
            (p.alias_or_name or "").lower(): p
            for p in select.expressions if isinstance(p, exp.Alias)
        }
        if expr.name.lower() not in output_aliases:
            return False
        target = output_aliases[expr.name.lower()]
        target_sql = target.this.sql(dialect="sqlite").lower()
        return any(g.sql(dialect="sqlite").lower() == target_sql for g in group_exprs)

    def _aggregate_column(self, agg: exp.AggFunc, proj: exp.Expression,
                          rel: RelOutput, group_row_control: list[Dep],
                          group_row_unknown: bool, group_reasons: list[str]) -> ResolvedColumn:
        name = (proj.alias_or_name or agg.sql_name()).lower()
        display = self._output_spelling(proj, agg)
        fname = agg.sql_name().lower()
        # sqlglot v30 represents DISTINCT either as arg or as a Distinct wrapper.
        distinct_node = agg.args.get("distinct")
        distinct_inner = agg.this if isinstance(agg.this, exp.Distinct) else None
        is_distinct = distinct_node is not None or distinct_inner is not None
        # v30 keeps the single aggregate argument in `this`; tolerate both shapes.
        raw_args = list(agg.expressions)
        if agg.this is not None and not isinstance(agg.this, (exp.Star, exp.Distinct)):
            raw_args = [agg.this]

        if fname == "count":
            star = isinstance(agg.this, exp.Star)
            if star:
                if raw_args:
                    raise self._err(E_AGGREGATE, "COUNT(*) takes no arguments", agg)
                er = ExprResult.empty()
            else:
                if distinct_inner is not None:
                    raw_args = list(distinct_inner.expressions)
                if len(raw_args) != 1:
                    raise self._err(E_AGGREGATE, "COUNT takes one expression", agg)
                er = self._analyze_expr(raw_args[0], rel.bindings)
                # Whether an input row counts (non-null) is a control effect;
                # COUNT(DISTINCT x) also compares values, so add value deps.
                er.control = union_deps(er.control,
                                        self._as_control(er.value, R_COUNT_INPUT))
                if is_distinct:
                    # Distinct comparison compares the actual cell values.
                    er.control = union_deps(er.control,
                                            self._as_control(er.value, R_DISTINCT))
                else:
                    # Plain COUNT(x): null/non-null only, no computed value source.
                    er.value = []
        elif fname == "sum":
            if len(raw_args) != 1 or is_distinct:
                raise self._err(E_AGGREGATE,
                                "SUM takes one expression (SUM(DISTINCT ...) unsupported)",
                                agg)
            er = self._analyze_expr(raw_args[0], rel.bindings)
        else:
            raise self._err(E_UNSUPPORTED,
                            f"aggregate {fname.upper()} is outside the supported subset", agg)

        col = ResolvedColumn(
            name=name, name_display=display,
            value=er.value,
            control=union_deps(er.control, group_row_control, rel.row_control),
            unknown=er.unknown or group_row_unknown,
            unknown_reasons=sorted(set(er.unknown_reasons + group_reasons)),
            nullable_sources=er.nullable_sources,
            location=_node_span(proj), name_location=self._alias_location(proj),
        )
        return col

    def _project(self, select: exp.Select, rel: RelOutput) -> RelOutput:
        if select.args.get("group") is not None or self._select_has_aggregate(select):
            return rel  # grouped projection was built by _apply_group

        out_cols: list[ResolvedColumn] = []
        used_names: set[str] = set()
        for proj in select.expressions:
            if isinstance(proj, exp.Star):
                for col in rel.columns:
                    clone = ResolvedColumn(
                        name=col.name, name_display=col.name_display,
                        value=list(col.value), control=union_deps(col.control, rel.row_control),
                        unknown=col.unknown or rel.row_unknown,
                        unknown_reasons=sorted(set(
                            col.unknown_reasons + rel.row_unknown_reasons)),
                        nullable_sources=list(col.nullable_sources),
                        location=col.location, name_location=col.name_location,
                    )
                    out_cols.append(self._check_dup(clone, used_names))
                continue

            if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                qualifier = proj.table.lower()
                binding = self._binding_by_alias(qualifier, rel.bindings, proj)
                for col in binding.rel.columns:
                    clone = ResolvedColumn(
                        name=col.name, name_display=col.name_display,
                        value=list(col.value), control=union_deps(col.control, rel.row_control),
                        unknown=col.unknown or rel.row_unknown,
                        unknown_reasons=sorted(set(
                            col.unknown_reasons + rel.row_unknown_reasons)),
                        nullable_sources=list(col.nullable_sources),
                        location=col.location, name_location=col.name_location,
                    )
                    out_cols.append(self._check_dup(clone, used_names))
                continue

            expr = proj.this if isinstance(proj, exp.Alias) else proj
            er = self._analyze_expr(expr, rel.bindings)
            col = ResolvedColumn(
                name=(proj.alias_or_name or expr.name or "expr").lower(),
                name_display=self._output_spelling(proj, expr),
                value=er.value,
                control=union_deps(er.control, rel.row_control),
                unknown=er.unknown or rel.row_unknown,
                unknown_reasons=sorted(set(er.unknown_reasons + rel.row_unknown_reasons)),
                nullable_sources=er.nullable_sources,
                location=_node_span(proj), name_location=self._alias_location(proj),
            )
            out_cols.append(self._check_dup(col, used_names))

        return RelOutput(
            columns=out_cols, row_control=rel.row_control,
            row_unknown=rel.row_unknown, row_unknown_reasons=rel.row_unknown_reasons,
            bindings=rel.bindings,
        )

    # ====================================================================== #
    # Expression analysis
    # ====================================================================== #
    def _analyze_expr(self, node: exp.Expression,
                      bindings: list[Binding]) -> ExprResult:
        if isinstance(node, exp.Column):
            return self._resolve_column_node(node, bindings).expr_result()

        if isinstance(node, exp.Literal) or isinstance(node, exp.Null) or node is exp.Null:
            return ExprResult.empty()

        if isinstance(node, exp.Paren):
            return self._analyze_expr(node.this, bindings)

        if isinstance(node, exp.Neg):
            return self._analyze_expr(node.this, bindings)

        if isinstance(node, exp.Case):
            return self._analyze_case(node, bindings)

        # Aggregates are legal only through the grouped path.
        if isinstance(node, exp.AggFunc):
            raise self._err(E_AGGREGATE,
                            f"aggregate {node.sql_name().upper()} is only valid in an "
                            "aggregate SELECT/GROUP BY context", node)

        if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.DPipe)):
            return ExprResult.merge(
                self._analyze_expr(node.this, bindings),
                self._analyze_expr(node.expression, bindings),
            )

        if isinstance(node, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
                             exp.And, exp.Or, exp.Not, exp.Is, exp.In, exp.Like,
                             exp.Between, exp.Glob, exp.RegexpLike)):
            parts = [self._analyze_expr(c, bindings)
                     for c in node.flatten() if isinstance(c, exp.Expression)]
            merged = ExprResult.merge(*parts) if parts else ExprResult.empty()
            # A predicate yields no output value. Keep the operand value deps
            # intact (with base_column reason); the calling context reclassifies
            # them as filter / join_pairing / case_condition control deps.
            # Operand control deps (e.g. nested CASE conditions) are preserved.
            return merged

        if isinstance(node, exp.Anonymous):
            return self._analyze_function(node, bindings)

        if isinstance(node, exp.Func):
            return self._analyze_builtin_func(node, bindings)

        # CAST/Collate etc. are deliberately outside the contracted subset.
        raise self._err(E_UNSUPPORTED,
                        f"unsupported expression: {type(node).__name__} ({node.sql()[:40]})",
                        node)

    def _analyze_case(self, node: exp.Case, bindings: list[Binding]) -> ExprResult:
        results: list[ExprResult] = []
        # Simple CASE: CASE x WHEN v THEN ... - x is itself a control input.
        if node.args.get("this") is not None:
            base = self._analyze_expr(node.this, bindings)
            base.control = union_deps(self._with_reason(base.control, R_CASE_COND),
                                      self._as_control(base.value, R_CASE_COND))
            base.value = []
            results.append(base)
        for if_node in node.args.get("ifs") or []:
            cond = self._analyze_expr(if_node.this, bindings)
            cond.control = union_deps(self._with_reason(cond.control, R_CASE_COND),
                                      self._as_control(cond.value, R_CASE_COND))
            cond.value = []
            then = self._analyze_expr(if_node.args["true"], bindings)
            results.append(cond)
            results.append(then)
        if node.args.get("default") is not None:
            results.append(self._analyze_expr(node.args["default"], bindings))
        out = ExprResult.merge(*results)
        # The conditions select which value appears: keep them as control deps
        # on the resulting cell (already R_CASE_COND above).
        return out

    def _func_arg_nodes(self, node: exp.Expression) -> list[exp.Expression]:
        """All scalar argument expressions of a function call."""
        args: list[exp.Expression] = []
        if isinstance(node.this, exp.Star) or isinstance(node.this, exp.Distinct):
            pass
        elif isinstance(node.this, exp.Expression):
            args.append(node.this)
        for extra in node.expressions:
            if isinstance(extra, exp.Expression):
                args.append(extra)
        return args

    def _analyze_builtin_func(self, node: exp.Func,
                              bindings: list[Binding]) -> ExprResult:
        # CAST and similar type-construction functions are outside the subset.
        if isinstance(node, (exp.Cast, exp.TryCast)):
            raise self._err(E_UNSUPPORTED,
                            f"unsupported function: {node.sql_name().upper()} "
                            "(type-changing functions are not in the SQL subset)", node)
        fname = node.sql_name().lower()
        decl = self.functions.get(fname)
        if decl is not None:
            # A catalog declaration overrides the default built-in treatment,
            # e.g. abs registered as argument_dependent or unknown.
            args = [self._analyze_expr(a, bindings) for a in self._func_arg_nodes(node)]
            return self._apply_function_declaration(node, fname, decl, args)
        if fname not in _DEFAULT_DETERMINISTIC_FUNCS:
            raise self._err(
                E_UNKNOWN_FUNCTION,
                f"built-in function {node.sql_name()!r} is not in the deterministic "
                "allow-list and is not registered in catalog.functions; register it as "
                "argument_dependent or unknown instead of guessing",
                node,
            )
        args = [self._analyze_expr(a, bindings) for a in self._func_arg_nodes(node)]
        return ExprResult.merge(*args) if args else ExprResult.empty()

    def _apply_function_declaration(self, node: exp.Expression, fname: str,
                                    decl: dict[str, Any],
                                    args: list[ExprResult]) -> ExprResult:
        merged = ExprResult.merge(*args) if args else ExprResult.empty()
        dependency = decl.get("dependency", "unknown")
        if dependency == "argument_dependent":
            return merged
        if dependency == "unknown":
            merged.unknown = True
            reason = f"unknown function {node.name}({', '.join(a.sql() for a in node.expressions)})"
            merged.unknown_reasons = sorted(set(merged.unknown_reasons + [reason]))
            return merged
        raise self._err(E_UNKNOWN_FUNCTION,
                        f"function {fname!r} has invalid dependency declaration "
                        f"{dependency!r}", node)

    def _analyze_function(self, node: exp.Anonymous, bindings: list[Binding]) -> ExprResult:
        fname = node.name.lower()
        decl = self.functions.get(fname)
        if decl is None:
            raise self._err(
                E_UNKNOWN_FUNCTION,
                f"function {node.name!r} is not registered in catalog.functions; register it "
                "as argument_dependent or unknown rather than guessing its dependencies",
                node,
            )
        args = [self._analyze_expr(a, bindings) for a in node.expressions]
        return self._apply_function_declaration(node, fname, decl, args)

    # ====================================================================== #
    # Name resolution
    # ====================================================================== #
    def _resolve_column_node(self, col: exp.Column,
                             bindings: list[Binding]) -> ResolvedColumn:
        qualifier = col.table.lower() if col.table else None
        return self._resolve_column(qualifier, col.name.lower(), bindings, col)

    def _resolve_column(self, qualifier: str | None, name: str,
                        bindings: list[Binding], node: exp.Expression,
                        exclude_aliases: set[str] | None = None) -> ResolvedColumn:
        candidates: list[tuple[Binding, ResolvedColumn]] = []
        for b in bindings:
            if exclude_aliases and b.alias in exclude_aliases:
                continue
            if qualifier is not None and b.alias != qualifier:
                continue
            for c in b.rel.columns:
                if c.name == name:
                    candidates.append((b, c))
        if qualifier is not None and not candidates:
            raise self._err(E_MISSING_COLUMN,
                            f"no binding {qualifier!r} exposes column {name!r}", node)
        if not candidates:
            visible = ", ".join(sorted({c.name for b in bindings for c in b.rel.columns}))
            raise self._err(E_MISSING_COLUMN,
                            f"unknown column {name!r}; visible columns: {visible}", node)
        if len(candidates) > 1:
            # USING merge: the same-named columns were coalesced into one
            # output column owned by the left-side binding, so an unqualified
            # reference resolves to that single merged column.
            merged_owners = {
                b for b, c in candidates
                if b.rel.using_merged and c.name in b.rel.using_merged
            }
            if qualifier is None and len(merged_owners) == 1:
                owner = next(iter(merged_owners))
                col = next(c for b, c in candidates if b is owner)
                return self._with_binding_row_control(col, owner)
            owners = ", ".join(sorted({b.alias_display for b, _ in candidates}))
            raise self._err(E_AMBIGUOUS_COLUMN,
                            f"column {name!r} is ambiguous; present in bindings: {owners}. "
                            "Qualify it with an alias; the view is rejected rather than guessed",
                            node)
        binding, col = candidates[0]
        return self._with_binding_row_control(col, binding)

    def _with_binding_row_control(self, col: ResolvedColumn,
                                  binding: Binding) -> ResolvedColumn:
        """Attach the binding's row-level control to a resolved column.

        A column referenced in an ON/WHERE predicate must carry the row
        controls of its own relation (e.g. a right table filtered inside a
        subquery), otherwise an unprojected right-side control source would be
        lost. Returns a fresh object so cached columns stay untouched.
        """
        if not binding.rel.row_control:
            return col
        return ResolvedColumn(
            name=col.name, name_display=col.name_display,
            value=list(col.value),
            control=union_deps(col.control, binding.rel.row_control),
            unknown=col.unknown or binding.rel.row_unknown,
            unknown_reasons=sorted(set(
                col.unknown_reasons + binding.rel.row_unknown_reasons)),
            nullable_sources=list(col.nullable_sources),
            location=col.location, name_location=col.name_location,
        )

    def _binding_by_alias(self, alias: str, bindings: list[Binding],
                          node: exp.Expression) -> Binding:
        for b in bindings:
            if b.alias == alias:
                return b
        raise self._err(E_MISSING_COLUMN, f"unknown table alias {alias!r}", node)

    def _column_from_binding(self, binding: Binding, name: str,
                             node: exp.Expression) -> ResolvedColumn:
        for c in binding.rel.columns:
            if c.name == name:
                return c
        raise self._err(E_MISSING_COLUMN,
                        f"binding {binding.alias_display!r} has no column {name!r}", node)

    # ====================================================================== #
    # Helpers
    # ====================================================================== #
    def _as_control(self, value_deps: list[Dep], reason: str) -> list[Dep]:
        return [
            Dep(kind=CONTROL, base=d.base, reason=reason, path=d.path,
                base_alias=d.base_alias, base_alias_display=d.base_alias_display)
            for d in value_deps
        ]

    def _classify_predicate(self, cond: ExprResult, reason: str) -> list[Dep]:
        """Predicate columns become control deps under ``reason``.

        Control deps already classified by nested contexts (CASE inside a
        predicate, etc.) keep their original reason; plain column references
        carried in ``control`` by the predicate walker take the new reason.
        """
        return union_deps(
            self._with_reason(cond.control, reason),
            self._as_control(cond.value, reason),
        )

    def _with_reason(self, deps: list[Dep], reason: str) -> list[Dep]:
        return [
            Dep(kind=d.kind, base=d.base, reason=reason, path=d.path,
                base_alias=d.base_alias, base_alias_display=d.base_alias_display)
            for d in deps
        ]

    def _check_dup(self, col: ResolvedColumn, used: set[str]) -> ResolvedColumn:
        if col.name in used:
            raise LineageError(
                E_DUPLICATE_OUTPUT,
                f"duplicate output column name {col.name_display!r}; rename the columns so "
                "downstream unqualified references stay unique",
                col.name_location or col.location, self.view_name,
            )
        used.add(col.name)
        return col

    def _is_star(self, expr: exp.Expression) -> bool:
        if isinstance(expr, exp.Star):
            return True
        return isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star)

    def _select_has_aggregate(self, select: exp.Select) -> bool:
        for proj in select.expressions:
            expr = proj.this if isinstance(proj, exp.Alias) else proj
            if expr.find(exp.AggFunc) is not None:
                return True
        return False

    def _single_outer_aggregate(self, expr: exp.Expression) -> exp.AggFunc | None:
        """Return the aggregate when ``expr`` is exactly one aggregate call."""
        if isinstance(expr, exp.AggFunc):
            inner = [n for n in expr.walk() if isinstance(n, exp.AggFunc)]
            if len(inner) > 1:
                raise self._err(E_AGGREGATE, "nested aggregates are not supported", expr)
            return expr
        if expr.find(exp.AggFunc) is not None:
            raise self._err(E_AGGREGATE,
                            "aggregate cannot be embedded inside another scalar expression "
                            "in the SELECT list", expr)
        return None

    def _reject_aggregate(self, node: exp.Expression, message: str) -> None:
        if node.find(exp.AggFunc) is not None:
            raise self._err(E_AGGREGATE, message, node)

    def _output_spelling(self, proj: exp.Expression, expr: exp.Expression) -> str:
        if isinstance(proj, exp.Alias):
            return proj.args["alias"].name
        if isinstance(expr, exp.Column):
            return expr.name
        return expr.sql(dialect="sqlite")

    def _alias_location(self, proj: exp.Expression) -> SourceLocation | None:
        if isinstance(proj, exp.Alias):
            return _ident_location(proj.args["alias"])
        if isinstance(proj, exp.Column):
            return _ident_location(proj.this)
        return None

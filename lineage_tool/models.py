"""Core data model for the lineage catalog.

Every dependency path ends on a *base column* (a column of a catalog table).
A :class:`Dep` carries the full chain of hops that explains how the base
column reaches a view output column, so the same base column reached through
two different table aliases or two different views yields two distinct,
bindable sources instead of one collapsed name.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any

VALUE = "value"
CONTROL = "control"

# Control-dep reason vocabulary (kept stable; used by tests and reports).
R_FILTER = "filter"            # WHERE predicate: decides row presence
R_JOIN = "join_pairing"        # ON/USING: decides which rows pair (and multiplicity)
R_GROUP = "group_key"          # GROUP BY key: decides grouping / row presence
R_CASE_COND = "case_condition" # CASE condition: picks the value inside a row
R_COUNT_INPUT = "count_input"  # COUNT(expr): null/non-null of input decides a row's 1
R_LEFT_NULL = "left_null_side" # LEFT JOIN null-extension: right side may go NULL / multiply
R_DISTINCT = "distinct_input"  # COUNT(DISTINCT ...) input feeds the distinct decision


@dataclass(frozen=True)
class BaseRef:
    """A column of a catalog base table.

    Identity is case-insensitive (SQLite semantics); the original spellings
    are preserved for display.
    """

    table: str
    column: str
    table_display: str
    column_display: str

    @staticmethod
    def key(table: str, column: str) -> str:
        return f"{table.lower()}.{column.lower()}"

    @property
    def id(self) -> str:
        return self.key(self.table, self.column)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table_display,
            "column": self.column_display,
            "id": self.id,
        }


@dataclass(frozen=True)
class Hop:
    """One edge in an explanation path: output (object, column) -> input binding.

    ``alias`` is the FROM-clause alias (or object name when unaliased) so two
    scans of the same table under different aliases stay distinguishable.
    """

    from_view: str
    from_view_display: str
    from_column: str
    from_column_display: str
    to_object: str
    to_object_display: str
    alias: str
    alias_display: str
    to_column: str | None
    to_column_display: str | None
    kind: str  # view | cte | table | left_null_side | merged_using

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Dep:
    """One dependency edge from an output column to a base column."""

    kind: str          # VALUE | CONTROL
    base: BaseRef
    reason: str
    path: tuple[Hop, ...]
    # Alias role at the base-table scan, preserved even after path trimming.
    base_alias: str
    base_alias_display: str

    def dedup_key(self) -> tuple[str, str, tuple[tuple[str, str, str, str], ...]]:
        # The reason is intentionally NOT part of the key: the same base column
        # reaching the output through one identical binding path must appear
        # once even when several operators classify it (the reasons are merged
        # by ``dedupe_deps``).
        return (
            self.kind,
            self.base.id,
            tuple(
                (
                    h.from_view.lower(),
                    h.from_column.lower(),
                    h.alias.lower(),
                    h.kind,
                )
                for h in self.path
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "reason": self.reason,
            "base": self.base.to_dict(),
            "base_alias": self.base_alias_display,
            "path": [h.to_dict() for h in self.path],
        }


@dataclass
class SourceLocation:
    """Byte offset and 1-based line/column inside the view SQL file."""

    line: int
    col: int
    start: int
    end: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ColumnAnalysis:
    name: str                       # output column name, original spelling
    name_display: str
    location: SourceLocation | None # position of the projection expression
    name_location: SourceLocation | None  # position of the alias/name identifier
    value_deps: list[Dep] = field(default_factory=list)
    control_deps: list[Dep] = field(default_factory=list)
    unknown: bool = False
    unknown_reasons: list[str] = field(default_factory=list)
    # Base columns whose value flows in through a LEFT JOIN right side, i.e.
    # whose null-extended value can appear. Recorded as control deps too;
    # this flag keeps the semantics explicit.
    nullable_sources: list[str] = field(default_factory=list)

    @property
    def precision(self) -> str:
        """exact | unknown | no_value_source (e.g. COUNT(*), literal-only)."""
        if self.unknown:
            return "unknown"
        if not self.value_deps:
            return "no_value_source"
        return "exact"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name_display,
            "precision": self.precision,
            "unknown": self.unknown,
            "unknown_reasons": self.unknown_reasons,
            "location": self.location.to_dict() if self.location else None,
            "name_location": self.name_location.to_dict() if self.name_location else None,
            "nullable_sources": self.nullable_sources,
            "value_sources": [d.to_dict() for d in self.value_deps],
            "control_sources": [d.to_dict() for d in self.control_deps],
        }


@dataclass
class ViewAnalysis:
    name: str
    name_display: str
    sql_file: str
    sql_text: str
    sql_sha256: str
    status: str                     # ok | error
    columns: list[ColumnAnalysis] = field(default_factory=list)
    # Object names referenced in FROM/CTE resolution, lowercased keys.
    source_objects: list[str] = field(default_factory=list)
    source_roles: list[dict[str, Any]] = field(default_factory=list)
    view_dependencies: list[str] = field(default_factory=list)  # other views only
    cte_names: list[str] = field(default_factory=list)
    error: dict[str, Any] | None = None
    # Fingerprint of the inputs this analysis was built from.
    input_fingerprint: str | None = None
    schema_fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name_display,
            "sql_file": self.sql_file,
            "sql_sha256": self.sql_sha256,
            "status": self.status,
            "source_objects": self.source_objects,
            "source_roles": self.source_roles,
            "view_dependencies": self.view_dependencies,
            "cte_names": self.cte_names,
            "input_fingerprint": self.input_fingerprint,
            "schema_fingerprint": self.schema_fingerprint,
            "error": self.error,
            "columns": [c.to_dict() for c in self.columns],
        }


def dedupe_deps(deps: list[Dep]) -> list[Dep]:
    """Stable de-duplication preserving full binding identity.

    Dependencies with the same kind/base/path are merged; their classification
    reasons are combined (e.g. one column can be both a join key and a filter
    input through different clauses - but the identical edge is stored once
    with both reasons).
    """
    merged: dict[Any, Dep] = {}
    reasons: dict[Any, set[str]] = {}
    for d in deps:
        key = d.dedup_key()
        reason_tokens = set(d.reason.split("+"))
        if key not in merged:
            merged[key] = d
            reasons[key] = reason_tokens
        else:
            reasons[key] |= reason_tokens
    out = []
    for key, d in merged.items():
        unique_reasons = sorted(reasons[key])
        if len(unique_reasons) == 1:
            out.append(d)
        else:
            out.append(Dep(
                kind=d.kind, base=d.base, reason="+".join(unique_reasons),
                path=d.path, base_alias=d.base_alias,
                base_alias_display=d.base_alias_display,
            ))
    return sorted(out, key=lambda d: (d.kind, d.base.id, d.reason, d.base_alias))


def union_deps(*groups: list[Dep]) -> list[Dep]:
    out: list[Dep] = []
    for g in groups:
        out.extend(g)
    return dedupe_deps(out)

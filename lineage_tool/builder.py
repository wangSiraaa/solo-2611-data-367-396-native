"""Cross-view analysis, incremental rebuild and the persistent catalog."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from .errors import (
    E_BAD_CATALOG, E_CASCADED, E_CYCLE, E_LIMIT_EXCEEDED, E_MISSING_OBJECT,
    E_MISSING_VIEW_FILE, FailureReport, LineageError,
)
from .analyzer import Analyzer, RelOutput, parse_select
from sqlglot import exp
from .models import (
    BaseRef, ColumnAnalysis, Dep, Hop, SourceLocation, ViewAnalysis, dedupe_deps,
)

MAX_VIEWS = 20
MAX_SOURCES_PER_VIEW = 5
MAX_BASE_TABLES = 10
MAX_COLUMNS_PER_TABLE = 20

CATALOG_VERSION = 1


# --------------------------------------------------------------------------- #
# Catalog loading
# --------------------------------------------------------------------------- #

def load_catalog(path: str | os.PathLike) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise LineageError(E_BAD_CATALOG, f"catalog file not found: {p}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise LineageError(E_BAD_CATALOG, f"catalog is not valid JSON: {exc}")
    if not isinstance(data, dict) or "tables" not in data:
        raise LineageError(E_BAD_CATALOG, "catalog must contain a 'tables' object")
    tables = data["tables"]
    if not isinstance(tables, dict):
        raise LineageError(E_BAD_CATALOG, "catalog.tables must be an object")
    if len(tables) > MAX_BASE_TABLES:
        raise LineageError(E_LIMIT_EXCEEDED,
                           f"at most {MAX_BASE_TABLES} base tables are supported, "
                           f"found {len(tables)}")
    norm_tables: dict[str, dict[str, str]] = {}
    schema: dict[str, dict[str, str]] = {}
    for tname, cols in tables.items():
        if not isinstance(cols, dict) or not cols:
            raise LineageError(E_BAD_CATALOG,
                               f"table {tname!r} must map column names to SQLite types")
        if len(cols) > MAX_COLUMNS_PER_TABLE:
            raise LineageError(E_LIMIT_EXCEEDED,
                               f"table {tname!r} has {len(cols)} columns; limit is "
                               f"{MAX_COLUMNS_PER_TABLE}")
        lower_cols: dict[str, str] = {}
        schema[tname] = {}
        seen: set[str] = set()
        for cname, ctype in cols.items():
            key = cname.lower()
            if key in seen:
                raise LineageError(E_BAD_CATALOG,
                                   f"duplicate column {cname!r} (case-insensitive) in table "
                                   f"{tname!r}")
            seen.add(key)
            lower_cols[key] = cname
            schema[tname][cname] = ctype
        norm_tables[tname.lower()] = lower_cols

    functions = data.get("functions", {})
    if not isinstance(functions, dict):
        raise LineageError(E_BAD_CATALOG, "catalog.functions must be an object")
    norm_funcs: dict[str, dict[str, Any]] = {}
    for fname, decl in functions.items():
        if not isinstance(decl, dict) or decl.get("dependency") not in (
                "argument_dependent", "unknown"):
            raise LineageError(E_BAD_CATALOG,
                               f"function {fname!r} must declare dependency = "
                               "'argument_dependent' or 'unknown'")
        norm_funcs[fname.lower()] = dict(decl)
    return {
        "tables_lower": norm_tables,
        "schema": schema,
        "functions": norm_funcs,
        "raw": data,
    }


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def schema_fingerprint(schema: dict[str, dict[str, str]]) -> str:
    payload = json.dumps(schema, sort_keys=True, ensure_ascii=False)
    return _sha256(payload)


# --------------------------------------------------------------------------- #
# Builder
# --------------------------------------------------------------------------- #

class Builder:
    def __init__(self, catalog_path: str | os.PathLike, views_dir: str | os.PathLike,
                 out_dir: str | os.PathLike):
        self.catalog_path = Path(catalog_path)
        self.views_dir = Path(views_dir)
        self.out_dir = Path(out_dir)
        cat = load_catalog(self.catalog_path)
        self.tables = cat["tables_lower"]
        self.schema = cat["schema"]
        self.functions = cat["functions"]
        self.raw_catalog = cat["raw"]
        self.schema_fp = schema_fingerprint(self.schema)

        self.view_sql: dict[str, str] = {}          # lower name -> sql text
        self.view_display: dict[str, str] = {}
        self.view_files: dict[str, str] = {}
        self.analyses: dict[str, ViewAnalysis] = {}
        self.rels: dict[str, RelOutput] = {}
        self.failures: list[FailureReport] = []
        # Per-build resolution state.
        self._stack: list[str] = []
        self._load_views()

    # -- view discovery ----------------------------------------------------- #
    def _load_views(self) -> None:
        if not self.views_dir.exists():
            raise LineageError(E_BAD_CATALOG, f"views directory not found: {self.views_dir}")
        files = sorted(p for p in self.views_dir.iterdir()
                       if p.is_file() and p.suffix.lower() == ".sql")
        if len(files) > MAX_VIEWS:
            raise LineageError(E_LIMIT_EXCEEDED,
                               f"at most {MAX_VIEWS} views are supported, found {len(files)}")
        seen: set[str] = set()
        for p in files:
            name = p.stem
            key = name.lower()
            if key in seen:
                raise LineageError(E_BAD_CATALOG,
                                   f"duplicate view name {name!r} (case-insensitive)")
            seen.add(key)
            text = p.read_text(encoding="utf-8")
            self.view_sql[key] = text
            self.view_display[key] = name
            self.view_files[key] = p.name

    # -- persistence -------------------------------------------------------- #
    def _store_paths(self) -> dict[str, Path]:
        d = self.out_dir
        return {
            "manifest": d / "manifest.json",
            "schema": d / "schema.json",
            "functions": d / "functions.json",
            "views": d / "views",
            "failures": d / "failures.json",
            "graph": d / "dependency_graph.json",
        }

    def load_existing(self) -> dict[str, Any] | None:
        manifest = self._store_paths()["manifest"]
        if not manifest.exists():
            return None
        try:
            return json.loads(manifest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def save(self) -> None:
        paths = self._store_paths()
        paths["views"].mkdir(parents=True, exist_ok=True)
        manifest = {
            "catalog_version": CATALOG_VERSION,
            "tool": "local-column-lineage",
            "schema_fingerprint": self.schema_fp,
            "catalog_file": str(self.catalog_path),
            "views_dir": str(self.views_dir),
            "sql_dialect": "sqlite",
            "limits": {
                "max_views": MAX_VIEWS,
                "max_sources_per_view": MAX_SOURCES_PER_VIEW,
                "max_base_tables": MAX_BASE_TABLES,
                "max_columns_per_table": MAX_COLUMNS_PER_TABLE,
            },
            "views": {
                name: {
                    "file": a.sql_file,
                    "status": a.status,
                    "sql_sha256": a.sql_sha256,
                    "input_fingerprint": a.input_fingerprint,
                    "schema_fingerprint": a.schema_fingerprint,
                    "view_dependencies": a.view_dependencies,
                }
                for name, a in self.analyses.items()
            },
        }
        paths["manifest"].write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        paths["schema"].write_text(
            json.dumps(self.schema, indent=2, ensure_ascii=False), encoding="utf-8")
        paths["functions"].write_text(
            json.dumps(self.raw_catalog.get("functions", {}), indent=2,
                       ensure_ascii=False),
            encoding="utf-8")
        # One JSON document per view - the queryable per-view catalog.
        for old in paths["views"].glob("*.json"):
            old.unlink()
        for name, analysis in self.analyses.items():
            (paths["views"] / f"{name}.json").write_text(
                json.dumps(analysis.to_dict(), indent=2, ensure_ascii=False),
                encoding="utf-8")
        paths["failures"].write_text(
            json.dumps({"failures": [f.to_dict() for f in self.failures]},
                       indent=2, ensure_ascii=False),
            encoding="utf-8")
        paths["graph"].write_text(
            json.dumps(self._dependency_graph(), indent=2, ensure_ascii=False),
            encoding="utf-8")

    def _dependency_graph(self) -> dict[str, Any]:
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, str]] = []
        for tname, cols in self.schema.items():
            nodes.append({"kind": "table", "name": tname, "columns": list(cols)})
        for name, analysis in self.analyses.items():
            nodes.append({
                "kind": "view", "name": analysis.name_display,
                "status": analysis.status,
                "columns": [c.name_display for c in analysis.columns],
            })
            for dep in analysis.view_dependencies:
                edges.append({"from": analysis.name_display, "to": dep, "type": "view"})
            for obj in analysis.source_objects:
                if obj in self.tables:
                    edges.append({"from": analysis.name_display, "to": obj, "type": "table"})
        return {"nodes": nodes, "edges": edges}

    # -- build -------------------------------------------------------------- #
    def build(self, full: bool = False) -> dict[str, Any]:
        self.failures = []
        existing = None if full else self.load_existing()
        reusable: dict[str, ViewAnalysis] = {}
        if existing and existing.get("schema_fingerprint") == self.schema_fp:
            for name, meta in existing.get("views", {}).items():
                vp = self.out_dir / "views" / f"{name}.json"
                if not vp.exists():
                    continue
                try:
                    doc = json.loads(vp.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    continue
                if doc.get("sql_sha256") == _sha256(self.view_sql.get(name, "")):
                    reusable[name] = self._analysis_from_dict(doc)
        # Reusable = view whose SQL, schema and transitive inputs are unchanged.
        # The analysis *conclusion* is trusted, but each view is still walked
        # again so downstream scans have a live RelOutput (the walk is cheap
        # and deterministic at the contracted catalog size).
        reusable = {k: v for k, v in reusable.items() if k in self.view_sql}
        reusable_ok = {k for k, v in reusable.items() if v.status == "ok"}

        self.analyses: dict[str, ViewAnalysis] = {}
        self.rels = {}
        self._stack = []

        # Explicit cycle detection across view references, independent of the
        # recursion guard below, so the reported code is circular_view_reference.
        cycles = self._detect_cycles()
        cyclic: set[str] = set()
        for cycle in cycles:
            cyclic.update(cycle)
            chain = " -> ".join(self.view_display.get(n, n)
                                for n in cycle + [cycle[0]])
            for view_name in cycle:
                self.analyses[view_name] = ViewAnalysis(
                    name=view_name, name_display=self.view_display[view_name],
                    sql_file=self.view_files[view_name],
                    sql_text=self.view_sql[view_name],
                    sql_sha256=_sha256(self.view_sql[view_name]),
                    status="error",
                    error=FailureReport(
                        E_CYCLE, f"circular view reference detected: {chain}",
                        view=self.view_display[view_name],
                        sql_file=self.view_files[view_name],
                    ).to_dict(),
                    schema_fingerprint=self.schema_fp,
                )

        # Resolve every non-cyclic view; recursion naturally follows the
        # dependency order, so transitive inputs are always fresh. The stored
        # input fingerprints below make the dependency chain queryable and let
        # validate detect stale chains without re-running the analyzer.
        for name in sorted(self.view_sql):
            if name not in self.analyses:
                self._analyze_view(name)

        reused = 0
        # Enforce source-count limits and produce failure documents.
        for name, analysis in self.analyses.items():
            if name in reusable_ok and analysis.status == "ok" and name not in cyclic:
                reused += 1
            if analysis.status == "ok":
                distinct_sources = {o for o in analysis.source_objects}
                if len(distinct_sources) > MAX_SOURCES_PER_VIEW:
                    analysis.status = "error"
                    analysis.error = FailureReport(
                        E_LIMIT_EXCEEDED,
                        f"view references {len(distinct_sources)} sources; limit is "
                        f"{MAX_SOURCES_PER_VIEW}",
                        view=analysis.name_display, sql_file=analysis.sql_file,
                    ).to_dict()
            if analysis.error is not None:
                self.failures.append(FailureReport(
                    analysis.error["code"], analysis.error["message"],
                    view=name, sql_file=analysis.sql_file,
                    location=analysis.error.get("location"),
                    caused_by=analysis.error.get("caused_by"),
                ))
        self.save()
        return {
            "analyzed": len(self.analyses),
            "ok": sum(1 for a in self.analyses.values() if a.status == "ok"),
            "errors": sum(1 for a in self.analyses.values() if a.status == "error"),
            "reused": reused,
            "failures": [f.to_dict() for f in self.failures],
            "out_dir": str(self.out_dir),
        }

    def _input_fingerprint(self, view_name: str) -> str:
        parts = [_sha256(self.view_sql[view_name]), self.schema_fp]
        # Discover view deps from current SQL via a lightweight parse; the
        # full analysis records the same list once computed.
        existing = self.analyses.get(view_name)
        if existing is not None and existing.status == "ok":
            for dep in existing.view_dependencies:
                up = self.analyses.get(dep)
                parts.append(dep)
                parts.append(up.input_fingerprint if up and up.input_fingerprint else "missing")
        else:
            parts.extend(self._parse_view_dependencies(view_name))
        return _sha256("|".join(parts))

    def _parse_view_dependencies(self, view_name: str) -> list[str]:
        try:
            stmt = parse_select(self.view_sql[view_name], view_name)
        except LineageError:
            return []
        deps: list[str] = []
        for tbl in stmt.find_all(exp.Table):
            key = tbl.name.lower()
            if key in self.view_sql and key not in self.tables:
                deps.append(key)
        return sorted(set(deps))

    def _detect_cycles(self) -> list[list[str]]:
        graph: dict[str, list[str]] = {
            name: self._parse_view_dependencies(name) for name in self.view_sql
        }
        color: dict[str, int] = {n: 0 for n in graph}
        cycles: list[list[str]] = []

        def visit(node: str, stack: list[str]) -> None:
            color[node] = 1
            stack.append(node)
            for nxt in graph.get(node, []):
                if nxt not in color:
                    continue
                if color[nxt] == 1:
                    idx = stack.index(nxt)
                    cycles.append(stack[idx:])
                elif color[nxt] == 0:
                    visit(nxt, stack)
            stack.pop()
            color[node] = 2

        for n in sorted(graph):
            if color[n] == 0:
                visit(n, [])
        return cycles

    # -- per-view analysis -------------------------------------------------- #
    def _analyze_view(self, name: str, force: bool = False) -> ViewAnalysis:
        if name in self.analyses and not force:
            return self.analyses[name]
        if name in self._stack:
            cycle = " -> ".join(self.view_display.get(n, n) for n in self._stack + [name])
            err = LineageError(E_CYCLE, f"circular view reference detected: {cycle}")
            failed = self._failure_analysis(name, err)
            self.analyses[name] = failed
            return failed

        sql = self.view_sql.get(name)
        if sql is None:
            err = LineageError(E_MISSING_VIEW_FILE,
                               f"view {name!r} is referenced but has no SQL file")
            failed = self._failure_analysis(name, err)
            self.analyses[name] = failed
            return failed

        self._stack.append(name)
        analysis = ViewAnalysis(
            name=name, name_display=self.view_display[name],
            sql_file=self.view_files[name], sql_text=sql,
            sql_sha256=_sha256(sql), status="ok",
        )
        try:
            stmt = parse_select(sql, name)
            analyzer = Analyzer(
                self.tables, self.functions, self._resolve_view_ref, name,
                self.view_files[name],
            )
            rel = analyzer.analyze_select(stmt)
            self.rels[name] = rel
            analysis.columns = [self._to_column_analysis(c) for c in rel.columns]
            analysis.source_objects = sorted({
                b.object_name for b in rel.bindings if b.object_name
            })
            analysis.source_roles = [
                {
                    "alias": b.alias_display,
                    "object": b.object_display,
                    "kind": b.kind,
                    "location": b.role_location.to_dict() if b.role_location else None,
                }
                for b in rel.bindings
            ]
            analysis.view_dependencies = sorted(
                o for o in analysis.source_objects if o in self.view_sql
            )
            analysis.cte_names = sorted({
                b.object_name for b in rel.bindings if b.kind == "cte"
            })
        except LineageError as exc:
            failed = self._failure_analysis(name, exc)
            self._stack.pop()
            self.analyses[name] = failed
            return failed
        finally:
            if self._stack and self._stack[-1] == name:
                self._stack.pop()

        analysis.schema_fingerprint = self.schema_fp
        analysis.input_fingerprint = self._input_fingerprint(name)
        self.analyses[name] = analysis
        return analysis

    def _resolve_view_ref(self, name: str, location: SourceLocation | None,
                          parent_view: str) -> RelOutput:
        if name in self.tables:
            raise LineageError(E_MISSING_OBJECT,
                               f"{name!r} is a base table, not a view/CTE", location,
                               parent_view)
        if name not in self.view_sql:
            raise LineageError(E_MISSING_OBJECT,
                               f"referenced object {name!r} is neither a catalog table, "
                               "a CTE nor a known view", location, parent_view)
        upstream = self._analyze_view(name)
        if upstream.status == "error":
            raise LineageError(
                E_CASCADED,
                f"upstream view {upstream.name_display!r} is invalid, so this view cannot be "
                "analyzed (no dependencies are guessed)",
                location, parent_view,
            )
        # Hand back an isolated copy: the analyzer wraps/prefixes scan columns
        # in place, and the cached upstream RelOutput may be scanned by several
        # downstream views (or several aliases) and must never be mutated.
        return _copy_rel(self.rels[name])

    def _failure_analysis(self, name: str, exc: LineageError,
                          caused_by: dict[str, Any] | None = None) -> ViewAnalysis:
        report = FailureReport(
            code=exc.code, message=exc.message,
            view=self.view_display.get(name, name),
            sql_file=self.view_files.get(name),
            location=exc.location.to_dict() if exc.location else None,
            caused_by=caused_by,
        ).to_dict()
        return ViewAnalysis(
            name=name, name_display=self.view_display.get(name, name),
            sql_file=self.view_files.get(name, ""),
            sql_text=self.view_sql.get(name, ""),
            sql_sha256=_sha256(self.view_sql.get(name, "")),
            status="error", error=report,
            schema_fingerprint=self.schema_fp,
        )

    def _to_column_analysis(self, col) -> ColumnAnalysis:
        return ColumnAnalysis(
            name=col.name, name_display=col.name_display,
            location=col.location, name_location=col.name_location,
            value_deps=dedupe_deps(col.value),
            control_deps=dedupe_deps(col.control),
            unknown=col.unknown,
            unknown_reasons=col.unknown_reasons,
            nullable_sources=col.nullable_sources,
        )

    def _analysis_from_dict(self, doc: dict[str, Any]) -> ViewAnalysis:
        """Reconstruct a reusable analysis, including all column dependencies."""
        columns = [
            ColumnAnalysis(
                name=c["name"].lower(), name_display=c["name"],
                location=_loc(c.get("location")),
                name_location=_loc(c.get("name_location")),
                value_deps=[_dep_from_dict(d) for d in c.get("value_sources", [])],
                control_deps=[_dep_from_dict(d) for d in c.get("control_sources", [])],
                unknown=c.get("unknown", False),
                unknown_reasons=list(c.get("unknown_reasons", [])),
                nullable_sources=list(c.get("nullable_sources", [])),
            )
            for c in doc.get("columns", [])
        ]
        return ViewAnalysis(
            name=doc["name"].lower(), name_display=doc["name"],
            sql_file=doc["sql_file"], sql_text="",
            sql_sha256=doc["sql_sha256"], status=doc["status"],
            columns=columns,
            source_objects=doc.get("source_objects", []),
            source_roles=doc.get("source_roles", []),
            view_dependencies=doc.get("view_dependencies", []),
            cte_names=doc.get("cte_names", []),
            input_fingerprint=doc.get("input_fingerprint"),
            schema_fingerprint=doc.get("schema_fingerprint"),
            error=doc.get("error"),
        )


def _loc(doc: dict[str, Any] | None) -> SourceLocation | None:
    if not doc:
        return None
    return SourceLocation(line=doc["line"], col=doc["col"],
                          start=doc["start"], end=doc["end"])


def _dep_from_dict(doc: dict[str, Any]):
    base_doc = doc["base"]
    base = BaseRef(
        table=base_doc["table"].lower(), column=base_doc["column"].lower(),
        table_display=base_doc["table"], column_display=base_doc["column"],
    )
    hops = tuple(
        Hop(
            from_view=h["from_view"].lower() if h["from_view"] else "",
            from_view_display=h["from_view_display"],
            from_column=h["from_column"].lower() if h["from_column"] else "",
            from_column_display=h["from_column_display"],
            to_object=h["to_object"].lower() if h["to_object"] else "",
            to_object_display=h["to_object_display"],
            alias=h["alias"].lower() if h["alias"] else "",
            alias_display=h["alias_display"],
            to_column=h["to_column"].lower() if h["to_column"] else None,
            to_column_display=h["to_column_display"],
            kind=h["kind"],
        )
        for h in doc.get("path", [])
    )
    return Dep(
        kind=doc["kind"], base=base, reason=doc["reason"], path=hops,
        base_alias=doc["base_alias"].lower(),
        base_alias_display=doc["base_alias"],
    )


def _copy_rel(rel: RelOutput) -> RelOutput:
    """Deep-copy a cached relation so downstream scans cannot mutate it."""
    from .analyzer import Binding, ResolvedColumn

    def copy_col(c: ResolvedColumn) -> ResolvedColumn:
        return ResolvedColumn(
            name=c.name, name_display=c.name_display,
            value=list(c.value), control=list(c.control),
            unknown=c.unknown, unknown_reasons=list(c.unknown_reasons),
            nullable_sources=list(c.nullable_sources),
            location=c.location, name_location=c.name_location,
        )

    copy = RelOutput(
        columns=[copy_col(c) for c in rel.columns],
        row_control=list(rel.row_control),
        row_unknown=rel.row_unknown,
        row_unknown_reasons=list(rel.row_unknown_reasons),
        using_merged=set(rel.using_merged),
    )
    copy.bindings = [
        Binding(b.alias, b.alias_display, b.kind, b.object_name, b.object_display,
                copy, b.role_location)
        for b in rel.bindings
    ]
    return copy

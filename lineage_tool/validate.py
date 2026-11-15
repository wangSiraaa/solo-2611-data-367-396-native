"""Validation of a built catalog against the current catalog.json / views."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .builder import (
    MAX_BASE_TABLES, MAX_COLUMNS_PER_TABLE, MAX_SOURCES_PER_VIEW, MAX_VIEWS,
    _sha256, load_catalog, schema_fingerprint,
)
from .errors import E_CYCLE, E_MISSING_OBJECT, E_STALE_SCHEMA, FailureReport


def validate(catalog_path: str | Path, views_dir: str | Path,
             out_dir: str | Path) -> dict[str, Any]:
    catalog_path, views_dir, out_dir = Path(catalog_path), Path(views_dir), Path(out_dir)
    problems: list[dict[str, Any]] = []

    manifest_path = out_dir / "manifest.json"
    if not manifest_path.exists():
        return {
            "valid": False,
            "problems": [FailureReport(
                "no_catalog", "no built catalog found; run build first",
            ).to_dict()],
        }
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    # 1) Schema drift.
    try:
        cat = load_catalog(catalog_path)
    except Exception as exc:  # catalog itself invalid
        problems.append(FailureReport(
            getattr(exc, "code", "invalid_catalog"), str(exc),
        ).to_dict())
        return {"valid": False, "problems": problems}

    current_schema_fp = schema_fingerprint(cat["schema"])
    if manifest.get("schema_fingerprint") != current_schema_fp:
        problems.append(FailureReport(
            E_STALE_SCHEMA,
            "catalog schema changed since the lineage catalog was built; rebuild required",
        ).to_dict())

    # 2) View files: added / removed / modified.
    current_views = {p.stem.lower(): p for p in views_dir.glob("*.sql")}
    recorded = manifest.get("views", {})
    for name, path in current_views.items():
        text = path.read_text(encoding="utf-8")
        if name not in recorded:
            problems.append(FailureReport(
                "new_view", f"view file {path.name} is not part of the built catalog",
                view=name, sql_file=path.name,
            ).to_dict())
        elif recorded[name]["sql_sha256"] != _sha256(text):
            problems.append(FailureReport(
                "stale_view_sql",
                f"view {path.name} changed since analysis; rebuild required",
                view=name, sql_file=path.name,
            ).to_dict())
    for name in recorded:
        if name not in current_views:
            problems.append(FailureReport(
                "missing_view_file",
                f"analyzed view {name!r} has no SQL file anymore", view=name,
            ).to_dict())

    # 3) Cross-view graph: missing base objects and cycles.
    view_docs: dict[str, dict[str, Any]] = {}
    for p in (out_dir / "views").glob("*.json"):
        view_docs[p.stem] = json.loads(p.read_text(encoding="utf-8"))

    known_objects = set(cat["tables_lower"]) | set(view_docs)
    for name, doc in view_docs.items():
        for obj in doc.get("source_objects", []):
            if obj not in known_objects:
                problems.append(FailureReport(
                    E_MISSING_OBJECT,
                    f"view {doc['name']} references unknown object {obj!r}",
                    view=doc["name"], sql_file=doc.get("sql_file"),
                ).to_dict())
        if doc.get("status") == "error":
            problems.append(FailureReport(
                doc["error"]["code"] if doc.get("error") else "analysis_error",
                doc["error"]["message"] if doc.get("error") else "analysis failed",
                view=doc["name"], sql_file=doc.get("sql_file"),
                location=doc["error"].get("location") if doc.get("error") else None,
            ).to_dict())

    cycle = _find_cycle(view_docs)
    if cycle:
        problems.append(FailureReport(
            E_CYCLE, "circular view reference: " + " -> ".join(cycle),
        ).to_dict())

    # 4) Contract limits.
    if len(view_docs) > MAX_VIEWS:
        problems.append(FailureReport("limit", f"more than {MAX_VIEWS} views").to_dict())
    if len(cat["tables_lower"]) > MAX_BASE_TABLES:
        problems.append(FailureReport("limit", f"more than {MAX_BASE_TABLES} tables").to_dict())
    for tname, cols in cat["tables_lower"].items():
        if len(cols) > MAX_COLUMNS_PER_TABLE:
            problems.append(FailureReport(
                "limit", f"table {tname} exceeds {MAX_COLUMNS_PER_TABLE} columns").to_dict())
    for name, doc in view_docs.items():
        if len(set(doc.get("source_objects", []))) > MAX_SOURCES_PER_VIEW:
            problems.append(FailureReport(
                "limit",
                f"view {doc['name']} exceeds {MAX_SOURCES_PER_VIEW} sources").to_dict())

    return {"valid": not problems, "problems": problems,
            "view_count": len(view_docs), "table_count": len(cat["tables_lower"])}


def _find_cycle(docs: dict[str, dict[str, Any]]) -> list[str] | None:
    color: dict[str, int] = {n: 0 for n in docs}  # 0 white, 1 gray, 2 black
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        color[node] = 1
        stack.append(node)
        for nxt in docs[node].get("view_dependencies", []):
            if nxt not in color:
                continue
            if color[nxt] == 1:
                idx = stack.index(nxt)
                return stack[idx:] + [nxt]
            found = visit(nxt)
            if found:
                return found
        stack.pop()
        color[node] = 2
        return None

    for n in sorted(docs):
        if color[n] == 0:
            found = visit(n)
            if found:
                return [docs[x]["name"] for x in found]
    return None

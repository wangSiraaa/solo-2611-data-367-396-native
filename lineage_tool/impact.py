"""Change-impact classification over a built lineage catalog.

Buckets (kept strictly separate, per the contract):

* ``exact``      - resolvable views without any unknown-function taint that
                   mention a changed base column through value/control deps
* ``uncertain``  - outputs tainted by an ``unknown`` opaque function, or
                   downstream of an invalid view; never counted as exact hit
                   and never as exactly unaffected
* ``unaffected`` - resolvable, untainted outputs provably independent of the
                   changed base columns
* ``invalid``    - drop/type_change bindings that stop being valid

A view is placed in exactly one bucket: if any of its columns is unknown, the
view-level answer is uncertain even when other columns have precise hits. The
per-column records still expose every precise path, so nothing is hidden.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_view_docs(out_dir: str | Path) -> dict[str, dict[str, Any]]:
    views_dir = Path(out_dir) / "views"
    docs: dict[str, dict[str, Any]] = {}
    if not views_dir.exists():
        return docs
    for p in sorted(views_dir.glob("*.json")):
        docs[p.stem] = json.loads(p.read_text(encoding="utf-8"))
    return docs


def _norm_ref(ref: str) -> str:
    if "." not in ref:
        raise ValueError(f"base column reference must be table.column, got {ref!r}")
    table, column = ref.split(".", 1)
    return f"{table.lower()}.{column.lower()}"


def _dep_matches(dep: dict[str, Any], targets: set[str]) -> bool:
    return dep["base"]["id"] in targets


def _path_explain(dep: dict[str, Any]) -> dict[str, Any]:
    return {
        "base": dep["base"],
        "reason": dep["reason"],
        "dependency_kind": dep["kind"],
        "base_alias": dep["base_alias"],
        "path": [
            {
                "view": hop["from_view_display"],
                "output_column": hop["from_column_display"],
                "via_object": hop["to_object_display"],
                "via_alias": hop["alias_display"],
                "input_column": hop["to_column_display"],
                "hop_kind": hop["kind"],
            }
            for hop in dep["path"]
        ],
    }


def _all_base_ids(docs: dict[str, dict[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for doc in docs.values():
        for col in doc.get("columns", []):
            for group in ("value_sources", "control_sources"):
                for dep in col.get(group, []):
                    ids.add(dep["base"]["id"])
    return ids


def analyze_impact(
    out_dir: str | Path,
    base_refs: list[str],
    change_kind: str,
    base_tables: list[str] | None = None,
) -> dict[str, Any]:
    if change_kind not in ("value_change", "drop", "type_change"):
        raise ValueError(
            f"change_kind must be value_change, drop or type_change, got {change_kind!r}")
    docs = _load_view_docs(out_dir)
    target_cols = {_norm_ref(r) for r in base_refs}
    target_tables = {t.lower() for t in (base_tables or [])}
    all_targets = set(target_cols)
    for t in target_tables:
        all_targets.update(cid for cid in _all_base_ids(docs) if cid.startswith(t + "."))

    result: dict[str, Any] = {
        "change_kind": change_kind,
        "changed_base_columns": sorted(target_cols),
        "changed_base_tables": sorted(target_tables),
        "exact": {},
        "uncertain": {},
        "unaffected": {},
        "invalid": [],
        "summary": {},
    }

    for name, doc in docs.items():
        entry = _impact_view(doc, all_targets)
        result[entry.pop("_bucket")][name] = entry

    if change_kind in ("drop", "type_change"):
        result["invalid"] = _binding_invalidity(
            docs, target_cols, target_tables, change_kind)

    result["summary"] = {
        "exact_views": len(result["exact"]),
        "uncertain_views": len(result["uncertain"]),
        "unaffected_views": len(result["unaffected"]),
        "invalid_bindings": len(result["invalid"]),
    }
    return result


def _impact_view(doc: dict[str, Any], targets: set[str]) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "view": doc["name"],
        "status": doc["status"],
        "columns": [],
    }
    if doc["status"] == "error":
        entry["note"] = ("view fails analysis; dependencies cannot be proven complete, "
                         "reported as uncertain rather than dependency-free")
        entry["_bucket"] = "uncertain"
        return entry

    view_has_unknown = False
    view_has_hit = False
    for col in doc.get("columns", []):
        col_entry = _impact_column(col, targets)
        entry["columns"].append(col_entry)
        if col_entry["unknown"]:
            view_has_unknown = True
        if col_entry["affected"]:
            view_has_hit = True

    if view_has_unknown:
        entry["note"] = ("at least one output depends on an opaque unknown function; "
                         "completeness cannot be guaranteed")
        entry["_bucket"] = "uncertain"
    elif view_has_hit:
        entry["_bucket"] = "exact"
    else:
        entry["_bucket"] = "unaffected"
    return entry


def _impact_column(col: dict[str, Any], targets: set[str]) -> dict[str, Any]:
    value_hits = [
        _path_explain(d) for d in col.get("value_sources", [])
        if _dep_matches(d, targets)
    ]
    control_hits = [
        _path_explain(d) for d in col.get("control_sources", [])
        if _dep_matches(d, targets)
    ]
    return {
        "column": col["name"],
        "precision": col["precision"],
        "unknown": bool(col.get("unknown")),
        "unknown_reasons": col.get("unknown_reasons", []),
        "value_change_paths": value_hits,
        "control_change_paths": control_hits,
        "affected": bool(value_hits or control_hits),
        "location": col.get("location"),
    }


def _binding_invalidity(docs: dict[str, dict[str, Any]], target_cols: set[str],
                        target_tables: set[str], change_kind: str) -> list[dict[str, Any]]:
    """Locate the first binding level at which each view becomes invalid."""
    out: list[dict[str, Any]] = []
    for name, doc in sorted(docs.items()):
        if doc["status"] == "error":
            out.append({
                "view": doc["name"],
                "level": "view",
                "kind": change_kind,
                "reason": "view already fails analysis",
                "error": doc.get("error"),
            })
            continue
        level = _first_invalid_level(doc, target_cols, target_tables)
        if level is not None:
            out.append({"view": doc["name"], "kind": change_kind, **level})
    return out


def _first_invalid_level(doc: dict[str, Any], target_cols: set[str],
                         target_tables: set[str]) -> dict[str, Any] | None:
    # 1) A whole source table dropped -> FROM binding invalid.
    for role in doc.get("source_roles", []):
        if role["object"].lower() in target_tables:
            return {
                "level": "from_binding",
                "reason": "bound source table is dropped/type-changed at table level",
                "binding": role,
            }
    # 2) A referenced column dropped/type-changed -> column binding invalid.
    hits: list[dict[str, Any]] = []
    for col in doc.get("columns", []):
        for group in ("value_sources", "control_sources"):
            for dep in col.get(group, []):
                if dep["base"]["id"] in target_cols:
                    first_hop = dep["path"][0] if dep["path"] else None
                    hits.append({
                        "level": "column_binding",
                        "reason": "bound base column no longer exists or its type changed",
                        "column": col["name"],
                        "binding": {
                            "object": first_hop["to_object_display"] if first_hop else None,
                            "alias": first_hop["alias_display"] if first_hop else None,
                        },
                        "base": dep["base"],
                        "dependency_kind": dep["kind"],
                        "path": _path_explain(dep)["path"],
                    })
    if hits:
        first = dict(hits[0])
        first["all_invalid_bindings"] = hits
        return first
    return None

"""Command line interface: build | lineage | impact | validate."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .builder import Builder
from .impact import analyze_impact
from .validate import validate as run_validate


def _print(payload: Any, as_json: bool) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _load_view_doc(out_dir: Path, view: str) -> dict[str, Any]:
    path = out_dir / "views" / f"{view.lower()}.json"
    if not path.exists():
        raise SystemExit(f"view {view!r} not found in catalog {out_dir}")
    return json.loads(path.read_text(encoding="utf-8"))


def cmd_build(args: argparse.Namespace) -> int:
    builder = Builder(args.catalog, args.views, args.out)
    summary = builder.build(full=args.full)
    _print(summary, True)
    return 1 if summary["errors"] else 0


def cmd_lineage(args: argparse.Namespace) -> int:
    doc = _load_view_doc(Path(args.out), args.view)
    if doc["status"] != "ok":
        payload = {"view": doc["name"], "status": "error", "error": doc.get("error")}
        _print(payload, True)
        return 2
    columns = {c["name"].lower(): c for c in doc["columns"]}
    if args.column:
        key = args.column.lower()
        if key not in columns:
            raise SystemExit(
                f"column {args.column!r} not found in view {args.view}; "
                f"columns: {[c['name'] for c in doc['columns']]}")
        payload = {"view": doc["name"], "column": columns[key]}
    else:
        payload = {
            "view": doc["name"],
            "status": doc["status"],
            "sql_file": doc["sql_file"],
            "sql_sha256": doc["sql_sha256"],
            "source_roles": doc["source_roles"],
            "columns": doc["columns"],
        }
    _print(payload, True)
    return 0


def cmd_impact(args: argparse.Namespace) -> int:
    refs = [r for r in (args.base_column or [])]
    tables = args.base_table or []
    if not refs and not tables:
        raise SystemExit("impact requires at least one --base-column table.column "
                         "or --base-table table")
    payload = analyze_impact(args.out, refs, args.change_kind, tables)
    _print(payload, True)
    # Non-zero when exact or invalid effects exist? Keep exit 0 for queries;
    # only usage errors raise. Catalog consumers parse the JSON.
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    report = run_validate(args.catalog, args.views, args.out)
    _print(report, True)
    return 0 if report["valid"] else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lineage",
        description="Local column-level lineage and change-impact tool (SQLite SELECT subset)")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("build", help="analyze views and write the persistent catalog")
    p.add_argument("--catalog", required=True)
    p.add_argument("--views", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--full", action="store_true",
                   help="ignore any previous analysis and rebuild everything")
    p.set_defaults(func=cmd_build)

    p = sub.add_parser("lineage", help="query value/control sources of a view column")
    p.add_argument("--out", required=True)
    p.add_argument("--view", required=True)
    p.add_argument("--column", help="output column; omit for the whole view")
    p.set_defaults(func=cmd_lineage)

    p = sub.add_parser("impact", help="impact of a base-column/table change")
    p.add_argument("--out", required=True)
    p.add_argument("--base-column", action="append",
                   help="table.column (repeatable); case-insensitive")
    p.add_argument("--base-table", action="append",
                   help="table name for whole-table drop (repeatable)")
    p.add_argument("--change-kind", required=True,
                   choices=["value_change", "drop", "type_change"])
    p.set_defaults(func=cmd_impact)

    p = sub.add_parser("validate", help="check catalog freshness, cycles and missing objects")
    p.add_argument("--catalog", required=True)
    p.add_argument("--views", required=True)
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_validate)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

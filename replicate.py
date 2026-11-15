#!/usr/bin/env python3
"""Command line front end for the two-replica causal row replicator.

    python3 replicate.py --db A.sqlite write r1 \
        --state '{"name":"Alice","qty":10}' --expected-heads seed:1
    python3 replicate.py --db A.sqlite export-events --out a.events.json
    python3 replicate.py --db B.sqlite receive a.events.json

Run ``python3 replicate.py --help`` for all subcommands.  Every command except
``init`` operates on one SQLite file only; events travel through files.
"""

import argparse
import json
import sys
from pathlib import Path

from sync_tool.engine import (ConflictError, Engine, EventError)

EXIT_OK, EXIT_USAGE, EXIT_CONFLICT, EXIT_EVENT = 0, 1, 2, 3


def _emit(obj):
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2))

def _parse_heads(args):
    heads = []
    if getattr(args, "expected_heads_json", None) is not None:
        parsed = json.loads(args.expected_heads_json)
        if not isinstance(parsed, list):
            raise ValueError("--expected-heads-json must be a JSON list")
        heads.extend(parsed)
    for value in getattr(args, "expected_heads", None) or []:
        if value == "":
            continue
        heads.extend(p.strip() for p in value.split(",") if p.strip())
    if args.request:
        req = json.loads(Path(args.request).read_text(encoding="utf-8"))
        heads = list(req.get("expected_heads", req.get("parents", heads)))
    return heads


def _load_request(args):
    if not getattr(args, "request", None):
        return None
    return json.loads(Path(args.request).read_text(encoding="utf-8"))


def _engine(args):
    trace = None
    if getattr(args, "trace_sql", None):
        fh = open(args.trace_sql, "a", encoding="utf-8")

        def trace(sql):
            fh.write(sql + "\n")
            fh.flush()

        return Engine(args.db, trace=trace), fh
    return Engine(args.db), None


def cmd_init(args):
    Engine.init(args.db, args.replica)
    _emit({"initialized": args.db, "replica": args.replica})


def cmd_prepare(args):
    Engine.prepare(args.db)
    eng = Engine(args.db)
    _emit({"prepared": args.db, "replica": eng.replica(),
           "snapshot": eng.state_snapshot()})


def _report_conflict(exc):
    _emit({"ok": False, "error": "conflict", "code": exc.code,
           "reason": exc.reason, "detail": exc.detail})
    return EXIT_CONFLICT


def _local_op(args, kind):
    request = _load_request(args)
    row_id = args.row if getattr(args, "row", None) else request["row_id"]
    if kind == "delete":
        state = None
    elif getattr(args, "state_json", None) is not None:
        state = json.loads(args.state_json)
    elif request is not None and "state" in request:
        state = request["state"]
    else:
        state = None
    heads = _parse_heads(args)
    engine, fh = _engine(args)
    try:
        ev = engine.local_write(
            row_id, state, heads, kind=kind,
            fail_before_outbox=getattr(args, "fail_before_outbox", False),
            fail_before_commit=getattr(args, "fail_before_commit", False),
            crash_before_outbox=getattr(args, "crash_before_outbox", False),
            crash_before_commit=getattr(args, "crash_before_commit", False),
            op_id=getattr(args, "op_id", None))
        snap = engine.query_row(row_id)
        _emit({"ok": True, "event": ev, "version_id": ev["version_id"],
               "origin": ev["origin"], "seq": ev["seq"],
               "parents": ev["parents"], "row": snap})
        return EXIT_OK
    except ConflictError as exc:
        return _report_conflict(exc)
    finally:
        if fh:
            fh.close()


def cmd_write(args):
    return _local_op(args, "write")


def cmd_delete(args):
    return _local_op(args, "delete")


def cmd_resolve(args):
    if args.deleted:
        args.state_json = None
    elif args.state_json is None and not args.request:
        raise SystemExit("resolve needs --state JSON or --deleted")
    return _local_op(args, "resolve")


def cmd_export_events(args):
    engine, fh = _engine(args)
    try:
        only = args.only or None
        result = engine.export_events(args.out, replica_id=args.replica_id,
                                      version_ids=only)
        _emit(result)
        return EXIT_OK
    finally:
        if fh:
            fh.close()


def cmd_receive(args):
    engine, fh = _engine(args)
    try:
        receipts = []
        for path in args.files:
            receipts.extend(engine.receive_file(
                path, fail_after_persist=args.fail_after_persist,
                crash_after_persist=args.crash_after_persist))
        summary = {"ok": True, "receipts": receipts,
                   "counts": {}}
        for r in receipts:
            summary["counts"][r["outcome"]] = \
                summary["counts"].get(r["outcome"], 0) + 1
        _emit(summary)
        if args.strict and any(r["outcome"] == "rejected" for r in receipts):
            return EXIT_EVENT
        return EXIT_OK
    except EventError as exc:
        _emit({"ok": False, "error": "event", "code": exc.code,
               "reason": exc.reason})
        return EXIT_EVENT
    finally:
        if fh:
            fh.close()


def cmd_query_row(args):
    engine, fh = _engine(args)
    try:
        _emit(engine.query_row(args.row))
        return EXIT_OK
    finally:
        if fh:
            fh.close()


def cmd_export_data(args):
    engine, fh = _engine(args)
    try:
        data = engine.export_data()
        if args.out:
            Path(args.out).write_text(
                json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2)
                + "\n", encoding="utf-8")
        _emit(data)
        return EXIT_OK
    finally:
        if fh:
            fh.close()


def cmd_audit(args):
    engine, fh = _engine(args)
    try:
        report = engine.audit()
        report["history"] = engine.log_history()
        if args.out:
            Path(args.out).write_text(
                json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2)
                + "\n", encoding="utf-8")
        _emit(report)
        return EXIT_OK
    finally:
        if fh:
            fh.close()


def _probe_opts(p):
    p.add_argument("--op-id", metavar="TOKEN",
                   help="idempotency token; a retried commit returns the "
                        "original version")
    p.add_argument("--fail-before-outbox", action="store_true")
    p.add_argument("--fail-before-commit", action="store_true")
    p.add_argument("--crash-before-outbox", action="store_true",
                   help="hard-exit the process mid-transaction (probe)")
    p.add_argument("--crash-before-commit", action="store_true",
                   help="hard-exit the process mid-transaction (probe)")
    p.add_argument("--trace-sql", metavar="FILE")


def _heads_opts(p):
    p.add_argument("--expected-heads", action="append", metavar="ID[,ID]",
                   help="observed current head version id(s); repeatable")
    p.add_argument("--expected-heads-json", metavar="JSON",
                   help='JSON list, e.g. \'["A:1","B:1"]\'')
    p.add_argument("--expected-heads-file", metavar="FILE",
                   help="JSON file containing the head list")
    p.add_argument("--request", metavar="FILE",
                   help="request JSON with row_id/state/parents")


def build_parser():
    p = argparse.ArgumentParser(
        prog="replicate.py", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init", help="create a fresh replica database")
    s.add_argument("--db", required=True)
    s.add_argument("--replica", required=True, choices=["A", "B"])
    s.set_defaults(func=cmd_init)

    s = sub.add_parser(
        "prepare", help="extend a copied seed database with replication tables")
    s.add_argument("--db", required=True)
    s.set_defaults(func=cmd_prepare)

    s = sub.add_parser("write", help="local write (mode=write)")
    s.add_argument("--db", required=True)
    s.add_argument("row", nargs="?")
    s.add_argument("--state", "--state-json", dest="state_json")
    _heads_opts(s)
    _probe_opts(s)
    s.set_defaults(func=cmd_write)

    s = sub.add_parser("delete", help="local tombstone (mode=delete)")
    s.add_argument("--db", required=True)
    s.add_argument("row", nargs="?")
    _heads_opts(s)
    _probe_opts(s)
    s.set_defaults(func=cmd_delete)

    s = sub.add_parser("resolve", help="explicit resolution of the exact heads")
    s.add_argument("--db", required=True)
    s.add_argument("row", nargs="?")
    s.add_argument("--state", "--state-json", dest="state_json")
    s.add_argument("--deleted", action="store_true",
                   help="resolve to a tombstone")
    _heads_opts(s)
    _probe_opts(s)
    s.set_defaults(func=cmd_resolve)

    s = sub.add_parser("export-events", help="write local outbox events to a file")
    s.add_argument("--db", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--only", action="append", metavar="VERSION_ID",
                   help="export only the given version id; repeatable")
    s.add_argument("--replica-id", default=None)
    s.add_argument("--trace-sql", metavar="FILE")
    s.set_defaults(func=cmd_export_events)

    s = sub.add_parser("receive", help="apply event file(s) to this replica")
    s.add_argument("--db", required=True)
    s.add_argument("files", nargs="+")
    s.add_argument("--strict", action="store_true",
                   help="exit 3 if any item was rejected")
    s.add_argument("--fail-after-persist", action="store_true")
    s.add_argument("--crash-after-persist", action="store_true")
    s.add_argument("--trace-sql", metavar="FILE")
    s.set_defaults(func=cmd_receive)

    s = sub.add_parser("query-row", help="report absent/resolved/deleted/conflicted")
    s.add_argument("--db", required=True)
    s.add_argument("row")
    s.add_argument("--trace-sql", metavar="FILE")
    s.set_defaults(func=cmd_query_row)

    s = sub.add_parser("export-data", help="normal rows plus conflict report")
    s.add_argument("--db", required=True)
    s.add_argument("--out")
    s.add_argument("--trace-sql", metavar="FILE")
    s.set_defaults(func=cmd_export_data)

    s = sub.add_parser("audit", help="persisted evidence and consistency report")
    s.add_argument("--db", required=True)
    s.add_argument("--out")
    s.add_argument("--trace-sql", metavar="FILE")
    s.set_defaults(func=cmd_audit)

    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "expected_heads_file", None):
        args.expected_heads_json = Path(
            args.expected_heads_file).read_text(encoding="utf-8")
    try:
        return args.func(args)
    except ConflictError as exc:
        return _report_conflict(exc)
    except EventError as exc:
        _emit({"ok": False, "error": "event", "code": exc.code,
               "reason": exc.reason})
        return EXIT_EVENT
    except (ValueError, KeyError, FileNotFoundError, json.JSONDecodeError) as exc:
        _emit({"ok": False, "error": "usage", "reason": str(exc)})
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())

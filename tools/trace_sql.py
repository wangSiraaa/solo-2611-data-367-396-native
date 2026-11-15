#!/usr/bin/env python3
"""Produce a real SQL-write transcript for one write and one receive.

    python3 tools/trace_sql.py [--workdir trace]

Writes ``A-write.sql``, ``B-receive.sql`` and ``trace-summary.json``.  The
transcript contains every SQL statement SQLite actually executed (via
``Connection.set_trace_callback``), which shows the new version, the business
update, the outbox row and the local seq bump landing inside one
``BEGIN IMMEDIATE``/``COMMIT``, and that reception writes no outbox row.
"""

import argparse
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
import sys

sys.path.insert(0, str(ROOT))

from sync_tool.engine import Engine, canonical  # noqa: E402


def capture(db, fn, out_path):
    lines = []
    engine = Engine(db, trace=lambda sql: lines.append(sql))
    result = fn(engine)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return result, lines


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default="trace")
    args = ap.parse_args(argv)
    work = (ROOT / args.workdir).resolve()
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    for name in ("A", "B"):
        shutil.copy(ROOT / ("seed-%s.sqlite" % name), work / ("%s.sqlite" % name))
        Engine.prepare(work / ("%s.sqlite" % name))

    event, write_sql = capture(
        work / "A.sqlite",
        lambda eng: eng.local_write("r1", {"name": "Alice", "qty": 10},
                                    ["seed:1"]),
        work / "A-write.sql")

    def do_receive(eng):
        return eng.receive([event])

    receipts, receive_sql = capture(
        work / "B.sqlite", do_receive, work / "B-receive.sql")

    A, B = Engine(work / "A.sqlite"), Engine(work / "B.sqlite")
    summary = {
        "event": event,
        "A_hexdump_sql": write_sql,
        "B_receive_sql": receive_sql,
        "A_snapshot": A.state_snapshot(),
        "B_snapshot": B.state_snapshot(),
        "B_receipts": receipts,
        "checks": {
            "begin_immediate_in_write": any(
                "BEGIN IMMEDIATE" in s for s in write_sql),
            "outbox_insert_in_write": any(
                "INSERT INTO source_outbox" in s for s in write_sql),
            "items_upsert_in_write": any(
                "INSERT INTO items" in s for s in write_sql),
            "meta_seq_bump_in_write": any(
                "UPDATE meta SET seq" in s for s in write_sql),
            "no_outbox_insert_on_receive": not any(
                "INSERT INTO source_outbox" in s for s in receive_sql),
            "receive_inserts_version": any(
                "INSERT INTO version" in s for s in receive_sql),
            "receive_updates_items": any(
                "INSERT INTO items" in s for s in receive_sql),
            "receive_does_not_touch_meta_seq": not any(
                "UPDATE meta SET seq" in s for s in receive_sql),
        },
    }
    (work / "trace-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8")
    print(json.dumps({"summary": str(work / "trace-summary.json"),
                      "checks": summary["checks"],
                      "B_outbox": B.state_snapshot()["outbox_count"],
                      "B_heads": B.query_row("r1")["heads"]}, indent=2))
    ok = all(summary["checks"].values())
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

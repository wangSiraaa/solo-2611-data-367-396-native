#!/usr/bin/env python3
"""Run scenario.json against two independent working copies via event files.

Usage:
    python3 run_scenario.py [--workdir run] [--keep]

The script never reads the peer's database: A and B only exchange the event
files produced by ``export-events`` / consumed by ``receive``.  Every step
prints the observed state so the transcript is directly reviewable; a JSON
summary is written to ``<workdir>/scenario-report.json``.
"""

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPLICATE = ROOT / "replicate.py"


class Trace:
    def __init__(self):
        self.steps = []

    def add(self, step, **kw):
        entry = {"step": step}
        entry.update(kw)
        self.steps.append(entry)
        print(json.dumps(entry, ensure_ascii=False, sort_keys=True))


def run(args, expect=(0,)):
    proc = subprocess.run([sys.executable, str(REPLICATE)] + args,
                          capture_output=True, text=True, cwd=str(ROOT))
    if proc.returncode not in expect:
        raise RuntimeError("command failed (%d): %s\nstdout:%s\nstderr:%s"
                           % (proc.returncode, " ".join(args), proc.stdout,
                              proc.stderr))
    try:
        out = json.loads(proc.stdout)
    except json.JSONDecodeError:
        out = proc.stdout
    return proc.returncode, out


def q(db, row, trace, label):
    _, out = run(["query-row", "--db", str(db), row])
    trace.add(label, db=db.name, status=out["status"], heads=out["heads"],
              state=out.get("state"),
              members=out.get("members"),
              pending=out.get("pending"))
    return out


def data(db, trace, label):
    _, out = run(["export-data", "--db", str(db)])
    trace.add(label, db=db.name, rows=out["rows"], excluded=out["excluded"])
    return out


def receive(db, files, trace, label, expect=(0,)):
    rc, out = run(["receive", "--db", str(db)] + [str(f) for f in files],
                  expect=expect)
    trace.add(label, db=db.name, counts=out.get("counts", out),
              receipts=out.get("receipts", out))
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", default="run")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args(argv)
    work = (ROOT / args.workdir).resolve()
    if work.exists() and not args.keep:
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)
    A, B = work / "A.sqlite", work / "B.sqlite"
    shutil.copy(ROOT / "seed-A.sqlite", A)
    shutil.copy(ROOT / "seed-B.sqlite", B)
    run(["prepare", "--db", str(A)])
    run(["prepare", "--db", str(B)])

    scenario = json.loads((ROOT / "scenario.json").read_text())
    trace = Trace()
    events = {}       # version_id -> event, per replica outbox snapshots
    delivered = {"A": [], "B": []}
    seq = 0

    def export(rep):
        nonlocal seq
        seq += 1
        path = work / ("%s-%02d.events.json" % (rep, seq))
        _, out = run(["export-events", "--db", str(A if rep == "A" else B),
                      "--out", str(path)])
        trace.add("export", replica=rep, path=path.name,
                  version_ids=out["version_ids"])
        return path

    for op in scenario["operations"]:
        if "at" in op:
            rep = op["at"]
            db = A if rep == "A" else B
            heads = op["parents"]
            cmd = op["kind"]
            common = ["--db", str(db), op["row_id"],
                      "--expected-heads", ",".join(heads)]
            if cmd == "delete":
                common = ["delete"] + common
            elif cmd == "resolve":
                if op["state"] is None:
                    common = ["resolve"] + common + ["--deleted"]
                else:
                    common = ["resolve"] + common + [
                        "--state", json.dumps(op["state"])]
            else:
                common = ["write"] + common + [
                    "--state", json.dumps(op["state"])]
            _, out = run(common)
            trace.add("local_" + cmd, replica=rep,
                      version_id=out["version_id"], origin=out["origin"],
                      seq=out["seq"], parents=out["parents"],
                      row_status=out["row"]["status"],
                      heads=out["row"]["heads"])
            events[out["version_id"]] = out["event"]
        elif "deliver" in op:
            spec = op["deliver"]
            if spec == "B:1 -> A":
                path = export("B")
                receive(A, [path], trace, "deliver B:1 -> A")
                q(A, "r1", trace, "A_after_B1")
            elif spec == "A:2 -> B before A:1":
                only = work / "A-a2-only.events.json"
                run(["export-events", "--db", str(A), "--out", str(only),
                     "--only", "A:2"])
                receive(B, [only], trace, "deliver A:2 -> B (A:1 absent)")
                q(B, "r1", trace, "B_pending_A2")
            elif spec == "A:1 -> B twice":
                only = work / "A-a1-twice.events.json"
                run(["export-events", "--db", str(A), "--out", str(only),
                     "--only", "A:1"])
                receive(B, [only], trace, "deliver A:1 -> B (1st)")
                q(B, "r1", trace, "B_after_A1")
                receive(B, [only], trace, "deliver A:1 -> B (2nd/duplicate)")
                q(B, "r1", trace, "B_after_dup")
            elif spec == "A:3 -> B and B:2 -> A in either order":
                pa = export("A")
                pb = export("B")
                receive(B, [pa], trace, "deliver A:3 -> B")
                receive(A, [pb], trace, "deliver B:2 -> A")
                q(A, "r1", trace, "A_conflict_after_A3_B2")
                q(B, "r1", trace, "B_conflict_after_A3_B2")
                data(A, trace, "A_export_data_conflict")
                data(B, trace, "B_export_data_conflict")
            elif spec == "A:4 -> B; replay old A:1":
                pa = export("A")
                receive(B, [pa], trace, "deliver A:4 -> B")
                replay = work / "A-replay-old.events.json"
                run(["export-events", "--db", str(A), "--out", str(replay),
                     "--only", "A:1"])
                receive(B, [replay], trace, "replay old A:1 at B")
                q(B, "r1", trace, "B_final")
                q(A, "r1", trace, "A_final")
                data(A, trace, "A_export_data_final")
                data(B, trace, "B_export_data_final")
            else:
                raise SystemExit("unhandled delivery: %s" % spec)

    # Convergence check: identical business rows, heads and conflict report.
    _, da = run(["export-data", "--db", str(A)])
    _, db_ = run(["export-data", "--db", str(B)])
    _, qa = run(["query-row", "--db", str(A), "r1"])
    _, qb = run(["query-row", "--db", str(B), "r1"])
    convergence = {
        "data_equal": da == db_,
        "heads_equal": qa["heads"] == qb["heads"],
        "status_equal": qa["status"] == qb["status"],
        "A": {"heads": qa["heads"], "status": qa["status"]},
        "B": {"heads": qb["heads"], "status": qb["status"]},
        "rows_A": da["rows"], "rows_B": db_["rows"],
        "excluded_A": da["excluded"], "excluded_B": db_["excluded"],
    }
    trace.add("convergence", **convergence)

    audits = {}
    for name, db in (("A", A), ("B", B)):
        _, out = run(["audit", "--db", str(db)])
        audits[name] = {
            "replica": out["replica"],
            "outbox_count": out["outbox_count"],
            "outbox_version_ids": out["outbox_version_ids"],
            "received_version_ids": out["received_version_ids"],
            "local_echoes": out["local_echoes"],
            "pending": out["pending"],
            "rejections": out["rejections"],
            "head_changes": out["head_changes"],
            "sql_writes_evidence": out["sql_writes_evidence"],
        }
        trace.add("audit", db=db.name, **audits[name])

    report = {"workdir": str(work), "steps": trace.steps,
              "convergence": convergence, "audits": audits}
    (work / "scenario-report.json").write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8")
    print(json.dumps({"report": str(work / "scenario-report.json"),
                      "converged": convergence["data_equal"]
                      and convergence["heads_equal"]},
                     sort_keys=True))
    return 0 if convergence["data_equal"] and convergence["heads_equal"] else 1


if __name__ == "__main__":
    sys.exit(main())

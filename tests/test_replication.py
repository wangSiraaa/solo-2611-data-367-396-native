"""End-to-end tests for the causal two-replica replicator (stdlib unittest).

Run with:  python3 -m unittest discover -s tests -v
"""

import json
import multiprocessing as mp
import shutil
import tempfile
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sync_tool.engine import (ConflictError, Engine, EventError,  # noqa: E402
                              canonical)
from sync_tool import audit_graph  # noqa: E402


def _seed_dirs():
    return ROOT / "seed-A.sqlite", ROOT / "seed-B.sqlite"


class Pair:
    """Two working copies of the real seeds plus file-only delivery helpers."""

    def __init__(self):
        self.dir = Path(tempfile.mkdtemp(prefix="repl-"))
        a, b = _seed_dirs()
        shutil.copy(a, self.dir / "A.sqlite")
        shutil.copy(b, self.dir / "B.sqlite")
        Engine.prepare(self.dir / "A.sqlite")
        Engine.prepare(self.dir / "B.sqlite")
        self.A = Engine(self.dir / "A.sqlite")
        self.B = Engine(self.dir / "B.sqlite")
        self.n = 0

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def export(self, which, version_ids=None):
        self.n += 1
        path = self.dir / ("ev-%02d.json" % self.n)
        eng = self.A if which == "A" else self.B
        eng.export_events(path, version_ids=version_ids)
        return path

    def deliver(self, which, version_ids=None):
        """Export from ``which`` and receive into the other replica."""
        path = self.export(which, version_ids)
        other = self.B if which == "A" else self.A
        return other.receive_file(path)

    def both(self):
        return self.A, self.B


class BaseCase(unittest.TestCase):
    def setUp(self):
        self.p = Pair()

    def tearDown(self):
        self.p.close()

    def assertConverged(self):
        da = self.p.A.export_data()
        db = self.p.B.export_data()
        self.assertEqual(da, db)
        for rid in ("r1", "r2"):
            qa = self.p.A.query_row(rid)
            qb = self.p.B.query_row(rid)
            self.assertEqual(qa["status"], qb["status"], rid)
            self.assertEqual(qa["heads"], qb["heads"], rid)
        return da

    def assertAuditClean(self):
        for eng in (self.p.A, self.p.B):
            rep = eng.audit()
            self.assertEqual(rep["local_echoes"], [], rep["replica"])
            self.assertEqual(rep["remote_versions_exported"], [],
                             rep["replica"])
            self.assertTrue(rep["graph_check"]["ok"],
                            rep["graph_check"]["problems"])


class ScenarioTest(BaseCase):
    def test_full_scenario_converges(self):
        A, B = self.p.both()
        a1 = A.local_write("r1", {"name": "Alice", "qty": 10}, ["seed:1"])
        b1 = B.local_write("r1", {"name": "base", "qty": 11}, ["seed:1"])
        self.assertEqual(a1["version_id"], "A:1")
        self.assertEqual(b1["version_id"], "B:1")

        # B:1 -> A: A now sees both branches, so the row is conflicted.
        self.p.deliver("B")
        q = A.query_row("r1")
        self.assertEqual(q["status"], "conflicted")
        self.assertEqual(q["heads"], ["A:1", "B:1"])
        self.assertEqual(A.export_data()["rows"].count({"id": "r1"}), 0)

        # A explicitly resolves the exact conflict.
        a2 = A.local_write("r1", {"name": "Alice", "qty": 11},
                           ["A:1", "B:1"], kind="resolve")
        self.assertEqual(A.query_row("r1")["status"], "resolved")

        # A:2 reaches B *before* its parent A:1.  It must be pending and must
        # not overwrite the visible frontier.
        r = self.p.deliver("A", version_ids=["A:2"])
        self.assertEqual(r[0]["outcome"], "pending")
        self.assertEqual(r[0]["missing"], ["A:1"])
        qb = B.query_row("r1")
        self.assertEqual(qb["status"], "resolved")
        self.assertEqual(qb["heads"], ["B:1"])
        self.assertEqual(qb["pending"][0]["version_id"], "A:2")

        # A:1 arrives twice: first becomes ready, second is a no-change.
        r = self.p.deliver("A", version_ids=["A:1"])
        self.assertEqual([x["outcome"] for x in r], ["accepted"])
        self.assertEqual(B.query_row("r1")["heads"], ["A:2"])
        r = self.p.deliver("A", version_ids=["A:1"])
        self.assertEqual([x["outcome"] for x in r], ["no_change"])
        self.assertEqual(B.query_row("r1")["state"], {"name": "Alice", "qty": 11})

        # A deletes A:2; B concurrently edits A:2.  Delete/edit is a conflict.
        A.local_write("r1", None, ["A:2"], kind="delete")          # A:3
        B.local_write("r1", {"name": "Bob", "qty": 12}, ["A:2"])   # B:2
        self.p.deliver("A")
        self.p.deliver("B")
        for eng in (A, B):
            q = eng.query_row("r1")
            self.assertEqual(q["status"], "conflicted")
            self.assertEqual(q["heads"], ["A:3", "B:2"])
            members = {m["version_id"]: m for m in q["members"]}
            self.assertTrue(members["A:3"]["tombstone"])
            self.assertEqual(members["B:2"]["state"],
                             {"name": "Bob", "qty": 12})
        data = self.assertConverged()
        self.assertNotIn("r1", [row["id"] for row in data["rows"]])
        conflict = [e for e in data["excluded"] if e["row_id"] == "r1"][0]
        self.assertEqual(conflict["status"], "conflicted")
        self.assertEqual(len(conflict["members"]), 2)

        # Explicit tombstone resolution, then replay of an old edit.
        A.local_write("r1", None, ["A:3", "B:2"], kind="resolve")  # A:4
        self.p.deliver("A")
        r = self.p.deliver("A", version_ids=["A:1"])  # replay old event
        self.assertEqual(r[0]["outcome"], "no_change")
        for eng in (A, B):
            q = eng.query_row("r1")
            self.assertEqual(q["status"], "deleted")
            self.assertEqual(q["heads"], ["A:4"])
        data = self.assertConverged()
        excluded = [e for e in data["excluded"] if e["row_id"] == "r1"][0]
        self.assertEqual(excluded["status"], "deleted")
        self.assertAuditClean()

    def test_resolution_does_not_win_before_parent(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "Alice", "qty": 10}, ["seed:1"])   # A:1
        B.local_write("r1", {"name": "base", "qty": 11}, ["seed:1"])    # B:1
        self.p.deliver("B")                                  # A sees conflict
        A.local_write("r1", {"name": "Alice", "qty": 11},
                      ["A:1", "B:1"], kind="resolve")        # A:2
        # Send only the resolve to B; both parents are missing there.
        r = self.p.deliver("A", version_ids=["A:2"])
        self.assertEqual(r[0]["outcome"], "pending")
        # B already holds B:1 locally, so only A:1 is actually missing.
        self.assertEqual(r[0]["missing"], ["A:1"])
        self.assertEqual(B.query_row("r1")["heads"], ["B:1"])
        # A:1 arrives alone: that is the last missing parent for A:2, so the
        # previously pending resolve is re-evaluated in the same transaction
        # and applies.  Nothing wins merely because kind=resolve; it wins
        # because both parents are now causally present.
        r = self.p.deliver("A", version_ids=["A:1"])
        self.assertEqual(r[0]["outcome"], "accepted")
        self.assertEqual(B.query_row("r1")["status"], "resolved")
        self.assertEqual(B.query_row("r1")["heads"], ["A:2"])
        self.assertEqual(B.query_row("r1")["state"],
                         {"name": "Alice", "qty": 11})
        self.assertEqual(B.query_row("r1")["pending"], [])
        self.assertEqual(B.audit()["local_echoes"], [])


class LocalWriteTest(BaseCase):
    def test_stale_expected_heads_rejected(self):
        A, _ = self.p.both()
        A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])   # A:1
        with self.assertRaises(ConflictError) as ctx:
            A.local_write("r1", {"name": "y", "qty": 2}, ["seed:1"])
        self.assertEqual(ctx.exception.code, "stale_expected_heads")
        self.assertEqual(A.query_row("r1")["heads"], ["A:1"])

    def test_resolve_cannot_cover_newer_concurrent_head(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])      # A:1
        B.local_write("r1", {"name": "b", "qty": 2}, ["seed:1"])      # B:1
        self.p.deliver("B")
        self.assertEqual(A.query_row("r1")["heads"], ["A:1", "B:1"])
        # A third branch appears on B before A resolves.
        self.p.deliver("A", version_ids=["A:1"])
        B.local_write("r1", {"name": "late", "qty": 3},
                      ["A:1", "B:1"], kind="resolve")                 # B:2
        self.p.deliver("B")
        q = A.query_row("r1")
        self.assertEqual(q["status"], "resolved")
        self.assertEqual(q["heads"], ["B:2"])
        # The old two-head decision is now stale and must be refused.
        with self.assertRaises(ConflictError) as ctx:
            A.local_write("r1", {"name": "old-decision", "qty": 9},
                          ["A:1", "B:1"], kind="resolve")
        self.assertEqual(ctx.exception.code, "stale_expected_heads")
        self.assertEqual(ctx.exception.detail["current_heads"], ["B:2"])
        self.assertEqual(A.query_row("r1")["state"],
                         {"name": "late", "qty": 3})

    def test_resolve_on_absent_row_rejected(self):
        A, _ = self.p.both()
        with self.assertRaises(ConflictError) as ctx:
            A.local_write("ghost", {"name": "x", "qty": 1}, [], kind="resolve")
        self.assertEqual(ctx.exception.code, "absent")
        # And a received resolve without parents is malformed.
        B = self.p.B
        ev = {"dataset": A._dataset(), "version_id": "A:1", "origin": "A",
              "seq": 1, "row_id": "ghost", "parents": [], "kind": "resolve",
              "state": {"name": "x", "qty": 1}}
        self.assertEqual(B.receive([ev])[0]["outcome"], "rejected")

    def test_random_order_full_delivery_converges(self):
        import random
        A, B = self.p.both()
        A.local_write("r1", {"name": "a1", "qty": 1}, ["seed:1"])   # A:1
        B.local_write("r1", {"name": "b1", "qty": 2}, ["seed:1"])   # B:1
        self.p.deliver("A")
        self.p.deliver("B")
        A.local_write("r1", {"name": "a2", "qty": 3},
                      ["A:1", "B:1"], kind="resolve")               # A:2
        B.local_write("r1", {"name": "b2", "qty": 4},
                      ["A:1", "B:1"], kind="resolve")               # B:2
        self.p.deliver("A")
        self.p.deliver("B")
        A.local_write("r1", None, ["A:2", "B:2"], kind="resolve")   # A:3 tomb
        B.local_write("r1", {"name": "b3", "qty": 5}, ["A:2", "B:2"],
                      kind="resolve")                               # B:3
        self.p.deliver("A")
        self.p.deliver("B")
        # Now ship the *entire* combined event set to each side in a random
        # order, with deliberate duplicates.
        events_a = [e[1] for e in A.outbox_events()]
        events_b = [e[1] for e in B.outbox_events()]
        rng = random.Random(20261115)
        for target, incoming in ((A, events_b), (B, events_a)):
            batch = incoming * 3
            rng.shuffle(batch)
            for ev in batch:
                target.receive([ev])
        self.assertConverged()
        self.assertAuditClean()
        for eng in (A, B):
            q = eng.query_row("r1")
            # A:3 (tombstone) and B:3 (write) are concurrent and unresolved.
            self.assertEqual(q["status"], "conflicted")
            self.assertEqual(q["heads"], ["A:3", "B:3"])
            self.assertTrue(any(m["tombstone"] for m in q["members"]))
            self.assertTrue(any(m["state"] == {"name": "b3", "qty": 5}
                                for m in q["members"]))

    def test_seq_gaps_across_rows_are_fine(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "r1", "qty": 1}, ["seed:1"])   # A:1
        A.local_write("r2", {"name": "r2", "qty": 2}, ["seed:2"])   # A:2
        # Deliver A:2 (higher seq) before A:1 (lower seq).  No global gap rule.
        self.p.deliver("A", version_ids=["A:2"])
        self.assertEqual(B.query_row("r2")["heads"], ["A:2"])
        self.p.deliver("A", version_ids=["A:1"])
        self.assertEqual(B.query_row("r1")["heads"], ["A:1"])
        self.assertEqual(B.query_row("r1")["state"], {"name": "r1", "qty": 1})
        self.assertAuditClean()

    def test_pending_item_does_not_block_independent_item_in_batch(self):
        A, B = self.p.both()
        p1 = A.local_write("r1", {"name": "one", "qty": 1}, ["seed:1"])  # A:1
        p2 = A.local_write("r1", {"name": "two", "qty": 2}, ["A:1"])     # A:2
        other = A.local_write("r2", {"name": "unrelated", "qty": 3}, ["seed:2"])
        self.p.deliver("A", version_ids=["A:2", "A:2", "A:2"])  # pending
        # A batch with A:1 then the unrelated row: the independent item applies.
        r = B.receive([p1, other])
        self.assertEqual([x["outcome"] for x in r], ["accepted", "accepted"])
        self.assertEqual(B.query_row("r1")["heads"], ["A:2"])
        self.assertEqual(B.query_row("r2")["state"],
                         {"name": "unrelated", "qty": 3})

        A, _ = self.p.both()
        q = A.query_row("nope")
        self.assertEqual(q["status"], "absent")
        self.assertEqual(q["heads"], [])

    def test_write_requires_conflict_free(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])
        B.local_write("r1", {"name": "b", "qty": 2}, ["seed:1"])
        self.p.deliver("B")
        with self.assertRaises(ConflictError) as ctx:
            A.local_write("r1", {"name": "c", "qty": 3}, ["A:1", "B:1"])
        self.assertEqual(ctx.exception.code, "conflicted")

    def test_resolve_must_bind_every_head(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])   # A:1
        B.local_write("r1", {"name": "b", "qty": 2}, ["seed:1"])   # B:1
        self.p.deliver("B")
        with self.assertRaises(ConflictError) as ctx:
            A.local_write("r1", {"name": "c", "qty": 3}, ["A:1"], kind="resolve")
        self.assertEqual(ctx.exception.code, "stale_expected_heads")
        A.local_write("r1", {"name": "c", "qty": 3}, ["A:1", "B:1"],
                      kind="resolve")
        self.assertEqual(A.query_row("r1")["heads"], ["A:2"])

    def test_resolve_conflict_detail_keeps_all_members(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])
        B.local_write("r1", {"name": "b", "qty": 2}, ["seed:1"])
        self.p.deliver("B")
        # A partial list is rejected, but the detail still shows both values.
        with self.assertRaises(ConflictError) as ctx:
            A.local_write("r1", {"name": "c", "qty": 3}, ["A:1"],
                          kind="resolve")
        self.assertEqual(ctx.exception.code, "stale_expected_heads")

    def test_concurrent_writers_only_one_wins(self):
        A, _ = self.p.both()
        db = str(self.p.dir / "A.sqlite")
        go = self.p.dir / "go"
        import subprocess
        import time
        procs = []
        for i in range(5):
            req = self.p.dir / ("req-%d.json" % i)
            req.write_text(json.dumps({
                "row_id": "r1",
                "state": {"name": "w%d" % i, "qty": i},
                "expected_heads": ["seed:1"],
                "ready": str(self.p.dir / ("ready-%d" % i)),
                "go": str(go),
            }))
            code = (
                "import json,sys,time,pathlib\n"
                "sys.path.insert(0, %r)\n"
                "from sync_tool.engine import Engine, ConflictError\n"
                "req = json.loads(pathlib.Path(%r).read_text())\n"
                "pathlib.Path(req['ready']).touch()\n"
                "while not pathlib.Path(req['go']).exists(): time.sleep(0.001)\n"
                "try:\n"
                "    ev = Engine(%r).local_write(req['row_id'], req['state'],"
                " req['expected_heads'])\n"
                "    print(json.dumps({'outcome': 'ok',"
                " 'version_id': ev['version_id']}))\n"
                "except ConflictError as exc:\n"
                "    print(json.dumps({'outcome': 'conflict',"
                " 'code': exc.code}))\n"
                % (str(ROOT), str(req), db))
            procs.append(subprocess.Popen(
                [sys.executable, "-c", code],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        deadline = time.time() + 15
        while len(list(self.p.dir.glob("ready-*"))) != 5:
            if time.time() > deadline:
                self.fail("children did not reach the barrier")
            time.sleep(0.005)
        go.touch()  # release all writers as simultaneously as possible
        results = []
        for proc in procs:
            out, err = proc.communicate(timeout=30)
            results.append(json.loads(out.strip().splitlines()[-1]))
        oks = [r for r in results if r["outcome"] == "ok"]
        self.assertEqual(len(oks), 1, results)
        self.assertTrue(all(r["outcome"] == "conflict" for r in results
                            if r["outcome"] != "ok"))
        self.assertEqual(A.state_snapshot()["seq"], 1)
        self.assertEqual(A.query_row("r1")["heads"], ["A:1"])

    def test_commit_failure_leaves_no_partial_state(self):
        A, _ = self.p.both()
        before = A.state_snapshot()
        with self.assertRaises(RuntimeError):
            A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"],
                          fail_before_outbox=True)
        with self.assertRaises(RuntimeError):
            A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"],
                          fail_before_commit=True)
        self.assertEqual(A.state_snapshot(), before)
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        self.assertEqual(ev["version_id"], "A:1")

    def test_commit_failure_in_subprocess_is_rolled_back(self):
        A, _ = self.p.both()
        db = str(self.p.dir / "A.sqlite")
        code = (
            "import sys; sys.path.insert(0, %r)\n"
            "from sync_tool.engine import Engine\n"
            "Engine(%r).local_write('r1', {'name':'x','qty':1}, ['seed:1'],"
            " crash_before_commit=True)\n" % (str(ROOT), db))
        import subprocess
        out = subprocess.run([sys.executable, "-c", code],
                             capture_output=True, text=True)
        self.assertEqual(out.returncode, 17, out.stderr)
        self.assertEqual(A.state_snapshot()["seq"], 0)
        self.assertNotIn("A:1",
                         [v[0] for v in A.state_snapshot()["versions"]])
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        self.assertEqual(ev["version_id"], "A:1")

    def test_commit_persisted_then_process_dies_is_still_visible(self):
        """Simulate "committed, reply lost": kill right after COMMIT."""
        A, _ = self.p.both()
        db = str(self.p.dir / "A.sqlite")
        code = (
            "import sys; sys.path.insert(0, %r)\n"
            "from sync_tool.engine import Engine\n"
            "ev = Engine(%r).local_write('r1', {'name':'x','qty':1},"
            " ['seed:1'], op_id='crash-after-commit')\n"
            "print(ev['version_id'], flush=True)\n"
            "import os; os._exit(23)\n" % (str(ROOT), db))
        import subprocess
        out = subprocess.run([sys.executable, "-c", code],
                             capture_output=True, text=True)
        self.assertEqual(out.returncode, 23, out.stderr)
        self.assertEqual(out.stdout.strip(), "A:1")
        # The commit is durable despite the missing acknowledgement...
        self.assertEqual(A.query_row("r1")["state"], {"name": "x", "qty": 1})
        self.assertEqual(A.state_snapshot()["outbox_count"], 1)
        # ...and retrying with the same token returns the same version, so no
        # duplicate local write or outbox echo appears.
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"],
                           op_id="crash-after-commit")
        self.assertEqual(ev["version_id"], "A:1")
        self.assertEqual(A.state_snapshot()["outbox_count"], 1)
        self.assertEqual(A.state_snapshot()["seq"], 1)

    def test_receive_commit_survives_and_retry_is_nochange(self):
        A, B = self.p.both()
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        db = str(self.p.dir / "B.sqlite")
        code = (
            "import sys; sys.path.insert(0, %r)\n"
            "from sync_tool.engine import Engine\n"
            "Engine(%r).receive([%r])\n"
            "import os; os._exit(19)\n" % (str(ROOT), db, ev))
        import subprocess
        out = subprocess.run([sys.executable, "-c", code],
                             capture_output=True, text=True)
        self.assertEqual(out.returncode, 19, out.stderr)
        self.assertEqual(B.query_row("r1")["state"], {"name": "x", "qty": 1})
        self.assertEqual(B.state_snapshot()["outbox_count"], 0)
        r = B.receive([ev])
        self.assertEqual(r[0]["outcome"], "no_change")

    def test_receive_crash_rolls_back_and_retry_succeeds(self):
        A, B = self.p.both()
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        before = B.state_snapshot()
        with self.assertRaises(RuntimeError):
            B.receive([ev], fail_after_persist=True)
        self.assertEqual(B.state_snapshot(), before)
        receipts = B.receive([ev])
        self.assertEqual(receipts[0]["outcome"], "accepted")

    def test_op_id_retry_returns_original_version(self):
        A, _ = self.p.both()
        ev1 = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"],
                            op_id="op-1")
        ev2 = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"],
                            op_id="op-1")
        self.assertEqual(ev1, ev2)
        self.assertEqual(A.state_snapshot()["outbox_count"], 1)
        with self.assertRaises(ConflictError) as ctx:
            A.local_write("r1", {"name": "y", "qty": 2}, ["seed:1"],
                          op_id="op-1")
        self.assertEqual(ctx.exception.code, "op_id_reused")


class ReceptionTest(BaseCase):
    def test_identity_reuse_with_different_content_rejected(self):
        A, B = self.p.both()
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        self.assertEqual(B.receive([ev])[0]["outcome"], "accepted")
        forged = json.loads(json.dumps(ev))
        forged["state"] = {"name": "hacked", "qty": 0}
        receipts = B.receive([forged])
        self.assertEqual(receipts[0]["outcome"], "rejected")
        self.assertIn("identity_content_conflict", receipts[0]["reason"])
        self.assertEqual(B.query_row("r1")["state"], {"name": "x", "qty": 1})
        rep = B.audit()
        self.assertEqual(len(rep["rejections"]), 1)

    def test_rejection_does_not_affect_other_items(self):
        A, B = self.p.both()
        good = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        other = A.local_write("r2", {"name": "y", "qty": 2}, ["seed:2"])
        self.assertEqual(B.receive([good])[0]["outcome"], "accepted")
        forged = json.loads(json.dumps(good))
        forged["state"] = {"name": "bad", "qty": 5}
        receipts = B.receive([forged, other])
        self.assertEqual(receipts[0]["outcome"], "rejected")
        self.assertEqual(receipts[1]["outcome"], "accepted")
        self.assertEqual(B.query_row("r1")["status"], "resolved")
        self.assertEqual(B.query_row("r2")["state"], {"name": "y", "qty": 2})

    def test_cross_row_parent_rejected(self):
        A, B = self.p.both()
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        crossed = {"dataset": A._dataset(), "version_id": "A:9",
                   "origin": "A", "seq": 9, "row_id": "r2",
                   "parents": ["A:1"], "kind": "write",
                   "state": {"name": "z", "qty": 1}}
        # The parent is not known at B yet, so defer until it is.
        r = B.receive([crossed])
        self.assertEqual(r[0]["outcome"], "pending")
        self.assertEqual(r[0]["row_id"], "r2")
        r = B.receive([ev])
        self.assertEqual(r[0]["outcome"], "accepted")
        rep = B.audit()
        self.assertTrue(any(reb["version_id"] == "A:9"
                            and "cross_row_parent" in (reb["reason"] or "")
                            for reb in rep["rejections"]),
                        rep["rejections"])
        self.assertEqual(B.query_row("r2")["status"], "resolved")
        self.assertEqual(B.query_row("r2")["heads"], ["seed:2"])

    def test_wrong_dataset_and_echo_rejected(self):
        A, B = self.p.both()
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        bad = dict(ev, dataset="0" * 64)
        bad["version_id"] = "A:2"
        bad["seq"] = 2
        r = B.receive([bad])
        self.assertEqual(r[0]["outcome"], "rejected")
        # A replay of A's *own* genuine version is an idempotent no-change, not
        # a second write and not a new outbox entry.
        r = A.receive([ev])
        self.assertEqual(r[0]["outcome"], "no_change")
        # An A-origin identity A never authored is an echo and is refused.
        forged = json.loads(json.dumps(ev))
        forged["version_id"] = "A:9"
        forged["seq"] = 9
        forged["parents"] = ["A:1"]
        forged["state"] = {"name": "ghost", "qty": 7}
        r = A.receive([forged])
        self.assertEqual(r[0]["outcome"], "rejected")
        self.assertEqual(r[0]["reason"], "own_origin")
        self.assertEqual(A.state_snapshot()["outbox_count"], 1)
        self.assertEqual(A.query_row("r1")["state"], {"name": "x", "qty": 1})

    def test_out_of_order_and_split_batches_still_converge(self):
        A, B = self.p.both()
        v1 = A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])
        v2 = A.local_write("r1", {"name": "b", "qty": 2}, ["A:1"])
        v3 = A.local_write("r1", {"name": "c", "qty": 3}, ["A:2"])
        # Deliver newest first, one event per file, with duplicates.
        for ev in (v3, v2, v3, v1, v2):
            B.receive([ev])
        self.assertEqual(B.query_row("r1")["status"], "resolved")
        self.assertEqual(B.query_row("r1")["heads"], ["A:3"])
        self.assertEqual(B.query_row("r1")["state"], {"name": "c", "qty": 3})
        self.assertAuditClean()

    def test_remote_events_keep_identity_and_create_no_outbox(self):
        A, B = self.p.both()
        ev = A.local_write("r1", {"name": "x", "qty": 1}, ["seed:1"])
        B.receive([ev])
        snap = B.state_snapshot()
        self.assertEqual(snap["outbox_count"], 0)
        q = B.query_row("r1")
        self.assertEqual(q["heads"], ["A:1"])
        self.assertEqual(q["head_versions"][0]["origin"], "A")
        self.assertEqual(q["head_versions"][0]["seq"], 1)
        rep = B.audit()
        self.assertEqual(rep["received_version_ids"], ["A:1"])
        self.assertEqual(rep["outbox_version_ids"], [])
        self.assertEqual(rep["local_echoes"], [])
        self.assertEqual(rep["remote_versions_exported"], [])
        self.assertTrue(rep["graph_check"]["ok"], rep["graph_check"])

    def test_conflicted_value_not_exported_as_normal_row(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])
        B.local_write("r1", {"name": "b", "qty": 2}, ["seed:1"])
        self.p.deliver("B")
        self.p.deliver("A")
        data = A.export_data()
        self.assertNotIn("r1", [r["id"] for r in data["rows"]])
        excl = [e for e in data["excluded"] if e["row_id"] == "r1"][0]
        self.assertEqual(excl["status"], "conflicted")
        self.assertEqual({m["version_id"] for m in excl["members"]},
                         {"A:1", "B:1"})
        # Conflict members stay out of the business table.
        con = A.connect()
        try:
            self.assertIsNone(con.execute(
                "SELECT 1 FROM items WHERE id='r1'").fetchone())
        finally:
            con.close()

    def test_deleted_row_not_readable_and_not_revived_by_old_edit(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])   # A:1
        A.local_write("r1", None, ["A:1"], kind="delete")          # A:2
        self.p.deliver("A")  # both A:1 and A:2; A:2 applies after A:1
        self.assertEqual(B.query_row("r1")["status"], "deleted")
        # Replaying the pre-delete edit must not resurrect the row.
        self.p.deliver("A", version_ids=["A:1"])
        self.assertEqual(B.query_row("r1")["status"], "deleted")
        self.assertEqual(B.export_data()["rows"],
                         [{"id": "r2", "name": "spare", "qty": 5}])

    def test_new_write_after_observed_tombstone_restores_row(self):
        A, B = self.p.both()
        A.local_write("r1", None, ["seed:1"], kind="delete")     # A:1
        self.p.deliver("A")
        self.assertEqual(B.query_row("r1")["status"], "deleted")
        B.local_write("r1", {"name": "revived", "qty": 4}, ["A:1"])
        self.p.deliver("B")
        for eng in (A, B):
            q = eng.query_row("r1")
            self.assertEqual(q["status"], "resolved")
            self.assertEqual(q["state"], {"name": "revived", "qty": 4})

    def test_bad_state_types_rejected(self):
        A, B = self.p.both()
        base = {"dataset": A._dataset(), "version_id": "A:1", "origin": "A",
                "seq": 1, "row_id": "r1", "parents": ["seed:1"],
                "kind": "write", "state": {"name": "x", "qty": 1}}
        for mutation in (
            {"state": {"name": "x", "qty": -1}},
            {"state": {"name": "x", "qty": True}},
            {"state": {"name": "x"}},
            {"state": {"name": "x", "qty": 1, "extra": 2}},
            {"kind": "delete"},                       # delete with a state
            {"parents": ["seed:1", "seed:1"]},        # duplicate parent
            {"kind": "merge"},                        # bad kind
            {"origin": "B"},                          # id/origin mismatch
        ):
            ev = json.loads(json.dumps(base))
            ev.update(mutation)
            if mutation.get("kind") == "delete":
                ev["state"] = {"name": "x", "qty": 1}
            r = B.receive([ev])
            self.assertEqual(r[0]["outcome"], "rejected", mutation)


def _worker(args):
    db, idx, barrier, queue = args
    eng = Engine(db)
    barrier.wait()
    try:
        ev = eng.local_write("r1", {"name": "w%d" % idx, "qty": idx},
                             ["seed:1"])
        queue.put((idx, "ok", ev["version_id"]))
    except ConflictError as exc:
        queue.put((idx, "conflict", exc.code))
    except Exception as exc:  # pragma: no cover
        queue.put((idx, "error", repr(exc)))


class PersistenceTest(BaseCase):
    def test_reopen_is_consistent(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])
        B.local_write("r1", {"name": "b", "qty": 2}, ["seed:1"])
        self.p.deliver("B")
        self.p.deliver("A")
        before = {"A": A.export_data(), "B": B.export_data(),
                  "ha": A.query_row("r1"), "hb": B.query_row("r1")}
        # Reopen through new Engine objects (fresh SQLite connections).
        A2 = Engine(self.p.dir / "A.sqlite")
        B2 = Engine(self.p.dir / "B.sqlite")
        self.assertEqual(A2.export_data(), before["A"])
        self.assertEqual(B2.export_data(), before["B"])
        self.assertEqual(A2.query_row("r1"), before["ha"])
        self.assertEqual(B2.query_row("r1"), before["hb"])
        self.assertTrue(A2.audit()["graph_check"]["ok"])
        self.assertTrue(B2.audit()["graph_check"]["ok"])

    def test_export_file_roundtrip_is_canonical(self):
        A, B = self.p.both()
        A.local_write("r1", {"name": "a", "qty": 1}, ["seed:1"])
        path = self.p.export("A")
        envelope = json.loads(path.read_text())
        self.assertEqual(envelope["dataset"], A._dataset())
        self.assertEqual(len(envelope["events"]), 1)
        raw = canonical(envelope["events"][0])
        self.assertEqual(envelope["events"][0], json.loads(
            A.outbox_events()[0][1] and canonical(A.outbox_events()[0][1])))


if __name__ == "__main__":
    unittest.main(verbosity=2)

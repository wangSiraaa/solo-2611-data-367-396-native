"""
test_flow.py -- end-to-end verification of the local session producer.

Covers, with assertions on actual table contents and access counters:
  1. bridge merge (two sessions -> one, explicit old-session deletes)
  2. bridge retract split (one -> two, member-accurate re-publish)
  3. same-tick multiple events (all contribute, stable event_id order)
  4. revision across a session boundary (split/merge driven by content edit)
  5. watermark exactly at last_time + gap (finalizes, equality included)
  6. late event/revision/retract rejection with retained reasons, no data change
  7. another key's formal-path event-read counter never moves
  8. independent consumer applies delete/upsert batches, members + totals
  9. independent full-recompute oracle agrees (audit)
 10. replay idempotence: same stream on a fresh db => identical business output
 11. hot events older than W that still feed an open session are NOT deleted
 12. snapshot = archive UNION hot, no missing/duplicated members
 13. the provided events.json + probe.py oracle agree
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest

from sessionizer import Engine, query_snapshot, query_batches, run_audit
from oracle import sessionize, replay_oracle
from consumer import apply_changefeed

GAP = 5


def fresh_engine(path: str, gap: int = GAP) -> Engine:
    con = sqlite3.connect(path, isolation_level=None)
    Engine.initialize(con, gap, -10_000, 10_000, -10_000, 10_000)
    con.close()
    return Engine(path)


def index_by_members(rows):
    return {(r["key"], tuple(r["members"])): r for r in rows}


class FlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "t.db")
        self.eng = fresh_engine(self.db)

    def tearDown(self):
        self.eng.close()
        self.tmp.cleanup()

    def feed(self, ops):
        return [self.eng.handle(op) for op in ops]

    def snapshot(self):
        return query_snapshot(self.eng.con)

    # ------------------------------------------------------------ 1+2 bridge
    def test_bridge_merge_then_retract_split(self):
        self.feed([
            {"op": "upsert", "event_id": "a", "key": "u", "time": 0, "value": 3},
            {"op": "upsert", "event_id": "b", "key": "u", "time": 10, "value": 7},
        ])
        snap = index_by_members(self.snapshot())
        self.assertEqual(set(snap), {("u", ("a",)), ("u", ("b",))})
        self.assertEqual(snap[("u", ("a",))]["sum"], 3)
        self.assertEqual(snap[("u", ("b",))]["sum"], 7)

        # bridge at t=5: |5-0|<=5 and |10-5|<=5 -> single merged session
        self.eng.handle({"op": "upsert", "event_id": "br", "key": "u",
                         "time": 5, "value": 2})
        snap = index_by_members(self.snapshot())
        self.assertEqual(list(snap), [("u", ("a", "b", "br"))])
        merged = snap[("u", ("a", "b", "br"))]
        self.assertEqual((merged["start"], merged["end"], merged["count"],
                          merged["sum"]), (0, 10, 3, 12))
        self.assertEqual(merged["tier"], "hot")

        # the merge batch must explicitly retract BOTH old results, not just
        # publish a new total
        merge_batch = [
            b for b in query_batches(self.eng.con)
            if any(c["trigger_event"] == "br" and c["op"] == "delete"
                   for c in b["changes"])][0]
        ops = [(c["op"], tuple(c["members"])) for c in merge_batch["changes"]]
        self.assertIn(("delete", ("a",)), ops)
        self.assertIn(("delete", ("b",)), ops)
        self.assertIn(("upsert", ("a", "b", "br")), ops)
        # delete-before-upsert ordering inside the batch
        self.assertLess(
            next(i for i, c in enumerate(merge_batch["changes"])
                 if c["op"] == "delete"),
            next(i for i, c in enumerate(merge_batch["changes"])
                 if c["op"] == "upsert"))

        # retract bridge -> split back into the two original member sets
        self.eng.handle({"op": "retract", "event_id": "br"})
        snap = index_by_members(self.snapshot())
        self.assertEqual(set(snap), {("u", ("a",)), ("u", ("b",))})
        split_batch = [
            b for b in query_batches(self.eng.con)
            if any(c["trigger_event"] == "br" and c["op"] == "delete"
                   and tuple(c["members"]) == ("a", "b", "br")
                   for c in b["changes"])][0]
        ops = [(c["op"], tuple(c["members"])) for c in split_batch["changes"]]
        self.assertIn(("delete", ("a", "b", "br")), ops)
        self.assertIn(("upsert", ("a",)), ops)
        self.assertIn(("upsert", ("b",)), ops)
        # restored sessions keep the original aggregates
        self.assertEqual(snap[("u", ("a",))]["sum"], 3)
        self.assertEqual(snap[("u", ("b",))]["sum"], 7)

    # ----------------------------------------------------------- 3 same tick
    def test_same_tick_multiple_events(self):
        ops = [
            {"op": "upsert", "event_id": "z", "key": "k", "time": 4, "value": 1},
            {"op": "upsert", "event_id": "a", "key": "k", "time": 4, "value": 2},
            {"op": "upsert", "event_id": "m", "key": "k", "time": 4, "value": 3},
        ]
        self.feed(ops)
        snap = self.snapshot()
        self.assertEqual(len(snap), 1)
        s = snap[0]
        self.assertEqual(s["members"], ["a", "m", "z"])   # stable id ordering
        self.assertEqual(s["count"], 3)
        self.assertEqual(s["sum"], 6)
        self.assertEqual((s["start"], s["end"]), (4, 4))

    # -------------------------------------------------- 4 revision across gap
    def test_revision_across_boundary(self):
        self.feed([
            {"op": "upsert", "event_id": "a", "key": "k", "time": 0, "value": 1},
            {"op": "upsert", "event_id": "b", "key": "k", "time": 6, "value": 10},
        ])
        self.assertEqual(len(self.snapshot()), 2)          # diff 6 > gap 5

        # revise b to t=5 -> diff 5 <= gap -> merge; same id, changed content
        self.eng.handle({"op": "upsert", "event_id": "b", "key": "k",
                         "time": 5, "value": 10})
        snap = self.snapshot()
        self.assertEqual(len(snap), 1)
        self.assertEqual(snap[0]["members"], ["a", "b"])
        self.assertEqual(snap[0]["end"], 5)

        # identical replay: zero new business changes
        before = self.eng.con.execute(
            "SELECT COUNT(*) c FROM changes").fetchone()["c"]
        r = self.eng.handle({"op": "upsert", "event_id": "b", "key": "k",
                             "time": 5, "value": 10})
        after = self.eng.con.execute(
            "SELECT COUNT(*) c FROM changes").fetchone()["c"]
        self.assertTrue(r["noop"])
        self.assertEqual(before, after)

        # key immutability on revision
        rej = self.eng.handle({"op": "upsert", "event_id": "b", "key": "OTHER",
                               "time": 5, "value": 10})
        self.assertFalse(rej["accepted"])
        self.assertIn("immutable", rej["reason"])

    def test_unknown_retract_rejected(self):
        r = self.eng.handle({"op": "retract", "event_id": "ghost"})
        self.assertFalse(r["accepted"])
        self.assertIn("unknown", r["reason"])
        rows = self.eng.con.execute("SELECT reason FROM rejections").fetchall()
        self.assertTrue(any("unknown" in x["reason"] for x in rows))

    # --------------------------------------------- 5 watermark finalization
    def test_watermark_exact_last_plus_gap_finalizes(self):
        self.feed([
            {"op": "upsert", "event_id": "a", "key": "u", "time": 0, "value": 3},
            {"op": "upsert", "event_id": "b", "key": "u", "time": 4, "value": 7},
        ])
        # end=4, gap=5 -> finalizable exactly when W >= 9
        r = self.eng.handle({"op": "watermark", "time": 8})
        self.assertEqual(r["archived"], [])
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM hot_sessions")
            .fetchone()["c"], 1)

        r = self.eng.handle({"op": "watermark", "time": 9})  # equality
        self.assertEqual(len(r["archived"]), 1)
        arch = self.eng.con.execute(
            "SELECT * FROM archived_sessions").fetchall()
        hot = self.eng.con.execute("SELECT * FROM hot_sessions").fetchall()
        self.assertEqual(len(arch), 1)
        self.assertEqual(len(hot), 0)
        # hot state cleaned: finalized members removed from events
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM events").fetchone()["c"],
            0)
        # snapshot still contains the session once, from the cold tier
        snap = self.snapshot()
        self.assertEqual(len(snap), 1)
        self.assertEqual(snap[0]["tier"], "archive")
        self.assertEqual(snap[0]["members"], ["a", "b"])
        self.assertEqual(snap[0]["sum"], 10)
        # the batch publishes only archive-tier moves: zero HOT business
        # changes (same identity/content), and the move is an explicit upsert
        wm_rows = self.eng.con.execute(
            "SELECT op, tier, session_id, cnt, total, members FROM changes "
            "WHERE seq=?", (r["seq"],)).fetchall()
        self.assertEqual(len(wm_rows), 1)
        self.assertEqual((wm_rows[0]["op"], wm_rows[0]["tier"]),
                         ("upsert", "archive"))
        self.assertEqual(wm_rows[0]["session_id"], snap[0]["session_id"])
        self.assertEqual(
            self.eng.con.execute(
                "SELECT COUNT(*) c FROM changes WHERE seq=? AND tier='hot'",
                (r["seq"],)).fetchone()["c"], 0)

    def test_old_open_session_events_are_not_cleaned(self):
        # session [0 .. 4] stays open at W=4 because 4+5 > 4; its old events
        # are time <= W yet must remain in the hot table and still contribute
        self.feed([
            {"op": "upsert", "event_id": "a", "key": "u", "time": 0, "value": 1},
            {"op": "upsert", "event_id": "b", "key": "u", "time": 4, "value": 2},
        ])
        self.eng.handle({"op": "watermark", "time": 4})
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM events").fetchone()["c"],
            2)
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM hot_sessions")
            .fetchone()["c"], 1)
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM archived_sessions")
            .fetchone()["c"], 0)
        # one tick later, end+gap == 9 == W -> finalization becomes legal
        self.eng.handle({"op": "watermark", "time": 9})
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM events").fetchone()["c"],
            0)
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM archived_sessions")
            .fetchone()["c"], 1)

    # ------------------------------------------------------------- 6 late ops
    def test_late_upsert_revision_retract_rejected_without_data_change(self):
        self.feed([
            {"op": "upsert", "event_id": "a", "key": "u", "time": 0, "value": 3},
            {"op": "upsert", "event_id": "b", "key": "u", "time": 10, "value": 7},
            {"op": "watermark", "time": 5},
        ])
        before = query_snapshot(self.eng.con)

        r1 = self.eng.handle({"op": "upsert", "event_id": "late", "key": "u",
                              "time": 5, "value": 99})
        self.assertFalse(r1["accepted"])
        self.assertIn("watermark", r1["reason"])

        r2 = self.eng.handle({"op": "upsert", "event_id": "a", "key": "u",
                              "time": 2, "value": 9})   # old time frozen
        self.assertFalse(r2["accepted"])

        r3 = self.eng.handle({"op": "retract", "event_id": "a"})
        self.assertFalse(r3["accepted"])

        # published data unchanged and no new business changes
        self.assertEqual(query_snapshot(self.eng.con), before)
        self.assertEqual(
            self.eng.con.execute(
                "SELECT COUNT(*) c FROM changes WHERE seq IN "
                "(SELECT seq FROM rejections)").fetchone()["c"], 0)
        reasons = [r["reason"] for r in self.eng.con.execute(
            "SELECT reason FROM rejections ORDER BY seq")]
        self.assertEqual(len(reasons), 3)

        # watermark retreat refused
        r4 = self.eng.handle({"op": "watermark", "time": 4})
        self.assertFalse(r4["accepted"])

    def test_archived_event_cannot_be_upserted_or_retracted(self):
        self.feed([
            {"op": "upsert", "event_id": "a", "key": "u", "time": 0, "value": 3},
            {"op": "watermark", "time": 5},
        ])
        self.assertEqual(
            self.eng.con.execute("SELECT COUNT(*) c FROM archived_sessions")
            .fetchone()["c"], 1)
        r1 = self.eng.handle({"op": "retract", "event_id": "a"})
        self.assertFalse(r1["accepted"])
        r2 = self.eng.handle({"op": "upsert", "event_id": "a", "key": "u",
                              "time": 99, "value": 1})
        self.assertFalse(r2["accepted"])

    # ------------------------------------------- 7 unrelated key access count
    def test_other_key_event_reads_never_charged(self):
        self.feed([
            {"op": "upsert", "event_id": "a1", "key": "A", "time": 0, "value": 1},
            {"op": "upsert", "event_id": "a2", "key": "A", "time": 3, "value": 1},
            {"op": "upsert", "event_id": "b1", "key": "B", "time": 0, "value": 1},
        ])
        def reads():
            return {r["key_id"]: r["reads"] for r in self.eng.con.execute(
                "SELECT key_id, reads FROM key_access")}

        before = reads()
        self.assertIn("B", before)
        # many A-only operations, including a merge and a split
        for op in [
            {"op": "upsert", "event_id": "a3", "key": "A", "time": 6, "value": 1},
            {"op": "upsert", "event_id": "a4", "key": "A", "time": 9, "value": 1},
            {"op": "retract", "event_id": "a3"},
            {"op": "upsert", "event_id": "a2", "key": "A", "time": 2, "value": 5},
        ]:
            self.eng.handle(op)
        after = reads()
        self.assertEqual(before["B"], after["B"],
                         "formal path scanned another key's events")
        self.assertGreater(after["A"], before["A"])

        # watermark that finalizes nothing on B leaves B's counter untouched
        self.eng.handle({"op": "watermark", "time": 1})   # B end=0 -> 0+5>1
        self.assertEqual(reads()["B"], before["B"])

    # --------------------------------------------- 8+9 consumer/oracle audit
    def test_consumer_and_oracle_audit(self):
        ops = self._mixed_stream()
        self.feed(ops)
        report = run_audit(self.eng.con)
        self.assertTrue(report["ok"], json.dumps(report, indent=2))
        self.assertEqual(report["session_mismatches"], [])
        self.assertEqual(report["consumer_mismatches"], [])
        self.assertTrue(report["deterministic_replay"])

        cons = apply_changefeed(self.eng.con)
        self.assertEqual(cons["mismatches"], [])
        self.assertEqual(cons["materialized_sessions"],
                         report["published_session_count"])
        # every consumed delete referenced an existing materialized row
        self.assertGreater(cons["business_changes"], 0)

    def _mixed_stream(self):
        return [
            {"op": "upsert", "event_id": "a", "key": "u", "time": 0, "value": 3},
            {"op": "upsert", "event_id": "b", "key": "u", "time": 10, "value": 7},
            {"op": "upsert", "event_id": "br", "key": "u", "time": 5, "value": 2},
            {"op": "upsert", "event_id": "c", "key": "v", "time": 2, "value": 4},
            {"op": "upsert", "event_id": "d", "key": "v", "time": 3, "value": 6},
            {"op": "retract", "event_id": "br"},
            {"op": "watermark", "time": 6},
            {"op": "upsert", "event_id": "late", "key": "u", "time": 5,
             "value": 99},                       # rejected
            {"op": "upsert", "event_id": "e", "key": "v", "time": 20,
             "value": 8},                        # new session on v
            {"op": "watermark", "time": 15},
        ]

    # ------------------------------------------------- 10 replay idempotence
    def test_replay_produces_identical_business_output(self):
        ops = self._mixed_stream()
        self.feed(ops)
        snap1 = query_snapshot(self.eng.con)
        ch1 = [
            {k: v for k, v in c.items() if k != "watermark"}
            for b in query_batches(self.eng.con) for c in b["changes"]
        ]
        db2 = os.path.join(self.tmp.name, "replay.db")
        eng2 = fresh_engine(db2)
        for op in ops:
            eng2.handle(op)
        snap2 = query_snapshot(eng2.con)
        ch2 = [
            {k: v for k, v in c.items() if k != "watermark"}
            for b in query_batches(eng2.con) for c in b["changes"]
        ]
        self.assertEqual(snap1, snap2)
        self.assertEqual(ch1, ch2)
        eng2.close()

    # --------------------------------------------- 12 union no miss/duplicate
    def test_snapshot_union_disjoint_and_complete(self):
        self.feed([
            {"op": "upsert", "event_id": "a", "key": "u", "time": 0, "value": 1},
            {"op": "upsert", "event_id": "b", "key": "u", "time": 3, "value": 1},
            {"op": "upsert", "event_id": "c", "key": "u", "time": 50, "value": 1},
            {"op": "watermark", "time": 8},       # finalizes [a,b], keeps c
        ])
        snap = self.snapshot()
        all_members = [m for s in snap for m in s["members"]]
        self.assertEqual(sorted(all_members), ["a", "b", "c"])  # no dup/miss
        tiers = {(tuple(s["members"])): s["tier"] for s in snap}
        self.assertEqual(tiers[("a", "b")], "archive")
        self.assertEqual(tiers[("c",)], "hot")

    # ------------------------------------------------------- 13 provided data
    def test_provided_events_json_matches_probe_oracle(self):
        with open(os.path.join(os.path.dirname(__file__), "events.json")) as fh:
            ops = json.load(fh)
        with open(os.path.join(os.path.dirname(__file__), "profile.json")) as fh:
            gap = json.load(fh)["session_gap"]

        db = os.path.join(self.tmp.name, "provided.db")
        con = sqlite3.connect(db, isolation_level=None)
        Engine.initialize(con, gap, -10_000, 10_000, -10_000, 10_000)
        con.close()
        eng = Engine(db)
        results = [eng.handle(op) for op in ops]
        accepted = [r for r in results if r["accepted"]]
        rejected = [r for r in results if not r["accepted"]]
        self.assertEqual(len(rejected), 1)                 # the late event
        self.assertIn("watermark", rejected[0]["reason"])

        # probe.py is explicitly a small *current-set* oracle: run it over
        # the surviving hot events. Full correctness incl. the archive is
        # proven by the full-stream oracle replay below.
        active_events = [{
            "event_id": r["event_id"], "key": r["key_id"],
            "time": r["time"], "value": r["value"]}
            for r in eng.con.execute(
                "SELECT event_id,key_id,time,value FROM events")]
        probe_expected = sessionize(active_events, gap)
        oracle_state, _ = replay_oracle(ops, gap)

        snap = query_snapshot(eng.con)
        got = sorted(
            (s["key"], s["start"], s["end"], tuple(s["members"]), s["sum"])
            for s in snap if s["tier"] == "hot")
        exp_probe = sorted(
            (g["key"], g["start"], g["end"], tuple(g["ids"]), g["sum"])
            for g in probe_expected)
        self.assertEqual(got, exp_probe)

        exp_oracle = sorted(
            (g["key"], g["start"], g["end"], tuple(g["ids"]), g["sum"])
            for gs in oracle_state["sessions"].values() for g in gs)
        self.assertEqual(
            sorted((s["key"], s["start"], s["end"], tuple(s["members"]),
                    s["sum"]) for s in snap),
            exp_oracle)

        report = run_audit(eng.con)
        self.assertTrue(report["ok"], json.dumps(report, indent=2))
        eng.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

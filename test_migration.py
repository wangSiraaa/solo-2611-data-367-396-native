"""Barrier reproduction tests for the migration protocol.

Each test builds a fresh workdir. Interleavings are forced with explicit
threading.Events / held snapshots, not sleeps (sleeps only stretch the cutover
pause so a probe can observe it).

  T1  snapshot fixed, then business commits: old snapshot keeps reading old
      values, new writes still commit, outbox delivers them
  T2  new event applied at target, then stale backfill arrives: rev guard wins
  T3  copy cursor passed, then insert/delete behind it: outbox catches them,
      tombstones are not resurrected
  T4  after fence/cutover, old-generation writes to the old shard are rejected;
      a stale-route client is MOVED and re-executes nothing
  T5  request committed but receipt never consumed; same-id retry after
      cutover returns the original receipt and creates no new rev
  T6  same request_id with different content is rejected before and after
  T7  two business processes interleaved across cutover: one persistent
      decision per successful request, reads always one owner's complete set
Plus: idempotent resubmission, abort-on-divergence, cross-process steps.
"""
from pathlib import Path
import json
import shutil
import sqlite3
import tempfile
import threading
import time
import unittest

from prepare import prepare
from shard import Shard
from router import shard_path
from client import Client, ConflictError
from coordinator import Coordinator, MigrationError
from verify import verify

TENANT, OTHER = 'moving', 'stay'
G0, G1 = 1, 2


def wdir():
    return Path(tempfile.mkdtemp(prefix='mig-test-'))


def seed(workdir, batch_size=2):
    prepare(workdir)
    c = Coordinator.create(workdir, 'm1', TENANT, 'shard-a', 'shard-b', G0, G1, batch_size)
    return c


def drive_to_ready(c, workdir, writes_between=()):
    c.start()
    for req_id, payload in writes_between:
        Shard(shard_path(workdir, 'shard-a')).write(TENANT, G0, req_id, payload)
    while c.step(1)['phase'] == 'COPYING':
        pass
    return c.status()


class T1SnapshotFixed(unittest.TestCase):
    def test_writes_commit_while_snapshot_open(self):
        wd = wdir()
        c = seed(wd)
        st = c.start()
        c1 = st['boundary_clock']
        src = Shard(shard_path(wd, 'shard-a'))
        # business commits AFTER the snapshot was taken
        r = src.write(TENANT, G0, 'w1', {'op': 'put', 'row_id': 'r0', 'value': 777})
        self.assertGreater(r['rev'], c1)
        # the fixed snapshot still reads the old value; a fresh read sees the new one
        snap_row = [x for x in src.snapshot_rows(c._snap, TENANT, '', 100) if x['row_id'] == 'r0'][0]
        self.assertEqual(snap_row['value'], 0)
        fresh = src.read(TENANT, G0)
        self.assertEqual([x for x in fresh if x['row_id'] == 'r0'][0]['value'], 777)
        # backfill reads the OLD value, outbox delivers the NEW one
        st = c.step(1)
        self.assertEqual(st['stats']['backfill_rows_written'], 2)      # r0@1, r1@2 from old snapshot
        self.assertEqual(st['stats']['outbox_rows_written'], 1)        # r0@10 via outbox
        tgt = Shard(shard_path(wd, 'shard-b'))
        with tgt.connect() as conn:
            row = dict(conn.execute("SELECT * FROM records WHERE tenant=? AND row_id='r0'",
                                    (TENANT,)).fetchone())
        self.assertEqual((row['value'], row['rev']), (777, c1 + 1))
        drive_to_ready(c, wd)
        c.cutover()
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class T2StaleBackfillAfterNewEvent(unittest.TestCase):
    def test_rev_guard_wins(self):
        wd = wdir()
        c = seed(wd)
        c.start()
        src = Shard(shard_path(wd, 'shard-a'))
        r = src.write(TENANT, G0, 'upd-r5', {'op': 'put', 'row_id': 'r5', 'value': 555})
        c.step(1)                                   # backfills r0,r1; replay applies r5@9 NOW
        tgt = Shard(shard_path(wd, 'shard-b'))
        with tgt.connect() as conn:
            row = dict(conn.execute("SELECT * FROM records WHERE tenant=? AND row_id='r5'",
                                    (TENANT,)).fetchone())
        self.assertEqual((row['value'], row['rev']), (555, r['rev']))
        c.step(1)                                   # backfills r2,r3
        st = c.step(1)                              # backfills r4,r5: r5@5 arrives STALE
        with tgt.connect() as conn:
            row = dict(conn.execute("SELECT * FROM records WHERE tenant=? AND row_id='r5'",
                                    (TENANT,)).fetchone())
        self.assertEqual((row['value'], row['rev']), (555, r['rev']),
                         'stale backfill must not overwrite the newer event')
        # the stale row was read but not written
        self.assertLess(st['stats']['backfill_rows_written'], st['stats']['backfill_rows_read'])
        drive_to_ready(c, wd)
        c.cutover()
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class T3BehindCursor(unittest.TestCase):
    def test_insert_and_delete_behind_cursor(self):
        wd = wdir()
        c = seed(wd)
        c.start()
        c.step(1)                                   # cursor now at 'r1'
        src = Shard(shard_path(wd, 'shard-a'))
        ins = src.write(TENANT, G0, 'ins-r00', {'op': 'put', 'row_id': 'r00', 'value': 444})
        dele = src.write(TENANT, G0, 'del-r0', {'op': 'delete', 'row_id': 'r0'})
        ghost = src.write(TENANT, G0, 'del-zz', {'op': 'delete', 'row_id': 'zz'})  # never existed
        # delete r3 BEFORE its backfill: tombstone applied first, stale row arrives later
        dele3 = src.write(TENANT, G0, 'del-r3', {'op': 'delete', 'row_id': 'r3'})
        drive_to_ready(c, wd)
        tgt = Shard(shard_path(wd, 'shard-b'))
        with tgt.connect() as conn:
            recs = {r['row_id']: dict(r) for r in
                    conn.execute('SELECT * FROM records WHERE tenant=?', (TENANT,)).fetchall()}
        self.assertEqual((recs['r00']['value'], recs['r00']['rev']), (444, ins['rev']))
        self.assertEqual((recs['r0']['deleted'], recs['r0']['rev']), (1, dele['rev']))
        self.assertEqual((recs['zz']['deleted'], recs['zz']['rev']), (1, ghost['rev']))
        self.assertEqual((recs['r3']['deleted'], recs['r3']['rev']), (1, dele3['rev']),
                         'stale backfill must not resurrect a deleted row')
        c.cutover()
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class T4FenceRejectsOldRoute(unittest.TestCase):
    def test_old_generation_rejected_after_cutover(self):
        wd = wdir()
        c = seed(wd)
        drive_to_ready(c, wd)
        st = c.cutover()
        fc = st['fence_clock']
        src = Shard(shard_path(wd, 'shard-a'))
        # a brand-new request at the old generation is rejected by the fenced source
        with self.assertRaises(ValueError) as cm:
            src.write(TENANT, G0, 'new-at-old-gen', {'op': 'put', 'row_id': 'rx', 'value': 1})
        self.assertEqual(str(cm.exception), 'STALE_OWNER_GENERATION')
        # an old request retried at the fenced source still returns its stored
        # receipt -- that is not a new write and the source clock does not move
        again = src.write(TENANT, G0, 'seed-2', {'op': 'put', 'row_id': 'r2', 'value': 20})
        self.assertEqual(again['rev'], 3)
        with src.connect() as conn:
            clock = conn.execute('SELECT clock FROM tenant_state WHERE tenant=?', (TENANT,)).fetchone()[0]
        self.assertEqual(clock, fc)
        # a client with a pre-cutover route cache is MOVED and lands at gen 2
        stale = Client(wd, 'stale')
        stale.cache[TENANT] = {'owner': 'shard-a', 'generation': G0}
        r = stale.write(TENANT, 'stale-1', {'op': 'put', 'row_id': 'rx', 'value': 1})
        self.assertEqual((r['accepted_generation'], r['rev']), (G1, fc + 1))
        self.assertTrue(any(a['result'] == 'MOVED' for e in stale.log for a in e['attempts']))
        with src.connect() as conn:
            self.assertIsNone(conn.execute("SELECT 1 FROM requests WHERE tenant=? AND request_id='stale-1'",
                                           (TENANT,)).fetchone())
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class T5LostReceipt(unittest.TestCase):
    def test_retry_after_cutover_returns_original(self):
        wd = wdir()
        c = seed(wd)
        src = Shard(shard_path(wd, 'shard-a'))
        c.start()
        # request commits at the source; the client "never consumes" the receipt
        lost = src.write(TENANT, G0, 'lost-1', {'op': 'put', 'row_id': 'r9', 'value': 42})
        drive_to_ready(c, wd)
        c.cutover()
        tgt = Shard(shard_path(wd, 'shard-b'))
        clock_before = tgt.connect().execute('SELECT clock FROM tenant_state WHERE tenant=?',
                                             (TENANT,)).fetchone()[0]
        client = Client(wd, 'retrying')
        got = client.write(TENANT, 'lost-1', {'op': 'put', 'row_id': 'r9', 'value': 42})
        self.assertEqual(got, lost, 'same request_id + same content must return the original receipt')
        self.assertEqual(got['accepted_generation'], G0)
        clock_after = tgt.connect().execute('SELECT clock FROM tenant_state WHERE tenant=?',
                                            (TENANT,)).fetchone()[0]
        self.assertEqual(clock_before, clock_after, 'a replayed receipt must not allocate a new rev')
        with tgt.connect() as conn:
            n = conn.execute("SELECT count(*) FROM outbox WHERE tenant=? AND rev=?",
                             (TENANT, got['rev'])).fetchone()[0]
        self.assertEqual(n, 0, 'the replayed request must not appear in the target outbox')
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class T6RequestIdConflict(unittest.TestCase):
    def test_same_id_different_content_rejected(self):
        wd = wdir()
        c = seed(wd)
        client = Client(wd, 'x')
        client.write(TENANT, 'rid', {'op': 'put', 'row_id': 'r0', 'value': 1})
        with self.assertRaises(ConflictError):
            client.write(TENANT, 'rid', {'op': 'put', 'row_id': 'r0', 'value': 2})
        drive_to_ready(c, wd)
        c.cutover()
        # after cutover the conflict is still detected (receipt was imported)
        with self.assertRaises(ConflictError):
            client.write(TENANT, 'rid', {'op': 'put', 'row_id': 'r0', 'value': 3})
        # and the original content still returns the original receipt
        r = client.write(TENANT, 'rid', {'op': 'put', 'row_id': 'r0', 'value': 1})
        self.assertEqual(r['accepted_generation'], G0)
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class T7StormAcrossCutover(unittest.TestCase):
    def test_two_clients_one_decision_each(self):
        wd = wdir()
        c = seed(wd, batch_size=3)
        c.start()
        pause = threading.Event()
        probe_saw_retry = threading.Event()
        results, errors = {}, {}

        def storm(name, n):
            cl = Client(wd, name, retry_sleep=0.005)
            if name == 'probe':
                cl.on_attempt = lambda op, t, a: a['result'] == 'RETRY' and probe_saw_retry.set()
            out = []
            for i in range(n):
                if i == n // 2:
                    pause.wait(10)                  # hold some writes for the pause window
                try:
                    r = cl.write(TENANT, '%s-w%d' % (name, i),
                                 {'op': 'put', 'row_id': '%s-row%d' % (name, i), 'value': i})
                    out.append(r)
                    if i % 4 == 0:
                        cl.read(TENANT)
                except Exception as e:              # noqa: BLE001 - recorded and asserted below
                    errors.setdefault(name, []).append(str(e))
                    break
            results[name] = (cl, out)

        threads = [threading.Thread(target=storm, args=(n, 12)) for n in ('A', 'B', 'probe')]
        for t in threads:
            t.start()
        time.sleep(0.05)
        while c.step(1)['phase'] == 'COPYING':
            time.sleep(0.01)

        def after_fence(coord):
            pause.set()                             # release the held writes into the pause
            probe_saw_retry.wait(10)
            time.sleep(0.2)                         # stretch the observable pause

        st = c.cutover(hooks={'after_fence': after_fence})
        fc = st['fence_clock']
        for t in threads:
            t.join(30)
            self.assertFalse(t.is_alive())
        self.assertEqual(errors, {})
        self.assertTrue(probe_saw_retry.is_set(), 'probe must observe the RETRY window')

        # every successful request: exactly one persistent decision, exactly one rev
        receipts = {}
        for name, (cl, out) in results.items():
            for r in out:
                rid = r['request_id']
                self.assertNotIn(rid, receipts, 'duplicate receipt for one request_id')
                receipts[rid] = r
        revs = [r['rev'] for r in receipts.values()]
        self.assertEqual(len(revs), len(set(revs)), 'two requests must never share a rev')
        sa = sqlite3.connect(str(shard_path(wd, 'shard-a')))
        sb = sqlite3.connect(str(shard_path(wd, 'shard-b')))
        for rid, r in receipts.items():
            canon = json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
            if r['rev'] <= fc:
                self.assertEqual(r['accepted_generation'], G0)
                for db in (sa, sb):                 # committed pre-fence: on both shards, identical
                    row = db.execute('SELECT receipt FROM requests WHERE tenant=? AND request_id=?',
                                     (TENANT, rid)).fetchone()
                    self.assertIsNotNone(row, rid)
                    self.assertEqual(json.loads(row[0]), r)
            else:
                self.assertEqual(r['accepted_generation'], G1)
                self.assertIsNone(sa.execute('SELECT 1 FROM requests WHERE tenant=? AND request_id=?',
                                             (TENANT, rid)).fetchone())
                row = sb.execute('SELECT receipt FROM requests WHERE tenant=? AND request_id=?',
                                 (TENANT, rid)).fetchone()
                self.assertEqual(json.loads(row[0]), r)
        # reads: each client sees a single owner; owner flips at most once (a -> b)
        for name, (cl, out) in results.items():
            owners = [e['served_by']['owner'] for e in cl.log if e['op'] == 'read']
            self.assertLessEqual(len(set(owners)), 2)
            seen_b = False
            for o in owners:
                if o == 'shard-b':
                    seen_b = True
                else:
                    self.assertFalse(seen_b, 'a read was served by the old owner after the new one')
        # final quiescent read equals a direct dump of the one authoritative owner
        final = Client(wd, 'final').read(TENANT)
        self.assertEqual(final['owner'], 'shard-b')
        direct = Shard(shard_path(wd, 'shard-b')).read(TENANT, G1)
        self.assertEqual(final['rows'], direct)
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class IdempotentResubmit(unittest.TestCase):
    def test_same_migration_id_returns_current_phase(self):
        wd = wdir()
        c = seed(wd)
        again = Coordinator.create(wd, 'm1', TENANT, 'shard-a', 'shard-b', G0, G1, 2)
        self.assertEqual(again.j['phase'], 'INIT')
        with self.assertRaises(MigrationError):
            Coordinator.create(wd, 'm1', 'other-tenant', 'shard-a', 'shard-b', G0, G1, 2)
        c.start()
        self.assertEqual(c.start()['phase'], 'COPYING')     # re-start: no new boundary
        drive_to_ready(c, wd)
        st1 = c.cutover()
        st2 = c.cutover()                                    # resubmitted cutover: DONE, no reopen
        self.assertEqual(st2['phase'], 'DONE')
        self.assertEqual(st1['fence_clock'], st2['fence_clock'])
        src_state = st2['source_state']
        self.assertEqual((src_state['writable'], src_state['generation']), (0, G0))
        self.assertEqual(c.start()['phase'], 'DONE')         # start never reopens the old owner
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)

    def test_cutover_requires_ready(self):
        wd = wdir()
        c = seed(wd)
        c.start()
        with self.assertRaises(MigrationError) as cm:
            c.cutover()
        self.assertIn('NOT_READY', str(cm.exception))
        shutil.rmtree(wd)


class AbortOnDivergence(unittest.TestCase):
    def test_conflict_aborts_and_preserves_scene(self):
        wd = wdir()
        c = seed(wd)
        c.start()
        # inject a divergent row at the shadow target: same rev, different content
        tgt = Shard(shard_path(wd, 'shard-b'))
        with tgt.connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            conn.execute("INSERT INTO records VALUES (?,?,?,?,?)", (TENANT, 'r0', 12345, 1, 0))
        with self.assertRaises(ValueError) as cm:
            c.step(1)
        self.assertIn('REV_CONTENT_CONFLICT', str(cm.exception))
        st = c.status()
        self.assertEqual(st['phase'], 'ABORTED')
        self.assertIn('REV_CONTENT_CONFLICT', st['error'])
        with self.assertRaises(MigrationError):
            c.step(1)
        with self.assertRaises(MigrationError):
            c.cutover()
        # scene preserved: divergent row still there for forensics
        with tgt.connect() as conn:
            row = dict(conn.execute("SELECT * FROM records WHERE tenant=? AND row_id='r0'",
                                    (TENANT,)).fetchone())
        self.assertEqual(row['value'], 12345)
        shutil.rmtree(wd)


class CrossProcessSteps(unittest.TestCase):
    def test_steps_without_held_snapshot(self):
        """CLI-style: start in one coordinator object, step in fresh ones
        (no snapshot survives a process restart; each step re-opens one)."""
        wd = wdir()
        c = seed(wd)
        c.start()
        src = Shard(shard_path(wd, 'shard-a'))
        src.write(TENANT, G0, 'during-copy', {'op': 'put', 'row_id': 'r0', 'value': 5})
        for _ in range(20):
            st = Coordinator(wd, 'm1').step(1)      # a new object == a new process
            if st['phase'] != 'COPYING':
                break
        self.assertEqual(st['phase'], 'READY')
        st = Coordinator(wd, 'm1').cutover()
        self.assertEqual(st['phase'], 'DONE')
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


class CutoverCrashResume(unittest.TestCase):
    """Every cutover mutation is durable before it is journaled; a crash in a
    window is reconciled from the durable shard/route state on the next run."""

    def test_crash_between_fence_and_journal(self):
        wd = wdir()
        c = seed(wd)
        drive_to_ready(c, wd)
        # simulate: fence committed at the source, coordinator died before journaling
        fc = Shard(shard_path(wd, 'shard-a')).fence(TENANT, G0)
        st = Coordinator(wd, 'm1').cutover()
        self.assertEqual(st['phase'], 'DONE')
        self.assertEqual(st['fence_clock'], fc)
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)

    def test_crash_between_activate_and_journal(self):
        wd = wdir()
        c = seed(wd)
        drive_to_ready(c, wd)

        def crash_after_activate(coord):
            Shard(shard_path(wd, 'shard-b')).activate(TENANT, G1)   # durable...
            raise RuntimeError('simulated crash before journaling')  # ...but not journaled

        with self.assertRaises(RuntimeError):
            c.cutover(hooks={'after_catchup': crash_after_activate})
        self.assertEqual(Coordinator(wd, 'm1').status()['phase'], 'CAUGHT_UP')
        st = Coordinator(wd, 'm1').cutover()                        # reconciles ACTIVATED
        self.assertEqual(st['phase'], 'DONE')
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)

    def test_crash_between_publish_and_journal(self):
        wd = wdir()
        c = seed(wd)
        drive_to_ready(c, wd)

        def crash_after_publish(coord):
            from router import Router
            Router(wd).publish(TENANT, 'shard-b', G1)               # durable...
            raise RuntimeError('simulated crash before journaling')

        with self.assertRaises(RuntimeError):
            c.cutover(hooks={'after_activate': crash_after_publish})
        self.assertEqual(Coordinator(wd, 'm1').status()['phase'], 'ACTIVATED')
        st = Coordinator(wd, 'm1').cutover()                        # reconciles DONE
        self.assertEqual(st['phase'], 'DONE')
        self.assertTrue(verify(wd, TENANT, 'm1'))
        shutil.rmtree(wd)


if __name__ == '__main__':
    unittest.main(verbosity=2)

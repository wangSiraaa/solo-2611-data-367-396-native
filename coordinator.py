"""Migration coordinator: online tenant migration shard-a -> shard-b.

Design (snapshot + outbox catch-up, fence-and-publish cutover):

  INIT       validate routes/tenant states, create the durable journal.
  COPYING    boundary C1 = source tenant clock at the first snapshot (journaled).
             Backfill rows in stable row_id order from a held read snapshot
             (WAL: business writes keep committing while it is open), page
             pre-existing request receipts by request_id, and replay source
             outbox events with rev > hwm (hwm starts at C1). The target is a
             shadow: it applies rows by source rev, never by arrival order, so
             stale backfill can never overwrite a newer event and deletes are
             never resurrected.
  READY      row cursor and receipt cursor both exhausted.
  FENCED     source.fence(start_generation) -> fence_clock, journaled. The
             source now rejects new writes (duplicates still return stored
             receipts, which is not a new write).
  CAUGHT_UP  outbox replayed until hwm == fence_clock. All receipts are now at
             the target: rev <= C1 came via receipt paging, C1 < rev <=
             fence_clock came with their outbox events.
  ACTIVATED  target.activate(new_generation). Target clock == fence_clock, so
             the first new target write gets rev fence_clock+1: imported revs
             and new revs can never collide.
  DONE       routing.json published {owner: target, generation: new_generation}.

No cross-database atomicity is claimed: fence (source db), activate (target db)
and publish (routing file) are three independent durable commits. Safety comes
from order + idempotence: the source is fenced before the target is activated
(so two owners never accept new writes at once), every step is idempotent, and
a crashed cutover reconciles the journaled phase with the durable shard state
and resumes. Between fence and publish neither owner accepts new writes: that
is the one explicit, observable pause where clients see RETRY.

Completeness invariant (why re-opening a snapshot after a restart is safe):
every committed rev r either has r <= C1 and is therefore visible to EVERY
backfill snapshot (all snapshots are taken at clock >= C1), or has r > C1 and
is therefore replayed from the outbox (hwm starts at C1 and the outbox has no
commit holes). Receipts: rev <= C1 receipts exist before the migration starts
and are always found by paging; rev > C1 receipts arrive with their events.
"""
from pathlib import Path
import fcntl
import json
import os
import tempfile
import time

from shard import Shard
from router import Router, shard_path

PHASES = ('INIT', 'COPYING', 'READY', 'FENCED', 'CAUGHT_UP', 'ACTIVATED', 'DONE', 'ABORTED')


class MigrationError(Exception):
    pass


def _utc():
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime()) + ('%.3f' % (time.time() % 1))[1:]


class Coordinator:
    def __init__(self, workdir, migration_id):
        self.workdir = Path(workdir)
        self.migration_id = migration_id
        self.jpath = self.workdir / ('migration-%s.json' % migration_id)
        self.lockpath = self.workdir / ('migration-%s.lock' % migration_id)
        self.router = Router(workdir)
        self.j = None
        self._snap = None        # held read snapshot connection (in-process only)
        self._snap_clock = None
        self._load()
        if self.j is None:
            raise MigrationError('NO_SUCH_MIGRATION: ' + migration_id)

    # ---------------------------------------------------------------- journal
    def _load(self):
        if self.jpath.exists():
            self.j = json.loads(self.jpath.read_text())
            self.source = Shard(shard_path(self.workdir, self.j['source']))
            self.target = Shard(shard_path(self.workdir, self.j['target']))

    def _save(self):
        fd, tmp = tempfile.mkstemp(dir=str(self.workdir), prefix='.mig-', suffix='.tmp')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(self.j, f, ensure_ascii=False, sort_keys=True, indent=1)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.jpath)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def _transition(self, phase, **fields):
        note = fields.pop('note', None)
        self.j.update(fields)
        self.j['phase'] = phase
        entry = {'ts': _utc(), 'phase': phase,
                 'route': self.router.load().get(self.j['tenant']),
                 'source_state': self._state(self.source),
                 'target_state': self._state(self.target)}
        entry.update(fields)
        if note:
            entry['note'] = note
        self.j['history'].append(entry)
        self._save()

    def _state(self, shard):
        with shard.connect() as c:
            r = c.execute('SELECT * FROM tenant_state WHERE tenant=?', (self.j['tenant'],)).fetchone()
            return dict(r) if r else None

    # ------------------------------------------------------------------ lock
    class _op:
        def __init__(self, path):
            self.fd = open(path, 'a+')
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.fd.close()
                raise MigrationError('COORDINATOR_BUSY')

        def __enter__(self):
            return self

        def __exit__(self, *a):
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()

    def _oplock(self):
        return self._op(self.lockpath)

    # ------------------------------------------------------------------ init
    @classmethod
    def create(cls, workdir, migration_id, tenant, source, target,
               start_generation, new_generation, batch_size):
        """init entry point. Re-submitting an existing migration id returns the
        current phase instead of starting over."""
        workdir = Path(workdir)
        jpath = workdir / ('migration-%s.json' % migration_id)
        if jpath.exists():
            existing = cls(workdir, migration_id)
            want = {'tenant': tenant, 'source': source, 'target': target,
                    'start_generation': start_generation, 'new_generation': new_generation}
            for k, v in want.items():
                if existing.j[k] != v:
                    raise MigrationError('MIGRATION_ID_CONFLICT: %s != %s' % (k, v))
            return existing
        if not (1 <= batch_size <= 100):
            raise MigrationError('BATCH_SIZE_OUT_OF_RANGE')
        if new_generation <= start_generation:
            raise MigrationError('NEW_GENERATION_MUST_EXCEED_START')
        router = Router(workdir)
        route = router.route(tenant)
        if route['owner'] != source or route['generation'] != start_generation:
            raise MigrationError('ROUTE_NOT_AT_SOURCE')
        src = Shard(shard_path(workdir, source))
        tgt = Shard(shard_path(workdir, target))
        with src.connect() as c:
            s = c.execute('SELECT * FROM tenant_state WHERE tenant=?', (tenant,)).fetchone()
            if not s or s['generation'] != start_generation or not s['writable']:
                raise MigrationError('SOURCE_NOT_WRITABLE_OWNER')
        with tgt.connect() as c:
            t = c.execute('SELECT * FROM tenant_state WHERE tenant=?', (tenant,)).fetchone()
        if t is None:
            tgt.initialize_tenant(tenant, start_generation, False)
        elif t['writable'] or t['generation'] != start_generation:
            raise MigrationError('TARGET_NOT_CLEAN_SHADOW')
        j = {'migration_id': migration_id, 'tenant': tenant, 'source': source, 'target': target,
             'start_generation': start_generation, 'new_generation': new_generation,
             'batch_size': batch_size, 'phase': 'INIT',
             'boundary_clock': None, 'cursor': '', 'receipt_cursor': '',
             'rows_done': False, 'receipts_done': False, 'hwm': None, 'fence_clock': None,
             'stats': {'backfill_rows_read': 0, 'backfill_rows_written': 0,
                       'outbox_events_replayed': 0, 'outbox_rows_written': 0,
                       'receipts_read': 0, 'receipts_imported': 0},
             'history': [], 'error': None, 'created': _utc()}
        self = cls.__new__(cls)
        self.workdir, self.migration_id = workdir, migration_id
        self.jpath, self.lockpath = jpath, workdir / ('migration-%s.lock' % migration_id)
        self.router, self.j = router, j
        self.source, self.target = src, tgt
        self._snap = self._snap_clock = None
        self._transition('INIT')
        return self

    # ----------------------------------------------------------------- start
    def start(self):
        """start entry point: fix the source snapshot and the continuation
        boundary C1. Idempotent: an existing later phase is just reported."""
        with self._oplock():
            self._load()
            if self.j['phase'] == 'ABORTED':
                raise MigrationError('ABORTED: ' + str(self.j['error']))
            if self.j['phase'] != 'INIT':
                return self.status()
            conn, clock = self.source.snapshot(self.j['tenant'])
            self._snap, self._snap_clock = conn, clock
            self.j['boundary_clock'] = clock
            self.j['hwm'] = clock
            self._transition('COPYING', boundary_clock=clock)
            return self.status()

    # ------------------------------------------------------------------ step
    def _ensure_snapshot(self):
        """In-process the snapshot from start() is reused (fixed old snapshot).
        After a process restart a fresh snapshot is opened for this step only;
        correctness does not depend on which snapshot a row is read from (see
        module docstring)."""
        if self._snap is None:
            self._snap, self._snap_clock = self.source.snapshot(self.j['tenant'])
        return self._snap

    def _close_snapshot(self):
        if self._snap is not None:
            self._snap.close()
            self._snap = self._snap_clock = None

    def _target_rev(self, row_id):
        with self.target.connect() as c:
            r = c.execute('SELECT rev FROM records WHERE tenant=? AND row_id=?',
                          (self.j['tenant'], row_id)).fetchone()
            return r['rev'] if r else None

    def _has_receipt(self, request_id):
        with self.target.connect() as c:
            return c.execute('SELECT 1 FROM requests WHERE tenant=? AND request_id=?',
                             (self.j['tenant'], request_id)).fetchone() is not None

    def _apply_backfill(self, row):
        st = self.j['stats']
        st['backfill_rows_read'] += 1
        prior = self._target_rev(row['row_id'])
        if prior is None or row['rev'] > prior:
            st['backfill_rows_written'] += 1
        self.target.apply_snapshot_row(row)   # rev-guarded: stale rows are dropped by the shard

    def _replay(self, max_events=100000):
        """Replay committed outbox events with rev > hwm, in rev order."""
        st = self.j['stats']
        done = 0
        while done < max_events:
            events = self.source.events_after(self.j['tenant'], self.j['hwm'], 500)
            if not events:
                return
            for ev in events:
                row = ev['row']
                prior = self._target_rev(row['row_id'])
                if prior is None or row['rev'] > prior:
                    st['outbox_rows_written'] += 1
                if not self._has_receipt(ev['request']['request_id']):
                    st['receipts_imported'] += 1
                self.target.apply_event(ev)   # row + original receipt, one target txn
                self.j['hwm'] = row['rev']
                st['outbox_events_replayed'] += 1
                done += 1
                if done >= max_events:
                    return

    def step(self, batches=1):
        """step entry point: copy `batches` row batches + receipt batches from
        the snapshot, then catch up on newly committed outbox events."""
        with self._oplock():
            self._load()
            ph = self.j['phase']
            if ph == 'ABORTED':
                raise MigrationError('ABORTED: ' + str(self.j['error']))
            if ph == 'INIT':
                raise MigrationError('NOT_STARTED')
            if ph != 'COPYING':
                return self.status()          # READY or beyond: nothing to do
            tenant, bs = self.j['tenant'], self.j['batch_size']
            try:
                snap = self._ensure_snapshot()
                for _ in range(batches):
                    if not self.j['rows_done']:
                        rows = self.source.snapshot_rows(snap, tenant, self.j['cursor'], bs)
                        for row in rows:
                            self._apply_backfill(row)
                        if rows:
                            self.j['cursor'] = rows[-1]['row_id']
                        if len(rows) < bs:
                            self.j['rows_done'] = True
                    if not self.j['receipts_done']:
                        recs = self.source.receipt_rows(tenant, self.j['receipt_cursor'], bs)
                        for r in recs:
                            self.j['stats']['receipts_read'] += 1
                            if not self._has_receipt(r['request_id']):
                                self.j['stats']['receipts_imported'] += 1
                            self.target.import_receipt(r)
                        if recs:
                            self.j['receipt_cursor'] = recs[-1]['request_id']
                        if len(recs) < bs:
                            self.j['receipts_done'] = True
                    self._save()          # per-batch: a crash re-runs at most one batch
                self._replay()
                if self.j['rows_done'] and self.j['receipts_done']:
                    self._transition('READY')
                else:
                    self._save()
            except ValueError as e:
                if 'CONFLICT' in str(e):
                    self._abort(str(e))
                raise
            return self.status()

    # --------------------------------------------------------------- cutover
    def _reconcile(self):
        """Adopt durable shard/route state that a crashed cutover committed
        before journaling. Every cutover mutation is idempotent, so observing
        it is proof it happened."""
        tenant = self.j['tenant']
        ph = self.j['phase']
        if ph in ('COPYING', 'READY'):
            s = self._state(self.source)
            if s and not s['writable'] and s['generation'] == self.j['start_generation']:
                self._transition('FENCED', fence_clock=s['clock'], note='reconciled')
                ph = 'FENCED'
        if ph in ('FENCED', 'CAUGHT_UP'):
            t = self._state(self.target)
            if t and t['writable'] and t['generation'] == self.j['new_generation']:
                if self.j['hwm'] != self.j['fence_clock']:
                    raise MigrationError('CATCHUP_INCOMPLETE_BUT_ACTIVATED')
                self._transition('ACTIVATED', note='reconciled')
                ph = 'ACTIVATED'
        if ph == 'ACTIVATED':
            r = self.router.route(tenant)
            if r['owner'] == self.j['target'] and r['generation'] == self.j['new_generation']:
                self._transition('DONE', note='reconciled')

    def cutover(self, hooks=None):
        """cutover entry point: fence -> catch up to the fence boundary ->
        activate target at the new generation -> publish the new route.
        Requires READY; resumable after a crash via _reconcile()."""
        hooks = hooks or {}
        with self._oplock():
            self._load()
            self._reconcile()
            ph = self.j['phase']
            if ph == 'DONE':
                return self.status()
            if ph == 'ABORTED':
                raise MigrationError('ABORTED: ' + str(self.j['error']))
            if ph in ('INIT', 'COPYING'):
                raise MigrationError('NOT_READY: finish copy steps first')
            try:
                if self.j['phase'] == 'READY':
                    fc = self.source.fence(self.j['tenant'], self.j['start_generation'])
                    self._transition('FENCED', fence_clock=fc)
                    if 'after_fence' in hooks:
                        hooks['after_fence'](self)
                if self.j['phase'] == 'FENCED':
                    for _ in range(1000):     # source is fenced: replay terminates
                        self._replay()
                        if self.j['hwm'] == self.j['fence_clock']:
                            break
                    else:
                        raise MigrationError('CATCHUP_INCOMPLETE')
                    self._transition('CAUGHT_UP')
                    if 'after_catchup' in hooks:
                        hooks['after_catchup'](self)
                if self.j['phase'] == 'CAUGHT_UP':
                    self.target.activate(self.j['tenant'], self.j['new_generation'])
                    self._transition('ACTIVATED')
                    if 'after_activate' in hooks:
                        hooks['after_activate'](self)
                if self.j['phase'] == 'ACTIVATED':
                    self.router.publish(self.j['tenant'], self.j['target'], self.j['new_generation'])
                    self._transition('DONE')
                    if 'after_publish' in hooks:
                        hooks['after_publish'](self)
            except ValueError as e:
                if 'CONFLICT' in str(e) or 'MISMATCH' in str(e):
                    self._abort(str(e))
                raise
            finally:
                self._close_snapshot()
            return self.status()

    # ----------------------------------------------------------------- misc
    def _abort(self, reason):
        self.j['error'] = reason
        self._close_snapshot()
        self._transition('ABORTED')

    def status(self):
        self._load()
        out = dict(self.j)
        out['route'] = self.router.load().get(self.j['tenant'])
        out['source_state'] = self._state(self.source)
        out['target_state'] = self._state(self.target)
        return out

    def cleanup(self, confirm=False):
        """Optional: delete THIS tenant's rows from the old source after DONE.
        Never touches other tenants. Not required; the demo keeps the scene."""
        if not confirm:
            raise MigrationError('CLEANUP_NEEDS_CONFIRM')
        with self._oplock():
            self._load()
            if self.j['phase'] != 'DONE':
                raise MigrationError('NOT_DONE')
            route = self.router.route(self.j['tenant'])
            if route['owner'] != self.j['target']:
                raise MigrationError('ROUTE_NOT_MOVED')
            t = self.j['tenant']
            with self.source.connect() as c:
                c.execute('BEGIN IMMEDIATE')
                for table in ('records', 'requests', 'outbox', 'tenant_state'):
                    c.execute('DELETE FROM %s WHERE tenant=?' % table, (t,))
            self._transition('DONE', note='source-cleaned')
            return self.status()

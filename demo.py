"""Full migration schedule demo with controlled barriers.

Usage: python3 -B demo.py <fresh-workdir>

Schedule (all phases printed at the end, with routes and generations):
  0  prepare two real shards + routing.json, seed data
  1  pre-migration business traffic (receipts that must survive the move)
  2  init + start: fix source snapshot and continuation boundary C1
  3  BARRIER: business commits while the snapshot read txn is open
  4  step-wise backfill by row_id + receipt paging + outbox catch-up,
     with scenario writes interleaved (update/delete/insert-behind-cursor)
  5  a committed request whose receipt the client "never consumed"
  6  cutover: fence -> catch-up -> activate -> publish, with a probe client
     observing the RETRY window inside the pause
  7  post-cutover: same-id retries return original receipts, conflicting id
     rejected, stale-route client gets MOVED, untouched tenant keeps working
  8  independent row-by-row / request-by-request verification
"""
from pathlib import Path
import json
import sys
import threading

sys.path.insert(0, str(Path(__file__).parent))
from prepare import prepare
from shard import Shard
from router import shard_path
from client import Client, ConflictError
from coordinator import Coordinator
from verify import verify

SCHED = []


def note(phase, msg, **kw):
    SCHED.append({'phase': phase, 'msg': msg, **kw})
    print('  [%-4s] %s' % (phase, msg))


def fail(msg):
    print('DEMO FAILED: ' + msg)
    sys.exit(1)


def main():
    if len(sys.argv) != 2:
        raise SystemExit('usage: python3 -B demo.py <fresh-workdir>')
    workdir = Path(sys.argv[1])
    if workdir.exists():
        raise SystemExit('workdir %s exists; every demo run needs a fresh directory' % workdir)
    scenario = json.loads((Path(__file__).parent / 'scenarios.json').read_text())
    tenant, untouched = scenario['tenant'], scenario['untouched_tenant']
    src_name, tgt_name = scenario['source'], scenario['target']
    g0, g1 = scenario['start_generation'], scenario['new_generation']
    bs = scenario['snapshot_batch_rows']

    print('phase 0: prepare %s' % workdir)
    prepare(workdir)
    source = Shard(shard_path(workdir, src_name))

    print('phase 1: pre-migration business traffic')
    A = Client(workdir, 'A')
    B = Client(workdir, 'B')
    A.write(tenant, 'pre-migration-a', {'op': 'put', 'row_id': 'pa1', 'value': 101})
    B.write(untouched, 'stay-live-1', {'op': 'put', 'row_id': 's2', 'value': 5})
    note('1', 'pre-migration writes committed at source', extra='pre-migration-a, stay-live-1')

    print('phase 2: init + start')
    coord = Coordinator.create(workdir, 'm1', tenant, src_name, tgt_name, g0, g1, bs)
    st = coord.start()
    c1 = st['boundary_clock']
    note('2', 'snapshot fixed, continuation boundary C1=%d' % c1, boundary=c1)

    print('phase 3: business commits while the snapshot read txn is open')
    r = A.write(tenant, scenario['operations_during_copy'][0]['request_id'],
                scenario['operations_during_copy'][0]['payload'])
    assert r['rev'] > c1, 'write must commit past the snapshot boundary'
    snap_row = [x for x in source.snapshot_rows(coord._snap, tenant, '', 100) if x['row_id'] == 'r0'][0]
    assert snap_row['value'] == 0, 'fixed snapshot still reads the old value'
    note('3', 'write committed at rev %d while snapshot reads old r0=%d' % (r['rev'], snap_row['value']))

    print('phase 4: step-wise copy with scenario writes interleaved')
    st = coord.step(1)          # batch 1: r0,r1 from the OLD snapshot, then catch-up
    note('4', 'step 1: backfilled r0,r1 as of C1, then caught up (r0 -> 777 via outbox)',
         stats=st['stats'])
    for op in scenario['operations_during_copy'][1:]:
        A.write(tenant, op['request_id'], op['payload'])
    B.write(tenant, 'pre-cutover-b', {'op': 'put', 'row_id': 'r5', 'value': 555})
    note('4', 'delete r1 + insert r00 behind cursor + update r5 committed at source')
    while coord.step(1)['phase'] == 'COPYING':
        pass
    st = coord.status()
    note('4', 'copy phase READY', stats=st['stats'])
    tgt = Shard(shard_path(workdir, tgt_name))
    with tgt.connect() as c:
        row = dict(c.execute("SELECT * FROM records WHERE tenant=? AND row_id='r1'", (tenant,)).fetchone())
    assert row['deleted'] == 1, 'tombstone for r1 must be at the shadow target'
    note('4', 'shadow target holds r1 tombstone rev %d (delete not resurrected)' % row['rev'])

    print('phase 5: a request commits; the client never consumes the receipt')
    lost = source.write(tenant, g0, 'lost-receipt', {'op': 'put', 'row_id': 'r9', 'value': 999})
    note('5', 'lost-receipt committed at source rev %d, response dropped' % lost['rev'])

    print('phase 6: cutover with a probe client inside the pause')
    pause_begin = threading.Event()
    probe_saw_retry = threading.Event()
    probe_result = {}
    probe = Client(workdir, 'probe', retry_sleep=0.01)
    probe.on_attempt = lambda op, t, att: att['result'] == 'RETRY' and probe_saw_retry.set()

    def probe_write():
        probe_result['write'] = probe.write(tenant, 'during-pause', {'op': 'put', 'row_id': 'r7', 'value': 7})

    def probe_read():
        probe_result['read'] = probe.read(tenant)

    pause_begin.set()           # probes may attempt as soon as they like; they will hit the fence
    tw = threading.Thread(target=probe_write)
    tr = threading.Thread(target=probe_read)

    def after_fence(c):
        tw.start(); tr.start()
        if not probe_saw_retry.wait(10):
            fail('probe never observed the RETRY window')

    st = coord.cutover(hooks={'after_fence': after_fence})
    tw.join(); tr.join()
    fc = st['fence_clock']
    w = probe_result['write']
    assert w['accepted_generation'] == g1 and w['rev'] == fc + 1, w
    rd = probe_result['read']
    assert (rd['owner'], rd['generation']) == (tgt_name, g1), rd
    n_retry = sum(1 for e in probe.log for a in e.get('attempts', []) if a['result'] == 'RETRY')
    n_moved = sum(1 for e in probe.log for a in e.get('attempts', []) if a['result'] == 'MOVED')
    note('6', 'cutover DONE: fence_clock=%d, probe saw %d RETRY + %d MOVED, first new-gen write rev %d'
         % (fc, n_retry, n_moved, w['rev']))

    print('phase 7: post-cutover retries, conflict, stale route, untouched tenant')
    clock_before = Shard(shard_path(workdir, tgt_name)).connect().execute(
        'SELECT clock FROM tenant_state WHERE tenant=?', (tenant,)).fetchone()[0]
    r1 = A.write(tenant, scenario['retry_after_cutover'],
                 scenario['operations_during_copy'][0]['payload'])
    assert r1['rev'] == scenario_rev(coord, scenario['retry_after_cutover']) and r1['accepted_generation'] == g0
    r2 = A.write(tenant, 'lost-receipt', {'op': 'put', 'row_id': 'r9', 'value': 999})
    assert r2 == lost, 'retry after cutover must return the original receipt, got %r want %r' % (r2, lost)
    r3 = A.write(tenant, 'seed-3', {'op': 'put', 'row_id': 'r3', 'value': 30})
    assert r3['rev'] == 4 and r3['accepted_generation'] == g0, 'pre-migration receipt must replay'
    clock_after = Shard(shard_path(workdir, tgt_name)).connect().execute(
        'SELECT clock FROM tenant_state WHERE tenant=?', (tenant,)).fetchone()[0]
    assert clock_after == clock_before, 'replayed receipts must not create new revs'
    note('7', 'same-id retries returned original receipts (rev %d,%d,%d), no new revs'
         % (r1['rev'], r2['rev'], r3['rev']))
    try:
        A.write(tenant, scenario['retry_after_cutover'], {'op': 'put', 'row_id': 'r0', 'value': 999})
        fail('same request_id with different content must be rejected')
    except ConflictError:
        note('7', 'same request_id + different content -> REQUEST_ID_CONFLICT')
    stale = Client(workdir, 'stale')
    stale.cache[tenant] = {'owner': src_name, 'generation': g0}   # route cached before cutover
    rs = stale.write(tenant, 'stale-route-write', {'op': 'put', 'row_id': 'r8', 'value': 888})
    assert rs['accepted_generation'] == g1 and rs['rev'] > fc
    assert any(a['result'] == 'MOVED' for e in stale.log for a in e['attempts'])
    with source.connect() as c:
        assert c.execute("SELECT 1 FROM requests WHERE tenant=? AND request_id='stale-route-write'",
                         (tenant,)).fetchone() is None
    note('7', 'stale-route write: old owner rejected, MOVED, executed at gen %d rev %d; source untouched'
         % (g1, rs['rev']))
    B.write(tenant, 'post-cutover-b', {'op': 'put', 'row_id': 'r6', 'value': 666})
    B.write(untouched, 'stay-live-2', {'op': 'put', 'row_id': 's3', 'value': 6})
    readback = A.read(tenant)
    assert readback['owner'] == tgt_name
    note('7', 'post-cutover writes at target; %s still served from %s' % (untouched, src_name))

    print('phase 8: independent verification')
    ok = verify(workdir, tenant, 'm1')
    st = coord.status()
    print('\n==== schedule (journal history) ====')
    for h in st['history']:
        print('  %-19s %-9s route=%s src(gen=%s,w=%s,clock=%s) tgt(gen=%s,w=%s,clock=%s)' % (
            h['ts'], h['phase'], h['route'],
            h['source_state']['generation'], h['source_state']['writable'], h['source_state']['clock'],
            h['target_state']['generation'], h['target_state']['writable'], h['target_state']['clock']))
    print('\n==== stats ====')
    print(json.dumps(st['stats'], indent=1, sort_keys=True))
    print('\n==== demo notes ====')
    for n in SCHED:
        print(' ', n['phase'], n['msg'])
    if not ok:
        fail('independent verification failed')
    print('\nDEMO OK')


def scenario_rev(coord, request_id):
    with coord.source.connect() as c:
        r = c.execute('SELECT receipt FROM requests WHERE tenant=? AND request_id=?',
                      (coord.j['tenant'], request_id)).fetchone()
        return json.loads(r['receipt'])['rev']


if __name__ == '__main__':
    main()

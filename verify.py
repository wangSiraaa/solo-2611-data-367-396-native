"""Independent post-migration verifier.

Reads the two SQLite databases, routing.json and the migration journal directly
(raw sqlite3, no coordinator code) and re-derives what the world must look like:

  * the source outbox is the ground truth for every committed decision up to the
    fence boundary: revs 1..fence_clock, contiguous (the facility commits each
    outbox row in the same transaction that allocates its rev, so no holes);
  * replaying source events <= fence_clock, then target events > fence_clock,
    must reproduce the target records EXACTLY, row by row (value, rev, deleted);
  * every request receipt must exist at the target identical to the source
    (content hash + original receipt), request by request -- including receipts
    that existed before the migration started;
  * no duplicate execution: one request_id -> one distinct receipt -> one rev;
    imported revs (<= fence_clock) and new target revs (> fence_clock) never
    overlap. We compare request_id/rev/receipts, never dedup by value.

Run: python3 -B verify.py <workdir> [--tenant T] [--migration-id m1]
Exit code 0 = all checks pass, 1 = at least one failure.
"""
from pathlib import Path
import argparse
import json
import sqlite3
import sys


def canonical(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


class Report:
    def __init__(self):
        self.checks = []

    def check(self, name, ok, detail=''):
        self.checks.append((name, bool(ok), detail))
        print(('PASS' if ok else 'FAIL'), name, ('-- ' + str(detail)[:400] if detail else ''))

    @property
    def ok(self):
        return all(ok for _, ok, _ in self.checks)


def rows(c, sql, args=()):
    c.row_factory = sqlite3.Row
    return [dict(r) for r in c.execute(sql, args).fetchall()]


def verify(workdir, tenant=None, migration_id=None):
    workdir = Path(workdir)
    rep = Report()
    routing = json.loads((workdir / 'routing.json').read_text())
    journals = sorted(workdir.glob('migration-*.json'))
    journal = None
    if migration_id:
        journal = json.loads((workdir / ('migration-%s.json' % migration_id)).read_text())
    elif len(journals) == 1:
        journal = json.loads(journals[0].read_text())
    if tenant is None:
        if journal is None:
            raise SystemExit('no migration journal found; pass --tenant')
        tenant = journal['tenant']
    if journal is None:
        raise SystemExit('verify needs the migration journal to check claims against the databases')
    source_name, target_name = journal['source'], journal['target']
    start_gen, new_gen = journal['start_generation'], journal['new_generation']
    a = sqlite3.connect(str(workdir / (source_name + '.sqlite')))
    b = sqlite3.connect(str(workdir / (target_name + '.sqlite')))

    # ---- A. routing and tenant states -------------------------------------
    route = routing.get(tenant)
    rep.check('A1 route points at target with new generation',
              route == {'owner': target_name, 'generation': new_gen}, route)
    sa = rows(a, 'SELECT * FROM tenant_state WHERE tenant=?', (tenant,))
    sa = sa[0] if sa else None
    rep.check('A2 source tenant fenced at start generation',
              sa and sa['generation'] == start_gen and sa['writable'] == 0, sa)
    ta = rows(b, 'SELECT * FROM tenant_state WHERE tenant=?', (tenant,))
    ta = ta[0] if ta else None
    rep.check('A3 target tenant writable at new generation',
              ta and ta['generation'] == new_gen and ta['writable'] == 1, ta)

    # ---- B. fence boundary -------------------------------------------------
    fence_clock = sa['clock'] if sa else None
    rep.check('B1 journal fence_clock == source clock', journal.get('fence_clock') == fence_clock,
              {'journal': journal.get('fence_clock'), 'source_clock': fence_clock})
    ob_a = rows(a, 'SELECT rev, event FROM outbox WHERE tenant=? ORDER BY rev', (tenant,))
    revs = [r['rev'] for r in ob_a]
    rep.check('B2 source outbox is contiguous 1..fence_clock',
              revs == list(range(1, fence_clock + 1)), {'count': len(revs), 'fence_clock': fence_clock})
    max_src_rec_rev = rows(a, 'SELECT max(rev) m FROM records WHERE tenant=?', (tenant,))[0]['m'] or 0
    rep.check('B3 no source row newer than the fence boundary', max_src_rec_rev <= fence_clock,
              max_src_rec_rev)

    # ---- C/D. row-by-row: replay events -> model, compare both shards ------
    events_a = [(r['rev'], json.loads(r['event'])) for r in ob_a]
    model = {}
    seq_ok = True
    for expect_rev, (rev, ev) in enumerate(events_a, start=1):
        if rev != expect_rev or ev['row']['rev'] != rev:
            seq_ok = False
        row = ev['row']
        model[row['row_id']] = (row['value'], row['rev'], row['deleted'])
        if ev['request']['receipt']['rev'] != rev:
            seq_ok = False
    rep.check('C1 source outbox events self-consistent (rev order, receipt.rev)', seq_ok)
    src_records = {r['row_id']: (r['value'], r['rev'], r['deleted'])
                   for r in rows(a, 'SELECT * FROM records WHERE tenant=?', (tenant,))}
    rep.check('C2 source records == replay of source outbox', src_records == model,
              {'diff': {k: (src_records.get(k), model.get(k))
                        for k in set(src_records) | set(model) if src_records.get(k) != model.get(k)}})

    tgt_records = {r['row_id']: (r['value'], r['rev'], r['deleted'])
                   for r in rows(b, 'SELECT * FROM records WHERE tenant=?', (tenant,))}
    ob_b = rows(b, 'SELECT rev, event FROM outbox WHERE tenant=? ORDER BY rev', (tenant,))
    tgt_events = {}
    tgt_seq_ok = True
    for i, r in enumerate(ob_b):
        ev = json.loads(r['event'])
        if r['rev'] != fence_clock + 1 + i or ev['row']['rev'] != r['rev']:
            tgt_seq_ok = False
        tgt_events[r['rev']] = ev
    rep.check('D1 target outbox revs contiguous and all > fence_clock', tgt_seq_ok,
              {'first': ob_b[0]['rev'] if ob_b else None, 'fence_clock': fence_clock})
    # every target row with rev <= fence_clock must equal the source model exactly;
    # every row with rev > fence_clock must be justified by a target outbox event
    bad = {}
    for rid, (v, rev, d) in tgt_records.items():
        if rev <= fence_clock:
            if model.get(rid) != (v, rev, d):
                bad[rid] = ('imported-mismatch', (v, rev, d), model.get(rid))
        else:
            ev = tgt_events.get(rev)
            if not ev or ev['row']['row_id'] != rid or (ev['row']['value'], ev['row']['deleted']) != (v, d):
                bad[rid] = ('unjustified-new-row', (v, rev, d))
    missing = {rid: m for rid, m in model.items()
               if rid not in tgt_records or tgt_records[rid][1] < m[1]}
    rep.check('D2 target rows: imported rows identical, new rows justified, none missing',
              not bad and not missing, {'bad': bad, 'missing': missing})
    full = dict(model)
    for rev in sorted(tgt_events):
        row = tgt_events[rev]['row']
        full[row['row_id']] = (row['value'], row['rev'], row['deleted'])
    rep.check('D3 target records == replay(source<=fence)+replay(target>fence)', full == tgt_records,
              {'diff': {k: (full.get(k), tgt_records.get(k))
                        for k in set(full) | set(tgt_records) if full.get(k) != tgt_records.get(k)}})

    # ---- E/F. request-by-request receipts ----------------------------------
    req_a = {r['request_id']: r for r in rows(a, 'SELECT * FROM requests WHERE tenant=?', (tenant,))}
    req_b = {r['request_id']: r for r in rows(b, 'SELECT * FROM requests WHERE tenant=?', (tenant,))}
    not_imported = {rid for rid, r in req_a.items()
                    if rid not in req_b or req_b[rid]['content_sha'] != r['content_sha']
                    or req_b[rid]['receipt'] != r['receipt']}
    rep.check('E1 every source receipt imported identically (incl. pre-migration)', not not_imported,
              {'missing_or_different': sorted(not_imported)[:10]})
    fabricated = {}
    for rid, r in req_b.items():
        rec = json.loads(r['receipt'])
        if rec['rev'] <= fence_clock:
            if rid not in req_a or req_a[rid]['receipt'] != r['receipt']:
                fabricated[rid] = 'old-rev-receipt-not-in-source'
        else:
            ok = (rec.get('accepted_generation') == new_gen and rec.get('status') == 'ACCEPTED'
                  and rec.get('tenant') == tenant and rec.get('request_id') == rid
                  and rec.get('content_sha') == r['content_sha']
                  and rec['rev'] in tgt_events
                  and tgt_events[rec['rev']]['request']['request_id'] == rid)
            if not ok:
                fabricated[rid] = 'new-receipt-inconsistent'
    rep.check('E2 target receipts: old revs proven by source, new revs well-formed gen-%d' % new_gen,
              not fabricated, fabricated)
    # one request_id -> one persistent decision -> one rev, across both shards
    decisions = {}
    dup = {}
    for shard_reqs in (req_a, req_b):
        for rid, r in shard_reqs.items():
            key = (r['content_sha'], r['receipt'])
            if rid in decisions and decisions[rid] != key:
                dup[rid] = 'two different receipts for one request_id'
            decisions[rid] = key
    rep.check('F1 each request_id has exactly one persistent decision', not dup, dup)
    rev_owner = {}
    rev_dup = {}
    for rid, r in decisions.items():
        rev = json.loads(r[1])['rev']
        if rev in rev_owner:
            rev_dup[rev] = (rev_owner[rev], rid)
        rev_owner[rev] = rid
    rep.check('F2 no two requests share a rev (no double execution)', not rev_dup, rev_dup)
    # every receipt rev is backed by exactly one outbox event on the owning shard
    orphan = []
    for rid, (sha, receipt) in decisions.items():
        rev = json.loads(receipt)['rev']
        src = a if rev <= fence_clock else b
        ev = src.execute('SELECT event FROM outbox WHERE tenant=? AND rev=?', (tenant, rev)).fetchone()
        if not ev or json.loads(ev[0])['request']['request_id'] != rid:
            orphan.append((rid, rev))
    rep.check('F3 every receipt maps to exactly one outbox event', not orphan, orphan)

    # ---- G. other tenants untouched ----------------------------------------
    others = [t for t in routing if t != tenant]
    for t in others:
        rt = routing[t]
        st = rows(a if rt['owner'] == source_name else b,
                  'SELECT * FROM tenant_state WHERE tenant=?', (t,))
        st = st[0] if st else None
        rep.check('G1 %s: route matches owner state, still writable' % t,
                  st and st['generation'] == rt['generation'] and st['writable'] == 1,
                  {'route': rt, 'state': st})
        ob = rows(a, 'SELECT rev, event FROM outbox WHERE tenant=? ORDER BY rev', (t,))
        m = {}
        for r in ob:
            row = json.loads(r['event'])['row']
            m[row['row_id']] = (row['value'], row['rev'], row['deleted'])
        recs = {r['row_id']: (r['value'], r['rev'], r['deleted'])
                for r in rows(a, 'SELECT * FROM records WHERE tenant=?', (t,))}
        rep.check('G2 %s: source records == own outbox replay' % t, recs == m)
        on_b = (rows(b, 'SELECT 1 x FROM records WHERE tenant=? LIMIT 1', (t,))
                + rows(b, 'SELECT 1 x FROM requests WHERE tenant=? LIMIT 1', (t,))
                + rows(b, 'SELECT 1 x FROM outbox WHERE tenant=? LIMIT 1', (t,))
                + rows(b, 'SELECT 1 x FROM tenant_state WHERE tenant=? LIMIT 1', (t,)))
        rep.check('G3 %s: nothing leaked onto target shard' % t, not on_b)

    # ---- H. journal / stats -------------------------------------------------
    rep.check('H1 journal phase DONE', journal.get('phase') == 'DONE', journal.get('phase'))
    rep.check('H2 boundary_clock <= fence_clock',
              journal.get('boundary_clock') is not None and journal['boundary_clock'] <= fence_clock,
              {'boundary': journal.get('boundary_clock'), 'fence': fence_clock})
    tgt_clock_ok = ta and ta['clock'] == max([fence_clock] + list(tgt_events))
    rep.check('H3 target clock == max(fence_clock, own outbox)', tgt_clock_ok,
              {'target_clock': ta['clock'] if ta else None})
    print('\n%d checks, %d passed, %d failed' %
          (len(rep.checks), sum(1 for _, ok, _ in rep.checks if ok),
           sum(1 for _, ok, _ in rep.checks if not ok)))
    return rep.ok


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('workdir')
    p.add_argument('--tenant')
    p.add_argument('--migration-id')
    a = p.parse_args()
    sys.exit(0 if verify(a.workdir, a.tenant, a.migration_id) else 1)

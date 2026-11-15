"""Storage engine: immutable version log + transactionally published projections.

Design summary (full argument in DESIGN.md):

* Seed versions are materialised in the same ``version`` log as every other
  version (origin ``seed``), so the initial frontier (``seed:1`` for ``r1``,
  ``seed:2`` for ``r2``) needs no special casing in the causal code.
* A local replica owns exactly one acyclic "local chain" of versions built by
  ``write``/``delete``/``resolve``.  Each local version is created in a single
  ``BEGIN IMMEDIATE`` transaction that also bumps ``meta.seq``, updates the
  business projection, records the version and frontier, and appends exactly
  one ``source_outbox`` row.  The pre-condition (expected_heads equals the
  current ready frontier, and a plain write/delete sees at most one head) is
  checked under that write lock, so two writers that both observed the same
  head cannot both commit.
* Reception is the *only* other way a ``version`` row appears.  A received
  event is stored with its identity and canonical content exactly as sent and
  is never given a local seq nor copied into ``source_outbox``.  There is no
  code path from ``receive`` to a local write, hence no echo.
* ``version`` is append-only.  ``ready`` is a function of the parent set (all
  parents must be known and ready); it is recomputed from the log in the same
  transaction as every mutation, so pending and ready states publish together.
  Because each reception recomputes the whole row, a late parent automatically
  makes previously pending children ready, and batch/order/repetition do not
  matter.
* The frontier is the set of ready versions of a row that no other ready
  version of that row names as a parent.  It is derived only from immutable
  identity + parents, so replicas holding the same version set compute the
  same heads regardless of arrival order, wall clock, or seq magnitude.
* The business projection (``items``) contains exactly the resolved rows
  (exactly one non-tombstone head).  ``conflict_member`` keeps every value of
  every multi-head conflict outside the business table, so a conflict can never
  masquerade as one normal row while all branches stay legible.
* ``claim`` / ``inbox`` retain every received item and its evidence, including
  rejected identity reuse and cross-row parents.
"""

import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    replica TEXT PRIMARY KEY,
    seq INTEGER NOT NULL,
    dataset TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS items(
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    qty INTEGER NOT NULL CHECK(qty >= 0)
);
CREATE TABLE IF NOT EXISTS seed_versions(
    version_id TEXT PRIMARY KEY,
    row_id TEXT NOT NULL,
    state_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_outbox(
    version_id TEXT PRIMARY KEY,
    event_json TEXT NOT NULL,
    event_sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS received(
    version_id TEXT PRIMARY KEY,
    event_json TEXT NOT NULL,
    event_sha256 TEXT NOT NULL
);

-- Append-only version log, local and remote and seed alike.
CREATE TABLE IF NOT EXISTS version(
    version_id TEXT PRIMARY KEY,
    event_sha256 TEXT NOT NULL,
    event_json TEXT NOT NULL,
    origin TEXT NOT NULL,
    seq INTEGER NOT NULL,
    dataset TEXT NOT NULL,
    row_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state_json TEXT,              -- canonical state; NULL means tombstone
    parents_json TEXT NOT NULL,
    local INTEGER NOT NULL,       -- 1 iff this replica authored the version
    ready INTEGER NOT NULL DEFAULT 0,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS version_row ON version(row_id);
CREATE INDEX IF NOT EXISTS version_origin_seq ON version(origin, seq);

-- Materialised current head set per row (ready versions only).
CREATE TABLE IF NOT EXISTS frontier(
    row_id TEXT NOT NULL,
    version_id TEXT NOT NULL,
    PRIMARY KEY(row_id, version_id)
);
CREATE INDEX IF NOT EXISTS frontier_version ON frontier(version_id);

-- Every head of a conflicted row, with its exact value or tombstone.
CREATE TABLE IF NOT EXISTS conflict_member(
    row_id TEXT NOT NULL,
    version_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    state_json TEXT,
    PRIMARY KEY(row_id, version_id)
);

-- Every reception attempt with the evidence used for the decision.
CREATE TABLE IF NOT EXISTS inbox(
    item_no INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id TEXT,
    event_sha256 TEXT,
    event_json TEXT,
    outcome TEXT NOT NULL,
    reason TEXT,
    received_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_version ON inbox(version_id);

-- Durable per-event reception state; conflicting reuse keeps two rows.
CREATE TABLE IF NOT EXISTS claim(
    version_id TEXT NOT NULL,
    event_sha256 TEXT NOT NULL,
    canonical INTEGER NOT NULL,
    status TEXT NOT NULL,          -- accepted | pending | duplicate | rejected
    reason TEXT,
    first_seen TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(version_id, event_sha256)
);

-- Observation trail of frontier transitions (local writes and receptions).
CREATE TABLE IF NOT EXISTS head_change(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_id TEXT NOT NULL,
    action TEXT NOT NULL,
    before_json TEXT NOT NULL,
    after_json TEXT NOT NULL,
    at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS head_change_row ON head_change(row_id);

-- Optional caller-supplied operation token.  Makes a local operation safe to
-- retry after a commit whose acknowledgement was lost: the same token maps to
-- the same already-committed version instead of creating a second one.
CREATE TABLE IF NOT EXISTS local_op(
    op_id TEXT PRIMARY KEY,
    version_id TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Export/delivery bookkeeping (audit only; never drives state).
CREATE TABLE IF NOT EXISTS outbox_delivery(
    version_id TEXT PRIMARY KEY,
    delivered_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS export_run(
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    exported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS receive_run(
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    item_count INTEGER NOT NULL,
    received_at TEXT NOT NULL
);
"""

KINDS = ("write", "delete", "resolve")
REPLICAS = ("A", "B")
SEED = [{"id": "r1", "name": "base", "qty": 10},
        {"id": "r2", "name": "spare", "qty": 5}]
ZERO_SHA = hashlib.sha256(b"").hexdigest()


class EventError(Exception):
    """A received item is malformed or violates the contract."""

    def __init__(self, code, reason):
        super().__init__("%s: %s" % (code, reason))
        self.code = code
        self.reason = reason


class ConflictError(Exception):
    """A local operation's pre-condition does not match the current state."""

    def __init__(self, code, reason, detail=None):
        super().__init__("%s: %s" % (code, reason))
        self.code = code
        self.reason = reason
        self.detail = detail or {}


def canonical(value):
    """Contract canonical JSON (identical to source.py)."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _hard_exit(c):
    """Simulate process death mid-transaction: no COMMIT/ROLLBACK is issued.

    ``os._exit`` bypasses SQLite's normal close path so the parent can verify
    that an uncommitted write leaves no trace and can be retried cleanly.
    """
    os._exit(17)


def _norm_state(state):
    """Validate full-row shape and return canonical text, or None."""
    if state is None:
        return None
    if not isinstance(state, dict):
        raise ValueError("state must be null or an object")
    if set(state) != {"name", "qty"}:
        raise ValueError("state must contain exactly name and qty")
    name, qty = state["name"], state["qty"]
    if not isinstance(name, str):
        raise ValueError("state.name must be a string")
    if not isinstance(qty, int) or isinstance(qty, bool):
        raise ValueError("state.qty must be an integer")
    if qty < 0:
        raise ValueError("state.qty must be >= 0")
    return canonical({"name": name, "qty": qty})


class _Tx:
    """Real BEGIN IMMEDIATE transaction; commits or rolls back."""

    def __init__(self, c):
        self.c = c

    def __enter__(self):
        self.c.execute("BEGIN IMMEDIATE")
        return self.c

    def __exit__(self, exc_type, exc, tb):
        self.c.execute("COMMIT" if exc_type is None else "ROLLBACK")
        return False


class Engine:
    def __init__(self, db_path, trace=None):
        self.path = str(db_path)
        self.trace = trace

    # ------------------------------------------------------------------ infra
    def connect(self):
        c = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        c.execute("PRAGMA foreign_keys=ON")
        if self.trace is not None:
            c.set_trace_callback(self.trace)
        return c

    @staticmethod
    def _tx(c):
        return _Tx(c)

    def _dataset(self):
        c = self.connect()
        try:
            return c.execute("SELECT dataset FROM meta").fetchone()[0]
        finally:
            c.close()

    def replica(self, c=None):
        own = c is None
        c = c or self.connect()
        try:
            return c.execute("SELECT replica FROM meta").fetchone()[0]
        finally:
            if own:
                c.close()

    @classmethod
    def init(cls, db_path, replica):
        if replica not in REPLICAS:
            raise ValueError("replica must be A or B")
        if Path(db_path).exists():
            raise FileExistsError("refusing to overwrite %s" % db_path)
        dataset = digest(SEED)
        c = sqlite3.connect(str(db_path))
        try:
            c.executescript(SCHEMA)
            c.execute("INSERT INTO meta VALUES(?,?,?)", (replica, 0, dataset))
            now = _now()
            for i, row in enumerate(SEED, 1):
                vid = "seed:%d" % i
                state_json = canonical({"name": row["name"], "qty": row["qty"]})
                event = {"dataset": dataset, "version_id": vid, "origin": "seed",
                         "seq": i, "row_id": row["id"], "parents": [],
                         "kind": "write", "state": json.loads(state_json)}
                raw = canonical(event)
                c.execute("INSERT INTO items VALUES(?,?,?)",
                          (row["id"], row["name"], row["qty"]))
                c.execute("INSERT INTO seed_versions VALUES(?,?,?)",
                          (vid, row["id"], state_json))
                c.execute(
                    "INSERT INTO version(version_id,event_sha256,event_json,"
                    "origin,seq,dataset,row_id,kind,state_json,parents_json,"
                    "local,ready,received_at) VALUES(?,?,?,?,?,?,?,?,?,?,0,1,?)",
                    (vid, ZERO_SHA, raw, "seed", i, dataset, row["id"], "write",
                     state_json, "[]", now))
                c.execute("INSERT INTO frontier VALUES(?,?)", (row["id"], vid))
                c.execute(
                    "INSERT INTO head_change(row_id,action,before_json,"
                    "after_json,at) VALUES(?,?,?,?,?)",
                    (row["id"], "init", "[]", canonical([vid]), now))
            c.commit()
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    @classmethod
    def prepare(cls, db_path):
        """Extend a copied seed database with the replication tables.

        Seed versions from ``seed_versions`` are materialised into the version
        log (origin ``seed``) with their business rows as the initial frontier.
        Idempotent; existing versions are left untouched.
        """
        c = sqlite3.connect(str(db_path))
        try:
            c.executescript(SCHEMA)
            row = c.execute("SELECT replica,seq,dataset FROM meta").fetchone()
            if row is None:
                raise ValueError("not a replica database")
            replica, seq, dataset = row
            changed = False
            for vid, row_id, state_json in c.execute(
                    "SELECT version_id,row_id,state_json FROM seed_versions "
                    "ORDER BY version_id"):
                if c.execute("SELECT 1 FROM version WHERE version_id=?",
                             (vid,)).fetchone():
                    continue
                if not vid.startswith("seed:"):
                    raise ValueError("unexpected seed version id %s" % vid)
                seq_no = int(vid.split(":", 1)[1])
                event = {"dataset": dataset, "version_id": vid,
                         "origin": "seed", "seq": seq_no, "row_id": row_id,
                         "parents": [], "kind": "write",
                         "state": json.loads(state_json)}
                now = _now()
                c.execute(
                    "INSERT INTO version(version_id,event_sha256,event_json,"
                    "origin,seq,dataset,row_id,kind,state_json,parents_json,"
                    "local,ready,received_at) VALUES(?,?,?,?,?,?,?,?,?,?,0,1,?)",
                    (vid, ZERO_SHA, canonical(event), "seed", seq_no, dataset,
                     row_id, "write", state_json, "[]", now))
                c.execute("INSERT INTO frontier VALUES(?,?)", (row_id, vid))
                c.execute(
                    "INSERT INTO head_change(row_id,action,before_json,"
                    "after_json,at) VALUES(?,?,?,?,?)",
                    (row_id, "prepare", "[]", canonical([vid]), now))
                changed = True
            c.commit()
            return replica
        except Exception:
            c.rollback()
            raise
        finally:
            c.close()

    # -------------------------------------------------------------- row reads
    def _head_ids(self, c, row_id):
        return [r[0] for r in c.execute(
            "SELECT version_id FROM frontier WHERE row_id=? ORDER BY version_id",
            (row_id,))]

    def _versions(self, c, row_id):
        rows = c.execute(
            "SELECT version_id,event_sha256,event_json,origin,seq,row_id,kind,"
            "state_json,parents_json,local,ready FROM version WHERE row_id=? "
            "ORDER BY version_id", (row_id,))
        keys = ("version_id", "event_sha256", "event_json", "origin", "seq",
                "row_id", "kind", "state_json", "parents_json", "local",
                "ready")
        return [dict(zip(keys, r)) for r in rows]

    def _row_status(self, c, row_id):
        heads = self._head_ids(c, row_id)
        if not heads:
            return "absent", heads
        if len(heads) > 1:
            return "conflicted", heads
        state_json = c.execute(
            "SELECT state_json FROM version WHERE version_id=?",
            (heads[0],)).fetchone()[0]
        return ("deleted" if state_json is None else "resolved"), heads

    def _missing(self, c, parents):
        missing = []
        for p in parents:
            row = c.execute(
                "SELECT ready FROM version WHERE version_id=?", (p,)).fetchone()
            if row is None or not row[0]:
                missing.append(p)
        return missing

    def _set_business(self, c, row_id, state):
        if state is None:
            c.execute("DELETE FROM items WHERE id=?", (row_id,))
        else:
            c.execute(
                "INSERT INTO items VALUES(?,?,?) ON CONFLICT(id) DO UPDATE "
                "SET name=excluded.name,qty=excluded.qty",
                (row_id, state["name"], state["qty"]))

    # ------------------------------------------------------------ projection
    def _recompute_row(self, c, row_id, action=None):
        """Publish ready flags, frontier, conflicts and business row.

        Runs inside the caller's write transaction and derives every output
        from the append-only ``version`` table, so it is replay-safe and order
        independent.
        """
        before = self._head_ids(c, row_id)
        versions = self._versions(c, row_id)
        ready = {v["version_id"] for v in versions if v["ready"]}
        newly_ready = set()
        changed = True
        while changed:
            changed = False
            for v in versions:
                if v["version_id"] in ready:
                    continue
                parents = json.loads(v["parents_json"])
                if all(p in ready for p in parents):
                    ready.add(v["version_id"])
                    newly_ready.add(v["version_id"])
                    c.execute("UPDATE version SET ready=1 WHERE version_id=?",
                              (v["version_id"],))
                    changed = True
        # Clear stale flags (defensive; cannot normally happen).
        for v in versions:
            if v["version_id"] not in ready and v["ready"]:
                c.execute("UPDATE version SET ready=0 WHERE version_id=?",
                          (v["version_id"],))
        for vid in sorted(newly_ready):
            c.execute("UPDATE claim SET status='accepted',reason=NULL "
                      "WHERE version_id=? AND canonical=1 AND status='pending'",
                      (vid,))

        by_id = {v["version_id"]: v for v in versions}
        referenced = set()
        for v in versions:
            if v["version_id"] in ready:
                referenced.update(json.loads(v["parents_json"]))
        heads = sorted(vid for vid in ready if vid not in referenced)

        c.execute("DELETE FROM frontier WHERE row_id=?", (row_id,))
        for h in heads:
            c.execute("INSERT INTO frontier VALUES(?,?)", (row_id, h))
        c.execute("DELETE FROM conflict_member WHERE row_id=?", (row_id,))

        if not heads:
            c.execute("DELETE FROM items WHERE id=?", (row_id,))
            status = "absent"
        elif len(heads) == 1:
            head = by_id[heads[0]]
            state = (json.loads(head["state_json"])
                     if head["state_json"] is not None else None)
            self._set_business(c, row_id, state)
            status = "deleted" if state is None else "resolved"
        else:
            # A conflict must never look like one normal business row.
            c.execute("DELETE FROM items WHERE id=?", (row_id,))
            for h in heads:
                head = by_id[h]
                c.execute("INSERT INTO conflict_member VALUES(?,?,?,?)",
                          (row_id, h, head["kind"], head["state_json"]))
            status = "conflicted"

        if before != heads and action is not None:
            c.execute(
                "INSERT INTO head_change(row_id,action,before_json,after_json,at)"
                " VALUES(?,?,?,?,?)",
                (row_id, action, canonical(before), canonical(heads), _now()))
        return status, heads

    # ---------------------------------------------------------- local writers
    def local_write(self, row_id, state, expected_heads, kind="write",
                    fail_before_outbox=False, fail_before_commit=False,
                    crash_before_outbox=False, crash_before_commit=False,
                    op_id=None):
        """Atomically create one local version.

        Succeeds only if, under the write lock, the ready frontier equals
        ``expected_heads`` and (for write/delete) has at most one head.  The
        parent list is exactly that frontier.  Returns the event dict.

        ``op_id`` is an optional idempotency token: retrying after a commit
        whose reply was lost returns the original version rather than creating
        a second one.
        """
        if kind not in KINDS:
            raise ValueError("kind must be one of %s" % (KINDS,))
        if not isinstance(row_id, str) or not row_id:
            raise ValueError("row_id must be a non-empty string")
        if kind == "delete" and state is not None:
            raise ValueError("delete must carry a null state")
        if kind == "write" and state is None:
            raise ValueError("write requires a complete row state")
        state_json = _norm_state(state)
        state_obj = json.loads(state_json) if state_json is not None else None
        if (not isinstance(expected_heads, (list, tuple))
                or any(not isinstance(e, str) for e in expected_heads)):
            raise ValueError("expected_heads must be a list of version ids")
        if len(set(expected_heads)) != len(expected_heads):
            raise ValueError("expected_heads must not contain duplicates")
        expected = sorted(expected_heads)
        request_sha = digest({"row_id": row_id, "state": state_obj,
                              "expected_heads": expected, "kind": kind})

        c = self.connect()
        try:
            with self._tx(c):
                if op_id is not None:
                    prior = c.execute(
                        "SELECT version_id,request_sha256 FROM local_op "
                        "WHERE op_id=?", (op_id,)).fetchone()
                    if prior is not None:
                        if prior[1] != request_sha:
                            raise ConflictError(
                                "op_id_reused",
                                "op_id already committed with a different "
                                "request", {"op_id": op_id,
                                            "version_id": prior[0]})
                        event_json = c.execute(
                            "SELECT event_json FROM version WHERE "
                            "version_id=?", (prior[0],)).fetchone()[0]
                        return json.loads(event_json)
                replica, seq, dataset = c.execute(
                    "SELECT replica,seq,dataset FROM meta").fetchone()
                heads = self._head_ids(c, row_id)
                if kind == "resolve":
                    if not heads:
                        raise ConflictError(
                            "absent", "resolve needs a current head to bind",
                            {"expected_heads": expected, "current_heads": []})
                    if heads != expected:
                        raise ConflictError(
                            "stale_expected_heads",
                            "resolve must bind the exact current head set",
                            {"expected_heads": expected,
                             "current_heads": heads})
                else:
                    if len(heads) > 1:
                        members = {
                            vid: {"kind": kind, "state": json.loads(sj)
                                  if sj is not None else None}
                            for vid, kind, sj in c.execute(
                                "SELECT version_id,kind,state_json FROM "
                                "conflict_member WHERE row_id=? "
                                "ORDER BY version_id", (row_id,))}
                        raise ConflictError(
                            "conflicted",
                            "row has %d heads; write/delete require conflict "
                            "free state, use resolve" % len(heads),
                            {"expected_heads": expected, "current_heads": heads,
                             "members": members})
                    if heads != expected:
                        raise ConflictError(
                            "stale_expected_heads",
                            "expected_heads does not match current heads",
                            {"expected_heads": expected,
                             "current_heads": heads,
                             "status": self._row_status(c, row_id)[0]})
                seq += 1
                version_id = "%s:%d" % (replica, seq)
                event = {
                    "dataset": dataset,
                    "version_id": version_id,
                    "origin": replica,
                    "seq": seq,
                    "row_id": row_id,
                    "parents": list(expected),
                    "kind": kind,
                    "state": state_obj,
                }
                raw = canonical(event)
                sha = hashlib.sha256(raw.encode()).hexdigest()
                c.execute("UPDATE meta SET seq=?", (seq,))
                c.execute(
                    "INSERT INTO version(version_id,event_sha256,event_json,"
                    "origin,seq,dataset,row_id,kind,state_json,parents_json,"
                    "local,ready,received_at) VALUES(?,?,?,?,?,?,?,?,?,?,1,0,?)",
                    (version_id, sha, raw, replica, seq, dataset, row_id, kind,
                     state_json, canonical(expected), _now()))
                if fail_before_outbox:
                    raise RuntimeError("probe: interrupted before outbox row")
                if crash_before_outbox:
                    _hard_exit(c)
                c.execute("INSERT INTO source_outbox VALUES(?,?,?)",
                          (version_id, raw, sha))
                if op_id is not None:
                    c.execute("INSERT INTO local_op VALUES(?,?,?,?)",
                              (op_id, version_id, request_sha, _now()))
                self._recompute_row(c, row_id, action="local_%s" % kind)
                if fail_before_commit:
                    raise RuntimeError("probe: interrupted before commit")
                if crash_before_commit:
                    _hard_exit(c)
                return event
        finally:
            c.close()

    # ------------------------------------------------------------- reception
    def _validate_event(self, ev, own, dataset):
        """Structural validation.  Returns a normalised dict."""
        if not isinstance(ev, dict):
            raise EventError("malformed", "item is not a JSON object")
        required = {"dataset", "version_id", "origin", "seq", "row_id",
                    "parents", "kind", "state"}
        missing = required - set(ev)
        if missing:
            raise EventError("malformed", "missing fields %s" % sorted(missing))
        unknown = set(ev) - required
        if unknown:
            raise EventError("malformed", "unknown fields %s" % sorted(unknown))
        if ev["dataset"] != dataset:
            raise EventError("dataset_mismatch", "event dataset differs")
        vid = ev["version_id"]
        if not isinstance(vid, str) or ":" not in vid:
            raise EventError("invalid_identity", "version_id must be origin:seq")
        origin, seq_txt = vid.rsplit(":", 1)
        if origin not in REPLICAS:
            raise EventError("invalid_origin", "origin must be A or B")
        if origin != ev["origin"]:
            raise EventError("invalid_identity",
                             "version_id origin does not match origin field")
        seq = ev["seq"]
        if (not isinstance(seq, int) or isinstance(seq, bool) or seq <= 0
                or str(seq) != seq_txt):
            raise EventError("invalid_identity", "seq does not match version_id")
        row_id = ev["row_id"]
        if not isinstance(row_id, str) or not row_id:
            raise EventError("invalid_row", "row_id must be a non-empty string")
        kind = ev["kind"]
        if kind not in KINDS:
            raise EventError("invalid_kind", "kind must be write/delete/resolve")
        parents = ev["parents"]
        if (not isinstance(parents, list)
                or any(not isinstance(p, str) or not p for p in parents)
                or len(set(parents)) != len(parents)):
            raise EventError("invalid_parents",
                             "parents must be a list of unique ids")
        if vid in parents:
            raise EventError("invalid_parents", "version cannot parent itself")
        if kind == "resolve" and not parents:
            raise EventError("invalid_parents",
                             "resolve must bind at least one head parent")
        try:
            state_json = _norm_state(ev["state"])
        except ValueError as exc:
            raise EventError("invalid_state", str(exc))
        if kind == "delete" and state_json is not None:
            raise EventError("invalid_state", "delete must carry a null state")
        if kind == "write" and state_json is None:
            raise EventError("invalid_state",
                             "write requires a complete row state")
        # Preserve the sender's content exactly: parents order is part of the
        # canonical bytes and hence of the version identity.  Causal logic
        # treats parents as a set, but nothing is rewritten on reception.
        raw = canonical(ev)
        return {
            "version_id": vid, "origin": origin, "seq": seq,
            "dataset": ev["dataset"], "row_id": row_id, "kind": kind,
            "state_json": state_json, "parents": parents,
            "event_json": raw,
            "event_sha256": hashlib.sha256(raw.encode()).hexdigest(),
        }

    def receive(self, events, batch_name=None, fail_after_persist=False,
                crash_after_persist=False):
        """Receive a batch; each item is classified and applied independently.

        Every item runs in its own ``BEGIN IMMEDIATE`` transaction so a
        rejection cannot affect the other legal items of the batch, and the
        version row, ready/pending state, frontier, conflict and business
        projections become visible together or not at all.
        """
        if isinstance(events, dict):
            events = events["events"] if "events" in events else [events]
        receipts = []
        c = self.connect()
        try:
            own, dataset = c.execute(
                "SELECT replica,dataset FROM meta").fetchone()
            for item_no, item in enumerate(events):
                receipts.append(self._receive_one(
                    c, item_no, item, own, dataset, fail_after_persist,
                    crash_after_persist))
            if batch_name is not None:
                with self._tx(c):
                    c.execute(
                        "INSERT INTO receive_run(path,item_count,received_at) "
                        "VALUES(?,?,?)", (batch_name, len(receipts), _now()))
        finally:
            c.close()
        return receipts

    def _receipt(self, item_no, outcome, vid=None, reason=None, origin=None,
                 seq=None, row_id=None, status=None, heads=None, missing=None,
                 error=None, sha=None):
        r = {"item": item_no, "outcome": outcome, "version_id": vid,
             "reason": reason}
        if origin is not None:
            r.update(origin=origin, seq=seq, row_id=row_id)
        if sha is not None:
            r["event_sha256"] = sha
        if status is not None:
            r["row_status"] = status
        if heads is not None:
            r["heads"] = heads
        if missing is not None:
            r["missing"] = missing
        if error is not None:
            r["error"] = error
        return r

    def _reject(self, c, item_no, vid, sha, raw, reason, origin, seq, row_id,
                error=None):
        c.execute(
            "INSERT INTO inbox(version_id,event_sha256,event_json,outcome,"
            "reason,received_at) VALUES(?,?,?,?,?,?)",
            (vid, sha, raw, "rejected", reason, _now()))
        if isinstance(vid, str):
            c.execute(
                "INSERT INTO claim(version_id,event_sha256,canonical,status,"
                "reason,first_seen,last_seen,attempts) VALUES(?,?,0,'rejected',"
                "?,?,?,1) ON CONFLICT(version_id,event_sha256) DO UPDATE SET "
                "last_seen=excluded.last_seen,attempts=claim.attempts+1",
                (vid, sha, reason, _now(), _now()))
        return self._receipt(item_no, "rejected", vid, reason=reason,
                             origin=origin, seq=seq, row_id=row_id,
                             error=error or reason, sha=sha)

    def _receive_one(self, c, item_no, item, own, dataset, fail_after_persist,
                     crash_after_persist=False):
        # Parse/canonicalise once; a malformed item is recorded, not fatal.
        if isinstance(item, str):
            try:
                ev = json.loads(item)
            except json.JSONDecodeError as exc:
                with self._tx(c):
                    return self._reject(c, item_no, None, None, item,
                                        "malformed_json", None, None, None,
                                        error=str(exc))
        else:
            ev = item
        try:
            raw = canonical(ev)
        except (TypeError, ValueError) as exc:
            with self._tx(c):
                return self._reject(c, item_no, None, None, repr(item),
                                    "malformed_json", None, None, None,
                                    error=str(exc))
        sha = hashlib.sha256(raw.encode()).hexdigest()
        vid = ev.get("version_id") if isinstance(ev, dict) else None
        origin = ev.get("origin") if isinstance(ev, dict) else None
        seq = ev.get("seq") if isinstance(ev, dict) else None
        row_id = ev.get("row_id") if isinstance(ev, dict) else None

        try:
            norm = self._validate_event(ev, own, dataset)
        except EventError as exc:
            with self._tx(c):
                return self._reject(c, item_no,
                                    vid if isinstance(vid, str) else None,
                                    sha, raw, "%s: %s" % (exc.code, exc.reason),
                                    origin, seq, row_id, error=exc.reason)
        vid = norm["version_id"]
        sha = norm["event_sha256"]
        raw = norm["event_json"]
        row_id = norm["row_id"]

        with self._tx(c):
            # ---- identity de-duplication / echo suppression ----------------
            prior = c.execute(
                "SELECT event_sha256 FROM version WHERE version_id=?",
                (vid,)).fetchone()
            outbox = c.execute(
                "SELECT event_sha256 FROM source_outbox WHERE version_id=?",
                (vid,)).fetchone()
            seen = prior or outbox
            if seen is not None:
                if seen[0] != sha:
                    return self._reject(
                        c, item_no, vid, sha, raw,
                        "identity_content_conflict:first_sha256=%s" % seen[0],
                        origin, seq, row_id,
                        error="identity reused with different canonical content")
                c.execute(
                    "INSERT INTO inbox(version_id,event_sha256,event_json,"
                    "outcome,reason,received_at) VALUES(?,?,?,?,?,?)",
                    (vid, sha, raw, "duplicate", None, _now()))
                c.execute(
                    "INSERT INTO claim(version_id,event_sha256,canonical,status,"
                    "reason,first_seen,last_seen,attempts) VALUES(?,?,1,"
                    "'duplicate',NULL,?,?,1) ON CONFLICT(version_id,event_sha256)"
                    " DO UPDATE SET last_seen=excluded.last_seen,"
                    "attempts=claim.attempts+1", (vid, sha, _now(), _now()))
                status, heads = self._row_status(c, row_id)
                ready = c.execute("SELECT ready FROM version WHERE "
                                  "version_id=?", (vid,)).fetchone()
                # A duplicate of a still-pending version is still a no-change,
                # but the receipt explains the dependency state.
                missing = (None if ready is None or ready[0]
                           else self._missing(
                               c, json.loads(c.execute(
                                   "SELECT parents_json FROM version WHERE "
                                   "version_id=?", (vid,)).fetchone()[0])))
                return self._receipt(item_no, "no_change", vid,
                                     reason="pending_duplicate" if missing
                                     else None,
                                     origin=origin, seq=seq, row_id=row_id,
                                     status=status, heads=heads, sha=sha,
                                     missing=missing)
            if origin == own:
                # Echo of an identity we never authored: never adopt it.
                return self._reject(c, item_no, vid, sha, raw, "own_origin",
                                    origin, seq, row_id,
                                    error="origin is this replica but version "
                                          "is not in the local outbox")
            # Bytes equal to a previously rejected item: report it again.
            prior_claim = c.execute(
                "SELECT status,reason FROM claim WHERE version_id=? AND "
                "event_sha256=?", (vid, sha)).fetchone()
            if prior_claim is not None and prior_claim[0] == "rejected":
                c.execute(
                    "UPDATE claim SET last_seen=?,attempts=attempts+1 "
                    "WHERE version_id=? AND event_sha256=?", (_now(), vid, sha))
                c.execute(
                    "INSERT INTO inbox(version_id,event_sha256,event_json,"
                    "outcome,reason,received_at) VALUES(?,?,?,?,?,?)",
                    (vid, sha, raw, "rejected", prior_claim[1], _now()))
                return self._receipt(item_no, "rejected", vid,
                                     reason=prior_claim[1], origin=origin,
                                     seq=seq, row_id=row_id, sha=sha,
                                     error="duplicate of earlier rejection")

            # ---- parent / row validation -----------------------------------
            for p in norm["parents"]:
                prow = c.execute(
                    "SELECT row_id FROM version WHERE version_id=?",
                    (p,)).fetchone()
                if prow is not None and prow[0] != row_id:
                    return self._reject(
                        c, item_no, vid, sha, raw,
                        "cross_row_parent:%s_belongs_to_%s" % (p, prow[0]),
                        origin, seq, row_id,
                        error="parent %s belongs to row %s" % (p, prow[0]))

            # ---- persist immutable version, then publish projections ------
            c.execute(
                "INSERT INTO version(version_id,event_sha256,event_json,origin,"
                "seq,dataset,row_id,kind,state_json,parents_json,local,ready,"
                "received_at) VALUES(?,?,?,?,?,?,?,?,?,?,0,0,?)",
                (vid, sha, raw, origin, seq, norm["dataset"], row_id,
                 norm["kind"], norm["state_json"], canonical(norm["parents"]),
                 _now()))
            c.execute("INSERT INTO received VALUES(?,?,?)", (vid, raw, sha))
            affected = self._sweep_cross_row(c, vid, row_id)
            if fail_after_persist:
                raise RuntimeError("probe: interrupted before receive commit")
            if crash_after_persist:
                _hard_exit(c)
            self._recompute_row(c, row_id,
                                action="receive_%s" % norm["origin"])
            for other in affected:
                self._recompute_row(c, other, action="reject_cross_row")
            missing = self._missing(c, norm["parents"])
            if missing:
                outcome, reason = "pending", "missing_parents"
            else:
                outcome, reason = "accepted", None
            c.execute(
                "INSERT INTO claim(version_id,event_sha256,canonical,status,"
                "reason,first_seen,last_seen,attempts) VALUES(?,?,1,?,?,?,?,1)"
                " ON CONFLICT(version_id,event_sha256) DO UPDATE SET "
                "status=excluded.status,reason=excluded.reason,"
                "last_seen=excluded.last_seen,attempts=claim.attempts+1",
                (vid, sha, outcome, reason, _now(), _now()))
            c.execute(
                "INSERT INTO inbox(version_id,event_sha256,event_json,outcome,"
                "reason,received_at) VALUES(?,?,?,?,?,?)",
                (vid, sha, raw, outcome, reason, _now()))
            status, heads = self._row_status(c, row_id)
            return self._receipt(item_no, outcome, vid, reason=reason,
                                 origin=origin, seq=seq, row_id=row_id,
                                 status=status, heads=heads,
                                 missing=missing if missing else None, sha=sha)

    def _sweep_cross_row(self, c, new_vid, new_row):
        """Re-classify pending versions that now reference a wrong-row parent."""
        affected = set()
        pending = c.execute(
            "SELECT version_id,row_id,parents_json,event_sha256 FROM version "
            "WHERE ready=0").fetchall()
        for vid, row_id, parents_json, sha in pending:
            if vid == new_vid:
                continue
            if new_vid not in json.loads(parents_json):
                continue
            if row_id == new_row:
                continue
            raw = c.execute("SELECT event_json FROM version WHERE version_id=?",
                            (vid,)).fetchone()[0]
            c.execute("DELETE FROM version WHERE version_id=?", (vid,))
            c.execute("DELETE FROM received WHERE version_id=?", (vid,))
            c.execute(
                "INSERT INTO inbox(version_id,event_sha256,event_json,outcome,"
                "reason,received_at) VALUES(?,?,?,?,?,?)",
                (vid, sha, raw, "rejected",
                 "cross_row_parent:%s_belongs_to_%s" % (new_vid, new_row),
                 _now()))
            c.execute(
                "UPDATE claim SET status='rejected',reason=?,last_seen=? "
                "WHERE version_id=? AND canonical=1",
                ("cross_row_parent", _now(), vid))
            affected.add(row_id)
        return sorted(affected)

    # --------------------------------------------------------------- queries
    def query_row(self, row_id):
        c = self.connect()
        try:
            status, heads = self._row_status(c, row_id)
            versions = self._versions(c, row_id)
            by_id = {v["version_id"]: v for v in versions}
            head_info = []
            for h in heads:
                v = by_id[h]
                head_info.append({
                    "version_id": h,
                    "origin": v["origin"],
                    "seq": v["seq"],
                    "kind": v["kind"],
                    "local": bool(v["local"]),
                    "state": json.loads(v["state_json"])
                    if v["state_json"] is not None else None,
                    "tombstone": v["state_json"] is None,
                })
            pending = []
            for v in versions:
                if not v["ready"]:
                    parents = json.loads(v["parents_json"])
                    pending.append({
                        "version_id": v["version_id"],
                        "origin": v["origin"],
                        "seq": v["seq"],
                        "kind": v["kind"],
                        "parents": parents,
                        "missing": self._missing(c, parents),
                    })
            members = []
            if status == "conflicted":
                members = [
                    {"version_id": vid, "kind": kind,
                     "state": json.loads(sj) if sj is not None else None,
                     "tombstone": sj is None}
                    for vid, kind, sj in c.execute(
                        "SELECT version_id,kind,state_json FROM conflict_member "
                        "WHERE row_id=? ORDER BY version_id", (row_id,))]
            result = {
                "row_id": row_id,
                "status": status,
                "heads": heads,
                "head_versions": head_info,
                "pending": pending,
            }
            if status == "resolved":
                result["state"] = head_info[0]["state"]
            elif status == "deleted":
                result["state"] = None
                result["tombstone"] = True
            elif status == "conflicted":
                result["members"] = members
            return result
        finally:
            c.close()

    def export_data(self):
        c = self.connect()
        try:
            rows = [{"id": r[0], "name": r[1], "qty": r[2]} for r in c.execute(
                "SELECT id,name,qty FROM items ORDER BY id")]
            normal_ids = {r["id"] for r in rows}
            excluded = []
            rids = sorted({r[0] for r in c.execute(
                "SELECT row_id FROM frontier UNION SELECT row_id FROM version")})
            for rid in rids:
                if rid in normal_ids:
                    continue
                status, heads = self._row_status(c, rid)
                item = {"row_id": rid, "status": status, "heads": heads}
                if status == "conflicted":
                    item["reason"] = ("conflicted: multiple causally "
                                      "independent heads; not a normal row")
                    item["members"] = [
                        {"version_id": vid, "kind": kind,
                         "state": json.loads(sj) if sj is not None else None,
                         "tombstone": sj is None}
                        for vid, kind, sj in c.execute(
                            "SELECT version_id,kind,state_json FROM "
                            "conflict_member WHERE row_id=? ORDER BY version_id",
                            (rid,))]
                elif status == "deleted":
                    item["reason"] = "deleted: current head is a tombstone"
                elif status == "absent":
                    item["reason"] = ("absent: no ready version "
                                      "(possibly pending dependencies)")
                else:
                    item["reason"] = "excluded"
                excluded.append(item)
            return {"rows": rows, "excluded": excluded,
                    "counts": {"normal": len(rows),
                               "excluded": len(excluded)}}
        finally:
            c.close()

    # --------------------------------------------------------------- export
    def outbox_events(self):
        c = self.connect()
        try:
            return [(r[0], json.loads(r[1]), r[2]) for r in c.execute(
                "SELECT version_id,event_json,event_sha256 FROM source_outbox "
                "ORDER BY version_id")]
        finally:
            c.close()

    def export_events(self, out_path, replica_id=None, version_ids=None):
        events = self.outbox_events()
        if version_ids is not None:
            want = set(version_ids)
            events = [e for e in events if e[0] in want]
        envelope = {
            "dataset": self._dataset(),
            "replica": replica_id or self.replica(),
            "events": [e[1] for e in events],
        }
        out = Path(out_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=str(out.parent), delete=False)
        try:
            tmp.write(canonical(envelope) + "\n")
            tmp.flush()
            os.fsync(tmp.fileno())
        finally:
            tmp.close()
        os.replace(tmp.name, out)
        c = self.connect()
        try:
            with self._tx(c):
                now = _now()
                for vid, _, _ in events:
                    c.execute(
                        "INSERT INTO outbox_delivery VALUES(?,?) ON CONFLICT"
                        "(version_id) DO UPDATE SET "
                        "delivered_at=excluded.delivered_at", (vid, now))
                c.execute(
                    "INSERT INTO export_run(path,event_count,exported_at) "
                    "VALUES(?,?,?)", (str(out), len(events), now))
        finally:
            c.close()
        return {"path": str(out), "count": len(events),
                "version_ids": [e[0] for e in events],
                "replica": envelope["replica"],
                "dataset": envelope["dataset"]}

    def receive_file(self, path, **kwargs):
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(data, dict) and "events" in data:
            if data.get("dataset") is not None and data["dataset"] != self._dataset():
                raise EventError("dataset_mismatch", "file dataset differs")
            events = data["events"]
        elif isinstance(data, list):
            events = data
        else:
            events = [data]
        receipts = self.receive(events, batch_name=str(path), **kwargs)
        return receipts

    # ----------------------------------------------------------------- audit
    def state_snapshot(self):
        c = self.connect()
        try:
            return {
                "rows": [tuple(r) for r in c.execute(
                    "SELECT id,name,qty FROM items ORDER BY id")],
                "heads": [tuple(r) for r in c.execute(
                    "SELECT row_id,version_id FROM frontier ORDER BY 1,2")],
                "versions": [tuple(r) for r in c.execute(
                    "SELECT version_id,event_sha256,ready FROM version "
                    "ORDER BY version_id")],
                "conflict_members": [tuple(r) for r in c.execute(
                    "SELECT row_id,version_id,kind,state_json FROM "
                    "conflict_member ORDER BY 1,2")],
                "outbox_count": c.execute(
                    "SELECT count(*) FROM source_outbox").fetchone()[0],
                "seq": c.execute("SELECT seq FROM meta").fetchone()[0],
            }
        finally:
            c.close()

    def audit(self):
        """Report real persisted evidence and check projections from the log."""
        c = self.connect()
        try:
            replica = c.execute("SELECT replica FROM meta").fetchone()[0]
            versions = [dict(zip(
                ("version_id", "origin", "seq", "row_id", "kind", "state_json",
                 "parents_json", "local", "ready", "event_sha256"), r))
                for r in c.execute(
                    "SELECT version_id,origin,seq,row_id,kind,state_json,"
                    "parents_json,local,ready,event_sha256 FROM version "
                    "ORDER BY origin,seq,version_id")]
            outbox = [dict(version_id=r[0], sha=r[1], event=json.loads(r[2]))
                      for r in c.execute(
                          "SELECT version_id,event_sha256,event_json FROM "
                          "source_outbox ORDER BY version_id")]
            received = [dict(version_id=r[0], sha=r[1])
                        for r in c.execute(
                            "SELECT version_id,event_sha256 FROM received "
                            "ORDER BY version_id")]
            claims = [dict(zip(
                ("version_id", "event_sha256", "canonical", "status", "reason",
                 "attempts"), r))
                for r in c.execute(
                    "SELECT version_id,event_sha256,canonical,status,reason,"
                    "attempts FROM claim ORDER BY version_id,event_sha256")]
            rejections = [dict(zip(
                ("item_no", "version_id", "event_sha256", "outcome", "reason"),
                r)) for r in c.execute(
                "SELECT item_no,version_id,event_sha256,outcome,reason FROM "
                "inbox WHERE outcome='rejected' ORDER BY item_no")]
            head_changes = [dict(zip(
                ("row_id", "action", "before", "after", "at"),
                (r[0], r[1], json.loads(r[2]), json.loads(r[3]), r[4])))
                for r in c.execute(
                    "SELECT row_id,action,before_json,after_json,at FROM "
                    "head_change ORDER BY id")]
            local_versions = [v for v in versions if v["local"] == 1]
            local_ids = {v["version_id"] for v in local_versions}
            outbox_ids = {e["version_id"] for e in outbox}
            received_ids = {r["version_id"] for r in received}
            pending = [dict(version_id=v["version_id"], row_id=v["row_id"],
                            parents=json.loads(v["parents_json"]),
                            missing=self._missing(c, json.loads(v["parents_json"])))
                       for v in versions if not v["ready"]]
            exported = [dict(zip(("path", "event_count", "exported_at"), r))
                        for r in c.execute(
                            "SELECT path,event_count,exported_at FROM export_run "
                            "ORDER BY run_id")]
            received_files = [dict(zip(("path", "item_count", "received_at"), r))
                              for r in c.execute(
                                  "SELECT path,item_count,received_at FROM "
                                  "receive_run ORDER BY run_id")]
            # Cross-check the persisted projections with the independent event
            # graph interpreter (audit only; never on the live path).  Seed
            # versions participate like any other root event.
            from . import audit_graph
            all_events = [json.loads(event_json) for _, event_json in c.execute(
                "SELECT version_id,event_json FROM version "
                "ORDER BY version_id")]
            interpreted = audit_graph.interpret(all_events)
            persisted = {}
            for rid in {v["row_id"] for v in versions}:
                status, heads = self._row_status(c, rid)
                entry = {"status": status, "heads": heads}
                if status == "resolved":
                    entry["state"] = json.loads(c.execute(
                        "SELECT state_json FROM version WHERE version_id=?",
                        (heads[0],)).fetchone()[0])
                elif status == "deleted":
                    entry["state"] = None
                elif status == "conflicted":
                    entry["state"] = None
                else:
                    entry["state"] = None
                entry["members"] = [
                    {"version_id": m_vid, "kind": kind,
                     "state": json.loads(sj) if sj is not None else None,
                     "tombstone": sj is None}
                    for m_vid, kind, sj in c.execute(
                        "SELECT version_id,kind,state_json FROM "
                        "conflict_member WHERE row_id=? ORDER BY version_id",
                        (rid,))]
                persisted[rid] = {"status": status, "heads": heads,
                                  "members": entry["members"]}
                persisted[rid]["state"] = entry["state"] if status != "conflicted" else None
                if status == "conflicted":
                    persisted[rid]["members"] = entry["members"]
                else:
                    persisted[rid]["members"] = []
            audit_problems = audit_graph.compare(interpreted, persisted)
            return {
                "replica": replica,
                "dataset": self._dataset(),
                "version_count": len(versions),
                "local_versions": local_versions,
                "outbox_count": len(outbox),
                "outbox_version_ids": sorted(outbox_ids),
                "received_count": len(received),
                "received_version_ids": sorted(received_ids),
                # An echo would be a locally authored version that also appears
                # in `received`, or a received version copied into the outbox.
                "local_echoes": sorted(local_ids & received_ids),
                "remote_versions_exported": sorted(outbox_ids - local_ids),
                "pending": pending,
                "rejections": rejections,
                "claims": claims,
                "head_changes": head_changes,
                "exports": exported,
                "receive_files": received_files,
                "graph_check": {"problems": audit_problems,
                                "ok": not audit_problems},
                "sql_writes_evidence": {
                    "items_rows": c.execute(
                        "SELECT count(*) FROM items").fetchone()[0],
                    "version_rows": len(versions),
                    "outbox_rows": len(outbox),
                    "received_rows": len(received),
                    "inbox_rows": c.execute(
                        "SELECT count(*) FROM inbox").fetchone()[0],
                    "head_change_rows": len(head_changes),
                },
            }
        finally:
            c.close()

    def log_history(self):
        c = self.connect()
        try:
            return [dict(row_id=r[0], action=r[1], before=json.loads(r[2]),
                         after=json.loads(r[3]), at=r[4])
                    for r in c.execute(
                        "SELECT row_id,action,before_json,after_json,at FROM "
                        "head_change ORDER BY id")]
        finally:
            c.close()

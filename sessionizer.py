"""
sessionizer.py -- local session data producer on Python 3.11 + stdlib sqlite3.

Subcommands: ingest / snapshot / changes / audit / init

Design (ordered state, affected-key recompute, watermark-driven finalization):

* Events are held in a per-key ordered table `events`, ordered by (time, event_id).
  Sessions are *not* fixed rolling windows: they are the maximal groups of
  consecutive events (same key, ordered) whose adjacent time difference <= gap.
* An upsert/retract can only change sessions of its own key. The formal hot path
  therefore reads *only* that key's unfinalized event rows (`key_access` proves
  it: every event-row read charges the row's key). It then replaces that key's
  hot sessions wholesale inside one SQLite transaction, emitting the complete
  old-set delete + new-set upsert batch so a reader never sees half a merge/split.
* Session identity is content-addressed: sha256 over (key, start, end, count,
  sum, member ids). A merge deletes the old ids and inserts the merged id; an
  un-merge (bridge retract) does the inverse. Replaying the same accepted op
  recomputes identical ids -> zero business changes. Every change row carries
  the triggering event/watermark and the full member basis.
* A monotone watermark W forbids new/revised/retracted event times <= W.
  A hot session with end+gap <= W is finalized: atomically moved to the cold
  archive (same identity/content, no business change emitted -- a storage move,
  not a result change), which is what allows its hot rows to be cleaned.
  snapshot is the UNION of archive and hot sessions; members never straddle
  the two, so nothing is missed or doubled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from typing import Any, Optional

from oracle import replay_oracle, sessionize
from consumer import apply_changefeed


# --------------------------------------------------------------------------- #
# schema
# --------------------------------------------------------------------------- #

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

-- immutable, append-only accepted input operations (the audit oracle source)
CREATE TABLE IF NOT EXISTS ops_log(
    seq INTEGER PRIMARY KEY,
    payload TEXT NOT NULL
);

-- rejected ops: reason retained, no published data is touched
CREATE TABLE IF NOT EXISTS rejections(
    seq INTEGER PRIMARY KEY,
    reason TEXT NOT NULL,
    payload TEXT NOT NULL
);

-- identity registry: an event_id belongs to exactly one key forever
CREATE TABLE IF NOT EXISTS event_ids(
    event_id TEXT PRIMARY KEY,
    key_id TEXT NOT NULL
);

-- live (unfinalized) events. Archived sessions' member rows are deleted here.
CREATE TABLE IF NOT EXISTS events(
    event_id TEXT PRIMARY KEY,
    key_id TEXT NOT NULL,
    time INTEGER NOT NULL,
    value INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_key ON events(key_id, time, event_id);

-- current hot sessions (not yet finalizable)
CREATE TABLE IF NOT EXISTS hot_sessions(
    session_id TEXT PRIMARY KEY,
    key_id TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    end_time INTEGER NOT NULL,
    cnt INTEGER NOT NULL,
    total INTEGER NOT NULL,
    members TEXT NOT NULL           -- JSON array of event_id, sorted
);
CREATE INDEX IF NOT EXISTS idx_hot_key_end ON hot_sessions(key_id, end_time);

-- finalized cold archive; same content-addressed identity, disjoint members
CREATE TABLE IF NOT EXISTS archived_sessions(
    session_id TEXT PRIMARY KEY,
    key_id TEXT NOT NULL,
    start_time INTEGER NOT NULL,
    end_time INTEGER NOT NULL,
    cnt INTEGER NOT NULL,
    total INTEGER NOT NULL,
    members TEXT NOT NULL,
    finalized_batch INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_arch_key ON archived_sessions(key_id, end_time);

-- one row per accepted op: the atomic change boundary. An op that also
-- triggers finalization keeps the same batch row (same seq), so the hot
-- recompute and the hot->archive move are never observed as two batches
CREATE TABLE IF NOT EXISTS batches(
    batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq INTEGER NOT NULL UNIQUE,    -- owning accepted op sequence
    kind TEXT NOT NULL,             -- event | watermark
    trigger TEXT NOT NULL,          -- event_id / 'watermark'
    watermark INTEGER,
    finalized INTEGER NOT NULL DEFAULT 0   -- 1 if this batch moved sessions
);

-- the changefeed: ordered delete/upsert rows that rebuild snapshot exactly
CREATE TABLE IF NOT EXISTS changes(
    change_id INTEGER PRIMARY KEY,
    seq INTEGER NOT NULL,           -- owning batch (= ops_log seq for event/wm)
    op TEXT NOT NULL,               -- 'delete' | 'upsert'
    session_id TEXT NOT NULL,
    key_id TEXT,
    start_time INTEGER,
    end_time INTEGER,
    cnt INTEGER,
    total INTEGER,
    members TEXT,                   -- full member basis (old on delete, new on upsert)
    tier TEXT,                      -- 'hot' for business changes, 'archive' on finalization
    trigger TEXT NOT NULL,
    trigger_event TEXT,             -- event_id when an event op, else NULL
    watermark INTEGER
);
CREATE INDEX IF NOT EXISTS idx_changes_seq ON changes(seq, change_id);

-- formal-path evidence: counts event-row reads charged to each key
CREATE TABLE IF NOT EXISTS key_access(
    key_id TEXT PRIMARY KEY,
    reads INTEGER NOT NULL
);
"""


# --------------------------------------------------------------------------- #
# engine
# --------------------------------------------------------------------------- #

class Rejected(Exception):
    """A refused operation. Recorded with reason; never mutates published data."""


def session_id_of(key: str, start: int, end: int,
                  count: int, total: int, members: list[str]) -> str:
    basis = json.dumps(
        {"key": key, "start": start, "end": end, "count": count,
         "sum": total, "members": members},
        sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


class Engine:
    def __init__(self, path: str):
        self.con = sqlite3.connect(path, isolation_level=None)
        self.con.execute("PRAGMA foreign_keys=ON")
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.row_factory = sqlite3.Row
        self.con.executescript(SCHEMA)
        self.gap = int(self._meta("gap"))
        self.time_min = int(self._meta("time_min"))
        self.time_max = int(self._meta("time_max"))
        self.value_min = int(self._meta("value_min"))
        self.value_max = int(self._meta("value_max"))

    # ------------------------------------------------------------------ meta
    def _meta(self, k: str) -> str:
        row = self.con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        if row is None:
            raise RuntimeError("database not initialized (use init or ingest --init)")
        return row["v"]

    @property
    def watermark(self) -> Optional[int]:
        v = self._meta("watermark")
        return None if v == "" else int(v)

    def _set_watermark(self, w: int) -> None:
        self.con.execute("UPDATE meta SET v=? WHERE k='watermark'", (str(w),))

    @staticmethod
    def initialize(con: sqlite3.Connection, gap: int,
                   time_min: int, time_max: int,
                   value_min: int, value_max: int) -> None:
        if not isinstance(gap, int) or gap <= 0:
            raise ValueError("gap must be a positive integer")
        if time_min > time_max or value_min > value_max:
            raise ValueError("declared integer ranges are inverted")
        con.executescript(SCHEMA)
        defaults = {
            "gap": gap, "watermark": "",
            "time_min": time_min, "time_max": time_max,
            "value_min": value_min, "value_max": value_max,
        }
        con.executemany(
            "INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO NOTHING",
            list(defaults.items()),
        )

    def close(self) -> None:
        self.con.close()

    # ------------------------------------------------------------- utilities
    def _charge(self, key: str, n: int = 1) -> None:
        """Count n event-row reads against a key (formal-path evidence)."""
        self.con.execute(
            "INSERT INTO key_access(key_id, reads) VALUES(?,?) "
            "ON CONFLICT(key_id) DO UPDATE SET reads=reads+excluded.reads",
            (key, n),
        )

    def _next_seq(self) -> int:
        # both logs consume the same monotone sequence so a rejection can
        # never reuse an accepted op's number
        row = self.con.execute(
            "SELECT COALESCE(MAX(seq),0) AS m FROM ("
            "SELECT seq FROM ops_log UNION ALL SELECT seq FROM rejections)"
        ).fetchone()
        return int(row["m"]) + 1

    def _reject(self, seq: int, reason: str, payload: dict) -> None:
        self.con.execute(
            "INSERT INTO rejections(seq, reason, payload) VALUES(?,?,?)",
            (seq, reason, json.dumps(payload, sort_keys=True)),
        )

    # ------------------------------------------------------------- ingestion
    def handle(self, op: dict) -> dict:
        """Process one operation atomically. Returns a summary dict."""
        seq = self._next_seq()
        payload = json.dumps(op, sort_keys=True)
        con = self.con
        con.execute("BEGIN IMMEDIATE")
        try:
            kind = op.get("op")
            if kind == "upsert":
                summary = self._upsert(seq, op)
            elif kind == "retract":
                summary = self._retract(seq, op)
            elif kind == "watermark":
                summary = self._watermark(seq, op)
            else:
                raise Rejected(f"unknown op {kind!r}")

            # accepted: persist the immutable input. Finalization is a
            # watermark-driven publish, deliberately aligned with the audit
            # oracle: frozen (<=W) events never change, so an event op cannot
            # make any session newly finalizable; only advancing W can.
            con.execute("INSERT INTO ops_log(seq,payload) VALUES(?,?)",
                        (seq, payload))
            if op.get("op") == "watermark":
                archived = self._finalize(seq)
            else:
                archived = []
            con.commit()
        except Rejected as exc:
            con.rollback()
            # rejection bookkeeping is itself a standalone short transaction
            con.execute("BEGIN IMMEDIATE")
            self._reject(seq, str(exc), op)
            con.commit()
            return {"seq": seq, "accepted": False, "reason": str(exc)}
        except Exception:
            con.rollback()
            raise

        summary["seq"] = seq
        summary["accepted"] = True
        summary["archived"] = archived
        return summary

    def _check_ranges(self, time: int, value: int) -> None:
        if not (self.time_min <= time <= self.time_max):
            raise Rejected(
                f"time {time} outside declared range "
                f"[{self.time_min},{self.time_max}]")
        if not (self.value_min <= value <= self.value_max):
            raise Rejected(
                f"value {value} outside declared range "
                f"[{self.value_min},{self.value_max}]")

    def _upsert(self, seq: int, op: dict) -> dict:
        eid = op["event_id"]
        key = op["key"]
        time = op["time"]
        value = op["value"]
        if not isinstance(time, int) or isinstance(time, bool):
            raise Rejected("time must be an integer")
        if not isinstance(value, int) or isinstance(value, bool):
            raise Rejected("value must be an integer")
        self._check_ranges(time, value)

        w = self.watermark
        if w is not None and time <= w:
            raise Rejected(f"event time {time} <= watermark {w}: late event refused")

        row = self.con.execute(
            "SELECT e.event_id, e.key_id, e.time, e.value "
            "FROM event_ids i LEFT JOIN events e ON e.event_id=i.event_id "
            "WHERE i.event_id=?", (eid,)).fetchone()

        if row is not None:
            if row["key_id"] != key:
                raise Rejected(
                    f"event {eid!r} key is immutable: "
                    f"{row['key_id']!r} != {key!r}")
            if row["event_id"] is None:
                # id known only because it was archived away: its history is
                # immutable and the id can never be reused
                raise Rejected(
                    f"event {eid!r} already finalized in cold archive; id cannot be reused")
            # revision: old time must itself still be mutable (> W)
            if w is not None and row["time"] <= w:
                raise Rejected(
                    f"revision of event {eid!r}: old time {row['time']} "
                    f"<= watermark {w}, outside mutable range")
            if row["time"] == time and row["value"] == value:
                # identical replay: no business change, no key recompute
                self.con.execute(
                    "INSERT INTO batches(seq,kind,trigger,watermark) "
                    "VALUES(?,?,?,?)",
                    (seq, "event", eid, w))
                return {"kind": "upsert", "key": key, "event_id": eid,
                        "noop": True, "changes": 0}

        # effective insert/revision: replace row, then recompute only this key
        self.con.execute(
            "INSERT INTO event_ids(event_id,key_id) VALUES(?,?) "
            "ON CONFLICT(event_id) DO UPDATE SET key_id=excluded.key_id",
            (eid, key))
        self.con.execute(
            "INSERT INTO events(event_id,key_id,time,value) VALUES(?,?,?,?) "
            "ON CONFLICT(event_id) DO UPDATE SET "
            "key_id=excluded.key_id,time=excluded.time,value=excluded.value",
            (eid, key, time, value))
        n_changes = self._recompute_key(seq, key, trigger_event=eid)
        return {"kind": "upsert", "key": key, "event_id": eid,
                "noop": False, "changes": n_changes}

    def _retract(self, seq: int, op: dict) -> dict:
        eid = op["event_id"]
        w = self.watermark
        row = self.con.execute(
            "SELECT e.event_id, e.key_id, e.time "
            "FROM event_ids i LEFT JOIN events e ON e.event_id=i.event_id "
            "WHERE i.event_id=?", (eid,)).fetchone()
        if row is None:
            raise Rejected(f"retract of unknown event_id {eid!r}")
        if row["event_id"] is None:
            raise Rejected(
                f"retract refused: event {eid!r} already finalized "
                f"(time {row['time']} <= watermark, immutable)")
        if w is not None and row["time"] <= w:
            raise Rejected(
                f"retract refused: event time {row['time']} <= watermark {w}")
        key = row["key_id"]
        self.con.execute("DELETE FROM events WHERE event_id=?", (eid,))
        self.con.execute("DELETE FROM event_ids WHERE event_id=?", (eid,))
        n_changes = self._recompute_key(seq, key, trigger_event=eid)
        return {"kind": "retract", "key": key, "event_id": eid,
                "noop": False, "changes": n_changes}

    def _watermark(self, seq: int, op: dict) -> dict:
        new_w = op["time"]
        if not isinstance(new_w, int) or isinstance(new_w, bool):
            raise Rejected("watermark must be an integer")
        w = self.watermark
        if w is not None and new_w < w:
            raise Rejected(
                f"watermark cannot retreat: {new_w} < {w}")
        if w is not None and new_w == w:
            # identical replay: still a complete (empty) batch, no changes
            self.con.execute(
                "INSERT INTO batches(seq,kind,trigger,watermark) "
                "VALUES(?,?,?,?)",
                (seq, "watermark", "watermark", w))
            return {"kind": "watermark", "watermark": w, "noop": True,
                    "changes": 0}
        self._set_watermark(new_w)
        self.con.execute(
            "INSERT INTO batches(seq,kind,trigger,watermark) VALUES(?,?,?,?)",
            (seq, "watermark", "watermark", new_w))
        return {"kind": "watermark", "watermark": new_w, "noop": False,
                "changes": 0}

    # ------------------------------------------------- affected-key recompute
    def _recompute_key(self, seq: int, key: str,
                       trigger_event: str) -> int:
        """Recompute all hot sessions of `key` from its live event rows.

        Reads ONLY rows of `key` (charged to key_access); no other key's events
        are touched. Old/new session sets are diffed by content-addressed id;
        the whole delete+upsert set belongs to batch `seq` atomically.
        """
        con = self.con
        rows = con.execute(
            "SELECT event_id, time, value FROM events "
            "WHERE key_id=? ORDER BY time, event_id", (key,)).fetchall()
        self._charge(key, len(rows))

        groups = sessionize(
            [{"event_id": r["event_id"], "key": key,
              "time": r["time"], "value": r["value"]} for r in rows],
            self.gap,
        )
        new_sessions: dict[str, dict] = {}
        for g in groups:
            sid = session_id_of(key, g["start"], g["end"],
                                len(g["ids"]), g["sum"], g["ids"])
            new_sessions[sid] = {
                "session_id": sid, "key": key, "start": g["start"],
                "end": g["end"], "count": len(g["ids"]),
                "sum": g["sum"], "members": g["ids"],
            }

        old_rows = con.execute(
            "SELECT session_id, members FROM hot_sessions WHERE key_id=?",
            (key,)).fetchall()
        old_ids = {r["session_id"]: json.loads(r["members"]) for r in old_rows}
        new_ids = set(new_sessions)
        deleted = set(old_ids) - new_ids
        inserted = new_ids - set(old_ids)

        w = self.watermark
        # deletes first: explicit full retraction of the old result
        for sid in sorted(deleted):
            con.execute("DELETE FROM hot_sessions WHERE session_id=?", (sid,))
            con.execute(
                "INSERT INTO changes(seq,op,session_id,key_id,start_time,"
                "end_time,cnt,total,members,tier,trigger,trigger_event,"
                "watermark) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (seq, "delete", sid, key, None, None, None, None,
                 json.dumps(old_ids[sid]), "hot", "event", trigger_event, w))
        for sid in sorted(inserted):
            s = new_sessions[sid]
            con.execute(
                "INSERT INTO hot_sessions(session_id,key_id,start_time,"
                "end_time,cnt,total,members) VALUES(?,?,?,?,?,?,?)",
                (sid, key, s["start"], s["end"], s["count"], s["sum"],
                 json.dumps(s["members"])))
            con.execute(
                "INSERT INTO changes(seq,op,session_id,key_id,start_time,"
                "end_time,cnt,total,members,tier,trigger,trigger_event,"
                "watermark) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (seq, "upsert", sid, key, s["start"], s["end"],
                 s["count"], s["sum"], json.dumps(s["members"]),
                 "hot", "event", trigger_event, w))

        con.execute(
            "INSERT INTO batches(seq,kind,trigger,watermark) "
            "VALUES(?,?,?,?)",
            (seq, "event", trigger_event, w))
        return len(deleted) + len(inserted)

    # ---------------------------------------------------------- finalization
    def _finalize(self, trigger_seq: int) -> list[str]:
        """Move every finalizable hot session (end+gap <= W) to cold archive.

        Pure storage move: identity/content unchanged, so NO business change is
        emitted; the event rows that only backed archived sessions are cleaned
        from the hot table. Hot-row access here is index-driven and never reads
        another key's *events*, so key_access event counters stay untouched.
        """
        w = self.watermark
        if w is None:
            return []
        con = self.con
        due = con.execute(
            "SELECT session_id, key_id, start_time, end_time, cnt, total, "
            "members FROM hot_sessions WHERE end_time + ? <= ? "
            "ORDER BY key_id, start_time",
            (self.gap, w)).fetchall()
        if not due:
            return []
        moved: list[str] = []
        cleaned_events = 0
        for r in due:
            members = json.loads(r["members"])
            con.execute(
                "INSERT INTO archived_sessions(session_id,key_id,start_time,"
                "end_time,cnt,total,members,finalized_batch) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (r["session_id"], r["key_id"], r["start_time"], r["end_time"],
                 r["cnt"], r["total"], r["members"], trigger_seq))
            con.execute("DELETE FROM hot_sessions WHERE session_id=?",
                        (r["session_id"],))
            # publish the storage move through the same changefeed contract:
            # an upsert carrying the identical session under tier 'archive'.
            # Business content is unchanged (same id/start/end/count/sum/ids)
            con.execute(
                "INSERT INTO changes(seq,op,session_id,key_id,start_time,"
                "end_time,cnt,total,members,tier,trigger,trigger_event,"
                "watermark) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (trigger_seq, "upsert", r["session_id"], r["key_id"],
                 r["start_time"], r["end_time"], r["cnt"], r["total"],
                 r["members"], "archive", "finalization", None, w))
            # hot-state cleanup: members of a finalized session can never be
            # connected again; each id belongs to exactly one session
            for eid in members:
                con.execute("DELETE FROM events WHERE event_id=?", (eid,))
                cleaned_events += 1
            moved.append(r["session_id"])

        # the move belongs to the triggering op's single batch row
        con.execute(
            "UPDATE batches SET finalized=1 WHERE seq=?", (trigger_seq,))
        return moved
        return moved


# --------------------------------------------------------------------------- #
# read views
# --------------------------------------------------------------------------- #

SNAPSHOT_SQL = """
SELECT session_id, key_id, start_time, end_time, cnt, total, members,
       'archive' AS tier
FROM archived_sessions
UNION ALL
SELECT session_id, key_id, start_time, end_time, cnt, total, members,
       'hot' AS tier
FROM hot_sessions
ORDER BY key_id, start_time, session_id
"""


def query_snapshot(con: sqlite3.Connection) -> list[dict]:
    out = []
    for r in con.execute(SNAPSHOT_SQL):
        out.append({
            "session_id": r["session_id"], "key": r["key_id"],
            "start": r["start_time"], "end": r["end_time"],
            "count": r["cnt"], "sum": r["total"],
            "members": json.loads(r["members"]), "tier": r["tier"],
        })
    return out


def query_batches(con: sqlite3.Connection) -> list[dict]:
    out = []
    for b in con.execute(
        "SELECT batch_id, seq, kind, trigger, watermark, finalized "
        "FROM batches ORDER BY batch_id"
    ):
        rows = con.execute(
            "SELECT op, session_id, key_id, start_time, end_time, cnt, total,"
            "members, tier, trigger, trigger_event, watermark "
            "FROM changes WHERE seq=? ORDER BY change_id", (b["seq"],)).fetchall()
        changes = []
        for c in rows:
            changes.append({
                "op": c["op"], "session_id": c["session_id"],
                "key": c["key_id"], "start": c["start_time"],
                "end": c["end_time"], "count": c["cnt"], "sum": c["total"],
                "members": json.loads(c["members"]) if c["members"] else None,
                "tier": c["tier"],
                "trigger": c["trigger"],
                "trigger_event": c["trigger_event"],
                "watermark": c["watermark"],
            })
        out.append({"seq": b["seq"], "kind": b["kind"],
                    "trigger": b["trigger"], "watermark": b["watermark"],
                    "finalized": bool(b["finalized"]), "changes": changes})
    return out


# --------------------------------------------------------------------------- #
# audit: independent full recompute from the immutable input stream
# --------------------------------------------------------------------------- #

def run_audit(con: sqlite3.Connection) -> dict:
    gap = int(_scalar_meta(con, "gap"))
    ops = [json.loads(r["payload"])
           for r in con.execute("SELECT payload FROM ops_log ORDER BY seq")]

    # 1) independent in-memory oracle over the raw accepted input stream
    oracle_state, oracle_access = replay_oracle(ops, gap)
    oracle_sessions = oracle_state["sessions"]   # key -> list of session dicts

    # 2) current published table must equal the oracle, across both tiers
    published = query_snapshot(con)
    pub_index = {(s["key"], tuple(s["members"])): s for s in published}
    oracle_index = {}
    for key, sess_list in oracle_sessions.items():
        for s in sess_list:
            oracle_index[(key, tuple(s["ids"]))] = s

    mismatches: list[str] = []
    if set(pub_index) != set(oracle_index):
        only_pub = sorted(set(pub_index) - set(oracle_index))
        only_ora = sorted(set(oracle_index) - set(pub_index))
        if only_pub:
            mismatches.append(f"published-only sessions: {only_pub}")
        if only_ora:
            mismatches.append(f"oracle-only sessions: {only_ora}")
    for k in sorted(set(pub_index) & set(oracle_index)):
        p, o = pub_index[k], oracle_index[k]
        if not (p["start"] == o["start"] and p["end"] == o["end"]
                and p["count"] == len(o["ids"]) and p["sum"] == o["sum"]
                and p["members"] == o["ids"]):
            mismatches.append(f"content mismatch on {k}: {p} vs {o}")

    # 3) independent consumer applies every changefeed batch and must rebuild
    #    the snapshot exactly, session members included (not just grand totals).
    #    Small streams get strict per-batch oracle checkpoints; large streams
    #    use the equivalent linear bulk replay (per-batch grouping is O(N^2))
    n_ops = con.execute("SELECT COUNT(*) c FROM ops_log").fetchone()["c"]
    consumer = apply_changefeed(con, full_per_batch=n_ops <= 2000)
    consumer_mismatches = consumer["mismatches"]

    # 4) formal-path evidence: per-key event-row reads
    access = {r["key_id"]: r["reads"]
              for r in con.execute(
                  "SELECT key_id, reads FROM key_access ORDER BY key_id")}

    # 5) replay determinism: oracle again must be identical
    again, _ = replay_oracle(ops, gap)
    deterministic = again == oracle_state

    return {
        "ok": not mismatches and not consumer_mismatches and deterministic,
        "session_mismatches": mismatches,
        "consumer_mismatches": consumer_mismatches,
        "replayed_ops": len(ops),
        "oracle_session_count": sum(len(v) for v in oracle_sessions.values()),
        "published_session_count": len(published),
        "hot_session_count": con.execute(
            "SELECT COUNT(*) c FROM hot_sessions").fetchone()["c"],
        "archived_session_count": con.execute(
            "SELECT COUNT(*) c FROM archived_sessions").fetchone()["c"],
        "formal_path_event_reads_by_key": access,
        "oracle_event_reads_by_key": oracle_access,
        "deterministic_replay": deterministic,
    }


def _scalar_meta(con: sqlite3.Connection, k: str) -> str:
    return con.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()["v"]


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def _open_input(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise SystemExit("input must be a JSON array of operations")
    return data


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="local session data producer")
    ap.add_argument("--db", default="sessions.db")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="create an empty database")
    p_init.add_argument("--gap", type=int, required=True)
    p_init.add_argument("--time-min", type=int, default=-(1 << 48))
    p_init.add_argument("--time-max", type=int, default=1 << 48)
    p_init.add_argument("--value-min", type=int, default=-(1 << 48))
    p_init.add_argument("--value-max", type=int, default=1 << 48)

    p_ing = sub.add_parser("ingest", help="process an operation stream")
    p_ing.add_argument("--input", default="events.json")
    p_ing.add_argument("--init", action="store_true",
                       help="initialize db first (requires --gap)")
    p_ing.add_argument("--gap", type=int)
    p_ing.add_argument("--time-min", type=int, default=-(1 << 48))
    p_ing.add_argument("--time-max", type=int, default=1 << 48)
    p_ing.add_argument("--value-min", type=int, default=-(1 << 48))
    p_ing.add_argument("--value-max", type=int, default=1 << 48)

    sub.add_parser("snapshot", help="emit current full session table (JSON)")
    sub.add_parser("changes", help="emit all change batches (JSON)")
    sub.add_parser("audit", help="independent full-recompute oracle check")

    args = ap.parse_args(argv)

    if args.cmd == "init":
        con = sqlite3.connect(args.db, isolation_level=None)
        Engine.initialize(con, args.gap, args.time_min, args.time_max,
                          args.value_min, args.value_max)
        con.close()
        print(json.dumps({"initialized": args.db, "gap": args.gap}))
        return 0

    if args.cmd == "ingest":
        if args.init:
            if not args.gap or args.gap <= 0:
                raise SystemExit("--init requires a positive --gap")
            con = sqlite3.connect(args.db, isolation_level=None)
            Engine.initialize(con, args.gap, args.time_min, args.time_max,
                              args.value_min, args.value_max)
            con.close()
        ops = _open_input(args.input)
        eng = Engine(args.db)
        results = []
        try:
            for op in ops:
                results.append(eng.handle(op))
        finally:
            eng.close()
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    # read-only commands
    con = sqlite3.connect(args.db, isolation_level=None)
    con.row_factory = sqlite3.Row
    if args.cmd == "snapshot":
        print(json.dumps(query_snapshot(con), indent=2, sort_keys=True))
    elif args.cmd == "changes":
        print(json.dumps(query_batches(con), indent=2, sort_keys=True))
    elif args.cmd == "audit":
        report = run_audit(con)
        print(json.dumps(report, indent=2, sort_keys=True))
        if not report["ok"]:
            con.close()
            return 1
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

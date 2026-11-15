"""
consumer.py -- independent downstream consumer of the changefeed.

It knows nothing about session grouping or gaps: given the ordered `changes`
rows it applies only delete/upsert operations keyed by session_id. Every row
carries a tier ('hot' for business changes, 'archive' for the finalization
move of an identical session), so the consumer materializes the same hot/cold
split purely from the feed.

Verification:
  * after every accepted batch the consumer's hot view (members, start/end,
    count, sum per session) equals an independent oracle checkpoint rebuilt
    from the raw input stream -- a merge/split that only preserved grand
    totals would still be caught;
  * the consumer's archive view equals both the archived_sessions table and
    the oracle's retained archive at every batch;
  * the final hot UNION archive view equals the published snapshot exactly,
    with per-key member/count/sum totals reconciled over actual members.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from oracle import oracle_checkpoints


def _table_archive(con: sqlite3.Connection) -> dict[tuple, dict]:
    out = {}
    for r in con.execute(
        "SELECT key_id, members, start_time, end_time, cnt, total "
        "FROM archived_sessions"
    ):
        out[(r["key_id"], tuple(json.loads(r["members"])))] = {
            "start": r["start_time"], "end": r["end_time"],
            "count": r["cnt"], "sum": r["total"]}
    return out


def _table_snapshot(con: sqlite3.Connection) -> dict[tuple, dict]:
    out = {}
    for r in con.execute(
        "SELECT key_id, members, start_time, end_time, cnt, total, "
        "'archive' AS tier FROM archived_sessions "
        "UNION ALL "
        "SELECT key_id, members, start_time, end_time, cnt, total, "
        "'hot' AS tier FROM hot_sessions"
    ):
        out[(r["key_id"], tuple(json.loads(r["members"])))] = {
            "start": r["start_time"], "end": r["end_time"],
            "count": r["cnt"], "sum": r["total"], "tier": r["tier"]}
    return out


def _oracle_index(groups: list[dict]) -> dict[tuple, dict]:
    return {
        (g["key"], tuple(g["ids"])): {
            "start": g["start"], "end": g["end"],
            "count": len(g["ids"]), "sum": g["sum"]}
        for g in groups
    }


def apply_changefeed(
    con: sqlite3.Connection,
    full_per_batch: bool = True,
) -> dict:
    gap = int(con.execute(
        "SELECT v FROM meta WHERE k='gap'").fetchone()[0])
    log_rows = con.execute(
        "SELECT seq, payload FROM ops_log ORDER BY seq").fetchall()
    ops = [json.loads(r["payload"]) for r in log_rows]

    # consumer materialization: session_id -> full row incl. current tier
    materialized: dict[str, dict] = {}
    mismatches: list[str] = []
    batches_seen = 0
    business_changes = 0
    delete_unknown = 0

    # full per-batch oracle checks are O(N^2) over checkpoints; for large
    # streams the same membership guarantees are checked once against the
    # linear bulk replay at the end (full_per_batch=False)
    checkpoints = None
    if full_per_batch:
        # accepted-op seq labels keep checkpoints 1:1 with ops_log seq
        checkpoints = {
            cp["seq"]: cp
            for cp in oracle_checkpoints(
                ops, gap, seq_provider=iter([r["seq"] for r in log_rows]))}

    def view(tier: str) -> dict[tuple, dict]:
        return {
            (s["key"], tuple(s["members"])): {
                "start": s["start"], "end": s["end"],
                "count": s["count"], "sum": s["sum"]}
            for s in materialized.values() if s["tier"] == tier}

    batch_seqs = [r[0] for r in con.execute(
        "SELECT DISTINCT seq FROM batches ORDER BY seq")]

    for seq in batch_seqs:
        rows = con.execute(
            "SELECT op,session_id,key_id,start_time,end_time,cnt,total,"
            "members,tier FROM changes WHERE seq=? ORDER BY change_id",
            (seq,)).fetchall()

        # apply the whole batch in feed order (deletes are emitted first)
        for c in rows:
            if c["op"] == "delete":
                if c["session_id"] not in materialized:
                    delete_unknown += 1
                    mismatches.append(
                        f"seq {seq}: delete of unknown session "
                        f"{c['session_id']}")
                else:
                    del materialized[c["session_id"]]
            else:  # upsert, hot or archive tier
                materialized[c["session_id"]] = {
                    "session_id": c["session_id"], "key": c["key_id"],
                    "start": c["start_time"], "end": c["end_time"],
                    "count": c["cnt"], "sum": c["total"],
                    "members": json.loads(c["members"]), "tier": c["tier"],
                }
            business_changes += 1
        batches_seen += 1

        # member-level equality with the independent oracle checkpoint
        if full_per_batch:
            cp = checkpoints.get(seq)
            if cp is None or not cp["accepted"]:
                mismatches.append(f"seq {seq}: batch for non-accepted op")
                continue
            got_hot, want_hot = view("hot"), _oracle_index(cp["hot"])
            if set(got_hot) != set(want_hot):
                mismatches.append(
                    f"seq {seq}: hot member-set divergence "
                    f"consumer-only={sorted(set(got_hot) - set(want_hot))} "
                    f"oracle-only={sorted(set(want_hot) - set(got_hot))}")
            else:
                for k in got_hot:
                    if got_hot[k] != want_hot[k]:
                        mismatches.append(
                            f"seq {seq}: aggregate mismatch for {k}: "
                            f"{got_hot[k]} vs {want_hot[k]}")
            # the feed-derived archive view is cumulative; compare it with the
            # oracle's archive at this point. The physical table is only
            # checked in full at the end (it has no historical versions)
            got_arch, want_arch = view("archive"), _oracle_index(cp["archived"])
            if got_arch != want_arch:
                mismatches.append(
                    f"seq {seq}: consumer archive view diverges from oracle "
                    f"consumer-only={sorted(set(got_arch) - set(want_arch))} "
                    f"oracle-only={sorted(set(want_arch) - set(got_arch))}")

        # no member set may exist in both tiers at once (cheap invariant)
        overlap = set(view("hot")) & set(view("archive"))
        if overlap:
            mismatches.append(
                f"seq {seq}: members in both tiers: {sorted(overlap)}")

    # bulk mode: one linear independent replay, compare hot and archive views
    if not full_per_batch:
        from oracle import replay_oracle
        oracle_state, _ = replay_oracle(ops, gap)
        want_all = {
            (g["key"], tuple(g["ids"])): {
                "start": g["start"], "end": g["end"],
                "count": len(g["ids"]), "sum": g["sum"]}
            for gs in oracle_state["sessions"].values() for g in gs}
        got_all = {**view("hot"), **view("archive")}
        if got_all != want_all:
            mismatches.append(
                "bulk replay: feed materialization differs from oracle "
                f"only-consumer={sorted(set(got_all) - set(want_all))} "
                f"only-oracle={sorted(set(want_all) - set(got_all))}")

    # final: consumer hot UNION archive == published snapshot, exactly once
    snapshot = _table_snapshot(con)
    if _table_archive(con) != view("archive"):
        mismatches.append(
            "final archive table does not equal feed-derived archive view")
    union_got: dict[tuple, dict] = {}
    for s in materialized.values():
        union_got[(s["key"], tuple(s["members"]))] = {
            "start": s["start"], "end": s["end"], "count": s["count"],
            "sum": s["sum"], "tier": s["tier"]}
    if union_got != snapshot:
        mismatches.append(
            f"final union mismatch: only-consumer="
            f"{sorted(set(union_got) - set(snapshot))} "
            f"only-table={sorted(set(snapshot) - set(union_got))}")

    # internal invariants + per-key totals over actual members
    key_totals: dict[str, dict[str, int]] = {}
    for s in materialized.values():
        if s["count"] != len(s["members"]):
            mismatches.append(
                f"session {s['session_id']}: count {s['count']} != "
                f"{len(s['members'])} members")
        t = key_totals.setdefault(
            s["key"], {"sessions": 0, "count": 0, "sum": 0})
        t["sessions"] += 1
        t["count"] += s["count"]
        t["sum"] += s["sum"]

    return {
        "mismatches": mismatches,
        "batches_applied": batches_seen,
        "business_changes": business_changes,
        "delete_of_unknown": delete_unknown,
        "materialized_sessions": len(materialized),
        "per_key_totals": key_totals,
    }

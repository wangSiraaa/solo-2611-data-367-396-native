"""
oracle.py -- independent full-recompute oracle.

This module deliberately does NOT import or reuse any incremental state from
sessionizer. It rebuilds the whole world from the raw accepted operation
stream: plain dicts of active/archived events + the same grouping rule as
probe.py (the small-sample reference). It is the audit source of truth and
also tracks its own event-row reads per key.

`oracle_checkpoints` yields the complete state after every accepted op so an
independent consumer can verify each change batch, not only the final table.
`replay_oracle` is the linear-time bulk variant used by the audit on large
streams (read counters maintained from per-key active counts, no rescans).
"""

from __future__ import annotations

from typing import Any, Iterator, Optional


def sessionize(events: list[dict], gap: int) -> list[dict]:
    """Group events into sessions: same key, (time, event_id) order, adjacent
    time difference <= gap stays in one session; same-tick distinct ids all
    contribute. Pure function shared by the oracle and the engine."""
    out: list[dict] = []
    for key in sorted({r["key"] for r in events}):
        rows = sorted(
            (r for r in events if r["key"] == key),
            key=lambda r: (r["time"], r["event_id"]),
        )
        current: list[dict] = []
        for row in rows:
            if current and row["time"] - current[-1]["time"] > gap:
                out.append(_make_group(key, current))
                current = []
            current.append(row)
        if current:
            out.append(_make_group(key, current))
    return out


def _make_group(key: str, rows: list[dict]) -> dict:
    ids = sorted(r["event_id"] for r in rows)
    return {
        "key": key,
        "start": rows[0]["time"],
        "end": rows[-1]["time"],
        "ids": ids,
        "sum": sum(r["value"] for r in rows),
    }


def _groups_for(evs: list[dict], gap: int) -> list[dict]:
    rows = sorted(evs, key=lambda r: (r["time"], r["event_id"]))
    groups: list[dict] = []
    current: list[dict] = []
    for row in rows:
        if current and row["time"] - current[-1]["time"] > gap:
            groups.append(_make_group(current[0]["key"], current))
            current = []
        current.append(row)
    if current:
        groups.append(_make_group(current[0]["key"], current))
    return groups


class _OracleEngine:
    """Stream replayer independent of sessionizer's incremental tables."""

    def __init__(self, gap: int):
        self.gap = gap
        self.active: dict[str, tuple[str, int, int]] = {}  # eid -> (k,t,v)
        self.id_key: dict[str, str] = {}
        self.key_count: dict[str, int] = {}
        self.archived: list[dict] = []
        self.retired: set[str] = set()
        self.watermark: Optional[int] = None
        self.reads: dict[str, int] = {}
        self.rejected: list[dict] = []

    def _finalize(self, w: int) -> None:
        by_key: dict[str, list[dict]] = {}
        for eid, (k, t, v) in self.active.items():
            by_key.setdefault(k, []).append(
                {"event_id": eid, "key": k, "time": t, "value": v})
        for key, evs in by_key.items():
            for g in _groups_for(evs, self.gap):
                if g["end"] + self.gap <= w:
                    self.archived.append(g)
                    for eid in g["ids"]:
                        self.active.pop(eid, None)
                        self.key_count[key] -= 1
                        self.retired.add(eid)

    def handle(self, op: dict) -> tuple[bool, Optional[str]]:
        kind = op["op"]
        reason: Optional[str] = None
        if kind == "upsert":
            eid = op["event_id"]
            key, t, val = op["key"], op["time"], op["value"]
            if self.watermark is not None and t <= self.watermark:
                reason = f"event time {t} <= watermark {self.watermark}"
            elif eid in self.id_key and self.id_key[eid] != key:
                reason = "key immutable"
            elif eid in self.retired:
                reason = "event id retired in archive"
            elif (eid in self.active and self.watermark is not None
                  and self.active[eid][1] <= self.watermark):
                reason = "old event time frozen by watermark"
            else:
                is_new = eid not in self.active
                self.active[eid] = (key, t, val)
                self.id_key[eid] = key
                if is_new:
                    self.key_count[key] = self.key_count.get(key, 0) + 1
                # event rows read by this recompute: whole live set of key
                self.reads[key] = self.reads.get(key, 0) + self.key_count[key]
        elif kind == "retract":
            eid = op["event_id"]
            if eid not in self.id_key:
                reason = "unknown event_id"
            elif eid in self.retired:
                reason = "event id retired in archive"
            else:
                key, t = self.id_key[eid], self.active[eid][1]
                if self.watermark is not None and t <= self.watermark:
                    reason = f"event time {t} <= watermark {self.watermark}"
                else:
                    self.active.pop(eid, None)
                    self.id_key.pop(eid, None)
                    self.key_count[key] -= 1
                    self.reads[key] = self.reads.get(key, 0) + self.key_count[key]
        elif kind == "watermark":
            new_w = op["time"]
            if self.watermark is not None and new_w < self.watermark:
                reason = "watermark retreat"
            else:
                self.watermark = new_w
                self._finalize(new_w)   # finalization driven by watermark only
        else:
            reason = f"unknown op {kind!r}"

        if reason is not None:
            self.rejected.append({"op": op, "reason": reason})
        return reason is None, reason

    def hot_groups(self) -> list[dict]:
        return sessionize(
            [{"event_id": e, "key": k, "time": t, "value": v}
             for e, (k, t, v) in self.active.items()], self.gap)


def oracle_checkpoints(
    ops: list[dict],
    gap: int,
    seq_provider: Optional[Any] = None,
) -> Iterator[dict]:
    """Yield an independent full state snapshot after each input op.

    Rejections are decided purely from the stream and the watermark derived
    from it. Finalized sessions are retained in an immutable archive (never
    dropped), mirroring the engine's hot/cold split.

    `seq_provider`, when given, is an iterator whose next value labels each
    checkpoint (used to align with the engine's dense ops_log sequence, which
    counts no-op replays as accepted ops). Default labels are 1..N.
    """
    seq_iter = seq_provider if seq_provider is not None else iter(
        range(1, len(ops) + 1))
    engine = _OracleEngine(gap)
    for op in ops:
        seq = next(seq_iter)
        accepted, reason = engine.handle(op)
        yield {
            "seq": seq,
            "accepted": accepted,
            "reject_reason": reason,
            "watermark": engine.watermark,
            "archived": [dict(g) for g in engine.archived],
            "hot": engine.hot_groups(),
            "rejected": [dict(r) for r in engine.rejected],
            "reads": dict(engine.reads),
        }


def replay_oracle(ops: list[dict], gap: int) -> tuple[dict, dict[str, int]]:
    """Linear bulk replay: final state + per-key event-read counts.

    Only the final live set is grouped; intermediate checkpoints are skipped.
    Read counters come from maintained per-key active counts, so the total
    work is linear in the stream length rather than quadratic.
    """
    engine = _OracleEngine(gap)
    for op in ops:
        engine.handle(op)
    sessions: dict[str, list[dict]] = {}
    for g in list(engine.archived) + engine.hot_groups():
        sessions.setdefault(g["key"], []).append(g)
    return (
        {"sessions": sessions, "rejected": engine.rejected,
         "watermark": engine.watermark,
         "retired": sorted(e for g in engine.archived for e in g["ids"])},
        dict(engine.reads),
    )

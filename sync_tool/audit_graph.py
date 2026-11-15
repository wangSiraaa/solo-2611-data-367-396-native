"""Independent, small-scale event-graph interpreter used only by ``audit``.

It deliberately shares no code with the live replication path: it takes plain
event dictionaries and recomputes readiness, heads, status and conflict members
from parents alone.  ``audit`` uses it to cross-check the materialised
projections; the real data path never imports this module.
"""


def _canon_state(event):
    state = event.get("state")
    if state is None:
        return None
    return {"name": state["name"], "qty": state["qty"]}


def interpret(events):
    """Return ``{row_id: {...}}`` for a full set of version events.

    A version is ready iff every parent is a ready version of the same row.  A
    ready version is a head iff no other ready version names it as a parent.
    """
    rows = {}
    for e in events:
        rows.setdefault(e["row_id"], {})[e["version_id"]] = e
    result = {}
    for row_id, versions in rows.items():
        ready = set()
        changed = True
        while changed:
            changed = False
            for vid, event in versions.items():
                if vid in ready:
                    continue
                if all(p in ready for p in event["parents"]):
                    ready.add(vid)
                    changed = True
        referenced = {p for vid in ready for p in versions[vid]["parents"]}
        heads = sorted(ready - referenced)
        pending = sorted(
            vid for vid in versions
            if vid not in ready
            and all(p in versions for p in versions[vid]["parents"]))
        missing_parents = sorted(
            vid for vid in versions
            if vid not in ready
            and any(p not in versions or p not in ready
                    for p in versions[vid]["parents"]))
        entry = {"heads": heads, "pending": pending,
                 "unresolved": missing_parents}
        if not heads:
            entry["status"] = "absent"
            entry["state"] = None
            entry["members"] = []
        elif len(heads) == 1:
            state = _canon_state(versions[heads[0]])
            entry["state"] = state
            entry["status"] = "deleted" if state is None else "resolved"
            entry["members"] = []
        else:
            entry["status"] = "conflicted"
            entry["state"] = None
            entry["members"] = [
                {"version_id": h, "kind": versions[h]["kind"],
                 "state": _canon_state(versions[h]),
                 "tombstone": _canon_state(versions[h]) is None}
                for h in heads]
        result[row_id] = entry
    return result


def compare(interpreted, persisted):
    """Compare an interpretation with ``Engine.audit``'s persisted projection.

    ``persisted`` maps row_id -> {"status", "heads", "members"}.
    """
    problems = []
    for row_id in sorted(set(interpreted) | set(persisted)):
        want = interpreted.get(row_id)
        got = persisted.get(row_id)
        if want is None or got is None:
            problems.append({"row_id": row_id, "reason": "row only on one side",
                             "interpreted": want, "persisted": got})
            continue
        if want["status"] != got["status"]:
            problems.append({"row_id": row_id, "reason": "status differs",
                             "interpreted": want["status"],
                             "persisted": got["status"]})
        if want["heads"] != got["heads"]:
            problems.append({"row_id": row_id, "reason": "heads differ",
                             "interpreted": want["heads"],
                             "persisted": got["heads"]})
        if want["members"] != got["members"]:
            problems.append({"row_id": row_id, "reason": "members differ",
                             "interpreted": want["members"],
                             "persisted": got["members"]})
        if want.get("state") != got.get("state"):
            problems.append({"row_id": row_id, "reason": "state differs",
                             "interpreted": want.get("state"),
                             "persisted": got.get("state")})
    return problems

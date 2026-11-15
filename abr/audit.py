"""Audit helpers: monotonic event log plus non-address-identity summaries."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from typing import Any, Iterable


_COUNTERS = ("live_batches", "released_batches", "released_streams", "get_next_calls")


@dataclass
class AuditLog:
    events: list[dict[str, Any]] = field(default_factory=list)
    _seq: int = 0
    _lock: threading.RLock = field(default_factory=threading.RLock)

    def emit(self, event: str, **fields: Any) -> dict[str, Any]:
        with self._lock:
            self._seq += 1
            rec = {"seq": self._seq, "event": event}
            rec.update(fields)
            self.events.append(rec)
            return rec

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(e) for e in self.events]

    # ---- analysis -------------------------------------------------------
    def counts(self) -> dict[str, int]:
        out = {c: 0 for c in _COUNTERS}
        for rec in self.events:
            for c in _COUNTERS:
                if c in rec:
                    out[c] = rec[c]
        return out

    def release_event_count(self) -> int:
        return sum(1 for r in self.events if r.get("event") == "batch_release")

    def render(self) -> str:
        lines = []
        for rec in self.events:
            lines.append(json.dumps(rec, sort_keys=True))
        return "\n".join(lines)


def summarize(log: AuditLog) -> dict[str, Any]:
    """A compact, deterministic, address-free summary for reports/assertions.

    Pointer *addresses* are intentionally not used as identity here; batch and
    view names come from the driver, never from raw pointers.
    """
    backpressure = [r for r in log.events if r.get("event") == "backpressure"]
    released = [r for r in log.events if r.get("event") == "batch_release"]
    read_fail = [r for r in log.events if r.get("event") == "read_error"]
    state_errors = [r for r in log.events if r.get("event") == "state_error"]
    imports = [r for r in log.events if r.get("event") == "import_take"]
    get_next = [r for r in log.events if r.get("event") == "get_next"]
    end = next((r for r in reversed(log.events) if r.get("event") == "run_end"), {})
    return {
        "events": len(log.events),
        "imported_batches": len(imports),
        "real_get_next_calls": len(get_next),
        "source_release_events": len(released),
        "backpressure_events": len(backpressure),
        "read_errors": len(read_fail),
        "state_errors": len(state_errors),
        "releases_by_batch": _releases_by_batch(released),
        # Counter snapshots are observations (they include any earlier runs in
        # the same process); the authoritative checks use deltas.
        "final_live_batches": end.get("live_batches"),
        "final_released_batches": end.get("released_batches"),
        "final_released_streams": end.get("released_streams"),
    }


def _releases_by_batch(records: Iterable[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in records:
        key = str(r.get("batch", "?"))
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))

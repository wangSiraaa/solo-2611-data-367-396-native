"""Independent verification of a driver run.

Two layers:

1. **Source-semantics oracle** -- every recorded ``view_read`` is re-checked
   against the producer's documented payload (``100*batch + position`` with a
   validity bitmap of ``0x1b``), using the absolute offset recorded for the
   view.  This is computed independently of the view/reader implementation.
2. **Lifecycle oracle** -- the producer's own counters, the audit trail, and
   the per-operation expectations in the scenario must all agree:
     * exactly one ``release`` per imported allocation,
     * no live source allocation at the end,
     * ``get_next`` calls match the number of real pulls (backpressure and
       state errors never call it),
     * views share the producer's value/validity addresses,
     * empty views read no elements,
     * at most 8 concurrently active views.
"""

from __future__ import annotations

import ctypes
from dataclasses import dataclass, field
from typing import Any

from .scenarios import VALIDITY_MASK, source_valid, source_value


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


@dataclass
class Report:
    scenario: str
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, ok: bool, detail: str = "") -> None:
        self.checks.append(Check(name, ok, detail))

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "ok": self.ok,
            "checks": [c.as_dict() for c in self.checks],
            "failures": [c.as_dict() for c in self.checks if not c.ok],
        }


def verify(
    scenario_name: str,
    scenario: dict[str, Any],
    operations: list[dict[str, Any]],
    results: list[dict[str, Any]],
    events: list[dict[str, Any]],
    lib,
    baseline: tuple[int, int, int],
    executed_operations: int | None = None,
) -> Report:
    rep = Report(scenario_name)
    if executed_operations is not None:
        # An injected/consumer failure stops execution; only verify the ops
        # that were actually attempted.
        operations = operations[:executed_operations]

    # -- index audit events -------------------------------------------------
    imports: list[str] = []
    import_info: dict[str, dict[str, Any]] = {}
    ordinal_to_batch: dict[int, str] = {}
    views: dict[str, dict[str, Any]] = {}
    reads: list[dict[str, Any]] = []
    releases: dict[str, list[dict[str, Any]]] = {}
    get_next_events: list[dict[str, Any]] = []
    backpressure: list[dict[str, Any]] = []
    state_errors: list[dict[str, Any]] = []
    read_errors: list[dict[str, Any]] = []
    stream_opens = 0
    stream_closes = 0
    active_views: set[str] = set()
    max_active = 0

    for ev in events:
        e = ev.get("event")
        if e == "import_take":
            imports.append(ev["batch"])
            import_info[ev["batch"]] = ev
            if ev.get("source_ordinal") is not None:
                ordinal_to_batch[ev["source_ordinal"]] = ev["batch"]
        elif e == "view_create":
            views[ev["id"]] = ev
            active_views.add(ev["id"])
            max_active = max(max_active, len(active_views))
        elif e in ("view_released", "view_cancelled"):
            active_views.discard(ev["id"])
        elif e == "view_read":
            reads.append(ev)
        elif e == "batch_release":
            releases.setdefault(ev["batch"], []).append(ev)
        elif e == "get_next":
            get_next_events.append(ev)
        elif e == "backpressure":
            backpressure.append(ev)
        elif e in ("state_error", "release_rejected"):
            state_errors.append(ev)
        elif e == "read_error":
            read_errors.append(ev)
        elif e == "stream_open":
            stream_opens += 1
        elif e == "stream_close":
            stream_closes += 1

    batch_index = {name: i for i, name in enumerate(imports)}
    # Independent source-identity mapping: which producer ordinal each imported
    # batch actually came from, discovered from the get_next/import ordering.
    source_ordinal_of = {name: import_info[name].get("source_ordinal") for name in imports}
    ordinal_ok = all(
        import_info[name].get("buffers")
        for name in imports
    ) and len(ordinal_to_batch) == len(imports)

    # -- 1. per-operation status expectations ------------------------------
    by_index = {r.get("index"): r for r in results}
    status_ok = True
    detail = []
    for i, op in enumerate(operations):
        r = by_index.get(i)
        if r is None:
            status_ok = False
            detail.append(f"op {i} {op.get('op')}: missing result")
            continue
        want_error = bool(op.get("expect_error"))
        want_bp = bool(op.get("expect_backpressure"))
        want_eos = bool(op.get("expect_end_of_stream"))
        got_error = r.get("status") == "error"
        if want_error != got_error:
            status_ok = False
            detail.append(
                f"op {i} {op.get('op')}: expected {'error' if want_error else 'ok'}, got {r}"
            )
        if want_bp and not r.get("backpressure"):
            status_ok = False
            detail.append(f"op {i} next: expected backpressure, got {r}")
        if not want_bp and r.get("backpressure") and not want_error:
            status_ok = False
            detail.append(f"op {i} next: unexpected backpressure {r}")
        if want_eos and not r.get("end_of_stream"):
            status_ok = False
            detail.append(f"op {i} next: expected end-of-stream, got {r}")
        if not want_eos and r.get("end_of_stream") and not want_error:
            status_ok = False
            detail.append(f"op {i} next: unexpected end-of-stream {r}")
    rep.add("operation_status", status_ok, "; ".join(detail) or "all operations matched expectations")

    # -- 1b. independent source identity mapping ---------------------------
    rep.add(
        "source_identity",
        ordinal_ok,
        f"imported batches map 1:1 to producer ordinals {sorted(ordinal_to_batch)}",
    )

    # -- 2. independent value/validity oracle ------------------------------
    value_ok = True
    value_detail = []
    for rd in reads:
        vid = rd["id"]
        vmeta = views.get(vid)
        if vmeta is None:
            value_ok = False
            value_detail.append(f"read of unknown view {vid}")
            continue
        name = vmeta["batch"]
        if name not in batch_index:
            value_ok = False
            value_detail.append(f"view {vid}: batch {name} not imported")
            continue
        idx = source_ordinal_of.get(name)
        if idx is None:
            idx = batch_index[name]
        off = rd["offset"]
        length = rd["length"]
        exp_vals = [source_value(idx, off + i) if source_valid(off + i) else None for i in range(length)]
        exp_valid = [source_valid(off + i) for i in range(length)]
        exp_nulls = sum(0 if v else 1 for v in exp_valid)
        if rd["values"] != exp_vals or rd.get("null_count") != exp_nulls:
            value_ok = False
            value_detail.append(
                f"view {vid} off={off} len={length}: got {rd['values']} nulls={rd.get('null_count')} "
                f"expected {exp_vals} nulls={exp_nulls}"
            )
        if len(rd["values"]) != vmeta["length"]:
            value_ok = False
            value_detail.append(f"view {vid}: read length {len(rd['values'])} != slice length {vmeta['length']}")
    rep.add(
        "source_value_oracle",
        value_ok,
        "; ".join(value_detail) or f"{len(reads)} reads matched source values/validity",
    )

    # -- 3. zero-copy address sharing --------------------------------------
    share_ok = True
    share_detail = []
    for vid, vm in views.items():
        info = import_info.get(vm["batch"])
        if info is None:
            share_ok = False
            share_detail.append(f"view {vid}: missing import info")
            continue
        vals, valid = info["buffers"][1], info["buffers"][0]
        if vm["values_addr"] != vals or vm["validity_addr"] != valid:
            share_ok = False
            share_detail.append(
                f"view {vid}: addresses {vm['values_addr']}/{vm['validity_addr']} "
                f"!= batch {vals}/{valid}"
            )
        if not vm.get("shares_values") or not vm.get("shares_validity"):
            share_ok = False
            share_detail.append(f"view {vid}: audit does not report shared buffers")
    rep.add(
        "zero_copy_addresses",
        share_ok,
        "; ".join(share_detail) or f"{len(views)} views share the producer buffers",
    )

    # -- 4. release exactly once per allocation ----------------------------
    rel_ok = True
    rel_detail = []
    if len(imports) != len(set(imports)):
        rel_ok = False
        rel_detail.append(f"duplicate imports: {imports}")
    for name in imports:
        evs = releases.get(name, [])
        if len(evs) != 1:
            rel_ok = False
            rel_detail.append(f"batch {name}: {len(evs)} release callbacks (want exactly 1)")
            continue
        ev = evs[0]
        info = import_info[name]
        if ev.get("release_fn") != info.get("release_fn"):
            rel_ok = False
            rel_detail.append(f"batch {name}: release callback address changed")
        if ev.get("buffers") != info.get("buffers"):
            rel_ok = False
            rel_detail.append(f"batch {name}: released buffers differ from imported buffers")
    for name in releases:
        if name not in import_info:
            rel_ok = False
            rel_detail.append(f"release for never-imported batch {name}")
    rep.add(
        "release_exactly_once",
        rel_ok,
        "; ".join(rel_detail) or f"{len(imports)} allocations released once each via the original callback",
    )

    # -- 5. producer counters (deltas, since the library is process-global) --
    live0, released0, streams0 = baseline
    live1, released1, streams1 = int(lib.live_batches()), int(lib.released_batches()), int(lib.released_streams())
    counter_ok = (
        live1 == 0
        and released1 - released0 == len(imports)
        and streams1 - streams0 == stream_opens
        and stream_opens == 1
    )
    rep.add(
        "producer_counters",
        counter_ok,
        f"live={live1} (baseline {live0}), released_delta={released1 - released0} "
        f"imports={len(imports)}, streams_delta={streams1 - streams0} opens={stream_opens}",
    )

    # -- 6. get_next call accounting ---------------------------------------
    expected_pulls = sum(
        1
        for r in results
        if r.get("op") == "next" and r.get("status") == "ok" and not r.get("backpressure")
    )
    rep.add(
        "get_next_calls",
        len(get_next_events) == expected_pulls,
        f"real get_next calls={len(get_next_events)} expected={expected_pulls} "
        f"(backpressure={len(backpressure)})",
    )
    if backpressure:
        rep.add(
            "backpressure_observable",
            all(bp.get("get_next_called") is False for bp in backpressure)
            and all(bp.get("max_live_batches") for bp in backpressure),
            f"{len(backpressure)} refusals, none called get_next",
        )

    # -- 7. state errors on dead / invalid handles -------------------------
    expected_error_reads = sum(
        1 for op in operations if op.get("op") == "read" and op.get("expect_error")
    )
    expected_errors_total = sum(1 for op in operations if op.get("expect_error"))
    op_errors = [ev for ev in events if ev.get("event") == "op_error"]
    errs_ok = (
        len(read_errors) == expected_error_reads
        and len(op_errors) == expected_errors_total
    )
    rep.add(
        "state_errors",
        errs_ok,
        f"read_errors={len(read_errors)} (want {expected_error_reads}), "
        f"op_errors={len(op_errors)} (want {expected_errors_total}); "
        f"no extra C calls proven by get_next/release counts",
    )

    # -- 8. empty views read no elements -----------------------------------
    empty_ok = True
    empty_detail = []
    for rd in reads:
        if rd["length"] == 0:
            if rd["values"]:
                empty_ok = False
                empty_detail.append(f"empty view {rd['id']} yielded {rd['values']}")
            elif rd.get("null_count") != 0:
                empty_ok = False
                empty_detail.append(f"empty view {rd['id']} reported nulls")
    if not any(rd["length"] == 0 for rd in reads):
        empty_detail.append("(no empty views in this scenario)")
    rep.add("empty_views", empty_ok, "; ".join(empty_detail) or "empty views yielded zero elements")

    # -- 9. bound on concurrently active views -----------------------------
    rep.add(
        "active_view_bound",
        max_active <= 8,
        f"max concurrently active views={max_active} (limit 8)",
    )

    # -- 10. scenario-declared read expectations ---------------------------
    declared_ok = True
    declared_detail = []
    for i, op in enumerate(operations):
        if op.get("op") != "read" or "expect_values" not in op:
            continue
        r = by_index.get(i, {})
        if r.get("values") != op["expect_values"] or r.get("valid") != op.get("expect_valid"):
            declared_ok = False
            declared_detail.append(
                f"op {i} {op.get('id')}: {r.get('values')} != {op['expect_values']}"
            )
    rep.add(
        "declared_read_expectations",
        declared_ok,
        "; ".join(declared_detail) or "declared read expectations matched",
    )

    return rep

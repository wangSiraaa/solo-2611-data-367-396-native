"""Scenario *composer*: builds a deterministic run set that covers every
required behaviour in the task, plus the supplied scenario.json.

Each scenario records, for every ``read`` of a view, an independently computed
expectation derived from the producer's known deterministic payload:

    batch k, position p  ->  value = 100 * k + p
    validity bitmap for every batch is 0x1b  ->  position 2 of each batch is NULL

The expected sequence is computed from *source semantics* only (batch index and
absolute position), never by reading through the view implementation, so the
checks are independent of the importer.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
REFERENCE_SCENARIO = ROOT / "scenario.json"
VALIDITY_MASK = 0x1B  # source.c: b->valid = 0x1b for every batch


def source_value(batch_index: int, position: int) -> int:
    return 100 * batch_index + position


def source_valid(position: int) -> bool:
    return bool((VALIDITY_MASK >> position) & 1)


def expected_read(batch_index: int, absolute_offset: int, length: int) -> dict[str, Any]:
    values = []
    valid = []
    nulls = 0
    for i in range(length):
        pos = absolute_offset + i
        ok = source_valid(pos)
        valid.append(ok)
        values.append(source_value(batch_index, pos) if ok else None)
        nulls += 0 if ok else 1
    return {"values": values, "valid": valid, "null_count": nulls}


def _op(**kw):
    return kw


def _view_op(view_id, batch, start, length, parent=None, expect_error=None):
    d = _op(op="view", id=view_id, batch=batch, start=start, length=length)
    if parent:
        d["parent"] = parent
    if expect_error:
        d["expect_error"] = expect_error
    return d


def _read_op(view_id, batch_index, absolute_offset, length, expect_error=None):
    d = _op(op="read", id=view_id)
    if expect_error:
        d["expect_error"] = expect_error
        d["expect_value_count"] = 0
    else:
        exp = expected_read(batch_index, absolute_offset, length)
        d["expect_values"] = exp["values"]
        d["expect_valid"] = exp["valid"]
        d["expect_null_count"] = exp["null_count"]
    return d


def _next(batch, expect_backpressure=False, expect_error=False, expect_end_of_stream=False):
    d = _op(op="next", batch=batch)
    if expect_backpressure:
        d["expect_backpressure"] = True
    if expect_error:
        d["expect_error"] = True
    if expect_end_of_stream:
        d["expect_end_of_stream"] = True
    return d


def _expect_error_op(kind, **kw):
    d = _op(op=kind, **kw)
    d["expect_error"] = True
    return d


def scenarios() -> dict[str, dict[str, Any]]:
    sc: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 0. The scenario shipped with the task, exactly as given.
    # ------------------------------------------------------------------
    sc["given"] = {
        "description": "Verbatim scenario.json supplied with the exercise.",
        "max_live_exported_batches": 2,
        "operations": json.loads(REFERENCE_SCENARIO.read_text())["operations"],
    }

    # ------------------------------------------------------------------
    # 1. Sibling view closed first; root import dropped before either view.
    # ------------------------------------------------------------------
    sc["sibling_first"] = {
        "description": "Root import dropped first; one sibling released, the "
        "other keeps reading and outlives the stream close.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("vleft", "a", 0, 3),
            _view_op("vright", "a", 2, 3),
            _op(op="drop_import", batch="a"),
            _read_op("vleft", 0, 0, 3),          # [0,1,None]
            _op(op="release", id="vleft"),
            _read_op("vright", 0, 2, 3),         # [None,3,4] -> NULL at abs pos 2
            _op(op="close_stream"),
            _read_op("vright", 0, 2, 3),         # still valid after stream close
            _op(op="release", id="vright"),
        ],
    }

    # ------------------------------------------------------------------
    # 2. Root import dropped while a view is alive; last view ends -> release.
    # ------------------------------------------------------------------
    sc["root_first"] = {
        "description": "Root handle dropped before its views; batch is released "
        "exactly when the last dependent view ends.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("v", "a", 1, 4),
            _op(op="drop_import", batch="a"),
            _read_op("v", 0, 1, 4),              # [1,None,3,4]
            _op(op="release", id="v"),
            _next("b"),
            _view_op("vb", "b", 0, 5),
            _op(op="drop_import", batch="b"),
            _op(op="release", id="vb"),
        ],
    }

    # ------------------------------------------------------------------
    # 3. Stream closed first, then batches continue to be readable/releasable.
    # ------------------------------------------------------------------
    sc["stream_first"] = {
        "description": "Stream is closed right after the first import; the "
        "already-moved batch stays valid and can still be sliced and read.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _op(op="close_stream"),
            _view_op("v", "a", 0, 5),
            _read_op("v", 0, 0, 5),
            _op(op="drop_import", batch="a"),
            _op(op="release", id="v"),
            _expect_error_op("close_stream"),     # second close is a state error
            _expect_error_op("next", batch="b"),  # cannot pull from closed stream
        ],
    }

    # ------------------------------------------------------------------
    # 4. Nested slicing, each level offsetting -- addresses stay the same.
    # ------------------------------------------------------------------
    sc["nested"] = {
        "description": "Nested continuous slices accumulate offsets; values and "
        "validity addresses are shared all the way down.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("l0", "a", 0, 5),
            _view_op("l1", "l0", 1, 4, parent="l0"),
            _view_op("l2", "l1", 1, 2, parent="l1"),
            _read_op("l2", 0, 2, 2),             # [None,3]
            _read_op("l1", 0, 1, 4),             # [1,None,3,4]
            _read_op("l0", 0, 0, 5),
            _op(op="release", id="l0"),          # release parent first
            _read_op("l2", 0, 2, 2),             # children still valid
            _op(op="cancel", id="l1"),
            _read_op("l2", 0, 2, 2),
            _op(op="release", id="l2"),
            _op(op="drop_import", batch="a"),
        ],
    }

    # ------------------------------------------------------------------
    # 5. NULL bit positions under non-zero offsets, including all-NULL and
    #    empty slices.
    # ------------------------------------------------------------------
    sc["null_bits"] = {
        "description": "Validity is read at absolute bit offset; empty views "
        "read zero elements but still own a dependency.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("one", "a", 2, 1),           # exactly the NULL slot
            _read_op("one", 0, 2, 1),             # [None]
            _view_op("tail", "a", 3, 2),          # bits 3,4 -> valid
            _read_op("tail", 0, 3, 2),            # [3,4]
            _view_op("empty", "a", 5, 0),         # zero length at end
            _read_op("empty", 0, 5, 0),           # [] and 0 nulls
            _view_op("empty2", "a", 0, 0),
            _read_op("empty2", 0, 0, 0),
            _op(op="drop_import", batch="a"),
            _op(op="release", id="one"),
            _op(op="release", id="tail"),
            _op(op="release", id="empty"),
            _op(op="release", id="empty2"),
        ],
    }

    # ------------------------------------------------------------------
    # 6. Cancellation: cancelled views refuse reads/release; siblings unaffected.
    # ------------------------------------------------------------------
    sc["cancel"] = {
        "description": "Cancelling one view does not invalidate its sibling, and "
        "a cancelled view raises a clear state error on further use.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("c", "a", 0, 5),
            _view_op("s", "a", 0, 5),
            _op(op="cancel", id="c"),
            _expect_error_op("read", id="c"),
            _expect_error_op("release", id="c"),
            _expect_error_op("cancel", id="c"),
            _read_op("s", 0, 0, 5),               # sibling still fine
            _op(op="drop_import", batch="a"),
            _op(op="release", id="s"),
        ],
    }

    # ------------------------------------------------------------------
    # 7. Read on an already-released view is a state error, not a C call.
    # ------------------------------------------------------------------
    sc["released_read"] = {
        "description": "Operations on a released handle report a state error and "
        "never re-enter the producer callbacks.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("v", "a", 0, 5),
            _op(op="release", id="v"),
            _expect_error_op("read", id="v"),
            _expect_error_op("read", id="v"),
            _expect_error_op("release", id="v"),
            _op(op="drop_import", batch="a"),
            _expect_error_op("drop_import", batch="a"),
            _expect_error_op("release", id="v"),  # already released: error
        ],
    }

    # ------------------------------------------------------------------
    # 8. Backpressure: slot budget of 2, three live batches desired.
    # ------------------------------------------------------------------
    sc["backpressure"] = {
        "description": "Two live batches fill the bound; next is refused without "
        "calling get_next. Releasing a dependent frees a slot and execution "
        "continues without busy waiting or buffering the whole input.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("va", "a", 0, 5),
            _next("b"),
            _view_op("vb", "b", 0, 5),
            _next("c", expect_backpressure=True),  # 0 get_next calls
            _op(op="release", id="va"),            # still b in use -> still full
            _next("c", expect_backpressure=True),
            _op(op="drop_import", batch="a"),      # frees slot (a had no views)
            _next("c"),                            # now succeeds
            _view_op("vc", "c", 0, 5),
            _read_op("vc", 2, 0, 5),
            _op(op="release", id="vb"),
            _op(op="release", id="vc"),
            _op(op="drop_import", batch="b"),
            _op(op="drop_import", batch="c"),
        ],
    }

    # ------------------------------------------------------------------
    # 9. Bounds validation: out-of-range slices are rejected, no corruption.
    # ------------------------------------------------------------------
    sc["bounds"] = {
        "description": "Illegal slices (negative start, overrun, underflow via "
        "nested parent) are rejected without disturbing live handles.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("v", "a", 0, 5),
            _view_op("neg", "a", -1, 2, expect_error=True),
            _view_op("over", "a", 2, 4, expect_error=True),
            _view_op("over2", "a", 5, 1, expect_error=True),
            _view_op("zero_ok", "a", 5, 0),
            _view_op("parent_over", "v", 4, 2, parent="v", expect_error=True),
            _view_op("parent_neg", "v", -1, 1, parent="v", expect_error=True),
            _read_op("v", 0, 0, 5),
            _op(op="release", id="v"),
            _op(op="release", id="zero_ok"),
            _op(op="drop_import", batch="a"),
        ],
    }

    # ------------------------------------------------------------------
    # 9b. Import-handle lifecycle: no new dependents after the root is gone,
    #     but pre-existing views remain valid.
    # ------------------------------------------------------------------
    sc["import_handle_lifecycle"] = {
        "description": "A dropped import handle cannot spawn new views, while "
        "views created before the drop continue to read.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("keep", "a", 0, 5),
            _op(op="drop_import", batch="a"),
            _view_op("late", "a", 0, 1, expect_error=True),
            _read_op("keep", 0, 0, 5),
            _op(op="release", id="keep"),
        ],
    }

    # ------------------------------------------------------------------
    # 10. Interleaved downstreams with an explicit barrier: a "slow" reader
    #     reads after a stream close and a sibling release; a "fast" reader
    #     finishes and releases early. Uses three batches and the slot budget.
    # ------------------------------------------------------------------
    sc["barrier_multi"] = {
        "description": "Multiple downstream views interleave reads, cancellations "
        "and releases across an explicit barrier. A view keeps its batch slot "
        "after the root import is dropped; backpressure is observed until the "
        "last dependent ends.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("slow", "a", 1, 4),
            _view_op("fast", "a", 0, 3),
            _op(op="release", id="fast"),
            _op(op="drop_import", batch="a"),
            _next("b"),
            _view_op("mid", "b", 0, 5),
            _op(op="sleep", seconds=0),                # explicit barrier marker
            _read_op("slow", 0, 1, 4),                 # [1,None,3,4]
            _read_op("mid", 1, 0, 5),                  # [100,101,None,103,104]
            _next("c", expect_backpressure=True),      # a(held by slow) + b
            _op(op="cancel", id="mid"),
            _next("c", expect_backpressure=True),      # b root still held
            _op(op="drop_import", batch="b"),          # frees b
            _next("c"),                                # now succeeds
            _view_op("vc1", "c", 0, 2),
            _view_op("vc2", "c", 2, 3),
            _read_op("vc1", 2, 0, 2),                  # [200,201]
            _op(op="release", id="vc1"),
            _read_op("vc2", 2, 2, 3),                  # [None,203,204]
            _op(op="release", id="slow"),              # last dependent of a
            _op(op="release", id="vc2"),
            _op(op="drop_import", batch="c"),
            _op(op="close_stream"),
        ],
    }

    # ------------------------------------------------------------------
    # 11. Exception-safety: an operation fails (bad op), cleanup still releases
    #     every taken resource.
    # ------------------------------------------------------------------
    sc["exception_cleanup"] = {
        "description": "A view read raises mid-scenario while a batch and its "
        "views are still live; the driver's cleanup must still release every "
        "imported root exactly once and close the stream exactly once.",
        "max_live_exported_batches": 2,
        "fail_at_operation": 3,
        "expect_uncaught_exception": True,
        "operations": [
            _next("a"),
            _view_op("v", "a", 0, 5),
            _op(op="read", id="v"),
            _view_op("v2", "a", 1, 3),
            _op(op="drop_import", batch="a"),
            _read_op("v2", 0, 1, 3),
            _op(op="close_stream"),
        ],
    }

    # ------------------------------------------------------------------
    # 12. Zero-length / empty-view release semantics at batch level.
    # ------------------------------------------------------------------
    sc["empty_views"] = {
        "description": "Empty views never yield elements and can be released in "
        "any order without affecting siblings; batch frees at last handle end.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("e0", "a", 0, 0),
            _view_op("e5", "a", 5, 0),
            _view_op("full", "a", 0, 5),
            _read_op("e0", 0, 0, 0),
            _read_op("e5", 0, 5, 0),
            _op(op="drop_import", batch="a"),
            _op(op="release", id="e0"),
            _op(op="release", id="full"),
            _op(op="release", id="e5"),
        ],
    }

    # ------------------------------------------------------------------
    # 13. Many batches (up to 20) with staggered views, exercising repeated
    #     slot turnover. Only 3 real producer batches exist; pull all three.
    # ------------------------------------------------------------------
    sc["all_batches"] = {
        "description": "Pull all three producer batches, create several views on "
        "each, and release in a mixed order.",
        "max_live_exported_batches": 2,
        "operations": [
            _next("a"),
            _view_op("a0", "a", 0, 5),
            _view_op("a1", "a", 1, 4),
            _next("b"),
            _view_op("b0", "b", 0, 5),
            _view_op("b1", "b", 2, 3),
            _op(op="release", id="a0"),
            _op(op="drop_import", batch="a"),      # frees a; a1 still holds none? a1 is live
            _next("c", expect_backpressure=True),  # a1 keeps batch a alive
            _read_op("a1", 0, 1, 4),
            _read_op("b1", 1, 2, 3),
            _op(op="release", id="a1"),            # now a is fully released
            _next("c"),
            _read_op("b0", 1, 0, 5),
            _op(op="drop_import", batch="b"),
            _op(op="release", id="b0"),
            _op(op="release", id="b1"),
            _view_op("c0", "c", 0, 5),
            _read_op("c0", 2, 0, 5),
            _op(op="release", id="c0"),
            _op(op="drop_import", batch="c"),
            _next("d", expect_end_of_stream=True),  # producer EOS (release == NULL)
            _next("e", expect_error=True),          # further next is a state error
            _op(op="close_stream"),
        ],
    }

    return sc


def write_scenarios(out_path: Path) -> None:
    out_path.write_text(json.dumps(scenarios(), indent=2, sort_keys=False) + "\n")


if __name__ == "__main__":
    import sys

    target = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "scenarios" / "scenarios.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    write_scenarios(target)
    print(f"wrote {target}")

"""A small, bounded driver executing operations from a scenario description.

Only *real* source calls are used: every ``next`` invokes the producer's
``get_next`` through the Arrow C Stream vtable, and every release invokes the
producer's original ``ArrowArray.release`` callback exactly once per allocation.

Policy used by this driver
--------------------------
The driver owns the imported root ``ImportedBatch`` handles (an "import
handle") separately from the steady-state resource accounting.  Live roots are
tracked here so that the ``max_live_exported_batches`` limit is observable:

* A *slot* is occupied while the driver still holds the root import handle for a
  batch **or** any view of that batch is still live.  (Both keep the producer's
  allocation alive.)
* When the slot budget is full, ``next`` does **not** call ``get_next`` at all;
  it emits a ``backpressure`` event and returns a failure result.  We never busy
  loop and never buffer the whole input.
* Releasing a dependent (view release/cancel) or dropping a root frees the slot
  as soon as the last dependent goes away, so the next ``next`` can proceed.

The driver is deliberately single-threaded: scenarios express interleavings as
an operation order.  Views are independent handles, so interleaving read /
cancel / release across views is safe as long as callers respect handle state.
"""

from __future__ import annotations

import ctypes as C
from typing import Any, Optional

from .audit import AuditLog
from .batch import BatchReleasedError, ImportedBatch, OwnershipError
from .stream import ProducerStream, StreamStateError
from .view import CANCELLED, LIVE, RELEASED, View, ViewStateError


class DriverError(RuntimeError):
    pass


# Limits from the exercise contract.  These are validated, not merely assumed.
MAX_BATCHES = 20
MAX_BATCH_ITEMS = 1000
MAX_ACTIVE_VIEWS = 8


class Driver:
    def __init__(
        self,
        lib,
        audit: AuditLog,
        max_live_batches: int = 2,
        open_stream: bool = True,
        max_batches: int = MAX_BATCHES,
        max_batch_items: int = MAX_BATCH_ITEMS,
        max_active_views: int = MAX_ACTIVE_VIEWS,
    ) -> None:
        self.lib = lib
        self.audit = audit
        self.max_live_batches = int(max_live_batches)
        if self.max_live_batches <= 0:
            raise ValueError("max_live_batches must be positive")
        self.max_batches = int(max_batches)
        self.max_batch_items = int(max_batch_items)
        self.max_active_views = int(max_active_views)
        self.stream: Optional[ProducerStream] = None
        if open_stream:
            self.stream = ProducerStream(lib, audit)
        # batch name -> imported root
        self.roots: dict[str, ImportedBatch] = {}
        self.root_dropped: dict[str, bool] = {}
        # batch name -> number of live views
        self.live_views_by_batch: dict[str, int] = {}
        self.views: dict[str, View] = {}
        # Events deliberately emitted by scenarios (state errors etc.)
        self.failures: list[dict[str, Any]] = []
        # Partial results are stored as we go, so an injected failure still
        # leaves an accurate record of what was attempted.
        self.last_results: list[dict[str, Any]] = []
        self._no_more_batches = False
        self.audit.emit(
            "driver_start",
            max_live_batches=self.max_live_batches,
            live_batches=int(lib.live_batches()),
        )

    # ------------------------------------------------------------------
    # Resource accounting
    # ------------------------------------------------------------------
    def occupied_slots(self) -> list[str]:
        names: list[str] = []
        for name, root in self.roots.items():
            if not root.released or self.live_views_by_batch.get(name, 0) > 0:
                names.append(name)
        return names

    def live_batch_count(self) -> int:
        return len(self.occupied_slots())

    def active_view_count(self) -> int:
        return sum(1 for v in self.views.values() if v.state == LIVE)

    def _note_source_state(self, **extra: Any) -> None:
        fields = {
            "live_batches": int(self.lib.live_batches()),
            "released_batches": int(self.lib.released_batches()),
            "released_streams": int(self.lib.released_streams()),
        }
        fields.update(extra)
        self.audit.emit("source_state", **fields)

    def _slot_full(self) -> Optional[dict[str, Any]]:
        live = self.live_batch_count()
        if live < self.max_live_batches:
            return None
        rec = {
            "attempted": "next",
            "live_batches": int(self.lib.live_batches()),
            "occupied_slots": self.occupied_slots(),
            "max_live_batches": self.max_live_batches,
            "get_next_called": False,
        }
        self.audit.emit("backpressure", **rec)
        return rec

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------
    def next(self, name: str) -> dict[str, Any]:
        if name in self.roots:
            raise DriverError(f"batch name {name!r} already imported")
        if len(self.roots) >= self.max_batches:
            err = DriverError(
                f"cannot next: already imported {len(self.roots)} batches "
                f"(limit {self.max_batches})"
            )
            self.audit.emit("state_error", op="get_next", state="batch_limit", message=str(err))
            raise err
        if self.stream is None or self.stream.closed:
            msg = "cannot next: stream is closed"
            self.audit.emit("state_error", op="get_next", state="closed", message=msg)
            raise StreamStateError(msg)
        if self._no_more_batches:
            msg = "cannot next: source stream already reported end-of-stream"
            self.audit.emit("state_error", op="get_next", state="end-of-stream", message=msg)
            raise StreamStateError(msg)
        blocked = self._slot_full()
        if blocked is not None:
            return {"op": "next", "batch": name, "backpressure": True, **blocked}

        array = self.stream.next_batch()
        if array is None:
            self._no_more_batches = True
            self._note_source_state(op="next", batch=name, end_of_stream=True)
            return {"op": "next", "batch": name, "end_of_stream": True}

        if array.length > self.max_batch_items:
            # Reject before taking ownership: the exporter still owns release.
            err = OwnershipError(
                f"batch {name!r}: length {array.length} exceeds limit {self.max_batch_items}"
            )
            self.audit.emit("import_rejected", batch=name, reason=str(err))
            if bool(array.release):
                from .abi import ArrayReleaseFn

                self.audit.emit(
                    "release_on_rejected_import",
                    batch=name,
                    release_fn=hex(int(array.release)),
                )
                ArrayReleaseFn(array.release)(C.byref(array))
            raise err

        try:
            root = ImportedBatch.take(
                name,
                array,
                self.audit,
                on_release=self._on_batch_released,
                source_ordinal=self.stream.produced_batches - 1,
            )
        except Exception:
            # Take only mutates the export descriptor after all validation, so
            # a raised take() may still leave release owned by the exporter.
            if bool(array.release):
                from .abi import ArrayReleaseFn

                self.audit.emit(
                    "release_on_rejected_import",
                    batch=name,
                    release_fn=hex(int(array.release)),
                )
                ArrayReleaseFn(array.release)(C.byref(array))
            raise

        self.roots[name] = root
        self.root_dropped[name] = False
        self.live_views_by_batch.setdefault(name, 0)
        self._note_source_state(op="next", batch=name, imported=True)
        return {
            "op": "next",
            "batch": name,
            "imported": True,
            "batch_length": root.length,
            "batch_offset": root.offset,
            "slots_used": self.live_batch_count(),
        }

    def view(
        self,
        view_id: str,
        batch: str,
        start: int = 0,
        length: Optional[int] = None,
        parent: Optional[str] = None,
    ) -> dict[str, Any]:
        if view_id in self.views:
            raise DriverError(f"view id {view_id!r} already exists")
        if self.active_view_count() >= self.max_active_views:
            err = DriverError(
                f"cannot create view: {self.active_view_count()} active views "
                f"(limit {self.max_active_views})"
            )
            self.audit.emit("state_error", op="view", state="view_limit", message=str(err))
            raise err
        try:
            if parent is not None:
                parent_view = self._get_view(parent)
                if parent_view.state != LIVE:
                    raise ViewStateError(f"parent view {parent!r} is {parent_view.state}")
                view = View(
                    view_id,
                    parent_view.batch,
                    int(start),
                    int(length if length is not None else parent_view.length - int(start)),
                    self.audit,
                    parent=parent_view,
                    on_terminal=self._on_view_terminal,
                )
                batch_name = parent_view.batch.name
            else:
                root = self.roots.get(batch)
                if root is None:
                    raise DriverError(f"no imported batch named {batch!r}")
                if self.root_dropped.get(batch):
                    err = BatchReleasedError(
                        f"batch {batch!r}: import handle was dropped; cannot create new views"
                    )
                    self.audit.emit(
                        "state_error", op="view", batch=batch, message=str(err)
                    )
                    raise err
                view = View(
                    view_id,
                    root,
                    int(start),
                    int(length if length is not None else root.length - int(start)),
                    self.audit,
                    on_terminal=self._on_view_terminal,
                )
                batch_name = batch
        except Exception as exc:
            self.audit.emit(
                "view_rejected",
                id=view_id,
                batch=batch,
                parent=parent,
                start=start,
                length=length,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise
        self.views[view_id] = view
        self.live_views_by_batch[batch_name] = self.live_views_by_batch.get(batch_name, 0) + 1
        return {
            "op": "view",
            "id": view_id,
            "batch": batch_name,
            "offset": view.offset,
            "length": view.length,
            "depth": view.depth,
            "slots_used": self.live_batch_count(),
        }

    def _get_view(self, view_id: str) -> View:
        try:
            return self.views[view_id]
        except KeyError as exc:
            raise DriverError(f"unknown view id {view_id!r}") from exc

    def read(self, view_id: str) -> dict[str, Any]:
        view = self._get_view(view_id)
        try:
            res = view.read()
        except (ViewStateError, BatchReleasedError) as exc:
            self.failures.append({"op": "read", "id": view_id, "error": str(exc)})
            self.audit.emit("read_error", id=view_id, error=str(exc), error_type=type(exc).__name__)
            raise
        return {"op": "read", **res}

    def release_view(self, view_id: str, reason: str = "scenario release") -> dict[str, Any]:
        view = self._get_view(view_id)
        try:
            view.release(reason)
        except ViewStateError as exc:
            self.failures.append({"op": "release", "id": view_id, "error": str(exc)})
            raise
        return {"op": "release", "id": view_id, "state": view.state}

    def cancel_view(self, view_id: str, reason: str = "scenario cancel") -> dict[str, Any]:
        view = self._get_view(view_id)
        try:
            view.cancel(reason)
        except ViewStateError as exc:
            self.failures.append({"op": "cancel", "id": view_id, "error": str(exc)})
            raise
        return {"op": "cancel", "id": view_id, "state": view.state}

    def drop_import(self, batch: str) -> dict[str, Any]:
        root = self.roots.get(batch)
        if root is None:
            raise DriverError(f"no imported batch named {batch!r}")
        if self.root_dropped.get(batch):
            err = BatchReleasedError(f"batch {batch!r}: import handle already dropped")
            self.audit.emit("state_error", op="drop_import", batch=batch, message=str(err))
            raise err
        root.drop_root()
        self.root_dropped[batch] = True
        self._note_source_state(op="drop_import", batch=batch)
        return {
            "op": "drop_import",
            "batch": batch,
            "released": root.released,
            "slots_used": self.live_batch_count(),
        }

    def close_stream(self) -> dict[str, Any]:
        if self.stream is None:
            raise DriverError("no stream to close")
        self.stream.close()
        self._note_source_state(op="close_stream")
        return {"op": "close_stream", "closed": True}

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------
    def _on_view_terminal(self, view: View) -> None:
        name = view.batch.name
        remaining = max(0, self.live_views_by_batch.get(name, 0) - 1)
        self.live_views_by_batch[name] = remaining
        self._note_source_state(
            op="view_terminal",
            id=view.id,
            batch=name,
            state=view.state,
            remaining_views=remaining,
        )

    def _on_batch_released(self, root: ImportedBatch, reason: str) -> None:
        self._note_source_state(op="batch_released", batch=root.name, reason=reason)
        self.root_dropped[root.name] = True

    # ------------------------------------------------------------------
    # Scenario execution
    # ------------------------------------------------------------------
    def execute(
        self,
        operations: list[dict[str, Any]],
        fail_at: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        self.last_results = results
        for index, op in enumerate(operations):
            if fail_at is not None and index == fail_at:
                # Simulate an unexpected consumer/driver failure with resources
                # still held; drain_all() by the caller must clean them up.
                self.audit.emit(
                    "injected_failure",
                    index=index,
                    occupied_slots=self.occupied_slots(),
                    live_batches=int(self.lib.live_batches()),
                )
                raise RuntimeError(f"injected consumer failure at operation {index}")
            kind = op.get("op")
            self.audit.emit("op", index=index, op=kind, args=_public_args(op))
            try:
                if kind == "next":
                    res = self.next(op["batch"])
                elif kind == "view":
                    res = self.view(
                        op["id"],
                        op["batch"],
                        start=int(op.get("start", 0)),
                        length=op.get("length"),
                        parent=op.get("parent"),
                    )
                elif kind == "read":
                    res = self.read(op["id"])
                elif kind == "release":
                    res = self.release_view(op["id"], op.get("reason", "scenario release"))
                elif kind == "cancel":
                    res = self.cancel_view(op["id"], op.get("reason", "scenario cancel"))
                elif kind == "drop_import":
                    res = self.drop_import(op["batch"])
                elif kind == "close_stream":
                    res = self.close_stream()
                elif kind == "sleep":
                    # Accepted for barrier-style scenarios; the driver is
                    # single-threaded, so this is observably a no-op.
                    res = {"op": "sleep", "seconds": op.get("seconds", 0)}
                else:
                    raise DriverError(f"unknown op {kind!r}")
            except Exception as exc:  # noqa: BLE001 - scenarios may expect failures
                err = {
                    "op": kind,
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "index": index,
                }
                self.audit.emit(
                    "op_error",
                    index=index,
                    op=kind,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                results.append(err)
            else:
                res = {"status": "ok", **res, "index": index}
                results.append(res)
        self.audit.emit(
            "driver_finish",
            live_batches=int(self.lib.live_batches()),
            released_batches=int(self.lib.released_batches()),
            released_streams=int(self.lib.released_streams()),
            occupied_slots=self.occupied_slots(),
        )
        return results

    # ------------------------------------------------------------------
    def drain_all(self, reason: str = "driver cleanup") -> None:
        """Best-effort deterministic cleanup after an unexpected error."""
        self.audit.emit("cleanup_start", reason=reason, occupied_slots=self.occupied_slots())
        for view in list(self.views.values()):
            if view.state == LIVE:
                try:
                    view.cancel(reason=reason)
                except Exception as exc:  # noqa: BLE001
                    self.audit.emit("cleanup_view_error", id=view.id, error=repr(exc))
        for name, root in list(self.roots.items()):
            if not self.root_dropped.get(name):
                try:
                    root.drop_root()
                    self.root_dropped[name] = True
                except Exception as exc:  # noqa: BLE001
                    self.audit.emit("cleanup_root_error", batch=name, error=repr(exc))
        if self.stream is not None and not self.stream.closed:
            try:
                self.stream.close()
            except Exception as exc:  # noqa: BLE001
                self.audit.emit("cleanup_stream_error", error=repr(exc))
        self.audit.emit(
            "cleanup_done",
            live_batches=int(self.lib.live_batches()),
            released_batches=int(self.lib.released_batches()),
            released_streams=int(self.lib.released_streams()),
        )


def _public_args(op: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in op.items() if k != "op"}

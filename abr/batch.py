"""Ownership takeover of Arrow C Data column batches.

Design notes
------------
``ImportedBatch.take`` performs a *validated move*:

* The ``ArrowArray`` descriptor is copied into Python-owned storage, then the
  caller's exposed struct has ``release = NULL`` and ``private_data = NULL``
  cleared.  Per the Arrow C Data move convention the importer now owns the one
  outstanding ``release`` obligation; the exporter must not call it again.
* Nothing is copied out of ``buffers``.  Values and validity live at whatever
  addresses the producer allocated.
* The original release callback address is captured *before* the move so that
  each allocated buffer can be released exactly once through the real C
  callback when the last dependent handle goes away.
* A recursion guard (``releasing``) makes a double release a no-op with a hard
  state error rather than a second C call.
"""

from __future__ import annotations

import ctypes as C
import threading
from typing import Any, Optional

from .abi import ArrayReleaseFn, ArrowArray


class OwnershipError(RuntimeError):
    pass


class BatchReleasedError(OwnershipError):
    pass


def _describe_array(a: ArrowArray) -> dict[str, Any]:
    bufs = []
    for i in range(max(0, a.n_buffers)):
        bufs.append(a.buffers[i] if a.buffers else None)
    return {
        "length": a.length,
        "offset": a.offset,
        "null_count": a.null_count,
        "n_buffers": a.n_buffers,
        "n_children": a.n_children,
        "buffers": bufs,
        "release_fn": a.release,
    }


class ImportedBatch:
    """Single-owner view root over one producer-exported column batch."""

    __slots__ = (
        "name",
        "array",
        "length",
        "offset",
        "null_count",
        "validity_addr",
        "values_addr",
        "buffers",
        "release_fn",
        "private_data",
        "_dep_count",
        "_root_alive",
        "_released",
        "_releasing",
        "_lock",
        "audit",
        "on_release",
        "_release_cb",
    )

    def __init__(self) -> None:
        raise TypeError("use ImportedBatch.take()")

    @classmethod
    def take(
        cls,
        name: str,
        source_array: ArrowArray,
        audit,
        on_release=None,
        source_ordinal: int | None = None,
    ) -> "ImportedBatch":
        if not bool(source_array.release):
            raise OwnershipError(
                f"batch {name!r}: exported ArrowArray has no release callback "
                "(already moved or never valid)"
            )
        if source_array.n_buffers < 2 or not source_array.buffers:
            raise OwnershipError(f"batch {name!r}: expected 2 buffers (validity, values)")
        if source_array.n_buffers != 2:
            raise OwnershipError(f"batch {name!r}: unsupported buffer count {source_array.n_buffers}")
        if source_array.n_children != 0:
            raise OwnershipError(f"batch {name!r}: nested children are out of scope")
        if source_array.length < 0 or source_array.offset < 0:
            raise OwnershipError(f"batch {name!r}: negative length/offset")
        if source_array.length > (1 << 62) or source_array.offset > (1 << 62):
            raise OwnershipError(f"batch {name!r}: implausible length/offset")
        if source_array.offset + source_array.length > (1 << 62):
            raise OwnershipError(f"batch {name!r}: offset+length overflow")

        snapshot = _describe_array(source_array)
        release_fn = int(source_array.release)
        private_data = source_array.private_data

        # Copy the descriptor (metadata only) and *move* ownership by clearing
        # the exporter's struct.  The C release callback must not be reachable
        # through the original export structure afterwards.
        owned = ArrowArray()
        C.memmove(C.addressof(owned), C.addressof(source_array), C.sizeof(ArrowArray))
        source_array.release = None
        source_array.private_data = None
        source_array.buffers = C.POINTER(C.c_void_p)()
        source_array.length = 0
        source_array.offset = 0
        source_array.null_count = 0
        source_array.n_buffers = 0

        self = object.__new__(cls)
        self.name = name
        self.array = owned
        self.length = int(snapshot["length"])
        self.offset = int(snapshot["offset"])
        self.null_count = int(snapshot["null_count"])
        self.buffers = list(snapshot["buffers"])
        self.validity_addr = int(snapshot["buffers"][0]) if snapshot["buffers"][0] else 0
        self.values_addr = int(snapshot["buffers"][1]) if snapshot["buffers"][1] else 0
        self.release_fn = release_fn
        self.private_data = private_data
        self._dep_count = 0
        self._root_alive = True
        self._released = False
        self._releasing = False
        self._lock = threading.RLock()
        self.audit = audit
        self.on_release = on_release
        self._release_cb = ArrayReleaseFn(release_fn)

        audit.emit(
            "import_take",
            batch=name,
            length=self.length,
            offset=self.offset,
            null_count=self.null_count,
            buffers=[hex(b) if b else None for b in self.buffers],
            release_fn=hex(release_fn),
            private_data=hex(private_data) if private_data else None,
            original_release_cleared=not bool(source_array.release),
            source_ordinal=source_ordinal,
        )
        return self

    # -- dependency counting ---------------------------------------------
    def add_dependency(self) -> None:
        with self._lock:
            if self._released or self._releasing:
                raise BatchReleasedError(f"batch {self.name!r}: cannot add dependency after release")
            self._dep_count += 1
            self.audit.emit("dep_add", batch=self.name, deps=self._dep_count)

    def drop_dependency(self, reason: str = "") -> None:
        with self._lock:
            if self._dep_count <= 0:
                raise OwnershipError(f"batch {self.name!r}: dependency count underflow ({reason})")
            self._dep_count -= 1
            deps = self._dep_count
            self.audit.emit("dep_drop", batch=self.name, deps=deps, reason=reason)
            if deps == 0 and self._root_alive:
                self.audit.emit(
                    "release_deferred",
                    batch=self.name,
                    reason="root still held",
                )
                return
            if deps == 0 and not self._root_alive:
                self._release_now(reason=f"last dependent dropped ({reason})" if reason else "last dependent dropped")

    def drop_root(self) -> None:
        with self._lock:
            if not self._root_alive:
                raise BatchReleasedError(f"batch {self.name!r}: root import already dropped")
            self._root_alive = False
            self.audit.emit("root_drop", batch=self.name, deps=self._dep_count)
            if self._dep_count == 0:
                self._release_now(reason="root dropped, no dependents")

    def _release_now(self, reason: str) -> None:
        # Called with self._lock held.
        if self._released or self._releasing:
            self.audit.emit(
                "release_rejected",
                batch=self.name,
                reason=reason,
                state="already released",
            )
            raise BatchReleasedError(f"batch {self.name!r}: release already performed")
        self._releasing = True
        try:
            self.audit.emit(
                "batch_release",
                batch=self.name,
                release_fn=hex(self.release_fn),
                buffers=[hex(b) if b else None for b in self.buffers],
                reason=reason,
            )
            self._release_cb(C.byref(self.array))
            if bool(self.array.release):
                self.audit.emit(
                    "post_release_still_owned",
                    batch=self.name,
                    release_fn=hex(int(self.array.release)),
                )
            self._released = True
        finally:
            self._releasing = False
            if self.on_release is not None:
                try:
                    self.on_release(self, reason)
                except Exception as exc:  # noqa: BLE001 - auditing must not break release
                    self.audit.emit("on_release_error", batch=self.name, error=repr(exc))

    # -- state -----------------------------------------------------------
    @property
    def released(self) -> bool:
        return self._released

    @property
    def deps(self) -> int:
        with self._lock:
            return self._dep_count

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "name": self.name,
                "length": self.length,
                "offset": self.offset,
                "null_count": self.null_count,
                "values_addr": hex(self.values_addr),
                "validity_addr": hex(self.validity_addr),
                "deps": self._dep_count,
                "root_alive": self._root_alive,
                "released": self._released,
            }

    def assert_live(self) -> None:
        if self._released:
            raise BatchReleasedError(f"batch {self.name!r}: underlying source batch already released")

    def __repr__(self) -> str:
        return f"<ImportedBatch {self.name} len={self.length} deps={self._dep_count} released={self._released}>"

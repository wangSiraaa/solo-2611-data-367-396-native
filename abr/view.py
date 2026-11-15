"""Zero-copy continuous slices (views) over an imported batch.

A view is *only* ``(values_addr, validity_addr, offset, length)`` plus a
dependency on the root ``ImportedBatch``.  No element is copied when a view is
created, and nested slicing accumulates offsets instead of materialising
parents.

NULLs are interpreted by the *absolute* bit position ``offset + i`` inside the
original validity bitmap, exactly as Arrow C Data specifies.
"""

from __future__ import annotations

import threading
from typing import Any, Optional

from .batch import BatchReleasedError, ImportedBatch
from .abi import load_bit, load_i64


class ViewStateError(RuntimeError):
    """Operation attempted on a released/cancelled view."""


LIVE = "live"
RELEASED = "released"
CANCELLED = "cancelled"


class View:
    __slots__ = (
        "id",
        "batch",
        "parent_id",
        "values_addr",
        "validity_addr",
        "offset",
        "length",
        "depth",
        "state",
        "_dep_held",
        "_reads",
        "_lock",
        "audit",
        "on_terminal",
    )

    def __init__(
        self,
        view_id: str,
        batch: ImportedBatch,
        start: int,
        length: int,
        audit,
        parent: Optional["View"] = None,
        on_terminal=None,
    ) -> None:
        if parent is not None:
            base_offset = parent.offset + start
            parent_len = parent.length
            parent_id = parent.id
            depth = parent.depth + 1
        else:
            base_offset = batch.offset + start
            parent_len = batch.length
            parent_id = None
            depth = 0

        if start < 0 or length < 0:
            raise ValueError(f"view {view_id!r}: negative start/length ({start}, {length})")
        if start + length > parent_len:
            raise ValueError(
                f"view {view_id!r}: slice [{start}, {start + length}) out of bounds "
                f"for parent length {parent_len}"
            )
        if base_offset < batch.offset:
            raise ValueError(f"view {view_id!r}: offset underflow")

        batch.assert_live()
        batch.add_dependency()

        self.id = view_id
        self.batch = batch
        self.parent_id = parent_id
        self.values_addr = batch.values_addr
        self.validity_addr = batch.validity_addr
        self.offset = base_offset
        self.length = length
        self.depth = depth
        self.state = LIVE
        self._dep_held = True
        self._reads = 0
        self._lock = threading.RLock()
        self.audit = audit
        self.on_terminal = on_terminal

        audit.emit(
            "view_create",
            id=view_id,
            batch=batch.name,
            parent=parent_id,
            offset=self.offset,
            length=self.length,
            depth=depth,
            values_addr=hex(self.values_addr),
            validity_addr=hex(self.validity_addr),
            shares_values=self.values_addr == batch.values_addr,
            shares_validity=self.validity_addr == batch.validity_addr,
        )

    # -- state -----------------------------------------------------------
    def _assert_live(self, op: str) -> None:
        if self.state != LIVE:
            err = ViewStateError(
                f"view {self.id!r}: cannot {op} while {self.state}"
            )
            self.audit.emit(
                "state_error",
                id=self.id,
                op=op,
                state=self.state,
                message=str(err),
            )
            raise err

    def read(self) -> dict[str, Any]:
        with self._lock:
            self._assert_live("read")
            self.batch.assert_live()
            values: list[Optional[int]] = []
            valid: list[bool] = []
            nulls = 0
            for i in range(self.length):
                bit = self.offset + i
                ok = True if not self.validity_addr else bool(load_bit(self.validity_addr, bit))
                if ok:
                    values.append(load_i64(self.values_addr, bit * 8))
                    valid.append(True)
                else:
                    values.append(None)
                    valid.append(False)
                    nulls += 1
            self._reads += 1
            result = {
                "id": self.id,
                "batch": self.batch.name,
                "offset": self.offset,
                "length": self.length,
                "values": values,
                "valid": valid,
                "null_count": nulls,
                "values_addr": hex(self.values_addr),
                "validity_addr": hex(self.validity_addr),
                "reads": self._reads,
            }
            self.audit.emit(
                "view_read",
                id=self.id,
                batch=self.batch.name,
                offset=self.offset,
                length=self.length,
                values=values,
                null_count=nulls,
                values_addr=hex(self.values_addr),
            )
            return result

    def release(self, reason: str = "view release") -> None:
        with self._lock:
            self._assert_live("release")
            self._terminate(RELEASED, reason)

    def cancel(self, reason: str = "view cancel") -> None:
        with self._lock:
            self._assert_live("cancel")
            self._terminate(CANCELLED, reason)

    def _terminate(self, new_state: str, reason: str) -> None:
        self.state = new_state
        self.audit.emit(
            "view_" + new_state,
            id=self.id,
            batch=self.batch.name,
            offset=self.offset,
            length=self.length,
            reason=reason,
            reads=self._reads,
        )
        if self._dep_held:
            self._dep_held = False
            self.batch.drop_dependency(reason=f"view {self.id} {new_state}")
        if self.on_terminal is not None:
            self.on_terminal(self)

    def status(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "batch": self.batch.name,
            "state": self.state,
            "offset": self.offset,
            "length": self.length,
            "depth": self.depth,
            "values_addr": hex(self.values_addr),
            "validity_addr": hex(self.validity_addr),
            "reads": self._reads,
        }

    def __repr__(self) -> str:
        return f"<View {self.id} batch={self.batch.name} off={self.offset} len={self.length} state={self.state}>"

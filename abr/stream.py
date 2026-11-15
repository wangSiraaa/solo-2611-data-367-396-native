"""Owner for one ArrowArrayStream exported by the producer.

Closing the stream releases only the *stream* allocation.  It never releases
column batches that were already handed over: per Arrow C Data those were moved
into the importer when ``get_next`` returned them.
"""

from __future__ import annotations

import ctypes as C
from typing import Optional

from .abi import (
    ArrowArray,
    ArrowArrayStream,
    ArrowSchema,
    GetLastErrorFn,
    GetNextFn,
    GetSchemaFn,
    SchemaReleaseFn,
    StreamReleaseFn,
)


class StreamStateError(RuntimeError):
    pass


class ProducerStream:
    def __init__(self, lib, audit) -> None:
        self.lib = lib
        self.audit = audit
        self.stream = ArrowArrayStream()
        lib.make_stream(C.byref(self.stream))
        if not self.stream.release or not self.stream.get_next:
            raise StreamStateError("producer returned an incomplete stream")
        self._ptr = C.pointer(self.stream)
        self._get_next = GetNextFn(self.stream.get_next)
        self._get_schema = GetSchemaFn(self.stream.get_schema)
        self._get_error = GetLastErrorFn(self.stream.get_last_error)
        self._stream_release = StreamReleaseFn(self.stream.release)
        self._closed = False
        self.get_next_calls = 0
        self.produced_batches = 0
        self.last_ordinal: int | None = None
        self.audit.emit(
            "stream_open",
            stream_addr=hex(C.addressof(self.stream)),
            get_next_fn=hex(int(self.stream.get_next)),
            release_fn=hex(int(self.stream.release)),
        )

    # ------------------------------------------------------------------
    def schema(self) -> dict:
        if self._closed:
            self.audit.emit("state_error", op="schema", state="closed")
            raise StreamStateError("stream already closed")
        schema = ArrowSchema()
        rc = self._get_schema(self._ptr, C.byref(schema))
        if rc != 0:
            raise StreamStateError(f"get_schema failed rc={rc}")
        info = {
            "format": schema.format.decode() if schema.format else None,
            "n_children": schema.n_children,
            "release_fn": hex(int(schema.release)) if schema.release else None,
        }
        if schema.release:
            SchemaReleaseFn(schema.release)(C.byref(schema))
        self.audit.emit("schema", **info)
        return info

    def next_batch(self) -> Optional[ArrowArray]:
        """Call the real get_next.  Returns a filled ArrowArray or None at EOS."""
        if self._closed:
            self.audit.emit("state_error", op="get_next", state="closed")
            raise StreamStateError("stream already closed")
        self.get_next_calls += 1
        out = ArrowArray()
        rc = self._get_next(self._ptr, C.byref(out))
        live = int(self.lib.live_batches())
        if rc != 0:
            err = self._get_error(self._ptr)
            msg = err.decode() if err else f"rc={rc}"
            self.audit.emit(
                "get_next",
                call=self.get_next_calls,
                rc=rc,
                error=msg,
                live_batches=live,
            )
            raise StreamStateError(f"get_next failed: {msg}")
        if not out.release:
            self.audit.emit(
                "get_next",
                call=self.get_next_calls,
                rc=rc,
                end_of_stream=True,
                live_batches=live,
            )
            return None
        ordinal = self.produced_batches
        self.produced_batches += 1
        self.audit.emit(
            "get_next",
            call=self.get_next_calls,
            rc=rc,
            ordinal=ordinal,
            length=out.length,
            offset=out.offset,
            null_count=out.null_count,
            n_buffers=out.n_buffers,
            live_batches=live,
            release_fn=hex(int(out.release)),
        )
        return out

    # ------------------------------------------------------------------
    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            self.audit.emit("state_error", op="close_stream", state="closed")
            raise StreamStateError("stream already closed")
        self._closed = True
        self.audit.emit(
            "stream_close",
            stream_addr=hex(C.addressof(self.stream)),
            release_fn=hex(int(self.stream.release)),
            live_batches=int(self.lib.live_batches()),
        )
        try:
            self._stream_release(self._ptr)
        finally:
            if self.stream.release:
                self.audit.emit(
                    "stream_release_left_owned",
                    release_fn=hex(int(self.stream.release)),
                )

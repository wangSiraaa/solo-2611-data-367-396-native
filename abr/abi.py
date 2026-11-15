"""ctypes binding for the real Arrow C Data / C Stream ABI used by source.c.

The struct layouts mirror arrow_c.h exactly.  Callbacks are exposed both as raw
function addresses (for address checks / auditing) and as typed CFUNCTYPE
callables (for actually invoking the producer).
"""

from __future__ import annotations

import ctypes as C
from typing import Optional


class ArrowSchema(C.Structure):
    pass


class ArrowArray(C.Structure):
    pass


class ArrowArrayStream(C.Structure):
    pass


ArrowSchema._fields_ = [
    ("format", C.c_char_p),
    ("name", C.c_char_p),
    ("metadata", C.c_char_p),
    ("flags", C.c_int64),
    ("n_children", C.c_int64),
    ("children", C.POINTER(C.POINTER(ArrowSchema))),
    ("dictionary", C.POINTER(ArrowSchema)),
    ("release", C.c_void_p),
    ("private_data", C.c_void_p),
]

ArrowArray._fields_ = [
    ("length", C.c_int64),
    ("null_count", C.c_int64),
    ("offset", C.c_int64),
    ("n_buffers", C.c_int64),
    ("n_children", C.c_int64),
    ("buffers", C.POINTER(C.c_void_p)),
    ("children", C.POINTER(C.POINTER(ArrowArray))),
    ("dictionary", C.POINTER(ArrowArray)),
    ("release", C.c_void_p),
    ("private_data", C.c_void_p),
]

ArrowArrayStream._fields_ = [
    ("get_schema", C.c_void_p),
    ("get_next", C.c_void_p),
    ("get_last_error", C.c_void_p),
    ("release", C.c_void_p),
    ("private_data", C.c_void_p),
]

# ABI sanity: these are the C sizes we rely on (64-bit pointers).
assert C.sizeof(ArrowSchema) == 72, C.sizeof(ArrowSchema)
assert C.sizeof(ArrowArray) == 80, C.sizeof(ArrowArray)
assert C.sizeof(ArrowArrayStream) == 40, C.sizeof(ArrowArrayStream)

# Callback prototypes (Arrow C ABI).
ArrayReleaseFn = C.CFUNCTYPE(None, C.POINTER(ArrowArray))
SchemaReleaseFn = C.CFUNCTYPE(None, C.POINTER(ArrowSchema))
GetSchemaFn = C.CFUNCTYPE(C.c_int, C.POINTER(ArrowArrayStream), C.POINTER(ArrowSchema))
GetNextFn = C.CFUNCTYPE(C.c_int, C.POINTER(ArrowArrayStream), C.POINTER(ArrowArray))
GetLastErrorFn = C.CFUNCTYPE(C.c_char_p, C.POINTER(ArrowArrayStream))
StreamReleaseFn = C.CFUNCTYPE(None, C.POINTER(ArrowArrayStream))

_INT64 = C.c_int64
_UINT8 = C.c_uint8


def addr_of_struct(struct: C.Structure) -> int:
    return C.addressof(struct)


def checked_add(ptr: Optional[int], byte_offset: int) -> Optional[int]:
    """Return ptr + byte_offset, or None when a null base is given.

    The producer's allocations are small and offset arithmetic is bounded by
    the validated batch length, so a plain addition with a negativity guard is
    sufficient here.
    """
    if ptr is None:
        return None
    total = ptr + byte_offset
    if total < ptr or total < 0:
        raise OverflowError("pointer arithmetic overflow")
    return total


def load_i64(base: Optional[int], byte_offset: int) -> int:
    if base is None:
        raise ValueError("null data buffer")
    return _INT64.from_address(base + byte_offset).value


def load_bit(base: Optional[int], bit_index: int) -> int:
    if base is None:
        raise ValueError("null validity buffer")
    return (_UINT8.from_address(base + (bit_index >> 3)).value >> (bit_index & 7)) & 1

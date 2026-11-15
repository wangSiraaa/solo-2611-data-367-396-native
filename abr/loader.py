"""Load (and if necessary build) the real producer supplied with the exercise."""

from __future__ import annotations

import ctypes as C
import os
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SOURCE_C = ROOT / "source.c"
INCLUDE_H = ROOT / "arrow_c.h"


def _lib_name() -> str:
    return "libsource.so" if sys.platform.startswith("linux") else "libsource.dylib"


def build_library(out_dir: Path | None = None) -> Path:
    out_dir = Path(out_dir or ROOT / "build")
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / _lib_name()
    # Prefer explicit CC, then gcc/clang, and only then generic `cc`: some
    # environments wrap `cc` in a policy shim that rejects ordinary compiler
    # flags, while gcc/clang are the real compilers.
    candidates = []
    if os.environ.get("CC"):
        candidates.append(os.environ["CC"])
    candidates += ["gcc", "clang", "cc"]
    cc = next((shutil.which(c) for c in candidates if shutil.which(c)), None)
    if cc is None:
        raise RuntimeError("no C compiler (gcc/clang/cc) found; cannot build libsource")
    cmd = [cc, "-std=c11", "-shared", "-fPIC", "-O2", str(SOURCE_C), "-I", str(ROOT), "-o", str(out)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"failed to build producer:\n$ {' '.join(cmd)}\n{proc.stderr}")
    return out


def find_library(explicit: str | None = None) -> Path:
    if explicit:
        p = Path(explicit).resolve()
        if not p.is_file():
            raise FileNotFoundError(p)
        return p
    for candidate in (ROOT / _lib_name(), ROOT / "build" / _lib_name()):
        if candidate.is_file():
            return candidate
    return build_library()


def load_library(explicit: str | None = None) -> C.CDLL:
    path = find_library(explicit)
    lib = C.CDLL(str(path))
    lib.make_stream.argtypes = [C.c_void_p]
    lib.make_stream.restype = None
    lib.live_batches.argtypes = []
    lib.live_batches.restype = C.c_int
    lib.released_batches.argtypes = []
    lib.released_batches.restype = C.c_int
    lib.released_streams.argtypes = []
    lib.released_streams.restype = C.c_int
    lib._resolved_path = str(path)  # type: ignore[attr-defined]
    return lib

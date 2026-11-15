"""通用辅助：原子写文件、确定性指纹。"""

import hashlib
import json
import os
import tempfile
from pathlib import Path


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_json(obj) -> bytes:
    """排序键、无多余空白的确定性 JSON 字节。"""
    return json.dumps(
        obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def source_snapshot_fingerprint(conn) -> str:
    """对源库逻辑内容（schema SQL + 全部表行）取确定性哈希。

    同一发布决定内先取此指纹再做任何写操作，保证发布绑定固定快照。
    """
    h = hashlib.sha256()
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    for name, sql in rows:
        h.update(canonical_json([name, sql]))
        cols = [
            r["name"]
            for r in conn.execute(f'PRAGMA table_info("{name}")')
        ]
        for row in conn.execute(
            f'SELECT * FROM "{name}" ORDER BY '
            + ",".join(f'"{c}"' for c in cols)
        ):
            h.update(canonical_json(list(row)))
    return h.hexdigest()


def atomic_write(path, data: bytes | str, mode: int = 0o644):
    """同目录临时文件 + fsync + rename，读取方只会看到完整文件或旧文件。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    is_bytes = isinstance(data, bytes)
    fd, tmp = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "wb" if is_bytes else "w",
                       encoding=None if is_bytes else "utf-8") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise

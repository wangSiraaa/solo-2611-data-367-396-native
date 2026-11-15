"""不可变段、提交清单与租约的本地文件存储。

布局：
  <root>/segments/seg_<id>.json   不可变段，一次写好后永不修改
  <root>/manifest.json            提交清单（原子 rename 切换）
  <root>/leases/lease_<pid>_<n>   读视图租约，保护正在读的旧段不被回收

posting 在段内以数组形式编码（元组语义）：
  [doc_id, revision, field, value_no, position, byte_start, byte_end]
按 (term, doc_id, value_no, position, byte_start) 排序，短语查询的
“同值 + 顺序位置”过滤完全在查询阶段显式完成。
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field

# posting 数组各下标
P_DOC = 0
P_REV = 1
P_FIELD = 2
P_VALUE = 3
P_POS = 4
P_BSTART = 5
P_BEND = 6

MANIFEST_NAME = "manifest.json"
SEG_DIR = "segments"
LEASE_DIR = "leases"


@dataclass
class Segment:
    """一个不可变段的内存视图。段文件落盘后内容永不改变。"""

    seg_id: int
    postings: dict[str, list[list]]                      # term -> posting 列表
    docs: dict[str, dict]                                # doc_id -> {revision, doc}

    def to_json_obj(self) -> dict:
        ordered = {
            term: sorted(
                self.postings[term],
                key=lambda p: (p[P_DOC], p[P_VALUE], p[P_POS], p[P_BSTART]),
            )
            for term in sorted(self.postings)
        }
        return {"seg_id": self.seg_id, "postings": ordered, "docs": self.docs}


@dataclass
class Manifest:
    """显式提交清单：只有出现在这里的段与版本可被新查询看到。"""

    segments: list[int] = field(default_factory=list)
    # doc_id -> {"revision": int, "op": "put"/"delete", "seg": int}
    active: dict[str, dict] = field(default_factory=dict)
    next_seg_id: int = 1
    next_rev: dict[str, int] = field(default_factory=dict)
    stopwords: list[str] = field(default_factory=list)
    generation: int = 0

    def to_json_obj(self) -> dict:
        return {
            "generation": self.generation,
            "segments": self.segments,
            "active": self.active,
            "next_seg_id": self.next_seg_id,
            "next_rev": self.next_rev,
            "stopwords": self.stopwords,
        }

    @classmethod
    def from_json_obj(cls, obj: dict) -> "Manifest":
        return cls(
            segments=list(obj["segments"]),
            active=dict(obj["active"]),
            next_seg_id=obj["next_seg_id"],
            next_rev=dict(obj.get("next_rev", {})),
            stopwords=list(obj.get("stopwords", [])),
            generation=obj.get("generation", 0),
        )


def ensure_layout(root: str) -> None:
    os.makedirs(os.path.join(root, SEG_DIR), exist_ok=True)
    os.makedirs(os.path.join(root, LEASE_DIR), exist_ok=True)


def seg_path(root: str, seg_id: int) -> str:
    return os.path.join(root, SEG_DIR, f"seg_{seg_id}.json")


def manifest_path(root: str) -> str:
    return os.path.join(root, MANIFEST_NAME)


def write_segment(root: str, seg: Segment) -> None:
    """段文件原子落盘：先写临时文件再 rename，读者永远看不到半段。"""
    ensure_layout(root)
    final = seg_path(root, seg.seg_id)
    fd, tmp = tempfile.mkstemp(
        prefix=f".seg_{seg.seg_id}.", suffix=".tmp", dir=os.path.join(root, SEG_DIR)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(seg.to_json_obj(), f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, final)
    except BaseException:
        os.unlink(tmp)
        raise


def load_segment(root: str, seg_id: int) -> Segment:
    with open(seg_path(root, seg_id), "r", encoding="utf-8") as f:
        obj = json.load(f)
    return Segment(seg_id=obj["seg_id"], postings=obj["postings"], docs=obj["docs"])


def write_manifest(root: str, manifest: Manifest) -> None:
    """清单原子切换：rename 之后的新查询看到完整清单，之前的查询仍持旧清单。"""
    ensure_layout(root)
    final = manifest_path(root)
    fd, tmp = tempfile.mkstemp(prefix=".manifest.", suffix=".tmp", dir=root)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(manifest.to_json_obj(), f, ensure_ascii=False, indent=1)
        os.replace(tmp, final)
    except BaseException:
        os.unlink(tmp)
        raise


def load_manifest(root: str) -> Manifest | None:
    path = manifest_path(root)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return Manifest.from_json_obj(json.load(f))


# ---------- 读视图租约 ----------


def create_lease(root: str) -> str:
    """登记一个读视图，返回租约文件路径。compact 回收段时必须避开在册租约。"""
    ensure_layout(root)
    pid = os.getpid()
    n = 0
    base = os.path.join(root, LEASE_DIR, f"lease_{pid}_")
    while True:
        path = f"{base}{n}"
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(pid).encode("ascii"))
            os.close(fd)
            return path
        except FileExistsError:
            n += 1


def release_lease(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _lease_pid(path: str) -> int | None:
    try:
        with open(path, "r", encoding="ascii") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def live_lease_pids(root: str) -> set[int]:
    """返回仍有效租约所属的进程集合；属主进程已死的租约视为过期。

    同进程内仍打开的读视图由引擎通过 _LOCAL_LEASES 显式补充保护，
    这里不能无条件加入当前 pid——否则同进程 reap 将永远无法回收。
    """
    ldir = os.path.join(root, LEASE_DIR)
    if not os.path.isdir(ldir):
        return set()
    alive: set[int] = set()
    for name in os.listdir(ldir):
        if not name.startswith("lease_"):
            continue
        pid = _lease_pid(os.path.join(ldir, name))
        if pid is not None and _pid_alive(pid):
            alive.add(pid)
    return alive

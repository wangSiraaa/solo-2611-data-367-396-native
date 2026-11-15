"""测试支撑：屏障原语与独立于引擎的逐行展开参考实现。

所有测试同步都走 threading.Event / StatusBoard 条件谓词，
禁止用“sleep 没超时”来推断无死锁。
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

from .model import Frame


class Barrier:
    """可复用暂停屏障：arm() 后 wait() 阻塞，release() 解除。

    与取消相互独立：引擎 cancel 后角色自身会在等待点退出，
    测试再 release 屏障即可让所有角色在有限步内收敛。
    """

    def __init__(self, name: str):
        self.name = name
        self._event = threading.Event()
        self.armed = threading.Event()
        self.reached = threading.Event()

    def arm(self) -> None:
        self._event.clear()
        self.armed.set()
        self.reached.clear()

    def wait(self, timeout: float = 10.0) -> None:
        self.reached.set()
        if not self._event.wait(timeout=timeout):
            raise AssertionError(f"屏障 {self.name} 等待解除超时")

    def release(self) -> None:
        self.armed.clear()
        self._event.set()

    @property
    def is_released(self) -> bool:
        return self._event.is_set()


def expected_expansion(source_paths: dict[str, Path]) -> dict[tuple, int]:
    """独立逐行展开参考：返回 {结果身份: 业务值} 的完整多重集字典。

    身份含 source_id，因此不同来源同 seq 不会合并；同业务值也不去重。
    """
    expected: dict[tuple, int] = {}
    for sid, path in sorted(source_paths.items()):
        with open(path, "rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                assert rec["source_id"] == sid
                for part_no in range(rec["fanout"]):
                    identity = (sid, rec["seq"], part_no)
                    assert identity not in expected, f"结果身份冲突 {identity}"
                    expected[identity] = rec["value"] * rec["value"] + part_no
    return expected


def read_outputs(path: Path) -> list[dict]:
    if not Path(path).exists():
        return []
    rows = []
    with open(path, "rb") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def output_multiset(rows: list[dict]) -> dict[tuple, int]:
    ms: dict[tuple, int] = {}
    for r in rows:
        key = (r["source_id"], r["seq"], r["part_no"])
        assert key not in ms, f"输出结果身份重复 {key}"
        ms[key] = r["value"]
    return ms


def make_worker_fail_hook(fail_identity: tuple[str, int], worker_id: int):
    """在 worker 处理指定输入前注入异常的钩子工厂。"""

    def hook(wid: int, frame: Frame):
        rec = frame.record
        if wid == worker_id and (rec["source_id"], rec["seq"]) == fail_identity:
            raise RuntimeError(
                f"注入故障: worker-{wid} 处理 {rec['source_id']}:{rec['seq']} 前失败"
            )

    return hook

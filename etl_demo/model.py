"""帧、配置、结果与异常定义。"""
from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

# 输入帧（单行 JSONL 编码后）硬上限，题目要求 256 字节。
MAX_INPUT_FRAME = 256
# 全局“尚未落盘的编码 payload”上限：待发送、队列、角色持有、重试暂存全部计入。
DEFAULT_BUDGET_LIMIT = 16 * 1024

DATA = "DATA"
EOS = "EOS"

_frame_counter = itertools.count(1)


class CancelledError(Exception):
    """取消控制被触发后，各角色在等待点抛出的统一异常。"""


class ChannelClosedError(Exception):
    """向已经写入 EOS 的通道继续 put 时抛出（正常协议下不应发生）。"""


@dataclass(eq=False)
class Frame:
    """通道中流动的一帧。

    kind=EOS 时 payload 必须为 b''（零字节，不占 payload 预算），
    且 EOS 与数据共用同一个 deque 排队，终止永远不会越过此前的数据。
    fid 是帧级唯一身份，用于 ByteBudget 的幂等释放，防止取消/异常竞争下
    同一 payload 被两个所有者重复归还。
    """

    kind: str
    payload: bytes
    record: dict | None = None
    origin: str = ""
    fid: int = -1

    @staticmethod
    def data(payload: bytes, record: dict, origin: str) -> "Frame":
        return Frame(DATA, payload, record, origin, next(_frame_counter))

    @staticmethod
    def eos(origin: str) -> "Frame":
        return Frame(EOS, b"", None, origin, -1)

    @property
    def is_eos(self) -> bool:
        return self.kind == EOS

    def identity(self) -> str | None:
        if self.record is None:
            return None
        return f"{self.record['source_id']}:{self.record['seq']}"


@dataclass
class Hooks:
    """测试控制入口。所有钩子均可选，默认 None 表示不介入。

    - worker_before_process: worker 拿到一条输入、展开之前调用，
      抛异常即模拟 worker 失败，返回 Event 时会先等屏障解除再处理。
    - sink_on_written: sink 每落盘一条后调用，测试可在此设置暂停屏障。
    - sink_fail_writes: 让 sink 的第 k 次写盘抛 OSError（k 从 1 开始），验证重试暂存。
    """

    worker_before_process: Callable[[int, Frame], Any] | None = None
    sink_on_written: Callable[[int, Frame], Any] | None = None
    sink_fail_writes: set[int] = field(default_factory=set)


@dataclass
class EngineConfig:
    sources: dict[str, Path]
    run_dir: Path
    output_name: str = "output.jsonl"
    # 一个 int 表示三组通道同容量；三元组分别对应 源→路由 / 路由→worker / worker→sink。
    capacity: int | tuple[int, int, int] = 1
    budget_limit: int = DEFAULT_BUDGET_LIMIT
    # 为 worker 输出推进保留的预算：输入帧最多占用 limit-output_reserved，
    # 保证 sink 停读或预算紧张时仍能产出输出帧、由 sink 排空解锁，杜绝循环等待。
    output_reserved: int = 256
    max_input_frame: int = MAX_INPUT_FRAME
    hooks: Hooks = field(default_factory=Hooks)
    sink_pause: threading.Event | None = None
    fsync_each_write: bool = False

    def capacities(self) -> tuple[int, int, int]:
        if isinstance(self.capacity, int):
            c = self.capacity
            return (c, c, c)
        return self.capacity


@dataclass
class RunResult:
    outcome: str  # complete | cancelled | failed
    output_path: Path
    complete_marker: Path | None
    outputs_written: int
    bytes_on_disk: int
    budget_peak: int
    budget_limit: int
    spool_capacity_bytes: int          # 本实现不落盘暂存，固定 0
    retry_staging_peak_bytes: int      # 内存重试暂存峰值（已计入预算）
    channel_capacities: tuple[int, int, int]
    channel_peak_frames: dict[str, int]
    fixed_thread_count: int            # 固定线程开销，与 payload 预算分开报告
    max_rss_kib: int | None            # 解释器整体 RSS 峰值（如可取）
    error: dict | None
    final_state: dict

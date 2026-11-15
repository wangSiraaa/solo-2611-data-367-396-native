"""离线有界 ETL 演示包。

拓扑：
    source_a ─┐                         ┌─> worker0 ─┐
              ├─> router ─按 key%2 分区─┤            ├─> sink(落盘)
    source_b ─┘                         └─> worker1 ─┘

全部线程内通信走自定义有界通道（容量 1/2 帧），全局编码 payload 由
ByteBudget 做信用式台账（上限默认 16KiB），取消走独立 Event，不经过数据队列。
"""
from .model import CancelledError, ChannelClosedError, Frame, EngineConfig, Hooks, RunResult
from .engine import Engine, run_pipeline
from .test_support import Barrier

__all__ = [
    "CancelledError",
    "ChannelClosedError",
    "Frame",
    "EngineConfig",
    "Hooks",
    "RunResult",
    "Engine",
    "run_pipeline",
    "Barrier",
]

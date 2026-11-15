"""真实有界帧通道 + 多路接收组。

设计要点：
- 每个 BoundedChannel 自带 Condition，容量为 1 或 2 帧（可配置），绝不扩容。
- put/get 都是就绪驱动：在条件变量上等待“有空位/有数据”，等待点同时
  响应独立的 cancel Event（取消不经过任何数据队列，不可能排在满队列尾部）。
- EOS 与 DATA 排在同一个 deque 里，终止帧严格排在数据之后，天然不会越过数据。
- ChannelGroup.get_any 实现“多路接收”：先无阻塞全量扫描任一已就绪通道；
  全部为空时在组共享的就绪条件上等待，任何成员通道的 put/close 都会通知它，
  唤醒后重新扫描。既不忙转，也不会漏掉另一路的通知，更不需要额外分发队列。
"""
from __future__ import annotations

import threading
from collections import deque
from typing import Iterable

from .model import EOS, CancelledError, ChannelClosedError, Frame
from .state import ByteBudget, StatusBoard


class BoundedChannel:
    def __init__(
        self,
        name: str,
        capacity: int,
        budget: ByteBudget,
        board: StatusBoard,
        cancel: threading.Event,
    ):
        if capacity not in (1, 2):
            raise ValueError("通道容量只允许配置为 1 或 2 帧")
        self.name = name
        self.capacity = capacity
        self._q: deque[Frame] = deque()
        self._cond = threading.Condition()
        # 多路接收组注册的额外就绪条件：本通道入队/排空时一并 notify。
        self._readiness: list[threading.Condition] = []
        self._closed = False
        self._budget = budget
        self._board = board
        self._cancel = cancel
        board.register_channel(name, capacity)

    # ---- 内部水位记账（持通道锁调用） ----
    def _bytes_locked(self) -> int:
        return sum(len(f.payload) for f in self._q)

    def _sync_board_locked(self) -> None:
        # 通道锁 -> state 锁，全引擎固定此加锁顺序，不会形成锁环。
        # 与 ByteBudget/角色持有共用 state 锁，保证水位与台账在同一临界区可见。
        with self._board.lock:
            self._board.channel(
                self.name,
                frames=len(self._q),
                payload_bytes=self._bytes_locked(),
                closed=self._closed,
            )

    def _notify_readiness(self) -> None:
        for c in self._readiness:
            with c:
                c.notify_all()

    def add_readiness(self, cond: threading.Condition) -> None:
        self._readiness.append(cond)

    def put(self, frame: Frame, role_name: str) -> bool:
        """放入一帧；满了就在此背压等待。返回 True 表示帧确定已入队。

        容量判定优先于取消：只要当前有空位就原子完成入队（所有权移交队列）；
        仅在“仍需等待且取消已触发”时返回 False，此时帧确定未入队、所有权
        仍归调用方，由调用方释放。不会出现“帧被拒绝但槽位已空”的悬空所有权。
        """
        with self._cond:
            while len(self._q) >= self.capacity:
                if self._closed:
                    raise ChannelClosedError(f"{self.name} 已关闭")
                if self._cancel.is_set():
                    return False
                self._board.role(role_name, state="blocked_put")
                self._cond.wait(timeout=0.05)
            if self._closed:
                raise ChannelClosedError(f"{self.name} 已关闭")
            self._q.append(frame)  # 字节已在帧创建时计入预算，入队不重复计数
            self._sync_board_locked()
            self._cond.notify_all()
        self._notify_readiness()
        return True

    def get(self, role_name: str) -> Frame:
        with self._cond:
            while not self._q:
                if self._cancel.is_set():
                    raise CancelledError(f"{role_name} 取消于 get {self.name}")
                self._board.role(role_name, state="blocked_get")
                self._cond.wait(timeout=0.05)
            # 已有数据即原子取走；取消在下一轮等待点再响应，保证在途帧所有权明确。
            frame = self._q.popleft()
            self._sync_board_locked()
            self._cond.notify_all()
            return frame

    def close(self, role_name: str) -> bool:
        """写入 EOS 终止帧：与数据同队列排队，保证终止不越过此前数据。"""
        enqueued = self.put(Frame.eos(role_name), role_name)
        if not enqueued:
            return False  # 取消中：零字节 EOS 无需入队，角色随后退出
        with self._cond:
            self._closed = True
            self._sync_board_locked()
            self._cond.notify_all()
        return True

    def drain(self) -> list[Frame]:
        """引擎收尾时排空残留帧（取消/异常路径），返回的帧由调用方释放预算。"""
        with self._cond:
            leftover = list(self._q)
            self._q.clear()
            self._closed = True
            self._sync_board_locked()
            self._cond.notify_all()
            return leftover

    @property
    def depth(self) -> int:
        with self._cond:
            return len(self._q)


class ChannelGroup:
    """一组通道上的多路接收：返回 (通道名, 帧)。

    就绪驱动而非忙等：组持有一个共享条件，成员通道每次入队都会 notify；
    扫描不到任何帧时在该条件上挂起，任一通道就绪或取消即被唤醒。
    """

    def __init__(self, channels: Iterable[BoundedChannel], cancel: threading.Event,
                 shared_ready: threading.Condition | None = None):
        self.channels = list(channels)
        self._cancel = cancel
        # 全引擎共享同一个就绪条件：任何通道（含属于另一个接收组的通道）
        # 的 put/close 都会 notify 它，杜绝“跨组通知丢失”。
        self._ready_cond = shared_ready or threading.Condition()
        for ch in self.channels:
            ch.add_readiness(self._ready_cond)

    def get_any(self, role_name: str) -> tuple[str, Frame]:
        while True:
            # 无阻塞全量扫描：任何通道已有帧（数据或 EOS）立即取走。
            # 扫描优先于取消判定——已有帧时先完成本次明确的所有权转移，
            # 取消只在“全部通道都空、确实需要继续等”时才响应，
            # 从而取消后仍能排空已经在途的数据，且在途帧归属始终确定。
            for ch in self.channels:
                with ch._cond:
                    if ch._q:
                        frame = ch._q.popleft()
                        ch._sync_board_locked()
                        ch._cond.notify_all()
                        return ch.name, frame
            if self._cancel.is_set():
                raise CancelledError(f"{role_name} 取消于多路接收")
            with self._ready_cond:
                self._ready_cond.wait(timeout=0.05)
            # 唤醒后无条件回到顶部重新扫描，不在此处抢先抛取消。

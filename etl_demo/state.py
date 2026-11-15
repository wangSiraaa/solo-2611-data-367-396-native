"""线程共享状态板：角色状态、通道水位、全局 payload 预算、事件流。"""
from __future__ import annotations

import threading
import time
from collections import deque

from .model import CancelledError


class ByteBudget:
    """全局“尚未落盘的编码 payload”信用台账。

    所有阶段共用同一本账：source 持有的待发送帧、各有界队列中的帧、
    router/worker/sink 当前持有的帧、sink 写盘失败后的重试暂存帧。
    帧从一个容器转移到另一个容器不重复计数（acquire 一次、release 一次）。
    EOS 帧 payload 为 b''，acquire(0) 自然不占字节。

    为处理取消/异常时“角色收尾”与“引擎 drain”可能接触同一帧的竞争，
    release 支持帧级幂等：每个 payload 帧有唯一 token，已释放的 token 再传
    入会被忽略而不是把台账减成负数。
    """

    def __init__(self, limit: int, reserved: int = 0,
                 lock: threading.Condition | None = None):
        self._limit = limit
        # reserved：为“输出推进”永久保留的额度。输入帧（source 建帧）只能使用
        # limit-reserved 空间，从而保证任意 worker 处理手头输入时，至少有空间
        # 产出一个输出帧，sink 可持续排空释放预算，避免输入囤积把输出饿死的环。
        self._reserved = reserved
        self._used = 0
        self._peak = 0
        self._released: set[int] = set()
        # 与 StatusBoard 共用同一把锁/条件：预算台账与所有权水位在同一临界区
        # 提交，inspect 读到的恒为一致状态，杜绝跨锁的瞬时账本不符。
        self._cond = lock or threading.Condition()

    @property
    def lock(self) -> threading.Condition:
        return self._cond

    def acquire(self, n: int, cancel: threading.Event, token: int | None = None,
                honor_reserved: bool = True) -> None:
        if n > self._limit:
            raise ValueError(f"单帧 {n} 字节超过全局预算 {self._limit}")
        cap = self._limit - (self._reserved if honor_reserved else 0)
        with self._cond:
            while self._used + n > cap:
                if cancel.is_set():
                    raise CancelledError("cancel while waiting byte budget")
                self._cond.wait(timeout=0.05)
            self._used += n
            if self._used > self._peak:
                self._peak = self._used

    def release(self, n: int, token: int | None = None) -> None:
        with self._cond:
            if token is not None:
                if token in self._released:
                    return  # 幂等：该帧已被某个所有者释放，忽略重复认领
                self._released.add(token)
            self._used -= n
            if self._used < 0:
                raise AssertionError(f"payload 台账为负: {self._used}")
            self._cond.notify_all()

    @property
    def used(self) -> int:
        with self._cond:
            return self._used

    @property
    def peak(self) -> int:
        with self._cond:
            return self._peak

    def assert_zero(self) -> None:
        with self._cond:
            assert self._used == 0, f"payload 泄漏 {self._used} 字节"

    def notify(self) -> None:
        with self._cond:
            self._cond.notify_all()


class StatusBoard:
    """供 inspect() 与测试屏障读取的活状态。所有字段都在 lock 下访问。"""

    def __init__(self, lock: threading.Condition | None = None):
        # 默认自建锁；引擎会注入与 ByteBudget 相同的 Condition，使预算台账
        # 与角色/通道水位在同一临界区串行提交，保证 inspect 的一致性。
        self.lock = lock or threading.Condition()
        self.roles: dict[str, dict] = {}
        self.channels: dict[str, dict] = {}
        self.events: deque[tuple[float, str, dict]] = deque(maxlen=512)
        self.outputs_written = 0
        self.bytes_on_disk = 0
        self.retry_staging_bytes = 0
        self.retry_staging_peak = 0
        self.error: dict | None = None
        self.outcome: str | None = None

    def register_role(self, name: str) -> None:
        with self.lock:
            self.roles[name] = {
                "state": "created",       # created|running|blocked_get|blocked_put|done|cancelled|failed
                "holding_bytes": 0,       # 当前持有 payload 字节
                "holding_identity": None,
                "records_seen": 0,
                "records_processed": 0,
                "outputs_emitted": 0,
                "eos_seen": 0,
            }
            self.lock.notify_all()

    def register_channel(self, name: str, capacity: int) -> None:
        with self.lock:
            self.channels[name] = {
                "capacity": capacity,
                "frames": 0,
                "payload_bytes": 0,
                "peak_frames": 0,
                "closed": False,
            }
            self.lock.notify_all()

    def role(self, name: str, **patch) -> None:
        with self.lock:
            self.roles[name].update(patch)
            self.lock.notify_all()

    def role_bump(self, name: str, **delta) -> None:
        with self.lock:
            r = self.roles[name]
            for k, v in delta.items():
                r[k] = r.get(k, 0) + v
            self.lock.notify_all()

    def channel(self, name: str, **patch) -> None:
        with self.lock:
            c = self.channels[name]
            c.update(patch)
            if patch.get("frames") is not None and c["frames"] > c["peak_frames"]:
                c["peak_frames"] = c["frames"]
            self.lock.notify_all()

    def event(self, name: str, **detail) -> None:
        with self.lock:
            self.events.append((time.monotonic(), name, detail))
            self.lock.notify_all()

    def retry_staging_add(self, n: int) -> None:
        with self.lock:
            self.retry_staging_bytes += n
            if self.retry_staging_bytes > self.retry_staging_peak:
                self.retry_staging_peak = self.retry_staging_bytes
            self.lock.notify_all()

    def retry_staging_remove(self, n: int) -> None:
        with self.lock:
            self.retry_staging_bytes -= n
            assert self.retry_staging_bytes >= 0
            self.lock.notify_all()

    def set_error(self, err: dict) -> None:
        with self.lock:
            if self.error is None:
                self.error = err
            self.outcome = "failed"
            self.lock.notify_all()

    def set_outcome(self, outcome: str) -> None:
        with self.lock:
            self.outcome = outcome
            self.lock.notify_all()

    def wait_until(self, predicate, timeout: float = 10.0):
        """以条件变量等待控制点达成；超时抛出带快照的 AssertionError。

        测试只依赖状态谓词，不依赖 sleep 时长。
        """
        deadline = time.monotonic() + timeout
        with self.lock:
            while True:
                snapshot = self.snapshot_locked()
                if predicate(snapshot):
                    return snapshot
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        "等待测试控制点超时，最后快照:\n"
                        + _pretty(snapshot)
                    )
                self.lock.wait(timeout=min(remaining, 0.05))

    def snapshot(self) -> dict:
        with self.lock:
            return self.snapshot_locked()

    def ownership_summary_locked(self) -> dict:
        """在 state 锁内读取的 payload 所有权汇总（调用方需持 state 锁或经 snapshot）。"""
        held = sum(r["holding_bytes"] for r in self.roles.values())
        queued = sum(c["payload_bytes"] for c in self.channels.values())
        return {
            "held_by_roles": held,
            "queued_in_channels": queued,
            "retry_staging": self.retry_staging_bytes,
            "book_total": held + queued + self.retry_staging_bytes,
        }

    def ownership_with_budget(self, budget: "ByteBudget") -> dict:
        """在单一 state 临界区内同时读取预算台账与所有权水位。

        二者共用 state 锁，本方法返回的 budget_used 与 book_total 是同一时刻的
        一致观测。角色持有字节的提交与预算增减可能相差一个语句，因此 book_total
        允许瞬时 <= budget_used（已计预算、board 水位尚未提交），但绝不允许 >
        （出现预算台账之外的游离字节）。
        """
        with self.lock:
            summary = self.ownership_summary_locked()
            summary["budget_used"] = budget.used
            summary["budget_limit"] = budget._limit
            return summary

    def snapshot_locked(self) -> dict:
        return {
            "outcome": self.outcome,
            "error": self.error,
            "outputs_written": self.outputs_written,
            "bytes_on_disk": self.bytes_on_disk,
            "retry_staging_bytes": self.retry_staging_bytes,
            "roles": {k: dict(v) for k, v in self.roles.items()},
            "channels": {k: dict(v) for k, v in self.channels.items()},
            "events": [
                {"t": round(t, 6), "name": n, "detail": d}
                for t, n, d in list(self.events)[-40:]
            ],
        }


def _pretty(snapshot: dict) -> str:
    lines = []
    lines.append(f"outcome={snapshot['outcome']} outputs={snapshot['outputs_written']}")
    for name, r in snapshot["roles"].items():
        lines.append(f"  role {name}: {r}")
    for name, c in snapshot["channels"].items():
        lines.append(f"  channel {name}: frames={c['frames']}/{c['capacity']} "
                     f"bytes={c['payload_bytes']} closed={c['closed']}")
    return "\n".join(lines)

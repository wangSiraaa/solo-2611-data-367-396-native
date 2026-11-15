"""执行引擎：装配 2 source + 1 router + 2 worker + 1 sink 的有界拓扑。

终止控制分两条独立通道：
- 数据终止：EOS 帧走数据队列排队，严格不越过数据；
- 取消：threading.Event，所有等待点（队列、预算、测试屏障）都轮询它，
  因此取消不可能排在已满的数据队列尾部，屏障解除后有限步内即可收敛。
任一角色异常 -> 记录错误归属 -> set cancel -> 其他角色在最近等待点退出
-> 引擎排空全部通道并按所有权释放残留 payload -> join 全部线程。
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from .channels import BoundedChannel
from .model import CancelledError, EngineConfig, RunResult
from .roles import router_runner, sink_runner, source_runner, worker_runner
from .state import ByteBudget, StatusBoard

THREAD_ROLES = ("source-a", "source-b", "router", "worker-0", "worker-1", "sink")


def _rss_kib() -> int | None:
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # Linux: KiB
    except Exception:
        return None


class Engine:
    def __init__(self, cfg: EngineConfig):
        self.cfg = cfg
        self.cancel_event = threading.Event()
        # 全引擎唯一的多路接收就绪条件：所有 ChannelGroup 共用，
        # 任何通道的入队/EOS 都能唤醒任意在多路接收上等待的角色。
        self.ready_cond = threading.Condition()
        # 预算台账与状态板共用同一把锁：预算增减与所有权水位同临界区提交，
        # inspect 在该锁内拿到的账本与水位必然一致。
        self.state_cond = threading.Condition()
        # 保留额度不得超过总预算（极紧预算下退化为“输入只能用总预算减去一个
        # 最小输出帧的空间”，至少保证一个输出帧可被授予）。
        reserved = min(cfg.output_reserved, max(0, cfg.budget_limit - 64))
        self.budget = ByteBudget(cfg.budget_limit, reserved=reserved,
                                 lock=self.state_cond)
        self.board = StatusBoard(lock=self.state_cond)
        c_src, c_wrk, c_sink = cfg.capacities()
        self.src_ch = {
            sid: BoundedChannel(f"src:{sid}", c_src, self.budget, self.board, self.cancel_event)
            for sid in sorted(cfg.sources)
        }
        self.worker_ch = [
            BoundedChannel(f"r2w:{i}", c_wrk, self.budget, self.board, self.cancel_event)
            for i in (0, 1)
        ]
        self.sink_ch = [
            BoundedChannel(f"w2s:{i}", c_sink, self.budget, self.board, self.cancel_event)
            for i in (0, 1)
        ]
        self.threads: dict[str, threading.Thread] = {}
        self._started = False
        self._joined = False
        self.run_dir = Path(cfg.run_dir)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.output_path = self.run_dir / cfg.output_name

    # ------------------------------------------------------------ 生命周期
    def start(self) -> None:
        if self._started:
            return
        self._started = True

        def wrap(target, name, args):
            # 线程内异常统一在此收口，不向 stderr 打印堆栈：
            # - CancelledError 是独立取消通道的正常控制流；
            # - 真实故障已由角色结构化写入 board.error（含 role/input_identity）。
            def runner():
                try:
                    target(*args)
                except CancelledError:
                    self.board.event("thread.exited_cancelled", thread=name)
                except Exception as exc:  # noqa: BLE001 - 引擎边界统一收口
                    self.board.event(
                        "thread.exited_failed",
                        thread=name,
                        type=type(exc).__name__,
                        message=str(exc),
                    )
            return threading.Thread(target=runner, name=name, daemon=False)

        for sid, ch in self.src_ch.items():
            name = f"source-{sid}"
            self.threads[name] = wrap(
                source_runner, name,
                (sid, self.cfg.sources[sid], ch, self.budget,
                 self.board, self.cancel_event, self.cfg.max_input_frame))
        self.threads["router"] = wrap(
            router_runner, "router",
            (self.src_ch, self.worker_ch, self.budget, self.board,
             self.cancel_event, self.ready_cond))
        for wid in (0, 1):
            name = f"worker-{wid}"
            self.threads[name] = wrap(
                worker_runner, name,
                (wid, self.worker_ch[wid], self.sink_ch[wid], self.budget,
                 self.board, self.cancel_event, self.cfg.hooks))
        self.threads["sink"] = wrap(
            sink_runner, "sink",
            (self.sink_ch, self.output_path, self.budget,
             self.board, self.cancel_event, self.cfg, self.ready_cond))
        self.board.event("engine.start", capacities=self.cfg.capacities())
        for t in self.threads.values():
            t.start()

    def cancel(self, reason: str = "test-cancel") -> None:
        """独立终止控制：不经过任何数据队列。"""
        self.board.event("engine.cancel", reason=reason)
        self.cancel_event.set()
        self.budget.notify()  # 唤醒可能在预算条件上等待的角色

    def inspect(self) -> dict:
        """活状态快照：角色状态/持有字节、通道水位、预算峰值、事件前缀。

        预算台账与状态板共用一把锁，整个快照在同一临界区读取，账本与水位一致。
        """
        with self.state_cond:
            snap = self.board.snapshot_locked()
            snap["budget"] = {
                "used": self.budget.used,
                "peak": self.budget.peak,
                "limit": self.cfg.budget_limit,
            }
        snap["threads_alive"] = {n: t.is_alive() for n, t in self.threads.items()}
        return snap

    def join(self, timeout: float = 30.0) -> RunResult:
        if not self._started:
            raise RuntimeError("engine 尚未 start")
        deadline = time.monotonic() + timeout
        failures: dict[str, str] = {}
        for name, t in self.threads.items():
            remaining = max(0.0, deadline - time.monotonic())
            t.join(timeout=remaining)
            if t.is_alive():
                raise RuntimeError(f"线程 {name} 在超时内未回收")
            if t.ident is not None:
                # 线程内部已经把结构化错误写入 board；这里兜底捕获未记录异常。
                pass

        # 统一排空所有通道。残留 DATA 帧用 fid 幂等释放：若角色收尾已认领
        # （取消/异常竞争），release 按 token 去重；仍在预算内的帧这里归还。
        drained = 0
        for ch in list(self.src_ch.values()) + self.worker_ch + self.sink_ch:
            for frame in ch.drain():
                if frame.payload:
                    self.budget.release(len(frame.payload), frame.fid)
                    drained += 1
        self._joined = True
        try:
            self.budget.assert_zero()  # 台账不归零说明有 payload 泄漏，直接失败
        except AssertionError:
            snap = self.inspect()
            raise AssertionError(
                f"payload 台账未归零，used={self.budget.used} drained={drained}\n"
                + json.dumps(snap, ensure_ascii=False, indent=2)
            )

        if self.board.error is not None:
            outcome = "failed"
        elif self.cancel_event.is_set() and self.board.outcome != "failed":
            outcome = "cancelled"
        else:
            outcome = "complete"
        self.board.set_outcome(outcome)

        marker = self.output_path.with_suffix(self.output_path.suffix + ".complete")
        result = RunResult(
            outcome=outcome,
            output_path=self.output_path,
            complete_marker=marker if marker.exists() else None,
            outputs_written=self.board.outputs_written,
            bytes_on_disk=self.board.bytes_on_disk,
            budget_peak=self.budget.peak,
            budget_limit=self.cfg.budget_limit,
            spool_capacity_bytes=0,  # 本实现不做有界落盘暂存，披露为 0
            retry_staging_peak_bytes=self.board.retry_staging_peak,
            channel_capacities=self.cfg.capacities(),
            channel_peak_frames={
                name: c["peak_frames"] for name, c in self.board.channels.items()
            },
            fixed_thread_count=len(self.threads),
            max_rss_kib=_rss_kib(),
            error=self.board.error,
            final_state=self.inspect(),
        )
        state_file = self.run_dir / "state.json"
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "outcome": result.outcome,
                    "outputs_written": result.outputs_written,
                    "bytes_on_disk": result.bytes_on_disk,
                    "budget_peak": result.budget_peak,
                    "budget_limit": result.budget_limit,
                    "spool_capacity_bytes": result.spool_capacity_bytes,
                    "retry_staging_peak_bytes": result.retry_staging_peak_bytes,
                    "channel_capacities": result.channel_capacities,
                    "channel_peak_frames": result.channel_peak_frames,
                    "fixed_thread_count": result.fixed_thread_count,
                    "max_rss_kib": result.max_rss_kib,
                    "error": result.error,
                    "snapshot": result.final_state,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        return result


def run_pipeline(cfg: EngineConfig, timeout: float = 30.0) -> RunResult:
    eng = Engine(cfg)
    eng.start()
    return eng.join(timeout=timeout)

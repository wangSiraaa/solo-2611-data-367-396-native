"""角色线程主体：两路 source、单 router、两个 worker、一个 sink。

帧所有权（payload 字节在 ByteBudget 中只计一次）：
    source 建帧(acquire) -> 源队列 -> router 持有 -> worker 队列
    -> worker 持有输入(release 输入) 后逐 part acquire 输出 -> sink 队列
    -> sink 持有 -> 落盘后 release；写盘失败则转入重试暂存（仍在预算内）。
"""
from __future__ import annotations

import json
import threading

from .channels import BoundedChannel, ChannelGroup
from .model import CancelledError, Frame
from .state import ByteBudget, StatusBoard


# ---------------------------------------------------------------- source
def source_runner(
    source_id: str,
    path,
    out: BoundedChannel,
    budget: ByteBudget,
    board: StatusBoard,
    cancel: threading.Event,
    max_input_frame: int,
) -> None:
    name = f"source-{source_id}"
    board.register_role(name)
    held: Frame | None = None
    last_seq: int | None = None
    try:
        board.role(name, state="running")
        with open(path, "rb") as f:
            for lineno, raw in enumerate(f, 1):
                line = raw.strip()
                if not line:
                    continue
                if len(line) > max_input_frame:
                    raise ValueError(
                        f"{name} 第 {lineno} 行 {len(line)} 字节 > 输入帧上限 {max_input_frame}"
                    )
                record = json.loads(line)
                if record.get("source_id") != source_id:
                    raise ValueError(f"{name} 第 {lineno} 行 source_id 不匹配: {record}")
                seq = record["seq"]
                if not isinstance(seq, int) or (last_seq is not None and seq <= last_seq):
                    raise ValueError(f"{name} seq 必须严格递增: {record}")
                last_seq = seq
                for k in ("key", "value", "fanout"):
                    if not isinstance(record.get(k), int):
                        raise ValueError(f"{name} 第 {lineno} 行字段 {k} 非整数")
                if not 0 <= record["fanout"] <= 3:
                    raise ValueError(f"{name} fanout 越界: {record}")

                # 背压点 1：全局 payload 信用不足时停发（不再读下一行）。
                # 输入帧遵守输出保留额度，避免在途输入把输出预算饿死形成循环等待。
                budget.acquire(len(line), cancel, honor_reserved=True)
                held = Frame.data(line, record, name)
                board.role(
                    name,
                    state="running",
                    holding_bytes=len(line),
                    holding_identity=held.identity(),
                )
                board.role_bump(name, records_seen=1)
                board.event("source.emit", source=source_id, identity=held.identity())
                # 背压点 2：通道满则阻塞，sink 停读时压力自然一路传回 source。
                if not out.put(held, name):
                    # 取消且帧未入队：所有权仍归 source，在此释放后退出。
                    budget.release(len(held.payload), held.fid)
                    held = None
                    raise CancelledError(f"{name} 取消于 put {out.name}")
                held = None
                board.role(name, holding_bytes=0, holding_identity=None)
        # 源终止只代表“不再产生输入”，EOS 走数据通道排队，不会越过已发数据。
        if not out.close(name):
            raise CancelledError(f"{name} 取消于发送 EOS")
        board.role(name, state="done")
        board.event("source.eos", source=source_id)
    except CancelledError:
        if held is not None:
            budget.release(len(held.payload), held.fid)
            held = None
        board.role(name, state="cancelled", holding_bytes=0, holding_identity=None)
        raise
    except Exception as exc:
        if held is not None:
            budget.release(len(held.payload), held.fid)
            held = None
        board.role(name, state="failed", holding_bytes=0, holding_identity=None)
        board.set_error({"role": name, "type": type(exc).__name__, "message": str(exc)})
        cancel.set()
        raise


# ---------------------------------------------------------------- router
def router_runner(
    source_channels: dict[str, BoundedChannel],
    worker_channels: list[BoundedChannel],
    budget: ByteBudget,
    board: StatusBoard,
    cancel: threading.Event,
    ready_cond: threading.Condition,
) -> None:
    name = "router"
    board.register_role(name)
    group = ChannelGroup(list(source_channels.values()), cancel, ready_cond)
    eos_remaining = len(source_channels)
    held: Frame | None = None
    try:
        board.role(name, state="running")
        while eos_remaining > 0:
            ch_name, frame = group.get_any(name)  # 多路接收两个 source
            if frame.is_eos:
                eos_remaining -= 1
                board.event("router.eos", from_channel=ch_name, remaining=eos_remaining)
                continue
            held = frame
            board.role(
                name,
                state="running",
                holding_bytes=len(frame.payload),
                holding_identity=frame.identity(),
            )
            # Python % 即数学模：负 key 也得到非负余数 0/1。
            target = frame.record["key"] % 2
            board.event("router.route", identity=frame.identity(), to=target)
            if not worker_channels[target].put(frame, name):  # 满则背压
                # 取消且未入队：帧仍归 router 持有，由引擎 drain 之外的此处兜底
                # （它已不在任何队列里），用 fid 幂等释放。
                budget.release(len(frame.payload), frame.fid)
                held = None
                raise CancelledError(f"{name} 取消于转发 {frame.identity()}")
            held = None
            board.role(name, holding_bytes=0, holding_identity=None)
        # 只有全部来源终止且输入全部转发完，才向每个 worker 发一个终止。
        for ch in worker_channels:
            if not ch.close(name):
                raise CancelledError(f"{name} 取消于发送 worker 终止")
        board.role(name, state="done")
        board.event("router.eos_forwarded")
    except CancelledError:
        if held is not None:
            # 仅当帧确定未入队（put 返回 False 路径已自行 release，正常不会到这）。
            budget.release(len(held.payload), held.fid)
            held = None
        board.role(name, state="cancelled", holding_bytes=0, holding_identity=None)
        raise
    except Exception as exc:
        board.role(name, state="failed", holding_bytes=0, holding_identity=None)
        board.set_error({"role": name, "type": type(exc).__name__, "message": str(exc)})
        cancel.set()
        raise


# ---------------------------------------------------------------- worker
def worker_runner(
    worker_id: int,
    inp: BoundedChannel,
    out: BoundedChannel,
    budget: ByteBudget,
    board: StatusBoard,
    cancel: threading.Event,
    hooks,
) -> None:
    name = f"worker-{worker_id}"
    board.register_role(name)
    held_input: Frame | None = None
    # inflight：已申请预算但尚未确认入队的输出帧，worker 拥有其 payload。
    # put 正常返回即从集合移除（所有权移交队列，由引擎 drain 释放）。
    inflight: set[Frame] = set()

    def held_bytes() -> int:
        return (len(held_input.payload) if held_input else 0) + sum(
            len(f.payload) for f in inflight
        )

    def release_inflight() -> None:
        for f in inflight:
            budget.release(len(f.payload), f.fid)
        inflight.clear()

    def sync_holding(state: str = "running") -> None:
        board.role(
            name,
            state=state,
            holding_bytes=held_bytes(),
            holding_identity=held_input.identity() if held_input else None,
        )

    try:
        board.role(name, state="running")
        while True:
            frame = inp.get(name)  # 仅一个来源通道，EOS 由 router 在排空后写入
            if frame.is_eos:
                board.role_bump(name, eos_seen=1)
                # 只有收到来源终止且此前属于自己的输入全部处理完，才发自己的终止。
                # EOS 与数据同队列排队，sink 侧终止不会越过此前任何输出帧。
                if not out.close(name):
                    raise CancelledError(f"{name} 取消于发送 sink 终止")
                board.role(name, state="done", holding_bytes=0, holding_identity=None)
                board.event("worker.eos", worker=worker_id)
                return
            held_input = frame
            board.role_bump(name, records_seen=1)
            # 取到输入帧的第一时间归还其 payload 预算：记录体 frame.record 仍在，
            # 展开只依赖结构化字段，不再需要原始字节。这样 worker 绝不会“占着输入
            # 字节等输出预算”，是打破预算/槽位循环等待的关键一步。
            record = frame.record
            sid, seq = record["source_id"], record["seq"]
            key, value, fanout = record["key"], record["value"], record["fanout"]
            budget.release(len(held_input.payload), held_input.fid)
            held_input = None
            sync_holding()

            if hooks is not None and hooks.worker_before_process is not None:
                barrier = hooks.worker_before_process(worker_id, frame)
                if isinstance(barrier, threading.Event):
                    while not barrier.is_set():
                        if cancel.is_set():
                            raise CancelledError(f"{name} 取消于测试屏障")
                        barrier.wait(timeout=0.05)

            for part_no in range(fanout):
                out_record = {
                    "source_id": sid,
                    "seq": seq,
                    "part_no": part_no,  # (source_id, seq, part_no) 即稳定结果身份
                    "key": key,
                    "value": value * value + part_no,
                }
                payload = json.dumps(out_record, separators=(",", ":")).encode()
                of = Frame.data(payload, out_record, name)
                # 资源偏序（打破预算/槽位循环等待）：
                # 1) 先申请 payload 预算——此刻不占用任何输出通道槽位；
                # 2) 预算到手后再等槽位并立即入队。
                # 预算不足时 worker 不占槽位，sink（不依赖预算、始终可运行）
                # 能持续排空在途帧释放预算；等槽位时持有的预算只能由 sink 排空
                # 来解锁，而 sink 不反向等待预算，因此不存在环。
                budget.acquire(len(payload), cancel, of.fid, honor_reserved=False)
                inflight.add(of)
                sync_holding()
                if not out.put(of, name):  # 满则背压，sink 排空后自然放行
                    raise CancelledError(f"{name} 取消于输出 {of.identity()}")
                inflight.discard(of)
                sync_holding()
                board.role_bump(name, outputs_emitted=1)

            board.role_bump(name, records_processed=1)  # fanout=0 也算已处理
            board.event("worker.processed", worker=worker_id, identity=f"{sid}:{seq}", fanout=fanout)
    except CancelledError:
        if held_input is not None:
            budget.release(len(held_input.payload), held_input.fid)
            held_input = None
        release_inflight()
        board.role(name, state="cancelled", holding_bytes=0, holding_identity=None)
        raise
    except Exception as exc:
        if held_input is not None:
            budget.release(len(held_input.payload), held_input.fid)
            held_input = None
        release_inflight()
        identity = frame.identity() if "frame" in locals() else None
        board.role(name, state="failed", holding_bytes=0, holding_identity=None)
        board.set_error(
            {
                "role": name,
                "type": type(exc).__name__,
                "message": str(exc),
                "input_identity": identity,
            }
        )
        cancel.set()
        raise


# ---------------------------------------------------------------- sink
def sink_runner(
    in_channels: list[BoundedChannel],
    output_path,
    budget: ByteBudget,
    board: StatusBoard,
    cancel: threading.Event,
    cfg,
    ready_cond: threading.Condition,
) -> None:
    name = "sink"
    board.register_role(name)
    group = ChannelGroup(in_channels, cancel, ready_cond)
    eos_remaining = len(in_channels)
    held: Frame | None = None
    retry: list[tuple[Frame, int]] = []  # (帧, 已尝试次数)，重试暂存，仍占 payload 预算
    written = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(output_path, "wb")

    def held_bytes() -> int:
        return (len(held.payload) if held else 0) + sum(len(f.payload) for f, _ in retry)

    def sync_holding(state: str = "running") -> None:
        board.role(name, state=state, holding_bytes=held_bytes())

    def wait_if_paused() -> None:
        pause = cfg.sink_pause
        if pause is None:
            return
        while pause.is_set():
            if cancel.is_set():
                raise CancelledError("sink 暂停期间收到取消")
            board.role(name, state="blocked_pause", holding_bytes=held_bytes())
            board.event("sink.paused", outputs_written=written)
            pause.wait(timeout=0.05)

    def write_frame(frame: Frame) -> bool:
        """返回 True 表示落盘成功；OSError 时转入/保留重试暂存并返回 False。

        成功时立即归还预算，保证“落盘”与“预算释放”之间没有取消点，
        杜绝同一帧被主循环与取消收尾重复释放。
        """
        nonlocal written
        try:
            if written + 1 in cfg.hooks.sink_fail_writes:
                cfg.hooks.sink_fail_writes.discard(written + 1)  # 只注入一次，重试即成功
                raise OSError(f"测试注入：第 {written + 1} 次写盘失败")
            fh.write(frame.payload + b"\n")
            fh.flush()
            if cfg.fsync_each_write:
                import os
                os.fsync(fh.fileno())
        except OSError:
            return False
        written += 1
        with board.lock:
            board.outputs_written = written
            board.bytes_on_disk = fh.tell() if hasattr(fh, "tell") else board.bytes_on_disk
        budget.release(len(frame.payload), frame.fid)  # 先释放：此后该帧不再属于任何在途容器
        board.event("sink.written", identity=frame.identity(), n=written)
        if cfg.hooks is not None and cfg.hooks.sink_on_written is not None:
            cfg.hooks.sink_on_written(written, frame)  # 钩子里取消也不会重复释放
        return True

    try:
        board.role(name, state="running")
        while eos_remaining > 0:
            wait_if_paused()  # 控制点：sink 暂停时不读取，上游自然背压

            if retry:
                frame, attempts = retry[0]
                if write_frame(frame):
                    # 预算已在 write_frame 内归还，这里只清暂存容器。
                    retry.pop(0)
                    board.retry_staging_remove(len(frame.payload))
                    sync_holding()
                else:
                    attempts += 1
                    retry[0] = (frame, attempts)
                    board.event("sink.retry", identity=frame.identity(), attempts=attempts)
                    if attempts >= 3:
                        raise OSError(f"帧 {frame.identity()} 连续 {attempts} 次写盘失败")
                    wait_if_paused()
                continue

            ch_name, frame = group.get_any(name)  # 多路接收两个 worker
            if frame.is_eos:
                eos_remaining -= 1
                board.event("sink.eos", from_channel=ch_name, remaining=eos_remaining)
                continue
            held = frame
            sync_holding()
            if not write_frame(frame):
                # 落盘失败：帧转入重试暂存，字节始终在预算内，不丢、不重复计数。
                retry.append((frame, 1))
                board.retry_staging_add(len(frame.payload))
                board.event("sink.staged", identity=frame.identity())
            # 成功时 write_frame 已释放预算；失败时帧已移交 retry。两种情况 held 都清空。
            held = None
            sync_holding()

        # 收到全部 worker 终止且此前输出全部排空（含重试暂存）才可声明完整。
        assert not retry and held is None
        fh.flush()
        bytes_on_disk = fh.tell()
        fh.close()
        marker = output_path.with_suffix(output_path.suffix + ".complete")
        with open(marker, "w", encoding="utf-8") as mf:
            json.dump({"outputs": written, "bytes": bytes_on_disk}, mf)
        board.role(name, state="done", holding_bytes=0)
        board.event("sink.complete", outputs=written)
    except CancelledError:
        if held is not None:
            budget.release(len(held.payload), held.fid)
            held = None
        for f, _ in retry:
            budget.release(len(f.payload), f.fid)
        retry.clear()
        board.role(name, state="cancelled", holding_bytes=0)
        raise
    except Exception as exc:
        if held is not None:
            budget.release(len(held.payload), held.fid)
            held = None
        for f, _ in retry:
            budget.release(len(f.payload), f.fid)
        retry.clear()
        board.role(name, state="failed", holding_bytes=0)
        board.set_error({"role": name, "type": type(exc).__name__, "message": str(exc)})
        cancel.set()
        raise
    finally:
        if not fh.closed:
            fh.close()

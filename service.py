"""本地共享查询结果服务（Python 3.11 + 标准库 sqlite3，单进程多线程）。

设计要点
========
* 一个规范键（快照摘要 + query_id + 带类型参数）对应一个 PhysicalQuery（物理执行），
  多个消费者共享同一次真实 sqlite 游标执行；结果写入受管 spill 文件，所有消费者从
  自己的独立偏移顺序重放，不共享会被竞争消费的迭代器。
* 准入按目录 max_encoded_bytes 预留 spool 容量；预留即占用，实际写入不再二次计入。
  pool_bytes 是合计上限（正在写入 / 已完成保留 / 已取消待清理的预留都计入）。
* 物理查询行数/编码字节上限在写入前判定，超限把该物理查询整体置 FAILED 并保留
  已写前缀，绝不先发布成功再补错。
* 驻留行缓冲至多 resident_rows（16）条，其余只在 spill 文件中。
* 所有注册表/状态变更在单一 Condition 锁内线性化；取消与新订阅的交错有确定顺序，
  新订阅者绝不会接到已撤销（CANCELLING）的执行上。
* threading.Event 屏障通过 Hooks 注入：首批前缀写出前、最后消费者退出前、终态发布前。
"""
from __future__ import annotations

import contextlib
import hashlib
import os
import pickle
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO

import sqlite3

from catalog import Catalog

FRAME_HEADER = 4  # 每帧 4 字节大端长度前缀
FETCH_BATCH = 32  # 游标每批 fetchmany 的行数
BARRIER_TIMEOUT = 60.0


def file_sha256(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    """以只读模式直连快照文件，禁止任何修改源库的尝试。"""
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True, check_same_thread=False)


def _encoded_row(row: tuple) -> bytes:
    """行的标准编码（pickle 协议 5），编码字节数即 spool 容量计量口径。"""
    return pickle.dumps(row, protocol=5)


class Hooks:
    """屏障注入点。默认全部 event 已 set（不阻塞）；演示时按需 clear 特定事件。"""

    def __init__(self) -> None:
        self.before_run = threading.Event()        # 物理执行开始前（确定性制造 PENDING）
        self.first_prefix = threading.Event()      # 首批前缀写出前
        self.last_consumer_exit = threading.Event()  # 最后消费者退出（取消）前
        self.before_terminal = threading.Event()   # 查询终态发布前
        # threading.Event 默认未 set；屏障语义要求默认放行，需要阻塞时由演示端 clear
        self.reset_all_set()

    def reset_all_set(self) -> None:
        """全部屏障恢复为默认放行状态。"""
        self.before_run.set()
        self.first_prefix.set()
        self.last_consumer_exit.set()
        self.before_terminal.set()

    def barrier(self, label: str, event: threading.Event,
                cond: threading.Condition) -> None:
        """在 Condition 锁内等待屏障：用 wait_for 临时释放锁，避免堵死其他 submitter。

        演示端 fire 时先 event.set() 再 cond.notify_all() 唤醒等待者。
        """
        if event.is_set():
            return
        if not cond.wait_for(event.is_set, timeout=BARRIER_TIMEOUT):
            raise RuntimeError(f"屏障 {label} 等待超时")


def fire(event: threading.Event, cond: threading.Condition) -> None:
    """释放一个屏障并唤醒在其上等待的工作线程。"""
    event.set()
    with cond:
        cond.notify_all()


# 物理查询生命周期
PENDING = "PENDING"        # 已建立，游标尚未执行
RUNNING = "RUNNING"        # 游标执行中，已有/可能已有前缀
CANCELLING = "CANCELLING"  # 最后消费者取消，正在撤销，新订阅必须等待清理
SUCCESS = "SUCCESS"
FAILED = "FAILED"
CANCELLED = "CANCELLED"

# 消费者终态
T_SUCCESS = "SUCCESS"
T_FAILED = "FAILED"
T_CANCELLED = "CANCELLED"
T_TIMEOUT = "TIMEOUT"
T_SERVICE_CLOSED = "SERVICE_CLOSED"


@dataclass
class ReadResult:
    rows: list
    state: str                    # CONTINUE / SUCCESS / FAILED / CANCELLED / TIMEOUT
    row_offset: int               # 本次读取后该消费者的下一行偏移
    reason: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state != "CONTINUE"


@dataclass
class ConsumerRecord:
    consumer_id: str
    key: str
    gen: int
    offset: int = 0
    created_at: float = field(default_factory=time.time)
    terminal_state: str | None = None
    terminal_reason: str | None = None
    reads: int = 0
    rows_delivered: int = 0
    ended_at: float | None = None


class PhysicalQuery:
    """一次真实 SQLite 执行及其可重放前缀（尾部驻留 + spill 文件）。"""

    def __init__(self, service: "QueryService", key: str, gen: int, qid: str,
                 spec: dict, params: tuple, spill_path: Path):
        self.svc = service
        self.key = key
        self.gen = gen
        self.qid = qid
        self.spec = spec
        self.params = params
        self.spill_path = spill_path
        self.state = PENDING
        self.failure_reason: str | None = None
        self.consumer_ids: set[str] = set()
        self.active_reads = 0
        self.created_at = time.time()
        self.terminal_at: float | None = None

        # 执行统计
        self.execute_count = 0
        self.fetch_count = 0

        # 容量 / spool 统计
        self.reserved_bytes = spec["max_encoded_bytes"]
        self.actual_encoded_bytes = 0   # 只累计行负载编码字节（与上限同口径）
        self.file_bytes = 0             # spill 文件实际字节（含帧头）
        self.total_rows = 0

        # 可重放前缀：索引 + 至多 resident_rows 条的尾部驻留缓冲
        self.row_index: list[tuple[int, int]] = []   # (文件偏移, 帧长度)
        self.tail: deque = deque()
        self._fh: IO[bytes] | None = None
        self._file_lock = threading.Lock()
        self.thread = threading.Thread(
            target=self._run, name=f"pq-{qid}-{gen}", daemon=True
        )

    # ---- 内部工具（均在服务 Condition 锁内或串行工作线程上使用） ----

    def _open_for_write(self) -> None:
        # r+b 模式句柄在物理查询整个生命周期内同时承担追加写与随机重放读，
        # 只在取消/淘汰/服务关闭时关闭；工作线程结束不关句柄。
        self._fh = open(self.spill_path, "w+b")

    def _append_row_locked(self, row: tuple) -> str | None:
        """写一行到 spill；返回失败原因（超限）或 None。行/字节上限在写入前判定。"""
        if self.total_rows >= self.spec["max_rows"]:
            return (f"行数上限 {self.spec['max_rows']} 已达到，"
                    f"物理查询 {self.qid} 判定失败")
        payload = _encoded_row(row)
        if self.actual_encoded_bytes + len(payload) > self.spec["max_encoded_bytes"]:
            return (f"编码字节上限 {self.spec['max_encoded_bytes']} 会被超过，"
                    f"物理查询 {self.qid} 判定失败")
        frame = len(payload).to_bytes(FRAME_HEADER, "big") + payload
        with self._file_lock:
            pos = self.file_bytes
            self._fh.write(frame)
            self._fh.flush()
        self.file_bytes += len(frame)
        self.actual_encoded_bytes += len(payload)
        self.row_index.append((pos, len(frame)))
        self.tail.append(row)
        if len(self.tail) > self.svc.resident_rows:
            self.tail.popleft()
        self.total_rows += 1
        return None

    def _materialize_locked(self, idx: int) -> tuple:
        """按全局行号取行：驻留尾部命中则用内存，否则从 spill 随机读取。"""
        tail_start = self.total_rows - len(self.tail)
        if idx >= tail_start:
            return self.tail[idx - tail_start]
        pos, frame_len = self.row_index[idx]
        with self._file_lock:
            self._fh.seek(pos)
            frame = self._fh.read(frame_len)
        if len(frame) != frame_len:
            raise RuntimeError("spill 帧读取不完整")
        return pickle.loads(frame[FRAME_HEADER:])

    # ---- 工作线程：唯一的真实 SQL 执行路径 ----

    def _run(self) -> None:
        svc = self.svc
        hooks = svc.hooks
        conn: sqlite3.Connection | None = None
        pending_failure: str | None = None
        try:
            with svc.cond:
                if self.state == CANCELLING:          # 执行未开始即被取消
                    self._finish_cancel_locked()
                    return
                hooks.barrier("before_run", hooks.before_run, svc.cond)
                if self.state == CANCELLING:
                    self._finish_cancel_locked()
                    return
                self._open_for_write()
                self.state = RUNNING

            # 真实游标在服务锁外执行；目录白名单 SQL + 只读连接保证安全
            conn = connect_readonly(svc.snapshot_path)
            cur = conn.cursor()
            cur.execute(self.spec["sql"], self.params)
            with svc.cond:
                self.execute_count += 1
            while True:
                batch = cur.fetchmany(FETCH_BATCH)
                with svc.cond:
                    self.fetch_count += 1
                    if self.state == CANCELLING:
                        self._finish_cancel_locked()
                        with contextlib.suppress(Exception):
                            conn.close()
                        return
                    if not batch:
                        break
                    # 首批前缀写出前屏障：演示迟到订阅者从 0 行重放
                    hooks.barrier("first_prefix", hooks.first_prefix, svc.cond)
                    for row in batch:
                        reason = self._append_row_locked(row)
                        if reason is not None:
                            pending_failure = reason
                            break
                    svc.cond.notify_all()
                    if pending_failure:
                        break
            with svc.cond:
                if self.state == CANCELLING:
                    self._finish_cancel_locked()
                    return
                target = FAILED if pending_failure else SUCCESS
                self.failure_reason = pending_failure
                # 终态发布前屏障：演示终态与迟到订阅的收敛
                hooks.barrier("before_terminal", hooks.before_terminal, svc.cond)
                self._publish_terminal_locked(target)
        except Exception as exc:  # noqa: BLE001 - 任何执行异常都要一致地广播给订阅者
            with svc.cond:
                if self.state == CANCELLING:
                    self._finish_cancel_locked()
                    return
                self.failure_reason = f"SQLite 执行异常: {type(exc).__name__}: {exc}"
                hooks.barrier("before_terminal", hooks.before_terminal, svc.cond)
                self._publish_terminal_locked(FAILED)
        finally:
            if conn is not None:
                with contextlib.suppress(Exception):
                    conn.close()
            # 注意：不在此关闭 self._fh —— 终态后的消费者仍要通过它随机重放，
            # 句柄由 _finish_cancel_locked / evict / close 统一回收。

    def _publish_terminal_locked(self, target: str) -> None:
        """状态与终态时间戳一次性发布；没有消费者的已完成查询等待淘汰。"""
        self.state = target
        self.terminal_at = time.time()
        self.svc.cond.notify_all()

    def _finish_cancel_locked(self) -> None:
        """撤销物理查询：关闭并删除 spill、释放预留、移出注册表。"""
        self.state = CANCELLED
        self.terminal_at = time.time()
        self.failure_reason = "最后消费者取消，物理查询已撤销"
        if self._fh is not None:
            with contextlib.suppress(Exception):
                self._fh.close()
            self._fh = None
        with contextlib.suppress(FileNotFoundError):
            os.remove(self.spill_path)
        self.svc.reserved_total -= self.reserved_bytes
        self.svc.spill_files.pop(self.key, None)
        if self.svc.queries.get(self.key) is self:
            del self.svc.queries[self.key]
        self.svc.cond.notify_all()

    # ---- 消费者读取（在服务 Condition 锁内完成状态判定与行重放） ----

    def read_locked(self, rec: ConsumerRecord, max_rows: int,
                    timeout: float) -> ReadResult:
        self.active_reads += 1
        try:
            deadline = time.time() + timeout
            while True:
                # 先交付连续前缀片段
                available = self.total_rows - rec.offset
                if available > 0:
                    n = min(max_rows, available)
                    rows = [self._materialize_locked(rec.offset + i) for i in range(n)]
                    rec.offset += n
                    rec.reads += 1
                    rec.rows_delivered += n
                    if self.state in (SUCCESS, FAILED, CANCELLED, CANCELLING):
                        if rec.offset >= self.total_rows:
                            return self._terminal_result(rec, rows)
                    return ReadResult(rows, "CONTINUE", rec.offset)

                # 无可用行：已终态则给出明确终态，否则等待新前缀
                if self.state in (SUCCESS, FAILED, CANCELLED, CANCELLING):
                    return self._terminal_result(rec, [])
                remaining = deadline - time.time()
                if remaining <= 0:
                    return ReadResult([], "TIMEOUT", rec.offset, "等待新前缀超时")
                self.svc.cond.wait(timeout=remaining)
        finally:
            self.active_reads -= 1

    def _terminal_result(self, rec: ConsumerRecord, rows: list) -> ReadResult:
        if self.state == SUCCESS:
            state, reason = T_SUCCESS, None
        elif self.state == FAILED:
            state, reason = T_FAILED, self.failure_reason
        else:  # CANCELLED / CANCELLING 对消费者呈现为取消终态
            state, reason = T_CANCELLED, self.failure_reason
        rec.terminal_state = state
        rec.terminal_reason = reason
        rec.ended_at = time.time()
        # 自然终态：立即从活跃表移入历史，终态不再被后续 cancel 覆盖
        if rec.consumer_id in self.svc.consumers:
            del self.svc.consumers[rec.consumer_id]
            self.svc.history[rec.consumer_id] = rec
            self.consumer_ids.discard(rec.consumer_id)
        return ReadResult(rows, state, rec.offset, reason)


class QueryService:
    """共享查询服务：submit / read / cancel / status / evict 均为线程安全 API。"""

    def __init__(self, snapshot_path: str | Path, catalog: Catalog,
                 pool_bytes: int, resident_rows: int = 16,
                 max_consumers: int = 6, spill_dir: str | Path | None = None,
                 hooks: Hooks | None = None):
        self.snapshot_path = Path(snapshot_path)
        self.snapshot_digest = file_sha256(self.snapshot_path)
        self.catalog = catalog
        self.pool_bytes = int(pool_bytes)
        self.resident_rows = int(resident_rows)
        self.max_consumers = int(max_consumers)
        self.spill_dir = Path(spill_dir or (Path(__file__).parent / "spill"))
        self.spill_dir.mkdir(parents=True, exist_ok=True)
        self.hooks = hooks or Hooks()

        self.cond = threading.Condition()
        self.queries: dict[str, PhysicalQuery] = {}   # 规范键 -> 当前物理查询
        self.consumers: dict[str, ConsumerRecord] = {}
        self.history: dict[str, ConsumerRecord] = {}
        self.spill_files: dict[str, Path] = {}
        self.reserved_total = 0                        # 已接受查询的预留合计
        self._consumer_seq = 0
        self._gen_seq = 0
        self.closed = False

    def replace_hooks(self, hooks: Hooks) -> None:
        """切换屏障集。注意：已在旧屏障上等待的工作线程仍引用旧对象，
        因此只能在没有 RUNNING/PENDING/CANCELLING 查询时切换。"""
        with self.cond:
            busy = [p.key for p in self.queries.values()
                    if p.state in (PENDING, RUNNING, CANCELLING)]
            if busy:
                raise RuntimeError(f"存在未终态物理查询，不能切换屏障: {busy}")
            self.hooks = hooks

    # ---------------- submit ----------------

    def submit(self, qid: str, params) -> dict:
        """提交（或收敛到）一个物理查询，返回独立 consumer_id。"""
        spec = self.catalog.validate(qid, params)
        params = tuple(params)
        key = Catalog.canonical_key(
            self.snapshot_digest, qid, params, spec["types"]
        )
        with self.cond:
            if self.closed:
                return {"accepted": False, "reason": "服务已关闭", "key": key}
            if len(self.consumers) >= self.max_consumers:
                return {"accepted": False,
                        "reason": f"活跃消费者上限 {self.max_consumers} 已达",
                        "key": key}

            # 线性化点 1：撤销中的查询必须先完成清理，新订阅绝不接到已撤销执行上
            existing = self.queries.get(key)
            while existing is not None and existing.state == CANCELLING:
                self.cond.wait()
                existing = self.queries.get(key)

            if existing is not None:
                pq = existing
            else:
                # 线性化点 2：准入按最大 spool 容量预留；预留即占用，不与实际双计
                if self.reserved_total + spec["max_encoded_bytes"] > self.pool_bytes:
                    return {
                        "accepted": False,
                        "reason": (f"pool 容量不足：已预留 {self.reserved_total} + 申请 "
                                   f"{spec['max_encoded_bytes']} > {self.pool_bytes}"),
                        "key": key,
                        "reserved_total": self.reserved_total,
                    }
                self._gen_seq += 1
                tag = hashlib.sha256(key.encode()).hexdigest()[:16]
                spill_path = self.spill_dir / f"pq-{tag}-gen{self._gen_seq}.spill"
                pq = PhysicalQuery(self, key, self._gen_seq, qid, spec, params,
                                   spill_path)
                self.queries[key] = pq
                self.spill_files[key] = spill_path
                self.reserved_total += spec["max_encoded_bytes"]
                pq.thread.start()

            self._consumer_seq += 1
            cid = f"c{self._consumer_seq:04d}"
            rec = ConsumerRecord(cid, key, pq.gen)
            self.consumers[cid] = rec
            pq.consumer_ids.add(cid)
            return {
                "accepted": True,
                "consumer_id": cid,
                "key": key,
                "gen": pq.gen,
                "state": pq.state,
                "execute_count": pq.execute_count,
            }

    # ---------------- read ----------------

    def read(self, consumer_id: str, max_rows: int, timeout: float = 30.0) -> ReadResult:
        if max_rows <= 0:
            raise ValueError("max_rows 必须为正整数")
        with self.cond:
            if self.closed:
                return ReadResult([], T_SERVICE_CLOSED, 0, "服务已关闭")
            rec = self.consumers.get(consumer_id)
            if rec is None:
                rec = self.history.get(consumer_id)
                if rec is None:
                    return ReadResult([], T_CANCELLED, 0, "未知 consumer_id")
                return ReadResult([], rec.terminal_state or T_CANCELLED,
                                  rec.offset, rec.terminal_reason)
            pq = self.queries.get(rec.key)
            if pq is None:
                # 物理查询已被撤销/淘汰，记录终态
                rec.terminal_state = rec.terminal_state or T_CANCELLED
                rec.terminal_reason = rec.terminal_reason or "物理查询已被回收"
                rec.ended_at = rec.ended_at or time.time()
                return ReadResult([], rec.terminal_state, rec.offset, rec.terminal_reason)
            return pq.read_locked(rec, max_rows, timeout)

    # ---------------- cancel ----------------

    def cancel(self, consumer_id: str) -> dict:
        """取消仅解除自身订阅；最后一个消费者退出时撤销物理查询。

        已自然到达 SUCCESS/FAILED 终态的消费者保留原终态，cancel 仅做幂等清理。
        """
        with self.cond:
            rec = self.consumers.pop(consumer_id, None)
            if rec is None:
                old = self.history.get(consumer_id)
                return {"cancelled": old is not None,
                        "consumer_id": consumer_id,
                        "terminal_state": old.terminal_state if old else None,
                        "note": "消费者已处于终态/已取消（幂等）"}
            if rec.terminal_state in (T_SUCCESS, T_FAILED):
                # 已自然终态：保留原终态与原因，只完成注册表迁移
                self.history[consumer_id] = rec
                pq = self.queries.get(rec.key)
                if pq is not None:
                    pq.consumer_ids.discard(consumer_id)
                return {"cancelled": True, "consumer_id": consumer_id,
                        "physical": "already_terminal",
                        "terminal_state": rec.terminal_state}
            rec.terminal_state = T_CANCELLED
            rec.terminal_reason = "客户端主动取消"
            rec.ended_at = time.time()
            self.history[consumer_id] = rec

            pq = self.queries.get(rec.key)
            if pq is None or pq is not self.queries.get(rec.key):
                return {"cancelled": True, "consumer_id": consumer_id,
                        "physical": "already_reclaimed"}
            pq.consumer_ids.discard(consumer_id)

            if not pq.consumer_ids and pq.state in (PENDING, RUNNING):
                # 最后消费者退出前屏障；等待期间锁被释放，新订阅者可能加入，
                # 因此屏障返回后必须在同一把锁内重新线性化检查。
                self.hooks.barrier("last_consumer_exit",
                                   self.hooks.last_consumer_exit, self.cond)
                if pq.consumer_ids or pq.state not in (PENDING, RUNNING):
                    # 交错期间有新订阅者接入：撤销作废，物理执行继续
                    return {"cancelled": True, "consumer_id": consumer_id,
                            "physical": "retained_after_barrier",
                            "remaining_consumers": len(pq.consumer_ids),
                            "gen": pq.gen}
                pq.state = CANCELLING
                self.cond.notify_all()
                # 工作线程若阻塞在屏障上，屏障 set 后自行走 _finish_cancel_locked；
                # 若恰好卡在 fetch（锁外），下一批 fetch 回来检测 CANCELLING 后清理。
                return {"cancelled": True, "consumer_id": consumer_id,
                        "physical": "cancelling", "gen": pq.gen}
            return {"cancelled": True, "consumer_id": consumer_id,
                    "physical": "retained",
                    "remaining_consumers": len(pq.consumer_ids),
                    "gen": pq.gen}

    # ---------------- evict ----------------

    def evict(self) -> dict:
        """确定淘汰：回收最老的、已终态且无消费者、无活跃读取的物理查询。"""
        with self.cond:
            candidates = [
                pq for pq in self.queries.values()
                if pq.state in (SUCCESS, FAILED)
                and not pq.consumer_ids and pq.active_reads == 0
            ]
            if not candidates:
                return {"evicted": False,
                        "reason": "没有可淘汰的已完成无消费者查询（活跃读者受保护）"}
            pq = min(candidates, key=lambda p: (p.terminal_at or 0, p.gen))
            if pq._fh is not None:
                with contextlib.suppress(Exception):
                    pq._fh.close()
                pq._fh = None
            with contextlib.suppress(FileNotFoundError):
                os.remove(pq.spill_path)
            self.reserved_total -= pq.reserved_bytes
            self.spill_files.pop(pq.key, None)
            del self.queries[pq.key]
            self.cond.notify_all()
            return {"evicted": True, "key": pq.key, "gen": pq.gen,
                    "state": pq.state, "released_bytes": pq.reserved_bytes,
                    "reserved_total": self.reserved_total}

    # ---------------- status / audit / close ----------------

    def status(self) -> dict:
        with self.cond:
            return {
                "snapshot_digest": self.snapshot_digest,
                "pool_bytes": self.pool_bytes,
                "resident_rows_limit": self.resident_rows,
                "max_consumers": self.max_consumers,
                "reserved_total": self.reserved_total,
                "spill_file_count": len(self.spill_files),
                "closed": self.closed,
                "physical_queries": [
                    {
                        "key": pq.key,
                        "gen": pq.gen,
                        "query_id": pq.qid,
                        "state": pq.state,
                        "execute_count": pq.execute_count,
                        "fetch_count": pq.fetch_count,
                        "rows": pq.total_rows,
                        "reserved_bytes": pq.reserved_bytes,
                        "actual_encoded_bytes": pq.actual_encoded_bytes,
                        "file_bytes": pq.file_bytes,
                        "resident_rows": len(pq.tail),
                        "consumers": sorted(pq.consumer_ids),
                        "active_reads": pq.active_reads,
                        "failure_reason": pq.failure_reason,
                    }
                    for pq in sorted(self.queries.values(), key=lambda p: p.gen)
                ],
                "active_consumers": [
                    {"consumer_id": cid, "key": rec.key, "gen": rec.gen,
                     "offset": rec.offset, "rows_delivered": rec.rows_delivered,
                     "reads": rec.reads}
                    for cid, rec in sorted(self.consumers.items())
                ],
                "consumer_terminals": [
                    {"consumer_id": cid, "gen": rec.gen,
                     "terminal_state": rec.terminal_state,
                     "terminal_reason": rec.terminal_reason,
                     "rows_delivered": rec.rows_delivered}
                    for cid, rec in sorted(self.history.items())
                ],
            }

    def audit(self) -> dict:
        """资源与不变量审计；任何不变量被破坏都会在 violations 中列出。"""
        with self.cond:
            violations: list[str] = []
            reserved_sum = sum(pq.reserved_bytes for pq in self.queries.values())
            if reserved_sum != self.reserved_total:
                violations.append(
                    f"预留合计不一致: 注册表 {reserved_sum} != 账面 {self.reserved_total}")
            if self.reserved_total > self.pool_bytes:
                violations.append(
                    f"预留 {self.reserved_total} 超过 pool {self.pool_bytes}")
            for pq in self.queries.values():
                if len(pq.tail) > self.resident_rows:
                    violations.append(f"{pq.qid} gen{pq.gen} 驻留行超过 "
                                      f"{self.resident_rows}")
                if pq.actual_encoded_bytes > pq.reserved_bytes:
                    violations.append(f"{pq.qid} gen{pq.gen} 实际编码字节超过预留")
                if pq.total_rows > pq.spec["max_rows"]:
                    violations.append(f"{pq.qid} gen{pq.gen} 行数超过上限")
                if pq.state in (SUCCESS, FAILED):
                    if not pq.spill_path.exists() and pq.total_rows > 0:
                        violations.append(f"{pq.qid} gen{pq.gen} 终态 spill 文件缺失")
                if pq.state in (PENDING, RUNNING, CANCELLING) and pq.terminal_at:
                    violations.append(f"{pq.qid} gen{pq.gen} 非终态却有终态时间戳")
            # 预留口径：文件实际尺寸不另计容量，防止“新文件还很小”被误当可用空间
            disk = {p.name: p.stat().st_size for p in self.spill_dir.glob("*.spill")}
            return {
                "reserved_total": self.reserved_total,
                "pool_bytes": self.pool_bytes,
                "reserved_sum_by_registry": reserved_sum,
                "spill_files_on_disk": disk,
                "active_consumer_count": len(self.consumers),
                "violations": violations,
                "ok": not violations,
            }

    def close(self) -> None:
        with self.cond:
            self.closed = True
            self.cond.notify_all()
        # 工作线程是守护线程；锁外清理文件，避免在 Condition 内做重 I/O
        for pq in list(self.queries.values()):
            pq.thread.join(timeout=5)
        with self.cond:
            for pq in list(self.queries.values()):
                if pq._fh is not None:
                    with contextlib.suppress(Exception):
                        pq._fh.close()
                with contextlib.suppress(FileNotFoundError):
                    os.remove(pq.spill_path)
            self.queries.clear()
            self.spill_files.clear()
            self.reserved_total = 0

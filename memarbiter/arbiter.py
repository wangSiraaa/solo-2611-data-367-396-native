"""共享记录槽仲裁器。

记账模型（任何时刻，对每个查询 q，恒等式严格成立）：
    held[q] = reserved[q] + resident[q] + pinned[q] + outbuf[q]
        reserved —— 已授予但工作线程尚未挂到具体记录/输出上的槽
        resident —— 驻留在 SortBuffer 中、可撤销（溢写落盘即释放）的记录槽
        pinned   —— 正在 pin 区写入、完成前物理上不可撤销的记录槽
        outbuf   —— 输出结果缓冲占用的记录槽
    全局守恒：sum(held[q]) + free == global_slots，held 总量永不超池。

held 只在授予（grant）与释放（spill_end/release_*/discard）时增量变动，
绝不从缓冲区快照反推，因此"授予已保留未使用"的槽不会被抹掉。

准入策略（可证明进展，不追求最优公平）：
  * 请求整批、全有或全无；按到达顺序进入 FIFO，队首不满足则等待，
    队首之后的请求绝不插队——固定优先级无法制造永久饿死；
  * 每个已注册查询有软下限 minimum（m）。空闲时保持；队首大请求需要槽时，
    按轮转（rotating victim，避免固定偏爱）要求其他查询先让出高于下限的
    可撤销槽，必要时把下限临时压到 0；
  * 撤销只是发指令，真正释放发生在设施 spill() 真实写盘返回之后；
  * pin 中的槽不可撤销，等其写完成再由同一个落盘路径释放；
  * 队首查询自己也可能持有可撤销驻留槽，其工作线程在申请前/等待检查点
    会自行溢放，因此不需要等待别人即可取得进展；
  * pin 区不接受新申请，设施写完成不以新 grant 为前提。
"""
import json
import threading
from dataclasses import dataclass
from pathlib import Path


class ArbiterError(RuntimeError):
    """仲裁器使用错误（非法请求等）。"""


@dataclass
class _QState:
    qid: str
    minimum: int
    held: int = 0
    reserved: int = 0     # 已授予未挂载
    pinned: int = 0       # pin 区，不可撤销
    resident: int = 0     # 驻留可撤销记录
    outbuf: int = 0       # 输出缓冲
    floor: int = 0        # 当前撤销下限
    waiting_n: int | None = None
    waiting_kind: str | None = None   # input / output
    order: int = 0
    cancelled: bool = False
    failed: bool = False
    done: bool = False

    @property
    def revocable(self):
        """非 pin 槽均可撤销：保留槽直接还，驻留槽溢写后还。"""
        return self.held - self.pinned


class EventLog:
    """追加式事件日志；每条事件带单调序号与线程名，供离线审计重放。

    可注册仅内存的监听器（不写盘），用于确定性测试在具体事件发生时
    set threading.Event，避免任何轮询/sleep。
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._seq = 0
        self._fh = open(self.path, "w", encoding="utf-8")
        self._listeners = []

    def add_listener(self, fn):
        with self._lock:
            self._listeners.append(fn)

    def emit(self, event_kind: str, qid: str | None = None, **fields):
        with self._lock:
            self._seq += 1
            event = {"seq": self._seq, "kind": event_kind,
                     "qid": qid, **fields}
            self._fh.write(json.dumps(event, ensure_ascii=False) + "\n")
            self._fh.flush()
            listeners = list(self._listeners)
        for fn in listeners:       # 锁外回调，避免与仲裁器锁嵌套
            fn(event)
        return self._seq

    def close(self):
        with self._lock:
            if not self._fh.closed:
                self._fh.close()

    @staticmethod
    def read(path: str | Path):
        events = []
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    events.append(json.loads(line))
        return events


class SlotArbiter:
    def __init__(self, global_slots: int, minimum_per_query: int, log: EventLog):
        self.pool = int(global_slots)
        self.minimum = int(minimum_per_query)
        self.log = log
        self._lock = threading.Condition()
        self._states: dict[str, _QState] = {}
        self._free = self.pool
        self._counter = 0          # FIFO 序号发生器
        self._victim_cursor = 0    # 轮转受害者指针
        self._qids: list[str] = []
        self._generation = 0       # 状态代：每次可能解除等待的变化都 +1

    # ------------------------------------------------------------------ 注册
    def register(self, qid: str):
        with self._lock:
            if qid in self._states:
                raise ArbiterError(f"query {qid} already registered")
            self._counter += 1
            st = _QState(qid=qid, minimum=self.minimum,
                         floor=self.minimum, order=self._counter)
            self._states[qid] = st
            self._qids.append(qid)
            self.log.emit("query_register", qid, minimum=self.minimum)

    @property
    def free_slots(self):
        with self._lock:
            return self._free

    def snapshot(self):
        """供测试/审计观察的一致性快照（锁内取值）。"""
        with self._lock:
            return {
                "pool": self.pool,
                "free": self._free,
                "queries": {
                    qid: {
                        "held": s.held, "reserved": s.reserved,
                        "pinned": s.pinned, "resident": s.resident,
                        "outbuf": s.outbuf, "floor": s.floor,
                        "waiting": s.waiting_n,
                        "cancelled": s.cancelled, "failed": s.failed,
                        "done": s.done,
                    }
                    for qid, s in self._states.items()
                },
            }

    # ------------------------------------------------------------- 内部工具
    def _bump_locked(self):
        """推进状态代并唤醒所有等待者（调用方持锁）。

        代计数把"谓词判断 + 进入等待"变为原子的谓词循环：任何在最后一次
        poll 之后发生的状态变化都会提升代，wait_event 据此不会漏唤醒。
        """
        self._generation += 1
        self._lock.notify_all()

    def _head(self):
        waiting = [s for s in self._states.values()
                   if s.waiting_n is not None and not s.done]
        return min(waiting, key=lambda s: s.order) if waiting else None

    def _replan_locked(self):
        """状态变化后重新计算撤销指令与可满足的队首授予。幂等。"""
        head = self._head()
        # 先把所有存活者的下限恢复到软下限，再按需逐步下压，
        # 撤销量始终"刚好"，不做永久压制。
        for s in self._states.values():
            if not s.done:
                s.floor = s.minimum
        if head is None:
            return
        need = head.waiting_n
        n = len(self._qids)
        order = [self._qids[(self._victim_cursor + i) % n]
                 for i in range(n)]
        for qid in order:
            if self._free >= need:
                break
            s = self._states[qid]
            if s is head or s.done:
                continue
            # 输出缓冲只能由其输出循环在成对生命周期内释放；作为受害者时
            # 不把 outbuf 计入可回收，避免等待新块期间当前块被双重归还。
            protected = s.outbuf if s.waiting_kind == "output" else 0
            revocable_now = s.held - s.pinned - protected
            reclaimable = max(0, revocable_now - s.floor)
            if reclaimable == 0 and s.floor > 0 and revocable_now > 0:
                # 软下限内仍不足：临时把下限压到 0（运行段已在盘上，
                # 查询可在后续检查点重新申请）。
                s.floor = 0
                self.log.emit("floor_lowered", qid, floor=0,
                              reason=f"head={head.qid} need={need}")
                reclaimable = max(0, revocable_now - s.floor)
            if reclaimable > 0:
                target_n = min(reclaimable, need - self._free)
                self.log.emit("revoke_request", qid, slots=target_n,
                              floor=s.floor, reason=f"head={head.qid}")
        self._victim_cursor = (self._victim_cursor + 1) % max(n, 1)
        # 队首现在可满足则整批授予（全有或全无，绝不部分授予）。
        if self._free >= need and not head.cancelled and not head.failed:
            self._free -= need
            head.held += need
            head.reserved += need   # 先记为保留槽，待 consume/输出时挂载
            head.waiting_n = None
            head.waiting_kind = None
            head.floor = max(head.floor, head.minimum)
            self.log.emit("grant", head.qid, slots=need,
                          held=head.held, free=self._free)
            self._bump_locked()

    # ------------------------------------------------------------- 轮询点
    def poll_terminal(self, qid: str):
        """只查询终态指令（取消/失败），不响应撤销。

        用于"授予已保留、即将 consume"这类不能让出当前批槽的点。
        """
        with self._lock:
            s = self._states[qid]
            if s.cancelled:
                return "cancelled"
            if s.failed:
                return "failed"
            return "ok"

    def poll_idle(self, qid: str):
        """空闲检查点：只处理别人请求引发的撤销/终态指令。

        输出缓冲由输出循环在成对生命周期内自行管理，不计入这里的撤销。
        """
        with self._lock:
            s = self._states[qid]
            if s.cancelled:
                return "cancelled"
            if s.failed:
                return "failed"
            protected = s.outbuf if s.waiting_kind == "output" else 0
            if s.held - s.pinned - protected - s.floor > 0:
                return "spill"
            return "wait"

    def poll_waiting(self, qid: str):
        """请求等待中的检查点：返回 granted/spill/wait/cancelled/failed。"""
        with self._lock:
            s = self._states[qid]
            if s.cancelled:
                return "cancelled"
            if s.failed:
                return "failed"
            if s.waiting_n is None:
                # 授予由某次 replan 完成：明确报告 granted。
                return "granted"
            self._replan_locked()
            if s.cancelled:
                return "cancelled"
            if s.waiting_n is None:
                return "granted"
            # 输入等待：队首自身持有的非 pin 槽先自行让出。
            # 输出等待：当前 outbuf 受保护，只能等别人释放。
            if s.waiting_kind != "output" and s.held - s.pinned > 0:
                return "spill"
            return "wait"

    def wait_event(self, qid: str, since_generation: int):
        """在条件变量上等待，直到状态代推进或查询终态。

        谓词（generation 变化 / cancelled / failed）在同一把锁内判断，
        因此"最后一次 poll"与"进入 wait"之间不可能漏掉通知。
        返回是否仍存活（未取消、未失败）。
        """
        with self._lock:
            s = self._states[qid]
            while (not s.cancelled and not s.failed
                   and self._generation == since_generation):
                self._lock.wait()
            return not (s.cancelled or s.failed)

    def current_generation(self):
        with self._lock:
            return self._generation

    # ------------------------------------------------------------- 请求
    def request(self, qid: str, n: int, kind: str = "input"):
        """整批申请 n 个槽。True=已授予，False=需等待，异常=已终态。

        kind 为 "input"（输入/驻留阶段）或 "output"（结果缓冲阶段）；
        output 等待期间，本查询已挂载的输出缓冲不作为撤销受害者。
        不做部分承诺：n 要么整批授予，要么在 FIFO 中等待直到整批满足，
        要么查询结束。pin 区中调用 request 直接拒绝。
        """
        if n <= 0:
            raise ArbiterError("request size must be positive")
        if kind not in ("input", "output"):
            raise ArbiterError("request kind must be input/output")
        with self._lock:
            s = self._states[qid]
            if s.pinned:
                raise ArbiterError(
                    f"{qid}: cannot request slots inside pin region")
            if n > self.pool:
                raise ArbiterError(f"request {n} exceeds pool {self.pool}")
            if s.done or s.cancelled or s.failed:
                raise ArbiterError(f"{qid}: request after terminal")
            if s.waiting_n is not None:
                raise ArbiterError(f"{qid}: overlapping request")
            self._counter += 1
            s.order = self._counter
            s.waiting_n = n
            s.waiting_kind = kind
            self.log.emit("request", qid, slots=n, req_kind=kind,
                          free=self._free)
            self._replan_locked()
            return s.waiting_n is None

    # ----------------------------------------------------- 槽挂载/释放
    def attach_consume(self, qid: str, n: int):
        """授予槽挂载为驻留记录（consume 之后）。"""
        with self._lock:
            s = self._states[qid]
            if n > s.reserved:
                raise ArbiterError(
                    f"{qid}: consume {n} > reserved {s.reserved}")
            s.reserved -= n
            s.resident += n
            self.log.emit("consume", qid, slots=n,
                          resident=s.resident + s.pinned)

    def attach_output(self, qid: str, n: int):
        """授予槽挂载为输出缓冲。"""
        with self._lock:
            s = self._states[qid]
            if n > s.reserved:
                raise ArbiterError(
                    f"{qid}: attach_output {n} > reserved {s.reserved}")
            s.reserved -= n
            s.outbuf += n
            self.log.emit("attach_output", qid, slots=n, outbuf=s.outbuf)

    def release_reserved(self, qid: str, n: int, reason: str):
        """归还已授予但从未挂载的槽（取消/主动让出）。"""
        if n <= 0:
            return
        with self._lock:
            s = self._states[qid]
            if n > s.reserved:
                raise ArbiterError(
                    f"{qid}: release_reserved {n} > reserved {s.reserved}")
            s.reserved -= n
            s.held -= n
            self._free += n
            self.log.emit("release_reserved", qid, slots=n, reason=reason,
                          held=s.held, free=self._free)
            self._replan_locked()
            self._bump_locked()

    # ------------------------------------------------------------- pin/溢写
    def spill_begin(self, qid: str, n: int):
        """进入 pin 区：n 个驻留槽转为 pinned，完成前不可撤销。"""
        with self._lock:
            s = self._states[qid]
            if n <= 0:
                raise ArbiterError("spill_begin requires n > 0")
            if s.pinned:
                raise ArbiterError(f"{qid}: nested pin region")
            if n > s.resident:
                raise ArbiterError(
                    f"{qid}: pin {n} > resident {s.resident}")
            s.resident -= n
            s.pinned += n
            self.log.emit("pin", qid, slots=n, held=s.held, free=self._free)
            self._replan_locked()

    def spill_end(self, qid: str, n: int, run_path: str,
                  cancelled: bool = False, failed: bool = False):
        """设施真实写盘完成（spill() 已返回）后调用，真正释放槽。

        只有这里能释放 pin 槽；revoke_request 从不直接释放。
        取消/失败与溢写交错时同样只走这一条路径，保证不重复释放。
        """
        with self._lock:
            s = self._states[qid]
            if n > s.pinned:
                raise ArbiterError(
                    f"{qid}: spill_end {n} > pinned {s.pinned}")
            s.pinned -= n
            s.held -= n
            self._free += n
            self.log.emit(
                "spill_written", qid, slots=n, run_path=run_path,
                released=n, cancelled=cancelled, failed=failed,
                held=s.held, free=self._free)
            self._replan_locked()
            self._bump_locked()

    # ------------------------------------------------------------- 输出槽
    def release_output(self, qid: str, n: int):
        """释放一批已写出的结果缓冲槽。"""
        if n <= 0:
            return
        with self._lock:
            s = self._states[qid]
            if n > s.outbuf:
                raise ArbiterError(
                    f"{qid}: release_output {n} > outbuf {s.outbuf}")
            s.outbuf -= n
            s.held -= n
            self._free += n
            self.log.emit("release_output", qid, slots=n,
                          held=s.held, free=self._free)
            self._replan_locked()
            self._bump_locked()

    # ------------------------------------------------------------- 终态
    def discard_resident(self, qid: str, n: int, status: str):
        """取消/失败时丢弃尚未溢写的驻留记录并归还其槽。

        与溢写释放走同一条守恒路径，区别仅在于记录不落盘；
        pin 中的槽不允许走这里（必须等真实写入闭合后由 spill_end 释放）。
        """
        if n <= 0:
            return
        with self._lock:
            s = self._states[qid]
            if s.pinned:
                raise ArbiterError(
                    f"{qid}: cannot discard pinned records ({s.pinned})")
            if n > s.resident:
                raise ArbiterError(
                    f"{qid}: discard {n} > resident {s.resident}")
            s.resident -= n
            s.held -= n
            self._free += n
            self.log.emit("release_discard", qid, slots=n, status=status,
                          held=s.held, free=self._free)
            self._replan_locked()
            self._bump_locked()

    def cancel(self, qid: str, phase: str):
        """请求取消某查询。只影响该查询自身，不触碰其他查询。"""
        with self._lock:
            s = self._states.get(qid)
            if s is None or s.done or s.cancelled or s.failed:
                return False
            s.cancelled = True
            self.log.emit("cancel_request", qid, phase=phase,
                          waiting=s.waiting_n, pinned=s.pinned, held=s.held)
            if s.waiting_n is not None:
                # 未获批的申请：明确结束等待，槽一分不拿。
                self.log.emit("request_ended", qid,
                              slots=s.waiting_n, outcome="cancelled")
                s.waiting_n = None
                s.waiting_kind = None
            self._replan_locked()
            self._bump_locked()
            return True

    def fail(self, qid: str, message: str):
        """标记算子错误；资源由该查询工作线程在清理路径统一归还。"""
        with self._lock:
            s = self._states.get(qid)
            if s is None or s.done or s.failed or s.cancelled:
                return False
            s.failed = True
            self.log.emit("fail_request", qid, error=message,
                          waiting=s.waiting_n, pinned=s.pinned)
            if s.waiting_n is not None:
                self.log.emit("request_ended", qid,
                              slots=s.waiting_n, outcome="failed")
                s.waiting_n = None
                s.waiting_kind = None
            self._replan_locked()
            self._bump_locked()
            return True

    def terminal(self, qid: str, status: str, granted_total: int):
        """工作线程清理完全部自身资源后登记终态。

        前置不变量：调用时该查询四个子账必须全零，即输入、溢写残留、
        保留槽与输出缓冲全部清理或成功完成。
        """
        with self._lock:
            s = self._states[qid]
            if s.done:
                raise ArbiterError(f"{qid}: duplicate terminal")
            if any(x != 0 for x in (s.held, s.pinned, s.resident,
                                    s.outbuf, s.reserved)):
                raise ArbiterError(
                    f"{qid}: terminal with held={s.held} pinned={s.pinned} "
                    f"resident={s.resident} outbuf={s.outbuf} "
                    f"reserved={s.reserved}")
            s.done = True
            s.waiting_n = None
            s.waiting_kind = None
            self.log.emit("terminal", qid, status=status,
                          granted_total=granted_total, free=self._free)
            self._replan_locked()
            self._bump_locked()

    def all_done(self):
        with self._lock:
            return bool(self._states) and all(
                s.done for s in self._states.values())

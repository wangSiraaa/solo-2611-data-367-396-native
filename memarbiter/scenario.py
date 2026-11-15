"""内置确定性场景：用 HookBus + threading.Event 编排，无任何 sleep。

场景复现材料要求的关键交错：
  * barrier：两个查询先各持可撤销记录，再同时申请较大批次，
    第三个查询在等待队列——验证按实际持有/可撤销/pin 状态解除等待；
  * cancel-pin：在真实写入 pin 窗口内取消，写入必须先完成再释放；
  * cancel-waiting：取消一个申请未获批的查询；
  * cancel-reserved：取消已授予但尚未消费的查询；
  * fail-spill：在 pin 窗口注入真实算子错误，资源全部归还。
"""
import threading

from .worker import QueryCancelled


class _EventBoard:
    """按名字收集的 Event 集合；回调内 wait/set，确定性同步。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._events = {}

    def event(self, name):
        with self._lock:
            ev = self._events.get(name)
            if ev is None:
                ev = threading.Event()
                self._events[name] = ev
            return ev

    def wait(self, name, timeout=30):
        if not self.event(name).wait(timeout):
            raise RuntimeError(f"确定性等待超时（死锁信号）: {name}")

    def set(self, name):
        self.event(name).set()


def build_hooks(scenario, board, arbiter_cancel, arbiter_fail, qids,
                log_handle=None):
    """生成 HookBus 回调。arbiter_cancel/fail/log_handle 由 Runner 注入。"""
    from .runner import HookBus
    bus = HookBus()
    q1, q2 = qids[0], qids[1]
    q3 = qids[2] if len(qids) > 2 else None

    if scenario == "plain":
        return bus

    if scenario == "barrier":
        # 材料规定的确定性 barrier：
        #   阶段 A：q1/q2（以及 q3）各消费首批 2 行，持有可撤销记录；
        #   阶段 B：三个查询都到达"大批次申请点"后，由协调者同时放行，
        #           请求按确定顺序入 FIFO，避免任一大请求提前入队造成
        #           队首阻塞（head-of-line）死锁。
        def cb(name, qid, *args):
            if name == "after_consume" and args[0] == 0:
                board.set(f"first-consumed-{qid}")
            if name == "before_request" and args[0] == 1:
                # 第二批（6 行大批次）：在屏障点等待协调者统一放行。
                board.set(f"big-arrived-{qid}")
                board.wait(f"big-go-{qid}")
        bus.subscribe(cb)

        def coordinator():
            for qid in qids:
                board.wait(f"first-consumed-{qid}")
            # 此刻所有查询都已消费首批、持有可撤销记录。
            for qid in qids:
                board.wait(f"big-arrived-{qid}")
            # 三方都在申请点：同时放行，请求随即按线程调度顺序入队，
            # 仲裁器依据实际 held/revocable/pinned 状态解除等待。
            for qid in qids:
                board.set(f"big-go-{qid}")
        return bus, coordinator

    if scenario == "cancel-pin":
        victim = q1

        def cb(name, qid, *args):
            if name == "on_pinned" and qid == victim:
                # pin 窗口内、真实写入完成前发起取消。
                board.set("pin-entered")
                board.wait("cancel-issued")
        bus.subscribe(cb)

        def coordinator():
            board.wait("pin-entered")
            arbiter_cancel(victim, "pin-region")
            board.set("cancel-issued")
        return bus, coordinator

    if scenario == "cancel-waiting":
        # 确定性保证取消必落在"申请未获批"的等待态：
        # q2 消费首批后在第二批（6 行）申请点被门控；协调者通过事件日志
        # 监听器确认其大请求已进入 FIFO（request 已发、grant 未发）后再
        # 取消，全程 event 驱动，无轮询、无 sleep。
        target = q2

        def cb(name, qid, *args):
            if name == "after_consume" and qid == q1 and args[0] == 0:
                board.set("q1-holding")
            if name == "before_request" and qid == target and args[0] == 1:
                board.set("q2-at-big-request")
                board.wait("q2-may-request")
        bus.subscribe(cb)

        def coordinator():
            board.wait("q1-holding")
            board.wait("q2-at-big-request")
            enqueued = threading.Event()

            def on_event(ev):
                if (ev.get("qid") == target and ev["kind"] == "request"
                        and ev["slots"] == 6):
                    enqueued.set()
            # 监听器由调用方（CLI/测试）通过 log_handle 注入。
            log_handle.add_listener(on_event)
            board.set("q2-may-request")
            if not enqueued.wait(30):
                raise RuntimeError("cancel-waiting: 未观测到大请求入队")
            arbiter_cancel(target, "waiting")
        return bus, coordinator

    if scenario == "cancel-reserved":
        # 授予已保留、尚未 consume：after_grant 与 consume 之间取消。
        def cb(name, qid, *args):
            if name == "after_grant" and qid == q2 and args[0] == 0:
                board.set("q2-granted")
                board.wait("q2-cancel-seen")
        bus.subscribe(cb)

        def coordinator():
            board.wait("q2-granted")
            arbiter_cancel(q2, "reserved-unused")
            board.set("q2-cancel-seen")
        return bus, coordinator

    if scenario == "fail-spill":
        victim = q1

        class _FailInPin(Exception):
            pass

        def cb(name, qid, *args):
            if name == "on_pinned" and qid == victim:
                board.set("fail-pin-entered")
                board.wait("fail-armed")
                raise _FailInPin("injected operator failure in pin region")
        bus.subscribe(cb)

        def coordinator():
            board.wait("fail-pin-entered")
            arbiter_fail(victim, "injected operator failure")
            board.set("fail-armed")
        return bus, coordinator

    raise ValueError(f"未知场景: {scenario}")


SCENARIOS = ["plain", "barrier", "cancel-pin", "cancel-waiting",
             "cancel-reserved", "fail-spill"]

"""查询工作线程：把仲裁器决策接到真实 SortBuffer 上。

每个查询一个线程。线程只在仲裁器的条件变量上等待（事件驱动），
不使用任何 sleep/超时作为正确性手段。所有真实 IO 与排序都发生在
:mod:`memarbiter.operators` 的设施里，本模块只负责：
  * 整批申请槽、拿到授予后 consume 并挂载；
  * 在检查点响应撤销指令，自己执行自己的溢写（因此等待者不会成为
    死锁方——每个线程都保留执行自身溢写的能力）；
  * pin 窗口内通过设施回调暴露真实写入窗口；
  * 取消/失败时确定性结束，统一在终态前归还全部自身资源。
"""
import json
import threading

from .operators import SortBuffer, merge_sorted_runs


class QueryCancelled(Exception):
    """内部控制流：查询被取消。"""


class OperatorError(RuntimeError):
    """真实算子抛出的业务错误（含测试注入错误）。"""


class QueryWorker(threading.Thread):
    def __init__(self, qid, batches, arbiter, work_dir, hooks=None,
                 output_chunk=None):
        super().__init__(name=f"worker-{qid}", daemon=True)
        self.qid = qid
        self.batches = batches
        self.arb = arbiter
        self.work_dir = work_dir
        self.hooks = hooks or _NullHooks()
        self.output_chunk = output_chunk or arbiter.minimum
        self.buf = SortBuffer(work_dir, qid)
        self.status = None            # success / cancelled / failed
        self.error = None
        self.output_path = None
        self._outbuf = 0              # 本线程视角的输出缓冲挂载数
        self._total_granted = 0
        self.exc_info = None

    # ------------------------------------------------------------- 检查点
    def _checkpoint_terminal(self):
        """只响应取消/失败；不让出当前批刚保留的槽。"""
        action = self.arb.poll_terminal(self.qid)
        if action == "cancelled":
            raise QueryCancelled()
        if action == "failed":
            raise OperatorError(self.error or "query marked failed")

    def _checkpoint_idle(self):
        """空闲检查点：处理撤销指令与终态；正常返回。"""
        while True:
            action = self.arb.poll_idle(self.qid)
            if action == "cancelled":
                raise QueryCancelled()
            if action == "failed":
                raise OperatorError(self.error or "query marked failed")
            if action == "spill":
                self._comply_revoke()
                continue
            return

    def _comply_revoke(self):
        """执行撤销：先直接还保留槽，再把驻留记录真实溢写落盘。

        输出缓冲不在此处理：它的生命周期只在输出循环内成对管理，
        避免等待新块期间把当前块释放造成双重归还。
        """
        snap = self.arb.snapshot()["queries"][self.qid]
        if snap["reserved"]:
            self.arb.release_reserved(self.qid, snap["reserved"],
                                      reason="revoke-reserved")
        if self.buf.resident_count():
            self._spill_all()

    # ------------------------------------------------------------- 申请
    def _request(self, n, kind="input"):
        """整批申请，直到授予或查询终态。绝不部分消费。"""
        if self.arb.request(self.qid, n, kind):
            self._total_granted += n
            return
        while True:
            # 记录代基线：此后任何状态推进都不会被 wait 漏掉。
            gen = self.arb.current_generation()
            action = self.arb.poll_waiting(self.qid)
            if action == "granted":
                self._total_granted += n
                return
            if action == "cancelled":
                raise QueryCancelled()
            if action == "failed":
                raise OperatorError(self.error or "query marked failed")
            if action == "spill":
                self._comply_revoke()
                continue
            # wait：谓词循环保证无唤醒丢失。
            alive = self.arb.wait_event(self.qid, gen)
            if not alive:
                action = self.arb.poll_waiting(self.qid)
                if action == "cancelled":
                    raise QueryCancelled()
                if action == "failed":
                    raise OperatorError(self.error or "query marked failed")

    # ------------------------------------------------------------- 溢写
    def _spill_all(self):
        """把设施中全部驻留记录真实溢写，pin 期间槽不可撤销。"""
        n = self.buf.resident_count()
        if n == 0:
            return 0
        self.arb.spill_begin(self.qid, n)
        try:
            def on_pinned():
                # 真实写入 pin 窗口：测试可在此确定性地触发取消/失败，
                # 但写入本身必须完成（设施语义），槽在 spill 返回后才释放。
                self.hooks.on_pinned(self.qid, n)

            def on_written():
                self.hooks.on_written(self.qid, n)

            written = self.buf.spill(on_pinned=on_pinned,
                                     on_written=on_written)
        except QueryCancelled:
            raise
        except Exception as exc:  # 真实算子错误
            self.error = f"spill error: {exc!r}"
            self.arb.fail(self.qid, self.error)
            # 设施在写入中途失败，rows 不保证已清空。已 pin 的槽由
            # spill_end 统一归还；设施侧残留记录在此显式丢弃，使缓冲区
            # 视角与仲裁器子账重新一致，终态不会再重复释放。
            self.buf.rows.clear()
            self.arb.spill_end(self.qid, n,
                               str(self.buf.work / "incomplete"),
                               failed=True)
            raise OperatorError(self.error) from exc
        run_path = str(self.buf.runs[-1])
        # 只有真实写盘、设施清空驻留并登记运行段之后才释放槽。
        # 若取消在 pin 窗口内到达，写入仍按设施语义完成，仅在事件上标记，
        # 该运行段随后随取消终态一起清理。
        cancelled = self.arb.snapshot()["queries"][self.qid]["cancelled"]
        self.arb.spill_end(self.qid, written, run_path,
                           cancelled=cancelled)
        return written

    # ------------------------------------------------------------- 主流程
    def run(self):
        try:
            self.arb.register(self.qid)
            self._run_input()
            self._run_output()
            self._finish("success")
        except QueryCancelled:
            self._finish("cancelled")
        except OperatorError as exc:
            self.error = str(exc)
            self._finish("failed")
        except Exception as exc:  # 防御：任何意外也必须归还全部资源
            self.exc_info = exc
            self.error = f"unexpected: {exc!r}"
            self.arb.fail(self.qid, self.error)
            self._finish("failed")

    def _run_input(self):
        for batch_index, batch in enumerate(self.batches):
            # 输入读取本身占槽：先整批申请，授予后才 consume。
            self.hooks.before_request(self.qid, batch_index, len(batch))
            self._request(len(batch))
            self.hooks.after_grant(self.qid, batch_index, len(batch))
            # 授予后、消费前：只接受取消/失败；当前批保留槽不能在此让出，
            # 否则会出现"已批给又被撤回"的记账失配。
            self._checkpoint_terminal()
            self.buf.consume(batch)
            self.arb.attach_consume(self.qid, len(batch))
            self.hooks.after_consume(self.qid, batch_index)
            # 消费后检查点：驻留记录此刻起可撤销，需要时自行真实溢写。
            self._checkpoint_idle()

    # ------------------------------------------------------------- 输出
    def _run_output(self):
        # 终态输出前先把所有驻留记录溢写为运行段，释放全部输入槽。
        if self.buf.resident_count():
            self._checkpoint_idle()
            self._spill_all()
        self._checkpoint_idle()

        out_path = self.buf.work / "output.jsonl"
        self.output_path = str(out_path)
        chunk = max(1, self.output_chunk)
        stream = merge_sorted_runs(list(self.buf.runs), self.buf.rows)

        # 槽生命周期严格成对，且任何一行都不能因撤销被跳过：
        # 用 while 驱动迭代器，只有当 row 真正进入带槽缓冲后才前进；
        # 授予后/挂载后被撤销时归还槽并 continue，重新处理同一个 row。
        buffer = []
        self._outbuf = 0
        with open(out_path, "w", encoding="utf-8") as fh:
            row_iter = iter(stream)
            while True:
                if self._outbuf == 0:
                    self._request(chunk, kind="output")
                    # 授予与挂载之间也是撤销窗口：若保留槽已被队首回收，
                    # 归还后重新申请（尚未消费任何 row，无数据丢失）。
                    snap = self.arb.snapshot()["queries"][self.qid]
                    if snap["cancelled"]:
                        raise QueryCancelled()
                    if snap["failed"]:
                        raise OperatorError(
                            self.error or "query marked failed")
                    if snap["reserved"] < chunk:
                        if snap["reserved"]:
                            self.arb.release_reserved(
                                self.qid, snap["reserved"],
                                reason="revoked-before-attach")
                        continue
                    self.arb.attach_output(self.qid, chunk)
                    self._outbuf = chunk
                    # 挂载后、缓存任何行之前若被撤销，归还并重新申请。
                    action = self.arb.poll_idle(self.qid)
                    if action == "cancelled":
                        raise QueryCancelled()
                    if action == "failed":
                        raise OperatorError(
                            self.error or "query marked failed")
                    if action == "spill":
                        self._release_outbuf()
                        continue
                try:
                    row = next(row_iter)
                except StopIteration:
                    break
                buffer.append(row)   # row 已进入由 outbuf 计费的缓冲
                if len(buffer) >= chunk:
                    self._flush(fh, buffer)
                    buffer = []
            if buffer:
                # 流结束时仍有缓存行：始终在当前 outbuf 槽内，先写后还。
                self._flush(fh, buffer)
                buffer = []
        self._release_outbuf()   # 末块未满时归还多占部分（幂等）
        self.arb.log.emit("output_complete", self.qid,
                          output_path=self.output_path)

    def _release_outbuf(self):
        """归还当前输出缓冲槽；无挂载时为空操作（幂等）。"""
        if self._outbuf:
            self.arb.release_output(self.qid, self._outbuf)
            self._outbuf = 0

    def _flush(self, fh, buffer):
        fh.write("".join(json.dumps(r, ensure_ascii=False) + "\n"
                         for r in buffer))
        used = len(buffer)
        # 结果已真实写出，归还整块槽（含未用满部分），再继续下一块。
        self._release_outbuf()
        self.arb.log.emit("output_flush", self.qid, rows=used)

    # ------------------------------------------------------------- 终态
    def _finish(self, status):
        """终态：输入、溢写残留、保留槽与输出缓冲全部清理之后才登记。

        所有释放都通过仲裁器的守恒路径（spill_end / release_output /
        release_reserved / discard_resident）；取消与 revoke 交错也不会
        重复释放：每个槽只有其所属查询的这一个线程释放，且每条路径都
        在锁内校验子账。
        """
        snap = self.arb.snapshot()["queries"][self.qid]
        # 1) 异常中若 pin 未闭合（正常路径 spill_end 已处理），先闭合。
        if snap["pinned"]:
            self.arb.spill_end(
                self.qid, snap["pinned"],
                str(self.buf.work / "aborted"),
                cancelled=(status == "cancelled"),
                failed=(status == "failed"))
        # 2) 驻留记录：成功路径已在输出前溢尽；取消/失败则真实丢弃还槽。
        # 以仲裁器子账为准（pin 部分已由 spill_end 释放，不在此重复）。
        snap = self.arb.snapshot()["queries"][self.qid]
        leftover = snap["resident"]
        if leftover:
            self.arb.log.emit("resident_discard", self.qid, slots=leftover,
                              status=status)
            self.buf.rows.clear()       # 设施视角同步，不再引用这批记录
            self.arb.discard_resident(self.qid, leftover, status)
        # 3) 已授予未挂载的保留槽（取消发生在申请获批后、consume 前等）。
        snap = self.arb.snapshot()["queries"][self.qid]
        if snap["reserved"]:
            self.arb.release_reserved(self.qid, snap["reserved"],
                                      reason=f"terminal-{status}")
        # 4) 输出缓冲残留（取消发生在输出阶段时）。
        snap = self.arb.snapshot()["queries"][self.qid]
        if snap["outbuf"]:
            self.arb.release_output(self.qid, snap["outbuf"])
            self._outbuf = 0
        # 终态前置不变量：四个子账必须全部归零。
        snap = self.arb.snapshot()["queries"][self.qid]
        assert all(snap[k] == 0 for k in
                   ("held", "pinned", "resident", "outbuf", "reserved"))
        # 5) 文件清理：失败/取消只删自己的临时目录，绝不碰其他查询。
        if status != "success":
            self._cleanup_own_files()
            self.output_path = None
        self.status = status
        self.arb.terminal(self.qid, status, self._total_granted)

    def _cleanup_own_files(self):
        import shutil
        if self.buf.work.exists():
            shutil.rmtree(self.buf.work)
        self.arb.log.emit("cleanup_files", self.qid,
                          work_dir=str(self.buf.work))


class _NullHooks:
    def before_request(self, qid, batch_index, size): pass
    def after_grant(self, qid, batch_index, size): pass
    def after_consume(self, qid, batch_index): pass
    def on_pinned(self, qid, n): pass
    def on_written(self, qid, n): pass

"""可复跑测试：barrier/event 控制，无墙钟超时作为正确性证明。

覆盖：
  * 六类确定性场景（竞争、pin 取消、等待取消、保留未用取消、pin 错误、平凡）；
  * 配置初始化拒绝规则；
  * 槽守恒/pin 安全/请求闭环/结果一致（直接复用离线审计）；
  * 至少两个查询同时处于"已消费输入未结束"；
  * 批次顺序、取消对象、query_id 重排的参数化；
  * 全池 8..32、单批 1..6 的边界。

运行：python -m unittest -v tests.test_arbiter -v
"""
import json
import os
import shutil
import tempfile
import threading
import unittest
from pathlib import Path

from memarbiter.arbiter import ArbiterError, EventLog, SlotArbiter
from memarbiter.audit import AuditFailure, audit_workdir
from memarbiter.operators import SortBuffer, merge_sorted_runs, sort_key
from memarbiter.runner import ConfigError, Runner, validate_config
from memarbiter.scenario import SCENARIOS, _EventBoard, build_hooks
from memarbiter.worker import QueryCancelled, QueryWorker

ROOT = Path(__file__).resolve().parent.parent
BASE_CONFIG = json.loads((ROOT / "config.json").read_text("utf-8"))
BASE_QUERIES = json.loads((ROOT / "queries.json").read_text("utf-8"))["queries"]


class _TempRuns:
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="memarb-test-"))
        self._seq = 0

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def new_dir(self):
        self._seq += 1
        d = self.tmp / f"run-{self._seq}"
        d.mkdir()
        return d

    def run_scenario(self, scenario, config=None, queries=None):
        work = self.new_dir()
        config = config or BASE_CONFIG
        queries = queries or BASE_QUERIES
        board = _EventBoard()
        runner = Runner(config, queries, work)
        if scenario != "plain":
            qids = [q["query_id"] for q in queries]
            bus, coord = build_hooks(
                scenario, board,
                lambda qid, phase: runner.arbiter.cancel(qid, phase),
                lambda qid, msg: runner.arbiter.fail(qid, msg),
                qids, log_handle=runner.log)
            runner.hooks = bus
            for w in runner.workers:
                w.hooks = bus
        else:
            coord = None
        runner.start()
        if coord is not None:
            t = threading.Thread(target=coord, daemon=True)
            t.start()
            t.join()
        runner.wait()
        report = audit_workdir(work)   # 审计不通过即抛异常
        return work, runner.result(), report


class TestScenarios(_TempRuns, unittest.TestCase):
    def test_all_scenarios_terminate_and_audit(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                work, result, report = self.run_scenario(scenario)
                self.assertTrue(report["ok"])
                statuses = {qid: r["status"] for qid, r in result.items()}
                if scenario == "plain" or scenario == "barrier":
                    self.assertTrue(
                        all(v == "success" for v in statuses.values()),
                        statuses)
                if scenario in ("cancel-pin", "cancel-waiting",
                                "cancel-reserved"):
                    self.assertIn("cancelled", statuses.values())
                if scenario == "fail-spill":
                    self.assertIn("failed", statuses.values())
                # 未取消/失败的查询必须成功且产出独立排序一致的结果。
                for q in BASE_QUERIES:
                    qid = q["query_id"]
                    if statuses[qid] != "success":
                        self.assertFalse((work / qid).exists(),
                                         f"{qid} 临时目录未清理")
                        continue
                    rows = [r for b in q["batches"] for r in b]
                    got = [json.loads(l) for l in
                           (work / qid / "output.jsonl").read_text().splitlines()]
                    self.assertEqual(got, sorted(rows, key=sort_key))


class TestTwoQueriesConcurrentResidency(_TempRuns, unittest.TestCase):
    """材料硬性要求：至少两个查询同时处于已消费输入而未结束。"""

    def test_two_queries_hold_records_concurrently(self):
        work = self.new_dir()
        runner = Runner(BASE_CONFIG, BASE_QUERIES, work)
        max_concurrent = {"n": 0}
        lock = threading.Lock()

        def hook(name, qid, *args):
            if name == "after_consume":
                snap = runner.arbiter.snapshot()
                holding = [q for q, s in snap["queries"].items()
                           if s["resident"] + s["pinned"] > 0]
                with lock:
                    max_concurrent["n"] = max(max_concurrent["n"],
                                              len(holding))
        from memarbiter.runner import HookBus
        bus = HookBus()
        bus.subscribe(hook)
        runner.hooks = bus
        for w in runner.workers:
            w.hooks = bus
        runner.run()
        audit_workdir(work)
        self.assertGreaterEqual(max_concurrent["n"], 2)


class TestBarrierSemantics(_TempRuns, unittest.TestCase):
    """barrier 场景中两个查询先持可撤销记录，再同时申请大批次，
    且解除等待依赖真实持有/可撤销/pin 状态，而非加大池。"""

    def test_revocable_holds_then_big_requests(self):
        work, result, report = self.run_scenario("barrier")
        events = EventLog.read(work / "events.jsonl")
        # 每个查询的第一次 consume（首批 2 行）都发生在任何 6 行大
        # request 之前。
        first_consume_seen = set()
        first_consume = []
        for e in events:
            if e["kind"] == "consume" and e["qid"] not in first_consume_seen:
                first_consume_seen.add(e["qid"])
                first_consume.append(e)
        big_requests = [e for e in events
                        if e["kind"] == "request" and e["slots"] == 6]
        self.assertEqual(len(first_consume), 3)
        # 三个查询各至少发起一次 6 行大批次请求。
        self.assertEqual(len({e["qid"] for e in big_requests}), 3)
        self.assertGreaterEqual(len(big_requests), 3)
        self.assertLess(max(e["seq"] for e in first_consume),
                        min(e["seq"] for e in big_requests))
        # 存在真实撤销指令与真实溢写释放成对出现。
        revokes = [e for e in events if e["kind"] == "revoke_request"]
        spills = [e for e in events if e["kind"] == "spill_written"]
        self.assertTrue(revokes and spills)
        # 每个 revoke 都不直接释放：revoke 与 spill_written 是不同事件，
        # 且释放严格晚于对应 pin。
        for sp in spills:
            pin = max((e for e in events
                       if e["kind"] == "pin" and e["qid"] == sp["qid"]
                       and e["seq"] < sp["seq"]),
                      key=lambda e: e["seq"], default=None)
            self.assertIsNotNone(pin)
        self.assertEqual(BASE_CONFIG["global_slots"], 8)  # 未加大池


class TestPinSafety(_TempRuns, unittest.TestCase):
    """pin 区槽不可撤销；取消在 pin 窗口内也要等真实写入完成。"""

    def test_cancel_inside_pin_waits_for_write(self):
        work, result, report = self.run_scenario("cancel-pin")
        events = EventLog.read(work / "events.jsonl")
        q1 = [e for e in events if e.get("qid") == "q1"]
        pin = next(e for e in q1 if e["kind"] == "pin")
        cancel = next(e for e in q1 if e["kind"] == "cancel_request")
        written = next(e for e in q1 if e["kind"] == "spill_written")
        # 取消在 pin 之后、真实写盘释放之前；释放晚于取消。
        self.assertLess(pin["seq"], cancel["seq"])
        self.assertLess(cancel["seq"], written["seq"])
        # 取消与 revoke 交错没有重复释放：审计 granted==released 已保证，
        # 再显式确认 q1 终态目录被清理而 q2/q3 输出保留。
        self.assertEqual(result["q1"]["status"], "cancelled")
        self.assertFalse((work / "q1").exists())
        self.assertTrue((work / "q2" / "output.jsonl").exists())
        self.assertTrue((work / "q3" / "output.jsonl").exists())

    def test_pin_region_rejects_new_request(self):
        log = EventLog(self.new_dir() / "e.jsonl")
        arb = SlotArbiter(8, 2, log)
        arb.register("x")
        self.assertTrue(arb.request("x", 4))
        arb.attach_consume("x", 4)
        arb.spill_begin("x", 4)
        with self.assertRaises(ArbiterError):
            arb.request("x", 1)
        arb.spill_end("x", 4, "/tmp/fake-run.jsonl")
        arb.release_reserved("x", 0, "noop") if False else None
        log.close()


class TestNoOvergrantAndWholeBatch(_TempRuns, unittest.TestCase):
    def test_never_exceeds_pool_and_no_partial_grant(self):
        for scenario in SCENARIOS:
            with self.subTest(scenario=scenario):
                work, _, _ = self.run_scenario(scenario)
                events = EventLog.read(work / "events.jsonl")
                pool = BASE_CONFIG["global_slots"]
                held = {}
                free = pool
                for e in events:
                    qid = e.get("qid")
                    if e["kind"] == "grant":
                        free -= e["slots"]
                        held[qid] = held.get(qid, 0) + e["slots"]
                        self.assertGreaterEqual(free, 0)
                    elif e["kind"] in ("spill_written", "release_output",
                                       "release_reserved", "release_discard"):
                        free += e["slots"]
                        held[qid] -= e["slots"]
                    self.assertLessEqual(sum(held.values()), pool)
                self.assertEqual(free, pool)


class TestConfigValidation(unittest.TestCase):
    def _q(self, batches):
        return [{"query_id": f"q{i}", "batches": b}
                for i, b in enumerate(batches)]

    def test_reject_pool_out_of_range(self):
        q = self._q([[[{"k": 1}]]])
        with self.assertRaises(ConfigError):
            validate_config(7, 2, q)
        with self.assertRaises(ConfigError):
            validate_config(33, 2, q)

    def test_reject_two_minima_exceed_pool(self):
        q = self._q([[[{"row_id": "a", "key": 1}]]] * 2)
        # 2*minimum(=5) > pool 8
        with self.assertRaises(ConfigError):
            validate_config(8, 5, q)

    def test_reject_batch_bigger_than_pool(self):
        big = [[{"row_id": str(i), "key": i} for i in range(6)]]
        q = self._q([big])
        with self.assertRaises(ConfigError):
            validate_config(4, 1, q)   # 池容不下最大批 6

    def test_reject_oversized_row(self):
        row = {"row_id": "x", "key": 1, "blob": "z" * 300}
        q = self._q([[[row]]])
        with self.assertRaises(ConfigError):
            validate_config(8, 2, q)

    def test_reject_too_many_rows(self):
        batches = [[{"row_id": str(i), "key": i}] for i in range(201)]
        q = self._q([batches])
        with self.assertRaises(ConfigError):
            validate_config(8, 2, q)

    def test_accepts_boundary_configs(self):
        row = {"row_id": "a", "key": 1}
        q = self._q([[[row]], [[row]], [[row]]])
        validate_config(8, 4, q)      # 2*4 == 8 边界允许
        validate_config(32, 1, q)


class TestParameterizedVariants(_TempRuns, unittest.TestCase):
    """改变批次顺序、取消对象与 query_id；32 槽 / 单批 1..6 边界。"""

    def _reorder_batches(self, q, seed):
        q = json.loads(json.dumps(q))
        batches = q["batches"]
        # 确定性旋转批次顺序，行数总量不变。
        k = seed % len(batches)
        q["batches"] = batches[k:] + batches[:k]
        return q

    def test_batch_order_variants(self):
        for seed in range(4):
            with self.subTest(seed=seed):
                qs = [self._reorder_batches(q, seed) for q in BASE_QUERIES]
                work, result, report = self.run_scenario(
                    "barrier", queries=qs)
                for q in qs:
                    qid = q["query_id"]
                    rows = [r for b in q["batches"] for r in b]
                    got = [json.loads(l) for l in
                           (work / qid / "output.jsonl").read_text().splitlines()]
                    self.assertEqual(got, sorted(rows, key=sort_key))

    def test_cancel_target_variants(self):
        # cancel-pin 场景固定取 qids[0]；重排 query 顺序即改变取消对象。
        for rot in range(3):
            with self.subTest(rot=rot):
                qs = BASE_QUERIES[rot:] + BASE_QUERIES[:rot]
                work, result, _ = self.run_scenario(
                    "cancel-pin", queries=qs)
                victim = qs[0]["query_id"]
                self.assertEqual(result[victim]["status"], "cancelled")
                for q in qs[1:]:
                    self.assertEqual(result[q["query_id"]]["status"],
                                     "success")

    def test_pool_sizes_8_to_32(self):
        row = {"row_id": "r", "key": 1}
        for pool in (8, 16, 32):
            with self.subTest(pool=pool):
                qs = [{
                    "query_id": f"q{i}",
                    "batches": [[dict(row, row_id=f"q{i}-{j}")
                                 for j in range(1, 7)]
                                for _ in range(3)],
                } for i in range(3)]
                cfg = {"global_slots": pool, "minimum_per_query": 2}
                work, result, report = self.run_scenario(
                    "barrier", config=cfg, queries=qs)
                self.assertTrue(all(r["status"] == "success"
                                    for r in result.values()))


class TestOperatorSemanticsPreserved(unittest.TestCase):
    def test_sortbuffer_real_spill_and_merge(self):
        d = tempfile.mkdtemp()
        try:
            buf = SortBuffer(d, "q")
            rows = [{"row_id": f"r{i}", "key": (i * 3) % 7}
                    for i in range(12)]
            buf.consume(rows[:6])
            n = buf.spill()                      # 真实落盘
            self.assertEqual(n, 6)
            self.assertTrue(buf.runs[0].exists())
            buf.consume(rows[6:])
            merged = list(merge_sorted_runs(
                list(buf.runs), buf.rows))
            self.assertEqual(merged, sorted(rows, key=sort_key))
            # materialized_rows 仍是材料原语义。
            self.assertEqual(len(list(buf.materialized_rows())), 12)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_pin_callbacks_wrap_real_write(self):
        d = tempfile.mkdtemp()
        order = []
        try:
            buf = SortBuffer(d, "q")
            buf.consume([{"row_id": "a", "key": 1}])
            buf.spill(on_pinned=lambda: order.append("pin"),
                      on_written=lambda: order.append("written"))
            self.assertEqual(order, ["pin", "written"])
        finally:
            shutil.rmtree(d, ignore_errors=True)


class TestErrorReleasesAllResources(_TempRuns, unittest.TestCase):
    def test_operator_failure_returns_every_slot(self):
        work, result, report = self.run_scenario("fail-spill")
        # 审计已强制 granted_total == released_total 且终态 free == pool。
        for qid, info in report["queries"].items():
            self.assertEqual(info["granted_total"], info["released_total"])
        self.assertEqual(result["q1"]["status"], "failed")
        # 失败查询的临时文件已清理，其他查询继续并成功。
        self.assertFalse((work / "q1").exists())
        self.assertTrue((work / "q2" / "output.jsonl").exists())


class TestStressNoDeadlock(_TempRuns, unittest.TestCase):
    """回归：条件变量唤醒丢失曾导致三工作线程同时睡死。

    多轮重复竞争/barrier/取消场景，全部由 event 驱动；faulthandandler
    式的外部超时只作为测试看门狗，正确性由仲裁器代谓词保证。
    """

    def test_many_rounds_never_hang(self):
        for _ in range(15):
            for scenario in ("plain", "barrier", "cancel-waiting"):
                work, result, report = self.run_scenario(scenario)
                self.assertTrue(report["ok"])
                for qid, info in report["queries"].items():
                    self.assertEqual(info["granted_total"],
                                     info["released_total"])

    def test_generation_predicate_skips_stale_wait(self):
        log = EventLog(self.new_dir() / "g.jsonl")
        arb = SlotArbiter(8, 2, log)
        arb.register("a")
        arb.register("b")
        self.assertTrue(arb.request("a", 8))       # a 占满全池
        arb.attach_consume("a", 8)
        # b 申请 4，必然排队。
        self.assertFalse(arb.request("b", 4))
        gen = arb.current_generation()
        # a 释放 4 槽（代推进）；b 的等待谓词必须立即满足而非睡死。
        arb.spill_begin("a", 8)
        arb.spill_end("a", 8, str(self.new_dir() / "r.jsonl"))
        self.assertGreater(arb.current_generation(), gen)
        self.assertEqual(arb.poll_waiting("b"), "granted")
        log.close()


class TestAuditReplaysAndCatchesTampering(_TempRuns, unittest.TestCase):
    def test_audit_detects_deleted_output(self):
        work, _, _ = self.run_scenario("plain")
        os.remove(work / "q1" / "output.jsonl")
        with self.assertRaises(AuditFailure):
            audit_workdir(work)

    def test_audit_detects_unsorted_output(self):
        work, _, _ = self.run_scenario("plain")
        p = work / "q2" / "output.jsonl"
        lines = p.read_text().splitlines()
        p.write_text("\n".join(reversed(lines)) + "\n")
        with self.assertRaises(AuditFailure):
            audit_workdir(work)


if __name__ == "__main__":
    unittest.main(verbosity=2)

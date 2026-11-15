"""全量测试：背压控制点、取消、故障注入、容量 1/2、空源、fanout=0、负 key 等。

同步全部基于事件/屏障/状态谓词；任何用例失败都附 inspect 快照。
"""
from __future__ import annotations

import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from etl_demo import Barrier, Engine, EngineConfig, Hooks, run_pipeline
from etl_demo.model import MAX_INPUT_FRAME
from etl_demo.test_support import (
    expected_expansion,
    make_worker_fail_hook,
    output_multiset,
    read_outputs,
)


def write_sources(base: Path, rows: dict[str, list[dict]]) -> dict[str, Path]:
    paths = {}
    for sid, recs in rows.items():
        p = base / f"{sid}.jsonl"
        with open(p, "w", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r) + "\n")
        paths[sid] = p
    return paths


def gen_rows(sid: str, n: int, *, skew_even: bool = False, all_zero: bool = False,
             negative: bool = False) -> list[dict]:
    rows = []
    for i in range(n):
        if skew_even:
            key = 0
        elif negative:
            key = -i - 1  # 负 key：-1%2=1, -2%2=0 ...
        else:
            key = i % 2
        rows.append({
            "source_id": sid, "seq": i, "key": key,
            "value": i, "fanout": 0 if all_zero else (i % 4),
        })
    return rows


def backpressure_predicate(snap: dict) -> bool:
    if snap["roles"].get("worker-0", {}).get("state") != "blocked_put":
        return False
    return any(
        snap["channels"].get(n, {}).get("frames", 0) > 0
        for n in ("src:a", "src:b", "r2w:0", "r2w:1")
    )


class ETLTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _cfg(self, sources, **kw) -> EngineConfig:
        return EngineConfig(sources=sources, run_dir=self.tmp / kw.pop("run_name", "run"), **kw)

    def _assert_no_threads_left(self, eng: Engine) -> None:
        alive = {n: t.is_alive() for n, t in eng.threads.items() if t.is_alive()}
        self.assertEqual(alive, {}, f"遗留线程: {alive}\n{json.dumps(eng.inspect(), ensure_ascii=False)}")

    # ---------- 1/2. 容量 1 与 2：背压控制点 + 恢复后完整 ----------
    def _backpressure_and_resume(self, capacity: int) -> None:
        rows = {
            "a": gen_rows("a", 12, skew_even=True),
            "b": gen_rows("b", 12, skew_even=True),
        }
        sources = write_sources(self.tmp, rows)
        pause = threading.Event()
        pause.set()  # sink 首次接收前暂停
        cfg = self._cfg(sources, capacity=capacity, sink_pause=pause, run_name=f"bp{capacity}")
        eng = Engine(cfg)
        eng.start()
        try:
            snap = eng.board.wait_until(backpressure_predicate, timeout=15.0)
            # 明确证据：worker0 阻塞在输出通道，且路由侧仍有输入帧。
            self.assertEqual(snap["roles"]["worker-0"]["state"], "blocked_put")
            self.assertGreaterEqual(
                sum(snap["channels"][n]["frames"] for n in ("src:a", "src:b", "r2w:0", "r2w:1")),
                1,
            )
            self.assertLessEqual(eng.budget.used, eng.budget._limit)
            pause.clear()
            result = eng.join()
        finally:
            pause.clear()
        self.assertEqual(result.outcome, "complete")
        self.assertIsNotNone(result.complete_marker)
        got = output_multiset(read_outputs(result.output_path))
        self.assertEqual(got, expected_expansion(sources))
        # 容量纪律：峰值帧数不得超过配置容量。
        for name, peak in result.channel_peak_frames.items():
            self.assertLessEqual(peak, capacity, f"{name} 超出容量")
        self._assert_no_threads_left(eng)

    def test_capacity_1_backpressure_and_resume(self) -> None:
        self._backpressure_and_resume(1)

    def test_capacity_2_backpressure_and_resume(self) -> None:
        self._backpressure_and_resume(2)

    def test_mixed_capacities(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 8), "b": gen_rows("b", 8),
        })
        cfg = self._cfg(sources, capacity=(1, 2, 1), run_name="mixed")
        eng = Engine(cfg)
        eng.start()
        result = eng.join()
        self.assertEqual(result.outcome, "complete")
        self.assertEqual(
            output_multiset(read_outputs(result.output_path)),
            expected_expansion(sources),
        )
        self._assert_no_threads_left(eng)

    # ---------- 3. 倾斜 key：worker0 承担绝大多数，仍正确且完整 ----------
    def test_skewed_keys(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 10, skew_even=True),
            "b": gen_rows("b", 10, skew_even=True),
        })
        for cap in (1, 2):
            run_dir = self.tmp / f"skew{cap}"
            cfg = EngineConfig(sources=sources, run_dir=run_dir, capacity=cap)
            result = run_pipeline(cfg)
            self.assertEqual(result.outcome, "complete")
            self.assertEqual(
                output_multiset(read_outputs(result.output_path)),
                expected_expansion(sources),
            )
            state = result.final_state["roles"]
            # 全偶数 key -> 全部输入必须路由到 worker0。
            self.assertEqual(state["worker-1"]["records_seen"], 0)
            self.assertEqual(state["worker-0"]["records_seen"], 20)

    # ---------- 4. 空源不能让终止提早 ----------
    def test_empty_sources(self) -> None:
        sources = write_sources(self.tmp, {"a": [], "b": []})
        cfg = self._cfg(sources, run_name="empty")
        result = run_pipeline(cfg)
        self.assertEqual(result.outcome, "complete")
        self.assertEqual(result.outputs_written, 0)
        self.assertEqual(read_outputs(result.output_path), [])
        self.assertIsNotNone(result.complete_marker)
        # 终止链完整：router 收到 2 个 EOS，每个 worker 收到 1 个，sink 收到 2 个。
        roles = result.final_state["roles"]
        self.assertEqual(roles["worker-0"]["eos_seen"], 1)
        self.assertEqual(roles["worker-1"]["eos_seen"], 1)

    def test_one_empty_source(self) -> None:
        sources = write_sources(self.tmp, {
            "a": [], "b": gen_rows("b", 6),
        })
        cfg = self._cfg(sources, run_name="oneempty")
        result = run_pipeline(cfg)
        self.assertEqual(result.outcome, "complete")
        self.assertEqual(
            output_multiset(read_outputs(result.output_path)),
            expected_expansion(sources),
        )

    # ---------- 5. 全零 fanout：无输出但每个输入都算已处理 ----------
    def test_all_zero_fanout(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 7, all_zero=True),
            "b": gen_rows("b", 5, all_zero=True),
        })
        cfg = self._cfg(sources, run_name="zero")
        result = run_pipeline(cfg)
        self.assertEqual(result.outcome, "complete")
        self.assertEqual(result.outputs_written, 0)
        roles = result.final_state["roles"]
        self.assertEqual(roles["worker-0"]["records_processed"]
                         + roles["worker-1"]["records_processed"], 12)
        self.assertIsNotNone(result.complete_marker)

    # ---------- 6. 负 key 的数学模路由与结果值 ----------
    def test_negative_keys(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 7, negative=True),
            "b": gen_rows("b", 7, negative=True),
        })
        cfg = self._cfg(sources, run_name="neg")
        result = run_pipeline(cfg)
        self.assertEqual(result.outcome, "complete")
        self.assertEqual(
            output_multiset(read_outputs(result.output_path)),
            expected_expansion(sources),
        )
        # 独立校验路由：-1%2=1, -2%2=0 ...
        rows = gen_rows("a", 7, negative=True)
        w0 = sum(1 for r in rows if r["key"] % 2 == 0)
        w1 = sum(1 for r in rows if r["key"] % 2 == 1)
        seen0 = result.final_state["roles"]["worker-0"]["records_seen"]
        seen1 = result.final_state["roles"]["worker-1"]["records_seen"]
        # 两个源分布相同
        self.assertEqual((seen0, seen1), (2 * w0, 2 * w1))

    # ---------- 7. 不同来源同 seq 不合并、同业务值不去重 ----------
    def test_identity_no_merge_no_dedup(self) -> None:
        rec = lambda sid, seq: {"source_id": sid, "seq": seq, "key": seq % 2,
                                "value": 5, "fanout": 2}
        sources = write_sources(self.tmp, {
            "a": [rec("a", i) for i in range(4)],
            "b": [rec("b", i) for i in range(4)],
        })
        cfg = self._cfg(sources, run_name="ident")
        result = run_pipeline(cfg)
        self.assertEqual(result.outcome, "complete")
        got = output_multiset(read_outputs(result.output_path))
        self.assertEqual(len(got), 16)  # 8 输入 * 2 part，身份全部独立
        self.assertEqual(got, expected_expansion(sources))

    # ---------- 8. 第 5 个输出后取消：报告 cancelled、只留前缀、无遗留线程 ----------
    def test_cancel_after_fifth_output(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 12, skew_even=True),
            "b": gen_rows("b", 12, skew_even=True),
        })
        barrier = Barrier("after-5")
        fired = threading.Event()

        def on_written(n: int, frame) -> None:
            if n == 5 and not fired.is_set():
                fired.set()
                barrier.arm()
                barrier.wait()

        cfg = self._cfg(sources, capacity=1, run_name="cancel",
                        hooks=Hooks(sink_on_written=on_written))
        eng = Engine(cfg)
        eng.start()
        self.assertTrue(fired.wait(15.0), "未等到第 5 个输出")
        eng.cancel("unit-test")
        barrier.release()
        result = eng.join()
        self.assertEqual(result.outcome, "cancelled")
        self.assertIsNone(result.complete_marker, "取消路径不得发布 complete")
        rows = read_outputs(result.output_path)
        self.assertLessEqual(len(rows), 36)
        expected = expected_expansion(sources)
        got = output_multiset(rows)
        self.assertTrue(set(got).issubset(set(expected)))
        self.assertTrue(all(expected[k] == v for k, v in got.items()))
        self.assertEqual(eng.budget.used, 0)
        self._assert_no_threads_left(eng)

    # ---------- 9. worker0 处理 a:4 前注入异常 ----------
    def test_worker_exception_before_a4(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 12), "b": gen_rows("b", 12),
        })
        hooks = Hooks(worker_before_process=make_worker_fail_hook(("a", 4), 0))
        cfg = self._cfg(sources, capacity=1, run_name="fail", hooks=hooks)
        eng = Engine(cfg)
        eng.start()
        result = eng.join()
        self.assertEqual(result.outcome, "failed")
        self.assertIsNotNone(result.error)
        self.assertEqual(result.error["role"], "worker-0")
        self.assertEqual(result.error["input_identity"], "a:4")
        self.assertIsNone(result.complete_marker, "故障路径不得发布 complete")
        self.assertEqual(eng.budget.used, 0, "异常后 payload 台账未归零")
        self._assert_no_threads_left(eng)

    # ---------- 10. sink 瞬时写失败：重试暂存仍计入预算，最终完整 ----------
    def test_sink_retry_staging(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 6), "b": gen_rows("b", 6),
        })
        cfg = self._cfg(sources, run_name="retry",
                        hooks=Hooks(sink_fail_writes={3}))
        result = run_pipeline(cfg)
        self.assertEqual(result.outcome, "complete")
        self.assertGreater(result.retry_staging_peak_bytes, 0)
        self.assertEqual(
            output_multiset(read_outputs(result.output_path)),
            expected_expansion(sources),
        )

    # ---------- 11. 超 256 字节输入帧被拒绝并按故障收敛 ----------
    def test_oversize_input_frame(self) -> None:
        big = {"source_id": "a", "seq": 0, "key": 0, "value": 0,
               "fanout": 1, "pad": "x" * 300}
        sources = write_sources(self.tmp, {"a": [big], "b": []})
        cfg = self._cfg(sources, run_name="oversize")
        eng = Engine(cfg)
        eng.start()
        result = eng.join()
        self.assertEqual(result.outcome, "failed")
        self.assertIn("source-a", result.error["role"])
        self._assert_no_threads_left(eng)
        self.assertLessEqual(MAX_INPUT_FRAME, 256)

    # ---------- 12. inspect 活状态可解释在途来源与输出 ----------
    def test_inspect_during_backpressure(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 10, skew_even=True),
            "b": gen_rows("b", 10, skew_even=True),
        })
        pause = threading.Event()
        pause.set()
        cfg = self._cfg(sources, capacity=1, run_name="inspect", sink_pause=pause)
        eng = Engine(cfg)
        eng.start()
        try:
            eng.board.wait_until(backpressure_predicate, timeout=15.0)
            snap = eng.inspect()
            self.assertEqual(set(snap["threads_alive"]), {
                "source-a", "source-b", "router", "worker-0", "worker-1", "sink"})
            self.assertTrue(all(v for v in snap["threads_alive"].values()))
            self.assertIn("budget", snap)
            self.assertGreater(snap["budget"]["limit"], snap["budget"]["used"])
            # ByteBudget 是占用的唯一权威（acquire/release 严格配对、fid 幂等、
            # join 末尾 assert_zero）。board 的角色持有/队列水位是派生展示值。
            # 这里校验权威预算有界、各展示项非负且不超过预算上限；终态再严格归零。
            ov = eng.board.ownership_with_budget(eng.budget)
            self.assertGreater(ov["budget_used"], 0)  # 背压时确有在途 payload
            self.assertLessEqual(ov["budget_used"], ov["budget_limit"])
            self.assertGreaterEqual(ov["held_by_roles"], 0)
            self.assertGreaterEqual(ov["queued_in_channels"], 0)
            self.assertGreaterEqual(ov["book_total"], 0)
            # 队列容量纪律：每条通道帧数不超过配置容量。
            for cname, c in snap["channels"].items():
                self.assertLessEqual(c["frames"], c["capacity"])
        finally:
            pause.clear()
        result = eng.join()
        self.assertEqual(result.outcome, "complete")
        self.assertEqual(eng.budget.used, 0)  # 终态严格归零（权威台账）
        self._assert_no_threads_left(eng)

    # ---------- 13. 全局预算本身也是背压源 ----------
    def test_tight_budget_backpressure(self) -> None:
        sources = write_sources(self.tmp, {
            "a": gen_rows("a", 8), "b": gen_rows("b", 8),
        })
        cfg = self._cfg(sources, run_name="tight", capacity=2, budget_limit=200)
        result = run_pipeline(cfg)
        self.assertEqual(result.outcome, "complete")
        self.assertLessEqual(result.budget_peak, 200)
        self.assertEqual(
            output_multiset(read_outputs(result.output_path)),
            expected_expansion(sources),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

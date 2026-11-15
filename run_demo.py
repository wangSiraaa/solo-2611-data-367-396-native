"""命令行入口：按 schedules.json 的控制点复跑 normal / cancel / worker_failure。

用法：
    python run_demo.py data run normal       # 暂停 sink，验证背压后恢复并完整
    python run_demo.py data run cancel       # 第 5 个输出后取消
    python run_demo.py data run worker_failure  # worker0 处理 a:4 前异常
    python run_demo.py data run all --capacity 2
    python run_demo.py data inspect normal   # 打印已保存的 state.json
"""
from __future__ import annotations

import json
import sys
import threading
from pathlib import Path

from etl_demo import Barrier, Engine, EngineConfig, Hooks
from etl_demo.test_support import (
    expected_expansion,
    make_worker_fail_hook,
    output_multiset,
    read_outputs,
)


def _sources(data_dir: Path) -> dict[str, Path]:
    return {
        "a": data_dir / "a.jsonl",
        "b": data_dir / "b.jsonl",
    }


def _backpressure_predicate(snap: dict) -> bool:
    """控制点：worker0 已被输出背压，且路由仍有未处理输入。"""
    w0 = snap["roles"].get("worker-0", {})
    if w0.get("state") != "blocked_put":
        return False
    pending = False
    for name in ("src:a", "src:b", "r2w:0", "r2w:1"):
        c = snap["channels"].get(name, {})
        if c.get("frames", 0) > 0:
            pending = True
    return pending


def run_normal(data_dir: Path, out_root: Path, capacity: int) -> dict:
    run_dir = out_root / f"normal-cap{capacity}"
    pause = threading.Event()
    pause.set()  # sink 首次接收前即暂停
    cfg = EngineConfig(
        sources=_sources(data_dir), run_dir=run_dir,
        capacity=capacity, sink_pause=pause,
    )
    eng = Engine(cfg)
    eng.start()
    # 以状态谓词等待背压真实发生，而非 sleep。
    snap = eng.board.wait_until(_backpressure_predicate, timeout=15.0)
    blocked_evidence = {
        "worker-0-state": snap["roles"]["worker-0"]["state"],
        "channels": {k: v["frames"] for k, v in snap["channels"].items() if v["frames"]},
        "budget_used_at_blockage": eng.budget.used,
    }
    pause.clear()  # 解除暂停，管道继续排空直到完整
    result = eng.join()
    rows = read_outputs(result.output_path)
    ok = output_multiset(rows) == expected_expansion(cfg.sources)
    return {
        "schedule": "normal",
        "capacity": capacity,
        "backpressure_observed": blocked_evidence,
        "outcome": result.outcome,
        "outputs": result.outputs_written,
        "multiset_matches_independent_expansion": ok,
        "complete_marker": str(result.complete_marker),
        "budget_peak": result.budget_peak,
        "channel_peak_frames": result.channel_peak_frames,
        "fixed_thread_count": result.fixed_thread_count,
        "max_rss_kib": result.max_rss_kib,
        "state_file": str(run_dir / "state.json"),
    }


def run_cancel(data_dir: Path, out_root: Path, capacity: int) -> dict:
    run_dir = out_root / f"cancel-cap{capacity}"
    barrier = Barrier("after-output-5")
    fired = threading.Event()

    def on_written(n: int, frame) -> None:
        if n == 5 and not fired.is_set():
            fired.set()
            eng.board.event("test.barrier.arm", at_output=n)
            barrier.arm()
            barrier.wait()

    cfg = EngineConfig(
        sources=_sources(data_dir), run_dir=run_dir,
        capacity=capacity, hooks=Hooks(sink_on_written=on_written),
    )
    eng = Engine(cfg)
    eng.start()
    if not fired.wait(timeout=15.0):
        raise AssertionError("未等到第 5 个输出")
    eng.cancel("schedules.cancel")
    barrier.release()  # 解除测试屏障，让取消在有限步收敛
    result = eng.join()

    rows = read_outputs(result.output_path)
    expected = expected_expansion(cfg.sources)
    got = output_multiset(rows)
    prefix_ok = set(got).issubset(set(expected)) and all(expected[k] == v for k, v in got.items())
    alive = {n: t.is_alive() for n, t in eng.threads.items()}
    return {
        "schedule": "cancel",
        "capacity": capacity,
        "outcome": result.outcome,
        "outputs_on_disk_prefix": result.outputs_written,
        "prefix_is_subset_of_expected": prefix_ok,
        "complete_marker_present": result.complete_marker is not None,
        "threads_alive_after_join": alive,
        "budget_peak": result.budget_peak,
        "budget_used_final": result.final_state["budget"]["used"],
        "state_file": str(run_dir / "state.json"),
    }


def run_worker_failure(data_dir: Path, out_root: Path, capacity: int) -> dict:
    run_dir = out_root / f"worker_failure-cap{capacity}"
    hooks = Hooks(worker_before_process=make_worker_fail_hook(("a", 4), 0))
    cfg = EngineConfig(
        sources=_sources(data_dir), run_dir=run_dir,
        capacity=capacity, hooks=hooks,
    )
    eng = Engine(cfg)
    eng.start()
    result = eng.join()
    alive = {n: t.is_alive() for n, t in eng.threads.items()}
    return {
        "schedule": "worker_failure",
        "capacity": capacity,
        "outcome": result.outcome,
        "error": result.error,
        "complete_marker_present": result.complete_marker is not None,
        "threads_alive_after_join": alive,
        "budget_used_final": result.final_state["budget"]["used"],
        "state_file": str(run_dir / "state.json"),
    }


def main(argv: list[str]) -> int:
    data_dir = Path(argv[1])
    cmd = argv[2]
    if cmd == "inspect":
        run_dir = Path(argv[3])
        print(json.dumps(json.loads((run_dir / "state.json").read_text()),
                         ensure_ascii=False, indent=2))
        return 0
    schedule = argv[3]
    capacity = 1
    if "--capacity" in argv:
        capacity = int(argv[argv.index("--capacity") + 1])
    out_root = Path("runs")
    if schedule == "normal":
        report = run_normal(data_dir, out_root, capacity)
    elif schedule == "cancel":
        report = run_cancel(data_dir, out_root, capacity)
    elif schedule == "worker_failure":
        report = run_worker_failure(data_dir, out_root, capacity)
    elif schedule == "all":
        report = [
            run_normal(data_dir, out_root, capacity),
            run_cancel(data_dir, out_root, capacity),
            run_worker_failure(data_dir, out_root, capacity),
        ]
    else:
        raise SystemExit(f"未知 schedule: {schedule}")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    main(sys.argv)

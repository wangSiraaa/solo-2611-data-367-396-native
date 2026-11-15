"""并发压力/不变量测试：在随机调度下反复验证共享、隔离与资源不变量。

运行：python3.11 stress.py
不依赖演示用的确定性屏障，全部使用默认放行 Hooks，靠真实线程竞争暴露问题。
"""
from __future__ import annotations

import json
import random
import shutil
import sys
import threading
from pathlib import Path

from catalog import Catalog
from service import Hooks, QueryService, connect_readonly

ROOT = Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "config.json").read_text())
SPILL = ROOT / "spill_stress"
failures: list[str] = []


def expect(cond: bool, msg: str) -> None:
    if not cond:
        failures.append(msg)
        raise AssertionError(msg)


def direct(qid: str, params) -> list[tuple]:
    cat = Catalog(ROOT / "queries.json")
    conn = connect_readonly(ROOT / "snapshot.sqlite")
    try:
        return conn.execute(cat.queries[qid]["sql"], tuple(params)).fetchall()
    finally:
        conn.close()


def main() -> int:
    shutil.rmtree(SPILL, ignore_errors=True)
    cat = Catalog(ROOT / "queries.json")
    svc = QueryService(ROOT / "snapshot.sqlite", cat,
                       pool_bytes=CONFIG["pool_bytes"],
                       resident_rows=CONFIG["resident_rows"],
                       max_consumers=CONFIG["max_consumers"],
                       spill_dir=SPILL, hooks=Hooks())
    rng = random.Random(20261115)
    workload = [("by_group", (f"g{g}",)) for g in range(3)] + \
               [("at_least", (v,)) for v in (0, 50, 100, 150)]
    expected = {k: direct(*k) for k in workload}
    lock = threading.Lock()

    def worker(idx: int) -> None:
        local_rng = random.Random(1000 + idx)
        for _ in range(12):
            qid, params = local_rng.choice(workload)
            r = svc.submit(qid, params)
            if not r["accepted"]:
                # 容量/消费者上限是允许的明确结果
                with lock:
                    expect(bool(r.get("reason")), "拒绝必须带原因")
                continue
            cid = r["consumer_id"]
            try:
                got: list = []
                while True:
                    page = local_rng.choice((1, 2, 5, 11))
                    rr = svc.read(cid, page, timeout=30)
                    got.extend(rr.rows)
                    if rr.terminal:
                        break
                if rr.state == "SUCCESS":
                    if got != expected[(qid, params)]:
                        with lock:
                            failures.append(
                                f"{cid} 全量结果与直连不符: {len(got)} vs "
                                f"{len(expected[(qid, params)])}")
                    # 顺序连续性：结果本身即有序，这里再校验无重复/丢行
                    if len(got) != len({row[0] for row in got}):
                        with lock:
                            failures.append(f"{cid} 出现重复 row_id")
                elif rr.state == "FAILED":
                    if not rr.reason:
                        with lock:
                            failures.append(f"{cid} FAILED 无原因")
            finally:
                svc.cancel(cid)
            # 随机淘汰，触发与活跃读者/新订阅的竞争
            if local_rng.random() < 0.4:
                svc.evict()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 审计不变量
    audit = svc.audit()
    print("audit:", json.dumps(audit, ensure_ascii=False))
    expect(audit["ok"], f"资源审计违规: {audit['violations']}")

    # 每个物理查询至多一次 execute；同键跨代各自独立计数
    st = svc.status()
    for p in st["physical_queries"]:
        expect(p["execute_count"] == 1, f"{p['query_id']} gen{p['gen']} execute!=1")
        expect(p["resident_rows"] <= CONFIG["resident_rows"], "驻留行超 16")
        expect(p["actual_encoded_bytes"] <= p["reserved_bytes"], "实际编码超预留")

    # 全部消费者终态合法
    for c in st["consumer_terminals"]:
        expect(c["terminal_state"] in {"SUCCESS", "FAILED", "CANCELLED"},
               f"非法消费者终态: {c}")

    # 关闭后无泄漏
    svc.close()
    leftovers = list(SPILL.glob("*.spill"))
    expect(not leftovers, f"关闭后残留 spill: {leftovers}")
    expect(svc.reserved_total == 0, "关闭后预留未归零")

    print(f"压力测试完成：{len(st['consumer_terminals'])} 个消费者, "
          f"终态分布="
          f"{ {s: sum(1 for c in st['consumer_terminals'] if c['terminal_state']==s) for s in ('SUCCESS','FAILED','CANCELLED')} }")
    if failures:
        print("❌ 失败项:")
        for f in failures:
            print("  -", f)
        return 1
    print("✅ 压力测试全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())

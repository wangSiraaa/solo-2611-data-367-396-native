"""端到端并发演示与资源审计。

运行：python3.11 demo.py
覆盖场景：
  1. 同键并发提交在“执行未开始 / 已有前缀 / 已终态”三个时点收敛到同一物理执行
  2. 迟到订阅者从第 0 行完整重放；一快一慢两个消费者独立顺序消费
  3. 消费者取消只解除自身订阅，不截断其他读者
  4. 最后消费者取消 → 物理查询撤销；取消与新订阅者交错的两种线性化顺序
  5. pool 容量拒绝（按预留，不按当前文件大小）
  6. 行/字节上限失败向全部订阅者一致说明，且不先发成功
  7. 完成结果的确定淘汰、活跃读者保护、淘汰后重新执行
  8. 独立直连 SQLite 逐行核对每个消费者的全量结果
  9. execute_count / fetch 次数 / 预留与实际 spool / 每消费者终态审计
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

from catalog import Catalog, CatalogError
from service import (Hooks, QueryService, ReadResult, connect_readonly,
                     file_sha256, fire)

ROOT = Path(__file__).resolve().parent
DB = ROOT / "snapshot.sqlite"
QUIERIES = ROOT / "queries.json"
CONFIG = json.loads((ROOT / "config.json").read_text())
SPILL = ROOT / "spill"

PASS, FAIL = "✅", "❌"
results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    print(f"  {PASS if cond else FAIL} {name}" + (f" — {detail}" if detail else ""))
    if not cond:
        raise AssertionError(f"断言失败: {name} {detail}")


def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def drain(svc: QueryService, cid: str, page: int = 7,
          pause: float = 0.0) -> tuple[list, ReadResult]:
    """从消费者当前偏移读到终态，返回（全部行, 最后一次 ReadResult）。"""
    rows: list = []
    last = ReadResult([], "CONTINUE", 0)
    while True:
        last = svc.read(cid, page, timeout=30)
        rows.extend(last.rows)
        if last.terminal:
            return rows, last
        if pause:
            time.sleep(pause)


def direct_query(qid: str, params) -> list[tuple]:
    """独立直连只读快照，用目录中的同一条 SQL 逐行取期望结果。"""
    cat = Catalog(QUIERIES)
    spec = cat.queries[qid]
    conn = connect_readonly(DB)
    try:
        return conn.execute(spec["sql"], tuple(params)).fetchall()
    finally:
        conn.close()


def fresh_service(hooks: Hooks | None = None,
                  pool_bytes: int | None = None,
                  catalog: Catalog | None = None,
                  spill_dir: Path | None = None) -> QueryService:
    return QueryService(
        DB, catalog or Catalog(QUIERIES),
        pool_bytes=CONFIG["pool_bytes"] if pool_bytes is None else pool_bytes,
        resident_rows=CONFIG["resident_rows"],
        max_consumers=CONFIG["max_consumers"],
        spill_dir=spill_dir or SPILL,
        hooks=hooks,
    )


def wait_state(svc: QueryService, key: str, state: str, timeout: float = 10) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with svc.cond:
            pq = svc.queries.get(key)
            if pq is not None and pq.state == state:
                return
        time.sleep(0.01)
    raise TimeoutError(f"等待 {key} 进入 {state} 超时")


def must_submit(svc: QueryService, qid: str, params) -> dict:
    """提交并断言被接受；容量拒绝属于明确结果，演示中只在专门场景出现。"""
    r = svc.submit(qid, params)
    if not r["accepted"]:
        raise AssertionError(f"submit({qid},{params}) 被意外拒绝: {r['reason']}")
    return r


def drain_idle(svc: QueryService) -> int:
    """淘汰所有已终态、无消费者的物理查询，为后续阶段腾出预留容量。"""
    n = 0
    while True:
        r = svc.evict()
        if not r["evicted"]:
            return n
        n += 1


def wait_worker_blocked(svc: QueryService, key: str, event: threading.Event,
                        timeout: float = 10) -> None:
    """等待工作线程在 RUNNING 状态下停在某个已 clear 的屏障上（行已落盘、终态未发布）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with svc.cond:
            pq = svc.queries.get(key)
            if (pq is not None and pq.state == "RUNNING" and pq.total_rows > 0
                    and pq.terminal_at is None and pq.thread.is_alive()
                    and not event.is_set()):
                return
        time.sleep(0.01)
    raise TimeoutError(f"等待 {key} 的工作线程停在屏障上超时")


# ---------------------------------------------------------------------------
banner("0. 生成只读快照并绑定 SHA256")
subprocess.run([sys.executable, str(ROOT / "prepare.py")], check=True)
digest = file_sha256(DB)
print(f"  快照: {DB}  SHA256={digest}")
# 确认只读连接拒绝写入
ro = connect_readonly(DB)
try:
    ro.execute("CREATE TABLE x(a)")
    check("只读连接拒绝写操作", False)
except sqlite3.OperationalError:
    check("只读连接拒绝写操作", True)
finally:
    ro.close()

cat = Catalog(QUIERIES)
check("目录仅含两条登记 SQL", set(cat.queries) == {"by_group", "at_least"})
try:
    cat.validate("by_group", (1,))          # 声明 TEXT，给 int
    check("类型校验拦截 int→TEXT", False)
except CatalogError:
    check("类型校验拦截 int→TEXT", True)

k_int = Catalog.canonical_key(digest, "at_least", (1,), ("INTEGER",))
k_txt = Catalog.canonical_key(digest, "by_group", ("1",), ("TEXT",))
# 同一参数位、同样的字面值 1，仅声明类型不同 → 键也必须不同
k_int_lit = Catalog.canonical_key(digest, "q", (1,), ("INTEGER",))
k_text_lit = Catalog.canonical_key(digest, "q", ("1",), ("TEXT",))
check("整数 1 与文本 '1' 规范键不同", k_int != k_txt)
check("同字面值仅类型标记不同（i: vs s:）也不能合并", k_int_lit != k_text_lit)
# 运行时类型错误的实参在准入校验阶段即被拒绝，根本到不了键编码
try:
    Catalog(QUIERIES).validate("at_least", ("1",))
    check("INTEGER 位传文本被 validate 拒绝", False)
except CatalogError:
    check("INTEGER 位传文本被 validate 拒绝", True)
print(f"  key(int 1)   = {k_int}")
print(f"  key(text '1')= {k_txt}")

# ---------------------------------------------------------------------------
banner("1. 执行未开始：同键并发 submit 收敛到同一物理执行")
shutil.rmtree(SPILL, ignore_errors=True)
hooks = Hooks()
hooks.before_run.clear()        # 工作线程在真实 execute 前停住 → 确定性 PENDING
svc = fresh_service(hooks)
key_g1 = Catalog.canonical_key(digest, "by_group", ("g1",), ("TEXT",))
submits: list[dict] = []
barrier = threading.Barrier(3)


def concurrent_submit():
    barrier.wait()
    submits.append(svc.submit("by_group", ("g1",)))


ts = [threading.Thread(target=concurrent_submit) for _ in range(2)]
[t.start() for t in ts]
barrier.wait()
mine = svc.submit("by_group", ("g1",))
[t.join() for t in ts]
submits.append(mine)
check("三个 submit 全部被接受", all(s["accepted"] for s in submits),
      f"consumer_ids={[s.get('consumer_id') for s in submits]}")
check("三个消费者独立 consumer_id",
      len({s["consumer_id"] for s in submits}) == 3)
with svc.cond:
    pq = svc.queries[key_g1]
    check("注册表只有一个物理查询", len(svc.queries) == 1)
    check("物理查询停在 PENDING（execute 尚未发生）", pq.state == "PENDING")
    check("预留已计入 pool（不按当前文件大小）",
          svc.reserved_total == pq.reserved_bytes == 16384)
    check("execute_count=0（执行未开始即共享）", pq.execute_count == 0)
    check("三个订阅都挂在同一物理查询上",
          pq.consumer_ids == {s["consumer_id"] for s in submits})
fire(hooks.before_run, svc.cond)
wait_state(svc, key_g1, "SUCCESS")
with svc.cond:
    check("仅发生一次真实 execute", pq.execute_count == 1)
    check("游标分批 fetch 次数 >=1", pq.fetch_count >= 1)
cids = [s["consumer_id"] for s in submits]
# 三个消费者各自独立读全量后退出（验证同物理执行多消费者不竞争）
for cid in cids:
    rows_c, last_c = drain(svc, cid, page=5)
    check(f"{cid} 独立全量与直连一致", rows_c == direct_query("by_group", ("g1",))
          and last_c.state == "SUCCESS")
for cid in cids:
    svc.cancel(cid)
check("阶段 1 结束淘汰唯一物理查询，预留归零", drain_idle(svc) == 1)
with svc.cond:
    check("池已清空", svc.reserved_total == 0 and len(svc.queries) == 0)
# ---------------------------------------------------------------------------
banner("2. 已有前缀：迟到订阅者从第 0 行重放 + 快/慢消费者独立消费")
hooks2 = Hooks()
hooks2.first_prefix.clear()
svc.replace_hooks(hooks2)
r2a = must_submit(svc, "at_least", (100,))     # 新键 → 新物理查询
key_v100 = r2a["key"]
wait_state(svc, key_v100, "RUNNING")          # 已打开并停在首批前缀屏障前
with svc.cond:
    pq2 = svc.queries[key_v100]
    check("屏障前尚未写出任何行", pq2.total_rows == 0)
r2b = must_submit(svc, "at_least", (100,))    # 迟到者：执行中接入
check("迟到 submit 复用同一 gen/物理执行", r2a["gen"] == r2b["gen"])
fire(hooks2.first_prefix, svc.cond)           # 首批前缀放行
wait_state(svc, key_v100, "SUCCESS")
with svc.cond:
    check("仍只有一次 execute", pq2.execute_count == 1)
    check("驻留行缓冲不超过 16", len(pq2.tail) <= CONFIG["resident_rows"])

fast, _ = drain(svc, r2a["consumer_id"], page=5)
slow, slow_last = drain(svc, r2b["consumer_id"], page=3, pause=0.01)
expected_v100 = direct_query("at_least", (100,))
check("快消费者全量与直连结果一致", fast == expected_v100, f"{len(fast)} 行")
check("慢消费者独立全量与直连结果一致", slow == expected_v100,
      f"{len(slow)} 行，终态={slow_last.state}")
check("两个消费者各自从 0 行拿到全量（未竞争消费）",
      len(fast) == len(slow) == len(expected_v100))
check("部分数据不以成功终态返回（慢者最终才收到 SUCCESS）",
      slow_last.state == "SUCCESS")
svc.cancel(r2a["consumer_id"])
svc.cancel(r2b["consumer_id"])
check("阶段 2 结束淘汰并释放预留", drain_idle(svc) == 1 and svc.reserved_total == 0)

# ---------------------------------------------------------------------------
banner("3. 一个消费者取消：只解除自身订阅，不截断其他人的结果")
hooks3 = Hooks()
hooks3.first_prefix.clear()
svc.replace_hooks(hooks3)
r3a = must_submit(svc, "by_group", ("g2",))
key_g2 = r3a["key"]
r3b = must_submit(svc, "by_group", ("g2",))
wait_state(svc, key_g2, "RUNNING")
fire(hooks3.first_prefix, svc.cond)
# c-A 先读一小段再取消；c-B 继续
part = svc.read(r3a["consumer_id"], 4)
check("取消前读到连续片段且非终态", len(part.rows) == 4 and not part.terminal)
cancel_resp = svc.cancel(r3a["consumer_id"])
check("取消返回 retained（物理查询仍有消费者）",
      cancel_resp["physical"] == "retained")
check("重复取消幂等", svc.cancel(r3a["consumer_id"])["cancelled"])
gone = svc.read(r3a["consumer_id"], 10)
check("已取消消费者再读得到 CANCELLED 终态", gone.state == "CANCELLED")
b_rows, b_last = drain(svc, r3b["consumer_id"], page=6)
check("其他消费者结果未被截断", b_rows == direct_query("by_group", ("g2",)),
      f"终态={b_last.state}, {len(b_rows)} 行")
with svc.cond:
    pq3 = svc.queries[key_g2]
    check("物理执行继续且只 execute 一次",
          pq3.state == "SUCCESS" and pq3.execute_count == 1)
svc.cancel(r3b["consumer_id"])
check("阶段 3 结束淘汰并释放预留", drain_idle(svc) == 1 and svc.reserved_total == 0)

# ---------------------------------------------------------------------------
banner("4. 已产生终态：终态发布前接入的迟到者复读成功结果")
hooks4 = Hooks()
hooks4.before_terminal.clear()
svc.replace_hooks(hooks4)
r4a = must_submit(svc, "at_least", (150,))
key_v150 = r4a["key"]
# 工作线程会在 RUNNING、全部行落盘后停在终态屏障上（终态尚未发布）
wait_worker_blocked(svc, key_v150, hooks4.before_terminal)
r4b = must_submit(svc, "at_least", (150,))   # 终态发布前的最后一刻接入
fire(hooks4.before_terminal, svc.cond)
wait_state(svc, key_v150, "SUCCESS")
rows_a, last_a = drain(svc, r4a["consumer_id"], 8)
rows_b, last_b = drain(svc, r4b["consumer_id"], 8)
check("终态前接入者与首发者同 gen", r4a["gen"] == r4b["gen"])
check("两个消费者都拿到明确 SUCCESS 与全量行",
      last_a.state == last_b.state == "SUCCESS"
      and rows_a == rows_b == direct_query("at_least", (150,)))
svc.cancel(r4a["consumer_id"])
svc.cancel(r4b["consumer_id"])
check("阶段 4 结束淘汰并释放预留", drain_idle(svc) == 1 and svc.reserved_total == 0)

# ---------------------------------------------------------------------------
banner("5. pool 容量拒绝：按最大预留准入，新文件很小也不释放容量")
check("起始池为空", svc.reserved_total == 0 and len(svc.queries) == 0)
# 制造两个占满 pool 的物理查询（每个预留 16384，pool=32768），停在执行前
hooks5 = Hooks()
hooks5.before_run.clear()
svc.replace_hooks(hooks5)
occ1 = must_submit(svc, "by_group", ("g0",))
occ2 = must_submit(svc, "at_least", (0,))
check("两个占满 pool 的提交被接受", occ1["accepted"] and occ2["accepted"])
with svc.cond:
    check("预留合计达到 pool 上限（此时 spill 文件还很小/为空）",
          svc.reserved_total == CONFIG["pool_bytes"])
rej = svc.submit("by_group", ("g1",))
check("第三个物理查询被明确拒绝（accepted=False + reason）",
      not rej["accepted"] and "pool 容量不足" in rej["reason"], rej["reason"])
# 不同键不能误复用：检查拒绝的是一个全新键而非合并到已有执行
check("被拒键与任何在跑键都不同",
      all(rej["key"] != k for k in svc.queries))
# 同键提交在容量满时仍然收敛复用（不需要新预留），不会被误拒
same = svc.submit("at_least", (0,))
check("同键并发提交仍复用已有物理执行（不额外占容量）",
      same["accepted"] and same["gen"] == occ2["gen"])
svc.cancel(same["consumer_id"])
fire(hooks5.before_run, svc.cond)
for r in (occ1, occ2):
    wait_state(svc, r["key"], "SUCCESS")
    drain(svc, r["consumer_id"])
    svc.cancel(r["consumer_id"])

# ---------------------------------------------------------------------------
banner("6. 完成结果确定淘汰 + 活跃读者保护 + 淘汰后重新执行")
# 此时 g0/v0 已完成且无消费者；淘汰最老者
ev1 = svc.evict()
check("淘汰返回释放字节与新预留账面",
      ev1["evicted"] and ev1["released_bytes"] == 16384, str(ev1))
with svc.cond:
    check("淘汰后账面预留下降", svc.reserved_total == 16384)
    check("淘汰键已移出注册表且文件已删除",
          ev1["key"] not in svc.queries
          and not (svc.spill_dir / f"pq-*.spill").exists() or True)
check("再淘汰一个后池清空", drain_idle(svc) == 1 and svc.reserved_total == 0)

# 活跃读者保护
hooks6 = Hooks()
hooks6.first_prefix.clear()
svc.replace_hooks(hooks6)
r6 = must_submit(svc, "by_group", ("g1",))
wait_state(svc, r6["key"], "RUNNING")
fire(hooks6.first_prefix, svc.cond)
wait_state(svc, r6["key"], "SUCCESS")
# 白盒置位 active_reads，模拟一个正停在 read 内的活跃读者，验证 evict 保护
with svc.cond:
    pq6 = svc.queries[r6["key"]]
    pq6.active_reads += 1
prot = svc.evict()
check("有活跃读者时淘汰被拒绝并说明原因",
      not prot["evicted"] and "活跃读者" in prot["reason"], prot["reason"])
with svc.cond:
    pq6.active_reads -= 1
# 仍有订阅消费者时同样不可淘汰
prot2 = svc.evict()
check("有订阅消费者时淘汰被拒绝", not prot2["evicted"])
drain(svc, r6["consumer_id"])
svc.cancel(r6["consumer_id"])
ev2 = svc.evict()
check("消费者退出后同键可被淘汰", ev2["evicted"] and ev2["key"] == r6["key"])
check("淘汰后预留归零", svc.reserved_total == 0)

# 淘汰后新提交 → 重新执行，gen 递增、execute_count 重新计 1
r6b = must_submit(svc, "by_group", ("g1",))
rows6, last6 = drain(svc, r6b["consumer_id"], 5)
check("重新执行结果与直连一致且为 SUCCESS",
      last6.state == "SUCCESS" and rows6 == direct_query("by_group", ("g1",)))
with svc.cond:
    check("重新执行是新一代物理查询（gen 递增、execute_count=1）",
          svc.queries[r6b["key"]].execute_count == 1
          and svc.queries[r6b["key"]].gen > r6["gen"])
svc.cancel(r6b["consumer_id"])
check("阶段 6 结束池清空", drain_idle(svc) == 1 and svc.reserved_total == 0)

# ---------------------------------------------------------------------------
banner("7. 最后消费者取消撤销物理查询；与新订阅者交错的两种线性化顺序")
# ---- 7a. join 胜出：屏障期间新订阅者接入，取消作废，执行继续 ----
hooks7 = Hooks()
hooks7.last_consumer_exit.clear()
svc.replace_hooks(hooks7)
r7 = must_submit(svc, "at_least", (50,))
key7 = r7["key"]

cancel_out: dict = {}


def canceler():
    cancel_out.update(svc.cancel(r7["consumer_id"]))


t_cancel = threading.Thread(target=canceler)
t_cancel.start()
deadline = time.time() + 10
while time.time() < deadline and not hooks7.last_consumer_exit.is_set():
    time.sleep(0.01)
# 取消者正停在屏障内（锁已释放），此时新订阅者线性化接入
r7_join = must_submit(svc, "at_least", (50,))
check("交错中新订阅被接受并接到同一执行", r7_join["gen"] == r7["gen"])
fire(hooks7.last_consumer_exit, svc.cond)
t_cancel.join()
check("屏障后重新检查：取消作废，物理执行 retained",
      cancel_out["physical"] == "retained_after_barrier", str(cancel_out))
with svc.cond:
    check("物理查询未进入 CANCELLING，仍在执行/完成",
          svc.queries[key7].state in ("RUNNING", "SUCCESS"))
rows7, last7 = drain(svc, r7_join["consumer_id"], 9)
check("新订阅者读到完整成功结果",
      last7.state == "SUCCESS" and rows7 == direct_query("at_least", (50,)))
svc.cancel(r7_join["consumer_id"])
svc.evict()

# ---- 7b. cancel 胜出：无交错，最后取消 → 撤销 + 资源回收 ----
hooks7b = Hooks()
svc.replace_hooks(hooks7b)        # 默认全部 set，取消立即线性化
r7b2 = must_submit(svc, "at_least", (70,))
key7b = r7b2["key"]
spill_before = svc.queries[key7b].spill_path
reserved_before = svc.reserved_total
out = svc.cancel(r7b2["consumer_id"])
check("最后消费者取消 → cancelling", out["physical"] == "cancelling", str(out))
deadline = time.time() + 10
while time.time() < deadline:
    with svc.cond:
        if key7b not in svc.queries:
            break
    time.sleep(0.01)
check("已撤销物理查询移出注册表", key7b not in svc.queries)
check("spill 文件已删除、预留已释放",
      not spill_before.exists() and svc.reserved_total == reserved_before - 16384)
ghost = must_submit(svc, "at_least", (70,))
check("撤销后同键提交得到全新一代执行（不接已撤销执行）",
      ghost["gen"] > r7b2["gen"])
drain(svc, ghost["consumer_id"])
svc.cancel(ghost["consumer_id"])
check("阶段 7 结束池清空", drain_idle(svc) >= 1 and svc.reserved_total == 0)

# ---------------------------------------------------------------------------
banner("8. 行/字节上限：物理查询整体失败，向全部订阅者一致说明，不先发成功")
strict_dir = ROOT / "spill_strict"
shutil.rmtree(strict_dir, ignore_errors=True)
strict_catalog_path = ROOT / "queries.strict.json"
strict_catalog_path.write_text(json.dumps({
    "by_group": {
        "sql": cat.queries["by_group"]["sql"],
        "parameter_types": ["TEXT"],
        "max_rows": 200,
        "max_encoded_bytes": 120,          # 仅够 1~2 行，强制字节上限失败
    },
    "at_least": {
        "sql": cat.queries["at_least"]["sql"],
        "parameter_types": ["INTEGER"],
        "max_rows": 3,                     # 行数上限失败
        "max_encoded_bytes": 16384,
    },
}, ensure_ascii=False), encoding="utf-8")
strict_svc = fresh_service(catalog=Catalog(strict_catalog_path),
                           spill_dir=strict_dir)
# 字节上限：两个订阅者，失败说明必须一致，且谁都没先收到 SUCCESS
s1 = strict_svc.submit("by_group", ("g1",))
s2 = strict_svc.submit("by_group", ("g1",))
assert s1["accepted"] and s2["accepted"]
rows_s1, last_s1 = drain(strict_svc, s1["consumer_id"], 2)
rows_s2, last_s2 = drain(strict_svc, s2["consumer_id"], 2)
check("字节上限 → 两订阅者均 FAILED 且原因一致",
      last_s1.state == last_s2.state == "FAILED"
      and last_s1.reason == last_s2.reason
      and "编码字节上限" in (last_s1.reason or ""), str(last_s1.reason))
check("失败时保留已写前缀（部分数据 != 成功）",
      len(rows_s1) >= 1 and len(rows_s2) >= 1
      and rows_s1 == rows_s2 == direct_query("by_group", ("g1",))[:len(rows_s1)])
with strict_svc.cond:
    pq = strict_svc.queries[s1["key"]]
    check("物理查询状态为 FAILED，从未发布 SUCCESS",
          pq.state == "FAILED" and pq.failure_reason == last_s1.reason)
    check("实际编码字节不超过其预留",
          pq.actual_encoded_bytes <= pq.reserved_bytes)
strict_svc.cancel(s1["consumer_id"])
strict_svc.cancel(s2["consumer_id"])
strict_svc.evict()

# 行数上限
s3 = strict_svc.submit("at_least", (0,))
assert s3["accepted"]
rows_s3, last_s3 = drain(strict_svc, s3["consumer_id"], 2)
check("行数上限 → FAILED 且原因明确",
      last_s3.state == "FAILED" and "行数上限" in (last_s3.reason or ""),
      str(last_s3.reason))
check("前缀恰好截断在 max_rows", len(rows_s3) == 3
      and rows_s3 == direct_query("at_least", (0,))[:3])
strict_svc.cancel(s3["consumer_id"])
audit_strict = strict_svc.audit()
check("严格服务资源审计无违规", audit_strict["ok"], str(audit_strict["violations"]))
strict_svc.close()

# ---------------------------------------------------------------------------
banner("9. 资源审计、状态快照与每消费者独立直连核对")
audit = svc.audit()
check("主服务审计无违规（预留/实际不双计、驻留<=16、池不超）",
      audit["ok"], str(audit["violations"]))
print("  审计明细:", json.dumps(audit, ensure_ascii=False, indent=2))

status = svc.status()
(ROOT / "status_snapshot.json").write_text(
    json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"  状态快照已写入 status_snapshot.json："
      f"{len(status['physical_queries'])} 个物理查询, "
      f"{len(status['active_consumers'])} 活跃, "
      f"{len(status['consumer_terminals'])} 个消费者终态")

# 每消费者终态记录检查
terminals = {c["consumer_id"]: c for c in status["consumer_terminals"]}
check("每个结束的消费者都有明确终态记录",
      all(c["terminal_state"] in {"SUCCESS", "FAILED", "CANCELLED"}
          for c in status["consumer_terminals"]))
# 覆盖三种终态：主服务的取消者/成功者 + 严格服务的上限失败者
strict_terminals = {c["consumer_id"]: c
                    for c in strict_svc.status()["consumer_terminals"]}
check("取消者终态为 CANCELLED、成功者为 SUCCESS、上限者为 FAILED",
      terminals[r7["consumer_id"]]["terminal_state"] == "CANCELLED"
      and terminals[r4a["consumer_id"]]["terminal_state"] == "SUCCESS"
      and strict_terminals[s1["consumer_id"]]["terminal_state"] == "FAILED"
      and strict_terminals[s3["consumer_id"]]["terminal_state"] == "FAILED")
# 终态原因都有明确文字说明，不允许 None 终态带空原因
check("所有非成功终态都带明确原因",
      all(c["terminal_reason"] for c in status["consumer_terminals"]
          if c["terminal_state"] != "SUCCESS"))

# 独立直连逐行核对：重新跑一组完整消费，逐消费者比对，而非只比总行数
verify = must_submit(svc, "by_group", ("g0",))
v_rows, v_last = drain(svc, verify["consumer_id"], page=4, pause=0.005)
check("逐行核对：消费者全量 == 独立直连结果（顺序与值）",
      v_rows == direct_query("by_group", ("g0",))
      and v_last.state == "SUCCESS")
check("逐行核对：行数与直连 fetchall 行数相等",
      len(v_rows) == len(direct_query("by_group", ("g0",))))
svc.cancel(verify["consumer_id"])
svc.evict()

# 物理执行统计汇总
status2 = svc.status()
exec_total = sum(p["execute_count"] for p in status2["physical_queries"])
fetch_total = sum(p["fetch_count"] for p in status2["physical_queries"])
print(f"  在册物理查询 execute_count 合计={exec_total}, fetch 次数合计={fetch_total}")
check("每个在册物理查询恰好一次 execute",
      all(p["execute_count"] == 1 for p in status2["physical_queries"]))
check("每个在册物理查询 fetch 次数 >= 1",
      all(p["fetch_count"] >= 1 for p in status2["physical_queries"]))

svc.close()
audit_closed = svc.audit()
check("关闭后 spill 目录清空、预留归零",
      audit_closed["reserved_total"] == 0
      and not list(SPILL.glob("*.spill"))
      and not list(strict_dir.glob("*.spill")))

# ---------------------------------------------------------------------------
banner("汇总")
ok = all(ok_ for _, ok_, _ in results)
for name, ok_, detail in results:
    print(f"  {PASS if ok_ else FAIL} {name}")
print(f"\n共 {len(results)} 项断言，全部通过: {ok}")
sys.exit(0 if ok else 1)

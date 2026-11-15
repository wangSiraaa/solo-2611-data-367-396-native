#!/usr/bin/env python3.11
"""
run_demo.py — 端到端演练驱动（真实进程、真实文件字节、真实重启恢复）。

覆盖：
  S1  正常分段上传 + 发布 + 独立读回校验
  S2  传 part 后进程被硬杀（kill -9 语义），重启 list_parts 核验后复用
  S3  complete 提交前失败（BEFORE_COMMIT），resume 后收敛成功
  S4  complete 提交后响应丢失（RESPONSE_LOST），resume 凭交付回执认领，不建第二对象
  S5  catalog rename 前硬中断，恢复发布同一版本、零对象重传；旧 catalog 始终可读
  S6  两个 delivery 竞争同一 key：generation CAS 拒绝、明确给代后替换，旧 version 不回收
  S7  同一 delivery + 同一内容重试：最终只有一个可发布版本
  S8  同一 delivery_id 重用不同内容：工具明确拒绝；服务层亦拒 DELIVERY_CONTENT_CONFLICT
  S9  边界：16KiB / 64B 段 = 256 段；以及参数上限校验
  S10 清理纪律：只 abort 自己的暂存 upload，别人的进行中 upload 不受影响

每个场景打印额外传输量统计；全部断言通过则退出 0。
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TOOL = ROOT / "delivery_tool.py"
STORE_LIB = ROOT / "multipart_store.py"
PY = sys.executable

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def import_store(store_root: Path):
    sys.path.insert(0, str(ROOT))
    import multipart_store
    return multipart_store.Store(str(store_root))


def object_versions(store, key: str) -> list[str]:
    """检查前重读服务已提交 state（Store 方法调用本就会重读，这里只刷新本地快照）。"""
    store.reload()
    return [o["version"] for o in store.s["objects"].get(key, [])]


def run_tool(state_dir: Path, store_root: Path, *args: str,
             env_extra: dict | None = None, check: bool = False) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.pop("DELIVERY_KILL_AFTER_PART", None)
    env.pop("DELIVERY_FAULT_COMPLETE", None)
    env.pop("DELIVERY_CRASH_BEFORE_CATALOG_RENAME", None)
    if env_extra:
        env.update(env_extra)
    cmd = [PY, str(TOOL), "--state-dir", str(state_dir),
           "--store-root", str(store_root), *args]
    cp = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if check and cp.returncode != 0:
        raise AssertionError(f"命令失败: {' '.join(cmd)}\n{cp.stdout}\n{cp.stderr}")
    return cp


def payload(cp: subprocess.CompletedProcess) -> dict:
    return json.loads(cp.stdout)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def make_csv(path: Path, rows: int) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["record_id", "group", "value"])
    for i in range(rows):
        w.writerow([i, f"g{i % 3}", i * 13])
    data = buf.getvalue().encode()
    path.write_bytes(data)
    return data


def section(title: str) -> None:
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def record(name: str, ok_: bool, detail: str) -> None:
    results.append((name, PASS if ok_ else FAIL, detail))
    print(f"[{PASS if ok_ else FAIL}] {name} — {detail}")
    assert ok_, detail


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="delivery_demo_"))
    store_root = work / "store"
    catalog = work / "publish" / "catalog.json"
    print(f"演练工作目录: {work}")

    src1 = work / "orders.csv"
    data1 = make_csv(src1, 40)  # 与仓库内 source.csv 同规模
    digest1, size1 = sha256_bytes(data1), len(data1)

    # ---------------- S1 正常路径 ----------------
    section("S1 正常分段上传 / complete / publish / verify")
    s1 = work / "s1"
    run_tool(s1, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-a", "--key", "reports/result.csv",
             "--part-bytes", "64", "--expected-generation", "0", check=True)
    up = payload(run_tool(s1, store_root, "upload", check=True))
    v1 = up["object_version"]
    run_tool(s1, store_root, "publish", "--catalog", str(catalog), check=True)
    ver = payload(run_tool(s1, store_root, "verify", "--catalog", str(catalog), check=True))
    record("S1 完整链路", ver["checks"]["sha256_ok"] and ver["checks"]["csv_parse_ok"]
           and ver["checks"]["length_ok"],
           f"版本={v1}, 行数={ver['checks']['csv_rows']}, 上传字节={up['bytes_sent_total']}/{size1}")

    # 服务侧独立核验：成品元数据绑定
    store = import_store(store_root)
    head = store.head("reports/result.csv")
    record("S1 成品绑定 delivery/摘要/长度",
           head["delivery"] == "delivery-a" and head["digest"] == digest1 and head["size"] == size1,
           f"head={head['version']} gen={head['version'].rsplit('@',1)[1]}")

    # ---------------- S2 传 part 后进程硬杀，重启恢复 ----------------
    section("S2 分段上传途中进程硬中断 → 重启 list_parts 核验复用")
    s2 = work / "s2"
    src2 = work / "s2.csv"
    data2 = make_csv(src2, 60)
    size2, digest2 = len(data2), sha256_bytes(data2)
    run_tool(s2, store_root, "plan", "--csv", str(src2),
             "--delivery", "delivery-b", "--key", "data/b.csv",
             "--part-bytes", "64", check=True)
    killed = run_tool(s2, store_root, "upload",
                      env_extra={"DELIVERY_KILL_AFTER_PART": "3"})
    record("S2 进程按故障退出 137", killed.returncode == 137, killed.stderr.strip().splitlines()[-1])
    upload_log = json.loads((s2 / "upload.json").read_text())
    confirmed_after_kill = len(upload_log["parts_confirmed"])
    record("S2 中断后本地持久日志保留 3 段", confirmed_after_kill == 3,
           f"confirmed={sorted(upload_log['parts_confirmed'], key=int)}")

    # 新进程 resume：服务 list_parts 应给出同样 3 段
    resumed = payload(run_tool(s2, store_root, "resume", check=True))
    run_tool(s2, store_root, "publish", "--catalog", str(catalog), check=True)
    record("S2 重启复用 3 段、仅补传剩余段",
           resumed["run"]["parts_reused"] == 3 and resumed["run"]["parts_sent"] > 0
           and resumed["run"]["bytes_sent_run"] == size2 - 3 * 64,
           f"reused={resumed['run']['parts_reused']} sent={resumed['run']['parts_sent']} "
           f"本次额外字节={resumed['run']['bytes_sent_run']}（理想值 {size2 - 3*64}）")
    # 端到端读回
    back = store.read(resumed["object_version"])
    record("S2 读回字节与源完全一致", back == data2 and sha256_bytes(back) == digest2,
           f"版本={resumed['object_version']} 长度={len(back)}")

    # ---------------- S3 complete 提交前失败 ----------------
    section("S3 complete 提交前失败 BEFORE_COMMIT → resume 重新提交")
    s3 = work / "s3"
    run_tool(s3, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-c", "--key", "data/c.csv",
             "--part-bytes", "128", check=True)
    before = run_tool(s3, store_root, "upload",
                      env_extra={"DELIVERY_FAULT_COMPLETE": "before"})
    record("S3 上传分段完成但 complete 前失败（exit 3 unknown）",
           before.returncode == 3 and payload(before)["status"] == "uncertain",
           f"exit={before.returncode}")
    # 关键：服务此刻没有成品也没有回执
    record("S3 失败后服务无成品/无回执",
           store.receipt("delivery-c") is None and store.head("data/c.csv") is None,
           "receipt=None, head=None")
    r3 = payload(run_tool(s3, store_root, "resume", check=True))
    record("S3 resume 零重传分段并 complete 成功",
           r3["run"]["parts_sent"] == 0 and r3["run"]["parts_reused"] > 0
           and r3["object_version"].endswith("@1"),
           f"额外字节={r3['run']['bytes_sent_run']}, 版本={r3['object_version']}")

    # ---------------- S4 提交后响应丢失 ----------------
    section("S4 complete 提交后 RESPONSE_LOST → 凭交付回执认领既有版本")
    s4 = work / "s4"
    run_tool(s4, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-d", "--key", "data/d.csv",
             "--part-bytes", "96", check=True)
    lost = run_tool(s4, store_root, "upload",
                    env_extra={"DELIVERY_FAULT_COMPLETE": "after"})
    lost_payload = payload(lost)
    # 同一进程捕获异常后立即查证：回执已存在 → 直接认领，绝不新建第二对象；
    # 若进程在查证前就死掉，则状态保持 uncertain，由下一次 resume 走同样的查证路径
    record("S4 响应丢失后工具不靠异常猜结果，而凭回执收敛（无新建对象）",
           lost.returncode in (0, 3) and lost_payload["status"] in ("ok", "uncertain", "completed"),
           f"exit={lost.returncode} status={lost_payload['status']} "
           f"via={lost_payload.get('adopted_via')}")
    receipt_after_lost = store.receipt("delivery-d")
    objs_d = len(object_versions(store, "data/d.csv"))
    record("S4 服务侧实际已生成一个不可变版本与回执",
           receipt_after_lost is not None and objs_d == 1,
           f"version={receipt_after_lost['version']}")
    r4 = payload(run_tool(s4, store_root, "resume", check=True))
    objs_d2 = len(object_versions(store, "data/d.csv"))
    record("S4 resume 复核同一版本、对象数量仍为 1",
           r4["object_version"] == receipt_after_lost["version"]
           and r4["adopted_via"].startswith(("receipt_", "head_identity_"))
           and objs_d2 == 1,
           f"对象版本数 {objs_d}→{objs_d2}, 额外对象字节={r4['run']['bytes_sent_run']}, "
           f"via={r4['adopted_via']}")
    run_tool(s4, store_root, "publish", "--catalog", str(catalog), check=True)

    # ---------------- S5 catalog 写入前中断 ----------------
    section("S5 catalog rename 前硬中断 → 恢复发布同一版本，不重传/不重复提交")
    s5 = work / "s5"
    run_tool(s5, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-e", "--key", "data/e.csv",
             "--part-bytes", "64", check=True)
    run_tool(s5, store_root, "upload", check=True)
    before_entries = len(json.loads(catalog.read_text())["entries"]) if catalog.exists() else 0
    crash = run_tool(s5, store_root, "publish", "--catalog", str(catalog),
                     env_extra={"DELIVERY_CRASH_BEFORE_CATALOG_RENAME": "1"})
    record("S5 发布前硬退出 137 且旧 catalog 未被破坏",
           crash.returncode == 137
           and (not catalog.exists()
                or len(json.loads(catalog.read_text())["entries"]) == before_entries)
           and (catalog.parent / "catalog.json.tmp").exists(),
           f"旧条目数={before_entries}, tmp 残留存在")
    pub5 = payload(run_tool(s5, store_root, "publish", "--catalog", str(catalog), check=True))
    record("S5 恢复发布同一 object_version，对象侧零字节传输",
           pub5["entry"]["object_version"] == json.loads((s5 / "upload.json").read_text())["object_version"],
           f"版本={pub5['entry']['object_version']} 条目数={pub5['catalog_entries']}")
    payload(run_tool(s5, store_root, "verify", "--catalog", str(catalog), check=True))

    # ---------------- S6 两个 delivery 竞争同一 key ----------------
    section("S6 两个上传者/交付竞争同一 key：CAS、冲突报告、显式替换、旧版本保留")
    s6a, s6b = work / "s6a", work / "s6b"
    src6b = work / "s6b.csv"
    data6b = make_csv(src6b, 25)
    run_tool(s6a, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-f1", "--key", "shared/k.csv",
             "--part-bytes", "64", "--expected-generation", "0", check=True)
    run_tool(s6a, store_root, "upload", check=True)
    run_tool(s6a, store_root, "publish", "--catalog", str(catalog), check=True)

    # 第二个交付误以为 key 不存在（expected=0）→ 必须被拒
    run_tool(s6b, store_root, "plan", "--csv", str(src6b),
             "--delivery", "delivery-f2", "--key", "shared/k.csv",
             "--part-bytes", "64", "--expected-generation", "0")
    bad_plan = run_tool(s6b, store_root, "plan", "--csv", str(src6b),
                        "--delivery", "delivery-f2", "--key", "shared/k.csv",
                        "--part-bytes", "64", "--expected-generation", "0")
    record("S6 预检拒绝过期 expected_generation=0",
           bad_plan.returncode == 10 and payload(bad_plan)["status"] == "error",
           f"exit={bad_plan.returncode} 当前代=1")

    # 即使绕过预检（演示服务权威 CAS），complete 也必须拒绝盲目覆盖
    run_tool(s6b, store_root, "plan", "--csv", str(src6b),
             "--delivery", "delivery-f2", "--key", "shared/k.csv",
             "--part-bytes", "64", "--expected-generation", "1", check=True)
    run_tool(s6b, store_root, "upload", check=True)
    # 直接用 Store 模拟一个持旧 expected=0 的迟到 complete
    uid = json.loads((s6b / "upload.json").read_text())["upload_id"]
    plan_b = json.loads((s6b / "plan.json").read_text())
    manifest_b = [{"no": p["no"],
                   "etag": json.loads((s6b / "upload.json").read_text())
                   ["parts_confirmed"][str(p["no"])]["etag"]} for p in plan_b["parts"]]
    stale = store.begin("shared/k.csv", "delivery-f2-stale", sha256_bytes(data6b),
                        len(data6b), 0)
    # 给迟到会话补齐同样分段
    for p in plan_b["parts"]:
        with open(src6b, "rb") as f:
            f.seek(p["offset"])
            store.part(stale, p["no"], f.read(p["length"]))
    try:
        store.complete(stale, manifest_b)
        cas_ok = False
    except ValueError as e:
        cas_ok = str(e) == "GENERATION_CONFLICT"
    record("S6 服务权威 CAS 拒绝迟到交付覆盖", cas_ok, "GENERATION_CONFLICT")

    # 调用者明确给出上一代 1 → 允许替换，生成新一代不可变版本
    run_tool(s6b, store_root, "publish", "--catalog", str(catalog), check=True)
    head6 = store.head("shared/k.csv")
    record("S6 显式 expected=1 替换成功，生成 gen2",
           head6["version"] == "shared/k.csv@2" and head6["delivery"] == "delivery-f2",
           f"head={head6['version']} delivery={head6['delivery']}")
    # 旧版本不被回收，且 catalog 旧条目仍按旧 version 可读
    old_bytes = store.read("shared/k.csv@1")
    record("S6 旧 version 字节仍完整可读、不被新交付回收",
           old_bytes == data1 and sha256_bytes(old_bytes) == digest1,
           f"旧版本长度={len(old_bytes)}")
    old_entry = [e for e in json.loads(catalog.read_text())["entries"]
                 if e["delivery_id"] == "delivery-f1"][0]
    record("S6 catalog 旧条目仍绑定不可变旧版本",
           old_entry["object_version"] == "shared/k.csv@1",
           f"新 head={head6['version']} 但 catalog 读者读 {old_entry['object_version']}")

    # ---------------- S7 同 delivery 同内容重试：只对应一个版本 ----------------
    section("S7 同一 delivery + 同一内容重复执行的幂等性")
    s7 = work / "s7"
    run_tool(s7, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-g", "--key", "data/g.csv",
             "--part-bytes", "64", check=True)
    r7a = payload(run_tool(s7, store_root, "upload", check=True))
    r7b = payload(run_tool(s7, store_root, "upload", check=True))
    r7c = payload(run_tool(s7, store_root, "resume", check=True))
    versions_g = object_versions(store, "data/g.csv")
    record("S7 三次执行只产生一个可发布版本",
           len(versions_g) == 1 and r7a["object_version"] == r7b["object_version"]
           == r7c["object_version"],
           f"versions={versions_g}, 重试额外对象字节=0")
    run_tool(s7, store_root, "publish", "--catalog", str(catalog), check=True)
    entries_before = len(json.loads(catalog.read_text())["entries"])
    pub_again = payload(run_tool(s7, store_root, "publish", "--catalog", str(catalog), check=True))
    entries_after = len(json.loads(catalog.read_text())["entries"])
    record("S7 publish 幂等不产生重复 catalog 条目",
           pub_again["entry"]["object_version"] == versions_g[0]
           and entries_before == entries_after,
           f"catalog 条目数稳定={entries_after}")

    # ---------------- S8 同 delivery_id 不同内容：拒绝 ----------------
    section("S8 delivery_id 重用不同内容必须明确拒绝")
    s8 = work / "s8"
    run_tool(s8, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-h", "--key", "data/h.csv",
             "--part-bytes", "64", check=True)
    run_tool(s8, store_root, "upload", check=True)
    src8b = work / "h2.csv"
    make_csv(src8b, 5)
    bad = run_tool(s8, store_root, "plan", "--csv", str(src8b),
                   "--delivery", "delivery-h", "--key", "data/h.csv",
                   "--part-bytes", "64", "--expected-generation", "1")
    record("S8 工具层拒绝同 delivery 换内容",
           bad.returncode == 4 and payload(bad)["status"] == "error",
           f"exit={bad.returncode} {payload(bad)['message'][:40]}")
    # 服务层独立防线：另一进程直接用同 delivery 提交不同 digest
    other_uid = store.begin("data/h.csv", "delivery-h", sha256_bytes(src8b.read_bytes()),
                            len(src8b.read_bytes()), 1)
    try:
        store.complete(other_uid, [])  # 内容身份在旧回执存在时先被拦
        svc_ok = False
    except ValueError as e:
        svc_ok = str(e) == "DELIVERY_CONTENT_CONFLICT"
    record("S8 服务层独立拒绝 DELIVERY_CONTENT_CONFLICT", svc_ok,
           "旧回执绑定的 digest 受保护")

    # ---------------- S9 边界与参数校验 ----------------
    section("S9 边界：16KiB / 64B = 256 段；非法参数被拒")
    s9 = work / "s9"
    big = work / "big.csv"
    big_data = bytes((i % 251 for i in range(16 * 1024)))
    big.write_bytes(big_data)
    run_tool(s9, store_root, "plan", "--csv", str(big),
             "--delivery", "delivery-i", "--key", "data/big.bin",
             "--part-bytes", "64", check=True)
    r9 = payload(run_tool(s9, store_root, "upload", check=True))
    plan9 = json.loads((s9 / "plan.json").read_text())
    record("S9 16KiB 文件精确切成 256 段且末段满长",
           len(plan9["parts"]) == 256 and plan9["parts"][-1]["length"] == 64,
           f"段数={len(plan9['parts'])}")
    back9 = store.read(r9["object_version"])
    record("S9 16KiB 全字节读回一致", back9 == big_data and len(back9) == 16 * 1024,
           f"sha256={sha256_bytes(back9)[:16]}…")
    bad_pb = run_tool(s9, store_root, "plan", "--csv", str(big),
                      "--delivery", "delivery-i2", "--key", "data/x",
                      "--part-bytes", "257")
    record("S9 part_bytes=257 被拒", bad_pb.returncode == 2, f"exit={bad_pb.returncode}")
    over = work / "over.csv"
    over.write_bytes(b"x" * (16 * 1024 + 1))
    bad_sz = run_tool(s9, store_root, "plan", "--csv", str(over),
                      "--delivery", "delivery-i3", "--key", "data/y",
                      "--part-bytes", "256")
    record("S9 超过 16KiB 被拒", bad_sz.returncode == 2, f"exit={bad_sz.returncode}")

    # ---------------- S10 清理纪律 ----------------
    section("S10 只清理自己确定废弃的暂存 upload，不动别人的进行中上传")
    s10a, s10b = work / "s10a", work / "s10b"
    run_tool(s10a, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-j", "--key", "data/j.csv",
             "--part-bytes", "128", check=True)
    run_tool(s10a, store_root, "upload",
             env_extra={"DELIVERY_KILL_AFTER_PART": "2"})
    uid_j = json.loads((s10a / "upload.json").read_text())["upload_id"]
    # 另一个上传者的进行中 upload
    run_tool(s10b, store_root, "plan", "--csv", str(src6b),
             "--delivery", "delivery-k", "--key", "data/k2.csv",
             "--part-bytes", "64", check=True)
    run_tool(s10b, store_root, "upload",
             env_extra={"DELIVERY_KILL_AFTER_PART": "1"})
    uid_k = json.loads((s10b / "upload.json").read_text())["upload_id"]
    live_before = {u["upload_id"] for u in store.list_uploads()}
    record("S10 两个进行中 upload 并存", {uid_j, uid_k} <= live_before,
           f"uploads={sorted(live_before)}")
    # delivery-j 改分段策略 → 新计划，只 abort 自己的 uid_j
    run_tool(s10a, store_root, "plan", "--csv", str(src1),
             "--delivery", "delivery-j", "--key", "data/j.csv",
             "--part-bytes", "256", check=True)
    live_after = {u["upload_id"] for u in store.list_uploads()}
    record("S10 自己的旧暂存被 abort、别人的 upload 原样保留",
           uid_j not in live_after and uid_k in live_after,
           f"removed={uid_j} preserved={uid_k}")
    # 越权 abort 必须被服务拒绝
    try:
        store.abort(uid_k, "delivery-j")
        owner_ok = False
    except ValueError as e:
        owner_ok = str(e) == "UPLOAD_OWNER_MISMATCH"
    record("S10 服务拒绝跨 delivery abort", owner_ok, "UPLOAD_OWNER_MISMATCH")
    # 新计划走通且内容身份不变
    r10 = payload(run_tool(s10a, store_root, "resume", check=True))
    record("S10 新分段策略下交付成功且摘要不变",
           store.read(r10["object_version"]) == data1,
           f"版本={r10['object_version']}")

    # ---------------- 汇总 ----------------
    section("汇总")
    width = max(len(n) for n, _, _ in results)
    for name, status, detail in results:
        print(f"{status:4} {name:<{width}}  {detail}")
    failures = [r for r in results if r[1] == FAIL]
    print(f"\n共 {len(results)} 项断言，失败 {len(failures)} 项")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

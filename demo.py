#!/usr/bin/env python3
"""确定性场景驱动：以独立子进程调用 loader.py（真实进程重开），
覆盖控制先到/数据先到/分区交错、重放折叠与损坏、缺片、依赖环、
失败父事务、提交后丢确认，以及空库拓扑重放与逐表核对。

每个场景使用独立数据库目录；断言失败立即非零退出。
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
LOADER = [sys.executable, str(ROOT / "loader.py")]
OUT = ROOT / "out" / "scenarios"
FIX = ROOT / "fixtures"

PASSED: list[str] = []
FAILED: list[str] = []


# ---------------------------------------------------------------- 工具

def run(args: list[str], stdin: str | None = None, check: bool = True) -> tuple[int, str, str]:
    p = subprocess.run(LOADER + args, input=stdin, capture_output=True, text=True)
    if check and p.returncode not in (0, 3):
        raise AssertionError(f"cmd failed: {args}\nstdout={p.stdout}\nstderr={p.stderr}")
    return p.returncode, p.stdout, p.stderr


def ingest(db: Path, source: str, frames: list[dict], end: bool = False) -> dict:
    lines = [json.dumps(f, ensure_ascii=False) for f in frames]
    if end:
        lines.append(json.dumps({"kind": "end", "source": source}))
    code, out, err = run(["ingest", str(db), "--source", source, "--file", "-"],
                         stdin="\n".join(lines) + ("\n" if lines else ""))
    return json.loads(out)


def work(db: Path, check: bool = True) -> dict:
    code, out, err = run(["work", str(db)], check=check)
    return json.loads(out)


def query(db: Path) -> dict:
    _, out, _ = run(["query", str(db)])
    return json.loads(out)


def inspect(db: Path) -> dict:
    _, out, _ = run(["inspect", str(db)])
    return json.loads(out)


def states(db: Path) -> dict:
    return work(db)["states"]


def fresh(name: str) -> Path:
    d = OUT / name
    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True)
    return d / "cdc.db"


def load_fixture(stream: str) -> list[dict]:
    return [json.loads(ln) for ln in (FIX / f"{stream}.jsonl").read_text().splitlines()
            if ln.strip()]


def control(txn: str) -> dict:
    return next(r for r in load_fixture("control") if r["txn"] == txn)


def frags(txn: str) -> list[dict]:
    out = []
    for s in ("p0", "p1"):
        out.extend(r for r in load_fixture(s) if r["txn"] == txn)
    return out


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        PASSED.append(name)
        print(f"  PASS {name}")
    else:
        FAILED.append(f"{name} {detail}")
        print(f"  FAIL {name} {detail}")


# ---------------------------------------------------------------- 场景

def s1_control_first() -> None:
    print("S1 控制先到：t1 缺 p1 期间反复 work，验证无半事务、t3 不被阻塞")
    db = fresh("s1")
    # 全部控制先到（不含 end）；p0 全部到达。此时 t3 已完整就绪、t4 abort，
    # 二者都不依赖 end 帧——end 只约束“缺片/缺依赖能否终态化”，不阻塞就绪提交
    ingest(db, "control", load_fixture("control"))
    p0 = load_fixture("p0")
    ingest(db, "p0", p0)
    st = states(db)
    check("s1.t3_ready_without_end_commits", st["t3"] == "committed", str(st))
    check("s1.t4_aborted_without_end", st["t4"] == "aborted", str(st))
    check("s1.t1_t2_receiving_incomplete_fragments",
          st["t1"] == "receiving" and st["t2"] == "waiting_dependency", str(st))
    # 半事务守卫：t1 未完整 → c1/o0 不可见；t2 父事务未提交 → o1 不可见
    check("s1.no_half_rows",
          query(db) == {"customers": [{"id": "c2", "name": "乙"}], "orders": []},
          str(query(db)))

    # t1 的 p1 到达：t1 原子提交（c1+o0 同时可见），同 pass 内 t2 解阻塞跟上
    t1p1 = [r for r in load_fixture("p1") if r["txn"] == "t1"]
    ingest(db, "p1", t1p1)
    st = states(db)
    check("s1.t1_committed", st["t1"] == "committed", str(st))
    check("s1.t2_committed_after_parent", st["t2"] == "committed", str(st))
    q = query(db)
    check("s1.t1_atomic_both_rows",
          {r["id"] for r in q["customers"]} == {"c1", "c2"}
          and {r["id"] for r in q["orders"]} == {"o0", "o1"}, str(q))

    # 显式结束三路（t4 的 p1 片段也要喂，abort 不影响）
    ingest(db, "p0", [], end=True)
    ingest(db, "p1", [r for r in load_fixture("p1") if r["txn"] == "t4"], end=True)
    ingest(db, "control", [], end=True)
    st = states(db)
    check("s1.terminal_states",
          st == {"t1": "committed", "t2": "committed", "t3": "committed", "t4": "aborted"},
          str(st))


def s2_data_first() -> None:
    print("S2 数据先到：全部数据落盘后控制才到，再给 end，逐次 query 无半事务")
    db = fresh("s2")
    ingest(db, "p0", load_fixture("p0"))
    ingest(db, "p1", load_fixture("p1"))
    check("s2.no_control_no_rows", query(db) == {"customers": [], "orders": []})
    st = states(db)
    check("s2.all_receiving", all(v == "receiving" for v in st.values()), str(st))
    # 控制逐个到达：先 t2（依赖未到控制）
    ingest(db, "control", [control("t2")])
    st = states(db)
    check("s2.t2_waiting_dependency", st["t2"] == "waiting_dependency", str(st))
    check("s2.still_no_rows", query(db) == {"customers": [], "orders": []})
    # t3 控制到达 → 独立事务立即提交
    ingest(db, "control", [control("t3")])
    st = states(db)
    check("s2.t3_committed_alone", st["t3"] == "committed", str(st))
    q = query(db)
    check("s2.only_c2_visible", [r["id"] for r in q["customers"]] == ["c2"]
          and q["orders"] == [], str(q))
    # t1 控制到达 → t1 提交，t2 随后提交；t4 abort
    ingest(db, "control", [control("t1"), control("t4")])
    st = states(db)
    check("s2.t1_t2_committed_t4_aborted",
          st["t1"] == "committed" and st["t2"] == "committed" and st["t4"] == "aborted",
          str(st))
    q = query(db)
    check("s2.final_rows",
          {r["id"] for r in q["customers"]} == {"c1", "c2"}
          and {r["id"] for r in q["orders"]} == {"o0", "o1"}, str(q))
    # end 全部到达，状态稳定
    for s, frames in (("p0", []), ("p1", []), ("control", [])):
        ingest(db, s, frames, end=True)
    st = states(db)
    check("s2.stable_after_end",
          st == {"t1": "committed", "t2": "committed", "t3": "committed", "t4": "aborted"},
          str(st))


def s3_interleaved_partitions() -> None:
    print("S3 分区交错 + 进程重开：逐帧交替喂入，每步 work 后 query 检查可见性边界")
    db = fresh("s3")
    timeline = [
        ("p0", next(r for r in load_fixture("p0") if r["txn"] == "t2")),   # t2 数据
        ("control", control("t1")),
        ("p0", next(r for r in load_fixture("p0") if r["txn"] == "t1")),   # t1 p0
        ("control", control("t2")),
        ("p1", next(r for r in load_fixture("p1") if r["txn"] == "t1")),   # t1 p1 补齐
        ("control", control("t3")),
        ("p0", next(r for r in load_fixture("p0") if r["txn"] == "t3")),
        ("control", control("t4")),
        ("p1", next(r for r in load_fixture("p1") if r["txn"] == "t4")),
    ]
    for i, (src, frame) in enumerate(timeline):
        ingest(db, src, [frame])
        st = states(db)
        q = query(db)
        # t1 未完整前，c1/o0 绝不可见；t2 绝不先于 t1
        if i < 4:
            check(f"s3.step{i}.no_half_rows", q == {"customers": [], "orders": []}, str(q))
        if i == 4:
            check("s3.t1_atomic_after_p1", st["t1"] == "committed", str(st))
            check("s3.t2_not_before_parent",
                  st["t2"] in ("waiting_dependency", "committed"), str(st))
            # t1 的 c1+o0 原子出现；t2 的 o1 即便同 pass 跟随，事件序也必须晚于 t1
            insp = inspect(db)
            commit_events = [e for e in insp["events"] if e["event"] == "committed"]
            order = [e["txn"] for e in commit_events]
            check("s3.t1_event_before_t2",
                  order.index("t1") < order.index("t2"), str(order))
            check("s3.t1_rows_atomic",
                  {r["id"] for r in q["orders"]} >= {"o0"}
                  and {r["id"] for r in q["customers"]} >= {"c1"}, str(q))
    # 每个 ingest/work/query 都是独立进程：状态完全来自 SQLite
    st = states(db)
    check("s3.final_commits",
          st["t1"] == "committed" and st["t2"] == "committed" and st["t3"] == "committed",
          str(st))
    for s in ("p0", "p1", "control"):
        ingest(db, s, [], end=True)
    check("s3.after_end_stable", states(db) == {
        "t1": "committed", "t2": "committed", "t3": "committed", "t4": "aborted"})
    # 重放已提交事务：相同数据重喂，回执不变
    receipts_before = {t["txn"]: t.get("receipt", {}).get("receipt")
                       for t in inspect(db)["txns"]}
    ingest(db, "p0", load_fixture("p0"))
    ingest(db, "p1", load_fixture("p1"))
    receipts_after = {t["txn"]: t.get("receipt", {}).get("receipt")
                      for t in inspect(db)["txns"]}
    check("s3.replay_same_receipt", receipts_before == receipts_after,
          f"{receipts_before} != {receipts_after}")
    check("s3.replay_folded_no_dupes",
          work(db)["committed"] == []
          and {r["id"] for r in query(db)["orders"]} == {"o0", "o1"}
          and {r["id"] for r in query(db)["customers"]} == {"c1", "c2"})


def s4_replay_and_corruption() -> None:
    print("S4 同片段重放折叠 / 同身份不同内容损坏 / 重复 op_no / 未声明分区 / 越界")
    db = fresh("s4")
    f = frags("t3")[0]
    ingest(db, "control", [control("t3")])
    r1 = ingest(db, "p0", [f])
    r2 = ingest(db, "p0", [f])  # 完全相同
    check("s4.identical_replay_folded", r2["folded"] == 1 and r2["violations"] == 0, str(r2))
    bad = dict(f, name="被篡改的名字")
    r3 = ingest(db, "p0", [bad])
    check("s4.content_conflict_detected", r3["violations"] == 1, str(r3))
    states(db)  # 驱动状态机：ingest 置 corrupt 标志，work 将其物化为 corrupt 终态
    insp = inspect(db)
    t3state = next(t for t in insp["txns"] if t["txn"] == "t3")
    codes = [v["code"] for v in insp["violations"]]
    # 损坏后 t3 不得提交；原始好片段仍保留（fragments 计数为 1，损坏帧未覆盖）
    check("s4.corrupt_state_keeps_good_fragment",
          t3state["state"] == "corrupt"
          and "content_conflict" in codes
          and t3state["fragments"]["received"] == {"p0": 1},
          str((t3state["state"], codes, t3state.get("fragments"))))
    # 好片段仍在，且 corrupted 事务不会提交
    st = states(db)
    check("s4.corrupt_never_commits", st["t3"] == "corrupt", str(st))

    # 重复 op_no：t1 的两个片段声明不同 op_no，造一个同 op_no 新片段
    dup = {"txn": "t1", "partition": "p1", "part_no": 0, "op_no": 0,
           "op": "put_order", "id": "o9", "customer": "c1", "amount": 1}
    ingest(db, "control", [control("t1")])
    ingest(db, "p0", [r for r in frags("t1") if r["partition"] == "p0"])
    r = ingest(db, "p1", [dup])
    check("s4.duplicate_op_no", r["violations"] == 1
          and "duplicate_op_no" in [v["code"] for v in inspect(db)["violations"]])

    # 未声明分区
    txn_x = {"txn": "x1", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"}
    ingest(db, "control", [txn_x])
    r = ingest(db, "p1", [{"txn": "x1", "partition": "p1", "part_no": 0, "op_no": 0,
                           "op": "put_order", "id": "ox", "customer": "c1", "amount": 1}])
    check("s4.undeclared_partition",
          "undeclared_partition" in [v["code"] for v in inspect(db)["violations"]])

    # part_no 越界（声明 1 片，送 part_no=1）
    r = ingest(db, "p0", [{"txn": "x1", "partition": "p0", "part_no": 1, "op_no": 1,
                          "op": "put_customer", "id": "cx", "name": "n"}])
    check("s4.part_no_out_of_range",
          "part_no_out_of_range" in [v["code"] for v in inspect(db)["violations"]])

    # 未知终态
    ingest(db, "control", [{"txn": "x2", "parts": {"p0": 1}, "dependencies": [],
                            "terminal": "prepare"}])
    check("s4.unknown_terminal",
          "unknown_terminal" in [v["code"] for v in inspect(db)["violations"]])

    # end 后再送数据：stream_closed
    ingest(db, "p0", [], end=True)
    r = ingest(db, "p0", [{"txn": "x3", "partition": "p0", "part_no": 0, "op_no": 0,
                           "op": "put_customer", "id": "cx3", "name": "n"}])
    check("s4.stream_closed",
          "stream_closed" in [v["code"] for v in inspect(db)["violations"]], str(r))


def s5_missing_fragment() -> None:
    print("S5 缺片到源结束：incomplete；依赖它的 t2 blocked；独立 t3 正常提交")
    db = fresh("s5")
    ingest(db, "control", load_fixture("control"))
    # t1 只给 p0，故意缺 p1；其余片段给齐
    ingest(db, "p0", load_fixture("p0"))
    ingest(db, "p1", [r for r in load_fixture("p1") if r["txn"] != "t1"])
    for s in ("p0", "p1", "control"):
        # 无 end 时 work：t1 仍 receiving，t3 已提交
        pass
    st_before = states(db)
    check("s5.t3_not_blocked_by_incomplete_t1",
          st_before["t3"] == "committed", str(st_before))
    check("s5.t1_receiving_before_end", st_before["t1"] == "receiving", str(st_before))
    for s in ("p0", "p1", "control"):
        ingest(db, s, [], end=True)
    st = states(db)
    check("s5.t1_incomplete", st["t1"] == "incomplete", str(st))
    check("s5.t2_blocked", st["t2"] == "blocked", str(st))
    check("s5.t3_still_committed", st["t3"] == "committed", str(st))
    q = query(db)
    check("s5.no_half_t1_rows",
          all(r["id"] != "c1" for r in q["customers"])
          and all(r["id"] != "o0" for r in q["orders"]), str(q))
    check("s5.c2_present", [r["id"] for r in q["customers"]] == ["c2"])


def s6_cycle_and_missing_dependency() -> None:
    print("S6 依赖环 rejected；源结束后未知依赖 missing_dependency；后继 blocked")
    db = fresh("s6")
    cyc = [
        {"txn": "a", "parts": {"p0": 1}, "dependencies": ["b"], "terminal": "commit"},
        {"txn": "b", "parts": {"p0": 1}, "dependencies": ["a"], "terminal": "commit"},
        {"txn": "c", "parts": {"p0": 1}, "dependencies": ["a"], "terminal": "commit"},
        {"txn": "d", "parts": {"p0": 1}, "dependencies": ["ghost"], "terminal": "commit"},
    ]
    ingest(db, "control", cyc)
    fa = {"txn": "a", "partition": "p0", "part_no": 0, "op_no": 0,
          "op": "put_customer", "id": "ca", "name": "a"}
    fb = {"txn": "b", "partition": "p0", "part_no": 0, "op_no": 0,
          "op": "put_customer", "id": "cb", "name": "b"}
    fc = {"txn": "c", "partition": "p0", "part_no": 0, "op_no": 0,
          "op": "put_customer", "id": "cc", "name": "c"}
    fd = {"txn": "d", "partition": "p0", "part_no": 0, "op_no": 0,
          "op": "put_customer", "id": "cd", "name": "d"}
    ingest(db, "p0", [fa, fb, fc, fd])
    # end 之前：d 的依赖未知 → waiting_dependency（不提前判失败）
    st = states(db)
    check("s6.waits_before_end",
          st["d"] == "waiting_dependency" and st["a"] == "rejected"
          and st["b"] == "rejected" and st["c"] == "blocked", str(st))
    for s in ("p0", "p1", "control"):
        ingest(db, s, [], end=True)
    st = states(db)
    check("s6.cycle_rejected", st["a"] == "rejected" and st["b"] == "rejected", str(st))
    check("s6.cycle_successor_blocked", st["c"] == "blocked", str(st))
    check("s6.missing_dependency", st["d"] == "missing_dependency", str(st))
    check("s6.no_rows_from_cycle", query(db)["customers"] == [])


def s7_failed_parent() -> None:
    print("S7 提交失败：注入 fail_before_commit（含 amount 越界自然失败），后继 blocked，无关事务继续")
    db = fresh("s7")
    # 7a 注入失败：t1 完整就绪前武装
    run(["arm-fault", str(db), "fail_before_commit", "--txn", "t1"])
    ingest(db, "control", load_fixture("control"))
    ingest(db, "p0", load_fixture("p0"))
    ingest(db, "p1", load_fixture("p1"))
    st = states(db)
    check("s7.t1_failed", st["t1"] == "failed", str(st))
    check("s7.t2_blocked", st["t2"] == "blocked", str(st))
    check("s7.t3_committed_despite_failure", st["t3"] == "committed", str(st))
    check("s7.failed_txn_no_rows",
          all(r["id"] not in ("c1",) for r in query(db)["customers"])
          and all(r["id"] not in ("o0", "o1") for r in query(db)["orders"]))
    insp = inspect(db)
    check("s7.failure_recorded",
          any(v["code"] == "injected_failure" for v in insp["violations"]))
    # 故障是一次性的；重跑 work 不会自动重试（failed 为终态），符合“提交失败明确 failed”
    st2 = states(db)
    check("s7.failed_is_terminal", st2["t1"] == "failed" and st2["t2"] == "blocked")

    # 7b 自然失败：amount=-1 的提交事务（把 t4 的 terminal 改成 commit）
    db2 = fresh("s7b")
    ctl = [dict(r) for r in load_fixture("control")]
    for r in ctl:
        if r["txn"] == "t4":
            r["terminal"] = "commit"
            r["dependencies"] = []  # 独立，直接暴露 amount 校验
    ingest(db2, "control", ctl)
    ingest(db2, "p0", load_fixture("p0"))
    ingest(db2, "p1", load_fixture("p1"))
    for s in ("p0", "p1", "control"):
        ingest(db2, s, [], end=True)
    st = states(db2)
    check("s7.negative_amount_failed", st["t4"] == "failed", str(st))
    reason = next(t["reason"] for t in inspect(db2)["txns"] if t["txn"] == "t4")
    check("s7.failure_reason_amount", reason and "amount_out_of_range" in reason, reason)

    # 7c int64 上界（一次性声明 2 片，避免控制帧自相矛盾）
    db3 = fresh("s7c")
    ingest(db3, "control", [
        {"txn": "big", "parts": {"p0": 2}, "dependencies": [], "terminal": "commit"}])
    ingest(db3, "p0", [
        {"txn": "big", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "cbig", "name": "n"},
        {"txn": "big", "partition": "p0", "part_no": 1, "op_no": 1,
         "op": "put_order", "id": "obig", "customer": "cbig", "amount": 2**63}])
    for s in ("p0", "p1", "control"):
        ingest(db3, s, [], end=True)
    st = states(db3)
    check("s7.int64_overflow_failed", st["big"] == "failed", str(st))


def s8_lost_ack() -> None:
    print("S8 提交后丢确认：COMMIT 已落盘、回执确认前进程退出；重开可核查且不重写")
    db = fresh("s8")
    run(["arm-fault", str(db), "lose_ack_after_commit", "--txn", "t3"])
    ingest(db, "control", [control("t3")])
    frames = [frags("t3")[0]]
    code, out, err = run(["ingest", str(db), "--source", "p0", "--file", "-"],
                         stdin=json.dumps(frames[0], ensure_ascii=False) + "\n")
    assert code == 0
    # work 进程在提交后以 3 退出
    code, out, err = run(["work", str(db)], check=False)
    check("s8.exit_code_3", code == 3, f"code={code} err={err}")
    check("s8.stderr_marker", "ack_lost_after_commit" in err, err)
    # 新进程核查：行已可见、状态 committed、回执存在且确定
    q = query(db)
    check("s8.row_durable", [r["id"] for r in q["customers"]] == ["c2"], str(q))
    insp = inspect(db)
    t3 = next(t for t in insp["txns"] if t["txn"] == "t3")
    check("s8.state_committed", t3["state"] == "committed", str(t3["state"]))
    receipt1 = t3["receipt"]["receipt"]
    # 重跑 work：无新提交，重放返回原回执（inspect 中回执不变）
    summary = work(db)
    check("s8.no_rewrite_on_recovery", summary["committed"] == [], str(summary))
    receipt2 = next(t for t in inspect(db)["txns"] if t["txn"] == "t3")["receipt"]["receipt"]
    check("s8.same_receipt_after_recovery", receipt1 == receipt2,
          f"{receipt1} != {receipt2}")
    check("s8.fault_disarmed",
          all(f["armed"] == 0 for f in inspect(db)["faults"]
              if f["name"] == "lose_ack_after_commit"))


def s9_topology_replay_oracle() -> None:
    print("S9 空库按合法依赖拓扑顺序重放完整源事务，独立 oracle 逐表核对最终行与回执")
    db = fresh("s9")
    ctl = load_fixture("control")
    order = ["t1", "t3", "t2", "t4"]  # 合法拓扑序（t2 在 t1 后；t4 abort 位置任意）
    ingest(db, "control", [control(t) for t in order])
    for t in order:
        for s in ("p0", "p1"):
            fs = [r for r in load_fixture(s) if r["txn"] == t]
            if fs:
                ingest(db, s, fs)
    for s in ("p0", "p1", "control"):
        ingest(db, s, [], end=True)
    st = states(db)
    check("s9.states", st == {
        "t1": "committed", "t2": "committed", "t3": "committed", "t4": "aborted"}, str(st))

    # 独立 oracle：按 op_no 顺序在内存重放 commit 事务
    customers: dict[str, str] = {}
    orders: dict[str, tuple[str, int]] = {}
    receipts: dict[str, int] = {}
    for t in ["t1", "t3", "t2"]:  # 拓扑序
        ops = sorted(frags(t), key=lambda r: r["op_no"])
        for r in ops:
            if r["op"] == "put_customer":
                customers[r["id"]] = r["name"]
            elif r["op"] == "put_order":
                orders[r["id"]] = (r["customer"], r["amount"])
            else:
                orders.pop(r["id"], None)
        receipts[t] = len(ops)
    q = query(db)
    check("s9.customers_match_oracle",
          sorted((r["id"], r["name"]) for r in q["customers"])
          == sorted(customers.items()))
    check("s9.orders_match_oracle",
          sorted((r["id"], r["customer_id"], r["amount"]) for r in q["orders"])
          == sorted((i, c, a) for i, (c, a) in orders.items()))
    insp = inspect(db)
    for t, n in receipts.items():
        rcpt = next(x["receipt"] for x in insp["txns"] if x["txn"] == t)
        check(f"s9.receipt_ops_{t}", rcpt["ops"] == n and rcpt["txn"] == t
              and rcpt["receipt"].startswith("rcpt_"))
    check("s9.no_receipt_for_abort",
          all(t["txn"] != "t4" or "receipt" not in t for t in insp["txns"]))

    # 再跑一次完整重放（所有源文件整体重喂）：状态/行/回执完全幂等
    before = json.dumps(query(db), sort_keys=True, ensure_ascii=False)
    r_before = {t["txn"]: t.get("receipt") for t in insp["txns"]}
    for s in ("p0", "p1", "control"):
        ingest(db, s, load_fixture(s))
    work(db)
    after = json.dumps(query(db), sort_keys=True, ensure_ascii=False)
    r_after = {t["txn"]: t.get("receipt") for t in inspect(db)["txns"]}
    check("s9.full_replay_idempotent_rows", before == after)
    check("s9.full_replay_idempotent_receipts", r_before == r_after)


def s10_write_conflict_contract() -> None:
    print("S10 写冲突合同：同 key 无依赖路径——双方未提交都拒绝；一方已提交只拒后来者")
    # 10a 双方均未提交、无依赖 → 都拒绝
    db = fresh("s10a")
    ingest(db, "control", [
        {"txn": "u1", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"},
        {"txn": "u2", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"},
    ])
    ingest(db, "p0", [
        {"txn": "u1", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "shared", "name": "first"},
        {"txn": "u2", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "shared", "name": "second"},
    ])
    for s in ("p0", "p1", "control"):
        ingest(db, s, [], end=True)
    st = states(db)
    check("s10a.both_rejected",
          st["u1"] == "rejected" and st["u2"] == "rejected", str(st))
    check("s10a.no_rows", query(db)["customers"] == [])

    # 10b u1 先提交，u2 后到且无依赖 → 只拒 u2，历史不动
    db2 = fresh("s10b")
    ingest(db2, "control", [
        {"txn": "u1", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"}])
    ingest(db2, "p0", [
        {"txn": "u1", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "shared", "name": "first"}])
    check("s10b.u1_committed", states(db2)["u1"] == "committed")
    ingest(db2, "control", [
        {"txn": "u2", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"}])
    ingest(db2, "p0", [
        {"txn": "u2", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "shared", "name": "second"}])
    for s in ("p0", "p1", "control"):
        ingest(db2, s, [], end=True)
    st = states(db2)
    check("s10b.history_kept_later_rejected",
          st["u1"] == "committed" and st["u2"] == "rejected", str(st))
    q = query(db2)
    check("s10b.first_value_wins",
          q["customers"] == [{"id": "shared", "name": "first"}], str(q))

    # 10c 合法情形：同 key 但有依赖路径 → 正常按序覆盖
    db3 = fresh("s10c")
    ingest(db3, "control", [
        {"txn": "u1", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"},
        {"txn": "u2", "parts": {"p0": 1}, "dependencies": ["u1"], "terminal": "commit"},
    ])
    ingest(db3, "p0", [
        {"txn": "u1", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "shared", "name": "first"},
        {"txn": "u2", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "shared", "name": "second"},
    ])
    for s in ("p0", "p1", "control"):
        ingest(db3, s, [], end=True)
    st = states(db3)
    check("s10c.dependency_path_allows_overwrite",
          st["u1"] == "committed" and st["u2"] == "committed", str(st))
    check("s10c.second_value_visible",
          query(db3)["customers"] == [{"id": "shared", "name": "second"}])


def s11_fk_contract() -> None:
    print("S11 外键合同：父对象须同事务先建或显式依赖；不推测隐藏依赖；delete_order 语义")
    # 11a 目标库已有 c1（由独立已提交事务建立），但新事务未声明依赖 → 仍然失败
    db = fresh("s11a")
    ingest(db, "control", [
        {"txn": "parent", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"},
        {"txn": "child", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"}])
    ingest(db, "p0", [
        {"txn": "parent", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "c1", "name": "甲"},
        {"txn": "child", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_order", "id": "oX", "customer": "c1", "amount": 5}])
    for s in ("p0", "p1", "control"):
        ingest(db, s, [], end=True)
    st = states(db)
    check("s11a.no_hidden_dependency",
          st["parent"] == "committed" and st["child"] == "failed", str(st))
    reason = next(t["reason"] for t in inspect(db)["txns"] if t["txn"] == "child")
    check("s11a.reason_fk", reason and "fk_parent_missing" in reason, reason)

    # 11b 显式声明依赖 → 成功
    db2 = fresh("s11b")
    ingest(db2, "control", [
        {"txn": "parent", "parts": {"p0": 1}, "dependencies": [], "terminal": "commit"},
        {"txn": "child", "parts": {"p0": 1}, "dependencies": ["parent"],
         "terminal": "commit"}])
    ingest(db2, "p0", [
        {"txn": "parent", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "c1", "name": "甲"},
        {"txn": "child", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_order", "id": "oX", "customer": "c1", "amount": 5}])
    for s in ("p0", "p1", "control"):
        ingest(db2, s, [], end=True)
    check("s11b.declared_dependency_ok",
          all(v == "committed" for v in states(db2).values()))

    # 11c 同事务内先建客户再下单（op_no 乱序到达，按 op_no 执行）
    db3 = fresh("s11c")
    ingest(db3, "control", [
        {"txn": "same", "parts": {"p0": 2}, "dependencies": [], "terminal": "commit"}])
    ingest(db3, "p0", [
        {"txn": "same", "partition": "p0", "part_no": 1, "op_no": 1,
         "op": "put_order", "id": "oY", "customer": "c9", "amount": 7},
        {"txn": "same", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "c9", "name": "九"}])
    for s in ("p0", "p1", "control"):
        ingest(db3, s, [], end=True)
    check("s11c.local_parent_opno_order", states(db3)["same"] == "committed",
          str(states(db3)))

    # 11d delete_order 缺失行为 no-op；存在则删除
    db4 = fresh("s11d")
    ingest(db4, "control", [
        {"txn": "del", "parts": {"p0": 3}, "dependencies": [], "terminal": "commit"}])
    ingest(db4, "p0", [
        {"txn": "del", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "cd", "name": "d"},
        {"txn": "del", "partition": "p0", "part_no": 1, "op_no": 1,
         "op": "put_order", "id": "od", "customer": "cd", "amount": 1},
        {"txn": "del", "partition": "p0", "part_no": 2, "op_no": 2,
         "op": "delete_order", "id": "ghost_order"}])
    for s in ("p0", "p1", "control"):
        ingest(db4, s, [], end=True)
    check("s11d.delete_missing_noop", states(db4)["del"] == "committed",
          str(states(db4)))
    # ghost_order 本就缺失，删除是 no-op；od 仅被 put、未被删，必须保留
    check("s11d.rows",
          query(db4)["orders"] == [{"id": "od", "customer_id": "cd", "amount": 1}])
    check("s11d.customer_kept",
          query(db4)["customers"] == [{"id": "cd", "name": "d"}])

    # 11e 同事务内 put 后 delete：净效果为订单不存在（且不触发与自身的写集冲突）
    db5 = fresh("s11e")
    ingest(db5, "control", [
        {"txn": "del2", "parts": {"p0": 3}, "dependencies": [], "terminal": "commit"}])
    ingest(db5, "p0", [
        {"txn": "del2", "partition": "p0", "part_no": 0, "op_no": 0,
         "op": "put_customer", "id": "ce", "name": "e"},
        {"txn": "del2", "partition": "p0", "part_no": 1, "op_no": 1,
         "op": "put_order", "id": "oe", "customer": "ce", "amount": 2},
        {"txn": "del2", "partition": "p0", "part_no": 2, "op_no": 2,
         "op": "delete_order", "id": "oe"}])
    for s in ("p0", "p1", "control"):
        ingest(db5, s, [], end=True)
    check("s11e.put_then_delete_commits", states(db5)["del2"] == "committed",
          str(states(db5)))
    check("s11e.order_net_deleted",
          query(db5)["orders"] == [] and
          query(db5)["customers"] == [{"id": "ce", "name": "e"}])


def main() -> int:
    shutil.rmtree(OUT, ignore_errors=True)
    OUT.mkdir(parents=True)
    for fn in (s1_control_first, s2_data_first, s3_interleaved_partitions,
               s4_replay_and_corruption, s5_missing_fragment,
               s6_cycle_and_missing_dependency, s7_failed_parent, s8_lost_ack,
               s9_topology_replay_oracle, s10_write_conflict_contract, s11_fk_contract):
        fn()
    print(f"\n==== {len(PASSED)} passed, {len(FAILED)} failed ====")
    if FAILED:
        for f in FAILED:
            print("FAILED:", f)
        return 1
    print("ALL SCENARIOS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

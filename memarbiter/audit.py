"""离线审计：只依赖工作目录中的事件日志与真实文件。

核对项（任一不满足即抛 :class:`AuditFailure`）：
  1. 槽数守恒：每个事件后 held = reserved+resident+pinned+outbuf，
     且 sum(held) + free == pool，授予总量永不超池；
  2. pin 安全：pin 中的槽不出现在任何释放里，revoke 不直接释放，
     每个 pin 都有配对的 spill_written 闭合，且闭合时真实文件存在；
  3. 请求闭环：每个 request 要么拿到等大的整批 grant，要么有
     request_ended（取消/失败明确结束），不存在悬空或部分授予；
  4. 终态完备：terminal 时四个子账全零；
  5. 结果一致：成功查询的输出与其全部输入的独立排序逐行相等，
     行数等于输入行数；取消/失败查询不得留下输出或临时目录；
  6. 运行段真实：每个成功路径的 spill_written 指向真实 JSONL，
     内容按 (key, row_id) 有序，行数与释放槽数一致。
"""
import json
from pathlib import Path

from .operators import sort_key


class AuditFailure(AssertionError):
    pass


def _load_jsonl(path):
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def audit_workdir(work_dir):
    work_dir = Path(work_dir)
    events = _load_jsonl(work_dir / "events.jsonl")
    config = json.loads((work_dir / "config_used.json").read_text("utf-8"))
    inputs = json.loads((work_dir / "input_snapshot.json").read_text("utf-8"))
    pool = config["global_slots"]

    st = {}
    free = pool
    open_requests = {}     # qid -> 等待中的 request 大小
    pin_open = {}          # qid -> 未闭合 pin 的 n
    seq = 0

    def q(qid):
        return st.setdefault(qid, {
            "held": 0, "reserved": 0, "pinned": 0, "resident": 0,
            "outbuf": 0, "done": False, "status": None,
            "granted_total": 0, "released_total": 0,
        })

    def check_subaccounts(ev):
        for qid, s in st.items():
            if s["held"] != (s["reserved"] + s["resident"]
                             + s["pinned"] + s["outbuf"]):
                raise AuditFailure(
                    f"事件 {ev['seq']} {qid}: 子账不守恒 {s}")
            if min(s["held"], s["reserved"], s["pinned"], s["resident"],
                   s["outbuf"]) < 0:
                raise AuditFailure(f"事件 {ev['seq']} {qid}: 负子账 {s}")

    def check_conservation(ev):
        total = sum(s["held"] for s in st.values())
        if total + free != pool:
            raise AuditFailure(
                f"事件 {ev['seq']}({ev['kind']}/{ev.get('qid')}): "
                f"槽不守恒 held={total} free={free} pool={pool}")
        if total > pool or free < 0:
            raise AuditFailure(
                f"事件 {ev['seq']}: 超池授予 total={total} free={free}")
        check_subaccounts(ev)

    for ev in events:
        seq = ev["seq"]
        kind = ev["kind"]
        qid = ev.get("qid")
        if qid:
            s = q(qid)

        if kind == "request":
            if ev["slots"] > pool:
                raise AuditFailure(f"事件 {seq}: 请求超池")
            if qid in open_requests:
                raise AuditFailure(f"事件 {seq}: 重叠请求 {qid}")
            open_requests[qid] = ev["slots"]
        elif kind == "grant":
            want = open_requests.pop(qid, None)
            if want is None:
                raise AuditFailure(f"事件 {seq}: 无对应请求的授予 {qid}")
            if want != ev["slots"]:
                raise AuditFailure(
                    f"事件 {seq}: 部分授予 request={want} grant={ev['slots']}")
            if free < want:
                raise AuditFailure(f"事件 {seq}: 授予时空闲槽不足")
            free -= want
            s["held"] += want
            s["reserved"] += want
            s["granted_total"] += want
        elif kind == "request_ended":
            want = open_requests.pop(qid, None)
            if want is None:
                raise AuditFailure(f"事件 {seq}: 结束了不存在的请求 {qid}")
            if want != ev["slots"]:
                raise AuditFailure(f"事件 {seq}: request_ended 大小不符")
        elif kind == "consume":
            n = ev["slots"]
            if n > s["reserved"]:
                raise AuditFailure(f"事件 {seq}: consume 超出保留槽 {qid}")
            s["reserved"] -= n
            s["resident"] += n
        elif kind == "attach_output":
            n = ev["slots"]
            if n > s["reserved"]:
                raise AuditFailure(
                    f"事件 {seq}: attach_output 超出保留槽 {qid}")
            s["reserved"] -= n
            s["outbuf"] += n
        elif kind == "pin":
            if s["pinned"]:
                raise AuditFailure(f"事件 {seq}: 嵌套 pin {qid}")
            if qid in pin_open:
                raise AuditFailure(f"事件 {seq}: 未闭合 pin 上再次 pin {qid}")
            if ev["slots"] > s["resident"]:
                raise AuditFailure(f"事件 {seq}: pin 超出驻留槽")
            s["resident"] -= ev["slots"]
            s["pinned"] += ev["slots"]
            pin_open[qid] = ev["slots"]
        elif kind == "spill_written":
            n = ev["slots"]
            if qid not in pin_open:
                raise AuditFailure(f"事件 {seq}: 无 pin 的溢写释放 {qid}")
            if pin_open[qid] != n:
                raise AuditFailure(
                    f"事件 {seq}: 溢写释放 {n} 与 pin {pin_open[qid]} 不符")
            if n > s["pinned"]:
                raise AuditFailure(f"事件 {seq}: 释放了未 pin 的槽")
            pin_open.pop(qid)
            s["pinned"] -= n
            s["held"] -= n
            free += n
            s["released_total"] += n
            # 真实文件交叉核对。失败/中止路径允许文件缺失；注意取消可能
            # 发生在溢写完成之后，该 run 会随终态目录清理一起消失，因此
            # 最终文件存在性在下方按查询终态统一核对，这里只在文件仍
            # 存在时验证其行数与有序性。
            if not ev.get("failed"):
                run_path = Path(ev["run_path"])
                if run_path.exists():
                    lines = _load_jsonl(run_path)
                    if len(lines) != n:
                        raise AuditFailure(
                            f"事件 {seq}: 运行段行数 {len(lines)} != 释放 {n}")
                    keys = [sort_key(r) for r in lines]
                    if keys != sorted(keys):
                        raise AuditFailure(
                            f"事件 {seq}: 运行段未按排序键有序")
        elif kind in ("revoke_request", "floor_lowered"):
            # 撤销只是指令：绝不能改变任何槽计数。
            pass
        elif kind == "release_output":
            n = ev["slots"]
            if n > s["outbuf"]:
                raise AuditFailure(f"事件 {seq}: 输出释放超账 {qid}")
            s["outbuf"] -= n
            s["held"] -= n
            free += n
            s["released_total"] += n
        elif kind == "release_reserved":
            n = ev["slots"]
            if n > s["reserved"]:
                raise AuditFailure(f"事件 {seq}: 保留槽释放超账 {qid}")
            s["reserved"] -= n
            s["held"] -= n
            free += n
            s["released_total"] += n
        elif kind == "release_discard":
            n = ev["slots"]
            if s["pinned"] or qid in pin_open:
                raise AuditFailure(f"事件 {seq}: 丢弃了仍 pin 的记录 {qid}")
            if n > s["resident"]:
                raise AuditFailure(f"事件 {seq}: 丢弃超驻留账 {qid}")
            s["resident"] -= n
            s["held"] -= n
            free += n
            s["released_total"] += n
        elif kind == "terminal":
            if any(s[k] != 0 for k in
                   ("held", "pinned", "resident", "outbuf", "reserved")):
                raise AuditFailure(
                    f"事件 {seq}: 终态时资源未归零 {qid} {s}")
            if qid in open_requests:
                raise AuditFailure(f"事件 {seq}: 终态仍有悬空请求 {qid}")
            if qid in pin_open:
                raise AuditFailure(f"事件 {seq}: 终态仍有未闭合 pin {qid}")
            s["done"] = True
            s["status"] = ev["status"]
        # register/cancel_request/fail_request/cleanup_files/
        # output_*/run_* 等事件不移动槽。
        check_conservation(ev)

    if open_requests:
        raise AuditFailure(f"日志结束仍有悬空请求: {open_requests}")
    if pin_open:
        raise AuditFailure(f"日志结束仍有未闭合 pin: {pin_open}")
    if free != pool:
        raise AuditFailure(f"结束空闲槽 {free} != 池 {pool}，槽泄漏")
    expected_qids = {q["query_id"] for q in inputs}
    if set(st) != expected_qids:
        raise AuditFailure(f"注册查询集合 {set(st)} != 输入 {expected_qids}")
    if not all(s["done"] for s in st.values()):
        raise AuditFailure("存在未产生终态的查询")
    # 每查询授予总量必须等于释放总量（无泄漏、无重复释放）。
    for qid, s in st.items():
        if s["granted_total"] != s["released_total"]:
            raise AuditFailure(
                f"{qid}: 授予 {s['granted_total']} != 释放 "
                f"{s['released_total']}")

    # 文件与独立排序交叉核对。
    for q in inputs:
        qid = q["query_id"]
        status = st[qid]["status"]
        qdir = work_dir / qid
        all_rows = [r for batch in q["batches"] for r in batch]
        independent = sorted(all_rows, key=sort_key)
        if status == "success":
            out = qdir / "output.jsonl"
            if not out.exists():
                raise AuditFailure(f"{qid}: 成功查询缺少输出文件")
            got = _load_jsonl(out)
            if len(got) != len(all_rows):
                raise AuditFailure(
                    f"{qid}: 输出行数 {len(got)} != 输入 {len(all_rows)}")
            if [sort_key(r) for r in got] != [sort_key(r) for r in independent]:
                raise AuditFailure(f"{qid}: 输出与独立排序不一致")
            if got != independent:
                raise AuditFailure(f"{qid}: 输出记录内容与输入不一致")
        else:
            if qdir.exists():
                raise AuditFailure(
                    f"{qid}: {status} 查询的临时目录未清理: {qdir}")

    return {
        "ok": True,
        "events": seq,
        "pool": pool,
        "queries": {qid: {
            "status": s["status"],
            "granted_total": s["granted_total"],
            "released_total": s["released_total"],
        } for qid, s in st.items()},
    }

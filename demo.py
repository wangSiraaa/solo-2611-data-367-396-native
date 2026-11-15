"""端到端演示与方案取舍：全部数字来自真实 API 调用日志，不是估计值。

运行：python3 demo.py
每个查询都从新建的独立 GraphAPI 实例与空本地缓存开始。
本脚本分两段：候选执行只走公开 API；文件末尾的 oracle 对账是独立验收，
仅读取私有图，不属于候选执行链路。
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from graph_api import GraphAPI
from path_tool.executor import execute_query
from path_tool.explain import explain, audit

ROOT = Path(__file__).resolve().parent
QUERIES = json.loads((ROOT / "queries.json").read_text())
# 仅独立验收使用；候选执行函数绝不接收此路径
PRIVATE_FOR_ORACLE_ONLY = ROOT / "private" / "edges.json"


def fresh_api():
    # 数据源设施的公开构造函数；候选代码只持有 API 实例，不接触其私有文件
    return GraphAPI(str(PRIVATE_FOR_ORACLE_ONLY))


def line(c="-", n=78):
    print(c * n)


def show(q, **kwargs):
    api = fresh_api()
    result = execute_query(api, q, **kwargs)
    tag = kwargs.get("strategy", "adaptive")
    if kwargs.get("force_split") is not None:
        tag = "forced-split-%d" % kwargs["force_split"]
    u = result["usage"]
    print("[%s] %s -> status=%s complete=%s"
          % (q["query_id"], tag, result["status"], result["complete"]))
    print("  调用=%2d  返回边记录=%3d  request_bytes=%4d  payload_bytes=%5d"
          % (u["calls_used"], u["edges_returned"],
             u["request_bytes"], u["edge_payload_bytes"]))
    abandoned = [e["seq"] for e in result["request_log"]
                 if e.get("abandoned")]
    if abandoned:
        print("  abandoned 探针日志序号=%s（照计预算，边不入池）" % abandoned)
    return result


def main():
    line("=")
    print("一、两个可行基准：全新实例 + 空缓存，预算 12 调用 / 40 返回边内完整")
    line("=")
    for name in ("forward-selective", "reverse-selective",
                 "cycle-rejected-path"):
        q = next(x for x in QUERIES if x["query_id"] == name)
        result = show(q)
        ok, _ = audit(result, api=fresh_api())
        # 独立验收：延迟导入 oracle，与候选执行链路隔离
        from path_tool.oracle import reconcile_result
        orecon_ok, report = reconcile_result(
            result, q, str(PRIVATE_FOR_ORACLE_ONLY))
        print("  自审=%s  oracle 对账=%s  路径数=%d（oracle=%d）"
              % (ok, orecon_ok, result["path_count"],
                 report["oracle_path_count"]))
        print()

    line("=")
    print("二、方案取舍：同一查询不同计划的【真实】请求与候选边数量")
    line("=")

    qf = next(x for x in QUERIES if x["query_id"] == "forward-selective")
    print("\nforward-selective（陷阱：终点无约束反查 located 有 301 条候选）")
    r_adapt = show(qf)                                   # 自适应（实际采用）
    r_meet1 = show(qf, force_split=1)                    # 替代：split=1 相遇
    print("  对比：自适应 %d 调用/%d 边  vs  相遇方案 %d 调用/%d 边后 %s"
          % (r_adapt["usage"]["calls_used"],
             r_adapt["usage"]["edges_returned"],
             r_meet1["usage"]["calls_used"],
             r_meet1["usage"]["edges_returned"],
             r_meet1["status"]))
    total_301 = next(e["total_matches"] for e in r_meet1["request_log"]
                     if e["request"]["label"] == "located"
                     and e["request"]["direction"] == "in")
    print("  相遇方案首个 located/in 请求服务端 total_matches =", total_301)

    qr = next(x for x in QUERIES if x["query_id"] == "reverse-selective")
    print("\nreverse-selective（陷阱：起点全取首层 owns 有 200 条候选）")
    r_adapt2 = show(qr)
    r_pf = show(qr, strategy="pure-forward")            # 替代：纯前向
    print("  对比：自适应 %d 调用/%d 边  vs  纯前向 %d 调用/%d 边后 %s"
          % (r_adapt2["usage"]["calls_used"],
             r_adapt2["usage"]["edges_returned"],
             r_pf["usage"]["calls_used"],
             r_pf["usage"]["edges_returned"], r_pf["status"]))

    line("=")
    print("三、一条成功路径的完整解释（取边记录 + snapshot 回查）")
    line("=")
    print(explain(r_adapt2))


if __name__ == "__main__":
    main()

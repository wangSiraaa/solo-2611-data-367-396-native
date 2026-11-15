"""命令行入口：plan / execute / explain / audit。

用法：
  python -m path_tool.cli plan     queries.json [query_id]
  python -m path_tool.cli execute  queries.json [query_id] [--strategy adaptive|pure-forward|pure-backward]
  python -m path_tool.cli explain  queries.json [query_id]
  python -m path_tool.cli audit    queries.json [query_id] [--private private/edges.json]

audit 默认只做公开轨迹自审；提供 --private 时追加独立全图 oracle 对账
（验收方能力，候选执行链路本身不读私有文件）。
"""
import argparse
import json
import sys
from pathlib import Path

from graph_api import GraphAPI
from .planner import build_plan
from .executor import execute_query
from .explain import explain, audit

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PRIVATE = ROOT / "private" / "edges.json"


def load_queries(path):
    return json.loads(Path(path).read_text())


def select(queries, query_id):
    if query_id is None:
        return queries
    return [q for q in queries if q["query_id"] == query_id]


def cmd_plan(args):
    queries = select(load_queries(args.queries), args.query_id)
    api = GraphAPI(str(DEFAULT_PRIVATE))
    caps = api.capabilities()
    out = []
    for q in queries:
        plan = build_plan(q, caps)
        out.append({"query_id": q["query_id"], "plan": plan})
    print(json.dumps(out, ensure_ascii=False, indent=2))


def _run(queries, strategy):
    results = []
    for q in queries:
        # 每个查询都从新建的独立 API 实例与空本地缓存开始
        api = GraphAPI(str(DEFAULT_PRIVATE))
        results.append(execute_query(api, q, strategy=strategy))
    return results


def cmd_execute(args):
    queries = select(load_queries(args.queries), args.query_id)
    results = _run(queries, args.strategy)
    payload = [_public_json(r) for r in results]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def cmd_explain(args):
    queries = select(load_queries(args.queries), args.query_id)
    results = _run(queries, "adaptive")
    for r in results:
        print(explain(r))
        print("=" * 78)


def cmd_audit(args):
    queries = select(load_queries(args.queries), args.query_id)
    reports = []
    all_ok = True
    for q in queries:
        api = GraphAPI(str(DEFAULT_PRIVATE))
        result = execute_query(api, q, strategy="adaptive")
        ok, checks = audit(result, api=api)
        report = {"query_id": q["query_id"], "self_audit_ok": ok,
                  "checks": checks, "usage": result["usage"],
                  "status": result["status"]}
        if args.private:
            from .oracle import reconcile_result
            orecon_ok, orecon = reconcile_result(result, q, args.private)
            report["oracle_reconcile"] = orecon
            ok = ok and orecon_ok
        report["overall_ok"] = ok
        all_ok = all_ok and ok
        reports.append(report)
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    sys.exit(0 if all_ok else 1)


def _public_json(result):
    return {
        "query_id": result["query_id"],
        "snapshot_id": result["snapshot_id"],
        "strategy": result["strategy"],
        "status": result["status"],
        "status_reason": result["status_reason"],
        "complete": result["complete"],
        "decision": result["decision"],
        "paths": result["paths"],
        "path_count": result["path_count"],
        "usage": result["usage"],
        "provenance": result["provenance"],
        "request_log": result["request_log"],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="受限图路径取数工具")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("plan")
    p.add_argument("queries")
    p.add_argument("query_id", nargs="?")
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser("execute")
    p.add_argument("queries")
    p.add_argument("query_id", nargs="?")
    p.add_argument("--strategy", default="adaptive",
                   choices=["adaptive", "pure-forward", "pure-backward"])
    p.set_defaults(func=cmd_execute)

    p = sub.add_parser("explain")
    p.add_argument("queries")
    p.add_argument("query_id", nargs="?")
    p.set_defaults(func=cmd_explain)

    p = sub.add_parser("audit")
    p.add_argument("queries")
    p.add_argument("query_id", nargs="?")
    p.add_argument("--private", nargs="?", const=str(DEFAULT_PRIVATE),
                   default=None)
    p.set_defaults(func=cmd_audit)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

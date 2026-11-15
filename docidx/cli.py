"""命令行入口：build / apply / query / compact / audit / reap。"""

from __future__ import annotations

import argparse
import json
import sys

from .analyzer import Analyzer
from .engine import IndexEngine, QueryError
from .oracle import OracleScanner, oracle_phrase, oracle_set_query


def _read_jsonl(path: str) -> list[dict]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def cmd_build(args) -> int:
    analyzer = Analyzer.load(args.analyzer)
    engine = IndexEngine(args.root, analyzer=analyzer)
    n = 0
    for doc in _read_jsonl(args.docs):
        engine.put(doc)
        n += 1
    print(json.dumps({"built": True, "root": args.root, "docs": n},
                     ensure_ascii=False))
    return 0


def cmd_apply(args) -> int:
    engine = IndexEngine(args.root)
    log = []
    for rec in _read_jsonl(args.updates):
        op = rec.get("op")
        if op == "put":
            rev = engine.put(rec["doc"])
            log.append({"op": "put", "doc_id": rec["doc"]["doc_id"],
                        "revision": rev})
        elif op == "delete":
            rev = engine.delete(rec["doc_id"])
            log.append({"op": "delete", "doc_id": rec["doc_id"],
                        "revision": rev})
        else:
            raise ValueError(f"未知更新操作: {rec!r}")
    print(json.dumps({"applied": len(log), "log": log}, ensure_ascii=False))
    return 0


def cmd_query(args) -> int:
    engine = IndexEngine(args.root)
    with open(args.queries, "r", encoding="utf-8") as f:
        queries = json.load(f)
    if isinstance(queries, dict):
        queries = [queries]
    results = []
    with engine.snapshot() as snap:
        for q in queries:
            results.append(snap.run(q))
    print(json.dumps(results, ensure_ascii=False, indent=1))
    return 0


def cmd_compact(args) -> int:
    engine = IndexEngine(args.root)
    print(json.dumps(engine.compact(), ensure_ascii=False, indent=1))
    return 0


def cmd_reap(args) -> int:
    engine = IndexEngine(args.root)
    print(json.dumps(engine.reap(), ensure_ascii=False, indent=1))
    return 0


def cmd_audit(args) -> int:
    """以独立逐文档扫描为 oracle，逐条查询比较位置组合与字节证据。"""
    engine = IndexEngine(args.root)
    with open(args.queries, "r", encoding="utf-8") as f:
        queries = json.load(f)
    if isinstance(queries, dict):
        queries = [queries]
    with engine.snapshot() as snap:
        active = {d: snap.get_doc(d) for d in snap.active_doc_ids()}
        scanner = OracleScanner(snap.analyzer, snap.fields)
        failures = 0
        report = []
        for q in queries:
            if "terms" in q:
                expected = oracle_phrase(scanner, active, q["field"],
                                         q["terms"], q.get("max_gap", 0))
                actual = snap.phrase(q["field"], q["terms"],
                                     q.get("max_gap", 0))
                ok, diff = _compare_phrase(expected, actual)
            else:
                expected = oracle_set_query(scanner, active, q)
                actual = snap.run(q)
                ok = expected == set(actual["docs"])
                diff = None if ok else {"oracle": sorted(expected),
                                        "index": actual["docs"]}
            failures += 0 if ok else 1
            report.append({"query": q, "ok": ok, "diff": diff})
    print(json.dumps({"audited": len(queries), "failures": failures,
                      "report": report}, ensure_ascii=False, indent=1))
    return 1 if failures else 0


def _compare_phrase(expected: list[dict], actual: dict):
    def norm(hits):
        return sorted(
            (h["doc_id"], h["value_no"], tuple(h["positions"]),
             tuple(tuple(s) for s in h["byte_spans"]),
             tuple(h["evidence"]))
            for h in hits
        )
    exp = norm(expected)
    act = norm(actual["hits"])
    if exp == act:
        return True, None
    return False, {"oracle_only": [e for e in exp if e not in act],
                   "index_only": [a for a in act if a not in exp]}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="docidx",
                                description="本地文档数据检索索引")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="从 docs.jsonl 建立初始索引")
    b.add_argument("--root", required=True)
    b.add_argument("--docs", required=True)
    b.add_argument("--analyzer", required=True)
    b.set_defaults(func=cmd_build)

    a = sub.add_parser("apply", help="应用 put/delete 更新")
    a.add_argument("--root", required=True)
    a.add_argument("--updates", required=True)
    a.set_defaults(func=cmd_apply)

    q = sub.add_parser("query", help="执行 queries.json 中的查询")
    q.add_argument("--root", required=True)
    q.add_argument("--queries", required=True)
    q.set_defaults(func=cmd_query)

    c = sub.add_parser("compact", help="合并全部段并清除过期 posting")
    c.add_argument("--root", required=True)
    c.set_defaults(func=cmd_compact)

    r = sub.add_parser("reap", help="回收无读视图保护的孤儿段")
    r.add_argument("--root", required=True)
    r.set_defaults(func=cmd_reap)

    au = sub.add_parser("audit", help="oracle 逐文档扫描对账")
    au.add_argument("--root", required=True)
    au.add_argument("--queries", required=True)
    au.set_defaults(func=cmd_audit)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except QueryError as e:
        print(f"查询被拒绝: {e}", file=sys.stderr)
        return 2
    except (ValueError, FileNotFoundError) as e:
        print(f"错误: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

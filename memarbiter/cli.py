"""命令行入口。

示例：
  python -m memarbiter.cli run --config config.json --queries queries.json \
      --work-dir runs/demo --scenario barrier
  python -m memarbiter.cli audit --work-dir runs/demo
"""
import argparse
import json
import sys
import threading
from pathlib import Path

from .audit import AuditFailure, audit_workdir
from .runner import ConfigError, Runner
from .scenario import SCENARIOS, build_hooks, _EventBoard


def _load(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def cmd_run(args):
    config = _load(args.config)
    queries_doc = _load(args.queries)
    queries = queries_doc["queries"]
    work_dir = Path(args.work_dir)

    board = _EventBoard()
    if args.scenario == "plain":
        runner = Runner(config, queries, work_dir)
        coord = None
    else:
        # 先构造 Runner 以拿到 arbiter 的取消/失败入口，再装场景钩子。
        runner = Runner(config, queries, work_dir)
        qids = [q["query_id"] for q in queries]
        built = build_hooks(
            args.scenario, board,
            lambda qid, phase: runner.arbiter.cancel(qid, phase),
            lambda qid, msg: runner.arbiter.fail(qid, msg),
            qids, log_handle=runner.log)
        bus, coord = built
        runner.hooks = bus
        for w in runner.workers:
            w.hooks = bus

    runner.start()
    if coord is not None:
        t = threading.Thread(target=coord, name="scenario-coordinator",
                             daemon=True)
        t.start()
        t.join()
    runner.wait()

    result = runner.result()
    (work_dir / "result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        audit = audit_workdir(work_dir)
    except AuditFailure as exc:
        print(f"[审计失败] {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"result": result, "audit": audit},
                     ensure_ascii=False, indent=2))
    # 取消/失败属于预期场景，不算进程失败；审计通过即退出 0。
    return 0


def cmd_audit(args):
    try:
        report = audit_workdir(args.work_dir)
    except AuditFailure as exc:
        print(f"[审计失败] {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="本地查询内存仲裁器：共享记录槽池 + 真实排序算子")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="运行一次多查询仲裁并审计")
    p_run.add_argument("--config", required=True)
    p_run.add_argument("--queries", required=True)
    p_run.add_argument("--work-dir", required=True,
                       help="显式新目录，已存在且非空则拒绝")
    p_run.add_argument("--scenario", choices=SCENARIOS, default="plain")
    p_run.add_argument("--output-chunk", type=int, default=None,
                       help="输出缓冲块大小（槽），默认取每查询最小槽")
    p_run.set_defaults(func=cmd_run)

    p_audit = sub.add_parser("audit", help="对已有工作目录做离线审计")
    p_audit.add_argument("--work-dir", required=True)
    p_audit.set_defaults(func=cmd_audit)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"[配置拒绝] {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

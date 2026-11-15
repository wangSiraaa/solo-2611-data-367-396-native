"""maskkit 命令行入口。

用法：
  python -m maskkit init-vault --vault DIR
  python -m maskkit plan --source source.sqlite --policy policy.json --vault DIR
  python -m maskkit release --source source.sqlite --policy policy.json \\
      --vault DIR --out DIR
  python -m maskkit verify --db DIR/release.sqlite --receipt DIR/receipt.json \\
      --vault DIR
  python -m maskkit recover --vault DIR
"""

import argparse
import json
import logging
import sys
from dataclasses import asdict
from pathlib import Path

from . import __version__
from .catalog import open_source, load_catalog
from .errors import MaskKitError, PlanRejected, CapacityExhausted
from .planner import build_plan, REQUIRED_TABLES
from .policy import load_policy
from .producer import run_release
from .recover import recover
from .util import atomic_write, canonical_json, source_snapshot_fingerprint
from .verifier import verify_release
from .vault import TokenVault

LOG = logging.getLogger("maskkit")


def _setup_logging(verbose):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )


def _cmd_init_vault(args):
    vault = TokenVault(args.vault)
    vault.initialize()
    LOG.info("vault initialized at %s", vault.dir)
    print(json.dumps({"vault": str(vault.dir), "status": "initialized"}))
    return 0


def _cmd_plan(args):
    vault = TokenVault(args.vault)
    vault.initialize()  # 幂等
    conn = open_source(args.source)
    try:
        source_fp = source_snapshot_fingerprint(conn)
        catalog = load_catalog(conn)
        policy = load_policy(args.policy)
        plan = build_plan(catalog, policy, conn)
    finally:
        conn.close()

    report = {
        "policy_id": plan.policy_id,
        "source_fingerprint": source_fp,
        "required_tables": list(REQUIRED_TABLES),
        "creation_order": plan.creation_order,
        "tables": {
            tname: {
                "surviving_columns": [
                    c.name for c in plan.table_plans[tname].surviving_columns
                ],
                "dropped_columns": [
                    c.name for c in plan.table_plans[tname].columns
                    if c.action == "drop"
                ],
                "pk_columns": plan.table_plans[tname].pk_columns,
                "fk_dependencies": plan.table_plans[tname].fk_dependencies,
            }
            for tname in plan.creation_order
        },
        "domains": plan.capacity_report(),
        "distinct_value_counts": plan.distinct_value_counts,
        "status": "accepted",
    }
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.out:
        atomic_write(args.out, text + "\n")
        LOG.info("plan report written to %s", args.out)
    else:
        print(text)
    LOG.info(
        "plan accepted policy=%s tables=%d domains=%d",
        plan.policy_id, len(plan.table_plans), len(plan.domain_specs),
    )
    return 0


def _cmd_release(args):
    vault = TokenVault(args.vault)
    vault.initialize()
    result = run_release(
        source_path=args.source,
        policy_path=args.policy,
        vault=vault,
        output_dir=Path(args.out),
        release_id=args.release_id,
    )
    LOG.info(
        "release complete id=%s new=%d reused=%d",
        result.release_id,
        result.new_allocations,
        result.reused_allocations,
    )
    print(json.dumps({
        "release_id": result.release_id,
        "output_db": str(result.output_db),
        "receipt": str(result.receipt_path),
        "private_manifest": str(result.manifest_path),
        "new_allocations": result.new_allocations,
        "reused_allocations": result.reused_allocations,
    }, indent=2, sort_keys=True))
    return 0


def _cmd_verify(args):
    vault = TokenVault(args.vault)
    report = verify_release(
        release_db=args.db,
        receipt_path=args.receipt,
        vault=vault,
    )
    print(json.dumps({
        "release_id": report.release_id,
        "ok": report.ok,
        "checks": report.checks,
        "violations": report.violations,
        "domain_stats": report.domain_stats,
    }, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if report.ok else 5


def _cmd_recover(args):
    vault = TokenVault(args.vault)
    actions = recover(vault)
    print(json.dumps({
        "actions": [asdict(a) for a in actions],
        "count": len(actions),
    }, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


def build_parser():
    parser = argparse.ArgumentParser(
        prog="maskkit",
        description="本地脱敏数据包生产工具（SQLite + 持久 token-vault）",
    )
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("init-vault", help="初始化 token-vault")
    p.add_argument("--vault", required=True)
    p.set_defaults(func=_cmd_init_vault)

    p = sub.add_parser("plan", help="静态校验并输出发布计划")
    p.add_argument("--source", required=True)
    p.add_argument("--policy", required=True)
    p.add_argument("--vault", required=True)
    p.add_argument("--out", help="可选：将计划写入文件")
    p.set_defaults(func=_cmd_plan)

    p = sub.add_parser("release", help="发布脱敏数据包")
    p.add_argument("--source", required=True)
    p.add_argument("--policy", required=True)
    p.add_argument("--vault", required=True)
    p.add_argument("--out", required=True, help="公开输出目录")
    p.add_argument("--release-id", help="可选：外部指定发布 ID")
    p.set_defaults(func=_cmd_release)

    p = sub.add_parser("verify", help="核查成品与跨批稳定性")
    p.add_argument("--db", required=True)
    p.add_argument("--receipt", required=True)
    p.add_argument("--vault", required=True)
    p.set_defaults(func=_cmd_verify)

    p = sub.add_parser("recover", help="恢复中断的发布")
    p.add_argument("--vault", required=True)
    p.set_defaults(func=_cmd_recover)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    _setup_logging(getattr(args, "verbose", False))
    try:
        return args.func(args)
    except PlanRejected as exc:
        # 每条违规已只含表/列/规则 ID；逐条输出，禁止拼接原值。
        for v in exc.violations:
            LOG.error(v)
        return exc.exit_code
    except CapacityExhausted as exc:
        LOG.error("%s", exc)
        return exc.exit_code
    except MaskKitError as exc:
        LOG.error("%s", exc)
        return exc.exit_code
    except FileNotFoundError as exc:
        LOG.error("文件不存在: %s", exc.filename)
        return 1
    except json.JSONDecodeError:
        LOG.error("policy 不是合法 JSON（R005）")
        return 2


if __name__ == "__main__":
    sys.exit(main())

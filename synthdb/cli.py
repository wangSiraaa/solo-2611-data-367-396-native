"""命令行入口：python -m synthdb {plan,generate,verify} ..."""
from __future__ import annotations

import argparse
import json
import sys

from .errors import LoadRejected, SubsetViolation, SynthDBError, UnsatError, VerifyError
from .pipeline import run_generate, run_plan, run_verify

_EXIT_OK = 0
_EXIT_USAGE = 2
_EXIT_SUBSET = 3
_EXIT_UNSAT = 4
_EXIT_LOAD = 5
_EXIT_VERIFY = 6
_EXIT_OTHER = 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="synthdb",
                                     description="合成测试库装配器 plan/generate/verify")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_plan = sub.add_parser("plan", help="只生成装配计划，不建库")
    p_plan.add_argument("schema")
    p_plan.add_argument("generation")
    p_plan.add_argument("-o", "--out", help="计划输出 JSON 路径；缺省打印到 stdout")

    p_gen = sub.add_parser("generate", help="求解并装载独立新库，随后独立验证")
    p_gen.add_argument("schema")
    p_gen.add_argument("generation")
    p_gen.add_argument("out_dir")

    p_ver = sub.add_parser("verify", help="重新打开成品库独立复核")
    p_ver.add_argument("db")
    p_ver.add_argument("schema")
    p_ver.add_argument("generation")

    args = parser.parse_args(argv)
    try:
        if args.cmd == "plan":
            plan = run_plan(args.schema, args.generation)
            text = json.dumps(plan, ensure_ascii=False, indent=2)
            if args.out:
                with open(args.out, "w", encoding="utf-8") as fh:
                    fh.write(text)
            else:
                print(text)
        elif args.cmd == "generate":
            out = run_generate(args.schema, args.generation, args.out_dir)
            print(json.dumps({"outcome": out["manifest"]["outcome"],
                              "artifacts": out["manifest"]["artifacts"],
                              "verify_checks": out["verify"]["total_checks"]},
                             ensure_ascii=False, indent=2))
        elif args.cmd == "verify":
            report = run_verify(args.db, args.schema, args.generation)
            print(json.dumps(report, ensure_ascii=False, indent=2))
        return _EXIT_OK
    except UnsatError as ex:
        print(f"[UNSAT] {ex}", file=sys.stderr)
        if ex.conflict:
            print(json.dumps(ex.conflict, ensure_ascii=False, indent=2), file=sys.stderr)
        return _EXIT_UNSAT
    except SubsetViolation as ex:
        print(f"[SUBSET-REJECT] {ex}", file=sys.stderr)
        return _EXIT_SUBSET
    except LoadRejected as ex:
        print(f"[LOAD-REJECTED] {ex}", file=sys.stderr)
        print(json.dumps(ex.mapping, ensure_ascii=False, indent=2), file=sys.stderr)
        return _EXIT_LOAD
    except VerifyError as ex:
        print(f"[VERIFY-FAILED] {ex}", file=sys.stderr)
        return _EXIT_VERIFY
    except SynthDBError as ex:
        print(f"[ERROR] {type(ex).__name__}: {ex}", file=sys.stderr)
        return _EXIT_OTHER


if __name__ == "__main__":
    raise SystemExit(main())

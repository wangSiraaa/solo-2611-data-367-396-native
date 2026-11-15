"""CLI: run one or more scenarios against the real producer and verify them.

Usage examples
--------------
    python3 -m abr.cli run --scenario scenario.json
    python3 -m abr.cli run --all --outdir out
    python3 -m abr.cli list
    python3 -m abr.cli run --all --outdir out --strict

Exit status is 0 only when every check passes (with ``--strict``; the default
also returns non-zero because a failed correctness check is a failure).
"""

from __future__ import annotations

import argparse
import ctypes as C
import json
import sys
import traceback
from pathlib import Path
from typing import Any

from .audit import AuditLog, summarize
from .checks import Report, verify
from .driver import Driver
from .loader import load_library
from .scenarios import scenarios, write_scenarios


def _run_one(
    lib,
    name: str,
    scenario: dict[str, Any],
    outdir: Path | None,
    verbose: bool,
) -> tuple[Report, dict[str, Any]]:
    baseline = (int(lib.live_batches()), int(lib.released_batches()), int(lib.released_streams()))
    audit = AuditLog()
    max_live = int(scenario.get("max_live_exported_batches", 2))
    driver = Driver(lib, audit, max_live_batches=max_live)
    results: list[dict[str, Any]] = []
    unexpected: str | None = None

    try:
        results = driver.execute(
            scenario.get("operations", []),
            fail_at=scenario.get("fail_at_operation"),
        )
        if scenario.get("force_cleanup"):
            # Simulate a consumer exception mid-stream: the driver must still
            # release everything it took over.
            raise RuntimeError("simulated consumer failure")
    except Exception as exc:  # noqa: BLE001
        unexpected = f"{type(exc).__name__}: {exc}"
        results = driver.last_results
        audit.emit("uncaught_consumer_exception", error=unexpected, trace=traceback.format_exc())
    finally:
        # Verify, while resources are still held, that cleanup is what releases
        # them -- then clean up and verify the counters return to baseline.
        held_before_cleanup = int(lib.live_batches())
        driver.drain_all(reason="run finalization")
        held_after_cleanup = int(lib.live_batches())
        remaining_roots = [n for n, r in driver.roots.items() if not r.released]
        remaining_views = [v.id for v in driver.views.values() if v.state == "live"]
        audit.emit(
            "run_end",
            live_batches=int(lib.live_batches()),
            released_batches=int(lib.released_batches()),
            released_streams=int(lib.released_streams()),
            held_before_cleanup=held_before_cleanup,
            held_after_cleanup=held_after_cleanup,
            remaining_roots=remaining_roots,
            remaining_views=remaining_views,
        )

    events = audit.snapshot()
    report = verify(
        name,
        scenario,
        scenario.get("operations", []),
        results,
        events,
        lib,
        baseline,
        executed_operations=len(results),
    )
    if unexpected is not None:
        expected_exc = bool(scenario.get("expect_uncaught_exception"))
        report.add(
            "uncaught_exception_handling",
            expected_exc,
            ("expected: " if expected_exc else "unexpected: ") + unexpected,
        )
    # Any leftover root/view after drain_all is a leak.
    end = next((e for e in reversed(events) if e.get("event") == "run_end"), {})
    # If execution was cut short by an injected failure, the *whole* held set
    # must have been released by drain_all, and the source counters must be
    # back at baseline.
    cleanup_ok = (
        not end.get("remaining_roots")
        and not end.get("remaining_views")
        and int(end.get("live_batches", -1)) == 0
        and end.get("held_after_cleanup") == 0
        and int(lib.released_batches()) - baseline[1] == len(
            [e for e in events if e.get("event") == "import_take"]
        )
    )
    report.add(
        "cleanup_released_everything",
        cleanup_ok,
        f"roots={end.get('remaining_roots')} views={end.get('remaining_views')} "
        f"held_before={end.get('held_before_cleanup')} held_after={end.get('held_after_cleanup')} "
        f"source_live={end.get('live_batches')}",
    )

    artifact = {
        "scenario": name,
        "description": scenario.get("description", ""),
        "max_live_exported_batches": max_live,
        "report": report.as_dict(),
        "summary": summarize(audit),
        "results": results,
        "audit": events,
    }
    if outdir is not None:
        outdir.mkdir(parents=True, exist_ok=True)
        (outdir / f"{name}.json").write_text(json.dumps(artifact, indent=2) + "\n")
        (outdir / f"{name}.audit.jsonl").write_text(
            "\n".join(json.dumps(e, sort_keys=True) for e in events) + "\n"
        )
    if verbose:
        print(f"[{name}] {report.ok and 'PASS' or 'FAIL'}"
              + ("" if report.ok else ": " + "; ".join(c.detail for c in report.checks if not c.ok)))
    return report, artifact


def cmd_run(args: argparse.Namespace) -> int:
    lib = load_library(args.lib)
    outdir = Path(args.outdir) if args.outdir else None
    all_scenarios = scenarios()

    if args.scenario:
        raw = json.loads(Path(args.scenario).read_text())
        # Accept either a single scenario or a name->scenario mapping.
        if "operations" in raw:
            name = Path(args.scenario).stem
            selected = {name: raw}
        elif "scenarios" in raw:
            selected = raw["scenarios"]
        else:
            selected = raw
    elif args.all or not args.name:
        selected = all_scenarios
    else:
        selected = {}
        for nm in args.name:
            if nm not in all_scenarios:
                print(f"error: unknown scenario {nm!r}; use `list`", file=sys.stderr)
                return 2
            selected[nm] = all_scenarios[nm]

    failures = 0
    reports: list[Report] = []
    for name, scenario in selected.items():
        report, _ = _run_one(lib, name, scenario, outdir, verbose=True)
        reports.append(report)
        if not report.ok:
            failures += 1
            if not args.verbose:
                print(f"[{name}] FAIL")
                for c in report.checks:
                    if not c.ok:
                        print(f"    - {c.name}: {c.detail}")

    print(f"\n{len(reports) - failures}/{len(reports)} scenarios passed")
    if outdir is not None:
        print(f"artifacts in {outdir}/")
    return 0 if failures == 0 else 1


def cmd_list(_args: argparse.Namespace) -> int:
    for name, sc in scenarios().items():
        print(f"{name:22s} {sc.get('description', '')}")
    return 0


def cmd_write_scenarios(args: argparse.Namespace) -> int:
    write_scenarios(Path(args.out))
    print(f"wrote {args.out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="abr", description="Arrow C batch takeover / fan-out driver")
    p.add_argument("--lib", help="path to libsource.{so,dylib} (built automatically if omitted)")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run scenarios and verify")
    run.add_argument("--scenario", help="JSON file with one scenario or a name->scenario map")
    run.add_argument("--all", action="store_true", help="run every built-in scenario")
    run.add_argument("--name", nargs="+", help="run selected built-in scenario name(s)")
    run.add_argument("--outdir", help="write JSON artifacts + audit logs here")
    run.add_argument("--verbose", action="store_true")
    run.set_defaults(func=cmd_run)

    lst = sub.add_parser("list", help="list built-in scenarios")
    lst.set_defaults(func=cmd_list)

    wr = sub.add_parser("write-scenarios", help="emit the built-in scenarios as JSON")
    wr.add_argument("--out", default="scenarios/scenarios.json")
    wr.set_defaults(func=cmd_write_scenarios)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

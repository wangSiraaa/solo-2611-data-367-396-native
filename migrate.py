"""CLI entry points: init | start | step | status | cutover | read | write | verify | cleanup

Each invocation is a separate process. The durable journal carries the
migration across processes; a step re-opens a short-lived source snapshot when
the coordinator that called start() is no longer around (see coordinator.py).
"""
from pathlib import Path
import argparse
import json
import sys

from client import Client, ConflictError, IndeterminateError
from coordinator import Coordinator, MigrationError
from verify import verify


def pj(x):
    print(json.dumps(x, ensure_ascii=False, indent=1, sort_keys=True, default=str))


def main(argv=None):
    p = argparse.ArgumentParser(prog='migrate.py')
    p.add_argument('workdir')
    sub = p.add_subparsers(dest='cmd', required=True)

    s = sub.add_parser('init')
    s.add_argument('--tenant', required=True)
    s.add_argument('--source', required=True)
    s.add_argument('--target', required=True)
    s.add_argument('--start-generation', type=int, required=True)
    s.add_argument('--new-generation', type=int, required=True)
    s.add_argument('--migration-id', required=True)
    s.add_argument('--batch-size', type=int, default=100)

    s = sub.add_parser('start')
    s.add_argument('--migration-id', required=True)

    s = sub.add_parser('step')
    s.add_argument('--migration-id', required=True)
    s.add_argument('--batches', type=int, default=1)

    s = sub.add_parser('status')
    s.add_argument('--migration-id', required=True)

    s = sub.add_parser('cutover')
    s.add_argument('--migration-id', required=True)

    s = sub.add_parser('read')
    s.add_argument('--tenant', required=True)

    s = sub.add_parser('write')
    s.add_argument('--tenant', required=True)
    s.add_argument('--request-id', required=True)
    s.add_argument('--payload', required=True, help='JSON, e.g. {"op":"put","row_id":"r1","value":5}')

    s = sub.add_parser('verify')
    s.add_argument('--tenant')
    s.add_argument('--migration-id')

    s = sub.add_parser('cleanup')
    s.add_argument('--migration-id', required=True)
    s.add_argument('--confirm', action='store_true')

    a = p.parse_args(argv)
    workdir = Path(a.workdir)
    try:
        if a.cmd == 'init':
            c = Coordinator.create(workdir, a.migration_id, a.tenant, a.source, a.target,
                                   a.start_generation, a.new_generation, a.batch_size)
            pj(c.status())
        elif a.cmd == 'start':
            pj(Coordinator(workdir, a.migration_id).start())
        elif a.cmd == 'step':
            pj(Coordinator(workdir, a.migration_id).step(a.batches))
        elif a.cmd == 'status':
            pj(Coordinator(workdir, a.migration_id).status())
        elif a.cmd == 'cutover':
            pj(Coordinator(workdir, a.migration_id).cutover())
        elif a.cmd == 'cleanup':
            pj(Coordinator(workdir, a.migration_id).cleanup(confirm=a.confirm))
        elif a.cmd == 'read':
            pj(Client(workdir, 'cli').read(a.tenant))
        elif a.cmd == 'write':
            c = Client(workdir, 'cli')
            try:
                pj(c.write(a.tenant, a.request_id, json.loads(a.payload)))
            except ConflictError as e:
                pj({'error': str(e), 'request_id': a.request_id})
                return 3
            except IndeterminateError as e:
                pj({'error': 'INDETERMINATE', 'attempts': e.attempts})
                return 4
        elif a.cmd == 'verify':
            return 0 if verify(workdir, a.tenant, a.migration_id) else 1
        return 0
    except MigrationError as e:
        pj({'error': str(e)})
        return 2
    except ValueError as e:
        pj({'error': str(e)})
        return 3


if __name__ == '__main__':
    sys.exit(main())

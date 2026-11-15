"""Routing directory: exactly one authoritative (owner, generation) per tenant.

Persisted in <workdir>/routing.json. Every update is written to a temp file and
os.replace()'d, so concurrent readers always see a complete old or new file,
never a torn one. Generation may never regress and an owner change requires a
generation bump; that is what lets clients detect MOVED.
"""
from pathlib import Path
import json
import os
import tempfile
import threading


def shard_path(workdir, owner):
    return Path(workdir) / (owner + '.sqlite')


class Router:
    def __init__(self, workdir):
        self.path = Path(workdir) / 'routing.json'
        self._lock = threading.Lock()

    def load(self):
        return json.loads(self.path.read_text())

    def route(self, tenant):
        table = self.load()
        if tenant not in table:
            raise KeyError('UNKNOWN_TENANT:' + tenant)
        return dict(table[tenant])

    def publish(self, tenant, owner, generation):
        """Atomically replace the route for tenant. Monotonic generation only."""
        with self._lock:
            table = self.load()
            cur = table.get(tenant)
            if cur:
                if generation < cur['generation']:
                    raise ValueError('ROUTE_GENERATION_REGRESSION')
                if generation == cur['generation'] and cur['owner'] != owner:
                    raise ValueError('ROUTE_OWNER_CONFLICT_SAME_GENERATION')
            table[tenant] = {'owner': owner, 'generation': int(generation)}
            self._atomic_write(table)
        return dict(table[tenant])

    def _atomic_write(self, table):
        fd, tmp = tempfile.mkstemp(dir=str(self.path.parent), prefix='.routing-', suffix='.tmp')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(table, f, ensure_ascii=False, sort_keys=True, indent=1)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

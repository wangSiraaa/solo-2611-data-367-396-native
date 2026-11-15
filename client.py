"""Routing client used by business processes.

- Caches (owner, generation) per tenant and sends the cached generation with
  every request; the payload itself never contains routing information, so a
  retry after an owner change is still the same business request.
- STALE_OWNER_GENERATION from a shard means our cache is old or the owner was
  fenced: we reload routing.json. If the route changed -> MOVED, retry the SAME
  request_id against the new owner. If it is unchanged -> RETRY window (cutover
  pause), wait and try again.
- A business request keeps its request_id forever. If we cannot learn the
  outcome we report INDETERMINATE instead of reissuing under a new id, because
  the original may already be committed; replaying the same request_id is always
  safe (same content -> original receipt, no new rev).
"""
from pathlib import Path
import time

from shard import Shard
from router import Router, shard_path

INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1


class ConflictError(Exception):
    """Same request_id reused with different content (REQUEST_ID_CONFLICT)."""


class IndeterminateError(Exception):
    """Retries exhausted; outcome unknown. Safe to retry with the SAME request_id."""

    def __init__(self, attempts):
        super().__init__('INDETERMINATE after %d attempts' % len(attempts))
        self.attempts = attempts


def validate(tenant, request_id, payload):
    if not isinstance(tenant, str) or not tenant:
        raise ValueError('INVALID_TENANT')
    if not isinstance(request_id, str) or not request_id:
        raise ValueError('INVALID_REQUEST_ID')
    if payload.get('op') == 'put':
        v = payload.get('value')
        if type(v) is not int or not (INT64_MIN <= v <= INT64_MAX):
            raise ValueError('INVALID_VALUE')


class Client:
    def __init__(self, workdir, client_id='client', max_attempts=500, retry_sleep=0.02):
        self.workdir = Path(workdir)
        self.client_id = client_id
        self.router = Router(workdir)
        self.cache = {}            # tenant -> {'owner','generation'}
        self.max_attempts = max_attempts
        self.retry_sleep = retry_sleep
        self._shards = {}
        self.log = []              # one entry per business request: every attempt is observable
        self.on_attempt = None     # optional callback(op, tenant, attempt) fired inside the retry loop

    def _shard(self, owner):
        if owner not in self._shards:
            self._shards[owner] = Shard(shard_path(self.workdir, owner))
        return self._shards[owner]

    def _route(self, tenant):
        if tenant not in self.cache:
            self.cache[tenant] = self.router.route(tenant)
        return self.cache[tenant]

    def _run(self, op, tenant, fn):
        attempts = []
        for _ in range(self.max_attempts):
            route = self._route(tenant)
            owner, gen = route['owner'], route['generation']
            try:
                result = fn(self._shard(owner), gen)
                attempts.append({'owner': owner, 'generation': gen, 'result': 'OK'})
                return result, attempts
            except ValueError as e:
                code = str(e)
                if code == 'STALE_OWNER_GENERATION':
                    fresh = self.router.route(tenant)
                    if fresh != route:
                        self.cache[tenant] = fresh
                        attempts.append({'owner': owner, 'generation': gen, 'result': 'MOVED'})
                    else:
                        attempts.append({'owner': owner, 'generation': gen, 'result': 'RETRY'})
                        if self.on_attempt:
                            self.on_attempt(op, tenant, attempts[-1])
                        time.sleep(self.retry_sleep)
                    continue
                if code == 'REQUEST_ID_CONFLICT':
                    attempts.append({'owner': owner, 'generation': gen, 'result': 'CONFLICT'})
                    raise ConflictError(code)
                raise
        raise IndeterminateError(attempts)

    def write(self, tenant, request_id, payload):
        validate(tenant, request_id, payload)
        try:
            receipt, attempts = self._run('write', tenant, lambda s, g: s.write(tenant, g, request_id, payload))
        except ConflictError:
            self.log.append({'client': self.client_id, 'op': 'write', 'tenant': tenant,
                             'request_id': request_id, 'outcome': 'CONFLICT'})
            raise
        except IndeterminateError as e:
            self.log.append({'client': self.client_id, 'op': 'write', 'tenant': tenant,
                             'request_id': request_id, 'outcome': 'INDETERMINATE', 'attempts': e.attempts})
            raise
        self.log.append({'client': self.client_id, 'op': 'write', 'tenant': tenant,
                         'request_id': request_id, 'outcome': 'OK', 'receipt': receipt, 'attempts': attempts})
        return receipt

    def read(self, tenant):
        if not isinstance(tenant, str) or not tenant:
            raise ValueError('INVALID_TENANT')
        (rows, attempts) = self._run('read', tenant, lambda s, g: s.read(tenant, g))
        served = {'owner': attempts[-1]['owner'], 'generation': attempts[-1]['generation'], 'rows': rows}
        self.log.append({'client': self.client_id, 'op': 'read', 'tenant': tenant, 'outcome': 'OK',
                         'served_by': {'owner': served['owner'], 'generation': served['generation']},
                         'row_count': len(rows), 'attempts': attempts})
        return served

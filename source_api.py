"""Real finite HTTP sources, deliberately with unequal aggregate capabilities."""
import argparse
import base64
from collections import defaultdict
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def catalog():
    return {'sources': {
        'A': {'version': 'A1', 'raw': True, 'aggregate': True, 'distinct_keys': True,
              'filter_regions': True, 'filter_min_cents': False, 'max_page_size': 32,
              'aggregate_state': 'count,sum_cents,nonnull_count,local_distinct_count'},
        'B': {'version': 'B1', 'raw': True, 'aggregate': False, 'distinct_keys': False,
              'filter_regions': True, 'filter_min_cents': False, 'max_page_size': 17}},
        'semantics': 'All source occurrences form one bag; customer domains overlap between sources.'}


def answer(request, sources, disabled=False):
    source = request['source']
    cap = catalog()['sources'][source]
    if request.get('version') != cap['version']:
        raise ValueError('snapshot mismatch')
    kind = request['kind']
    if kind not in ('raw', 'aggregate', 'distinct_keys') or not cap.get(kind):
        raise ValueError('unsupported source capability')
    if disabled and kind != 'raw':
        raise ValueError('aggregate capabilities disabled')
    if 'min_cents' in request:
        raise ValueError('source cannot apply amount filter')
    group_by = request.get('group_by', [])
    if group_by not in ([], ['region']):
        raise ValueError('unsupported grouping')
    rows = sources[source]
    if 'regions' in request:
        rows = [r for r in rows if r['region'] in request['regions']]
    if kind == 'raw':
        items = rows
    else:
        groups = defaultdict(list)
        for row in rows:
            groups[row['region'] if group_by else None].append(row)
        if not group_by and not rows:
            groups[None] = []
        items = []
        for key, members in sorted(groups.items(), key=lambda x: canonical(x[0])):
            group = [key] if group_by else []
            values = [r['amount_cents'] for r in members if r['amount_cents'] is not None]
            customers = {r['customer'] for r in members if r['customer'] is not None}
            if kind == 'aggregate':
                items.append({'group': group, 'count': len(members), 'sum_cents': sum(values),
                              'nonnull_count': len(values), 'local_distinct_count': len(customers)})
            else:
                items.extend({'group': group, 'customer': c} for c in sorted(customers))
    identity = hashlib.sha256(canonical({k: v for k, v in request.items()
                                       if k not in ('cursor', 'page_size')}).encode()).hexdigest()
    offset = 0
    if request.get('cursor'):
        token = json.loads(base64.urlsafe_b64decode(request['cursor']))
        if token['identity'] != identity:
            raise ValueError('cursor belongs to another request')
        offset = token['offset']
    size = request.get('page_size', cap['max_page_size'])
    if type(size) is not int or not 1 <= size <= cap['max_page_size']:
        raise ValueError('page size out of range')
    page = items[offset:offset + size]
    complete = offset + len(page) >= len(items)
    token = None if complete else base64.urlsafe_b64encode(
        canonical({'identity': identity, 'offset': offset + len(page)}).encode()).decode()
    return {'source': source, 'version': cap['version'], 'kind': kind, 'items': page,
            'cursor': token, 'complete': complete}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--port', type=int, default=0)
    p.add_argument('--ready-file', required=True)
    p.add_argument('--log-file', required=True)
    p.add_argument('--disable-aggregates', action='store_true')
    args = p.parse_args()
    sources = {s: json.loads((HERE / (s.lower() + '.json')).read_text()) for s in ('A', 'B')}

    class Handler(BaseHTTPRequestHandler):
        def send(self, status, value):
            raw = canonical(value).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return len(raw)

        def do_GET(self):
            value = catalog()
            if args.disable_aggregates:
                value['sources']['A'].update(aggregate=False, distinct_keys=False)
            self.send(200, value)

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            try:
                value = answer(request, sources, args.disable_aggregates)
                status = 200
            except (ValueError, KeyError, TypeError) as e:
                status, value = 400, {'error': str(e)}
            size = self.send(status, value)
            with Path(args.log_file).open('a') as out:
                out.write(canonical({'request': request, 'status': status, 'response_bytes': size,
                                     'items': len(value.get('items', []))}) + '\n')

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', args.port), Handler)
    Path(args.ready_file).write_text(canonical({'url': f'http://127.0.0.1:{server.server_port}'}))
    server.serve_forever()


if __name__ == '__main__':
    main()

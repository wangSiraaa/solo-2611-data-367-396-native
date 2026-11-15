"""Actual shard primitives. No migration coordinator or routing protocol is implemented."""
from pathlib import Path
import sqlite3,json,hashlib
def canonical(payload):return json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def payload_hash(payload):return hashlib.sha256(canonical(payload).encode()).hexdigest()
class Shard:
 def __init__(self,path):
  self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
  with self.connect() as c:
   c.executescript('''
   CREATE TABLE IF NOT EXISTS tenant_state(tenant TEXT PRIMARY KEY,generation INTEGER NOT NULL,writable INTEGER NOT NULL,clock INTEGER NOT NULL);
   CREATE TABLE IF NOT EXISTS records(tenant TEXT NOT NULL,row_id TEXT NOT NULL,value INTEGER,rev INTEGER NOT NULL,deleted INTEGER NOT NULL,PRIMARY KEY(tenant,row_id));
   CREATE TABLE IF NOT EXISTS requests(tenant TEXT NOT NULL,request_id TEXT NOT NULL,content_sha TEXT NOT NULL,receipt TEXT NOT NULL,PRIMARY KEY(tenant,request_id));
   CREATE TABLE IF NOT EXISTS outbox(tenant TEXT NOT NULL,rev INTEGER NOT NULL,event TEXT NOT NULL,PRIMARY KEY(tenant,rev));
   ''')
 def connect(self):
  c=sqlite3.connect(self.path,timeout=2);c.row_factory=sqlite3.Row;c.execute('PRAGMA journal_mode=WAL');return c
 def initialize_tenant(self,tenant,generation,writable):
  with self.connect() as c:c.execute('INSERT INTO tenant_state VALUES (?,?,?,0)',(tenant,generation,int(writable)))
 def write(self,tenant,generation,request_id,payload):
  if set(payload) not in ({'op','row_id','value'},{'op','row_id'}) or payload['op'] not in ('put','delete') or not isinstance(payload['row_id'],str) or not payload['row_id']:raise ValueError('INVALID_REQUEST')
  if payload['op']=='put' and (type(payload.get('value')) is not int):raise ValueError('INVALID_VALUE')
  if payload['op']=='delete' and 'value' in payload:raise ValueError('DELETE_HAS_VALUE')
  digest=payload_hash(payload)
  with self.connect() as c:
   c.execute('BEGIN IMMEDIATE')
   prior=c.execute('SELECT content_sha,receipt FROM requests WHERE tenant=? AND request_id=?',(tenant,request_id)).fetchone()
   if prior:
    if prior['content_sha']!=digest:raise ValueError('REQUEST_ID_CONFLICT')
    return json.loads(prior['receipt'])
   state=c.execute('SELECT * FROM tenant_state WHERE tenant=?',(tenant,)).fetchone()
   if not state or not state['writable'] or state['generation']!=generation:raise ValueError('STALE_OWNER_GENERATION')
   rev=state['clock']+1;deleted=int(payload['op']=='delete');value=None if deleted else payload['value']
   row={'tenant':tenant,'row_id':payload['row_id'],'value':value,'rev':rev,'deleted':deleted}
   receipt={'tenant':tenant,'request_id':request_id,'content_sha':digest,'accepted_generation':generation,'rev':rev,'status':'ACCEPTED'}
   c.execute('UPDATE tenant_state SET clock=? WHERE tenant=?',(rev,tenant))
   c.execute('INSERT INTO records VALUES (?,?,?,?,?) ON CONFLICT(tenant,row_id) DO UPDATE SET value=excluded.value,rev=excluded.rev,deleted=excluded.deleted',(tenant,payload['row_id'],value,rev,deleted))
   c.execute('INSERT INTO requests VALUES (?,?,?,?)',(tenant,request_id,digest,canonical(receipt)))
   event={'row':row,'request':{'tenant':tenant,'request_id':request_id,'content_sha':digest,'receipt':receipt}}
   c.execute('INSERT INTO outbox VALUES (?,?,?)',(tenant,rev,canonical(event)))
   return receipt
 def events_after(self,tenant,rev,limit):
  with self.connect() as c:return [json.loads(x[0]) for x in c.execute('SELECT event FROM outbox WHERE tenant=? AND rev>? ORDER BY rev LIMIT ?',(tenant,rev,limit))]
 def snapshot(self,tenant):
  c=self.connect();c.execute('BEGIN');clock=c.execute('SELECT clock FROM tenant_state WHERE tenant=?',(tenant,)).fetchone()[0]
  return c,clock
 def snapshot_rows(self,connection,tenant,after_id,limit):
  return [dict(x) for x in connection.execute('SELECT * FROM records WHERE tenant=? AND row_id>? ORDER BY row_id LIMIT ?',(tenant,after_id,limit))]
 def _apply_row(self,c,row):
  prior=c.execute('SELECT * FROM records WHERE tenant=? AND row_id=?',(row['tenant'],row['row_id'])).fetchone()
  if prior and prior['rev']==row['rev'] and dict(prior)!=row:raise ValueError('REV_CONTENT_CONFLICT')
  c.execute('INSERT INTO records VALUES (?,?,?,?,?) ON CONFLICT(tenant,row_id) DO UPDATE SET value=excluded.value,rev=excluded.rev,deleted=excluded.deleted WHERE excluded.rev>records.rev',(row['tenant'],row['row_id'],row['value'],row['rev'],row['deleted']))
  c.execute('UPDATE tenant_state SET clock=max(clock,?) WHERE tenant=?',(row['rev'],row['tenant']))
 def _apply_request(self,c,request):
  prior=c.execute('SELECT content_sha,receipt FROM requests WHERE tenant=? AND request_id=?',(request['tenant'],request['request_id'])).fetchone()
  encoded=canonical(request['receipt'])
  if prior and (prior['content_sha']!=request['content_sha'] or prior['receipt']!=encoded):raise ValueError('REQUEST_IMPORT_CONFLICT')
  c.execute('INSERT OR IGNORE INTO requests VALUES (?,?,?,?)',(request['tenant'],request['request_id'],request['content_sha'],encoded))
 def apply_snapshot_row(self,row):
  with self.connect() as c:c.execute('BEGIN IMMEDIATE');self._apply_row(c,row)
 def apply_event(self,event):
  with self.connect() as c:c.execute('BEGIN IMMEDIATE');self._apply_row(c,event['row']);self._apply_request(c,event['request'])
 def receipt_rows(self,tenant,after_id,limit):
  with self.connect() as c:return [{'tenant':tenant,'request_id':r['request_id'],'content_sha':r['content_sha'],'receipt':json.loads(r['receipt'])} for r in c.execute('SELECT * FROM requests WHERE tenant=? AND request_id>? ORDER BY request_id LIMIT ?',(tenant,after_id,limit))]
 def import_receipt(self,request):
  with self.connect() as c:c.execute('BEGIN IMMEDIATE');self._apply_request(c,request)
 def fence(self,tenant,generation):
  with self.connect() as c:
   c.execute('BEGIN IMMEDIATE');s=c.execute('SELECT * FROM tenant_state WHERE tenant=?',(tenant,)).fetchone()
   if not s or s['generation']!=generation:raise ValueError('FENCE_GENERATION_MISMATCH')
   c.execute('UPDATE tenant_state SET writable=0 WHERE tenant=?',(tenant,));return s['clock']
 def activate(self,tenant,generation):
  with self.connect() as c:
   c.execute('BEGIN IMMEDIATE');s=c.execute('SELECT generation FROM tenant_state WHERE tenant=?',(tenant,)).fetchone()
   if not s or generation<s['generation']:raise ValueError('ACTIVATE_GENERATION_MISMATCH')
   c.execute('UPDATE tenant_state SET generation=?,writable=1 WHERE tenant=?',(generation,tenant))
 def read(self,tenant,generation):
  with self.connect() as c:
   c.execute('BEGIN');s=c.execute('SELECT * FROM tenant_state WHERE tenant=?',(tenant,)).fetchone()
   if not s or not s['writable'] or s['generation']!=generation:raise ValueError('STALE_OWNER_GENERATION')
   return [dict(x) for x in c.execute('SELECT * FROM records WHERE tenant=? AND deleted=0 ORDER BY row_id',(tenant,))]

"""Low-level real SQLite write/outbox and inbox facilities; no causal replication engine."""
import sqlite3,json,hashlib,argparse
from pathlib import Path
SEED=[{'id':'r1','name':'base','qty':10},{'id':'r2','name':'spare','qty':5}]
def canonical(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)
def digest(value):return hashlib.sha256(canonical(value).encode()).hexdigest()
def connect(path):return sqlite3.connect(path,timeout=5)
def init(path,replica):
 if replica not in ('A','B'):raise ValueError('replica must be A or B')
 c=connect(path)
 c.executescript('''
 CREATE TABLE meta(replica TEXT PRIMARY KEY,seq INTEGER NOT NULL,dataset TEXT NOT NULL);
 CREATE TABLE items(id TEXT PRIMARY KEY,name TEXT NOT NULL,qty INTEGER NOT NULL CHECK(qty>=0));
 CREATE TABLE seed_versions(version_id TEXT PRIMARY KEY,row_id TEXT NOT NULL,state_json TEXT NOT NULL);
 CREATE TABLE source_outbox(version_id TEXT PRIMARY KEY,event_json TEXT NOT NULL,event_sha256 TEXT NOT NULL);
 CREATE TABLE received(version_id TEXT PRIMARY KEY,event_json TEXT NOT NULL,event_sha256 TEXT NOT NULL);
 ''')
 c.execute('INSERT INTO meta VALUES(?,?,?)',(replica,0,digest(SEED)))
 for i,row in enumerate(SEED,1):
  c.execute('INSERT INTO items VALUES(:id,:name,:qty)',row)
  c.execute('INSERT INTO seed_versions VALUES(?,?,?)',('seed:'+str(i),row['id'],canonical({'name':row['name'],'qty':row['qty']})))
 c.commit();c.close()
def set_business(c,row_id,state):
 if state is None:c.execute('DELETE FROM items WHERE id=?',(row_id,))
 else:c.execute('INSERT INTO items VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,qty=excluded.qty',(row_id,state['name'],state['qty']))
def local_write(path,row_id,state,parents,kind='write',fail_before_outbox=False):
 c=connect(path)
 try:
  c.execute('BEGIN IMMEDIATE');replica,seq,dataset=c.execute('SELECT replica,seq,dataset FROM meta').fetchone();seq+=1
  e={'dataset':dataset,'version_id':replica+':'+str(seq),'origin':replica,'seq':seq,'row_id':row_id,'parents':sorted(set(parents)),'kind':kind,'state':state}
  c.execute('UPDATE meta SET seq=?',(seq,));set_business(c,row_id,state)
  if fail_before_outbox:raise RuntimeError('probe: interrupted before outbox')
  raw=canonical(e);c.execute('INSERT INTO source_outbox VALUES(?,?,?)',(e['version_id'],raw,hashlib.sha256(raw.encode()).hexdigest()));c.commit();return e
 except:c.rollback();raise
 finally:c.close()
def receive_only(path,event):
 c=connect(path)
 try:
  dataset=c.execute('SELECT dataset FROM meta').fetchone()[0]
  if event['dataset']!=dataset:raise ValueError('dataset mismatch')
  raw=canonical(event);h=hashlib.sha256(raw.encode()).hexdigest();eid=event['version_id']
  prior=c.execute('SELECT event_sha256 FROM received WHERE version_id=?',(eid,)).fetchone()
  if prior:
   if prior[0]!=h:raise ValueError('identity content conflict')
   return False
  own=c.execute('SELECT event_sha256 FROM source_outbox WHERE version_id=?',(eid,)).fetchone()
  if own:
   if own[0]!=h:raise ValueError('identity content conflict')
   return False
  c.execute('INSERT INTO received VALUES(?,?,?)',(eid,raw,h));c.commit();return True
 finally:c.close()
def export_local(path):
 c=connect(path)
 try:return [json.loads(x[0]) for x in c.execute('SELECT event_json FROM source_outbox ORDER BY version_id')]
 finally:c.close()
if __name__=='__main__':
 p=argparse.ArgumentParser();sub=p.add_subparsers(dest='cmd',required=True)
 a=sub.add_parser('init');a.add_argument('db');a.add_argument('replica')
 a=sub.add_parser('write');a.add_argument('db');a.add_argument('request')
 a=sub.add_parser('inbox');a.add_argument('db');a.add_argument('event')
 a=sub.add_parser('export');a.add_argument('db')
 a=p.parse_args()
 if a.cmd=='init':init(a.db,a.replica)
 elif a.cmd=='write':
  d=json.loads(Path(a.request).read_text());print(canonical(local_write(a.db,d['row_id'],d['state'],d['parents'],d.get('kind','write'))))
 elif a.cmd=='inbox':print(canonical({'inserted':receive_only(a.db,json.loads(Path(a.event).read_text()))}))
 else:print(canonical(export_local(a.db)))

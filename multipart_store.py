import json,hashlib,fcntl,functools
from pathlib import Path
class Store:
 def __init__(self,root):self.root=Path(root);self.root.mkdir(parents=True,exist_ok=True);self.state=self.root/'state.json';self.s=json.loads(self.state.read_text()) if self.state.exists() else {'uploads':{},'objects':{},'receipts':{},'next':1}
 def _save(self):
  p=self.root/'tmp.json';p.write_text(json.dumps(self.s));p.replace(self.state)
 def begin(self,key,delivery,digest,size,expected=0):
  uid='u'+str(self.s['next']);self.s['next']+=1;self.s['uploads'][uid]={'key':key,'delivery':delivery,'digest':digest,'size':size,'expected':expected,'parts':{}};self._save();return uid
 def part(self,uid,no,data):
  e=hashlib.sha256(data).hexdigest();self.s['uploads'][uid]['parts'][str(no)]={'etag':e,'data':data.hex(),'size':len(data)};self._save();return e
 def list_parts(self,uid):return {k:{x:v[x] for x in ['etag','size']} for k,v in self.s['uploads'][uid]['parts'].items()}
 def complete(self,uid,manifest,fail=None):
  if fail=='before':raise OSError('BEFORE_COMMIT')
  u=self.s['uploads'][uid];old=self.s['receipts'].get(u['delivery'])
  if old:
   if old['digest']!=u['digest'] or old['key']!=u['key']:raise ValueError('DELIVERY_CONTENT_CONFLICT')
   return old
  current=self.s['objects'].get(u['key'],[]);generation=len(current)
  if generation!=u['expected']:raise ValueError('GENERATION_CONFLICT')
  if [x['no'] for x in manifest]!=list(range(1,len(manifest)+1)):raise ValueError('PART_SEQUENCE')
  data=b''
  for x in manifest:
   part=u['parts'][str(x['no'])]
   if part['etag']!=x['etag']:raise ValueError('PART_ETAG')
   data+=bytes.fromhex(part['data'])
  if len(data)!=u['size'] or hashlib.sha256(data).hexdigest()!=u['digest']:raise ValueError('OBJECT_IDENTITY')
  version=u['key']+'@'+str(generation+1);r={'version':version,'key':u['key'],'delivery':u['delivery'],'digest':u['digest'],'size':len(data)}
  self.s['objects'].setdefault(u['key'],[]).append({**r,'data':data.hex()});self.s['receipts'][u['delivery']]=r;self._save()
  if fail=='after':raise OSError('RESPONSE_LOST')
  return r
 def list_uploads(self):return [{k:u[k] for k in ['key','delivery','digest','size']}|{'upload_id':uid} for uid,u in self.s['uploads'].items()]
 def abort(self,uid,delivery):
  if self.s['uploads'][uid]['delivery']!=delivery:raise ValueError('UPLOAD_OWNER_MISMATCH')
  del self.s['uploads'][uid];self._save()
 def receipt(self,delivery):return self.s['receipts'].get(delivery)
 def head(self,key):
  a=self.s['objects'].get(key,[]);return {k:v for k,v in a[-1].items() if k!='data'} if a else None
 def read(self,version):
  for a in self.s['objects'].values():
   for obj in a:
    if obj['version']==version:return bytes.fromhex(obj['data'])
  raise KeyError(version)

# One durable service state, shared safely by independent uploader processes.
def _serialized(method):
 @functools.wraps(method)
 def call(self,*args,**kwargs):
  with (self.root/'service.lock').open('a+b') as lock:
   fcntl.flock(lock,fcntl.LOCK_EX)
   self.s=json.loads(self.state.read_text()) if self.state.exists() else {'uploads':{},'objects':{},'receipts':{},'next':1}
   try:return method(self,*args,**kwargs)
   finally:fcntl.flock(lock,fcntl.LOCK_UN)
 return call
for _name in ('begin','part','list_parts','complete','list_uploads','abort','receipt','head','read'):
 setattr(Store,_name,_serialized(getattr(Store,_name)))

"""Local API server fixture. Client code uses GraphAPI methods, not its private file."""
import json,hashlib
from pathlib import Path
CAPABILITIES={
 'owns':{'directions':['out'],'target_filter':True},
 'uses':{'directions':['out','in'],'target_filter':False},
 'located':{'directions':['out','in'],'target_filter':False}}
class GraphAPI:
 def __init__(self,private_file):
  raw=Path(private_file).read_bytes();self._rows=json.loads(raw);self._snapshot=hashlib.sha256(raw).hexdigest();self.audit=[]
 def capabilities(self):return {'snapshot_id':self._snapshot,'labels':CAPABILITIES,'max_keys':4,'page_size':25}
 def query_edges(self,request,token=None):
  if request['snapshot_id']!=self._snapshot:raise ValueError('SNAPSHOT_MISMATCH')
  label=request['label'];direction=request['direction'];keys=request['keys']
  if label not in CAPABILITIES or direction not in CAPABILITIES[label]['directions']:raise ValueError('UNSUPPORTED_DIRECTION')
  if not isinstance(keys,list) or not 1<=len(keys)<=4 or len(set(keys))!=len(keys):raise ValueError('BAD_KEYS')
  targets=request.get('target_keys')
  if targets is not None and (direction!='out' or not CAPABILITIES[label]['target_filter'] or len(targets)>4):raise ValueError('UNSUPPORTED_TARGET_FILTER')
  encoded=json.dumps(request,sort_keys=True,separators=(',',':')).encode();query_hash=hashlib.sha256(encoded).hexdigest()
  offset=0
  if token is not None:
   if token['snapshot_id']!=self._snapshot or token['query_hash']!=query_hash:raise ValueError('TOKEN_QUERY_MISMATCH')
   offset=token['offset']
  field='src' if direction=='out' else 'dst'
  rows=sorted((r for r in self._rows if r['label']==label and r[field] in keys and (targets is None or r['dst'] in targets)),key=lambda r:r['edge_id'])
  page=rows[offset:offset+25]
  next_token={'snapshot_id':self._snapshot,'query_hash':query_hash,'offset':offset+25} if offset+25<len(rows) else None
  response={'snapshot_id':self._snapshot,'edges':page,'total_matches':len(rows),'next_token':next_token}
  self.audit.append({'request':request,'token':token,'returned_edges':len(page),'request_bytes':len(encoded),'edge_payload_bytes':len(json.dumps(page,sort_keys=True,separators=(',',':')).encode()),'has_more':next_token is not None})
  return response

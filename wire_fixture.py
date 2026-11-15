import argparse,json,sys,hashlib
p=argparse.ArgumentParser();p.add_argument('--states',required=True);p.add_argument('--chunk-id',required=True);p.add_argument('--profile',default='binary64-exact-sum/v1');p.add_argument('--repeat-first',action='store_true');a=p.parse_args()
def emit(x):print(json.dumps(x,separators=(',',':')),flush=True)
emit({'kind':'HELLO','chunk_id':a.chunk_id,'semantic_profile':a.profile,'codecs':['fraction-pair/v1'],'max_frame_bytes':8192})
reply=json.loads(sys.stdin.readline())
if reply.get('kind')!='ACCEPT':emit({'kind':'REJECTED'});raise SystemExit(0)
h=hashlib.sha256();n=0
with open(a.states) as source:
 for line in source:
  state=json.loads(line);frame={'kind':'DATA','chunk_id':a.chunk_id,'seq':n,'state':state};raw=json.dumps(state,sort_keys=True,separators=(',',':')).encode();h.update(raw)
  for _ in range(2 if n==0 and a.repeat_first else 1):
   emit(frame);ack=json.loads(sys.stdin.readline());assert ack=={'kind':'ACK','seq':n}
  n+=1
emit({'kind':'TRAILER','chunk_id':a.chunk_id,'frames':n,'payload_sha256':h.hexdigest()})

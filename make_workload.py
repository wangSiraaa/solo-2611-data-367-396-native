from pathlib import Path
import argparse,json,struct,hashlib
p=argparse.ArgumentParser();p.add_argument('out');p.add_argument('--groups',type=int,default=4096);a=p.parse_args();root=Path(a.out);root.mkdir(parents=True,exist_ok=True)
maximum=struct.pack('>d',float.fromhex('0x1.fffffffffffffp+1023')).hex();tiny='0000000000000001';negative=struct.pack('>d',-float.fromhex('0x1.fffffffffffffp+1023')).hex();specs=[('p0',[maximum,tiny]),('p1',[negative])];chunks=[];rid=0
for name,values in specs:
 start=rid;path=root/(name+'.jsonl')
 with path.open('w') as f:
  for bits in values:
   for g in range(a.groups):f.write(json.dumps({'record_id':rid,'key':'g'+str(g).zfill(4),'bits':bits},separators=(',',':'))+'\n');rid+=1
 chunks.append({'chunk_id':name,'path':path.name,'range':[start,rid],'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
(root/'manifest.json').write_text(json.dumps({'dataset':'resource-demo','semantic_profile':'binary64-exact-sum/v1','groups':a.groups,'chunks':chunks},indent=2))
print(json.dumps({'records':rid,'groups':a.groups,'chunks':chunks}))

import json,sys
from pathlib import Path
p=Path(sys.argv[1]);p.mkdir(parents=True,exist_ok=True)
for side in ['left','right']:
 (p/(side+'.jsonl')).write_text(''.join(json.dumps({'row_id':side+str(i),'key':'k%04d'%((i*137)%1000),'value':i})+'\n' for i in range(1000)))

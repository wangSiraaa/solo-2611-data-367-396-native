from pathlib import Path
import json,sys
p=Path(sys.argv[1]);p.mkdir(parents=True,exist_ok=True)
for source in ('a','b'):
 with (p/(source+'.jsonl')).open('w') as f:
  for i in range(12):f.write(json.dumps({'source_id':source,'seq':i,'key':0 if i<9 else i%2,'value':i-(3 if source=='b' else 0),'fanout':i%4})+'\n')
print(json.dumps({'source_rows':24,'output_rows':36,'source_ids':['a','b']}))

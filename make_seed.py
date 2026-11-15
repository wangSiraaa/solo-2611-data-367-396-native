from pathlib import Path
import json,base64,sys
p=Path(sys.argv[1]);p.mkdir(parents=True,exist_ok=True)
(p/'rows.jsonl').write_text(''.join(json.dumps({'id':i,'payload':'sample-'+str(i)})+'\n' for i in range(12)))
(p/'keys.json').write_text(json.dumps({k:base64.b64encode(bytes([v])*32).decode() for k,v in [('k1',1),('k2',2),('k3',3)]}))
print(json.dumps({'rows':12,'test_keys':3,'synthetic':True}))

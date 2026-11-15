from pathlib import Path
import json,sys
p,offset,limit=map(int,sys.argv[1:4]);rows=[json.loads(x) for x in (Path(__file__).parent/'fixtures/log.jsonl').read_text().splitlines()];data=[x for x in rows if x['partition']==p and x['offset']>=offset][:limit]
print(json.dumps({'partition':p,'rows':data,'source_end_offset':5,'immutable':'demo-v1'}))

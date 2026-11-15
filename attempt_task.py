"""A real deterministic partition task; this file does not elect a winner."""
import argparse,csv,hashlib,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--input',required=True);p.add_argument('--partition',required=True);p.add_argument('--attempt',required=True);p.add_argument('--out',required=True);a=p.parse_args()
rows=[r for r in json.loads(Path(a.input).read_text()) if r['partition']==a.partition]
out=Path(a.out);out.mkdir(parents=True,exist_ok=False)
with (out/'data.csv').open('w',newline='') as f:
 w=csv.writer(f);w.writerow(['id','value','attempt']);w.writerows((r['id'],r['value']*2,a.attempt) for r in rows)
(out/'stats.json').write_text(json.dumps({'partition':a.partition,'attempt':a.attempt,'count':len(rows),'sum':sum(r['value']*2 for r in rows)},sort_keys=True))
files={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(out.iterdir())}
(out/'prepared.json').write_text(json.dumps({'partition':a.partition,'attempt':a.attempt,'files':files},sort_keys=True))
print(json.dumps({'partition':a.partition,'attempt':a.attempt,'files':files}))

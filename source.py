from pathlib import Path
import sys
if sys.argv[1] not in ('p0','p1','control'):raise SystemExit('unknown source')
print((Path(__file__).parent/'fixtures'/(sys.argv[1]+'.jsonl')).read_text(),end='')

if '--end' in sys.argv[2:]:
 import json
 print(json.dumps({'kind':'end','source':sys.argv[1]}))

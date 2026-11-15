import json
from pathlib import Path
class SortBuffer:
    def __init__(self,work,qid):self.work=Path(work)/qid;self.work.mkdir(parents=True,exist_ok=True);self.rows=[];self.runs=[]
    def consume(self,rows):self.rows.extend(rows)
    def spill(self,on_pinned=lambda:None,on_written=lambda:None):
        on_pinned();path=self.work/('run-%d.jsonl'%len(self.runs));path.write_text(''.join(json.dumps(r)+'\n' for r in sorted(self.rows,key=lambda r:(r['key'],r['row_id']))));on_written();n=len(self.rows);self.rows.clear();self.runs.append(path);return n
    def materialized_rows(self):
        for path in self.runs:
            for line in path.read_text().splitlines():yield json.loads(line)
        yield from self.rows

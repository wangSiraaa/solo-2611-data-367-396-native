import sqlite3,json
from pathlib import Path

def oracle(rows):
 c=sqlite3.connect(':memory:');c.executescript(Path(__file__).with_name('schema.sql').read_text());c.executemany('INSERT INTO input_rows VALUES(:row_id,:part,:ord,:value)',rows)
 sql='''
 SELECT row_id, SUM(value) OVER(PARTITION BY part ORDER BY ord ASC NULLS LAST GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
 SUM(value) OVER(PARTITION BY part ORDER BY ord ASC NULLS LAST,row_id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING),
 DENSE_RANK() OVER(PARTITION BY part ORDER BY ord ASC NULLS LAST)
 FROM input_rows ORDER BY row_id
 '''
 return [list(r) for r in c.execute(sql)]
if __name__=='__main__':print(json.dumps(oracle(json.loads(Path('rows.json').read_text()))))

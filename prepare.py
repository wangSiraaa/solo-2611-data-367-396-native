import sqlite3
from pathlib import Path
p=Path(__file__).resolve().parent;c=sqlite3.connect(p/'snapshot.sqlite');c.execute('drop table if exists observations');c.execute('create table observations(row_id integer primary key,group_id text,value integer)');c.executemany('insert into observations values (?,?,?)',[(i,'g'+str(i%3),i*7) for i in range(30)]);c.commit()

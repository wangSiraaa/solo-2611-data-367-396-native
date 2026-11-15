import sqlite3
from pathlib import Path
p=Path(__file__).resolve().parent;c=sqlite3.connect(p/'legacy.sqlite');c.executescript('drop table if exists messages;drop table if exists contacts;create table contacts(contact_id text primary key,tenant text not null,email text,note text,revision integer not null);create table messages(message_id text primary key,contact_id text not null references contacts(contact_id),body text);')
c.executemany('insert into contacts values (?,?,?,?,?)',[('c1','t1','Straße@example.com','keep',1),('c2','t1',' STRASSE@example.com ','old conflict',1),('c3','t2','STRASSE@example.com','other tenant',1),('c4','t1',None,'null allowed',1),('c5','t1','','empty unique key',1)])
c.executemany('insert into messages values (?,?,?)',[('m1','c1','one'),('m2','c2','two')]);c.commit()

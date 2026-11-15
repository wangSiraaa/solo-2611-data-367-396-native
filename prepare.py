import sqlite3,json,hashlib
from pathlib import Path
p=Path(__file__).resolve().parent;c=sqlite3.connect(p/'source.sqlite')
c.executescript('drop table if exists invoices;drop table if exists accounts;drop table if exists customers;create table customers(customer_id text primary key,email text unique);create table accounts(account_id text primary key,owner_id text not null references customers(customer_id));create table invoices(invoice_id text primary key,account_ref text references accounts(account_id),bill_to_email text references customers(email));')
seen={};collision=None
for i in range(1000):
 s='SRC_EMAIL_'+str(i);h=hashlib.sha256(s.encode()).hexdigest()[:2]
 if h in seen:collision=(seen[h],s,h);break
 seen[h]=s
for i,e in enumerate([collision[0],collision[1],'']):c.execute('insert into customers values (?,?)',('SRC_C'+str(i),e));c.execute('insert into accounts values (?,?)',('SRC_A'+str(i),'SRC_C'+str(i)));c.execute('insert into invoices values (?,?,?)',('I'+str(i),'SRC_A'+str(i),e))
c.commit();c.close();(p/'collision.json').write_text(json.dumps({'values':collision[:2],'sha256_prefix':collision[2]}))

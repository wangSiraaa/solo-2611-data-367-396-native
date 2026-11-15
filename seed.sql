CREATE TABLE duty(id integer PRIMARY KEY, on_call boolean NOT NULL);
INSERT INTO duty VALUES(1,true),(2,true);
CREATE TABLE balances(id integer PRIMARY KEY, amount integer NOT NULL);
INSERT INTO balances VALUES(1,100),(2,100);

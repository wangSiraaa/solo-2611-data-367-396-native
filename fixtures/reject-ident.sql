-- 违例：引号标识符与 schema 前缀
PRAGMA foreign_keys=ON;
CREATE TABLE main.bad("weird name" INTEGER);

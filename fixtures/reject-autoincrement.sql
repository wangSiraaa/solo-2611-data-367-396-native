-- 每个文件一条子集外结构，用于验证造库前拒绝；命名说明违例点
-- 本文件：AUTOINCREMENT
PRAGMA foreign_keys=ON;
CREATE TABLE bad(a INTEGER PRIMARY KEY AUTOINCREMENT, b INTEGER);

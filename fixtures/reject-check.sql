-- 违例：CHECK 含 OR / 函数 / 算术
PRAGMA foreign_keys=ON;
CREATE TABLE bad(a INTEGER CHECK(a = 1 OR a = 2), b INTEGER CHECK(abs(b) > 0), c INTEGER CHECK(c+1 < 3));

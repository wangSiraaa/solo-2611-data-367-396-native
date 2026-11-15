-- 列间 <= 约束；声明给固定违反值用于无解验证
PRAGMA foreign_keys=ON;
CREATE TABLE boxes(
  bid INTEGER PRIMARY KEY,
  low INTEGER NOT NULL,
  high INTEGER NOT NULL,
  CHECK(low <= high)
);

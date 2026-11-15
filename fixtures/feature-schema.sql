-- 覆盖 schema-subset.md 全部 CHECK 形态：BETWEEN(含负数) / IN / <=常量 / >=常量 / 两列 <=
-- 自引用外键环；单列 UNIQUE 中 NULL 互不相同语义
PRAGMA foreign_keys=ON;
CREATE TABLE nodes(
  node_id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  lo INTEGER NOT NULL CHECK(lo BETWEEN -10 AND 0),
  hi INTEGER CHECK(hi >= 1),
  cap INTEGER CHECK(cap <= 99),
  code INTEGER CHECK(code IN (7, 8, 9)),
  span_a INTEGER,
  span_b INTEGER,
  UNIQUE(span_a, span_b),
  CHECK(span_a <= span_b),
  FOREIGN KEY(parent_id) REFERENCES nodes(node_id) DEFERRABLE INITIALLY DEFERRED
);

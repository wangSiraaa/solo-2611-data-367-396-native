# 支持子集的精确定义
表和列名为ASCII标识符[a-zA-Z_][a-zA-Z0-9_]*，不带引号；无schema前缀。列类型仅INTEGER；列可声明NOT NULL，单列INTEGER PRIMARY KEY或表级PRIMARY KEY/UNIQUE(col[,col])。不支持AUTOINCREMENT、生成列、默认表达式和索引表达式。
外键为FOREIGN KEY(child[,child]) REFERENCES parent(parentcol[,parentcol]) DEFERRABLE INITIALLY DEFERRED；一到两列，匹配真实父PRIMARY KEY/UNIQUE，默认MATCH SIMPLE和NO ACTION，无自定义动作。允许自引用和跨表环。
CHECK表达式只支持一个原子或由AND连接的原子，原子为 col BETWEEN 整数 AND 整数、col IN(整数[,整数]*)、col <= 整数、col >= 整数、col <= col。整数可带负号；无函数、OR、算术、子查询。SQLite CHECK的NULL结果不拒绝该行，NOT NULL另行检查。
生成声明固定行槽，各one_of域是无重复整数/NULL，固定值同类型。总待分配单元格最多12，每域最多4值。无解证据可以包含原问题的全部行槽/域/约束：verify按有限笛卡尔积枚举此子问题，确认没有满足赋值；可以更小，不要求最小核。证明对象不能删掉仍被约束引用的固定键/行槽，选择的约束须附原schema位置及原文字节。

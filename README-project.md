# synthdb —— 小型关系 schema 的合成测试库装配器

根据真实 `schema.sql`（只读）与 `generation.json` 的固定行槽/有限域声明，在**独立新库**
装配可直接联调的 SQLite 测试数据。提供 `plan` / `generate` / `verify` 三个阶段。

## 核心保证

- **不删行、不复制、不增槽**：每表固定行槽数与固定键保留；固定值不可改，待分配值只能取各自 `one_of` 域。
- **严格子集白名单**：仅接受 `schema-subset.md` 的 SQL 形态；子集外约束（AUTOINCREMENT、DEFAULT、
  生成列、内联 REFERENCES、自定义外键动作、OR/函数/算术 CHECK、非 INTEGER、引号标识符、VIEW/INDEX…）
  在造库前定位拒绝（带行号+原文字节），绝不静默忽略。
- **真实 SQLite 语义**：UNIQUE/PK 的 NULL 互不相同；单列 INTEGER PK 隐式 NOT NULL；
  外键 MATCH SIMPLE（部分 NULL 豁免，否则整元组命中同一父行，复合 FK 不许分量各找各的）；
  CHECK 遇 NULL 不拒绝。
- **引用环共同分配**：外键图 Tarjan SCC 识别环（如 teams↔people），整体 CSP 求解，
  在 `foreign_keys=ON` 的单延迟事务装载，COMMIT 由 SQLite 终裁；绝不关外键装脏库。
- **无解可独立核对**：回溯求解 + 独立笛卡尔积枚举两条路径互证，不靠"尝试耗尽"判无解；
  冲突子问题给出行槽、有限域、真实约束原文与枚举零解证据。
- **确定性**：表/槽/变量/域全部规范化排序，改变表名、行槽顺序、域枚举次序不改变规范逻辑行。
- **verify 独立复核**：重开成品库，逐槽核对固定值/域/精确行数/类型，执行 PRAGMA 与独立业务 SQL
  （NOT NULL、CHECK 重算、GROUP BY 查重、整元组 NOT EXISTS 查孤儿），不止看 PRAGMA 无错。

## 安装与运行

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 1) 只看装配计划（约束来源、引用环、装载边界）
python -m synthdb plan schema.sql generation.json

# 2) 求解 + 装载新库 + 独立验证（产物写入 out/sat/）
python -m synthdb generate schema.sql generation.json out/sat

# 3) 对成品库重新打开独立复核
python -m synthdb verify out/sat/synthetic.db schema.sql generation.json

# 测试
python -m unittest discover -s tests -v
```

## 产物

有解（`out/sat/`）：

| 文件 | 内容 |
| --- | --- |
| `synthetic.db` | 独立成品 SQLite 库（外键真实开启） |
| `plan.json` | 约束来源、SCC 引用环、装载检查边界 |
| `assignments.jsonl` | 逐行槽赋值 + 来源（fixed / one_of、vid、声明域、规范域、取值） |
| `verify-report.json` | 上百项独立复核结果 |
| `manifest.json` | 输入哈希、schema 摘要、装载边界、行数、求解统计 |

无解（退出码 4，`out/.../`）：`unsat-conflict.json`（可独立重放的冲突子问题）+ `manifest.json`，
**不产生任何数据库文件**。

## 目录

```
synthdb/        parser(子集白名单) catalog(真实PRAGMA目录) declaration csp
                plan(Tarjan SCC) solver(FC+MRV回溯 / 独立枚举) loader verifier pipeline cli
tests/          32 项 unittest：手工小例、真实 SQLite 拒绝、独立枚举、复合FK整元组陷阱、
                NULL 语义、子集外拒绝、改名/换序/域序不变性、verify 防篡改、确定性、行号
fixtures/       覆盖全部 CHECK 形态、自引用、复合 FK、MATCH SIMPLE NULL、改名同构、
                列间 <=、六类子集外违例
DESIGN.md       求解/装载选择与替代方案（拓扑分阶段）在冲突定位与工作量上的取舍
```

退出码：`0` 成功；`3` 子集违例；`4` 无解；`5` 真实库拒绝（不发布）；`6` 独立验证失败。

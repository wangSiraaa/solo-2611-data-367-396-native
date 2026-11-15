# 合成测试库装配器 —— 设计说明

## 1. 目标与边界

读取**只读**的真实 `schema.sql` 与 `generation.json`，在**独立新库**中装配合成数据。
不从旧业务库取子集、不随机生成后删行：每张表的行槽数与固定键严格保留，行槽不丢弃、
不复制、不增加。约束规模固定为：≤4 表、每表 ≤6 行、≤12 个待分配单元格、每域 ≤4 值、
复合键 ≤2 列，列均为 INTEGER，外键均可延期，无触发器/任意表达式。

## 2. 模块与数据流

```
schema.sql ──► parser.py (sqlglot AST + 原文字节切分 + 子集白名单校验)
                 │            └─ 子集外结构：SubsetViolation，造库前拒绝
                 ▼
             catalog.py (一次性内存库执行 DDL，PRAGMA 读权威目录，与解析模型交叉核对)
generation.json ─► declaration.py (固定/one_of 校验、NULL 是否允许由目录决定、≤12/≤4 上限)
                 ▼
             csp.py (编译有限域 CSP：变量=one_of 单元格，固定值=常量)
                 ▼
        ┌────────┴─────────┐
   solver.py DFS+FC     solver.py 独立笛卡尔积枚举（互证，不靠"尝试耗尽"判无解）
        └────────┬─────────┘
                 ▼ 规范赋值
   loader.py（新库、foreign_keys=ON、单延迟事务、foreign_key_check、COMMIT 裁决）
                 ▼
   verifier.py（重开成品库：PRAGMA + 逐槽固定值/域/行数/类型 + 独立业务 SQL 查询）
                 ▼
   pipeline.py / cli.py（plan|generate|verify + manifest + assignments.jsonl）
```

### 2.1 plan（`plan.py`）
- **约束来源**：每张表的 NOT NULL / CHECK / PK / 复合 UNIQUE / 复合 FK，附 schema 行号与原文字节；
- **必须共同处理的引用关系**：外键图上跑 Tarjan 强连通分量（SCC）。大小>1 或自引用的分量
  即引用环（主例中 `teams ↔ people` 同环），环内表无法用逐表立即检查装载，必须共同分配；
- **装载检查边界**：新库、`PRAGMA foreign_keys=ON`、单事务、FK 全部 `DEFERRABLE INITIALLY DEFERRED`、
  COMMIT 前 `foreign_key_check`、COMMIT 由 SQLite 终裁；失败回滚并删除半成品。

## 3. 严格子集与"造库前拒绝"

`parser.py` 用 sqlglot 做语法/AST 校验，同时用自带顶层括号切分器保留每条约束的**原文字节**。
解析是**白名单**式的——只接受 `schema-subset.md` 列出的形态，任何其他结构立即抛
`SubsetViolation`（带行号+原文），包括：AUTOINCREMENT、生成列、DEFAULT、内联 REFERENCES、
非 INTEGER 类型、引号/schema 前缀标识符、非延迟或带 MATCH/ON 动作的外键、CHECK 中的
OR/函数/算术/其他比较符、VIEW/INDEX/TEMP/IF NOT EXISTS 等。绝不静默忽略。

`catalog.py` 再把同一 DDL 交给真实 SQLite 执行，用 `PRAGMA table_info/index_list/
index_info/foreign_key_list` 取权威目录，与解析模型逐项核对（列、NOT NULL、PK、UNIQUE、FK），
防止解析器与真实数据库语义漂移。

## 4. CSP 与 SQLite 语义对齐

变量 = `one_of` 单元格；固定单元格是常量，参与全部约束但不可改。约束三类：
1. **一元**：单列 CHECK（BETWEEN/IN/<=常量/>=常量），先做域过滤；
2. **同行二元**：`col <= col`，固定/变量任意组合，全常量情形在构建期静态求值；
3. **全局**：PK/复合 UNIQUE、复合外键。

关键语义严格按真实 SQLite 实现：
- **UNIQUE/PK 的 NULL**：含 NULL 的键互不冲突（`UNIQUE(a,b)` 中两行 `(1,NULL)` 不冲突）；
  单列 `INTEGER PRIMARY KEY` 隐式 NOT NULL，单独按严格非空处理；
- **外键 MATCH SIMPLE**：子元组任一列为 NULL 即豁免；否则**整个元组**必须命中**同一个父行**
  的父键元组——复合 FK 不允许两个分量分别匹配不同父行（见 `fixtures/composite-unsat.json`
  的 (1,2) 陷阱：分量 1、2 都存在，但不存在父行 (1,2)，判无解）；
- **CHECK 的 NULL**：CHECK 表达式结果为 NULL 不拒绝该行，NOT NULL 另行检查；
- 部分赋值下全局求值器返回**三态**（满足/违反/未定），前向检查只在明确违反时剪枝，
  避免把未赋值变量误当常量而误杀可行解（开发期该 bug 正是被"DFS vs 独立枚举"双路径分歧暴露的）。

## 5. 求解与装载组织的选择

### 5.1 采用的方案：整体解值（FC + MRV 回溯）+ 单延迟事务装载
- 所有变量一次性进入同一 CSP，天然处理环、同行多 FK 互相制约、复合 UNIQUE 与有限域耦合；
- 前向检查（FC）在每次赋值后裁剪其余变量域，MRV（最小剩余域）选变量，配合规范平局键
  （表名→规范槽号→列名）保证**确定性**；
- 域统一规范化为"整数升序、NULL 置尾"，变量/槽/表全部按规范键排序，因此**声明枚举顺序、
  行槽顺序、表名出现顺序都不影响规范结果**；seed 只记录在 manifest，不参与选值
  （题目不要求统计均匀/最大随机性）；
- 装载在一笔 `DEFERRABLE INITIALLY DEFERRED` 事务中完成，循环引用可在事务内暂挂，
  COMMIT 时由 SQLite 统一裁决，全程 `foreign_keys=ON`，不存在"关外键装脏库"。

### 5.2 另一种可行方案及其取舍
替代方案：**约束分解 + 拓扑排序分阶段装载**（按外键 DAG 求合法插入顺序，逐表立即检查）。
- 优点：无环时不必整体求解，单表约束可独立处理，中间状态更小、增量装载更直观；
- 缺点（本族不采用的原因）：
  1. 主例存在 `teams ↔ people` 引用环与自引用，纯拓扑排序**必然失败**，仍需退回延迟事务，
     收益被环抵消；
  2. 复合 UNIQUE + 有限域是跨槽全局约束，分解到单表后仍需联合回溯，分解并不降低问题耦合；
  3. 无解时，分阶段方案只能报"某阶段某表插不进去"，**冲突定位停留在装载期**；
     整体 CSP 可在装载前做删除过滤得到不可约约束子集，并对相关变量做笛卡尔积枚举给出
     可独立重放的冲突子问题（行槽+域+真实约束原文），定位质量明显更好。
- 因此本族选整体解值；拓扑分阶段更适合"无环、表多、每表约束局部"的更大规模场景，
  在本 ≤12 变量规模下其工作量优势不成立。

## 6. 无解判定与冲突子问题（绝不把"尝试耗尽"说成无解）

两条**代码独立**的判定路径互证：
- `solve()`：FC+MRV 回溯，找不到解时只记录最深失败点，不直接下无解结论；
- `enumerate_satisfiable()`：对变量规范域做朴素笛卡尔积递归，逐完整赋值跑全部约束。
  仅当独立枚举确认零解，才判无解。

冲突报告（`unsat-conflict.json`）：
- 对描述性约束做**删除过滤**——逐个尝试移除约束，移除后独立枚举仍无解则永久移除，
  得到相对该顺序不可约的约束子集（不声称最小核）；
- 列出相关行槽（变量给规范域、固定槽给单值域，含原始槽号）、有限域、约束原文与行号、
  枚举空间大小与满足赋值数（=0），核对者无需回看其他文件即可重放；
- 全固定无解（如 `generation-unsat.json` 两个相同的 `(team_id,rank)`）在 CSP 构建期
  静态求值即定位，报告注明"无待分配变量，按固定槽与真实约束直接核对"。

## 7. 装载失败不发布

`loader.py` 任何一步被真实 SQLite 拒绝：ROLLBACK → 关闭连接 → 删除半成品文件 →
把错误映射回候选表/约束（`LoadRejected.mapping`），整份输出不发布。由于 CSP 与 SQLite
同语义，正常流程不应触发；若触发说明语义实现有偏差，宁可不出库。

## 8. verify 的独立性（不止看 PRAGMA 无错）

重开成品库做四类独立复核（任一失败抛 `VerifyError`）：
1. **PRAGMA/目录层**：`foreign_keys` 可开、`foreign_key_check` 为空，table_info/index_info/
   foreign_key_list 与原目录一致；
2. **逐槽声明层**：精确行数=槽数；固定值未被改动；one_of 值落在**声明域**内；类型为
   INTEGER/NULL；行多重集与物化结果完全一致（无复制/丢失/额外行）；
3. **独立业务 SQL**：NOT NULL 计数、把每个 CHECK 翻成独立 SQL 谓词（含 NULL 不拒绝语义）重算、
   UNIQUE/PK 用 GROUP BY 独立查重、复合 FK 用整元组 `NOT EXISTS` 独立查孤儿；
4. **provenance 一致性**：逐槽来源记录与库内行、与求解赋值三者一致。
测试中通过"关外键塞孤儿/域外值/额外行"验证这些独立查询确实能抓脏库，而非依赖 PRAGMA 状态。

## 9. 确定性与可复现

相同 schema+声明+seed 重复运行得到相同逻辑行（`assignments.jsonl` 行多重集一致，
不要求物理字节相同）；改变表名、行槽顺序、域内枚举次序均不改变规范结果
（见 `TestCanonicalInvariance` 与改名夹具 `fixtures/renamed-*`）。

## 10. 运行方式

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python -m synthdb plan     schema.sql generation.json            # 只看计划
python -m synthdb generate schema.sql generation.json out/sat    # 求解+装载+验证
python -m synthdb verify   out/sat/synthetic.db schema.sql generation.json
python -m unittest discover -s tests -v                          # 29+ 项测试
```

交付物（out 目录）：`synthetic.db`、`plan.json`、`assignments.jsonl`（逐槽来源）、
`verify-report.json`、`manifest.json`（输入哈希+schema 摘要+装载边界+行数+求解统计）；
无解时为 `unsat-conflict.json` + `manifest.json`，不产生数据库文件。

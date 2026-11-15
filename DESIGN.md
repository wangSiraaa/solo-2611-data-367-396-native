# 本机会话数据生产器（Python 3.11 + SQLite）

按相邻事件间隔（不是固定滚动窗口）把同一 `key` 的事件流切成 session，支持迟到
桥接合并、桥接撤回拆分、同 tick 多贡献、内容修订、水位最终化与冷归档，并产出下游
只需 `delete/upsert` 即可增量维护的 changefeed。单进程、单 SQLite 库、单事务内完成，
无分布式框架与故障恢复依赖。

## 文件

| 文件 | 职责 |
| --- | --- |
| `sessionizer.py` | 引擎与 CLI：`init / ingest / snapshot / changes / audit` |
| `oracle.py` | 独立全量重算 oracle（不导入引擎任何状态），含逐操作检查点与线性批量重放 |
| `consumer.py` | 独立下游消费者：只按 changefeed 的 delete/upsert 物化，逐批与 oracle 对账 |
| `probe.py` | 题给小样本当前集 oracle（全量分组参考），不是增量目标 |
| `test_flow.py` | 13 个端到端验证（见下） |

## CLI

```bash
python3.11 sessionizer.py --db s.db init --gap 5
python3.11 sessionizer.py --db s.db ingest --input events.json          # 可增量续跑
python3.11 sessionizer.py --db s.db ingest --init --gap 5 --input events.json
python3.11 sessionizer.py --db s.db snapshot    # 冷归档 UNION 热 session，不漏不重
python3.11 sessionizer.py --db s.db changes     # 有序变更批，带触发事件/水位/成员依据
python3.11 sessionizer.py --db s.db audit       # 独立全量重算 + 消费者对账，失败退出码 1
python3.11 -m unittest test_flow -v
```

`init` 时声明整数范围：`--gap`（正整数）、`--time-min/max`、`--value-min/max`，
gap 在库生命周期内固定；越界 time/value 明确拒绝。

## 数据模型（SQLite）

- `events`：未最终化事件，按 `(key_id,time,event_id)` 建有序索引；同 tick 不同 id
  各自成行，分组时稳定排序，全部计入 session。
- `event_ids`：event_id → key 身份登记，保证 key 不可修订；归档后的 id 永久退役。
- `hot_sessions` / `archived_sessions`：热状态与冷归档，列含 start/end/cnt/total/
  members（完整 event_id JSON 数组）。两层成员集合永不相交。
- `ops_log`：**只追加**的已接受操作流，是 audit 独立重算的输入；`rejections` 保留
  拒绝原因与原 payload，拒绝不触碰任何已发布数据。两表共用同一单调 seq。
- `batches`：每个接受操作一行，是变更批的事务边界（`finalized` 标记该批是否含归档
  迁移）。
- `changes`：changefeed。`op ∈ {delete,upsert}`，带 `tier ∈ {hot,archive}`、
  `trigger_event`、`watermark` 和完整 `members`。delete 行携带**旧**成员依据。
- `key_access`：正式处理路径对每个 key 的事件行读取计数（隔离性证据）。

## Session 身份与变更语义

`session_id = sha256(key,start,end,count,sum,sorted(members))`，内容寻址：

- 桥接迟到：旧两个 id 各发一条 `delete`（携带各自旧成员），合并体发一条 `upsert`；
  撤回桥接时反向：合并体一条 `delete`，两个拆开的 session 各一条 `upsert`。
  读者绝不会只看到“新的总数”，旧结果被显式撤回。
- 同 id 同内容重放：重算出完全相同的 id 集合，diff 为空，**零业务变化**；该操作仍
  作为空批进入 `ops_log/batches`，重放整条流产生逐字节一致的业务输出。
- 内容修订（key 不变、新旧 time 都仍 > W）：身份随内容变化，旧 id 删、新 id 发。
- 一个操作的所有 delete/upsert 在同一 SQLite 事务、同一批次内提交，不存在半次
  合并/拆分的可见状态。

## 水位与最终化发布

- W 单调不减；`time <= W` 的新事件、修订（含旧 time 已冻结）、撤回一律拒绝并记录
  原因；水位回退拒绝。未知 event_id 的 retract 明确拒绝。
- 仅在水位推进批做最终化（与 audit oracle 的发布组织一致）：凡 `end+gap <= W` 的热
  session（**含等号**，即 last+gap 恰到 W）整体迁入 `archived_sessions`，随后才能
  从 `events` 清理其成员行。还未最终化的旧事件即使 `time<=W` 也保留，因为它们仍
  贡献当前 session（测试 `test_old_open_session_events_are_not_cleaned` 覆盖）。
- 归档是同 `session_id`、同内容的存储迁移：changefeed 发一条 `tier='archive'` 的
  `upsert`，没有 hot 业务内容变化；下游据此维护热/冷两层，成员集合不会两层并存。
- `snapshot` 是 `archived_sessions UNION ALL hot_sessions`，成员集合不相交 ⇒ 不漏
  不重。

## 影响范围定位与访问计数（题设硬性约束）

正式热路径对一个事件操作只 `SELECT ... WHERE key_id=?` 读取**该 key** 的未最终化
事件，重算该 key 的全部热 session 后按内容 id 做集合 diff；不扫描其他 key 的事件。
每次读取按行数计入 `key_access`。测试 `test_other_key_event_reads_never_charged`
对 key A 做合并、拆分、修订、撤回，断言 key B 的读取计数始终不变；万级规模冒烟同样
验证未触及 key 的计数冻结。最终化只走 `hot_sessions` 的 `(key,end)` 索引，不读
`events`，因此不改变任何 key 的事件读取计数。

## 两种可行维护方案的取舍

1. **受影响 key 的未最终化部分整体重算（本实现采用）**
   - 撤回最简单：旧热 session 集合与新集合按内容 id 做差，删旧发新，成员依据天然
     完整且正确；无需在事件节点上维护前驱/后继指针与局部边界。
   - 扫描成本：每事件操作读取该 key 全部活跃事件（上限 1000 行，带索引），与其他
     key 完全隔离；100 key × 1000 事件 × 10000 操作规模下实测 ingest 约 4 秒。
   - 归档：只在水位批按索引挑选 `end+gap<=W` 的热 session 迁移，热清理与归档严格
     同事务；归档后该 key 的重算输入自动缩小。
2. **有序状态上的局部边界维护（备选）**
   - 在按 `(time,event_id)` 有序的事件结构上定位插入/删除/修订点，只改写受影响的
     相邻边界：扫描更省（O(变化邻域) 而非整 key），适合单 key 活跃事件远超 1000 的
     场景。
   - 撤回更复杂：删一个桥接点可能同时闭合左边界、打开右边界，需要显式维护并发布
     两侧 session 的旧/新成员依据；同 tick 多点修订时边界传播要谨慎处理。
   - 归档组织不变，但热清理后必须同步修正前驱/后继链接，否则后续局部定位会引用已
     归档节点。本实现以可忽略的规模内重算成本换取了撤回正确性与实现简单性，未做
     最优性证明。

## audit 如何成为 oracle

`oracle.py` 不引用引擎表，从 `ops_log` 的原始操作流用普通 dict 全量重放：自行推导
水位、拒绝规则、session 分组与归档保留。`audit` 做四件事：

1. oracle 最终的归档+热 session 与发布快照按 `(key,成员集合)` 逐 session 比对
   start/end/count/sum；
2. 独立消费者逐批应用 changefeed（小流逐批对照 oracle 检查点，万级流用等价的线性
   批量重放，避免检查点 O(N²)），按成员而非总量核对；
3. 形式路径与 oracle 的每 key 事件读取计数互相印证；
4. 整条流再重放一次，断言结果确定、逐字节一致。

## 测试覆盖（`test_flow.py`，13 项）

桥接合并含旧结果双 delete、桥接撤回拆分、同 tick 多事件稳定排序、修订跨 gap 边界
与同内容重放零变化、key 不可修订、未知 id 撤回拒绝、水位恰到 `last+gap` 的等号
最终化、迟到 upsert/修订/撤回与水位回退拒绝且数据不变、归档 id 不可再用、未最终化
旧事件不被清理、其他 key 访问计数不变、消费者成员级对账、独立 oracle audit、重放
幂等、归档/热 UNION 不重不漏，以及题给 `events.json` 与 `probe.py` 规则一致。

# 设计说明：双副本因果行复制

## 1. 交付物与运行方式

| 文件 | 作用 |
| --- | --- |
| `replicate.py` | 命令行前端：`write` `delete` `resolve` `export-events` `receive` `query-row` `export-data` `audit`，另有 `init` / `prepare` |
| `sync_tool/engine.py` | 存储引擎：不可变版本日志、因果前沿、事务 outbox、接收判定、物化投影、审计 |
| `sync_tool/audit_graph.py` | 独立的小规模事件图解释器，仅被 `audit` 用于交叉核对，**不在正式路径上** |
| `run_scenario.py` | 按 `scenario.json` 实际操作两份工作副本，只通过事件文件交付 |
| `tools/trace_sql.py` | 记录一次本地写与一次接收的**真实 SQL 语句**（`set_trace_callback`） |
| `tests/test_replication.py` | 28 个 stdlib `unittest` 用例 |

```bash
cp seed-A.sqlite A.sqlite && cp seed-B.sqlite B.sqlite   # 原件只读
python3 replicate.py prepare --db A.sqlite
python3 replicate.py prepare --db B.sqlite

python3 replicate.py write  --db A.sqlite r1 --state '{"name":"Alice","qty":10}' --expected-heads seed:1
python3 replicate.py export-events --db A.sqlite --out a.events.json
python3 replicate.py receive --db B.sqlite a.events.json
python3 replicate.py query-row --db B.sqlite r1
python3 replicate.py export-data --db B.sqlite
python3 replicate.py audit --db B.sqlite

python3 run_scenario.py          # 完整 scenario + 收敛断言 + 审计
python3 -m unittest discover -s tests -v
python3 tools/trace_sql.py
```

`prepare` 是幂等的：它给复制的 seed 库补上复制表，并把 `seed_versions`
物化进版本日志（origin=`seed`），使初始前沿就是 `r1→seed:1`、`r2→seed:2`。
`init` 则从零建库（`SEED` 与 `source.py` 相同），供测试使用。

## 2. 存储模型

### 2.1 不可变版本日志

```
version(version_id PK, event_sha256, event_json, origin, seq, dataset,
        row_id, kind, state_json, parents_json, local, ready, received_at)
```

* 行只插入，不修改内容；`version_id` 与 `event_json` 原样保留发送方身份。
* 本地版本 `local=1` 且在同一事务写入 `source_outbox`；远端版本 `local=0`，
  **绝不**写 `source_outbox`，也绝不分配本地 `seq`。
* `ready` 是**由父集合推导**的：所有 parent 都是同 row 的已知且 ready 版本时
  ready=1。每次接收/写入都在同一事务里对整行重算，所以父版本后到会自动让
  pending 子版本转为 ready；批次乱序、拆分、重复都不影响结果。

### 2.2 前沿（heads）

`frontier(row_id, version_id)` 是 `version` 的纯函数式投影：
ready 版本中，没有被另一 ready 版本用 `parents` 引用者的集合。

* 只依赖不可变身份与 parents，不依赖到达顺序、墙钟或 `seq` 大小，因此持有
  相同版本集合的两个副本得到相同 heads。
* 0 个 head → `absent`；1 个非墓碑 → `resolved`；1 个墓碑 → `deleted`；
  ≥2 个互不支配 head → `conflicted`。

### 2.3 业务投影与冲突成员

* `items` 只包含 **resolved** 行。`deleted` 行不在 `items` 中；
  `conflicted` 行也**不在** `items` 中，其每个 head 的完整值/墓碑写入
  `conflict_member(row_id, version_id, kind, state_json)`。
* 因此冲突永远不会伪装成一条普通行；`export-data` 的 `rows` 只有正常行，
  `excluded` 逐行解释 `absent`/`deleted`/`conflicted` 并附完整成员值。
* `query-row` 返回 `status`、`heads`、每个 head 的原始 `state`/`tombstone`、
  `pending`（含具体缺依赖）、冲突 `members`。

### 2.4 接收依据与去重

* `received(version_id, event_json, event_sha256)`：已接受版本的原样载荷。
* `claim(version_id, event_sha256, canonical, status, reason, ...)`：
  每个身份×内容字节的最终判定（`accepted` / `pending` / `duplicate` /
  `rejected`）。同一 `version_id` 换内容会产生第二行证据，首份载荷不受影响。
* `inbox`：每次投递尝试的完整流水（含拒绝与重复），audit 可复现。
* `head_change`：前沿每次变化的 before/after 观察记录，用于证明"收到对方
  事件没有产生新的本地回声"（远端版本只让 head 变成远端 id，本地 outbox
  计数不变）。

## 3. 事务边界

### 3.1 本地写（write/delete/resolve）

单个 `BEGIN IMMEDIATE` 事务内完成：

1. 读 `meta.seq` 并在锁内校验 `expected_heads == frontier(row)`；
   普通 `write/delete` 还要求 head ≤ 1（否则 `conflicted`）。
2. `seq += 1`，构造事件（`parents` 恰为观察到的 head 集合），
   canonical JSON + sha256。
3. `INSERT version`（local=1, ready=0）→ `INSERT source_outbox` →
   `UPDATE meta SET seq` → `_recompute_row`（ready/frontier/conflict/
   `items`/`head_change`）→ `COMMIT`。
4. `op_id`（可选）与 `local_op` 表在同一事务里记录；提交后回复丢失的重试
   返回原版本而不是造第二个版本。

因为 head 校验在 `BEGIN IMMEDIATE` 写锁内完成，多个进程用同一个旧 head
并发写时只有一个能提交，其余得到明确的 `stale_expected_heads`（测试用 5 个
真实子进程验证）。

### 3.2 接收

每个事件一个 `BEGIN IMMEDIATE` 事务（"逐项回执"）：

1. 结构/身份/dataset/state 校验；非法项写入 `inbox` + `claim` 证据后
   `rejected`，不影响同批其它项。
2. 身份去重：`version`/`source_outbox` 已存在同 sha → `no_change`；
   同 id 不同 sha → `rejected(identity_content_conflict)`，保留首份。
3. `origin == 本副本` 但不在本地 outbox → `rejected(own_origin)`（回声）。
4. 跨行 parent（parent 已知但属于别的 row）→ `rejected`；若 parent 尚未到达，
   先 `pending`，等 parent 到达时由 `_sweep_cross_row` 重新判定并拒绝。
5. `INSERT version`(local=0) + `INSERT received`，再在同事务内
   `_recompute_row`：ready/pending 状态、frontier、conflict_member、
   `items`、`claim`、`inbox` 一起发布。

`--fail-after-persist` / `--crash-after-persist` 在持久化后、提交前中断；
重开后整项不存在，重试成为正常的 `accepted`。已提交后进程被杀则状态保留，
重试是 `no_change`（两种都在测试中覆盖）。

## 4. 为什么没有回声

* 远端事件只走 `receive` 路径：写 `version(local=0)`、`received`、`claim`、
  `inbox` 与投影，**没有**任何代码路径写 `source_outbox`，也**没有**递增
  `meta.seq`。
* 本地事件只由 `local_write` 产生，只写 `source_outbox`。
* `audit` 直接比较：本地版本 id 集合 ∩ received 集合＝∅；
  `outbox_version_ids - local_version_ids`＝∅。
* 即使把收到的事件原样回传给发送方，发送方识别 `origin==self && 已在
  outbox` 为 `no_change`，不会再次出站。

## 5. 为什么没有部分发布

所有可见状态（`items`、`frontier`、`conflict_member`、`claim`、`inbox`、
`ready`）都只在**持有写锁的同一事务**内修改并一起提交。中断只会整项回滚：
不存在"版本在但 head 没更新"或"head 更新了但业务行没改"的中间态。
`audit` 还从日志独立重算 heads/业务行并与物化投影比对。

## 6. 为什么不会丢冲突

* 前沿是纯粹的 parent DAG 投影；只要两个 head 互不支配，就保持
  `conflicted`，不会按到达顺序、`seq` 或来源挑选。
* 冲突 head 的完整值保存在 `conflict_member`，且该行**移出** `items`，
  无法被误当正常行导出。
* 本地普通写遇到多 head 会拒绝；只有显式 `resolve` 且 `expected_heads`
  精确等于当前全部 head 才创建新版本。若在裁决前又出现新 head，
  该旧裁决因 `expected_heads` 不匹配被拒绝（测试
  `test_resolve_cannot_cover_newer_concurrent_head`）。
* 墓碑是版本：重放旧编辑不会复活因果已覆盖的行；只有观察到当前墓碑 head
  后显式引用它写新版本才能恢复。

## 7. 与另一合理方案的比较

**备选方案：以行多版本表 + 接收时全量重算的"重新物化"路线**，即不持久化
`frontier`/`conflict_member`，每次查询或每次接收时都从全表事件重算。

* 接收事务：本方案每次接收只重算**该 row**（以及跨行拒绝时的少数 pending
  row），复杂度与该 row（每个 row 上限 2000 版本）相关；备选若全库重算，
  接收成本随总版本数增长，恰好把最热的接收路径变成 O(全库)，在 500 行
  ×2000 版本上限下明显更差。
* 重新物化：本方案把物化投影与日志一致性显式暴露为 `audit.graph_check`，
  用独立解释器验证；备选方案"查询时才算"虽然天然不会出现投影陈旧，但每次
  查询都要重建 DAG，且 `export-data`（需要扫描所有行状态）代价高。
* 两者都不会产生回声或部分发布，因为关键不在于是否物化，而在于：
  接收与本地写分属**不同代码路径**、版本不可变、所有发布在同一事务。
  本方案额外获得事务内可验证的来源证据（`claim`/`inbox`/`head_change`）。

另一个常见备选是"每副本一个全局版本向量 / Lamport 时钟作胜负"。本实现
明确不用：`seq` 只是源端唯一身份，不要求跨行连续，不能用大小判因果；
胜负只由 parent 可达性决定，这正是契约要求。

## 8. 边界与非目标

* 不做 schema 迁移、字段级合并、跨行外键、真实网络、任意生产库同步。
* 固定 A/B 两副本、单一 dataset、行键稳定。
* `state` 必须是完整 `{name,qty}`（`qty` 为 >=0 的整数）或 `NULL` 墓碑；
  `NULL` 与空字符串不同。
* 不回收墓碑；重放不会复活已被因果覆盖的行。

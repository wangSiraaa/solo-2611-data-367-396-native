# CDC 装载器设计说明（Python 3.11 + SQLite，单机）

只实现题定 JSONL 协议，不包含 Kafka 消费者或 PostgreSQL WAL 解码。
三个输入流 `p0` / `p1` / `control` 由 `source.py` 产出，每路内部有序，
调用方可以任意交错三路；每路必须以显式 `{"kind":"end","source":...}` 帧结束，
EOF 不代表结束。

## 1. 协议记录形态（与 fixtures 一致）

| 流 | 记录 |
|---|---|
| control | `{"txn","parts":{"p0":n,"p1":n},"dependencies":[txn...],"terminal":"commit"\|"abort"}` |
| p0/p1 | `{"txn","partition","part_no","op_no","op",...}` |
| 任意流尾 | `{"kind":"end","source":"p0"\|"p1"\|"control"}` |

数据片段按 `(txn, partition, part_no)` 唯一；`op_no` 在同一 txn 内全唯一。
操作只有 `put_customer(id,name)`、`put_order(id,customer,amount)`、`delete_order(id)`，
`put_*` 按主键覆盖，`delete_order` 对缺失行为 no-op。

## 2. 存储布局（单个 SQLite 文件，WAL 模式）

目标表与所有装载状态同库，因此“业务写入 + 提交标记 + 回执”可以放进
**同一个 SQLite 事务**（`BEGIN IMMEDIATE ... COMMIT`），天然满足原子可见性：

- 目标：`customers(id PK, name)`、`orders(id PK, customer_id FK→customers.id, amount CHECK>=0)`，
  连接级 `PRAGMA foreign_keys=ON`，外键同时有装载器自己的声明式校验。
- `cdc_stream(source PK, ended, records)`：三路结束位与帧计数。
- `cdc_txn(txn PK, parts_json, deps_json, terminal, state, reason, control_seen, corrupt)`。
- `cdc_fragment(txn,partition,part_no, op_no,payload_hash,payload_json,seq)`，
  主键 `(txn,partition,part_no)`（WITHOUT ROWID 聚簇索引＝片段索引），
  另建唯一索引 `(txn,op_no)` 兜底全事务 op_no 唯一。
- `cdc_write_set(txn,kind,id)`：每个 put 的写入映像，用于写冲突合同核对；
  `delete_order` 不产生写入映像，不入此表。
- `cdc_receipt(txn PK, receipt, committed_at, ops, receipt_json)`：本地回执。
- `cdc_violation`（全部损坏/违例流水）、`cdc_event`（状态机审计）、`cdc_fault`（故障注入开关）。

片段索引选择 `(txn, partition, part_no)` 聚簇主键：组装事务时按 txn 定位、
分区计数与边界核对都是索引内扫描；2MiB 总量下零额外内存结构即可完成组装。

## 3. 状态机

```
receiving ──控制/片段仍缺且源未全部end──▶ receiving（保持，不推测失败）
receiving ──全部end后仍缺控制/片段──────▶ incomplete
receiving ──依赖控制未到────────────────▶ waiting_dependency
waiting_dependency ──全部end后依赖仍未知─▶ missing_dependency
waiting_dependency ──依赖进入失败终态────▶ blocked
* ──协议损坏（见§5）────────────────────▶ corrupt（终态）
* ──依赖环 / 无依赖路径写冲突────────────▶ rejected（终态）
ready ──语义校验失败或提交异常───────────▶ failed（终态）
ready ──COMMIT 成功──────────────────────▶ committed（终态）
任意 ──terminal=abort 且无损坏───────────▶ aborted（终态，从不触碰目标）
```

- `ready` 不是停留态：调度器一旦算出 ready 就在同一 pass 直接物化，
  避免“已就绪但崩溃在状态行与业务行之间”的窗口。
- 终态集合：`committed/aborted/failed/rejected/corrupt/blocked/incomplete/missing_dependency`。
- `blocked` 是终态：其依赖已进入不可能再提交的终态；不相关事务继续调度，
  一个缺片/失败事务不会挡住整条流水线。
- abort 判定让位于 corrupt：损坏的 abort 事务保留 `corrupt` 可核查记录。

## 4. 就绪条件与调度（work）

事务可被调度当且仅当同时满足：

1. 控制帧已到且 `terminal=commit`；
2. `parts` 声明的每个分区片段数**精确**到齐（少了＝缺片，多了/越界＝损坏）；
3. 每个 dependency 均已 `committed`（未知依赖在源结束前保持 waiting，
   结束后转 missing_dependency；abort/failed/rejected/corrupt/blocked/incomplete/
   missing_dependency 的依赖令后继 blocked）；
4. 依赖图无环（DFS 三色标记，环上全部事务 rejected，环外后继 blocked）；
5. 提交前语义校验通过：amount 为 `0..2^63-1` 的纯整数（拒绝 bool/浮点/负数）；
   订单外键的父客户必须在**同 txn 内更早 op_no 建立**，或由 **dependencies 中显式声明
   且已提交**的事务建立——不推测隐藏依赖，哪怕目标库里恰好已存在该客户；
6. 写冲突合同：对每个同 `(kind,id)` 的写入映像，沿 dependencies 求传递祖先闭包，
   双方必须存在依赖路径。一方已 committed 而后来者无路径 → 只拒绝后来者，历史不动；
   双方都完整且未提交、双向均无路径 → 两者都 rejected，不按到达顺序取胜。
   任一方还不完整时跳过，留待后续 pass，不提前判决。

调度按 txn 名确定性排序，多 pass 迭代到无变化；每次提交后重新装载内存视图，
保证同 pass 内后继事务能看到父事务的最新状态（如 t1 提交后 t2 同 pass 跟上）。

物化在单连接内串行：`BEGIN IMMEDIATE` → 按 `op_no` 排序执行全部操作
（UPSERT 覆盖主键、delete 无行 no-op）→ 写回执与 committed 状态 → `COMMIT`。
因此客户不可能“先开放再补订单”：事务未提交前两表都没有它的任何行。

## 5. 损坏与幂等（ingest，每帧独立事务）

- 同身份 `(txn,partition,part_no)` 且 payload SHA-256 相同 → 折叠（folded）；
  哈希不同 → `content_conflict`，置 corrupt，**保留此前好片段**（输入只读）。
- 控制帧重放全同折叠，任何字段差异 → `control_conflict`。
- 未声明分区、`part_no` 超出声明上界、重复 `op_no`、未知 op/terminal、
  流形状不匹配、end 后再到帧（`stream_closed`）、end 的 source 不匹配、
  非法 JSON/类型 → 全部记入 `cdc_violation` 并置 corrupt，绝不静默丢帧。
- 控制迟到时会对已落盘片段回溯分区声明与 part_no 边界。
- 损坏检测在 ingest 落标志，在 work 物化为 corrupt 终态；corrupt 事务永不物化。

## 6. 两种事务物化边界的取舍

本实现采用 **A：完整事务组装后提交（assemble-then-commit）**。

| 维度 | A 组装后提交（本实现） | B 预解析 + 事务围栏（早期写入、可见性围栏） |
|---|---|---|
| 目标表内容 | 只含已提交事务，语义最直接 | 含未提交/abort 的影子行，需会话级围栏过滤，查询更复杂 |
| 缺片占用 | 片段只占状态库，目标库零污染；缺片永远不碰业务表 | 影子行/围栏元数据随到达增长，缺片与 abort 都要 GC |
| 依赖阻塞 | 未就绪事务不持任何业务锁/行，blocked 无目标侧代价 | 围栏状态需随依赖提交级联翻转，崩溃恢复要重建围栏 |
| 大事务延迟 | 提交瞬间一次性应用全部 op（本题 ≤200 片段/事务，单库串行无压力）；首字节延迟＝事务完整时间 | 行提前就位，就绪后只需翻围栏，就绪→可见延迟略低 |
| 内存 | 不需要整 txn 常驻：片段全部在 SQLite，提交时按 op_no 游标读取 | 同样需要围栏表跟踪，内存优势不明显 |
| 正确性面 | 原子性完全由一个 SQLite 事务保证，abort 天然零影响 | 必须证明围栏翻转、回滚清理、崩溃恢复三者原子，面更大 |

结论：在 ≤100 txn、两分区、2MiB、单写者的题设尺度下，A 的占用与延迟完全够用，
且正确性论证最短；B 的提前写入优势在单 SQLite 写者下无法兑现（写入仍串行），
反而引入影子行生命周期问题。解析/JSON 校验在线程池并行，SQLite 写入串行，
符合“解析可并行、写入串行、不要求分布式事务”的约束。

## 7. 持久化与崩溃恢复

所有状态（含未组装片段）都在 SQLite；进程无内存态，重开即复用。

- 已 committed 的 txn：work 重跑时处于终态被跳过，**不重做业务写入**；
  回执由 `txn + 片段哈希序列` 确定性派生，相同 txn 重放返回同一回执。
- `fail_before_commit`（`arm-fault`）：业务语句执行后、COMMIT 前回滚，
  目标无痕迹，事务转 failed（一次性故障自动解除武装），后继 blocked。
- `lose_ack_after_commit`：COMMIT 已落盘后进程以退出码 3 终止；
  重开后 query/inspect 可见行、committed 状态与回执，再跑 work 无新提交、无重写。
- 两种恢复结果都可用 `inspect` 的事件流（`commit_failed_injected` /
  `ack_lost_after_commit`）、违例表和回执表核查。

## 8. 内存边界

工作内存不缓存任何完整事务：ingest 逐帧写库，work 每次只把
`cdc_txn` 控制行（≤100 行的小元组）载入内存做图判定，片段在提交时
按 `op_no` 排序游标读取后逐行应用。256KiB 工作内存之外的容量全部由
SQLite 暂存；WAL 在每次干净关闭时 `wal_checkpoint(TRUNCATE)`，
交付的 `.db` 是单一自包含文件。

## 9. 明确不覆盖的范围

- 不实现 Kafka 协议/消费者组、不解析 PostgreSQL 原生 WAL；仅题定 JSONL。
- 不做自动重试：failed 是显式终态（题设要求“提交失败明确 failed”），
  需要人工介入后以新 txn 重放源数据。
- 不推测源未声明的依赖；不处理协议外 op、表或字段扩展。
- 单机单写者，无并发写库、无分布式事务、无跨进程锁竞争处理。
- 源文件本身的传输可靠性（重复/丢帧发生在进入 ingest 之前）不在边界内；
  进入装载器后的重复折叠与损坏检测完整覆盖。

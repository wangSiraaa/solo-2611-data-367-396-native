# 离线有界 ETL：设计与正确性说明

## 1. 拓扑与角色边界

```
 source-a ─┐   src:a (cap 1/2)                     r2w:0 (cap)   w2s:0 (cap)
           ├─> router ── key%2 == 0 ─> worker-0 ──────────────┐
 source-b ─┘        │                  r2w:1       w2s:1      ├─> sink ─> output.jsonl
                    └── key%2 == 1 ─> worker-1 ──────────────┘
```

固定 6 个线程（非守护线程，正常/取消/故障都 join 回收）：2 个 source、1 个 router、
2 个按 `key % 2` 分区的 worker、1 个 sink。角色之间**只**通过自定义有界通道
（`etl_demo/channels.py`）交换 `Frame`，没有共享工作队列、没有无限缓冲。

- 路由用 Python 的 `%`，即数学模：负 key 也得非负余数（`-1 % 2 == 1`、`-2 % 2 == 0`）。
- worker 对每条输入产生 `part_no = 0..fanout-1` 的结果，值为 `value*value + part_no`；
  `fanout=0` 不产出但计入 `records_processed`。
- 结果稳定身份是 `(source_id, seq, part_no)`；`(source_id, seq)` 是输入唯一身份，
  不同来源同 seq 不合并、同业务值不去重。
- sink 落盘后写 `output.jsonl.complete` 标记；只有完整路径才发布该标记，
  cancelled / failed 路径绝不发布。

## 2. 通道、帧与全局 payload 台账

### 有界帧通道（容量只允许 1 或 2）
每个 `BoundedChannel` 内部是一个 `deque` + 一把 `Condition`，容量在构造时固定、
运行期绝不扩容。`put` 满则阻塞背压，`get` 空则阻塞等待。DATA 与 EOS 排在**同一个
deque**，EOS 是普通一帧（payload 为 `b''`），因此终止天然排在此前所有数据之后，
不可能“终止越过数据”。

### 全局 16KiB payload 信用台账（ByteBudget）
所有“尚未落盘的编码 payload”共用一本账，覆盖题目列的全部位置：source 待发送、
各有界队列、router/worker/sink 当前持有、sink 写盘失败的重试暂存。规则：

- 帧创建时 `acquire(n)`，落盘或丢弃时 `release(n)`；帧在容器间转移**不重复计数**。
- 每帧有全局唯一 `fid`，`release` 按 token 幂等，防止取消/异常竞争下同一 payload
  被两个所有者重复归还。
- 引擎 `join` 末尾 `assert_zero()`：台账不归零直接判失败，泄漏无处藏身。

### 输出保留额度（打破预算/槽位循环等待的关键）
输入帧与输出帧共用预算会产生资源环：在途输入占满预算 → worker 等输出预算 →
router 等 worker 输入槽位 → sink 无输出可读。`ByteBudget` 因此保留
`output_reserved`（默认 256B）：**输入帧只能用 `limit - reserved` 的空间，worker
输出申请可用满全部额度**。于是任意 worker 处理手头输入时，总留有至少一个输出帧
的空间，sink（不依赖预算、始终可运行）可持续排空并释放预算，环被打破。
极紧预算下保留值会被钳制为 `limit - 64`，仍保证一个最小输出帧可授予。

## 3. 多路接收、发送与取消的推进方式（选型）

采用 **就绪驱动（readiness / credit-driven）+ 单一共享就绪条件**：

- 每个通道自己的 Condition 负责“有空位 / 有数据”的点对点唤醒。
- 所有 `ChannelGroup`（router 收两路 source、sink 收两路 worker）共用引擎级的
  一把就绪 Condition；任何通道 `put/close` 都 `notify` 它。
- `get_any` 先**无阻塞全量扫描**所有成员通道，任一通道已有帧立即取走；全部为空
  时才在共享就绪条件上挂起。这样：
  - 不会在另一路还有数据帧时优先消费 EOS（终止不越过数据）；
  - 不会因在某条空通道上等待而漏掉另一条通道的入队通知（跨组通知丢失）；
  - 不做忙轮询、不引入额外分发队列。

**为什么不用“并发排空”或“有界落盘”作为主方案**见第 6 节备选。

### 取消：独立终止控制，不进数据队列
`Engine.cancel_event` 是与数据通道完全独立的 `threading.Event`。所有等待点
（通道 put/get、多路接收、预算 acquire、测试屏障）都以 50ms 粒度轮询它，因此
取消不可能排在已满数据队列的尾部“送不到”。屏障解除后各角色在有限步内退出，
引擎排空全部通道、按 fid 幂等释放残留 payload、join 全部 6 个线程，报告
`cancelled` 与已落盘前缀，不写 complete 标记。

### 所有权契约（消除取消竞争下的双重释放）
- `put` 返回布尔：`True` 表示帧确定已入队（所有权移交队列，由引擎 drain 释放）；
  取消导致放弃时返回 `False`，帧确定未入队、仍归调用方，由调用方释放。
- worker 用 `inflight` 集合精确跟踪“已申请预算但未确认入队”的输出帧。
- 容量判定优先于取消：只要当前能取得空位/数据就先完成本次确定的所有权转移，
  取消在下一轮“确实还需等待”时才生效，避免在途帧归属不确定。

## 4. 终止协议（空源 / fanout=0 都不会提早）

1. source 读完（含空文件）才向自己的 src 通道写一个 EOS；EOS 与数据同队列。
2. router 多路接收，收齐**全部** source 的 EOS 且此前输入全部转发后，才向每个
   worker 通道各写一个 EOS。
3. worker 收到自己输入通道的 EOS（意味着属于它的输入已全部处理，fanout=0 也已
   记账）后，才向自己的 sink 通道写 EOS。
4. sink 收齐两个 worker 的 EOS 并排空此前全部输出（含重试暂存）后，flush、
   写 `.complete` 标记，声明完整。

空源只产生 EOS、不产生数据，终止链依旧逐级汇合；fanout=0 的输入被计为
已处理但不产出，都不会让终止提前。

## 5. 故障与重试

- worker 钩子可在处理指定输入前抛异常（测试在 worker-0 处理 `a:4` 前注入）。
  异常写入 `board.error`（含 role、类型、消息、input_identity），随后 `cancel.set()`
  让其他角色在最近等待点收敛，引擎统一回收，报告 `failed`、不发布 complete。
- sink 写盘抛 `OSError` 时把帧转入**内存重试暂存**（字节始终在预算内，计入
  `retry_staging_*` 指标），最多重试 3 次；测试钩子默认只注入一次失败。
- 不承诺异常后继续跑、恰好一次网络投递或掉电恢复（与题目一致）。

## 6. 备选方案及其正确性边界 / 占用差异

**备选 A：多进程 + 有界 multiprocessing.Queue / Pipe**
- 正确性边界：分区与终止协议可照搬，但跨进程序列化使“全局 16KiB 台账”难以精确——
  各进程内存独立，只能用信用报文（credit 消息）近似合并，报文本身有延迟，峰值
  约束变松；取消需额外的控制管道，EOS 与信用报文可能乱序，要再加序号握手。
- 占用差异：每进程一个解释器，固定开销从 6 线程（同进程，约 13MiB RSS 共享）
  涨到数个独立解释器（数十 MiB 级）；好处是绕开 GIL，CPU 密集的 `value*value`
  展开可真并行。本任务是 I/O/背压主导、数据量小，进程方案更重且台账不精确，故不用。

**备选 B：有界落盘暂存（spool-to-disk）**
- 做法：sink 暂停时，worker 把溢出输出写到容量有界的临时文件，内存只保留句柄。
- 正确性边界：能缓解长时间 sink 停机，但引入“第二套存储”，崩溃时磁盘暂存与最终
  输出的一致性需要额外事务/清理协议；题目允许但要求披露容量与实际字节。
- 占用差异：内存峰值下降，但增加磁盘字节和 fsync 成本，且吞吐受磁盘限制。
  本实现选择**不做落盘暂存**（`spool_capacity_bytes=0`），完全靠有界通道 + 预算
  背压把压力自然传回 source，语义最简单、崩溃面最小；如需要可在 sink 前加一个
  有界 spool 角色，复用同一本 ByteBudget 记账。

**备选 C：asyncio 单线程事件循环**
- 用 `asyncio.Queue(maxsize=1/2)` 配合单个 `cancel` Task 与 `wait(QUEUE_FIRST...)`
  多路接收。正确性边界与本方案等价，且无线程竞争、所有权更简单；但要求全链路
  协程化（文件写要用 executor 或 aiofiles），与现有同步 JSONL/sink 集成成本更高。
  线程 + Condition 是本任务下最直接的“真实有界传输”表达。

## 7. 测试协调（事件/屏障，不以 sleep 推断无死锁）

- `StatusBoard.wait_until(predicate)` 在 Condition 上等待状态谓词，超时附完整快照。
- 背压控制点谓词：`worker-0.state == blocked_put` 且 `src:* / r2w:*` 至少有一帧
  在途——这是“worker 已被输出背压、router 仍有输入”的直接证据，不靠时长。
- 取消用可复用 `Barrier`：sink 第 5 个输出后 arm，测试 `cancel()` 后再 release，
  断言 outcome=cancelled、无 complete 标记、输出是期望多重集的前缀、无线程残留。
- 每个用例收尾都断言 6 个线程全部 `is_alive()==False` 且预算台账归零。

## 8. 复跑命令

```bash
python3 generate.py data                 # 生成两路 JSONL（24 输入 / 36 输出）
python3 -m unittest tests.test_pipeline  # 15 个用例（容量1/2、倾斜、空源、全零fanout、
                                         #  负key、身份不去重、取消、故障、重试、超限、紧预算）
python3 run_demo.py data run normal                 # schedules.json: 暂停sink->背压证据->恢复->完整
python3 run_demo.py data run cancel                 # 第5个输出后取消
python3 run_demo.py data run worker_failure         # worker0 处理 a:4 前注入异常
python3 run_demo.py data run all --capacity 2       # 容量 2 复跑
python3 run_demo.py data inspect runs/normal-cap1   # 查看落盘 state.json
```

## 9. 实测占用（本机，容量 1，默认 16KiB 预算）

| 指标 | 值 |
|---|---|
| 固定线程数 | 6（source×2, router, worker×2, sink），与 payload 预算分开报告 |
| 解释器 RSS 峰值 | 约 12–13 MiB（`resource.getrusage`，含全部线程共享的解释器开销） |
| payload 预算峰值 | normal 容量1 ≈ 609 B、取消 ≈ 546 B；容量2 ≈ 792 B（均远低于 16KiB 上限） |
| 通道峰值帧数 | 容量 1 时每通道 ≤1；容量 2 时 ≤2（测试断言容量纪律） |
| 有界落盘暂存容量 | 0（本实现不 spool，`spool_capacity_bytes=0`） |
| 重试暂存峰值 | 正常 0；注入一次写失败时 = 该帧字节数（已计入预算，`retry_staging_peak_bytes`） |
| 输入帧上限 | 256 B，超限按 source 故障收敛 |
| 终态台账 | 任何结局 join 后 `ByteBudget.used == 0`，非零即判失败 |

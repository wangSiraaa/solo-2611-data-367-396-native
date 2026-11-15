# 本地共享查询结果服务（Python 3.11 + SQLite）

多个分析客户端并发提交**相同快照 + query_id + 带类型参数**的查询时，服务只执行一次
真实 SQLite 游标，结果写入受管 spill 文件形成可重放前缀，各客户端用独立
`consumer_id` 和独立读取位置从第 0 行顺序消费。无需 HTTP、账号、跨主机缓存或崩溃恢复。

## 文件

| 文件 | 职责 |
|---|---|
| `prepare.py` | 生成只读 `snapshot.sqlite`（30 行 `observations`） |
| `queries.json` | 查询目录：仅两条登记的参数化只读 SQL，含声明类型与行/字节上限 |
| `config.json` | `pool_bytes=32768`、`resident_rows=16`、`max_consumers=6` |
| `catalog.py` | 白名单只读校验、带类型参数校验、类型化规范键 |
| `service.py` | 核心：物理查询、spill 池、消费者、线性化取消/淘汰、屏障、审计 |
| `cli.py` | `serve/submit/read/cancel/status/evict/audit` 命令（Unix 域套接字） |
| `demo.py` | 9 个阶段、81 项断言的确定性并发演示与独立直连核对 |
| `stress.py` | 6 线程随机调度压力测试与不变量审计 |

## 运行

```bash
python3.11 prepare.py     # 生成/重建只读快照
python3.11 demo.py        # 确定性并发演示（81 项断言）
python3.11 stress.py      # 随机调度压力测试

# 命令行服务（也可直接 import service.QueryService 调用 API）
python3.11 cli.py serve
python3.11 cli.py submit by_group '["g1"]'
python3.11 cli.py read   c0001 100
python3.11 cli.py cancel c0001
python3.11 cli.py status
python3.11 cli.py evict
python3.11 cli.py audit
```

## 关键设计

### 规范键与类型隔离
键 = `快照 SHA256 | query_id | 每参数(类型标记:长度:值)`，长度前缀编码。
INTEGER 用 `i:`、TEXT 用 `s:`，因此整数 `1` 与文本 `'1'` 不会合并；运行时类型与
声明不符在 `Catalog.validate` 即被拒绝，不接受目录外任意 SQL（SELECT 白名单 +
写关键字黑名单 + 只读 URI 连接 `mode=ro`，三重约束）。

### 一次物理执行、可重放前缀（不共享迭代器）
一个规范键对应一个 `PhysicalQuery` 与一个工作线程：`cursor.execute` 一次、
`fetchmany(32)` 分批，行以 `4 字节长度 + pickle 帧` 追加到 spill 文件，并维护
`(文件偏移, 帧长)` 索引与至多 16 条的尾部驻留缓冲。每个消费者持独立偏移，读取时
驻留命中走内存、否则对同一 `r+b` 句柄随机定位回放——不存在多客户端竞争消费一个
迭代器的情况。

### 容量口径：预留即占用，不与实际双计
准入时按目录 `max_encoded_bytes` **预留**并立即计入 `reserved_total`；实际写入只
累计行编码字节用于上限判定，不再二次计入池容量。因此“新 spill 文件目前很小”绝不
会被当成可用空间。池满时新**物理键**被明确拒绝（`accepted=false` + 原因），同键
订阅仍收敛复用、不需要新预留。行/字节上限在**写入前**判定，超限把整个物理查询置
`FAILED` 并保留已写前缀，所有订阅者收到一致失败原因，绝不先发成功再补错。

### 线性化取消与淘汰
所有注册表/状态变更在单一 `Condition` 锁内完成：

* 消费者 `cancel` 只移除自身订阅；仍有其他消费者时物理执行与前缀继续可用。
* 最后一个消费者在 PENDING/RUNNING 退出 → 物理查询转 `CANCELLING`，工作线程在
  下一批 fetch 或屏障点检测到后删除 spill、释放预留、移出注册表。
* `submit` 遇到 `CANCELLING` 必须等清理完成，新订阅者绝不会接到已撤销执行；同键
  再提交得到 `gen` 递增的全新执行。
* 取消与新订阅交错时，`last_consumer_exit` 屏障返回后在锁内**重新检查**消费者集合：
  屏障期间有新订阅则取消作废（`retained_after_barrier`），否则撤销，二者构成一个
  明确的线性化顺序。
* `evict` 按 `(terminal_at, gen)` 确定顺序回收最老的、已终态且无消费者、
  `active_reads=0` 的查询；活跃读者与订阅者受保护，回收后同键可重新执行。

### threading.Event 屏障（`service.Hooks`）
默认全部事件已 set（不阻塞），演示按需 `clear()`：`before_run`（执行未开始）、
`first_prefix`（首批前缀写出前，复现迟到订阅从 0 重放）、`last_consumer_exit`
（最后消费者退出前，复现取消/加入竞争）、`before_terminal`（终态发布前）。屏障经
`Condition.wait_for(event.is_set)` 等待，等待期间释放服务锁，不堵死其他提交。

### 终态语义
`read(max_rows)` 只返回从消费者当前偏移开始的**连续片段**：有数据但未终态为
`CONTINUE`（部分数据不等于成功）；读到末尾且物理查询 SUCCESS/FAILED/CANCELLED 才
给明确终态并把消费者移入历史。自然终态（SUCCESS/FAILED）不会被后续 `cancel` 覆盖。
`status()` 记录每个物理查询的 `execute_count`、`fetch_count`、预留/实际 spool、
状态，以及每个 consumer 的终态与原因；`audit()` 校验预留合计、池上限、驻留行数、
spill 文件存在性等不变量。

## 演示覆盖（demo.py）

1. 执行未开始的同键并发 submit 收敛到同一 PENDING 物理执行（execute_count=0 时共享）
2. 已有前缀时迟到订阅从 0 重放；一快一慢两消费者独立全量消费
3. 一个消费者取消不截断其他人；重复取消幂等
4. 终态发布前接入的迟到者复读成功结果
5. 池容量按预留拒绝新物理键、同键仍复用、不同键不误合并
6. 确定淘汰、活跃读者保护、淘汰后新一代重新执行
7. 最后取消撤销物理查询；取消与新订阅交错的两种线性化顺序
8. 行上限/字节上限整体 FAILED，多订阅者原因一致且从未收到 SUCCESS
9. 资源审计、每消费者终态记录、**独立直连 SQLite 逐行核对每个消费者全量结果**

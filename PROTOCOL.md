# 在线水平分片迁移协议说明

将指定 tenant 从 `shard-a` 迁到 `shard-b`，源分片不停写。组件：

| 文件 | 角色 |
|---|---|
| `shard.py` | 给定材料：SQLite 行/tombstone/递增 rev/请求回执/outbox 原语 |
| `router.py` | 路由目录：`routing.json` 记录每个 tenant 唯一 `(owner, generation)`，原子替换发布 |
| `client.py` | 路由客户端：缓存路由、随请求携带 generation、MOVED/RETRY 处理、稳定 request_id |
| `coordinator.py` | 迁移协调器：快照+outbox 追赶、持久阶段机、fence/activate/publish cutover |
| `verify.py` | 独立核验：原始 sqlite3 逐行/逐请求重放比对，不用协调器代码 |
| `migrate.py` | CLI：init / start / step / status / cutover / read / write / verify / cleanup |
| `demo.py` | 完整日程演示（带可控 barrier 与 pause 探针），产出真实两个数据库 |
| `test_migration.py` |  barrier 复现测试 T1–T7 + 幂等/中止/跨进程用例 |

## 1. 业务语义与去重根基

- 行身份 `(tenant, row_id)`，`value` 为 64 位有符号整数；`put` 完整替换；`delete` 写带 rev 的 tombstone（删不存在行也成功，防止删除落在旧 backfill 之后被复活）。
- 每次成功业务请求在当前 owner 的**单库事务**里同时提交：行、tenant 递增 rev、`request_id` 内容哈希+原回执、outbox 事件。outbox rev 在同一事务分配/提交，**无领号空洞**（仅对本设施成立，不外推）。
- 同 `request_id`+同内容重试 → 返回**完全相同的原回执**，不分配新 rev；同 ID 不同内容 → `REQUEST_ID_CONFLICT`。请求内容不含 generation，换 owner 重试仍是同一业务请求。
- 因此"不重复执行"不靠 value 去重，而靠 `request_id → 唯一持久决定（content_sha+原回执）→ 唯一 rev`；核验器（F1/F2/F3）逐请求检查该链。

## 2. 代次（generation）与路由

- `routing.json` 每 tenant 唯一 `(owner, generation)`；发布为临时文件+`os.replace` 原子替换，读者只见完整旧/新文件。generation 只增；换 owner 必须升代。
- 客户端缓存路由并随每个新请求携带 generation（payload 内不含）。分片对 `writable=0` 或 generation 不符的新写返回 `STALE_OWNER_GENERATION`：
  - 路由已变 → **MOVED**：刷新缓存，以**同一 request_id** 打到新 owner；
  - 路由未变 → **RETRY** 窗口（cutover 停顿）：稍候重试，同一 request_id。
- 客户端永远不因"结果未知"而换新 request_id；重试耗尽只上报 INDETERMINATE（可继续以同 ID 重试）。fence 后的源对旧请求仍返回已存原回执——这不是新写，是安全的。

## 3. 迁移阶段机（持久化于 `migration-<id>.json`）

```
INIT → COPYING → READY → FENCED → CAUGHT_UP → ACTIVATED → DONE
  └────────────── 任一点发现分歧 → ABORTED（终态，保留现场）
```

依赖顺序（不可交换的理由）：

1. **接续边界 C1**：`start` 打开源 WAL 读快照并记录源 clock=C1（落盘）。快照打开期间新写照常提交（WAL 读写不互斥，T1 复现）。
2. **COPYING**：按稳定 `row_id` 游标分批 backfill（每批 1–100 行）；按 `request_id` 游标分页导入**迁移开始前已存在**的回执；同时重放 `rev > hwm` 的 outbox 事件（hwm 从 C1 起）。目标是 shadow：按**来源 rev** 应用（`excluded.rev > records.rev` 才覆盖），与到达顺序无关——旧 backfill 不会覆盖新事件（T2），删除不会复活（T3），游标后的插入/删除由 outbox 接续（T3）。
3. **READY**：行游标与回执游标均扫完。cutover 要求 READY，否则拒绝（`NOT_READY`）。
4. **FENCED**：`fence(tenant, start_generation)` 原子关闭源新写并返回 `fence_clock`（源最后提交边界）。**必须先 fence 再 activate**：任何时刻都不存在两个可写 owner。
5. **CAUGHT_UP**：重放 outbox 直到 `hwm == fence_clock`。此刻目标行/tombstone/回执完整：rev≤C1 的回执经分页导入，C1<rev≤fence_clock 的回执随事件导入。
6. **ACTIVATED**：`activate(new_generation)`。目标 clock==fence_clock，故目标首个新写 rev=fence_clock+1，**新 rev 必大于已导入来源边界**，两代 rev 域不重叠。
7. **DONE**：发布路由 `{owner: shard-b, generation: new_generation}`。activate 先于 publish，路由永不指向不可写 owner。

**不宣称跨库原子**：fence（源库）、activate（目标库）、publish（路由文件）是三次独立持久提交。安全性来自顺序+幂等：fence 单向不可重开；每个子步幂等；fence→publish 之间是唯一停顿窗口，新写**在两边都不被接受**，客户端看到可观察的 RETRY（demo 的探针在窗口内实测到 RETRY→MOVED→OK）。崩溃恢复：journal 落盘于每次外部变更之后，重启时 `_reconcile()` 用"可观察即已发生"的持久事实（源 writable=0 / 目标 gen=2 writable / 路由已发布）认领未落盘的子步并续跑；fence 后源 clock 冻结，认领的 fence_clock 必然正确。

**快照不跨进程**：源读事务不能持久化。进程内协调器持有固定旧快照完成整个 backfill（T1/T2 的"固定旧快照"语义）；跨进程（CLI 每步一个进程）或重启后，`step` 重新开一个短期快照。正确性不依赖某行从哪个快照读：每个 rev≤C1 对**所有**快照可见（快照 clock 都 ≥C1），每个 rev>C1 都会经 outbox 重放（hwm 从 C1 起且无提交空洞）。无法安全续接（如目标出现分歧数据 `REV_CONTENT_CONFLICT`）→ 明确 `ABORTED`，保留现场，须换新 migration id 重来；不伪称旧快照仍在。

**幂等提交**：同 migration id 重复 init/start/cutover 返回当前已完成阶段，不会重新打开已 fence 的旧 owner（源 writable 永不回 1）。

## 4. 复制期与切换后的读写路径

- 复制期路由仍指源：读由源返回完整 tenant 数据，写打到源；目标只是 shadow，业务请求不能写入（路由不指向它，且它 writable=0）。**从不拼接两份物理副本**——每次 read 只查一个 owner。
- cutover 窗口内：读写得到明确 RETRY；路由发布后客户端 MOVED 到新 owner。
- 切换后：旧 generation 新写打到旧分片 → `STALE_OWNER_GENERATION` 拒绝（或由客户端 MOVED 后以同 request_id 转投新 owner，新 owner 凭 requests 表去重，证明不重执）；旧请求在旧分片返回已存原回执不算新写。未迁移 tenant 全程在原归属读写，目标库不出现其任何行。

## 5. 独立核验（verify.py）

不经过协调器，直接重放两个库：源 outbox 连续性（1..fence_clock 无洞）；源记录==源 outbox 重放；目标记录==重放（源≤fence)+重放（目标>fence)（逐行含 tombstone）；每个源回执在目标完全一致（含迁移前回执）；目标 rev≤fence 的回执必在源中存在；新回执 `accepted_generation==new_gen` 且字段自洽；每个 request_id 全库唯一决定、任意两请求不共享 rev、每回执可映射到唯一 outbox 事件；未迁移 tenant 源内自洽、目标零泄漏、路由未动；journal 阶段/边界与库内事实一致。共 22 项，全过才退出 0。

## 6. 统计

journal 记录：实际 backfill 读行数/写入行数（差值=被 rev 守卫丢弃的过期行）、outbox 重放事件数/写入行数、回执读取/导入量、每次阶段迁移时的路由与双方 (generation, writable, clock)。demo 末尾打印完整日程与统计。

## 7. 另一可行方案与取舍

**受控双写/转发（未采用）**：shadow 追平后不 fence，源在单库事务提交前同步转发到目标（或应用层双写），目标凭 request_id 去重实现幂等，随后直接发布新路由，停顿为零。取舍：转发路径要处理"目标已成、源崩溃未 ack"的悬挂态（客户端以同 ID 重试可收敛，但恢复推理复杂）；源在切换后仍参与请求路径，故障面大；仍须升代 fence 掉队者。本设计选择**一次明确短暂停顿**换取单点决定与极简单的正确性论证：fence 即最后边界，无并发双 owner 窗口，崩溃恢复只需幂等重放。

**纯 outbox 全量重放（无 backfill）**：更简，但重放量是全历史；backfill 把重放量界到迁移窗口内。本题两者都做：backfill 为主、outbox 接续边界。

## 8. 运行

```bash
python3 -B prepare.py <fresh-dir>                 # 两个真实 SQLite + routing.json
python3 -B demo.py <fresh-dir>                    # 完整日程（barrier 复现+探针+核验）
python3 -B -m unittest test_migration -v          # T1–T7 等 11 个复现测试
# 或逐步 CLI：
python3 -B migrate.py <dir> init --tenant moving --source shard-a --target shard-b \
    --start-generation 1 --new-generation 2 --migration-id m1 --batch-size 3
python3 -B migrate.py <dir> start --migration-id m1
python3 -B migrate.py <dir> step --migration-id m1 --batches 1   # 重复至 READY
python3 -B migrate.py <dir> cutover --migration-id m1
python3 -B migrate.py <dir> read --tenant moving
python3 -B migrate.py <dir> write --tenant moving --request-id r1 --payload '{"op":"put","row_id":"x","value":1}'
python3 -B migrate.py <dir> verify --migration-id m1
python3 -B migrate.py <dir> status --migration-id m1
```

## 9. 边界与限制

固定本地两分片、≤4 tenant、每 tenant ≤10000 行、批 1–100 行、两业务客户端、单迁移协调器（flock 防并发协调）、一次迁一个 tenant、不做全局重分桶。不处理：任意时刻 SIGKILL 后恢复旧快照（明确 ABORTED 并用新 migration id 重建）、磁盘损坏恢复、跨主机共识、生产接入。可选 `cleanup --confirm` 仅在 DONE 后删除源中本 tenant 数据，不动其它 tenant。

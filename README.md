# 本地 CSV 分段上传与发布工具

Python 3.11，仅用标准库。工具 `delivery_tool.py` 通过 `multipart_store.py` 提供的
`Store` 可执行对象服务模拟器完成分段上传、条件提交与发布；**只调用 Store 的公开方法，
不直接读写服务的 `state.json`，也不管理服务的 `service.lock`**。

## 文件

| 文件 | 作用 |
| --- | --- |
| `multipart_store.py` | 给定的单机持久对象服务模拟器（flock 跨进程串行化、原子 state 替换、不可变版本、交付回执）。仅新增只读 `reload()` 辅助方法，未改任何既有语义 |
| `delivery_tool.py` | 交付工具，子命令 `plan / upload / resume / publish / verify` |
| `run_demo.py` | 10 个场景、31 项断言的真实进程端到端演练（含硬中断与故障注入） |
| `config.json` | 示例输入参数（delivery-a / reports/result.csv / 64B / gen 0） |
| `source.csv` | 示例只读 CSV（483 字节，8 段，末段 35 字节） |

## 约束与范围

- 单文件 ≤ 16 KiB；分段大小 64–256 字节可配；分段编号从 1 连续，末段可短；≤ 256 段；两个上传者。
- 服务保证**已提交操作持久**；不涉及公网、认证、分布式共识或恶意服务抵赖。
- 分段 etag 即该段**实际字节**的 SHA256；整对象在服务端组装后再次校验整对象 SHA256 与长度。

## 本地持久日志（原子写：tmp + fsync + rename）

状态目录下两个文件：

- `plan.json` —— 交付计划，绑定：源文件 SHA256、长度、分段策略（part_bytes 与每段 offset/length）、
  目标 key、delivery_id、expected_generation。
- `upload.json` —— 上传日志：`upload_id`、每段经服务确认的 `etag/size`、complete 尝试次数、
  `object_version`、服务交付回执、累计发送字节计数。每确认一段立即落盘。

## 命令

公共参数：`--state-dir <本地日志目录> --store-root <服务数据根> [--store-lib multipart_store.py]`

### 1. plan —— 计算并持久化交付计划

```
plan --csv <只读CSV> --delivery <id> --key <目标key> \
     --part-bytes <64..256> --expected-generation <0 或调用者明确给出的上一代>
```

- 预检 `head(key)`：目标不存在时期望 0；存在时当前代必须与 `--expected-generation` 相等，
  否则退出码 10（`GENERATION_CONFLICT`），不产生任何上传流量。服务端 `complete` 仍是权威 CAS。
- 同一状态目录复用 `delivery_id` 但**源摘要/长度/key 变了** → 退出码 4 明确拒绝，
  不允许把不同内容挂到旧 delivery（服务端另有 `DELIVERY_CONTENT_CONFLICT` 独立防线）。
- 同一原件仅 `part_bytes` 变化 → 视为**明确的新计划**：旧分段边界失效，先 `abort` 自己名下
  且无成品回执的旧暂存 upload，再清空本地段确认；别人的 upload 绝不触碰。
- 源文件在任何阶段重读后身份（SHA256+长度）与计划不符 → 退出码 12 拒绝继续，
  绝不把旧 part 位置套到新源。

### 2. upload —— 分段上传并 complete（可重复执行）

对计划中的每一段，只有在以下三者一致时才认定已确认：

1. 用计划 offset/length 从源文件读出的实际字节算出的 SHA256；
2. 本地 `upload.json` 记录的 etag；
3. 服务 `list_parts(upload_id)` 返回的同编号 etag 与 size。

任一不符（含同 `part_no` 内容不同）都重新上传该段，**不沿用旧日志确认**。
分段连续完整后提交 manifest（连续编号 + 每段 etag）与条件 `expected_generation`：

- 服务 CAS 成功 → 生成新的不可变 `object_version`（`key@N`），回执绑定
  delivery_id、整对象 SHA256、长度；工具把版本与回执落盘。
- 抛 `ValueError`（GENERATION_CONFLICT / DELIVERY_CONTENT_CONFLICT 等）→ 对应退出码报告，无覆盖。
- 抛 `OSError`（提交前失败或提交后 `RESPONSE_LOST`）→ **异常本身不用于判断对象有无**，
  状态置为 `complete_uncertain`：同进程先查 `receipt(delivery)`、再查 `head(key)` 的绑定身份，
  完全匹配才认领；key 上是别的 delivery/内容则退出码 11 报告冲突；都查不到则保持未知，交 resume。
- 已有 `object_version` 时重复 upload 只做只读复核，**绝不提交第二个对象**。

### 3. resume —— 中断/未知状态收敛（不盲目新建对象）

按顺序收敛，命中即返回：

1. 本地已有版本：按不可变 version `read` 回全部字节复核 SHA256/长度，通过则零额外对象字节返回。
2. 分段层：会话仍在就 `list_parts` 逐段核对编号/size/etag，缺啥补啥；会话已消失则重新 `begin`。
3. 查 `receipt(delivery_id)`：存在且 key/digest/size 与本次交付一致 → 认领该版本（`receipt_on_resume`）。
4. 无回执再查 `head(key)`：仅当成品绑定的 delivery、整对象 SHA256、长度三者全匹配才认领；
   不同 delivery 同 key 或 generation 已变化 → 退出码 11 报告冲突，不覆盖。
5. 回执与成品都不存在 → 证明此前从未提交成功，按原 `expected_generation` 重新 complete。

`list_uploads/list_parts` 只反映暂存状态，**不证明成品成功**；成品身份只采信回执或 head/read。

### 4. publish —— 发布 catalog（与对象完成是两个独立步骤）

- 发布前按 `object_version` 独立 `read` 整对象，核对 SHA256/长度，并核对服务回执版本一致，否则拒绝。
- catalog 为追加式 JSON：条目把 delivery 绑定到**不可变 `object_version` 与源摘要**；
  读者按 version 读，不按可变 key 猜当前。同 key 的新交付追加新条目，旧条目与旧 version 原样保留、不回收。
- 写 catalog 用 tmp + fsync + 原子 rename + 目录 fsync：rename 成功前旧 catalog 一直可读；
  中断后重跑 publish 发布的是**同一版本**，不重新上传、不重复 complete；同 delivery+同版本幂等。

### 5. verify —— 独立读回校验（不只比对 etag 字符串）

按 catalog 条目里的不可变 version 从服务读回**全部字节**，独立校验：

- 长度 == catalog/计划长度；整对象 SHA256 == catalog/计划摘要；
- 用 `csv` 模块解析读回字节与本地源文件，逐行比较；
- 同时打印 key 当前 head 版本与 catalog 版本，证明即使当前代已被新交付替换，旧 version 仍不可变可读。

退出码：0 成功；1 校验失败；2 参数错误；3 complete 未知待 resume；4 delivery 内容冲突；
10 generation 冲突；11 发现他人对象/拒绝覆盖；12 本地状态漂移。

## 恢复策略：持久交付回执 + 列举核验（固定重传，无随机退避）

未知状态如何收敛（决策表）：

| 观察到的状态 | 收敛动作 | 是否新建对象 |
| --- | --- | --- |
| 回执存在且身份匹配 | 直接认领既有 version | 否 |
| 无回执，head 绑定本 delivery 且摘要/长度匹配 | 认领 head 指向版本 | 否 |
| 无回执，head 是别的 delivery/内容或代已变 | 报告冲突，停 | 否（绝不覆盖） |
| 无回执且无成品，但暂存段齐全 | 用原 expected_generation 重新 complete | 是（此前确未提交成功） |
| 暂存段缺失/会话消失 | list_parts 核验后只重传缺失段，再 complete | 仅补缺失段 |
| 同一 delivery 已完成后任何重试 | 服务 complete 幂等返回旧回执 | 否 |

**实际额外传输量**（演练实测口径，对象字节而非元数据调用）：

- 传 part 中途硬杀：只重传缺失段。S2 实测 3 段已确认，恢复仅补 472 字节（= 文件 664B − 3×64B），无整文件重传。
- complete 提交前失败：分段全部已在服务暂存，resume 额外对象字节 **0**，仅一次 complete 调用。
- complete 提交后响应丢失：回执已持久，resume 额外对象字节 **0**，对象版本数保持 1。
- catalog 写入前中断：对象侧额外字节 **0**，仅重做本地原子写。
- 同 delivery 同内容任意重试：额外对象字节 **0**，全生命周期只有一个可发布版本。

## 故障注入（仅演练用，环境变量，单次进程生效）

| 变量 | 效果 |
| --- | --- |
| `DELIVERY_KILL_AFTER_PART=N` | 第 N 段确认并落盘后 `os._exit(137)`（模拟 kill -9） |
| `DELIVERY_FAULT_COMPLETE=before` | 服务在 complete 提交前抛 `OSError(BEFORE_COMMIT)` |
| `DELIVERY_FAULT_COMPLETE=after` | 服务已提交新版本后抛 `OSError(RESPONSE_LOST)` |
| `DELIVERY_CRASH_BEFORE_CATALOG_RENAME=1` | catalog tmp 落盘后、rename 前硬退出 |

## 完整运行示例

### 手工五连（参数取自 config.json，源为 source.csv）

```bash
python3.11 delivery_tool.py --state-dir run/state --store-root run/store \
  plan --csv source.csv --delivery delivery-a --key reports/result.csv \
  --part-bytes 64 --expected-generation 0
# part_count=8, last_part_length=35

python3.11 delivery_tool.py --state-dir run/state --store-root run/store upload
# parts_sent=8, bytes_sent_run=483, object_version="reports/result.csv@1"

python3.11 delivery_tool.py --state-dir run/state --store-root run/store \
  publish --catalog run/publish/catalog.json
# catalog 追加条目，绑定 object_version=reports/result.csv@1 与源 SHA256

python3.11 delivery_tool.py --state-dir run/state --store-root run/store \
  verify --catalog run/publish/catalog.json
# length_ok / sha256_ok / csv_parse_ok 全 true，csv_rows=49

python3.11 delivery_tool.py --state-dir run/state --store-root run/store resume
# "交付早已完成，版本身份复核一致（幂等 resume，零额外对象字节）"
```

### 中断恢复示例

```bash
# 传到第 3 段后进程被硬杀
DELIVERY_KILL_AFTER_PART=3 python3.11 delivery_tool.py \
  --state-dir run/state --store-root run/store upload        # 退出码 137

# 新进程恢复：list_parts 核对 3 段 etag/size 后只补传剩余段
python3.11 delivery_tool.py --state-dir run/state --store-root run/store resume

# complete 提交前失败 → 退出码 3（unknown），再 resume 即零对象字节收敛
DELIVERY_FAULT_COMPLETE=before python3.11 delivery_tool.py \
  --state-dir run/state --store-root run/store upload
python3.11 delivery_tool.py --state-dir run/state --store-root run/store resume

# 同 key 第二代交付：必须明确给出上一代
python3.11 delivery_tool.py --state-dir run/state2 --store-root run/store \
  plan --csv v2.csv --delivery delivery-b --key reports/result.csv \
  --part-bytes 128 --expected-generation 1
```

### 一键全场景演练

```bash
python3.11 run_demo.py
```

覆盖 S1–S10：正常分段、part 后硬中断恢复、complete 提交前失败、提交后响应丢失、
catalog 写入前中断、两个 delivery 竞争同 key（预检 + 服务权威 CAS + 显式替换 + 旧版本保留）、
同 delivery 同内容幂等、同 delivery 不同内容被工具与服务双层拒绝、16KiB/256 段边界、
暂存 upload 的所有权清理纪律。当前结果：**31 项断言全部 PASS**。

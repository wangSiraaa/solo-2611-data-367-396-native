# 双副本 SQLite 因果行复制

两个独立 SQLite 副本（A/B）离线编辑同一业务表 `items(id,name,qty)`，通过
**实际事件文件**交换变更并持续收敛。实现的不是"最后到达覆盖"，也不是把收到
的变更当本地新写再发回去。

## 快速开始

```bash
# 1. 复制 seed（原件只读），给工作副本补复制表
cp seed-A.sqlite A.sqlite
cp seed-B.sqlite B.sqlite
python3 replicate.py prepare --db A.sqlite
python3 replicate.py prepare --db B.sqlite

# 2. 本地写：只在无冲突且 head 完全匹配时成功
python3 replicate.py write --db A.sqlite r1 \
    --state '{"name":"Alice","qty":10}' --expected-heads seed:1

# 3. 导出真实事件文件，再由另一端接收
python3 replicate.py export-events --db A.sqlite --out a.events.json
python3 replicate.py receive --db B.sqlite a.events.json

# 4. 查询与导出
python3 replicate.py query-row --db B.sqlite r1
python3 replicate.py export-data --db B.sqlite
python3 replicate.py audit --db B.sqlite
```

`replicate.py --help`；`write`/`delete`/`resolve` 都接受
`--expected-heads ID[,ID]`（或 `--expected-heads-json` / `--request FILE`）。
退出码：`0` 成功，`1` 用法错误，`2` 本地冲突（含陈旧 head / 多 head），
`3` 严格模式下批内出现拒绝项。

## 语义要点

* **不可变版本日志**：每条版本保存完整行值或墓碑；远端版本保留原
  `origin/seq/version_id`，不分配本地 seq，不进入本地 outbox。
* **当前 head**：同一行 ready 版本中不被其它 ready 版本祖先覆盖的前沿；
  多个互不支配 head 即 `conflicted`，每个分支的完整值都保留。
* **本地写**：`BEGIN IMMEDIATE` 内校验 `expected_heads == 当前 head` 且
  head ≤ 1；新版本、业务效果、outbox、本地 seq 共同提交或回滚。并发用同一
  旧 head 写，只有一个成功，其余明确 `stale_expected_heads`。
* **接收**：乱序/重复/分批安全；缺 parents 先 `pending` 并报告具体缺依赖；
  父版本后到自动重判。拒绝项保留依据且不影响同批其它项。事件应用、pending/
  ready、前沿、业务/冲突投影在同一事务发布。
* **resolve**：必须显式给出完整行或墓碑并精确绑定当前全部 head；head 变化
  后旧裁决被拒绝。resolution 作为本地新事件出站，先于 parent 到达时仍是
  pending。
* **query-row** 区分 `absent`/`resolved`/`deleted`/`conflicted`；
  **export-data** 只导出正常行，所有被排除行都带状态解释，冲突行附每个
  分支的完整值/墓碑。
* **audit** 记录真实 SQL 写入计数、每个版本的原 origin、outbox 数、接收
  依据（claim/inbox）与 head 变化，并用独立事件图解释器交叉核对。

## 验证

```bash
python3 -m unittest discover -s tests -v   # 30 个用例
python3 run_scenario.py                    # scenario.json 全流程 + 收敛断言
python3 tools/trace_sql.py                 # 真实 SQL 语句转录
```

`run_scenario.py` 只通过 `export-events` 产出的文件在 A/B 之间传输，从不读取
对端数据库代替协议；结束时断言两副本 `export-data` 与 heads 完全一致，并输出
`run/scenario-report.json`。

设计、事务边界、回声/部分发布/冲突丢失的论证及备选方案比较见
[`DESIGN.md`](DESIGN.md)。

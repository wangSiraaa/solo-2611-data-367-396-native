# 受限图路径取数工具 — 设计与交付说明

## 1. 边界与合规

候选代码只通过 `GraphAPI.capabilities()` 与 `GraphAPI.query_edges(request, token)`
两个公开接口取数。全包检索可验证合规性：

```bash
grep -rn "_rows\|make_source\|private/edges" path_tool/ demo.py
# 仅 path_tool/oracle.py 出现 private 文件读取，而它是独立验收链路，
# 候选执行（executor/engine/planner/enumerator）从不导入它。
```

- 不读取私有边文件、不访问 `_rows`、不调用 `make_source.generate`、不做全量镜像；
- `capabilities()` 是元数据调用，不计边查询预算；
- 成熟图库本环境不可用（无 networkx），且图规模 ≤2000 边，自写邻接枚举即可，
  图库只能用于已取边组合，不能替代来源协议与完整性判断。

## 2. 模块与四个入口

| 模块 | 职责 |
|---|---|
| `path_tool/engine.py` | 所有 API 访问的唯一出口：snapshot 绑定、分页 token、预算、真实字节计量、请求日志 |
| `path_tool/planner.py` | 仅依据 capabilities 的静态规划：逐跳能力、可行切分点、target_filter 可行性 |
| `path_tool/executor.py` | 单探针自适应、纯前向/纯后向、强制切分点三种物化策略，边池只收完整请求 |
| `path_tool/enumerator.py` | 已取边上的标签序列匹配、简单路径检查、平行边保留、排序去重 |
| `path_tool/explain.py` | `explain` 人类可读解释；`audit` 公开轨迹自审 |
| `path_tool/oracle.py` | 独立验收：读私有图穷举全部简单路径，与候选结果逐路径对账（非候选链路） |
| `path_tool/cli.py` | `plan / execute / explain / audit` 四个命令行入口 |

```bash
python3 -m path_tool.cli plan    queries.json [query_id]
python3 -m path_tool.cli execute queries.json [query_id] [--strategy ...]
python3 -m path_tool.cli explain queries.json [query_id]
python3 -m path_tool.cli audit   queries.json [query_id] [--private private/edges.json]
python3 demo.py                # 端到端演示 + 真实数字的方案对比
```

## 3. 来源访问协议的落实

### 3.1 Snapshot 绑定
`FetchEngine` 初始化时读取一次 `capabilities()` 并绑定 `snapshot_id`；之后每个
请求都用该绑定值构造，响应 snapshot 不一致即 `ERROR`。来源不可变，所有日志条目
回查时都带同一 snapshot。

### 3.2 分页 token
token 同时绑定 snapshot 与完整请求（方向/label/keys/target_keys）。引擎只在
“同一完整请求”的连续翻页中沿用 `next_token`；前沿或过滤条件一变就构造新请求、
token 置 `None`，绝不复用旧 token（服务端的 `TOKEN_QUERY_MISMATCH` 负例测试覆盖）。
**一个请求只有读到 `next_token is null` 才标记取全**；只读第一页不证明取全。

### 3.3 能力驱动，不支持的方向不走
对每跳记录 out/in/target_filter 能力。切分点 `s` 可行当且仅当前缀标签全部支持
out、后缀标签全部支持 in；无可行切分点直接 `ERROR`，不发任何边请求、不伪造答案。
`target_keys` 只在 out 且能力声明 `target_filter` 时使用，且集合 ≤4；超过 4 个
相遇节点时降级为不带 target 的普通请求（测试覆盖）。in 方向永远不带 target_keys。

### 3.4 键分批
每请求 keys 1–4 个且不重复；任意大小前沿按 `max_keys=4` 分批，逐批完整翻页。

## 4. 自适应取数计划（实际采用）

1. **静态选向**：首个标签支持 out 且带 target_filter 时优先前向（末跳可挂终点
   约束）；否则末标签支持 in 时优先后向。
2. **单探针**：只对首选侧首跳发**一个**请求，先取第一页，立即得到服务端真实
   `total_matches`（该请求的候选数，不是路径总数）。
3. **至多一次换边**：当真实候选数 `> 边预算/2` 且对侧首跳能力可行时换边。
   被放弃的探针**不续页**，只有第一页计费，日志标记 `abandoned`，其边绝不进入
   组合池；换边后不回头，避免反复试探。
4. **不换边则续页取全**，探针结果直接成为该侧首跳物化。
5. 后续按可行切分点继续：小探针（选择性好）优先纯前向/纯后向一路到底；大探针
   选最早相遇的切分点，让对侧约束尽早收窄。

关键设计：探针只取一页就决策，而不是把整个请求翻完——否则 reverse-selective 的
200 边探针会在决策前就撑爆 40 边预算。

## 5. 完整性如何被保证（三个状态协作）

1. **来源分页完整性**：进入组合边池的每个逻辑请求都翻到 `next_token is null`。
   abandoned 探针不参与组合，其残留 `has_more` 不影响答案，但调用与边照计预算。
2. **物化不做简单性剪枝**：维护“累积可达节点集”作为请求 keys。一条回到早先节点
   的边（循环）会让该节点在新深度再次需要扩展；若只保留上一跳前沿就会漏掉该节点
   其他出边上的合法路径（本仓库曾测出该缺陷并修复，见 cycle 测试）。简单路径
   判断只在枚举器做。
3. **组合完整性**：仅当全部所需请求完整完成（`status==OK`）才枚举并置
   `complete=true`。先找到低 cost 路径、某页为空、达到预算都不能提前宣布完整。

预算终态：
- 发请求前剩余调用数为 0 → `INCOMPLETE`（不发布前缀为 complete）；
- 实际响应使累计返回边超过边预算 → `BUDGET_EXHAUSTED`，**先如实记账再终止**，
  超额记录保留，不谎报未超预算、不丢弃后再报正常；
- 协议错误（snapshot/方向/token/键）→ `ERROR`。

## 6. 路径语义

- 严格匹配 1–4 跳固定标签序列；节点不重复（simple path），回到起点的伪路径在
  枚举器被拒；
- 平行边（同 src/dst/label、不同 edge_id）各自产生独立路径；
- 同一 edge_id 经多个合法请求取得，边池按 edge_id 只存一条，不会复制成额外路径；
- 负 cost 合法；输出按 `(cost 总和, edge_id 序列)` 排序，返回全部路径而非布尔或
  单条最短。

## 7. 计量口径

- `calls_used`：真实 `query_edges` 调用数（含重扫、重复页、abandoned 探针）；
- `edges_returned`：累计返回边记录数，重复传输照计；
- `request_bytes` / `edge_payload_bytes`：与服务端 `audit` 同口径的真实编码字节，
  自审与服务端审计交叉校验；不把服务端扫描数当网络传输；
- 预算只统计边查询，capabilities 元数据调用不计。

## 8. 两个基准的真实结果（全新实例 + 空缓存）

| 查询 | 终态 | 调用 | 返回边 | 路径 | oracle |
|---|---|---|---|---|---|
| forward-selective | OK | 3 | 4 | 2 | 一致 |
| reverse-selective | OK | 4 | 28（含 1 个 abandoned 探针 25 边） | 1 | 一致 |
| cycle-rejected-path | OK | 3 | 3 | 0 | 一致 |

均在 12 调用 / 40 返回边内完整，逐路径与独立全图 oracle 的身份、顺序、cost、
节点序列完全一致。

## 9. 替代方案的真实差异（非估计，来自 `demo.py`）

**forward-selective**
- 自适应（采用）：3 调用 / 4 返回边，完整；
- 强制 split=1 相遇：首个 `located/in keys=[zF]` 服务端真实 `total_matches=301`，
  翻两页 50 边后即 `BUDGET_EXHAUSTED`（2 调用 / 50 边），无法完成。

**reverse-selective**
- 自适应（采用）：4 调用 / 28 返回边，完整（探针 25 边 abandoned + 3 边物化）；
- 纯前向：首层 `owns/out keys=[sB]` 真实候选 200，两页 50 边后
  `BUDGET_EXHAUSTED`（2 调用 / 50 边），无法完成。

差异由能力与**真实候选数量**共同决定，不是规划器估计值。

## 10. 测试

`cd tests && python3 -m unittest test_path_tool`（34 个用例）覆盖：
两个基准硬预算与 oracle 对账、全新实例/空缓存隔离、多页翻页、平行边、负 cost、
循环节点拒绝、无结果、多起终点、>4 前沿分批、>4 target 降级、4 跳、unsupported
方向、无可行切分点、target_filter 能力负例、token 换请求/snapshot 错配、
snapshot 篡改、空预算（INCOMPLETE）、零边预算与超额（BUDGET_EXHAUSTED 保记录）、
中途调用预算耗尽不发布完整答案、重复请求不复制路径、方案真实差异、explain/audit。

## 11. 明确不做

在线边修订、通用正则路径、图数据库写入、分布式共识、Web 界面、墙钟时限优化；
也不承诺任意输入都有预算内方案——预算不足时给 `INCOMPLETE`/`BUDGET_EXHAUSTED`
而不是伪装完整。一次成功标记对应且仅对应一份完整答案。

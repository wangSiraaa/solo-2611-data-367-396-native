# DESIGN — 可追溯近重复语料去重存储

`dedupstore.py` 为已分词文档维护候选索引、精确相似边、连通簇与代表样本，
对外提供 `build / put / remove / export / explain`。主要产物是可追溯去重语料
（`export.json`），不训练相似模型，不做语义相似、主题聚类、分词或代表文本拼接。

## 1. 数据模型与判定规则

- 文档：稳定 `id`（非空字符串）、`tokens`（字符串列表，**不做任何隐含规范化**）、
  整数 `quality`。
- profile：固定二元 shingle —— 相邻 token 对的**集合**（重复出现不重复加权），
  以 JSON 数组串确定性编码存库。少于 2 个 token 的文档 shingle 集为空。
- 阈值：正有理数 `p/q ∈ [1/5, 1]`，建库时固定（材料 profile.json 为 1/4）。
- 精确判定（无浮点）：
  - 两空集：相似（空桶规则）；恰一个空集：不相似；
  - 否则边存在 ⟺ `inter*q >= uni*p`（整数交叉乘法，恰等阈值判相似）。
- 相似边无向；簇 = 边图的连通分量。**不宣称**簇内任意两篇都达阈值；
  export 中每篇折叠文档给出到代表的**真实相似边路径**，间接相似不伪装成直接边。
- 代表：`quality` 最高；并列取 UTF-8 字节序最小 id（SQLite TEXT 的 BINARY 排序
  即 UTF-8 字节序；Python 侧显式以 `id.encode("utf-8")` 比较，两者一致）。

## 2. 候选完备性（不漏真实边）

设新文档 A 的集合大小为 `x`，阈值 `t = p/q`。对任意文档 B（大小 `y`）：

1. **大小过滤**：`J(A,B) ≤ min(x,y)/max(x,y)`，故相似的必要条件是
   `y ∈ [ceil(p*x/q), floor(x*q/p)]`（整数计算）。
2. **前缀子集**：相似时 `|A∩B| ≥ t(x+y)/(1+t) ≥ t·x`（由 `y ≥ t·x`），
   即 `|A∩B| ≥ ceil(p*x/q)`。于是 A 的**任意** `x - ceil(p*x/q) + 1` 元子集
   都与 B 相交：子集外只有 `ceil(p*x/q)-1` 个元素，装不下全部交集。
3. 因此：用 A 的任意该大小的 shingle 子集探测**完备倒排**（含每篇文档全部
   shingle），加上大小过滤，必然找到 B。∎

实现选择 **当前 df 最小（最稀有）** 的 k 个 shingle 作为探测键（并列按 shingle
字节序）。注意完备性由"任意子集"保证，**不依赖全局序一致**，因此 df 可随时变化、
无需维护存储化前缀索引或定期重建；稀有度只为选择性。热门 shingle 因 df 最高而
系统性避开探测键（见 demo_perf.py：200 篇共享热门 shingle 的文档产生 0 候选）；
当集合小到探测键覆盖全集时（test_dedup.py::TestHotToken），热门 posting 被完整
流式扫描，计数器给出精确断言。

**空桶**：`x = 0` 时候选为全部 `set_size = 0` 的文档（两空集相似），逐个按规则
判定并计入 `exact_comparisons`；非空文档永远不以空文档为候选（恰一个空集不相似）。

## 3. 存储与流式（本机 SQLite）

表：`docs` / `postings`（完备倒排，PK `(shingle, doc_id)`，另建 `(doc_id, shingle)`
索引）/ `shingle_df` / `edges`（含 `inter, uni, via, found_by`）/ `membership` /
`versions`（每次发布的完整 JSON 载荷）/ `meta`（阈值、版本、计数器）。

内存预算（题定 ≤ 10000 个文档 ID 同时展开）：

- posting 与空桶均以游标 `fetchmany(1000)` **流式**读取，候选随到随判，
  从不把整条 posting 装成列表；
- 候选去重 `seen`、BFS `frontier/visited` 都是单分量内 doc_id 集合，
  大小 ≤ 语料上限 5000 < 10000，天然在预算内；
- 单篇文档的 shingle 集驻留内存（题定豁免，已披露）；export 结果流不计入。

## 4. 簇维护：受影响分量重算（支持拆分）

不使用只支持合并的 union-find —— 桥点删除后它会留下错误大簇。每次增删：

1. 记录变更前受影响分量（目标文档旧分量、新邻居所在分量）的成员与代表；
2. 应用边差分（修订先**撤回旧 shingle/旧边**再建新边）；
3. 以"旧分量全体成员 ∪ 新邻居 ∪ 目标文档"为种子，在**当前边图**上 BFS，
   得到重算后的若干连通分量 —— 合并与拆分自然显现；
4. 成员集不变的旧分量**保留原 cluster_id**；变化的分配新 id，并发布事件：
   `formed / grown / merged / split / dissolved / representative_changed`
   （含 `from_clusters` 溯源）。代表按当前成员重算，变化一并发布。

相同内容（shingle 集与 quality 均同）重放：不发布任何变化。仅 quality 变化：
不动边，只重判所在簇代表。`remove` 未知 id：抛 `UnknownDocError`，明确拒绝，
不产生版本。

## 5. 版本发布与计数器

每次有效增删 = 一个完整版本（单事务提交），载荷含：边增删（带精确交/并集计数
与候选来源）、分量事件、代表变化、计数器差分与累计。`export` 在单事务内读取
当前一致版本；历史版本的事件序列存于 `versions` 表可查。

计数器（实际执行计数，不以计划估计替代）：

- `candidate_visits`：候选生成中实际检查的 posting 行数（大小过滤后、去重前）；
- `candidates_unique`：去重后进入精确判定的候选数；
- `exact_comparisons`：实际精确相似判定次数（含空桶规则判定）。

## 6. export / explain

`export` 产出独立 JSON（`near-dedup-export/1`）：版本、profile、阈值、汇总、
每簇的代表与成员（`kept_representative` / `collapsed`）、每篇折叠文档到代表的
**相似边路径**（逐边带精确交/并集计数；0/0 标注空集规则）、实际计数器。
可脱离内部数据库读回，保留全部来源 id 与路径。

`explain(doc_id)`：当前探测键（稀有前缀、大小窗口或空桶）、每条相似边的候选
来源（`via` shingle / `empty-bucket`、`found_by`）与精确交/并集计数、所属簇、
代表与到代表的路径。

## 7. 备选方案与取舍

- **候选索引**：① 完备倒排 + 稀有子集探测（本实现）：无需维护前缀索引、df 可变、
  删除零成本；代价是低阈值下探测键较多（t=1/5 时约 80% 的 shingle）。② 存储化
  前缀索引（固定全局序）：探测更省，但 df 漂移需定期重建前缀，删除/修订维护复杂。
  ③ 概率 LSH：只能作加速辅助，其漏召回结果不能作为完整簇 —— 本实现不采用。
- **删除拆分**：① 受影响分量 BFS 重算（本实现）：成本限于受影响区域，精确支持
  拆分；② union-find：只支持合并，桥点删除后留下错误大簇，不满足要求；
  ③ 动态连通性结构（如 Euler-tour tree）：渐近更优但实现复杂，5000 篇规模无必要。
- **热门 posting**：流式游标 + 大小过滤在 SQL 内完成，候选增量处理；备选是把
  整条 posting 物化后批量判定 —— 违反 ID 预算，不采用。
- **额外存储**：`edges` 冗余 `inter/uni/via` 与 `versions` 全量事件日志，用磁盘
  （不设紧预算）换 export/explain 的零重算可追溯；备选是导出时重算计数，省空间
  但丧失"实际发生"的计数语义。

## 8. 正确性验证与性能

- `test_dedup.py`：材料场景（桥点、重复文档、独立簇）逐版本对 probe.py；
  参数化参考 `reference.py` 与 probe.py 在随机输入上交叉验证；空/短文档、
  热门 token、阈值恰相等、quality 并列（含非 ASCII id）、桥点修订与删除、
  重放、未知 id 拒绝；4 个阈值下共 720 步随机增删改，**每步**与独立成对参考
  逐项比对边/分量/代表，并校验发布载荷的边差分精确解释状态变化。
- `demo_perf.py`：5000 篇稀疏可复现实例（种子固定），阈值 1/5。实测
  精确比较 203 次 vs 全量两两 12,497,500 次（0.0016%），200 个植入相似对
  全部召回，300 篇抽验子集与成对参考一致。不承诺高度重复语料也有此节省。

## 9. 运行

```bash
python3 -m unittest test_dedup -v     # 正确性
python3 run_materials.py              # 材料 → export.json（逐版本对 probe.py）
python3 demo_perf.py                  # 稀疏性能实例
python3 dedupstore.py --help          # CLI：build/put/remove/export/explain/stats/versions
```

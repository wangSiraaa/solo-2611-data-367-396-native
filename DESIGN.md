# 本地文档检索索引 — 设计说明

Python 3.11，零第三方依赖（仅标准库 `json/os/threading/fcntl/dataclasses`）。
分词规则与文档身份语义全部按题面手写，底层不使用任何 FTS/分词库默认语义。

## 1. 目录与模块

```
docidx/
  analyzer.py   码点扫描分词、casefold、停词、UTF-8 字节坐标
  storage.py    不可变段、原子提交清单、读视图租约的文件布局
  engine.py     写路径（put/delete）、Snapshot 读视图、短语/集合查询、compact、锁
  oracle.py     独立逐文档扫描器（不碰倒排），用于 audit 对账
  cli.py        build / apply / query / compact / reap / audit
verify.py       端到端验证（59 项断言 + 40 轮随机 fuzz + 跨进程租约）
```

索引目录布局（全部是本机普通文件）：

```
<root>/manifest.json            提交清单，os.replace 原子切换
<root>/segments/seg_<id>.json   不可变段，写一次后永不修改
<root>/leases/lease_<pid>_<n>   读视图租约
<root>/.write.lock              fcntl 排他写锁
```

## 2. 分析规则（analyzer.py）

- 按 **Unicode 码点** 扫描：连续满足 `ch.isalpha() or ch.isdigit()` 的码点构成一个词；
  标点、空格不产生词，也**不占 position**。
- `term = word.casefold()`；停词表同样先 casefold 再比较。
- `position` 是词在**该数组元素内**的序号，停词参与编号；停词只是不进倒排，
  后面词的 position 不会被压缩（`red the blue` → red=0、the=1、blue=2）。
- 每个词同时记录其在该字段值 **UTF-8 编码上的半开字节区间 `[start,end)`**，
  命中后用该区间直接从原文字节切片，因此 `café` 等非 ASCII 词也能精确回到原文。
- 源码字节与原文绝不改写：索引只保存坐标，文档原文存在段的 `docs` 区，
  `verify.py` 对输入对象做了原样断言。

## 3. 位置/值边界编码（题面要求说明之一）

posting 在段内是定长数组：

```
[doc_id, revision, field, value_no, position, byte_start, byte_end]
```

- `value_no` 是字段数组的下标，与 `field` 一起作为值边界写进每条 posting。
- 不同数组元素允许出现相同 position（两个值都可以有 position=0）。
  短语查询先按 `(doc_id, value_no)` **分组**，匹配只在组内进行，
  因此跨两个数组元素永远拼不出短语——这不是靠位置唯一，而是靠显式值边界。
- 距离判定在查询期显式完成：相邻命中要求 `1 <= pos[i+1]-pos[i] <= max_gap+1`。
  停词留下的位置空洞因此自然生效，无需任何特殊邻近操作符。
- **重复 term**：DFS 回溯时用 `used_pos` 与 `used_span` 两个集合保证同一出现
  （同一 position、同一字节区间）不能同时匹配查询中的两个重复词；
  `x x x` 对 `["x","x"]` 枚举出 `[0,1]`、`[1,2]` 两个组合而不会出现 `[0,0]`。
  值边界按组键天然隔离，重复词连接也不会跨值。

## 4. 不可变段、版本查找与文档身份

- 每次 `put`/`delete` 生成一个**新的不可变段**（delete 段只有墓碑记录、无 posting），
  revision 按 doc 单调递增，由清单 `next_rev` 维护；重开索引后继续递增。
- 清单 `active[doc_id] = {revision, op: put|delete, seg}` 是唯一的身份真相：
  - 读路径对每条 posting 显式检查 `active[doc].revision == posting.revision`
    且 op 为 put，旧 revision 与墓碑一律过滤；
  - 同 doc 多 revision 只有最后**提交**的版本可查，旧词不可能命中或复活；
  - `NOT` 的全集取 `active` 中 op=put 的文档（当前活跃集合），
    与历史 posting 里出现过哪些 doc 完全无关。
- `put` 是完整替换：新段只包含新文档的 posting，旧段保持不可变但已被清单判旧。
- 段与清单都走“临时文件 + `os.replace`”发布：读者要么读到完整旧清单，
  要么读到完整新清单，不存在半合并/半写入可见性。

## 5. compact 与读视图寿命（题面要求说明之二）

`Snapshot.snapshot()` 的固定顺序：

1. **先**在 `leases/` 登记租约文件；
2. 读清单并把清单内所有段整体加载进内存。

compact 流程（持写锁）：

1. 把清单内全部段读入，仅保留 `active` 中当前 put 版本的 posting，
   旧 revision/墓碑数据物理丢弃，写入一个新段；
2. `os.replace` 切换清单到只含新段的新一代（generation+1）；
3. 回收旧段文件时检查租约：存在在册租约（本进程集合 + 租约文件且属主进程存活）
   则保留，等视图 `close()` 后由下一次 `reap`/compact 清理。

影响：

- 旧读视图持有的是清单快照 + 段内存副本，且其租约阻止旧文件被删，
  所以合并期间旧查询照常完成，答案与合并前逐字节一致；
- 新查询在清单切换后只会看到合并后的单一段，不会重复命中
  （去重在合并期按完整 posting 键再做一遍防御性去重）；
- 租约是显式资源：`Snapshot` 支持上下文管理，`close()` 释放；
  属主进程已死的租约按过期处理，不影响后续回收。
- 单段时 compact 为 no-op。

并发模型：单写者（进程内 `threading.Lock` + 跨进程 `fcntl.flock` 排他锁），
读路径不加写锁，支持一个并发读者（题面约束）。不实现掉电恢复。

## 6. 查询形态与拒绝规则

- 短语：`{field, terms:[>=2 个非停词], max_gap:>=0}`，返回**所有**位置组合，
  每条含 `doc_id/revision/field/value_no/positions/byte_spans/evidence`。
- 单词：`{field, word}`，返回文档集合及全部出现（含字节证据）。
- 集合：`{field, and|or|not: [...]}`，叶子是单词节点；`not` 全集=当前活跃文档。
- 明确拒绝（`QueryError`，CLI 退出码 2）：未知 field、任何停词 term、
  短语少于两个 term、负/非整数 max_gap、无法识别的节点形态。
- 查询 term 同样做 casefold，因此 `ALPHA` 与 `alpha` 等价。

## 7. Oracle 与 audit

`oracle.py` 不读倒排：直接对当前活跃文档逐文档、逐数组元素重新分词，
用独立的有序回溯实现短语、独立实现 AND/OR/NOT。`audit` 对每个查询比较
`(doc_id, value_no, positions, byte_spans, evidence)` 五元组签名，
而不是只比命中数；`verify.py` 的 40 轮 fuzz（随机文档、停词集合、
put/delete 顺序、穿插 compact）全程用它对账。

## 8. CLI 用法

```
python3.11 -m docidx.cli build   --root IDX --docs docs.jsonl   --analyzer analyzer.json
python3.11 -m docidx.cli apply   --root IDX --updates updates.jsonl
python3.11 -m docidx.cli query   --root IDX --queries queries.json
python3.11 -m docidx.cli compact --root IDX
python3.11 -m docidx.cli reap    --root IDX
python3.11 -m docidx.cli audit   --root IDX --queries queries.json
python3.11 verify.py        # 全量验证
```

规模约束内（200 文档、每字段 4 值、每值 200 词、10 段）短语 DFS 只在
同值候选组内回溯，且按位置排序后可提前剪枝。

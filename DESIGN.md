# 本地列级血缘与变更影响工具 — 设计说明

## 1. 目标与边界

输入 `catalog.json`（基础表 schema + 标量函数依赖声明）与 `views/*.sql`
（每个文件一条 SQLite 兼容 SELECT），输出一个可查询的持久目录，回答：

> 某个基础列变化后，哪些报表**单元格的值**可能变、哪些只影响**行是否存在/
> 分组/连接配对**、哪些因为不透明函数而**暂时无法确定**、哪些可以精确判定
> **不受影响**。

明确不做：运行期任务血缘、执行计划/成本、权限策略、任意 SQL 支持。

### 支持的 SQL 子集

非递归 WITH、表/视图别名、限定与非限定列名、`*` 与 `alias.*`、
INNER/LEFT JOIN（ON 或 USING）、WHERE、GROUP BY、SUM/COUNT（含
COUNT(*)、COUNT(x)、COUNT(DISTINCT x)）、CASE、`+ - * / % ||`、
catalog 登记的确定性标量函数（另有一份小型内置确定性白名单，如
`abs/coalesce/substr/length/...`）。其余构造（HAVING、ORDER BY、LIMIT、
裸 DISTINCT、窗口、UNION、递归 CTE、逗号/CROSS JOIN、CAST 等）一律拒绝并
产生位置化失败报告，不猜测来源。

## 2. 中间表示：作用域 + 关系算子图

解析库使用 **sqlglot（SQLite 方言）**，只承担语法解析与 AST/位置信息；
跨视图解析、作用域隔离与依赖分类全部由本工具维护。

一条 SELECT 被自底向上归约为一棵小型关系算子树，每个算子产出
`RelOutput(columns, row_control, bindings)`：

| 算子 | 作用 |
| --- | --- |
| table scan | 每个表列产生一个 `value` 依赖（hop 记录 FROM 别名角色） |
| view/CTE scan | 深拷贝上游关系并为每列加一个入口 hop，保留完整解释链 |
| join | 计算连接控制依赖；LEFT JOIN 右列额外标注空侧/行乘法 |
| filter (WHERE) | 谓词列成为所有输出列的 `filter` 控制依赖 |
| group/aggregate | 分组键为 `group_key` 控制；SUM 值依赖、COUNT(*) 无值依赖 |
| projection | 星号展开、别名、标量表达式；重复输出名直接拒绝 |

每个输出列携带三样东西：

* `value_deps` — 基础列的值变化可能改变该输出单元格；
* `control_deps` — 基础列可能决定行存在、连接配对、分组或 CASE 选值；
* `unknown` — 依赖路径上出现了 catalog 声明为 `unknown` 的不透明函数。

依赖以 `Dep(kind, base, reason, path, base_alias)` 表示，`path` 是一串
`Hop`：`视图.输出列 -> 经对象/别名 -> 输入列`。去重键包含完整 hop 链与
别名角色，因此**同一张表以两个不同别名自联结时，两个角色不会被合并**；
reason 不进键，同一等价边上的多个分类原因（如 `filter+join_pairing`）会
被合并而非重复存储。

### 为什么能准确展开星号

`*` / `alias.*` 从不按文本猜测，而是遍历当前作用域中**已解析的绑定**
（FROM 别名 → RelOutput 的列集合）逐列复制，连同每列已有的值/控制依赖、
LEFT JOIN 空侧标记一起继承。绑定里没有的列不可能出现在展开结果中。

### 为什么能隔离作用域

* 每个 SELECT/子查询有独立的 `bindings` 列表，列解析
  `_resolve_column` 只在可见绑定内进行；
* CTE 名按 SQLite 规则遮蔽外层同名对象（夹具 `v_cte_shadow` 覆盖），
  CTE 体只在首次引用时分析一次并深拷贝给各扫描点；
* 非限定名命中多个绑定即 `ambiguous_column` 拒绝（USING 合并键例外，
  见下）；限定名找不到即 `unknown_column`；
* 视图/CTE 扫描返回**深拷贝**（`_copy_rel` / 每列新建 ResolvedColumn），
  同一上游被多个别名或多个下游扫描时不会互相污染、不会重复前缀 hop。

### USING 与 LEFT JOIN 空侧

* USING 的合并键在输出中只出现一次，值依赖同时来自左右两侧；合并后对该
  名字的非限定引用唯一解析到合并列（而不是误报歧义）；
* LEFT JOIN 右侧每列附加 `left_null_side` 控制依赖并登记
  `nullable_sources`：即使该列从未被投影，右表 ON 列仍能影响右侧 NULL
  扩展与行乘法；INNER JOIN 中未投影的右表 ON 列同样作为
  `join_pairing` 控制源保留（夹具 `v_ononly` 覆盖）。

## 3. 值/控制依赖分类规则（保守“可能影响”）

| 表达式/子句 | 值依赖 | 控制依赖 |
| --- | --- | --- |
| 普通列引用 | 该列 | 继承绑定行级控制 |
| 字面量/NULL | 无 | 无 |
| 算术/字符串拼接 | 所有操作数值依赖的并 | 操作数自带控制 |
| WHERE 谓词 | —（不产出值） | 谓词列 → `filter` |
| ON / USING | — | 两侧键 → `join_pairing` |
| GROUP BY 键 | 键原值保留 | 键 → 所有聚合输出的 `group_key` |
| SUM(x) | x | 分组键 + 输入自带控制 |
| COUNT(*) | **无任何值来源** | 筛选/连接/分组键 |
| COUNT(x) | 无（仅空/非空判定） | x → `count_input` + 分组键 |
| COUNT(DISTINCT x) | x（去重比较真实值） | `count_input` + `distinct_input` |
| CASE | THEN/ELSE 分支值 | WHEN 条件（及简单 CASE 的判别值）→ `case_condition` |
| 确定性函数 | 全部实参 | 实参自带控制 |
| `unknown` 函数 | 实参仍记录，但列被污染 | 同上，且 unknown 向下游传播 |

谓词节点先原样保留操作数的值依赖，由调用上下文统一改标控制原因，避免
WHERE 里的列被误标成 `case_condition` 之类的原因串扰。

不要求判断谓词等价性，也不证明某次合法数据修改一定改变结果——所有结论
都是保守的“可能影响”。但保守不等于全标 unknown：只有 catalog 明确声明
`unknown` 的函数才引入不确定性，确定性函数与关系算子保持精确。

### unknown 的传播纪律

`unknown` 函数的实参依赖仍然如实记录，但该输出列 `unknown=true`。任何
下游视图只要引用了被污染的列，对应输出也进入不确定状态；同一视图里与
不透明函数无关的兄弟列保持精确（夹具 `v_unk_down2`：`s2` 不确定、
`oid` 精确）。impact 时不确定视图单独成桶，**既不算精确命中也不算精确
不受影响**。无法解析的视图 fail-closed（`upstream_view_invalid` 级联），
绝不因为解析失败而输出“无依赖”。

## 4. 跨视图解析、循环与增量

* 视图按需递归解析，builder 维护解析栈做环保护，并在分析前用一次轻量
  拓扑检测给出 `circular_view_reference`（含完整环链）；
* 每个视图记录 `source_objects`、`source_roles`（别名→对象→类型→位置）、
  `view_dependencies`、SQL 的 sha256、schema 指纹与输入指纹；
* 增量 build 复用“SQL + schema + 传递输入均未变”的分析结论；被改视图的
  下游链通过递归顺序自然使用新结果，旧分析不会与新 SQL 混搭；
* `validate` 检测：schema 指纹漂移、视图文件新增/删除/改写、丢失基础
  对象、循环引用、合同规模上限（20 视图 / 每视图 5 源 / 10 表 / 每表
  20 列）。

## 5. 持久目录格式

```
<out>/
  manifest.json          # 指纹、规模上限、每视图 SQL/输入指纹与依赖
  schema.json            # 基础表 schema（源 schema 留档）
  functions.json         # 函数依赖声明留档
  dependency_graph.json  # 表/视图节点 + 视图→视图、视图→表边
  failures.json          # 全部解析/语义失败（代码+消息+SQL 位置）
  views/<view>.json      # 每视图一份可查询文档（见下）
```

每视图文档含状态、SQL 摘要（文件名 + sha256）、源角色、CTE 名、逐列的
`precision`（`exact | unknown | no_value_source`）、值/控制来源（含完整
path 与基础别名）、unknown 原因、空侧来源，以及每个投影表达式与别名的
字节级 `location`（line/col/start/end，直接可对原 SQL 切片）。

## 6. impact 语义

输入基础列集合与 `change_kind`：

* `value_change` — 按值/控制依赖分别给出**可解释路径**
  （`value_change_paths` / `control_change_paths`，含 reason 与 hop 链）；
* `drop` / `type_change` — 额外给出 `invalid` 列表，指出绑定失效的最近
  一级：整表消失 → `from_binding`；列消失/类型变化 → `column_binding`，
  并沿视图链向上级联；
* 桶位严格分离：`exact`（可解析、无 unknown 且命中）、`uncertain`
  （unknown 污染或视图自身失败）、`unaffected`（可解析、无 unknown 且
  证明无涉）。确定无关列必须出现在 unaffected，不允许为保守把整个目录
  标成 unknown。

## 7. 验证方法（`tests/`，28 个用例）

1. **预期图核对**：星号展开、未投影 ON 列、COUNT(*)、同名 CTE 遮蔽、
   USING 合并、CASE 条件、同表双别名角色、歧义/重名/未注册函数/递归
   CTE/缺对象/HAVING 的拒绝码、unknown 传播与桶位纪律；
2. **SQLite 反事实小库**：真实建表建视图执行；受控修改值列（单元格变、
   行数不变）、过滤列（行成员变化）、ON 键（配对与 LEFT 空侧修复）、
   CASE 条件列（选值变化）、无关列（结果逐行不变且 impact=unaffected）。
   数据执行只用于佐证声明，结构依赖图才是判定来源——“少量数据没变化”
   永远不会被当作无依赖的证据；
3. **增量/校验**：指纹复用计数、单视图修改后的链路重分析、schema 漂移、
   丢表 fail-closed、循环检测、5 源上限。

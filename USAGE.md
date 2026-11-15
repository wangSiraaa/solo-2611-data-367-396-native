# 使用说明

依赖：Python 3.11，解析库 sqlglot（已在 `.venv` 中安装；纯 Python，无其他
运行时依赖）。

```bash
python3 -m venv .venv
.venv/bin/pip install sqlglot

# 1) 构建持久目录（视图有解析错误时退出码 1，错误写入 failures.json）
.venv/bin/python -m lineage_tool.cli build \
    --catalog catalog.json --views views --out .lineage_catalog

# 2) 查询某视图全部列，或某一具体列的值/控制来源与 SQL 位置
.venv/bin/python -m lineage_tool.cli lineage --out .lineage_catalog --view report
.venv/bin/python -m lineage_tool.cli lineage --out .lineage_catalog \
    --view customer_totals --column n

# 3) 变更影响（--base-column 可重复；值变更）
.venv/bin/python -m lineage_tool.cli impact --out .lineage_catalog \
    --base-column orders.amount --change-kind value_change
# 仅影响选行/分组/连接的列：看 control_change_paths 与 reason
.venv/bin/python -m lineage_tool.cli impact --out .lineage_catalog \
    --base-column orders.status --change-kind value_change
# 删列/类型变更：额外给出绑定失效级别
.venv/bin/python -m lineage_tool.cli impact --out .lineage_catalog \
    --base-column orders.amount --change-kind drop
.venv/bin/python -m lineage_tool.cli impact --out .lineage_catalog \
    --base-table customers --change-kind drop

# 4) 校验目录新鲜度、循环引用、丢失对象与规模上限
.venv/bin/python -m lineage_tool.cli validate \
    --catalog catalog.json --views views --out .lineage_catalog

# 测试
.venv/bin/python -m unittest discover -s tests -v
```

## catalog.json 约定

```json
{
  "tables": {"表名": {"列名": "SQLITE类型"}},
  "functions": {
    "确定性函数名": {"dependency": "argument_dependent"},
    "不透明函数名": {"dependency": "unknown"}
  }
}
```

* 表名/列名按 SQLite 大小写不敏感处理，目录中保留原始拼写用于展示；
* `argument_dependent`：结果只依赖全部实参（值依赖实参的并集）；
* `unknown`：无法证明只依赖实参，实参仍记录但输出标记不确定并向下游传播；
* 未登记的非白名单函数直接拒绝视图，提示先登记，杜绝静默猜测。

## 输出阅读

每个视图列文档中：

* `precision`：`exact`（精确）/ `unknown`（含不透明函数）/
  `no_value_source`（如 COUNT(*)、纯字面量）；
* `value_sources` / `control_sources`：基础列 + reason
  （`base_column/filter/join_pairing/group_key/case_condition/count_input/
  distinct_input/left_null_side`）+ 跨视图 path + 基础表扫描别名；
* `location` / `name_location`：原 SQL 中的字节与行列位置；
* impact 的 `exact / uncertain / unaffected / invalid` 四个桶互斥，
  unknown 永不计入精确命中或精确不受影响。

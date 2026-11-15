# maskkit — 本地脱敏数据包生产工具

纯 Python 3.11 + 标准库 `sqlite3` 实现，无第三方依赖。输入只读 `source.sqlite`、
`policy.json` 和一份专用持久 token-vault，输出可独立打开的 `release.sqlite`
与不含原身份值的公开回执 `receipt.json`。所有输入均为合成数据。

## 1. 命令

```bash
python -m maskkit init-vault --vault DIR
python -m maskkit plan    --source source.sqlite --policy policy.json --vault DIR [--out plan.json]
python -m maskkit release --source source.sqlite --policy policy.json --vault DIR --out OUT_DIR [--release-id ID]
python -m maskkit verify  --db OUT_DIR/release.sqlite --receipt OUT_DIR/receipt.json --vault DIR
python -m maskkit recover --vault DIR
```

退出码：`0` 成功；`2` plan 静态拒绝；`3` 代号域耗尽；`4` 发布中断；`5` verify 失败；`1` 其它错误。

## 2. policy.json 格式

```json
{
  "policy_id": "p1",
  "sensitive_columns": ["customers.customer_id", "..."],
  "public_keep_columns": ["invoices.invoice_id"],
  "domains": {
    "customer": {"prefix": "C", "alphabet": "0123456789abcdef", "width": 2}
  },
  "columns": {
    "customers.customer_id": "tokenize:customer",
    "invoices.invoice_id": "keep",
    "some_table.legacy_col": "drop"
  }
}
```

- 动作仅三种：`drop` / `keep` / `tokenize:<domain>`。
- 令牌 = `prefix` + `width` 个 `alphabet` 字符；前缀计入格式且**跨域唯一**（R010）。
- 域容量 = `len(alphabet) ** width`。

等值域（与题目一致）：`customer_id`/`owner_id` 共用 `customer`；
`account_id`/`account_ref` 共用 `account`；`email`/`bill_to_email` 共用 `email`。
**原字符串完全相同且域相同才共享 token；不同域即使原文相同也得到不同公开 token。**
NULL 保留 NULL；空串是合法原值，会正常分配令牌。不做大小写归一、不按姓名归并。

## 3. plan 的静态规则（错误只引用表/列/规则 ID，绝不回显原值）

| 规则 | 含义 |
|---|---|
| R001 | 列策略引用了源库不存在的表/列 |
| R002 | 源库某列未登记任何列策略 |
| R003 | 敏感/公开清单引用了不存在的表/列 |
| R004 | 必需表（customers/accounts/invoices）缺失 |
| R005 | 动作字符串非法 |
| R006 | 引用了未声明的域 |
| R007 | 外键两端动作或域不一致（含父列无法解析、循环 FK） |
| R008 | alphabet/width/令牌长度非法或字母表含重复字符 |
| R009 | 域前缀非法（缺失或自身含重复字符） |
| R010 | 不同域前缀冲突 |
| R011 | 敏感列被声明为 `keep` |
| R012 | `drop` 落在参与 PK/UNIQUE/FK 的列上（会破坏声明约束） |
| R013 | 必需表清单配置非法 |
| R014 | 列策略与敏感/公开清单登记不一致 |
| R015 | 当批去重后域取值数超过代号容量 |

约束信息全部来自 `PRAGMA table_info / foreign_key_list / index_list`，
不解析 CREATE TABLE 文本。

## 4. 令牌分配（确定性碰撞处理 + 持久分配）

- vault 表 `token_mapping` 对 `(domain, value_sha256)` 与 `(domain, token)`
  各建一个 UNIQUE 约束，数据库层同时保证“同值不重复分配”和“同域不同值不同 token”。
- 原值以**域隔离哈希** `sha256(canonical([domain, value]))` 作业务键；完整原值只写入
  vault 内 `manifests/<release_id>.json`（权限 0600），不进公开目录、普通日志或回执。
- 候选起点 `start = int(sha256(domain || 0x00 || canonical([value])), 16) mod capacity`，
  随后线性探测 `(start + step) mod capacity` 直到找到未占用代号。两位摘要前缀碰撞的原值
  起点相同，但探测会落到不同代号——**不合并、不覆盖、不按批次重新编号**。
- 历史映射优先复用，因此新增值只取新代号，已发布身份永久稳定。代号不回收，探测链保持稳定。

### 加入新值
已存在 `(domain, value)` 直接复用旧 token；新值从确定性起点探测，占用第一个空位。

### 并发分配
所有写操作在 vault 目录 `lock` 文件的进程级 `flock(LOCK_EX)` 内、并以 SQLite
`BEGIN IMMEDIATE` 事务执行。两个进程同时首次遇到同值时，后到者阻塞，待前者提交后
读到同一行映射并复用——同一值不可能被分配两次。

### 重启
映射与发布状态都在 SQLite（WAL、`synchronous=FULL`）中持久化。进程重启后先跑 `recover`
依据状态机收敛，再继续发布；映射不依赖内存。

### 代号耗尽
当批域取值超过容量在 **plan 阶段**即以 R015 拒绝；运行时若 vault 已被历史发布占满，
`release` 以退出码 3 `CapacityExhausted` 拒绝，事务回滚，不留半成品。这是可预期拒绝而非损坏。

## 5. 发布事务与可恢复记录

发布决定在一个持锁窗口内先固定 **policy 规范指纹** 与 **源快照内容指纹**，再执行。
vault 中 `release_record` 状态机：

```
reserved ──写成品前崩溃──▶ abandoned
reserved/intermediate ──成品缺失或哈希不符──▶ abandoned（失败包永不标 complete）
intermediate ──成品存在且 sha256 匹配、私有清单齐全──▶ complete（补登）
complete ──成品外部丢失──▶ 报告但保留映射
```

成品先写同目录临时文件，`fsync` 后 `os.replace` 原子替换，读取方只见旧文件或完整新文件。
**vault 提交与成品发布分步进行，每一步都在 vault 留有可核查记录**；预留但最终未被使用的
token 允许保留在 `token_mapping` 中，不影响正确性。

## 6. verify 做了什么（不用“FK 检查为空”代替一一性）

- V01 成品 sha256 与回执一致；V02 `integrity_check`；V03 真实 `foreign_key_check`
- V04 表/列集合与回执一致；V05 NOT NULL 空值计数；V06 每个 UNIQUE 索引去重计数
- V07 主键非空且唯一；V08 release 记录确为 `complete`
- V09 逐令牌校验前缀/字母表/长度正则
- V10 成品令牌集合与本 release 私有清单**双向集合相等**
- V11 vault 同域令牌显式去重计数；V13 成品令牌均有 vault 背书
- V12 **跨批稳定性**：清单中每个原值当前仍解析到同一令牌
- V14 release 记录与回执的 policy/源/输出哈希指纹绑定

## 7. 目录与保密边界

```
vault/（显式 private，0700）
  vault.sqlite          # 映射 + 发布状态（0600）
  manifests/<rid>.json  # 原值->令牌完整映射（0600）
  lock                  # 进程排他锁
OUT_DIR/（公开）
  release.sqlite        # 仅脱敏后声明业务列，可独立打开
  receipt.json          # 指纹/计数/令牌格式，无原值
```

## 8. 复跑检查

```bash
python -m unittest discover -s tests -v   # 34 个用例
bash tests/smoke.sh                        # 端到端冒烟
```

覆盖：plan 各拒绝规则、两快照新增客户、跨表同 email、空串与 NULL、撞码值不同 token、
两进程同时首遇同值、代号域耗尽（plan 与运行时）、成品发布前中断与 recover、
成品被篡改/孤儿外键/NOT NULL 的 verify 反向用例。规模约束：每表 ≤200 行、每域 ≤64 不同值。

## 9. 明确不做

不抽取关系子集、不做身份推断、不做去重合并、无 Web 界面、不产出生产隐私合规判断；
不要求代号可逆，也不提供密码学匿名性证明。

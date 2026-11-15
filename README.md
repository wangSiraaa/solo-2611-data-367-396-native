# 跨分区事务材料
`source.py p0` / `source.py p1` / `source.py control`分别输出真实JSONL流。分区流允许跨事务交错；op_no是事务内部顺序，part_no是事务在该分区的顺序。control声明part_count、dependencies和terminal状态。
数据和控制可以由调用者任意交错，但每条源流内部顺序保持。相同片段可重送，内容不同的同身份片段属于损坏。源只读，不要求Kafka或PostgreSQL服务器。

## 交付物

| 文件 | 说明 |
|---|---|
| `loader.py` | 单机 CDC 装载器（Python 3.11，仅标准库 + SQLite），子命令 `ingest/work/query/inspect/arm-fault` |
| `demo.py` | 11 个确定性场景、91 项断言，全部以独立子进程驱动（真实进程重开） |
| `DESIGN.md` | 协议、片段索引、状态机、就绪/依赖队列、物化边界、恢复与未覆盖范围 |
| `out/deliver/cdc.db` | 用题给 fixtures 完整装载后的实际 SQLite 产物（含目标表与全部状态表） |
| `out/scenarios/` | 各场景独立数据库，可用 inspect/query 复查 |

## 快速检查

```bash
# 全量确定性场景（控制先到/数据先到/分区交错、重放折叠、损坏、缺片、
# 依赖环、失败父事务、提交后丢确认、空库拓扑重放、写冲突合同、外键合同）
python3 demo.py

# 手动端到端：--end 输出显式结束帧（EOF 不算结束）
python3 loader.py ingest out/demo/cdc.db --source control --file <(python3 source.py control --end)
python3 loader.py ingest out/demo/cdc.db --source p0      --file <(python3 source.py p0 --end)
python3 loader.py ingest out/demo/cdc.db --source p1      --file <(python3 source.py p1 --end)
python3 loader.py work    out/demo/cdc.db            # 调度+物化，输出 receipts 与全量状态
python3 loader.py query   out/demo/cdc.db --pretty   # 目标表可见行
python3 loader.py inspect out/demo/cdc.db --pretty   # 流结束位/事务状态/违例/审计/回执/故障开关
```

## 子命令

```
ingest DB --source {p0,p1,control} --file FILE|-   # 一路 JSONL 落盘；每帧独立事务，重复折叠/损坏留痕
work   DB [--once] [--max-passes N]                 # 多 pass 调度直到稳定；--once 单步便于观察边界
query  DB [--table customers|orders] [--pretty]
inspect DB [--txn T] [--pretty]
arm-fault DB {fail_before_commit|lose_ack_after_commit} --txn T
```

退出码：0 正常；2 输入 JSON 非法；3 `lose_ack_after_commit`（提交已持久化、确认丢失，重跑 work 核查）。

## 状态速查

`receiving → waiting_dependency → committed`；源全部 end 后缺片 `incomplete`、
依赖仍未知 `missing_dependency`；依赖失败后继 `blocked`；协议损坏 `corrupt`；
依赖环/无路径写冲突 `rejected`；提交语义失败 `failed`；终态 `aborted` 不触目标。
完整规则与两种物化方案取舍见 `DESIGN.md`。

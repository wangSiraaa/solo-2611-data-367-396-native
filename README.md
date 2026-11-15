# 本地查询内存仲裁器

三个排序查询共享一个有限记录槽池，复用材料中的真实排序算子 `SortBuffer`
（真实 consume / 带 pin 回调的文件溢写 / 运行段读取），新增共享授予、
回收、查询调度、确定性终态与离线审计。设计与调度选择见 [DESIGN.md](DESIGN.md)。

## 目录

- `operators.py` 材料原设施（语义保留）+ 最终 k 路归并入口
- `arbiter.py` 槽仲裁器：四子账记账、FIFO 准入、软下限可撤销授予、pin 安全
- `worker.py` 每查询工作线程：驱动真实算子、检查点溢写、取消/失败清理
- `runner.py` 配置校验与编排；`scenario.py` 确定性 barrier/event 场景
- `audit.py` 仅基于工作目录的守恒/pin/请求闭环/结果一致性审计
- `cli.py` 命令行；`tests/test_arbiter.py` 可复跑测试（22 个）

## 快速开始

```bash
python -m unittest tests.test_arbiter -v

python -m memarbiter.cli run --config config.json --queries queries.json \
    --work-dir runs/demo --scenario barrier
python -m memarbiter.cli audit --work-dir runs/demo
```

场景：`plain`（竞争）、`barrier`（两查询先持可撤销记录再同时申请大批次）、
`cancel-pin`（pin 区取消，写入先完成）、`cancel-waiting`（等待态取消）、
`cancel-reserved`（授予未用取消）、`fail-spill`（pin 区算子错误）。
全部由 `threading.Event` 控制交错，不使用 sleep 或墙钟超时作为正确性证明。

## 不变量摘要

- `held = reserved + resident + pinned + outbuf`，`sum(held)+free == pool`；
- 整批全有或全无，授予总量永不超池；pin 槽落盘前不可撤销、不可再申请；
- 撤销只发指令，真实释放发生在设施 `spill()` 写盘返回之后；
- 取消/失败只清理本查询资源与目录，终态前四子账必须全零。

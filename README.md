# 本地 exchange 输入与可控阻塞

离线有界 ETL：两路 JSONL 源 → 单 router 按 `key%2` 分区 → 两个 worker 展开 fanout
→ 慢速 sink 落盘。线程实现，自定义有界帧通道（容量 1/2），全局编码 payload 信用
台账（默认 16KiB），独立取消 Event，不使用任何分布式框架或无限队列。

## 入口

```bash
python3 generate.py data                  # 生成 a.jsonl / b.jsonl（24 输入，36 输出）
python3 -m unittest tests.test_pipeline   # 全量测试（15 用例）
python3 run_demo.py data run all          # schedules.json 的 normal/cancel/worker_failure
python3 run_demo.py data run all --capacity 2
python3 run_demo.py data inspect runs/normal-cap1
```

## 目录

- `etl_demo/model.py`：Frame、EngineConfig、Hooks、RunResult、256B/16KiB 常量
- `etl_demo/channels.py`：有界通道 + 多路接收组（共享就绪条件，EOS 与数据同队列）
- `etl_demo/state.py`：ByteBudget（含输出保留额度、fid 幂等释放）、StatusBoard
- `etl_demo/roles.py`：source / router / worker / sink 四个角色
- `etl_demo/engine.py`：拓扑装配、start/cancel/inspect/join、回收与 state.json
- `etl_demo/test_support.py`：Barrier、独立逐行展开参考、故障钩子工厂
- `tests/test_pipeline.py`：15 个 unittest 用例
- `run_demo.py`：按 schedules.json 控制点复跑的 CLI
- `DESIGN.md`：拓扑/状态格式、推进方式选型、备选方案与占用差异、终止/取消协议

设计原理、正确性边界与实际占用见 `DESIGN.md`。

"""编排层：配置校验、确定性钩子、多查询 Runner 与场景控制。

正确性不依赖墙钟：测试通过 :class:`HookBus` 在设施真实执行点
（申请前、授予后、消费后、pin 窗口内、写盘后）插入 barrier/event，
用事件等待而非 sleep 决定交错顺序。
"""
import json
import threading
from pathlib import Path

from .arbiter import EventLog, SlotArbiter
from .worker import QueryWorker


class ConfigError(ValueError):
    """非法配置（初始化即拒绝）。"""


def validate_config(global_slots, minimum_per_query, queries,
                    max_batch_size=6, max_row_bytes=256):
    """按材料约束校验配置；非法即抛 ConfigError。

    约束：
      * 8 <= 全局槽 <= 32；
      * 每查询最小槽 >= 1，且两个查询最小槽之和 <= 全池
        （保证任意时刻至少两个查询可以同时驻留运行）；
      * 查询数 1..3，每查询 <= 200 行，单批 1..6 行；
      * 合法配置必须容纳任一单个最大批：max_batch <= global_slots；
      * 每行序列化后不超过 256 字节。
    """
    if not isinstance(global_slots, int) or not (8 <= global_slots <= 32):
        raise ConfigError("global_slots 必须是 8..32 的整数")
    if not isinstance(minimum_per_query, int) or minimum_per_query < 1:
        raise ConfigError("minimum_per_query 必须 >= 1")
    if 2 * minimum_per_query > global_slots:
        raise ConfigError(
            f"两个查询最小槽之和 {2 * minimum_per_query} 超过全池 "
            f"{global_slots}，初始化拒绝")
    if not (1 <= len(queries) <= 3):
        raise ConfigError("查询数必须在 1..3 之间")
    max_batch = 0
    total_rows = 0
    for q in queries:
        qid = q["query_id"]
        rows_n = 0
        for bi, batch in enumerate(q["batches"]):
            if not (1 <= len(batch) <= max_batch_size):
                raise ConfigError(
                    f"{qid} 批 {bi} 大小 {len(batch)} 超出 1..{max_batch_size}")
            for r in batch:
                if "key" not in r or "row_id" not in r:
                    raise ConfigError(f"{qid} 记录缺少 key/row_id")
                if len(json.dumps(r, ensure_ascii=False).encode("utf-8")) \
                        > max_row_bytes:
                    raise ConfigError(
                        f"{qid} 存在超过 {max_row_bytes} 字节的记录")
            rows_n += len(batch)
            max_batch = max(max_batch, len(batch))
        if rows_n > 200:
            raise ConfigError(f"{qid} 行数 {rows_n} 超过 200")
        total_rows += rows_n
    if max_batch > global_slots:
        raise ConfigError(
            f"最大批 {max_batch} 超过全池 {global_slots}，无法容纳任一最大批")
    return {"max_batch": max_batch, "total_rows": total_rows}


class HookBus:
    """确定性钩子总线。

    每个钩子点同步调用已注册的回调；回调内可对 threading.Event
    wait/set，构造确定性 barrier。回调抛 QueryCancelled 可用于在
    pin 窗口内注入取消（由工作线程转成取消终态）。
    """

    def __init__(self):
        self._cbs = []
        self._lock = threading.Lock()

    def subscribe(self, callback):
        with self._lock:
            self._cbs.append(callback)

    def _fire(self, name, qid, *args):
        with self._lock:
            cbs = list(self._cbs)
        for cb in cbs:
            cb(name, qid, *args)

    def before_request(self, qid, batch_index, size):
        self._fire("before_request", qid, batch_index, size)

    def after_grant(self, qid, batch_index, size):
        self._fire("after_grant", qid, batch_index, size)

    def after_consume(self, qid, batch_index):
        self._fire("after_consume", qid, batch_index)

    def on_pinned(self, qid, n):
        self._fire("on_pinned", qid, n)

    def on_written(self, qid, n):
        self._fire("on_written", qid, n)


class Runner:
    def __init__(self, config, queries, work_dir, hooks=None,
                 output_chunk=None):
        self.config = config
        self.queries = queries
        self.work_dir = Path(work_dir)
        if self.work_dir.exists() and any(self.work_dir.iterdir()):
            raise ConfigError(f"工作目录必须是显式新目录：{self.work_dir} 非空")
        self.work_dir.mkdir(parents=True, exist_ok=True)
        validate_config(config["global_slots"],
                        config["minimum_per_query"], queries)
        # 输入快照写入工作目录，供离线审计独立核对结果。
        (self.work_dir / "input_snapshot.json").write_text(
            json.dumps(queries, ensure_ascii=False, indent=2),
            encoding="utf-8")
        (self.work_dir / "config_used.json").write_text(
            json.dumps(config, ensure_ascii=False, indent=2),
            encoding="utf-8")
        self.log = EventLog(self.work_dir / "events.jsonl")
        self.arbiter = SlotArbiter(
            config["global_slots"], config["minimum_per_query"], self.log)
        self.hooks = hooks or HookBus()
        self.workers = [
            QueryWorker(q["query_id"], q["batches"], self.arbiter,
                        self.work_dir, hooks=self.hooks,
                        output_chunk=output_chunk)
            for q in queries
        ]

    def start(self):
        self.log.emit("run_start", None,
                      pool=self.arbiter.pool,
                      minimum=self.arbiter.minimum,
                      queries=[w.qid for w in self.workers])
        for w in self.workers:
            w.start()

    def wait(self):
        for w in self.workers:
            w.join()
        self.log.emit("run_end", None)
        self.log.close()

    def run(self):
        self.start()
        self.wait()
        return self.result()

    def result(self):
        return {
            w.qid: {
                "status": w.status,
                "error": w.error,
                "output_path": w.output_path,
            } for w in self.workers
        }

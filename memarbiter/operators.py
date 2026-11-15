"""真实排序算子设施。

本模块保留材料中 ``SortBuffer`` 的全部真实语义，仅做两件事：
  1. 保持接口兼容（consume / spill 真实落盘 / 运行段读取）；
  2. 补上材料明确缺失的"最终多路归并入口" :func:`merge_sorted_runs`。

排序与输入输出语义不变：每个溢写运行段是按 (key, row_id) 排序的 JSONL
真实文件；最终输出由这些已排序运行段与驻留记录做 k 路归并得到，
不存在内存里对全部输入重新排序的旁路。
"""
import heapq
import json
from pathlib import Path


def sort_key(row):
    """全局输出排序键：先按 key，再按 row_id。"""
    return row["key"], row["row_id"]


class SortBuffer:
    """可执行的输入排序设施（材料原设施，接口与行为保持兼容）。

    状态只有：驻留记录列表 ``rows`` 与已落盘运行段路径 ``runs``。
    文件句柄不长期持有；标量元数据（路径、计数）不计记录槽。
    """

    def __init__(self, work, qid):
        self.work = Path(work) / qid
        self.work.mkdir(parents=True, exist_ok=True)
        self.rows = []
        self.runs = []

    def consume(self, rows):
        """消费一批记录，追加到驻留区。"""
        self.rows.extend(rows)

    def resident_count(self):
        return len(self.rows)

    def spill(self, on_pinned=lambda: None, on_written=lambda: None):
        """把当前全部驻留记录排序并溢写为一个新运行段。

        pin 窗口语义（供仲裁器/测试构造真实写窗口）：
          * ``on_pinned`` 在写入之前调用——此刻起这批记录进入 pin 区，
            其槽位在写入完成前不可撤销；
          * 真实文件写完后调用 ``on_written``；
          * 两个回调返回之后才清空驻留区并登记运行段，最后返回溢写行数。
        即调用方在 ``spill`` 返回之前绝不能释放这些槽位。
        """
        on_pinned()
        path = self.work / ("run-%d.jsonl" % len(self.runs))
        ordered = sorted(self.rows, key=sort_key)
        path.write_text("".join(json.dumps(r) + "\n" for r in ordered))
        on_written()
        n = len(self.rows)
        self.rows.clear()
        self.runs.append(path)
        return n

    def materialized_rows(self):
        """按运行段顺序读取全部已落盘记录，再追加当前驻留记录。"""
        for path in self.runs:
            for line in path.read_text().splitlines():
                yield json.loads(line)
        yield from self.rows


def _iter_run(path):
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            yield json.loads(line)


def merge_sorted_runs(run_paths, resident_rows, key=sort_key):
    """最终多路归并：材料缺失的入口。

    每个运行段本身已按 (key, row_id) 有序，驻留记录先就地排序成一个
    逻辑流，再用 :func:`heapq.merge` 做稳定 k 路归并。全程流式产出，
    不在内存中物化全部输出行。
    """
    streams = [_iter_run(p) for p in run_paths]
    streams.append(iter(sorted(resident_rows, key=key)))
    return heapq.merge(*streams, key=key)

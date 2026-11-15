"""测试夹具：合成受控图与可变能力的 API 子类。

合成图直接通过 GraphAPI 的公开构造函数落盘为临时私有文件，
测试代码本身不读取该文件（oracle 对账除外）。
"""
import copy
import json
import tempfile
from pathlib import Path

from graph_api import GraphAPI
import graph_api

_ORIGINAL_CAPS = copy.deepcopy(graph_api.CAPABILITIES)


def write_private(rows):
    tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
    json.dump(rows, tmp)
    tmp.close()
    return tmp.name


def make_api(rows, capabilities=None):
    private = write_private(rows)
    if capabilities is not None:
        # 仅测试用途：替换服务端能力声明，模拟“合法能力组合/不支持方向”
        graph_api.CAPABILITIES.clear()
        graph_api.CAPABILITIES.update(capabilities)
    return GraphAPI(private), private


def restore_standard_caps():
    """恢复 make_source 配套图的真实能力声明。"""
    graph_api.CAPABILITIES.clear()
    graph_api.CAPABILITIES.update(copy.deepcopy(_ORIGINAL_CAPS))


def use_synthetic_caps():
    """合成 L* 图使用的能力声明。"""
    graph_api.CAPABILITIES.clear()
    graph_api.CAPABILITIES.update(standard_caps())


def edge(eid, a, b, label, cost=1):
    return {"edge_id": eid, "src": a, "dst": b, "label": label, "cost": cost}


def dense_page_graph(label="L", page_edges=30):
    """单请求候选超过 25 条，强制多页翻页；含平行边与负 cost。"""
    rows = []
    for i in range(page_edges):
        cost = -5 if i % 7 == 0 else i
        rows.append(edge("%s-e%03d" % (label, i), "s", "m%d" % i, label, cost))
    # s -> m0 的平行边（不同 edge_id）
    rows.append(edge("%s-parallel" % label, "s", "m0", label, 100))
    rows.append(edge("T-t", "m0", "z", "T", 1))
    return rows


def cycle_graph():
    """会回到起点的伪路径必须被简单路径检查拒绝；另含一条合法简单路径。"""
    return [
        # 伪路径分支：s -> a -> s（第二跳回到起点，无法再合法扩展到 z）
        edge("c1", "s", "a", "L1"),
        edge("c2", "a", "s", "L2"),
        # 合法简单路径分支：s -> b -> z，标签序列同样是 L1,L2,L3
        edge("c3", "s", "b", "L1"),
        edge("c4", "b", "x", "L2"),
        edge("c5", "x", "z", "L3"),
    ]


def parallel_graph():
    """同 src/dst/label 不同 edge_id 的平行边产生不同路径。"""
    return [
        edge("p1", "s", "a", "L1", 1),
        edge("p2", "s", "a", "L1", 2),
        edge("p3", "a", "z", "L2", -4),
        edge("p4", "a", "z", "L2", 4),
    ]


def multi_endpoint_graph():
    rows = []
    for i in range(4):
        rows.append(edge("me-a-%d" % i, "s", "a%d" % i, "L1"))
        rows.append(edge("me-b-%d" % i, "a%d" % i, "z%d" % i, "L2"))
    return rows


def standard_caps():
    """合成图使用的 L* 标签能力。"""
    return {
        "L": {"directions": ["out", "in"], "target_filter": True},
        "L1": {"directions": ["out", "in"], "target_filter": True},
        "L2": {"directions": ["out", "in"], "target_filter": False},
        "L3": {"directions": ["out", "in"], "target_filter": False},
        "T": {"directions": ["out", "in"], "target_filter": False},
    }

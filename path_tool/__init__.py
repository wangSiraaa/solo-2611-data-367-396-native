"""受限图路径取数工具（候选实现）。

只能通过 GraphAPI.capabilities() 与 GraphAPI.query_edges() 两个公开接口取边。
本包内任何模块都不得读取私有边文件、访问 GraphAPI._rows 或导入 make_source。
独立 oracle（path_tool/oracle.py）仅用于验收对账，不属于候选执行链路。
"""
from .engine import (
    FetchEngine,
    Budget,
    INCOMPLETE,
    BUDGET_EXHAUSTED,
    OK,
    ERROR,
)
from .planner import analyze_query, build_plan
from .executor import execute_query
from .explain import explain, audit
from .enumerator import enumerate_paths

__all__ = [
    "FetchEngine",
    "Budget",
    "INCOMPLETE",
    "BUDGET_EXHAUSTED",
    "OK",
    "ERROR",
    "analyze_query",
    "build_plan",
    "execute_query",
    "explain",
    "audit",
    "enumerate_paths",
]

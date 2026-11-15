"""docidx：本地文档数据检索索引（不可变段 + 显式提交清单）。"""

from .analyzer import Analyzer
from .engine import IndexEngine, QueryError, Snapshot
from .oracle import OracleScanner, oracle_phrase, oracle_set_query

__all__ = [
    "Analyzer",
    "IndexEngine",
    "Snapshot",
    "QueryError",
    "OracleScanner",
    "oracle_phrase",
    "oracle_set_query",
]

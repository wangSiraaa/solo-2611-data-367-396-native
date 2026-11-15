"""本地查询内存仲裁器（共享记录槽池）。

公共入口：
    SlotArbiter / EventLog  —— 槽位授予、回收、终态与事件日志
    QueryWorker             —— 驱动真实 SortBuffer 的查询线程
    Runner                  —— 校验配置、编排多查询、产出审计
    audit_workdir           —— 仅基于工作目录的离线审计
    validate_config         —— 合法配置校验
"""
from .arbiter import SlotArbiter, EventLog, ArbiterError
from .operators import SortBuffer, merge_sorted_runs, sort_key
from .worker import QueryWorker, OperatorError, QueryCancelled
from .runner import Runner, validate_config, ConfigError
from .audit import audit_workdir, AuditFailure

__all__ = [
    "SlotArbiter",
    "EventLog",
    "ArbiterError",
    "SortBuffer",
    "merge_sorted_runs",
    "sort_key",
    "QueryWorker",
    "OperatorError",
    "QueryCancelled",
    "Runner",
    "validate_config",
    "ConfigError",
    "audit_workdir",
    "AuditFailure",
]

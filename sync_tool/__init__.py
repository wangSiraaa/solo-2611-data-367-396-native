"""Causal, bidirectional row replication for two independent SQLite replicas.

The engine lives in :mod:`sync_tool.engine`; the command line front end is
:mod:`sync_tool.cli`.  See ``DESIGN.md`` for the storage model and the argument
for why it has neither echoes nor partial publications.
"""

from .engine import Engine, EventError, ConflictError

__all__ = ["Engine", "EventError", "ConflictError"]

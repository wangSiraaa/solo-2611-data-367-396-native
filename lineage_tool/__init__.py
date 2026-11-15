"""Local column-level lineage and change-impact tool.

Modules
-------
models      persistent dataclasses (base refs, dependencies, view analyses)
errors      structured parse / semantic failures
analyzer    scope/relational-graph analyzer over a sqlglot AST
builder     cross-view resolution, incremental rebuild, catalog persistence
impact      change-impact classification
cli         build / lineage / impact / validate commands
"""

__all__ = ["models", "errors", "analyzer", "builder", "impact", "cli"]

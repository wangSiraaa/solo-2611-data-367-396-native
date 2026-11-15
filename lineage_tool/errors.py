"""Structured analysis failures.

Failures are never silently downgraded to "no dependency": an unparseable or
semantically ambiguous view gets ``status=error`` with a location-rich report,
and every view that depends on it fails closed with a cascaded reason.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import SourceLocation


class LineageError(Exception):
    def __init__(self, code: str, message: str, location: SourceLocation | None = None,
                 view: str | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.location = location
        self.view = view

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "view": self.view,
            "location": self.location.to_dict() if self.location else None,
        }


@dataclass
class FailureReport:
    code: str
    message: str
    view: str | None = None
    sql_file: str | None = None
    location: dict[str, Any] | None = None
    caused_by: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "view": self.view,
            "sql_file": self.sql_file,
            "location": self.location,
            "caused_by": self.caused_by,
        }


# Error code vocabulary.
E_SYNTAX = "sql_syntax_error"
E_UNSUPPORTED = "unsupported_sql_construct"
E_MISSING_OBJECT = "missing_base_object"
E_MISSING_COLUMN = "unknown_column"
E_AMBIGUOUS_COLUMN = "ambiguous_column"
E_DUPLICATE_OUTPUT = "duplicate_output_column"
E_UNKNOWN_FUNCTION = "unregistered_function"
E_AGGREGATE = "invalid_aggregate_usage"
E_GROUPING = "ungrouped_column_in_aggregate_query"
E_CYCLE = "circular_view_reference"
E_RECURSIVE_CTE = "recursive_cte_unsupported"
E_CASCADED = "upstream_view_invalid"
E_STALE_SCHEMA = "stale_schema"
E_LIMIT_EXCEEDED = "catalog_limit_exceeded"
E_BAD_CATALOG = "invalid_catalog"
E_MISSING_VIEW_FILE = "missing_view_file"

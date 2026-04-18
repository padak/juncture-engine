"""Diagnostics: error classification for migration triage.

During cross-dialect migrations (Snowflake -> DuckDB is the canonical
case) a single EXECUTE body can emit dozens of primary errors in one
``--continue-on-error`` pass. Without a classifier an operator has to
eyeball 40+ stack traces to see that 30 of them are one pattern
("VARCHAR vs BIGINT join") and 5 are another ("sentinel '--empty--' in
CAST").

:func:`classify_error` turns a DuckDB error string into a structured
:class:`ErrorClassification` with a coarse ``bucket``, a finer
``subcategory``, and a ``fix_hint`` template the author (or a repair
agent) can apply directly. The mapping is regex-driven and lives
entirely in this module — ``juncture.diagnostics`` has no adapter or
Project dependencies so it can be called from any layer (CLI, web UI,
agent prompt).

See ``docs/MIGRATION_TIPS.md`` §5.2 for the taxonomy this module
encodes.
"""

from __future__ import annotations

from juncture.diagnostics.classifier import (
    ErrorBucket,
    ErrorClassification,
    classify_error,
    classify_statement_errors,
)

__all__ = [
    "ErrorBucket",
    "ErrorClassification",
    "classify_error",
    "classify_statement_errors",
]

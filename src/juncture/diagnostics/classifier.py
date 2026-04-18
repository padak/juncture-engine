"""Regex-driven classifier for DuckDB error messages.

Public surface:
    classify_error(message) -> ErrorClassification
    classify_statement_errors(errors) -> list[ErrorClassification]

The classifier is deliberately a pure function — no DuckDB, no adapter,
no Project. This keeps it callable from every place a DuckDB error can
appear (adapter.close path, web UI backend, MCP tool, AI prompt builder).
New patterns are added by extending ``_RULES`` below.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum


class ErrorBucket(StrEnum):
    """Coarse-grained error family; drives triage UI grouping."""

    #: ``VARCHAR vs BIGINT`` / ``CASE mixing types`` — schema-aware
    #: ``translate_sql`` usually collapses these in one pass.
    TYPE_MISMATCH = "type_mismatch"
    #: ``No function matches 'fn(TYPES)'`` — argument type gap, usually
    #: fixed by :func:`harmonize_function_args`.
    FUNCTION_SIGNATURE = "function_signature"
    #: String can't be coerced to a target type (``CAST('' AS INT)``).
    #: Sentinel-detector territory.
    CONVERSION = "conversion"
    #: ``Table with name X does not exist`` — ambiguous until paired
    #: with the intra-script dependency DAG: primary or cascade?
    MISSING_OBJECT = "missing_object"
    #: Re-run without cleanup (``Table with name X already exists``).
    IDEMPOTENCE = "idempotence"
    #: SQLGlot couldn't even parse the statement; dialect-specific
    #: syntax (UNPIVOT, LATERAL FLATTEN) usually.
    PARSER = "parser"
    #: Everything else. When this grows > 5 % of a migration's errors,
    #: add a new rule to ``_RULES``.
    OTHER = "other"


@dataclass(frozen=True, kw_only=True)
class ErrorClassification:
    """A single classified DuckDB error.

    ``subcategory`` is finer than ``bucket`` and identifies the exact
    pattern (e.g. ``sentinel_string_to_int``); ``fix_hint`` is the
    minimal template an operator or agent applies to the SQL.
    ``operands`` captures the type names extracted from the error string
    when the regex has named groups for them — useful for auto-repair
    tools that need "wrap the VARCHAR operand".
    """

    bucket: ErrorBucket
    subcategory: str
    fix_hint: str
    error_message: str
    operands: dict[str, str]


# --- Rules ---------------------------------------------------------------
#
# Order matters: more specific patterns come first. Each rule is a
# (regex, bucket, subcategory, fix_hint) tuple; named groups in the regex
# populate ``ErrorClassification.operands``.

_RULES: tuple[tuple[re.Pattern[str], ErrorBucket, str, str], ...] = (
    # Conversion errors with a literal sentinel value in the message.
    # Example: `Conversion Error: Could not convert string '' to INT64`
    (
        re.compile(
            r"Conversion Error: Could not convert string "
            r"'(?P<value>[^']*)' to (?P<target>\w+)",
        ),
        ErrorBucket.CONVERSION,
        "sentinel_string_to_typed",
        "TRY_CAST(NULLIF(col, '{value}') AS {target})",
    ),
    # Defensive `ts_col = ''` on a TIMESTAMP column.
    (
        re.compile(r"Conversion Error: invalid timestamp field format"),
        ErrorBucket.CONVERSION,
        "empty_string_vs_timestamp",
        "Replace `col = ''` with `col IS NULL` (column is TIMESTAMP)",
    ),
    # CASE branch-type mismatch (Snowflake coerces, DuckDB doesn't).
    (
        re.compile(
            r"(?:Binder Error|BinderException).*"
            r"Cannot mix(?: values of type)? (?P<left>\w+) and (?P<right>\w+).*CASE",
            re.IGNORECASE | re.DOTALL,
        ),
        ErrorBucket.TYPE_MISMATCH,
        "case_mixed_types",
        "Wrap the mismatched branch(es) in CAST(... AS VARCHAR) "
        "or rewrite searched CASE into branches of the same type",
    ),
    # Comparison mismatch (JOIN ON, WHERE col = literal).
    (
        re.compile(
            r"(?:Binder Error|BinderException).*"
            r"Cannot compare values of type (?P<left>\w+) and (?P<right>\w+)",
            re.IGNORECASE | re.DOTALL,
        ),
        ErrorBucket.TYPE_MISMATCH,
        "comparison_type_mismatch",
        "Wrap the VARCHAR-typed operand in TRY_CAST(... AS {right}) "
        "(or swap {left}/{right} depending on which is VARCHAR)",
    ),
    # Function signature mismatch.
    (
        re.compile(
            r"(?:Binder Error|BinderException).*"
            r"No function matches(?: the given name and argument types)? "
            r"'(?P<fn>[A-Za-z_]+)\((?P<args>[^)]*)\)'",
            re.IGNORECASE | re.DOTALL,
        ),
        ErrorBucket.FUNCTION_SIGNATURE,
        "function_arg_type_mismatch",
        "Wrap {fn}'s VARCHAR argument in TRY_CAST(... AS DOUBLE) "
        "or check that the function expects the argument type you're passing",
    ),
    # Idempotence: table already exists without IF NOT EXISTS / OR REPLACE.
    (
        re.compile(
            r"(?:Catalog Error|CatalogException).*"
            r"Table with name (?P<name>[^\s]+) already exists",
            re.IGNORECASE | re.DOTALL,
        ),
        ErrorBucket.IDEMPOTENCE,
        "table_already_exists",
        "Use CREATE OR REPLACE TABLE or run `juncture run` against a fresh database",
    ),
    # Missing table — could be primary or cascade; caller needs DAG context.
    (
        re.compile(
            r"(?:Catalog Error|CatalogException).*"
            r"Table with name (?P<name>[^\s]+) does not exist",
            re.IGNORECASE | re.DOTALL,
        ),
        ErrorBucket.MISSING_OBJECT,
        "table_not_found",
        "Check whether an earlier failing statement should have produced {name} "
        "(cascade), or whether a seed is missing (primary)",
    ),
    # Parser failure (dialect-specific syntax DuckDB refuses).
    (
        re.compile(
            r"(?:Parser Error|ParserException).*",
            re.IGNORECASE | re.DOTALL,
        ),
        ErrorBucket.PARSER,
        "syntax_error",
        "SQLGlot couldn't translate the construct cleanly. "
        "Check for LATERAL FLATTEN, QUALIFY, or ::type casts and rewrite manually",
    ),
)


def classify_error(message: str) -> ErrorClassification:
    """Classify a single DuckDB error string.

    Returns :class:`ErrorClassification` with ``bucket=ErrorBucket.OTHER``
    if no rule matches. The classifier never raises — missing-signature
    is a normal outcome that tells the caller "add a rule for this one".
    """
    text = message or ""
    for pattern, bucket, subcategory, fix_template in _RULES:
        match = pattern.search(text)
        if match is None:
            continue
        operands = {k: v for k, v in match.groupdict().items() if v is not None}
        # Interpolate the template lazily; if a named group is missing the
        # fix_hint falls back to the raw template so we don't KeyError.
        try:
            fix_hint = fix_template.format(**operands)
        except (KeyError, IndexError):
            fix_hint = fix_template
        return ErrorClassification(
            bucket=bucket,
            subcategory=subcategory,
            fix_hint=fix_hint,
            error_message=text,
            operands=operands,
        )
    return ErrorClassification(
        bucket=ErrorBucket.OTHER,
        subcategory="unclassified",
        fix_hint="No rule matched; add a regex to juncture.diagnostics.classifier._RULES",
        error_message=text,
        operands={},
    )


def classify_statement_errors(
    errors: Iterable[object],
) -> list[ErrorClassification]:
    """Classify every ``.error`` on a list of :class:`StatementError`-like objects.

    The parameter is typed loosely so callers don't need to import
    :class:`juncture.adapters.base.StatementError`; any object with an
    ``.error`` string attribute works, as do plain strings.
    """
    out: list[ErrorClassification] = []
    for item in errors:
        if isinstance(item, str):
            out.append(classify_error(item))
        else:
            msg = getattr(item, "error", "") or ""
            out.append(classify_error(str(msg)))
    return out

"""SQL parsing helpers built on SQLGlot.

We use SQLGlot for three things:

1. ``ref('name')`` extraction -- discover DAG dependencies from SQL.
2. Jinja-free dependency inference -- peek at table names used in SELECT/JOIN.
3. Dialect translation -- DuckDB <-> Snowflake <-> BigQuery etc.

The ``ref()`` syntax is our own macro (no Jinja in MVP). In SQL files we accept
``{{ ref('orders') }}`` for dbt familiarity and ``$ref(orders)`` for a simpler
form that doesn't conflict with shells.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

# Two accepted ref() forms. Users can pick either; we rewrite both.
_REF_PATTERN = re.compile(
    r"""
    (?:
        \{\{\s*ref\(\s*['"](?P<jinja>[^'"]+)['"]\s*\)\s*\}\}
      | \$ref\(\s*(?P<bare>[A-Za-z_][A-Za-z0-9_.]*)\s*\)
    )
    """,
    re.VERBOSE,
)


@dataclass(frozen=True, kw_only=True)
class SQLParseResult:
    """Output of :func:`parse_sql` containing deps and rewritten SQL."""

    sql: str
    refs: list[str]
    rewritten: str  # SQL with ref(...) replaced by fully-qualified identifier placeholders


def extract_refs(sql: str) -> list[str]:
    """Return model names referenced via ``ref()`` in ``sql``.

    Order of first appearance is preserved and duplicates removed.
    """
    seen: dict[str, None] = {}
    for match in _REF_PATTERN.finditer(sql):
        name = match.group("jinja") or match.group("bare")
        if name and name not in seen:
            seen[name] = None
    return list(seen.keys())


def render_refs(sql: str, resolver: dict[str, str] | None = None) -> str:
    """Replace ``ref(name)`` tokens with the resolved table identifier.

    ``resolver`` maps model name -> materialized identifier (e.g.
    ``{"orders": "main.orders"}``). If missing the macro is substituted with
    the bare name, which is fine when we control the DuckDB schema.
    """
    resolver = resolver or {}

    def _swap(m: re.Match[str]) -> str:
        name = m.group("jinja") or m.group("bare")
        return resolver.get(name, name)

    return _REF_PATTERN.sub(_swap, sql)


def parse_sql(sql: str, *, dialect: str = "duckdb") -> SQLParseResult:
    """Validate ``sql`` and extract dependencies.

    Parsing is used both for dependency inference and, later, for column-level
    lineage. If SQLGlot cannot parse the query we still extract refs via the
    regex so users aren't blocked on exotic dialects.
    """
    import contextlib

    refs = extract_refs(sql)
    with contextlib.suppress(sqlglot.errors.ParseError):
        sqlglot.parse_one(render_refs(sql), read=dialect)
    return SQLParseResult(sql=sql, refs=refs, rewritten=render_refs(sql))


def translate_sql(sql: str, *, read: str, write: str) -> str:
    """Translate ``sql`` from one dialect to another via SQLGlot.

    Works for the typical DuckDB <-> Snowflake <-> BigQuery <-> Postgres matrix
    for the subset of SQL we support (SELECT, CTE, JOIN, window, most scalars).
    Translation is intentionally a best effort: we return the output even if
    some constructs pass through verbatim.
    """
    if read == write:
        return sql
    try:
        parsed = sqlglot.parse_one(sql, read=read)
    except sqlglot.errors.ParseError as exc:
        raise ValueError(f"Cannot parse SQL in dialect {read!r}: {exc}") from exc
    return parsed.sql(dialect=write)


def extract_table_references(sql: str, *, dialect: str = "duckdb") -> set[str]:
    """Return all table identifiers referenced in ``sql``.

    Useful when users write raw SQL without ref(): we can still detect which
    source tables are being read, even though we can't confirm they are
    Juncture-managed models.
    """
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except sqlglot.errors.ParseError:
        return set()
    return {
        table.name
        for table in tree.find_all(exp.Table)
        if table.name and not table.db  # skip fully-qualified foreign references
    }

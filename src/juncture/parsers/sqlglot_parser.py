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

    Multi-statement scripts are split on top-level semicolons, translated per
    statement, and rejoined. Statements SQLGlot cannot parse are passed through
    verbatim — migrated Snowflake bodies often contain dialect-specific syntax
    (UNPIVOT, AT TIME ZONE, QUALIFY, ``::`` casts) where a verbatim fallback is
    safer than failing the whole body.

    When translating to ``duckdb`` the output is additionally run through
    :func:`harmonize_case_types` so Snowflake's implicit VARCHAR coercion in
    CASE expressions gets explicit ``CAST(n AS VARCHAR)`` wrappers DuckDB
    requires.
    """
    if read == write and write != "duckdb":
        return sql

    statements = split_statements(sql) or [sql]
    translated: list[str] = []
    for stmt in statements:
        stripped = stmt.strip()
        if not stripped:
            continue
        try:
            parsed = sqlglot.parse_one(stripped, read=read)
        except sqlglot.errors.ParseError:
            # Fallback: SQLGlot couldn't parse it. Pass through verbatim rather
            # than failing the whole body; downstream DuckDB will surface a
            # precise error if the statement is actually broken.
            translated.append(stripped)
            continue
        # parse_one is typed as sqlglot.Expr (a broader base) but always returns
        # an Expression subclass at runtime; narrow explicitly for mypy.
        assert isinstance(parsed, exp.Expression)
        if write == "duckdb":
            harmonize_case_types(parsed)
        translated.append(parsed.sql(dialect=write))
    return ";\n".join(translated)


# --- CASE type harmonization (Snowflake implicit VARCHAR coerce -> DuckDB explicit) ---

# Expression classes whose return type is unambiguously a string.
_STRING_PRODUCERS: tuple[type[exp.Expression], ...] = (
    exp.Concat,
    exp.DPipe,
    exp.Substring,
    exp.Upper,
    exp.Lower,
    exp.Initcap,
    exp.Trim,
    exp.Replace,
    exp.RegexpReplace,
    exp.Pad,
    exp.Left,
    exp.Right,
    exp.Chr,
    exp.Repeat,
    exp.ToChar,
)

# Expression classes whose return type is unambiguously numeric.
_NUMERIC_PRODUCERS: tuple[type[exp.Expression], ...] = (
    exp.Add,
    exp.Sub,
    exp.Mul,
    exp.Div,
    exp.Mod,
    exp.Pow,
    exp.Neg,
    exp.Abs,
    exp.Round,
    exp.Ceil,
    exp.Floor,
    exp.Sqrt,
    exp.Length,
)


def _branch_kind(node: exp.Expression | None) -> str:
    """Classify ``node`` as ``STRING``, ``NUMERIC``, ``NULL`` or ``UNKNOWN``.

    Only makes a call when the answer is unambiguous from syntax alone (literal,
    explicit CAST, or a function whose return type is fixed). Column references,
    subqueries, and user-defined functions resolve to ``UNKNOWN`` because we
    don't annotate types against a schema.
    """
    if node is None or isinstance(node, exp.Null):
        return "NULL"
    if isinstance(node, exp.Literal):
        return "STRING" if node.is_string else "NUMERIC"
    if isinstance(node, exp.Cast):
        target = node.args.get("to")
        if isinstance(target, exp.DataType):
            if target.this in (
                exp.DataType.Type.VARCHAR,
                exp.DataType.Type.TEXT,
                exp.DataType.Type.CHAR,
                exp.DataType.Type.NCHAR,
                exp.DataType.Type.NVARCHAR,
            ):
                return "STRING"
            if target.this in (
                exp.DataType.Type.INT,
                exp.DataType.Type.BIGINT,
                exp.DataType.Type.SMALLINT,
                exp.DataType.Type.TINYINT,
                exp.DataType.Type.DECIMAL,
                exp.DataType.Type.DOUBLE,
                exp.DataType.Type.FLOAT,
            ):
                return "NUMERIC"
        return "UNKNOWN"
    if isinstance(node, _STRING_PRODUCERS):
        return "STRING"
    if isinstance(node, _NUMERIC_PRODUCERS):
        return "NUMERIC"
    if isinstance(node, exp.Coalesce):
        kinds = {_branch_kind(arg) for arg in [node.this, *(node.args.get("expressions") or [])]}
        kinds.discard("NULL")
        if kinds == {"STRING"}:
            return "STRING"
        if kinds == {"NUMERIC"}:
            return "NUMERIC"
        return "UNKNOWN"
    if isinstance(node, exp.Case):
        return _case_kind(node)
    return "UNKNOWN"


def _case_kind(case_node: exp.Case) -> str:
    """Classify an entire CASE by the union of its branches."""
    branches: list[exp.Expression] = []
    for if_node in case_node.args.get("ifs") or []:
        branch = if_node.args.get("true")
        if branch is not None:
            branches.append(branch)
    default = case_node.args.get("default")
    if default is not None:
        branches.append(default)
    kinds = {_branch_kind(b) for b in branches}
    kinds.discard("NULL")
    if kinds == {"STRING"}:
        return "STRING"
    if kinds == {"NUMERIC"}:
        return "NUMERIC"
    return "UNKNOWN"


def harmonize_case_types(tree: exp.Expression) -> exp.Expression:
    """Wrap numeric literals in ``CAST(n AS VARCHAR)`` when a CASE mixes them
    with string-producing branches.

    Snowflake auto-coerces ``THEN 0`` to ``'0'`` when other branches are
    VARCHAR; DuckDB is strict and raises ``BinderException: Cannot mix values
    of type VARCHAR and INTEGER_LITERAL``. We fix the literal side because
    that's where the fix is trivially safe — we know its value can always be
    rendered as a string. We deliberately do **not** cast non-literal numeric
    expressions (column refs, arithmetic) because we can't know whether the
    user intended the result to be string or numeric.

    Mutates ``tree`` in place and also returns it for chaining.
    """
    varchar = exp.DataType.build("VARCHAR")
    for case in list(tree.find_all(exp.Case)):
        branches: list[exp.Expression] = []
        for if_node in case.args.get("ifs") or []:
            branch = if_node.args.get("true")
            if branch is not None:
                branches.append(branch)
        default = case.args.get("default")
        if default is not None:
            branches.append(default)

        kinds = {_branch_kind(b) for b in branches}
        kinds.discard("NULL")
        kinds.discard("UNKNOWN")
        if kinds != {"STRING", "NUMERIC"}:
            continue

        for branch in branches:
            if isinstance(branch, exp.Literal) and not branch.is_string:
                branch.replace(exp.Cast(this=branch.copy(), to=varchar.copy()))
    return tree


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


def split_statements(sql: str) -> list[str]:
    """Split a multi-statement SQL script on top-level semicolons.

    Handles single-quoted strings and double-quoted identifiers so that
    semicolons inside them are not treated as separators. Line comments
    ``--`` and block comments ``/* ... */`` are preserved in the output
    (DuckDB is happy to parse them back).

    Kept as a hand-rolled scanner rather than delegating to
    :func:`sqlglot.parse` so migrated Snowflake scripts that SQLGlot can't
    fully parse still split cleanly into per-statement chunks.
    """
    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql)
    in_single = in_double = in_line_comment = in_block_comment = False
    while i < n:
        c = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if in_line_comment:
            buf.append(c)
            if c == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            buf.append(c)
            if c == "*" and nxt == "/":
                buf.append(nxt)
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue
        if in_single:
            buf.append(c)
            if c == "'" and nxt == "'":
                buf.append(nxt)
                i += 2
                continue
            if c == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            buf.append(c)
            if c == '"' and nxt == '"':
                buf.append(nxt)
                i += 2
                continue
            if c == '"':
                in_double = False
            i += 1
            continue
        if c == "-" and nxt == "-":
            in_line_comment = True
            buf.append(c)
            i += 1
            continue
        if c == "/" and nxt == "*":
            in_block_comment = True
            buf.append(c)
            buf.append(nxt)
            i += 2
            continue
        if c == "'":
            in_single = True
            buf.append(c)
            i += 1
            continue
        if c == '"':
            in_double = True
            buf.append(c)
            i += 1
            continue
        if c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements

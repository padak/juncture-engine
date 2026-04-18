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

import logging
import re
from dataclasses import dataclass

import networkx as nx
import sqlglot
from sqlglot import exp

log = logging.getLogger(__name__)

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


def translate_sql(
    sql: str,
    *,
    read: str,
    write: str,
    schema: dict[str, dict[str, str]] | None = None,
) -> str:
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

    When ``schema`` is provided (``{table: {col: type}}``) the output is
    further processed by schema-aware AST passes that fix the
    cross-dialect type-coercion gaps DuckDB enforces but Snowflake does
    not: VARCHAR-on-both-sides joins, aggregates over VARCHAR, timestamp
    arithmetic against integer literals, etc. See
    :func:`harmonize_binary_ops`, :func:`harmonize_function_args`,
    :func:`fix_timestamp_arithmetic`.
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
            if schema:
                parsed = _apply_schema_passes(parsed, schema)
        translated.append(parsed.sql(dialect=write))
    return ";\n".join(translated)


def _apply_schema_passes(tree: exp.Expression, schema: dict[str, dict[str, str]]) -> exp.Expression:
    """Run the schema-aware harmonisation passes in order.

    Isolated so the caller can ignore any single pass's failure: SQLGlot's
    :func:`qualify` is strict about ambiguous column references; for a
    migrated body with dialect-specific syntax a soft fallback to
    un-annotated translation is preferable to aborting the whole file.
    """
    from sqlglot.optimizer.annotate_types import annotate_types
    from sqlglot.optimizer.qualify import qualify
    from sqlglot.schema import MappingSchema

    try:
        mapping = MappingSchema(schema, dialect="duckdb")
        qualified = qualify(tree, schema=mapping, dialect="duckdb")
        annotated = annotate_types(qualified, schema=mapping)
    except Exception as exc:
        # qualify() can raise OptimizeError on ambiguous refs, unknown tables,
        # CROSS JOIN without USING, etc. None of that is fatal to the
        # translation — we just lose the type annotations and the schema
        # passes become no-ops for this statement.
        log.debug("Schema annotation skipped: %s", exc)
        return tree
    harmonize_binary_ops(annotated)
    harmonize_function_args(annotated)
    fix_timestamp_arithmetic(annotated)
    return annotated


# --- Schema-aware AST passes (target: DuckDB strict typing) ---

# DuckDB error messages that map cleanly to a harmonisation rule. These
# type families drive both the AST rewrite logic and the error classifier.
_VARCHAR_TYPES: frozenset[exp.DataType.Type] = frozenset(
    {
        exp.DataType.Type.VARCHAR,
        exp.DataType.Type.TEXT,
        exp.DataType.Type.CHAR,
        exp.DataType.Type.NCHAR,
        exp.DataType.Type.NVARCHAR,
    }
)
_NUMERIC_TYPES: frozenset[exp.DataType.Type] = frozenset(
    {
        exp.DataType.Type.INT,
        exp.DataType.Type.BIGINT,
        exp.DataType.Type.SMALLINT,
        exp.DataType.Type.TINYINT,
        exp.DataType.Type.DECIMAL,
        exp.DataType.Type.DOUBLE,
        exp.DataType.Type.FLOAT,
    }
)
_TEMPORAL_TYPES: frozenset[exp.DataType.Type] = frozenset(
    {
        exp.DataType.Type.DATE,
        exp.DataType.Type.TIMESTAMP,
        exp.DataType.Type.TIMESTAMPTZ,
        exp.DataType.Type.TIMESTAMPNTZ,
        exp.DataType.Type.TIMESTAMPLTZ,
    }
)


def _node_type(node: exp.Expression | None) -> exp.DataType.Type | None:
    """Return a node's annotated primary type, or ``None`` if unknown."""
    if node is None or node.type is None:
        return None
    return node.type.this


def _wrap_try_cast(node: exp.Expression, target: exp.DataType.Type) -> exp.Expression:
    """Wrap ``node`` in a ``TRY_CAST(... AS target)`` expression.

    Use :class:`exp.TryCast` rather than ``Cast(safe=True)`` because the
    latter serialises as plain ``CAST`` in the DuckDB dialect — which is
    strict and defeats the whole point of the wrap.
    """
    dtype = exp.DataType(this=target, expressions=[])
    return exp.TryCast(this=node.copy(), to=dtype.copy())


def harmonize_binary_ops(tree: exp.Expression) -> exp.Expression:
    """Wrap the VARCHAR side of a binary op when the other side is numeric/temporal.

    Taxonomy rows #3, #4, #9, #10 in ``docs/MIGRATION_TIPS.md``: DuckDB
    refuses to compare ``VARCHAR`` against ``BIGINT`` or ``TIMESTAMP``;
    Snowflake coerces silently. For every ``EQ/NEQ/GT/LT/GTE/LTE/Add/Sub``
    whose operands carry annotated types, we wrap the VARCHAR side in
    ``TRY_CAST(… AS <other>)`` so the comparison succeeds and bad values
    become NULL (the triage-safe behaviour).

    The tree must already carry annotations from
    :func:`sqlglot.optimizer.annotate_types`; un-annotated trees are a
    no-op. Mutates in place; returned for chaining.
    """
    binary_ops: tuple[type[exp.Expression], ...] = (
        exp.EQ,
        exp.NEQ,
        exp.GT,
        exp.GTE,
        exp.LT,
        exp.LTE,
    )
    for node in list(tree.find_all(*binary_ops)):
        left, right = node.this, node.expression
        left_t = _node_type(left)
        right_t = _node_type(right)
        if left_t is None or right_t is None:
            continue
        # VARCHAR <-> (numeric | temporal): wrap the VARCHAR side.
        if left_t in _VARCHAR_TYPES and right_t in (_NUMERIC_TYPES | _TEMPORAL_TYPES):
            left.replace(_wrap_try_cast(left, right_t))
        elif right_t in _VARCHAR_TYPES and left_t in (_NUMERIC_TYPES | _TEMPORAL_TYPES):
            right.replace(_wrap_try_cast(right, left_t))
    return tree


# Functions that expect a numeric input — wrap VARCHAR argument in TRY_CAST.
# TRIM / UPPER / LOWER want VARCHAR input; non-varchar args get wrapped the
# other way (numeric -> varchar) but that case is rarer and typically already
# handled by Snowflake-to-DuckDB SQLGlot translation.
_NUMERIC_INPUT_FUNCS: tuple[type[exp.Expression], ...] = (
    exp.Sum,
    exp.Avg,
    exp.Min,  # debatable but DuckDB is permissive; included for symmetry
    exp.Max,
    exp.Round,
    exp.Ceil,
    exp.Floor,
    exp.Abs,
)


def harmonize_function_args(tree: exp.Expression) -> exp.Expression:
    """Wrap VARCHAR arguments of numeric aggregates in ``TRY_CAST(… AS DOUBLE)``.

    Taxonomy row #6 in ``docs/MIGRATION_TIPS.md``:
    ``SUM(varchar_col)`` fails on DuckDB with ``No function matches
    'sum(VARCHAR)'`` while Snowflake auto-coerces. Wrap only the direct
    argument; if the argument is an expression whose output is already
    annotated numeric, leave it alone.
    """
    double_t = exp.DataType.Type.DOUBLE
    for node in list(tree.find_all(*_NUMERIC_INPUT_FUNCS)):
        arg = node.this
        if arg is None:
            continue
        arg_t = _node_type(arg)
        if arg_t is None or arg_t not in _VARCHAR_TYPES:
            continue
        arg.replace(_wrap_try_cast(arg, double_t))
    return tree


def fix_timestamp_arithmetic(tree: exp.Expression) -> exp.Expression:
    """Rewrite ``timestamp ± integer_literal`` to ``timestamp ± INTERVAL 'n' DAY``.

    Taxonomy row #8 in ``docs/MIGRATION_TIPS.md``: Snowflake treats a
    bare integer on the right of a ``TIMESTAMP`` as "that many days";
    DuckDB refuses with ``No function matches '-(TIMESTAMP,
    INTEGER_LITERAL)'``. Requires annotated types so we know which side
    is the TIMESTAMP — without annotations the pass is a no-op (the
    numeric literal could be anything).
    """
    for node in list(tree.find_all(exp.Add, exp.Sub)):
        left, right = node.this, node.expression
        left_t = _node_type(left)
        right_t = _node_type(right)

        # TIMESTAMP/DATE on the left, integer literal on the right.
        if left_t in _TEMPORAL_TYPES and isinstance(right, exp.Literal) and not right.is_string:
            right.replace(_interval_days(right.name))
            continue
        # Integer literal on the left, TIMESTAMP/DATE on the right (rare but
        # Snowflake permits). We commute by swapping operands so the interval
        # is still on the right.
        if right_t in _TEMPORAL_TYPES and isinstance(left, exp.Literal) and not left.is_string:
            interval = _interval_days(left.name)
            # ``a + 1 DAY`` == ``1 DAY + a`` semantically; keep ``-`` direction
            # by not swapping Sub (would flip sign).
            if isinstance(node, exp.Add):
                left.replace(interval)
            # For Sub we leave the expression as-is; fixing ``1 - ts`` is a
            # rewrite with sign inversion and DuckDB's error message on that
            # shape is already actionable for the author.
    return tree


def _interval_days(n: str) -> exp.Interval:
    """Build ``INTERVAL 'n' DAY`` for the given integer literal text."""
    return exp.Interval(
        this=exp.Literal.string(str(int(n))),
        unit=exp.Var(this="DAY"),
    )


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


# --- intra-script statement DAG (used by parallel EXECUTE + split-execute tools) ---

# Regex fallback for statements SQLGlot can't parse. Permissive enough to accept
# quoted identifiers with dots (``"in.c-db.carts"``) and dashes (``"oz-provize"``)
# that migrated Snowflake scripts routinely use.
_CREATE_OUT_RE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?"
    r'(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([A-Za-z0-9_.\-]+)"?',
    re.IGNORECASE,
)
_INSERT_OUT_RE = re.compile(
    r'^\s*INSERT\s+(?:OR\s+(?:REPLACE|IGNORE)\s+)?INTO\s+"?([A-Za-z0-9_.\-]+)"?',
    re.IGNORECASE,
)


@dataclass(frozen=True, kw_only=True)
class StatementNode:
    """One parsed statement inside a multi-statement script.

    Used as the node payload in :func:`build_statement_dag`. ``output`` is the
    name of the table/view the statement writes (``CREATE TABLE X`` /
    ``INSERT INTO X``), or ``None`` for read-only / dialect-only statements
    (e.g. ``SET``, ``USE``, ``BEGIN``). ``inputs`` are the single-part table
    names referenced by the statement — fully-qualified ``schema.table``
    references are skipped by :func:`extract_table_references`.
    """

    index: int
    sql: str
    output: str | None
    inputs: frozenset[str]


def detect_output_table(stmt: str, *, dialect: str = "duckdb") -> str | None:
    """Return the name of the table a DDL/DML statement writes, or ``None``.

    Recognised shapes:

    * ``CREATE [OR REPLACE] [TEMP] TABLE|VIEW [IF NOT EXISTS] <name> ...``
    * ``INSERT [OR REPLACE|IGNORE] INTO <name> ...``

    AST first (via SQLGlot); regex fallback for statements the parser
    rejects. Quoted identifiers keep their inner content (``"in.c-db.carts"``
    → ``in.c-db.carts``) so names match what ``extract_table_references``
    returns on downstream reads.
    """
    try:
        parsed = sqlglot.parse_one(stmt, read=dialect)
    except sqlglot.errors.ParseError:
        parsed = None

    if isinstance(parsed, exp.Create):
        kind = (parsed.args.get("kind") or "").upper()
        if kind in ("TABLE", "VIEW"):
            target = parsed.this
            if isinstance(target, exp.Schema):
                target = target.this
            if isinstance(target, exp.Table) and target.name:
                return target.name
    if isinstance(parsed, exp.Insert):
        target = parsed.this
        if isinstance(target, exp.Schema):
            target = target.this
        if isinstance(target, exp.Table) and target.name:
            return target.name

    for pat in (_CREATE_OUT_RE, _INSERT_OUT_RE):
        m = pat.match(stmt)
        if m:
            return m.group(1)
    return None


def build_statement_dag(sql: str, *, dialect: str = "duckdb") -> nx.DiGraph:
    """Build the intra-script dependency DAG of a multi-statement SQL body.

    The returned graph has one node per non-empty statement, keyed by its
    0-based index, with a :class:`StatementNode` attached as ``node["node"]``.
    An edge ``u -> v`` means statement ``u`` produced a table that statement
    ``v`` reads; only the *latest* producer before a read wires the edge, so
    scripts that rewrite the same table repeatedly still linearise correctly.

    Inputs that were never produced by an earlier statement (i.e. external
    tables — seeds, source data) contribute no edge: they are implicit roots
    of the layer-0 set.

    Callers typically feed the result to :func:`networkx.topological_generations`
    to iterate parallelisable layers.
    """
    graph: nx.DiGraph = nx.DiGraph()
    produced_by: dict[str, int] = {}

    for idx, stmt in enumerate(split_statements(sql)):
        output = detect_output_table(stmt, dialect=dialect)
        # extract_table_references walks every exp.Table node, including the
        # CREATE/INSERT target — strip it so INSERT INTO t SELECT * FROM t
        # doesn't turn into a spurious self-input.
        raw_inputs = extract_table_references(stmt, dialect=dialect)
        if output is not None:
            raw_inputs = raw_inputs - {output}
        inputs = frozenset(raw_inputs)
        node = StatementNode(index=idx, sql=stmt, output=output, inputs=inputs)
        graph.add_node(idx, node=node)
        for inp in inputs:
            src = produced_by.get(inp)
            if src is not None and src != idx:
                graph.add_edge(src, idx, via=inp)
        if output is not None:
            produced_by[output] = idx

    return graph

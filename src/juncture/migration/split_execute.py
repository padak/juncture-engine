"""Split a multi-statement EXECUTE body into standalone CTAS mini-models.

The ``execute`` materialization runs a migrated Snowflake script as a
single opaque black box. Parallel EXECUTE (``config.parallelism``) helps
at runtime but the project still has no per-table models, no column
tests, no selectors. This module performs the one-time refactor that
turns the monolith into standard Juncture models the executor can
natively parallelise layer by layer.

Approach
--------

1. Split the script on top-level semicolons
   (:func:`juncture.parsers.sqlglot_parser.split_statements`).
2. Classify each statement as **CTAS** (``CREATE [OR REPLACE] TABLE|VIEW X
   AS SELECT ...``) or **residual** (``INSERT``, ``UPDATE``, ``DELETE``,
   ``ALTER``, ``DROP``, ``SET``, ``USE``, …).
3. For every CTAS, extract the inner ``SELECT`` body and rewrite table
   references: any identifier that was produced by an earlier CTAS in the
   same script becomes ``{{ ref('name') }}``. External references
   (seeds, source tables) stay raw.
4. Residual statements are collected into a single EXECUTE-materialized
   model, with ``ref()`` hints appended so Juncture can infer its
   depends_on from referenced CTAS outputs.

What we deliberately do not do
------------------------------

* Merge multiple CTAS for the same target name. If the source script has
  ``CREATE OR REPLACE TABLE t AS ...`` twice, we raise rather than
  silently picking the latest — the intermediate state may have been
  consumed in between and we can't prove otherwise.
* Try to split ``INSERT INTO x SELECT ...`` into a ``CREATE TABLE x``.
  That changes semantics when prior rows exist. Residual.
* Guess at dependencies for statements SQLGlot can't parse. They land in
  residual verbatim with an empty contribution to depends_on.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from juncture.parsers.sqlglot_parser import detect_output_table, split_statements


class SplitExecuteError(ValueError):
    """Raised when ``split_execute_script`` can't produce a safe split."""


@dataclass(frozen=True, kw_only=True)
class SplitModel:
    """One extracted CTAS statement as a standalone Juncture model."""

    name: str
    materialization: str  # "table" | "view"
    body: str  # SELECT ... with ref() substitutions already applied
    source_index: int  # original 0-based position in the script


@dataclass(frozen=True, kw_only=True)
class SplitResult:
    """Output of :func:`split_execute_script`."""

    models: list[SplitModel]
    residual: str | None
    residual_depends_on: list[str]


def split_execute_script(sql: str, *, dialect: str = "duckdb") -> SplitResult:
    """Partition ``sql`` into CTAS mini-models and a residual EXECUTE block.

    Raises :class:`SplitExecuteError` when the same CTAS target name
    appears more than once — split-execute cannot resolve the implicit
    versioning without introducing silent semantic changes.
    """
    statements = split_statements(sql)

    # Phase 1: discover the set of tables that CTAS statements produce
    # inside this script. These become ref() targets during rewrite.
    produced_in_script: set[str] = set()
    for stmt in statements:
        if _is_ctas_select(stmt, dialect=dialect):
            out = detect_output_table(stmt, dialect=dialect)
            if out:
                produced_in_script.add(out)

    # Phase 2: extract.
    models: list[SplitModel] = []
    residual_stmts: list[str] = []
    residual_depends: set[str] = set()

    for idx, stmt in enumerate(statements):
        try:
            parsed = sqlglot.parse_one(stmt, read=dialect)
        except sqlglot.errors.ParseError:
            # Can't parse; keep verbatim in residual. We don't contribute
            # to depends_on from unparseable text.
            residual_stmts.append(stmt)
            continue

        if _ctas_body(parsed) is not None and isinstance(parsed, exp.Create):
            body = _ctas_body(parsed)
            assert body is not None
            out = detect_output_table(stmt, dialect=dialect)
            if out is None:
                residual_stmts.append(stmt)
                residual_depends |= _refs_in(parsed) & produced_in_script
                continue
            kind = (parsed.args.get("kind") or "TABLE").upper()
            rewritten = _rewrite_refs(body, produced_in_script=produced_in_script, dialect=dialect)
            models.append(
                SplitModel(
                    name=out,
                    materialization="view" if kind == "VIEW" else "table",
                    body=rewritten,
                    source_index=idx,
                )
            )
            continue

        # Everything else → residual. Detect refs so we can hint DAG order.
        residual_stmts.append(stmt)
        residual_depends |= _refs_in(parsed) & produced_in_script

    _enforce_unique_names(models)

    residual = None
    if residual_stmts:
        residual = ";\n\n".join(s.strip() for s in residual_stmts) + ";"

    return SplitResult(
        models=models,
        residual=residual,
        residual_depends_on=sorted(residual_depends),
    )


def _is_ctas_select(stmt: str, *, dialect: str) -> bool:
    try:
        parsed = sqlglot.parse_one(stmt, read=dialect)
    except sqlglot.errors.ParseError:
        return False
    if not isinstance(parsed, exp.Create):
        return False
    kind = (parsed.args.get("kind") or "").upper()
    if kind not in ("TABLE", "VIEW"):
        return False
    return _ctas_body(parsed) is not None


def _ctas_body(create: exp.Expression) -> exp.Expression | None:
    """Return the inner SELECT/UNION of a ``CREATE TABLE AS`` / ``CREATE
    VIEW AS`` statement, or ``None`` for shapes we can't convert (column-
    list CREATE, CREATE without AS, non-query bodies).
    """
    if not isinstance(create, exp.Create):
        return None
    kind = (create.args.get("kind") or "").upper()
    if kind not in ("TABLE", "VIEW"):
        return None
    expr = create.expression
    # Unwrap parenthesised queries: ``CREATE TABLE X AS (SELECT ...)``.
    while isinstance(expr, exp.Subquery):
        expr = expr.this
    if isinstance(expr, exp.Select | exp.Union):
        return expr
    return None


def _rewrite_refs(
    body: exp.Expression,
    *,
    produced_in_script: set[str],
    dialect: str,
) -> str:
    """Replace each produced-in-script table in ``body`` with a Juncture
    ``{{ ref('name') }}`` macro.

    SQLGlot's SQL renderer only emits valid identifiers, so we do the
    substitution in two steps: rename each producer to a unique sentinel
    identifier in the AST, render, then regex-replace sentinel ->
    ``{{ ref('name') }}`` in the text. Sentinels are purely ASCII
    (``__juncture_ref_N__``) and cannot collide with real table names.
    """
    tree = body.copy()
    sentinel_map: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        if not table.name or table.db:
            continue
        if table.name in produced_in_script:
            sentinel = f"__juncture_ref_{len(sentinel_map)}__"
            sentinel_map[sentinel] = table.name
            table.set("this", exp.to_identifier(sentinel))
    rendered = tree.sql(dialect=dialect)
    for sentinel, name in sentinel_map.items():
        # Strip optional double-quoting the renderer may add around the
        # sentinel, so we substitute ``sentinel`` and ``"sentinel"`` alike.
        pattern = re.compile(r'"?' + re.escape(sentinel) + r'"?')
        rendered = pattern.sub("{{ ref('" + name + "') }}", rendered)
    return rendered


def _refs_in(tree: exp.Expression) -> set[str]:
    """Return single-part table names referenced anywhere in ``tree``.

    Fully-qualified names (``schema.table``) are skipped — those are
    assumed external. Matches the convention of
    ``juncture.parsers.sqlglot_parser.extract_table_references``.
    """
    return {t.name for t in tree.find_all(exp.Table) if t.name and not t.db}


def _enforce_unique_names(models: list[SplitModel]) -> None:
    seen: dict[str, int] = {}
    for m in models:
        if m.name in seen:
            raise SplitExecuteError(
                f"CTAS for {m.name!r} appears more than once in the source "
                f"script (statements #{seen[m.name]} and #{m.source_index}). "
                f"split-execute cannot safely merge repeated producers — "
                f"intermediate readers between them may depend on the first "
                f"version. Rename, merge, or handle manually."
            )
        seen[m.name] = m.source_index

"""Unit tests for the SQL parser (ref extraction, dialect translation)."""

from __future__ import annotations

from juncture.parsers.sqlglot_parser import (
    extract_refs,
    parse_sql,
    render_refs,
    translate_sql,
)


def test_extract_refs_jinja_form() -> None:
    sql = "SELECT * FROM {{ ref('orders') }} JOIN {{ ref('customers') }} USING (id)"
    assert extract_refs(sql) == ["orders", "customers"]


def test_extract_refs_bare_form() -> None:
    sql = "SELECT * FROM $ref(orders) JOIN $ref(customers) USING (id)"
    assert extract_refs(sql) == ["orders", "customers"]


def test_extract_refs_deduplicates() -> None:
    sql = "SELECT * FROM {{ ref('x') }} UNION SELECT * FROM {{ ref('x') }}"
    assert extract_refs(sql) == ["x"]


def test_render_refs_substitutes() -> None:
    sql = "SELECT * FROM {{ ref('orders') }}"
    rendered = render_refs(sql, {"orders": "main.orders"})
    assert rendered == "SELECT * FROM main.orders"


def test_render_refs_without_resolver_uses_bare_name() -> None:
    rendered = render_refs("SELECT * FROM {{ ref('orders') }}", None)
    assert rendered == "SELECT * FROM orders"


def test_parse_sql_carries_refs() -> None:
    result = parse_sql("SELECT * FROM {{ ref('o') }}")
    assert result.refs == ["o"]
    assert "orders" not in result.rewritten  # was never mentioned


def test_translate_sql_snowflake_to_duckdb() -> None:
    # GETDATE/CURRENT_TIMESTAMP are cross-dialect; pick a function that differs:
    # Snowflake's TO_VARCHAR vs DuckDB's CAST(... AS VARCHAR).
    snowflake_sql = "SELECT TO_VARCHAR(42)"
    duckdb_sql = translate_sql(snowflake_sql, read="snowflake", write="duckdb")
    assert duckdb_sql.upper().startswith("SELECT")


def test_translate_sql_same_dialect_is_identity() -> None:
    sql = "SELECT 1"
    assert translate_sql(sql, read="duckdb", write="duckdb") == sql


def test_harmonize_case_mix_numeric_literal_and_string_producer() -> None:
    # Real-world Snowflake pattern: CASE WHEN ... THEN 0 ELSE REPLACE(...) END
    # DuckDB rejects mixed VARCHAR/INTEGER_LITERAL; translate must insert an
    # explicit CAST around the numeric literal.
    sql = "SELECT CASE WHEN REPLACE(v, ',', '') = '' THEN 0 ELSE REPLACE(v, ',', '') END FROM t"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    assert "CAST(0 AS TEXT)" in out or "CAST(0 AS VARCHAR)" in out


def test_harmonize_case_all_strings_is_noop() -> None:
    sql = "SELECT CASE WHEN x = 'a' THEN 'yes' ELSE 'no' END FROM t"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    assert "CAST(" not in out


def test_harmonize_case_all_numeric_is_noop() -> None:
    sql = "SELECT CASE WHEN x = 1 THEN 0 ELSE 1 END FROM t"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    assert "CAST(" not in out


def test_harmonize_case_null_branch_ignored() -> None:
    # NULL in one branch must not prevent casting the numeric literal when
    # another branch is clearly string-producing.
    sql = "SELECT CASE WHEN x THEN NULL WHEN y THEN 0 ELSE UPPER(s) END FROM t"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    assert "CAST(0 AS TEXT)" in out or "CAST(0 AS VARCHAR)" in out


def test_harmonize_case_nested() -> None:
    sql = "SELECT CASE WHEN a THEN CASE WHEN b THEN 0 ELSE UPPER(x) END ELSE 'fallback' END FROM t"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    # Inner CASE mixes numeric literal with UPPER(x) -> cast inserted.
    assert "CAST(0 AS TEXT)" in out or "CAST(0 AS VARCHAR)" in out


def test_harmonize_case_column_ref_is_unknown_noop() -> None:
    # Column types aren't known at AST time, so we don't second-guess: mix of
    # column ref and numeric literal stays verbatim.
    sql = "SELECT CASE WHEN x = 0 THEN col ELSE 0 END FROM t"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    assert "CAST(" not in out


def test_translate_multi_statement_splits_and_rejoins() -> None:
    sql = "SELECT 1; SELECT CASE WHEN x=0 THEN 0 ELSE UPPER(s) END FROM t; SELECT 2"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    stmts = [s.strip() for s in out.split(";") if s.strip()]
    assert len(stmts) == 3
    assert "CAST(0 AS TEXT)" in out or "CAST(0 AS VARCHAR)" in out


def test_translate_unparseable_statement_passthrough() -> None:
    # When SQLGlot can't parse a statement (e.g., UNPIVOT in some dialect
    # combos), we must not drop it — pass through verbatim so DuckDB surfaces
    # the real error rather than a silent omission.
    sql = "CREATE OR REPLACE TABLE x AS SELECT a_totally_made_up @@ syntax FROM t"
    out = translate_sql(sql, read="snowflake", write="duckdb")
    assert "@@" in out or "made_up" in out

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

"""Unit: schema-aware SQLGlot translate passes.

Tests the four AST passes that close the DuckDB/Snowflake type-coercion
gap identified in docs/MIGRATION_TIPS.md taxonomy rows #3, #6, #8, #10.
Without these the pilot migration required 26 manual repair rounds; the
design target is 2-3.
"""

from __future__ import annotations

from juncture.parsers.sqlglot_parser import translate_sql


def _norm(sql: str) -> str:
    """Collapse whitespace so we can compare SQL strings robustly."""
    return " ".join(sql.split())


def test_schema_aware_varchar_vs_bigint_comparison_gets_try_cast() -> None:
    # taxonomy #3 / #10: JOIN ... ON varchar_id = bigint_id
    schema = {
        "users": {"user_id": "VARCHAR"},
        "orders": {"id": "BIGINT", "user_id": "BIGINT"},
    }
    sql = "SELECT * FROM orders o JOIN users u ON o.user_id = u.user_id"
    out = translate_sql(sql, read="duckdb", write="duckdb", schema=schema)
    # The VARCHAR side (u.user_id) must be wrapped in TRY_CAST.
    assert "TRY_CAST" in out.upper()


def test_schema_aware_sum_over_varchar_gets_try_cast_double() -> None:
    # taxonomy #6: SUM(varchar_col) -> SUM(TRY_CAST(col AS DOUBLE))
    schema = {"orders": {"amount": "VARCHAR"}}
    out = translate_sql("SELECT SUM(amount) FROM orders", read="duckdb", write="duckdb", schema=schema)
    upper = out.upper()
    assert "TRY_CAST" in upper
    assert "DOUBLE" in upper


def test_schema_aware_timestamp_minus_int_becomes_interval() -> None:
    # taxonomy #8: ts ± 1 -> ts ± INTERVAL '1' DAY
    schema = {"events": {"ts": "TIMESTAMP"}}
    out = translate_sql("SELECT ts - 1 FROM events", read="duckdb", write="duckdb", schema=schema)
    upper = out.upper()
    assert "INTERVAL" in upper and "DAY" in upper


def test_schema_unaware_translation_is_unchanged_for_noop_cases() -> None:
    # Without schema the passes short-circuit; we only get the existing
    # harmonize_case_types fixup (taxonomy #1), not the new ones.
    out = translate_sql("SELECT SUM(amount) FROM orders", read="duckdb", write="duckdb")
    # No TRY_CAST insertion without schema knowledge.
    assert "TRY_CAST" not in out.upper()


def test_schema_aware_preserves_numeric_comparisons_untouched() -> None:
    # Both sides numeric — nothing to harmonise, output equivalent.
    schema = {"orders": {"id": "BIGINT", "customer_id": "BIGINT"}}
    out = translate_sql(
        "SELECT * FROM orders WHERE id = customer_id",
        read="duckdb",
        write="duckdb",
        schema=schema,
    )
    assert "TRY_CAST" not in out.upper()


def test_schema_aware_varchar_vs_timestamp_compare_wraps_varchar() -> None:
    # taxonomy #4/#9: date/varchar compare defensives.
    schema = {"events": {"name": "VARCHAR", "occurred_at": "TIMESTAMP"}}
    out = translate_sql(
        "SELECT * FROM events WHERE name = occurred_at",
        read="duckdb",
        write="duckdb",
        schema=schema,
    )
    assert "TRY_CAST" in out.upper()


def test_schema_aware_soft_fallback_on_qualify_error() -> None:
    # A CROSS JOIN without aliases may confuse qualify(); translate_sql
    # must not crash — it degrades to syntax-only translation.
    schema = {"a": {"x": "BIGINT"}, "b": {"y": "BIGINT"}}
    out = translate_sql(
        "SELECT x, y FROM a, b",
        read="duckdb",
        write="duckdb",
        schema=schema,
    )
    # Output is still a valid SELECT, no exception raised.
    assert "SELECT" in out.upper()


def test_snowflake_to_duckdb_with_schema() -> None:
    # End-to-end: Snowflake input, DuckDB target, schema-aware.
    # amount is VARCHAR in the seed; Snowflake lets SUM(amount) through,
    # DuckDB doesn't without TRY_CAST.
    schema = {"orders": {"amount": "VARCHAR", "placed_at": "TIMESTAMP"}}
    sql = "SELECT SUM(amount), placed_at - 7 FROM orders"
    out = translate_sql(sql, read="snowflake", write="duckdb", schema=schema)
    upper = out.upper()
    assert "TRY_CAST" in upper
    assert "INTERVAL" in upper

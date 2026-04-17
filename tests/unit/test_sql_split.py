"""Unit tests for the top-level SQL statement splitter used by EXECUTE materialization."""

from __future__ import annotations

from juncture.adapters.duckdb_adapter import _split_sql_statements


def test_simple_split() -> None:
    sql = "SELECT 1; SELECT 2; SELECT 3"
    assert _split_sql_statements(sql) == ["SELECT 1", "SELECT 2", "SELECT 3"]


def test_empty_trailing_semicolon() -> None:
    assert _split_sql_statements("SELECT 1;") == ["SELECT 1"]
    assert _split_sql_statements(";;;SELECT 1;;;") == ["SELECT 1"]


def test_semicolon_inside_string_literal() -> None:
    sql = "SELECT ';not a sep;' AS x; SELECT 2"
    parts = _split_sql_statements(sql)
    assert len(parts) == 2
    assert "';not a sep;'" in parts[0]


def test_semicolon_inside_quoted_identifier() -> None:
    sql = 'CREATE TABLE "a;b" AS SELECT 1; SELECT 2'
    parts = _split_sql_statements(sql)
    assert len(parts) == 2
    assert '"a;b"' in parts[0]


def test_line_comment_is_preserved() -> None:
    sql = "-- comment with ; inside\nSELECT 1; SELECT 2"
    parts = _split_sql_statements(sql)
    assert len(parts) == 2
    assert parts[0].startswith("-- comment")


def test_block_comment_spans_semicolon() -> None:
    sql = "/* big comment ; ; ;*/ SELECT 1; SELECT 2"
    parts = _split_sql_statements(sql)
    assert len(parts) == 2


def test_escaped_single_quote() -> None:
    sql = "SELECT 'it''s fine;' AS x; SELECT 2"
    parts = _split_sql_statements(sql)
    assert len(parts) == 2

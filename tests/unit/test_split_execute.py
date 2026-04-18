"""Tests for :mod:`juncture.migration.split_execute`."""

from __future__ import annotations

import pytest

from juncture.migration.split_execute import (
    SplitExecuteError,
    split_execute_script,
)


class TestBasicExtraction:
    def test_single_ctas(self) -> None:
        result = split_execute_script("CREATE TABLE a AS SELECT 1 AS x")
        assert len(result.models) == 1
        assert result.models[0].name == "a"
        assert result.models[0].materialization == "table"
        assert "SELECT 1 AS x" in result.models[0].body
        assert result.residual is None

    def test_create_or_replace(self) -> None:
        result = split_execute_script("CREATE OR REPLACE TABLE a AS SELECT 1")
        assert [m.name for m in result.models] == ["a"]

    def test_view(self) -> None:
        result = split_execute_script("CREATE VIEW v AS SELECT 1")
        assert result.models[0].materialization == "view"

    def test_preserves_source_index(self) -> None:
        sql = "CREATE TABLE a AS SELECT 1;CREATE TABLE b AS SELECT 2"
        result = split_execute_script(sql)
        assert [m.source_index for m in result.models] == [0, 1]


class TestRefRewriting:
    def test_chain_uses_ref_macros(self) -> None:
        sql = (
            "CREATE TABLE a AS SELECT 1 AS x;"
            "CREATE TABLE b AS SELECT x FROM a;"
            "CREATE TABLE c AS SELECT x FROM b"
        )
        result = split_execute_script(sql)
        by_name = {m.name: m for m in result.models}
        assert "{{ ref('a') }}" in by_name["b"].body
        assert "{{ ref('b') }}" in by_name["c"].body
        # "a" itself references no produced table, no ref().
        assert "ref(" not in by_name["a"].body

    def test_external_table_stays_raw(self) -> None:
        # `orders` is never produced in the script → stays raw (seed-like).
        sql = "CREATE TABLE stg AS SELECT id FROM orders"
        result = split_execute_script(sql)
        body = result.models[0].body
        assert "orders" in body
        assert "ref(" not in body

    def test_diamond(self) -> None:
        sql = (
            "CREATE TABLE a AS SELECT 1 AS x;"
            "CREATE TABLE b AS SELECT x FROM a;"
            "CREATE TABLE c AS SELECT x FROM a;"
            "CREATE TABLE d AS SELECT b.x FROM b JOIN c ON b.x = c.x"
        )
        result = split_execute_script(sql)
        by_name = {m.name: m for m in result.models}
        assert "{{ ref('a') }}" in by_name["b"].body
        assert "{{ ref('a') }}" in by_name["c"].body
        assert "{{ ref('b') }}" in by_name["d"].body
        assert "{{ ref('c') }}" in by_name["d"].body

    def test_mixed_produced_and_external_refs(self) -> None:
        sql = (
            "CREATE TABLE stg AS SELECT id FROM source_table;"
            "CREATE TABLE final_t AS SELECT s.id FROM stg s JOIN seeds_dim d ON s.id = d.id"
        )
        result = split_execute_script(sql)
        out_body = next(m.body for m in result.models if m.name == "final_t")
        # stg is produced → ref; seeds_dim is external → raw.
        assert "{{ ref('stg') }}" in out_body
        assert "seeds_dim" in out_body
        assert "ref('seeds_dim')" not in out_body


class TestResidual:
    def test_insert_goes_to_residual(self) -> None:
        sql = "CREATE TABLE a AS SELECT 1 AS x;INSERT INTO a VALUES (2)"
        result = split_execute_script(sql)
        assert [m.name for m in result.models] == ["a"]
        assert result.residual is not None
        assert "INSERT INTO a" in result.residual

    def test_residual_depends_on_produced_refs(self) -> None:
        sql = (
            "CREATE TABLE src AS SELECT 1 AS id, 'a' AS v;"
            "CREATE TABLE other AS SELECT 1 AS id;"
            "UPDATE other SET id = id + 1 WHERE id IN (SELECT id FROM src)"
        )
        result = split_execute_script(sql)
        # Both src and other are produced. UPDATE reads both, so the
        # residual depends on both.
        assert set(result.residual_depends_on) == {"src", "other"}

    def test_residual_ignores_external_refs(self) -> None:
        sql = "CREATE TABLE built AS SELECT 1 AS x;UPDATE external_t SET x = (SELECT x FROM built)"
        result = split_execute_script(sql)
        # external_t is not produced in-script → not in depends_on.
        assert result.residual_depends_on == ["built"]

    def test_drop_set_use_all_residual(self) -> None:
        sql = "SET memory_limit = '4GB';CREATE TABLE a AS SELECT 1;DROP TABLE IF EXISTS old_t"
        result = split_execute_script(sql)
        assert [m.name for m in result.models] == ["a"]
        assert result.residual is not None
        assert "SET memory_limit" in result.residual
        assert "DROP TABLE" in result.residual.upper()


class TestEdgeCases:
    def test_parenthesised_ctas_body(self) -> None:
        result = split_execute_script("CREATE TABLE a AS (SELECT 1 AS x)")
        assert len(result.models) == 1
        assert "SELECT 1" in result.models[0].body

    def test_duplicate_ctas_raises(self) -> None:
        sql = "CREATE OR REPLACE TABLE t AS SELECT 1;CREATE OR REPLACE TABLE t AS SELECT 2"
        with pytest.raises(SplitExecuteError, match="appears more than once"):
            split_execute_script(sql)

    def test_unparseable_statement_goes_to_residual_verbatim(self) -> None:
        # Snowflake-specific QUALIFY isn't in every SQLGlot release. If the
        # parser rejects, the statement still shows up in residual as-is.
        quirky = "-- snowflake ONLY\nMERGE INTO tgt USING src ON tgt.id = src.id"
        sql = "CREATE TABLE a AS SELECT 1;" + quirky
        result = split_execute_script(sql)
        # Either the quirky statement parsed as something sensible (→ residual)
        # or failed (→ residual verbatim). Both end up in residual.
        assert "a" in {m.name for m in result.models}
        assert result.residual is not None
        assert "MERGE INTO tgt" in result.residual

    def test_quoted_identifier_with_dot(self) -> None:
        # Keboola-style "out.campaigns" must survive round-trip:
        # both the model name and the ref() substitution use the bare form.
        sql = (
            'CREATE TABLE "out.campaigns" AS SELECT 1 AS x;'
            'CREATE TABLE downstream AS SELECT x FROM "out.campaigns"'
        )
        result = split_execute_script(sql)
        names = {m.name for m in result.models}
        assert "out.campaigns" in names
        downstream = next(m for m in result.models if m.name == "downstream")
        assert "{{ ref('out.campaigns') }}" in downstream.body

    def test_column_list_create_is_residual(self) -> None:
        # CREATE TABLE X (col INT) has no query body — can't become a model.
        sql = "CREATE TABLE fixed (id INT, name VARCHAR);INSERT INTO fixed VALUES (1, 'a')"
        result = split_execute_script(sql)
        assert result.models == []
        assert result.residual is not None
        assert "CREATE TABLE fixed" in result.residual
        assert "INSERT INTO fixed" in result.residual

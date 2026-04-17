"""Tests for :mod:`juncture.parsers.sqlglot_parser` statement-DAG helpers."""

from __future__ import annotations

import networkx as nx

from juncture.parsers.sqlglot_parser import (
    StatementNode,
    build_statement_dag,
    detect_output_table,
)


class TestDetectOutputTable:
    def test_create_table(self) -> None:
        assert detect_output_table("CREATE TABLE orders AS SELECT 1 AS id") == "orders"

    def test_create_or_replace_table(self) -> None:
        assert detect_output_table("CREATE OR REPLACE TABLE orders AS SELECT 1") == "orders"

    def test_create_temp_table(self) -> None:
        assert detect_output_table("CREATE TEMPORARY TABLE t AS SELECT 1") == "t"
        assert detect_output_table("CREATE TEMP TABLE t AS SELECT 1") == "t"

    def test_create_view(self) -> None:
        assert detect_output_table("CREATE VIEW v AS SELECT 1") == "v"
        assert detect_output_table("CREATE OR REPLACE VIEW v AS SELECT 1") == "v"

    def test_create_if_not_exists(self) -> None:
        assert detect_output_table("CREATE TABLE IF NOT EXISTS t AS SELECT 1") == "t"

    def test_insert_into(self) -> None:
        assert detect_output_table("INSERT INTO orders VALUES (1, 2)") == "orders"

    def test_insert_or_replace_into(self) -> None:
        # DuckDB extension — regex fallback must catch it even if AST fails.
        assert detect_output_table("INSERT OR REPLACE INTO orders VALUES (1)") == "orders"

    def test_quoted_identifier_with_dot(self) -> None:
        # Slevomat-style identifiers: Storage-bucket-prefixed names like
        # "in.c-db.carts" must round-trip through both detection and
        # extract_table_references so downstream reads match.
        assert detect_output_table('CREATE TABLE "in.c-db.carts" AS SELECT 1') == "in.c-db.carts"

    def test_quoted_identifier_with_dash(self) -> None:
        assert detect_output_table('CREATE TABLE "oz-provize" AS SELECT 1') == "oz-provize"

    def test_select_returns_none(self) -> None:
        assert detect_output_table("SELECT * FROM orders") is None

    def test_set_or_use_returns_none(self) -> None:
        assert detect_output_table("SET memory_limit = '16GB'") is None

    def test_unparseable_still_regex_matches(self) -> None:
        # Snowflake-specific syntax SQLGlot may or may not parse — the regex
        # fallback guarantees we still attribute the output.
        weird = (
            "CREATE OR REPLACE TABLE quarterly AS\n"
            "SELECT * FROM events QUALIFY ROW_NUMBER() OVER (ORDER BY ts) = 1"
        )
        assert detect_output_table(weird) == "quarterly"


class TestBuildStatementDag:
    def test_empty_script(self) -> None:
        g = build_statement_dag("")
        assert g.number_of_nodes() == 0
        assert g.number_of_edges() == 0

    def test_single_statement(self) -> None:
        g = build_statement_dag("CREATE TABLE a AS SELECT 1 AS x")
        assert g.number_of_nodes() == 1
        assert g.number_of_edges() == 0
        node: StatementNode = g.nodes[0]["node"]
        assert node.output == "a"
        assert node.inputs == frozenset()

    def test_three_stage_chain(self) -> None:
        sql = (
            "CREATE TABLE a AS SELECT 1 AS x;"
            "CREATE TABLE b AS SELECT x FROM a;"
            "CREATE TABLE c AS SELECT x FROM b;"
        )
        g = build_statement_dag(sql)
        assert g.number_of_nodes() == 3
        assert list(g.edges()) == [(0, 1), (1, 2)]
        # Three statements chained means three layers, one per statement.
        layers = list(nx.topological_generations(g))
        assert [sorted(layer) for layer in layers] == [[0], [1], [2]]

    def test_independent_statements_share_layer(self) -> None:
        sql = (
            "CREATE TABLE a AS SELECT 1;"
            "CREATE TABLE b AS SELECT 2;"
            "CREATE TABLE c AS SELECT 3;"
        )
        g = build_statement_dag(sql)
        assert g.number_of_edges() == 0
        layers = list(nx.topological_generations(g))
        assert len(layers) == 1
        assert sorted(layers[0]) == [0, 1, 2]

    def test_external_table_is_root(self) -> None:
        # ``orders`` is never produced inside the script — it's a seed/source.
        # The second statement has it as input but gets no intra-script edge.
        sql = (
            "CREATE TABLE stg AS SELECT * FROM orders;"
            "CREATE TABLE fact AS SELECT * FROM stg;"
        )
        g = build_statement_dag(sql)
        assert list(g.edges()) == [(0, 1)]
        layers = list(nx.topological_generations(g))
        assert [sorted(layer) for layer in layers] == [[0], [1]]

    def test_diamond(self) -> None:
        # a -> b, a -> c, b+c -> d
        sql = (
            "CREATE TABLE a AS SELECT 1 AS x;"
            "CREATE TABLE b AS SELECT x FROM a;"
            "CREATE TABLE c AS SELECT x FROM a;"
            "CREATE TABLE d AS SELECT b.x FROM b JOIN c ON b.x = c.x;"
        )
        g = build_statement_dag(sql)
        assert set(g.edges()) == {(0, 1), (0, 2), (1, 3), (2, 3)}
        layers = list(nx.topological_generations(g))
        assert [sorted(layer) for layer in layers] == [[0], [1, 2], [3]]

    def test_rewrite_uses_latest_producer(self) -> None:
        # When the same table is rewritten, the read wires to the most recent
        # producer. Downstream readers before the rewrite depend on the first
        # producer; after the rewrite they depend on the second.
        sql = (
            "CREATE OR REPLACE TABLE t AS SELECT 1 AS x;"
            "CREATE TABLE r1 AS SELECT x FROM t;"
            "CREATE OR REPLACE TABLE t AS SELECT 2 AS x;"
            "CREATE TABLE r2 AS SELECT x FROM t;"
        )
        g = build_statement_dag(sql)
        assert (0, 1) in g.edges()
        assert (2, 3) in g.edges()
        # r2 must NOT depend on statement 0: the rewrite at 2 supersedes it.
        assert (0, 3) not in g.edges()

    def test_node_carries_sql_and_inputs(self) -> None:
        sql = "CREATE TABLE a AS SELECT 1;CREATE TABLE b AS SELECT x FROM a"
        g = build_statement_dag(sql)
        node_b: StatementNode = g.nodes[1]["node"]
        assert node_b.index == 1
        assert node_b.output == "b"
        assert "a" in node_b.inputs
        assert node_b.sql.startswith("CREATE TABLE b")

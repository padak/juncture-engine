"""Unit tests for juncture.migration.keboola_sync_pull."""

from __future__ import annotations

from pathlib import Path

import pytest

from juncture.migration import migrate_keboola_sync_pull


def _make_sync_pull_tree(tmp_path: Path, *, sql_body: str) -> tuple[Path, Path]:
    """Create a fake sync-pull dir and a matching parquet seed source tree.

    Returns (transformation_dir, seeds_source_dir).
    """
    tx = tmp_path / "main/transformation/keboola.snowflake-transformation/my-task"
    tx.mkdir(parents=True)
    (tx / "_config.yml").write_text(
        """version: 2
name: My Task
input:
  tables:
    - source: in.c-src.raw_orders
      destination: in.orders
    - source: in.c-src.raw_customers
      destination: in.customers
    - source: in.c-other.missing_table
      destination: in.absent
output:
  tables:
    - source: out.daily_revenue
      destination: out.c-marts.daily_revenue
"""
    )
    (tx / "transform.sql").write_text(sql_body)

    seeds = tmp_path / "seeds-src"
    # Matching parquet for orders + customers; nothing for absent.
    for bucket, tbl in [("in-c-src", "raw_orders"), ("in-c-src", "raw_customers")]:
        d = seeds / bucket / tbl
        d.mkdir(parents=True)
        (d / "slice0.parquet").write_bytes(b"fake")
    return tx, seeds


def test_migrate_sync_pull_links_seeds_and_copies_sql(tmp_path: Path) -> None:
    sql = 'CREATE OR REPLACE TABLE "out.daily_revenue" AS SELECT 1 AS x;'
    tx, seeds = _make_sync_pull_tree(tmp_path, sql_body=sql)
    out = tmp_path / "proj"

    # Opt out of dialect translation to keep the assertion on the exact body;
    # the default (snowflake->duckdb) is exercised in a dedicated test below.
    result = migrate_keboola_sync_pull(
        transformation_dir=tx,
        output_dir=out,
        seeds_source=seeds,
        source_dialect="duckdb",
    )

    assert result.transformation_name == "my_task"
    assert result.sql_line_count >= 1
    assert result.seeds_linked == 2
    assert result.seeds_missing == ["in.absent"]
    assert result.output_tables == {"out.daily_revenue": "out.c-marts.daily_revenue"}
    assert result.sql_translated is False

    # Project skeleton
    assert (out / "juncture.yaml").exists()
    assert (out / "models" / "my_task.sql").read_text() == sql
    assert (out / "models" / "schema.yml").exists()
    # Seeds: symlinks to parquet dirs, named by destination alias.
    orders_link = out / "seeds" / "in.orders"
    assert orders_link.is_symlink()
    assert orders_link.resolve() == (seeds / "in-c-src" / "raw_orders").resolve()
    assert (out / "seeds" / "in.customers").is_symlink()
    assert not (out / "seeds" / "in.absent").exists()


def test_migrate_sync_pull_translates_snowflake_to_duckdb_by_default(tmp_path: Path) -> None:
    # The pilot-migration BinderException pattern: Snowflake implicitly coerces the
    # numeric literal `0` to VARCHAR when the ELSE branch is a string-producing
    # REPLACE. DuckDB requires an explicit CAST.
    sql = (
        'CREATE OR REPLACE TABLE "out.citytargets" AS '
        "SELECT CASE WHEN REPLACE(v, ',', '') = '' THEN 0 "
        "ELSE REPLACE(v, ',', '') END AS x FROM t;"
    )
    tx, seeds = _make_sync_pull_tree(tmp_path, sql_body=sql)
    out = tmp_path / "proj"

    result = migrate_keboola_sync_pull(
        transformation_dir=tx,
        output_dir=out,
        seeds_source=seeds,
    )

    assert result.sql_translated is True
    translated = (out / "models" / "my_task.sql").read_text()
    # Cast inserted around the numeric literal; `TEXT` is DuckDB's preferred
    # spelling for VARCHAR from the SQLGlot translator.
    assert "CAST(0 AS TEXT)" in translated or "CAST(0 AS VARCHAR)" in translated


def test_migrate_sync_pull_errors_on_missing_config(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(FileNotFoundError, match=r"_config\.yml"):
        migrate_keboola_sync_pull(
            transformation_dir=empty,
            output_dir=tmp_path / "out",
            seeds_source=tmp_path / "seeds",
        )

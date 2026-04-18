"""Unit: validate_sync_pull_migration — pre-flight without writing."""

from __future__ import annotations

from pathlib import Path

from juncture.migration import validate_sync_pull_migration


def _scaffold(
    tx_dir: Path,
    seeds_src: Path,
    *,
    sql_body: str,
    input_tables: list[dict] | None = None,
) -> None:
    tx_dir.mkdir(parents=True, exist_ok=True)
    seeds_src.mkdir(parents=True, exist_ok=True)
    (tx_dir / "transform.sql").write_text(sql_body)
    config = {
        "name": "my_transformation",
        "input": {"tables": input_tables or []},
        "output": {"tables": []},
    }
    import yaml as _yaml

    (tx_dir / "_config.yml").write_text(_yaml.safe_dump(config))


def test_validate_clean_project(tmp_path: Path) -> None:
    tx_dir = tmp_path / "tx"
    seeds = tmp_path / "seeds"
    _scaffold(tx_dir, seeds, sql_body="SELECT 1 AS a;\nSELECT 2 AS b;\n")
    report = validate_sync_pull_migration(tx_dir, seeds_source=seeds)
    assert report.statement_count == 2
    assert report.parse_errors == []
    assert report.seeds_missing == []


def test_validate_detects_parse_errors(tmp_path: Path) -> None:
    tx_dir = tmp_path / "tx"
    seeds = tmp_path / "seeds"
    # The second statement uses a construct SQLGlot can't parse; it
    # should appear in parse_errors with index 1.
    _scaffold(
        tx_dir,
        seeds,
        sql_body="SELECT 1 AS a;\nTHIS IS NOT VALID SQL AT ALL;\n",
    )
    report = validate_sync_pull_migration(tx_dir, seeds_source=seeds)
    assert report.statement_count == 2
    assert len(report.parse_errors) == 1
    assert report.parse_errors[0][0] == 1


def test_validate_detects_missing_seeds(tmp_path: Path) -> None:
    tx_dir = tmp_path / "tx"
    seeds = tmp_path / "seeds"
    _scaffold(
        tx_dir,
        seeds,
        sql_body="SELECT * FROM orders;\n",
        input_tables=[
            {"source": "in.c-main.orders", "destination": "orders"},
            {"source": "in.c-main.customers", "destination": "customers"},
        ],
    )
    # Only "orders" has parquet data; customers is missing.
    orders_dir = seeds / "in" / "c-main" / "orders"
    orders_dir.mkdir(parents=True)
    (orders_dir / "part-0.parquet").write_bytes(b"")  # stub file is enough
    report = validate_sync_pull_migration(tx_dir, seeds_source=seeds)
    assert "orders" not in report.seeds_missing
    assert "customers" in report.seeds_missing


def test_validate_does_not_write_project_files(tmp_path: Path) -> None:
    tx_dir = tmp_path / "tx"
    seeds = tmp_path / "seeds"
    _scaffold(tx_dir, seeds, sql_body="SELECT 1;")
    # Any output path in the vicinity must stay empty.
    output = tmp_path / "migrated"
    assert not output.exists()
    validate_sync_pull_migration(tx_dir, seeds_source=seeds)
    # Nothing got created.
    assert not output.exists()

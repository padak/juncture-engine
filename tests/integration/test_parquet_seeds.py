"""Integration: parquet-directory seeds load and are ref-able from SQL."""

from __future__ import annotations

from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

import duckdb  # noqa: E402

from juncture.core.runner import Runner, RunRequest  # noqa: E402


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def test_parquet_seed_loads_via_read_parquet(tmp_path: Path) -> None:
    project = tmp_path / "pqseed"
    (project / "models").mkdir(parents=True)
    seed_dir = project / "seeds" / "in-c-db" / "orders"
    # Two slices, simulating kbagent's sliced parquet output.
    _write_parquet(seed_dir / "slice_0.parquet", [{"id": 1, "amount": 100}])
    _write_parquet(seed_dir / "slice_1.parquet", [{"id": 2, "amount": 250}, {"id": 3, "amount": 75}])

    (project / "juncture.yaml").write_text(
        f"""name: pqseed
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {project}/pqseed.duckdb
"""
    )
    (project / "models" / "order_totals.sql").write_text(
        '-- Use the seed name which equals "<bucket>.<table>" from the dir layout\n'
        "SELECT SUM(amount) AS total FROM {{ ref('in-c-db.orders') }}"
    )

    report = Runner().run(RunRequest(project_path=project))
    assert report.ok, [r.error for r in report.models.runs if r.error]

    con = duckdb.connect(str(project / "pqseed.duckdb"))
    total = con.execute('SELECT total FROM main."order_totals"').fetchone()
    assert total == (425,)


def test_parquet_seed_coexists_with_csv_seed(tmp_path: Path) -> None:
    project = tmp_path / "mixed"
    (project / "models").mkdir(parents=True)
    pq_dir = project / "seeds" / "pq_source"
    _write_parquet(pq_dir / "p.parquet", [{"x": 1}, {"x": 2}])
    csv_path = project / "seeds" / "csv_source.csv"
    csv_path.write_text("y\n10\n20\n30\n")

    (project / "juncture.yaml").write_text(
        f"""name: mixed
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {project}/mixed.duckdb
"""
    )
    (project / "models" / "combined.sql").write_text(
        "SELECT "
        "(SELECT COUNT(*) FROM {{ ref('pq_source') }}) AS pq_rows, "
        "(SELECT COUNT(*) FROM {{ ref('csv_source') }}) AS csv_rows"
    )

    report = Runner().run(RunRequest(project_path=project))
    assert report.ok, [r.error for r in report.models.runs if r.error]

    con = duckdb.connect(str(project / "mixed.duckdb"))
    pq_rows, csv_rows = con.execute("SELECT pq_rows, csv_rows FROM main.combined").fetchone()
    assert pq_rows == 2
    assert csv_rows == 3

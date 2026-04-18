"""Integration: Project.seed_schemas() extracts seed column types.

Used by schema-aware translate_sql. Validates that:
- seeds/schema.yml overrides are always authoritative
- parquet files contribute their own DESCRIBE types when no override is set
- the method caches the result so a second call is cheap
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from juncture.core.project import Project


def _write_minimal_project(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "models").mkdir(exist_ok=True)
    (root / "models" / "m.sql").write_text("SELECT 1 AS x")
    (root / "juncture.yaml").write_text(
        f"""name: t
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {root}/t.duckdb
"""
    )


def test_seed_schemas_from_overrides_only(tmp_path: Path) -> None:
    project_path = tmp_path / "proj"
    _write_minimal_project(project_path)
    seeds = project_path / "seeds"
    seeds.mkdir()
    # Empty CSV — seed_schemas() doesn't probe CSV content, only overrides.
    (seeds / "orders.csv").write_text("id,amount\n1,9.99\n")
    (seeds / "schema.yml").write_text(
        """seeds:
  - name: orders
    columns:
      - name: id
        type: BIGINT
      - name: amount
        type: DECIMAL(18,2)
"""
    )
    project = Project.load(project_path)
    schemas = project.seed_schemas()
    assert schemas == {"orders": {"id": "BIGINT", "amount": "DECIMAL(18,2)"}}


def test_seed_schemas_from_parquet_describe(tmp_path: Path) -> None:
    project_path = tmp_path / "proj"
    _write_minimal_project(project_path)
    seeds = project_path / "seeds"
    parquet_dir = seeds / "customers"
    parquet_dir.mkdir(parents=True)

    # Write a tiny parquet file with known column types.
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "email": pa.array(["a@b", "c@d"], type=pa.string()),
            "signup_date": pa.array(["2024-01-01", "2024-02-01"], type=pa.string()),
        }
    )
    pq.write_table(table, parquet_dir / "part-0.parquet")

    project = Project.load(project_path)
    schemas = project.seed_schemas()
    assert "customers" in schemas
    cols = schemas["customers"]
    # BIGINT for int64, VARCHAR for string. DuckDB uppercases.
    assert cols["id"] == "BIGINT"
    assert "VARCHAR" in cols["email"]


def test_seed_schema_overrides_beat_parquet(tmp_path: Path) -> None:
    project_path = tmp_path / "proj"
    _write_minimal_project(project_path)
    seeds = project_path / "seeds"
    parquet_dir = seeds / "orders"
    parquet_dir.mkdir(parents=True)
    # Parquet would advertise VARCHAR for amount.
    table = pa.table({"amount": pa.array(["9.99"], type=pa.string())})
    pq.write_table(table, parquet_dir / "part-0.parquet")
    (seeds / "schema.yml").write_text(
        """seeds:
  - name: orders
    columns:
      - name: amount
        type: DECIMAL(18,2)
"""
    )
    project = Project.load(project_path)
    schemas = project.seed_schemas()
    # Override wins; the user told us it's DECIMAL.
    assert schemas["orders"]["amount"] == "DECIMAL(18,2)"


def test_seed_schemas_caches_to_disk(tmp_path: Path) -> None:
    project_path = tmp_path / "proj"
    _write_minimal_project(project_path)
    seeds = project_path / "seeds"
    parquet_dir = seeds / "events"
    parquet_dir.mkdir(parents=True)
    table = pa.table({"ts": pa.array(["2024-01-01"], type=pa.string())})
    pq.write_table(table, parquet_dir / "part-0.parquet")

    project = Project.load(project_path)
    first = project.seed_schemas()
    cache = project_path / ".juncture" / "seed_schemas.json"
    assert cache.exists()
    import json

    assert json.loads(cache.read_text()) == first
    # Second call still produces the same result (deterministic).
    project2 = Project.load(project_path)
    assert project2.seed_schemas() == first

"""Seed loading: CSV file or Parquet directory -> materialized source table.

Seeds sit under ``seeds/`` and are loaded once before models run. They are
not part of the model DAG; a model that wants to consume a seed simply
references it by name via ``ref('my_seed')`` exactly like another model.

Two seed layouts are recognised:

* ``seeds/<name>.csv`` — a single CSV file (DuckDB ``read_csv_auto``).
* ``seeds/<name>/*.parquet`` — a directory of Parquet slices (DuckDB
  ``read_parquet`` with a glob). This layout mirrors what
  ``kbagent storage unload-table --file-type parquet`` produces and
  preserves row-group boundaries without concatenation.

Seed names may contain dots (e.g. ``in.c-db.carts``) so that migrated
transformations can keep Snowflake-style quoted identifiers unchanged.
"""

from __future__ import annotations

import csv
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from juncture.adapters.base import Adapter
    from juncture.core.project import SeedSpec


def load_seeds(adapter: Adapter, seeds: list[SeedSpec], *, schema: str) -> dict[str, int]:
    """Materialize every seed as ``schema.<seed_name>``.

    Returns a mapping ``seed_name -> row_count`` for reporting.
    """
    counts: dict[str, int] = {}
    for seed in seeds:
        counts[seed.name] = _load_one(adapter, seed, schema=schema)
    return counts


def _load_one(adapter: Adapter, seed: SeedSpec, *, schema: str) -> int:
    fqn = adapter.resolve(seed.name, schema=schema)
    cursor = adapter._thread_cursor() if hasattr(adapter, "_thread_cursor") else None  # type: ignore[attr-defined]
    if cursor is None:
        raise NotImplementedError(f"Adapter {adapter.type_name!r} does not support seed loading yet")

    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    if adapter.type_name == "duckdb":
        return _load_duckdb(cursor, fqn, seed)

    # Generic fallback: parse CSV in Python and INSERT.
    return _load_generic(cursor, fqn, seed)


def _load_duckdb(cursor: Any, fqn: str, seed: SeedSpec) -> int:
    """Fast path: DuckDB reads CSV and Parquet natively, no Python iteration.

    Parquet seeds become VIEWs over ``read_parquet``. This keeps RAM use
    flat when hundreds of seed tables live in one project: the parquet
    files stay on disk and DuckDB streams rows through them lazily.

    CSV seeds become TABLEs (eager load) because every subsequent query
    would otherwise re-parse the CSV header-inferred schema.
    """
    if seed.format == "parquet":
        glob = f"{seed.path.as_posix().rstrip('/')}/*.parquet"
        cursor.execute(f"CREATE OR REPLACE VIEW {fqn} AS SELECT * FROM read_parquet('{glob}')")
    else:  # csv
        cursor.execute(
            f"CREATE OR REPLACE TABLE {fqn} AS "
            f"SELECT * FROM read_csv_auto('{seed.path.as_posix()}', header=true)"
        )
    count_row = cursor.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()
    return int(count_row[0]) if count_row else 0


def _load_generic(cursor: Any, fqn: str, seed: SeedSpec) -> int:
    """Slow path: read rows in Python and INSERT. CSV only for now."""
    if seed.format != "csv":
        raise NotImplementedError(
            f"Generic seed loader only supports CSV (got {seed.format!r}); "
            f"use DuckDB adapter for Parquet seeds"
        )
    rows = list(_read_csv(seed.path))
    if not rows:
        raise ValueError(f"Seed {seed.name} at {seed.path} is empty")
    columns = list(rows[0].keys())
    column_ddl = ", ".join(f'"{c}" VARCHAR' for c in columns)
    cursor.execute(f"CREATE OR REPLACE TABLE {fqn} ({column_ddl})")
    placeholders = ", ".join(["?"] * len(columns))
    cursor.executemany(
        f"INSERT INTO {fqn} VALUES ({placeholders})",
        [[row[c] for c in columns] for row in rows],
    )
    return len(rows)


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader)

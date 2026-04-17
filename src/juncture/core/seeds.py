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

Parallel loading
----------------
On DuckDB, every seed lands in the same database file so we can run
several ``CREATE VIEW`` / type-inference passes concurrently via
``adapter._thread_cursor()``. The worker count is capped by
``DuckDBAdapter.threads`` (from ``juncture.yaml``).
"""

from __future__ import annotations

import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

    from juncture.adapters.base import Adapter
    from juncture.core.project import SeedSpec

log = logging.getLogger(__name__)


def load_seeds(adapter: Adapter, seeds: list[SeedSpec], *, schema: str) -> dict[str, int]:
    """Materialize every seed as ``schema.<seed_name>``.

    Returns a mapping ``seed_name -> row_count`` for reporting.

    Uses a thread pool when the adapter provides ``_thread_cursor`` and
    has its ``threads`` attribute > 1. Otherwise runs serially.
    """
    max_workers = _seed_parallelism(adapter)
    if max_workers <= 1 or len(seeds) <= 1:
        return {seed.name: _load_one(adapter, seed, schema=schema) for seed in seeds}

    counts: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_load_one, adapter, seed, schema=schema): seed for seed in seeds}
        for future in as_completed(futures):
            seed = futures[future]
            try:
                counts[seed.name] = future.result()
            except Exception as exc:
                log.error("Seed %s failed: %s", seed.name, exc)
                raise
    return counts


def _seed_parallelism(adapter: Adapter) -> int:
    """Derive parallel seed workers from adapter config.

    DuckDB cursors are cheap and independent, so we happily spawn up to
    ``adapter.threads`` workers. Falls back to 1 for adapters that don't
    advertise the attribute.
    """
    threads = getattr(adapter, "threads", None)
    if threads is None:
        return 1
    try:
        return max(1, int(threads))
    except (TypeError, ValueError):
        return 1


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

    Parquet seeds become VIEWs over ``read_parquet`` with **inferred column
    types** (bigint/double/date/timestamp). This keeps RAM use flat across
    hundreds of seeds while letting downstream SQL run arithmetic and date
    math on columns that Keboola exports as plain VARCHAR.

    CSV seeds become TABLEs (eager load) because every subsequent query
    would otherwise re-parse the CSV header-inferred schema.
    """
    if seed.format == "parquet":
        from juncture.core.type_inference import build_typed_view_sql, infer_parquet_types

        glob = f"{seed.path.as_posix().rstrip('/')}/*.parquet"
        overrides = seed.schema_overrides or {}
        inference = infer_parquet_types(cursor, glob, overrides=overrides)
        cursor.execute(
            build_typed_view_sql(
                fqn, glob, inference.column_types, native_types=inference.native_types
            )
        )
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

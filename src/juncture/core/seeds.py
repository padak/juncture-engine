"""Seed loading: CSV -> materialized source table.

Seeds sit under ``seeds/`` and are loaded once before models run. They are
not part of the model DAG; a model that wants to consume a seed simply
references it by name via ``ref('my_seed')`` exactly like another model.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from juncture.adapters.base import Adapter
    from juncture.core.project import SeedSpec


def load_seeds(adapter: Adapter, seeds: list[SeedSpec], *, schema: str) -> dict[str, int]:
    """Materialize every seed as ``schema.<seed_name>``.

    Returns a mapping ``seed_name -> row_count`` for reporting.
    """
    counts: dict[str, int] = {}
    for seed in seeds:
        row_count = _load_one(adapter, seed, schema=schema)
        counts[seed.name] = row_count
    return counts


def _load_one(adapter: Adapter, seed: SeedSpec, *, schema: str) -> int:
    fqn = adapter.resolve(seed.name, schema=schema)
    cursor = adapter._thread_cursor() if hasattr(adapter, "_thread_cursor") else None  # type: ignore[attr-defined]

    if cursor is None:
        # Fallback: adapters without cursor concept (future) would go here.
        raise NotImplementedError(f"Adapter {adapter.type_name!r} does not support seed loading yet")

    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    # DuckDB can read CSV directly. For other adapters we'd upload the file
    # to a staging area and COPY INTO.
    if adapter.type_name == "duckdb":
        cursor.execute(
            f"CREATE OR REPLACE TABLE {fqn} AS "
            f"SELECT * FROM read_csv_auto('{seed.path.as_posix()}', header=true)"
        )
        row_count = cursor.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()[0]
        return int(row_count)

    # Generic fallback: parse CSV in Python and INSERT.
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

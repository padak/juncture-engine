"""Infer proper column types for VARCHAR-only parquet seeds.

Why this exists
---------------
Keboola Storage exports everything as VARCHAR by default. When we migrate a
Snowflake transformation to DuckDB verbatim, the SQL expects typed columns
(``amount + 1`` works on Snowflake because it implicit-casts VARCHAR to
NUMBER; DuckDB refuses). So we probe each column's actual content and
rewrite the seed VIEW with explicit casts.

Hybrid strategy
---------------
* Tables with ``<= full_scan_threshold`` rows (default 1 M): full-column
  scan. Deterministic, no risk of missing an anomaly late in the table.
* Tables above that: random sample of ``sample_size`` rows via DuckDB's
  ``USING SAMPLE ... ROWS``. Fast and statistically safe for uniform
  distributions.

Cast precedence (narrowest first)::

    BIGINT > DOUBLE > DATE > TIMESTAMP > VARCHAR

``BOOLEAN`` is intentionally excluded -- the test also matches ``'0'`` /
``'1'`` so any integer column would be classified as a flag and break
arithmetic downstream.

Empty columns (all NULL) fall back to VARCHAR.

Override
--------
A seed's ``schema.yml`` entry can set explicit types, which take precedence
over the inferred ones:

.. code-block:: yaml

    seeds:
      - name: orders
        columns:
          - { name: amount, type: DECIMAL(18,2) }
          - { name: id,     type: BIGINT }
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

# Order matters: narrower types are checked first, so a column that passes
# BIGINT never ends up classified as DOUBLE or DATE.
_PRECEDENCE: list[tuple[str, str]] = [
    ("bigint", "BIGINT"),
    ("double", "DOUBLE"),
    ("date", "DATE"),
    ("timestamp", "TIMESTAMP"),
]


@dataclass(kw_only=True)
class InferenceResult:
    """Outcome of inferring types for one seed."""

    column_types: dict[str, str] = field(default_factory=dict)
    native_types: dict[str, str] = field(default_factory=dict)
    rows_scanned: int = 0
    mode: str = "full"  # "full" | "sampled"


def infer_parquet_types(
    cursor: Any,
    parquet_glob: str,
    *,
    full_scan_threshold: int = 1_000_000,
    sample_size: int = 1_000_000,
    overrides: dict[str, str] | None = None,
) -> InferenceResult:
    """Return ``{column: duckdb_type}`` for a parquet dataset.

    The cursor must belong to an active DuckDB connection; both reads and
    type checks run through it.
    """
    overrides = overrides or {}
    row_count = int(cursor.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_glob}')").fetchone()[0])

    if row_count <= full_scan_threshold or row_count == 0:
        source = f"read_parquet('{parquet_glob}')"
        mode = "full"
    else:
        source = (
            f"(SELECT * FROM read_parquet('{parquet_glob}') "
            f"USING SAMPLE {int(sample_size)} ROWS) AS _juncture_sample"
        )
        mode = "sampled"

    # Read column names + their native parquet types. Columns that parquet
    # already advertises as typed (INT, DOUBLE, DATE, TIMESTAMP, BOOLEAN...)
    # don't need probing -- we trust the file. Only VARCHAR columns trigger
    # the string-probe pass, which is exactly the Keboola-Storage case.
    describe = cursor.execute(
        f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{parquet_glob}'))"
    ).fetchall()

    typed_columns: dict[str, str] = {}
    varchar_columns: list[str] = []
    for col_name, col_type in describe:
        upper = col_type.upper()
        if upper.startswith("VARCHAR") or upper == "STRING" or upper == "TEXT":
            varchar_columns.append(col_name)
        else:
            typed_columns[col_name] = upper

    # One query across VARCHAR columns keeps to a single parquet scan.
    #
    # DuckDB's TRY_CAST is permissive on numerics and dates:
    # * TRY_CAST('9.99' AS BIGINT)                 -> 10  (rounds, not NULL)
    # * TRY_CAST('2026-04-17 08:30:00' AS DATE)    -> 2026-04-17 (truncates)
    # So we guard BIGINT and DATE with strict regexes; DOUBLE and TIMESTAMP
    # still rely on TRY_CAST because they tolerate both shapes we want.
    parts: list[str] = []
    for col in varchar_columns:
        quoted = f'"{col}"'
        parts.extend(
            [
                f"SUM(CASE WHEN {quoted} IS NOT NULL "
                f"AND NOT (TRIM({quoted}) SIMILAR TO '-?[0-9]+') "
                f'THEN 1 ELSE 0 END) AS "{col}__fail_bigint"',
                f"SUM(CASE WHEN {quoted} IS NOT NULL AND TRY_CAST({quoted} AS DOUBLE) IS NULL "
                f'THEN 1 ELSE 0 END) AS "{col}__fail_double"',
                f"SUM(CASE WHEN {quoted} IS NOT NULL "
                f"AND NOT (TRIM({quoted}) SIMILAR TO '[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}') "
                f'THEN 1 ELSE 0 END) AS "{col}__fail_date"',
                f"SUM(CASE WHEN {quoted} IS NOT NULL AND TRY_CAST({quoted} AS TIMESTAMP) IS NULL "
                f'THEN 1 ELSE 0 END) AS "{col}__fail_timestamp"',
                f'SUM(CASE WHEN {quoted} IS NOT NULL THEN 1 ELSE 0 END) AS "{col}__non_null"',
            ]
        )

    types: dict[str, str] = {}
    # Native parquet types pass through unchanged (unless overridden).
    for col, native in typed_columns.items():
        types[col] = overrides.get(col, native)

    if parts:
        probe_sql = f"SELECT {', '.join(parts)} FROM {source}"
        row = cursor.execute(probe_sql).fetchone() or ()
        idx = 0
        for col in varchar_columns:
            fail_bigint, fail_double, fail_date, fail_timestamp, non_null = row[idx : idx + 5]
            idx += 5

            if col in overrides:
                types[col] = overrides[col]
                continue

            if not non_null:  # all NULL -> keep text
                types[col] = "VARCHAR"
                continue

            chosen = "VARCHAR"
            for fail_attr, duck_type in _PRECEDENCE:
                fail_value = {
                    "bigint": fail_bigint,
                    "double": fail_double,
                    "date": fail_date,
                    "timestamp": fail_timestamp,
                }[fail_attr]
                if int(fail_value) == 0:
                    chosen = duck_type
                    break
            types[col] = chosen

    log.info(
        "Type inference for %s: %d native + %d varchar-probed cols "
        "(%d typed total, %d still varchar), mode=%s, rows=%d",
        parquet_glob,
        len(typed_columns),
        len(varchar_columns),
        sum(1 for t in types.values() if t != "VARCHAR"),
        sum(1 for t in types.values() if t == "VARCHAR"),
        mode,
        row_count,
    )
    all_native = dict(typed_columns)
    for col in varchar_columns:
        all_native[col] = "VARCHAR"
    return InferenceResult(
        column_types=types,
        native_types=all_native,
        rows_scanned=row_count,
        mode=mode,
    )


def build_typed_view_sql(
    view_fqn: str,
    parquet_glob: str,
    column_types: dict[str, str],
    *,
    native_types: dict[str, str] | None = None,
) -> str:
    """Compose ``CREATE OR REPLACE VIEW`` that casts each column to its target type.

    ``native_types`` is the mapping parquet already advertises. Columns
    whose target type matches the native type are emitted without a cast
    to keep the VIEW readable.
    """
    if not column_types:
        # No schema discovered -- fall back to plain read_parquet so the VIEW still works.
        return f"CREATE OR REPLACE VIEW {view_fqn} AS SELECT * FROM read_parquet('{parquet_glob}')"

    native = native_types or {}
    projections: list[str] = []
    for col, duck_type in column_types.items():
        if native.get(col, "").upper().startswith(duck_type.upper()) or duck_type == "VARCHAR":
            projections.append(f'"{col}"')
        else:
            projections.append(f'TRY_CAST("{col}" AS {duck_type}) AS "{col}"')
    select_list = ",\n    ".join(projections)
    return (
        f"CREATE OR REPLACE VIEW {view_fqn} AS\n"
        f"SELECT\n    {select_list}\n"
        f"FROM read_parquet('{parquet_glob}')"
    )

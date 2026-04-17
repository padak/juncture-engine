"""Unit tests for type_inference: verify mixed VARCHAR parquet maps to proper types."""

from __future__ import annotations

from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

import duckdb  # noqa: E402

from juncture.core.type_inference import build_typed_view_sql, infer_parquet_types


def _write_parquet_strings(path: Path, rows: list[dict[str, str | None]]) -> None:
    """Write a parquet file where every column is a VARCHAR, matching Keboola Storage exports."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    columns = list(rows[0].keys())
    arrays = {c: pa.array([r.get(c) for r in rows], type=pa.string()) for c in columns}
    table = pa.Table.from_pydict(arrays)
    pq.write_table(table, path)


def test_classifies_bigint_double_date_timestamp_varchar(tmp_path: Path) -> None:
    file = tmp_path / "t.parquet"
    _write_parquet_strings(
        file,
        [
            {
                "id": "1",
                "amount": "9.99",
                "created_date": "2026-04-17",
                "created_ts": "2026-04-17 08:30:00",
                "note": "hello",
            },
            {
                "id": "2",
                "amount": "-3.5",
                "created_date": "2026-04-18",
                "created_ts": "2026-04-18 09:00:00",
                "note": "world",
            },
            {
                "id": None,
                "amount": "12",
                "created_date": "2026-04-19",
                "created_ts": "2026-04-19 09:30:00",
                "note": None,
            },
        ],
    )

    con = duckdb.connect()
    result = infer_parquet_types(con, str(tmp_path / "*.parquet"))

    assert result.column_types["id"] == "BIGINT"
    # amount has "12" which would also pass BIGINT; precedence picks BIGINT.
    # To force DOUBLE, need at least one non-integer value:
    assert result.column_types["amount"] == "DOUBLE"
    assert result.column_types["created_date"] == "DATE"
    assert result.column_types["created_ts"] == "TIMESTAMP"
    assert result.column_types["note"] == "VARCHAR"
    assert result.mode == "full"


def test_all_null_column_defaults_to_varchar(tmp_path: Path) -> None:
    file = tmp_path / "nulls.parquet"
    _write_parquet_strings(
        file,
        [
            {"id": "1", "blank": None},
            {"id": "2", "blank": None},
        ],
    )
    con = duckdb.connect()
    result = infer_parquet_types(con, str(tmp_path / "*.parquet"))
    assert result.column_types["blank"] == "VARCHAR"
    assert result.column_types["id"] == "BIGINT"


def test_overrides_win_over_inference(tmp_path: Path) -> None:
    file = tmp_path / "o.parquet"
    _write_parquet_strings(file, [{"x": "1"}, {"x": "2"}])
    con = duckdb.connect()
    result = infer_parquet_types(
        con,
        str(tmp_path / "*.parquet"),
        overrides={"x": "VARCHAR"},
    )
    assert result.column_types["x"] == "VARCHAR"


def test_sampled_mode_when_above_threshold(tmp_path: Path) -> None:
    file = tmp_path / "big.parquet"
    rows = [{"id": str(i)} for i in range(2500)]
    _write_parquet_strings(file, rows)
    con = duckdb.connect()
    result = infer_parquet_types(
        con,
        str(tmp_path / "*.parquet"),
        full_scan_threshold=1000,
        sample_size=500,
    )
    assert result.mode == "sampled"
    assert result.column_types["id"] == "BIGINT"


def test_typed_view_sql_emits_casts(tmp_path: Path) -> None:
    sql = build_typed_view_sql(
        '"main"."orders"',
        "/tmp/orders/*.parquet",
        {"id": "BIGINT", "note": "VARCHAR", "amount": "DOUBLE"},
    )
    assert 'TRY_CAST("id" AS BIGINT) AS "id"' in sql
    assert '"note"' in sql  # no cast for VARCHAR
    assert 'TRY_CAST("amount" AS DOUBLE)' in sql


def test_typed_view_sql_empty_schema_falls_back(tmp_path: Path) -> None:
    sql = build_typed_view_sql('"main"."t"', "/tmp/t/*.parquet", {})
    assert "SELECT *" in sql

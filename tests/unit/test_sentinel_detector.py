"""Unit: sentinel detection on VARCHAR columns.

The detector runs one aggregate probe and flags values that show up
above 2% of a column's non-null values as NULL placeholders. These
profiles are fed into schema-aware translate so CAST wrappers become
TRY_CAST(NULLIF(col, sentinel) AS ...) automatically.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from juncture.core.type_inference import (
    SentinelProfile,
    detect_sentinels,
    infer_parquet_types,
)


def _seed(tmp_path: Path, rows: list[dict[str, str | None]]) -> str:
    """Write a parquet file of ``rows`` and return its glob."""
    cols: dict[str, list[str | None]] = {}
    for row in rows:
        for k, v in row.items():
            cols.setdefault(k, []).append(v)
    # Use plain string arrays so we get VARCHAR on the DuckDB side.
    table = pa.table({k: pa.array(v, type=pa.string()) for k, v in cols.items()})
    path = tmp_path / "part-0.parquet"
    pq.write_table(table, path)
    return f"{tmp_path.as_posix()}/*.parquet"


def test_detect_empty_string_sentinel(tmp_path: Path) -> None:
    # 2/5 rows are empty string — well above the 2% threshold.
    glob = _seed(
        tmp_path,
        [
            {"role": "admin"},
            {"role": ""},
            {"role": "user"},
            {"role": ""},
            {"role": "guest"},
        ],
    )
    con = duckdb.connect(":memory:")
    sentinels = detect_sentinels(con, f"read_parquet('{glob}')", ["role"])
    assert "role" in sentinels
    profile = sentinels["role"]
    assert "" in profile.null_sentinels
    assert 0.0 < profile.abundance[""] <= 1.0


def test_detect_multiple_sentinels(tmp_path: Path) -> None:
    glob = _seed(
        tmp_path,
        [
            {"status": ""},
            {"status": "--empty--"},
            {"status": "active"},
            {"status": "n/a"},
            {"status": "active"},
            {"status": "--empty--"},
            {"status": "active"},
        ],
    )
    con = duckdb.connect(":memory:")
    sentinels = detect_sentinels(con, f"read_parquet('{glob}')", ["status"])
    assert "status" in sentinels
    found = set(sentinels["status"].null_sentinels)
    assert {"", "--empty--", "n/a"} <= found


def test_no_sentinels_empty_result(tmp_path: Path) -> None:
    glob = _seed(
        tmp_path,
        [
            {"email": "a@b.com"},
            {"email": "c@d.com"},
            {"email": "e@f.com"},
        ],
    )
    con = duckdb.connect(":memory:")
    sentinels = detect_sentinels(con, f"read_parquet('{glob}')", ["email"])
    assert sentinels == {}


def test_below_threshold_single_occurrence_ignored(tmp_path: Path) -> None:
    # One stray "Other" in 100 rows = 1% — below the 2% threshold, so
    # the word shouldn't be flagged as a null-carrier.
    rows: list[dict[str, str | None]] = [{"role": "user"} for _ in range(100)]
    rows[0] = {"role": "Other"}
    glob = _seed(tmp_path, rows)
    con = duckdb.connect(":memory:")
    sentinels = detect_sentinels(con, f"read_parquet('{glob}')", ["role"])
    assert sentinels == {}


def test_infer_parquet_types_with_sentinel_detection(tmp_path: Path) -> None:
    glob = _seed(
        tmp_path,
        [
            {"id": "1", "status": ""},
            {"id": "2", "status": "--empty--"},
            {"id": "3", "status": "active"},
            {"id": "4", "status": ""},
        ],
    )
    con = duckdb.connect(":memory:")
    result = infer_parquet_types(con, glob, detect_sentinels_also=True)
    # id is all numeric-looking -> BIGINT despite VARCHAR parquet native type.
    assert result.column_types["id"] == "BIGINT"
    # status has sentinels.
    assert "status" in result.sentinels
    assert isinstance(result.sentinels["status"], SentinelProfile)


def test_sentinels_absent_when_flag_off(tmp_path: Path) -> None:
    glob = _seed(tmp_path, [{"col": ""}, {"col": "x"}])
    con = duckdb.connect(":memory:")
    result = infer_parquet_types(con, glob)  # detect_sentinels_also defaults to False
    assert result.sentinels == {}

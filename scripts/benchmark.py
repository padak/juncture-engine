"""Quick benchmark: generate N rows, run a mini-pipeline, report timings.

Usage::

    python scripts/benchmark.py --rows 1_000_000 --threads 4

The point is to verify the Oldie-but-Goldie target of "≤10 % overhead over
raw query time" on the DuckDB adapter and to give us numbers to cite
against dbt-duckdb in the future.
"""

from __future__ import annotations

import argparse
import logging
import tempfile
import time
from pathlib import Path

import duckdb

from juncture.core.runner import RunRequest, Runner

logging.basicConfig(level=logging.WARNING)


def _build_project(root: Path, rows: int, threads: int) -> None:
    (root / "models").mkdir(parents=True, exist_ok=True)
    (root / "juncture.yaml").write_text(
        f"""name: bench
profile: local
default_schema: main
connections:
  local:
    type: duckdb
    path: {root}/bench.duckdb
    threads: {threads}
"""
    )
    (root / "models" / "raw.sql").write_text(
        f"SELECT range AS id, (range % 1000) AS bucket, random() AS amount "
        f"FROM range({rows})"
    )
    (root / "models" / "bucketed.sql").write_text(
        "SELECT bucket, COUNT(*) AS n, SUM(amount) AS total "
        "FROM {{ ref('raw') }} GROUP BY bucket"
    )
    (root / "models" / "top10.sql").write_text(
        "SELECT bucket, total FROM {{ ref('bucketed') }} ORDER BY total DESC LIMIT 10"
    )


def run_juncture(root: Path, threads: int) -> float:
    t0 = time.perf_counter()
    report = Runner().run(RunRequest(project_path=root, threads=threads))
    assert report.ok, "benchmark pipeline failed"
    return time.perf_counter() - t0


def run_raw_duckdb(rows: int, threads: int) -> float:
    # Pure DuckDB baseline: same three queries, no Juncture overhead.
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "bench.duckdb"
        con = duckdb.connect(str(db_path))
        if threads:
            con.execute(f"PRAGMA threads = {threads}")
        t0 = time.perf_counter()
        con.execute(
            f"CREATE OR REPLACE TABLE main.raw AS "
            f"SELECT range AS id, (range % 1000) AS bucket, random() AS amount FROM range({rows})"
        )
        con.execute(
            "CREATE OR REPLACE TABLE main.bucketed AS "
            "SELECT bucket, COUNT(*) AS n, SUM(amount) AS total FROM main.raw GROUP BY bucket"
        )
        con.execute(
            "CREATE OR REPLACE TABLE main.top10 AS "
            "SELECT bucket, total FROM main.bucketed ORDER BY total DESC LIMIT 10"
        )
        elapsed = time.perf_counter() - t0
        con.close()
        return elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "bench"
        _build_project(root, args.rows, args.threads)

        j_times = []
        r_times = []
        for _ in range(args.repeat):
            # Fresh bench each repeat: clear duckdb file between runs.
            duckdb_file = root / "bench.duckdb"
            if duckdb_file.exists():
                duckdb_file.unlink()
            j_times.append(run_juncture(root, args.threads))
            r_times.append(run_raw_duckdb(args.rows, args.threads))

    print(f"Rows: {args.rows:,}   Threads: {args.threads}   Repeat: {args.repeat}")
    print(f"Juncture (median):   {_median(j_times):.3f}s")
    print(f"Raw DuckDB (median): {_median(r_times):.3f}s")
    overhead = (_median(j_times) - _median(r_times)) / _median(r_times) * 100
    print(f"Overhead: {overhead:+.1f}%")


def _median(values: list[float]) -> float:
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


if __name__ == "__main__":
    main()

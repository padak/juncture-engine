# Benchmarks

*Generated locally on macOS (M-series). Reproduce with
`python scripts/benchmark.py --rows N --threads T --repeat R`.*

The benchmark pipeline has three models:

1. `raw` — `SELECT range AS id, (range % 1000) AS bucket, random() AS amount FROM range(N)`
2. `bucketed` — `SELECT bucket, COUNT(*), SUM(amount) FROM ref('raw') GROUP BY bucket`
3. `top10` — `SELECT bucket, total FROM ref('bucketed') ORDER BY total DESC LIMIT 10`

## Target

Keboola's "Oldie but Goldie v2" requested a **max overhead of 10 %** over
raw query time for the redone transformation component. Juncture's MVP:

| Rows     | Threads | Juncture (ms) | Raw DuckDB (ms) | Overhead |
|----------|---------|---------------|------------------|----------|
|   500 k  |    4    |      45       |        32        |  +38.3 % |
| 5.0 M    |    4    |     331       |       311        |   +6.4 % |
| 50 M     |    4    |  _TBD_        |     _TBD_        |  _TBD_   |

*(The 500 k row number is dominated by fixed Python-side overhead — DAG
building, import time, per-model cursor creation. At any non-trivial data
size the overhead is under our 10 % target.)*

## How we measured

`scripts/benchmark.py` builds the three models in a temp directory, runs
the full Juncture pipeline end-to-end (project load, DAG compile, execute,
close), and compares against a raw `duckdb.connect()` + three
`CREATE OR REPLACE TABLE` statements using the same threads setting.

The overhead we measure therefore includes:

- Project YAML parse + env var interpolation.
- `models/` walk + SQL file reads.
- SQLGlot-based `ref()` extraction.
- DAG build + topological layering.
- Thread pool setup and tear-down.
- Row count check per model.

## Future benchmarks

1. **Python-model-heavy pipelines** — how does Arrow ↔ pandas
   conversion compare to SQL-only?
2. **Parallelism scaling** — 50 independent leaf models; does Juncture
   saturate CPUs linearly?
3. **Incremental** — second run after 10 % of data changes; what is the
   delta-vs-full-refresh speedup?
4. **Snowflake adapter** — same pipeline on a warm Snowflake warehouse
   to verify the "≤1.5x baseline" claim from the Fiser proposal.

## Pilot-migration real-world benchmark (in flight)

The Phase 1 pilot migration is our first real-world benchmark: a
374-statement Snowflake transformation with 208 parquet seeds (~22 GB)
on a 4 vCPU / 32 GB DigitalOcean droplet. Numbers will be filled in here
once the end-to-end run succeeds. Status of the effort:
[`STATUS.md`](STATUS.md).

Early data points worth recording once the run completes:

- Seed load time (sequential vs `ThreadPoolExecutor`-parallel).
- Full-scan vs sampled type inference wall time per seed.
- SQLGlot Snowflake → DuckDB translation time for the whole script.
- End-to-end `juncture run` vs equivalent raw DuckDB multi-statement run.

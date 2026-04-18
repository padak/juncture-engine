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

## Pilot-migration real-world benchmark

The Phase 1 pilot migration is our first real-world benchmark: a
**374-statement Snowflake transformation with 208 parquet seeds (~22 GB)**
running on a 4 vCPU / 32 GB DigitalOcean droplet (Ubuntu 24.04, DuckDB
`threads: 4`, `memory_limit: 16GB`). Two project shapes are compared:

- **Monolith** — the original `migrate-sync-pull` output: one
  `EXECUTE`-materialized model that runs all 374 statements in sequence.
- **Split DAG** — same SQL after `juncture split-execute`: 311 per-CTAS
  models + one `_residual` execute model (INSERT/UPDATE/DDL), wired with
  inferred `ref()` edges. DAG depth 15 layers, widest layer 293 models.

Both shapes produce the same output tables, verified by row counts on
the four largest `out.*` tables.

### Scenario matrix

| Scenario | Project | Cold/warm | Executor flags | Wall clock | Peak RSS | CPU util | Models |
|---|---|---|---|---:|---:|---:|---:|
| **S1** | Monolith | cold | `--threads 4` | **13:35** | 7.8 GB | 367 % | 209 ✓ |
| **S2** | Monolith | warm | `--threads 4 --reuse-seeds` | **3:34** | 4.4 GB | 306 % | 209 ✓ |
| S3 | Monolith | warm | `--reuse-seeds`, `config.parallelism: 4` on the EXECUTE model | 0:04 | 190 MB | 107 % | **failed** (race) |
| **S4 cold** | Split DAG | cold | `--threads 4` | **13:02** | 8.6 GB | 378 % | 520 ✓ |
| **S4 t=1** | Split DAG | warm | `--threads 1 --reuse-seeds` | **3:30** | 4.9 GB | 304 % | 520 ✓ |
| **S4 t=4** | Split DAG | warm | `--threads 4 --reuse-seeds` | **3:06** | 6.2 GB | 352 % | 520 ✓ |
| **S4 t=8** | Split DAG | warm | `--threads 8 --reuse-seeds` | **3:07** | 7.5 GB | 344 % | 520 ✓ |

"Models" counts include the 208 seeds (loaded as VIEWs). All 520 nodes
succeed in every passing run; S3 fails after 178/179 because the
intra-script parallel EXECUTE has a known race condition on
`DROP` / `INSERT` colliding on the same table (tracked as **P3** in
[`STATUS.md`](STATUS.md)).

### Key findings

1. **Cold start is dominated by type inference, not SQL execution.**
   Both shapes spend ~10 of their ~13 cold minutes in hybrid type
   inference over the 208 parquet directories (~22 GB); the model-layer
   pass itself takes ~3 min in both shapes. The `--reuse-seeds` flag
   skips this entirely: the same database file is re-used, VIEWs are
   still there, only `BASE TABLE`s are dropped.
2. **Split DAG beats monolith when warm.** At `threads=4`, the split
   shape (3:06) is ~13 % faster than the monolith EXECUTE (3:34). The
   engine-level DAG parallelism beats the intra-EXECUTE sequential walk
   on this workload. This flips the "split-execute is for debug, not
   speed" assumption that was in the early notes.
3. **Threads > 4 is dead weight on 4 vCPU.** `t=4` and `t=8` are within
   ~1 s of each other (3:06 vs 3:07). The DAG is wide (peak 293) but the
   CPU count is the bottleneck; an 8-thread executor just context-
   switches more without gaining throughput.
4. **DuckDB's intra-query parallelism covers for `threads=1` executor.**
   `t=1` runs only 24 s slower than `t=4` on a DAG of 520 nodes,
   because each individual CTAS query still runs with 4 DuckDB threads
   (`connection.threads=4`). The executor thread pool only wins on
   layers that fan out wider than one query can parallelize internally.
5. **Parallel `EXECUTE` (intra-script DAG via `config.parallelism: N`)
   is currently unsafe** on migrated bodies — S3 hit the race after 3.9 s
   with `out.managers` being dropped by one worker while another was
   inserting into it. Until P3 is fixed, keep `parallelism: 1` on
   migrated multi-statement scripts. See
   [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md) §11.
6. **Peak memory grows with thread count**, as expected: 4.4 GB at
   t=1-implicit (S2) → 6.2 GB at t=4 → 7.5 GB at t=8. Budget 2 GB per
   executor thread on top of the DuckDB `memory_limit`.

### Layer profile (S4 t=4, warm)

From the executor logs:

| Layer | Models | Wall |
|---|---:|---:|
| 0 | 293 | 47 s (seed materialization + first staging CTAS) |
| 1 | 48 | 39 s |
| 2 | 17 | 10 s |
| 3 | 26 | 29 s |
| 4 | 29 | 9 s |
| 5 | 25 | 17 s |
| 6 | 12 | 12 s |
| 7 | 7 | 4 s |
| 8 | 26 | 5 s |
| 9–14 | 37 total | ~4 s |

The two costliest layers (0 and 3) account for >40 % of the wall; both
are parallel-friendly (293 and 26 models respectively), so they'll
benefit the most from any future CPU-scaling.

### Reproducing

The seven scenarios are scripted step by step in [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md)
§9 "Operational checklist for the next migration". In one sentence:
`juncture migrate-sync-pull` → `juncture sanitize` →
`juncture split-execute` → `juncture run` with the flag matrix above,
timing each through `/usr/bin/time -v` and watching with `vmstat 2` in
parallel.

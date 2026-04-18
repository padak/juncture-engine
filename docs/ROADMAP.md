# Roadmap

*Living document. Revise as priorities shift. For the current "where we
are right now" snapshot (Czech), see [`STATUS.md`](STATUS.md).*

## v0.1 — MVP · delivered (`04eaac5`)

- Project loader, YAML config, connection registry.
- Model discovery (SQL + Python), DAG building, cycle detection, layered
  topological sort, selector grammar (`+`, `tag:`).
- DuckDB adapter: `table` / `view` / `incremental` / `ephemeral`,
  per-thread cursors.
- SQLGlot parser: `ref()` extraction, dialect translation, best-effort
  validation.
- `@transform` decorator for Python models; shared `TransformContext`.
- Executor with parallelism, fail-fast, graceful skip-on-upstream-failure.
- Test runner with `not_null`, `unique`, `relationships`, `accepted_values`.
- CLI: `init`, `compile`, `run`, `test`, `docs`, `translate`.
- Examples: `simple` (4 SQL models) and `ecommerce` (5 SQL + 1 Python).
- Apache 2.0, Python 3.11+, zero required network access.

## v0.2 — "ergonomic MVP" · mostly delivered

Target: Juncture is safe to use on a real laptop pipeline of 20-50 models.

- [x] **Seeds (CSV)**: `seeds/*.csv` materialize as source tables.
- [x] **Seeds (Parquet)**: `seeds/<name>/*.parquet` as DuckDB `read_parquet`
      glob, materialized as VIEW (not TABLE) to avoid copying multi-GB
      datasets.
- [x] **Jinja mode** (`jinja: true`): StrictUndefined, `ref()` + `var()`
      helpers.
- [x] **Env var interpolation** in `juncture.yaml`: `${VAR}`,
      `${VAR:-fallback}`, `.env` auto-load.
- [x] **Custom SQL tests** under `tests/` (arbitrary `.sql` returning
      failing rows).
- [x] **Incremental state table** (`_juncture_state`).
- [x] **Migration tool** (`juncture migrate-keboola` + sync-pull migrator):
      converts Keboola SQL transformation layouts into a Juncture project.
- [x] **`EXECUTE` materialization** for multi-statement Snowflake SQL
      migrated as-is.
- [x] **Parallel EXECUTE**: intra-script DAG + `ThreadPoolExecutor` per
      topological layer, opt-in via `config.parallelism: N`. Default 1
      = sequential (back-compat).
- [x] **`juncture run --dry-run`**: plan-only mode — loads project,
      computes layers, surfaces intra-EXECUTE stats, without opening the
      adapter or loading seeds.
- [x] **`juncture split-execute`**: rewrites a multi-statement EXECUTE
      script into one `.sql` model per CTAS target with
      `{{ ref(...) }}` inference; non-CTAS statements (INSERT/UPDATE/…)
      collected into a residual EXECUTE model with auto-inferred
      depends_on.
- [x] **Hybrid type inference** for parquet seeds (full-scan < 1M rows,
      sample above).
- [x] **Parallel seed loading** via `ThreadPoolExecutor`.
- [x] `_discover_seeds` follows symlinks (for kbagent-produced parquet).
- [x] DuckDB `memory_limit` / `temp_directory` / `extensions` wiring.
- [ ] **Advanced selectors**: `path:marts/`, `state:modified+`.
- [ ] **Unit tests for models**: input → expected output in YAML.
- [ ] **`juncture docs --serve`**: minimal static HTML with DAG + column
      tables.
- [ ] **Structured logging** (JSON mode for ingestion).
- [ ] **pre-commit hooks**: ruff, mypy, basic schema.yml linting.

## Phase 3 — Slevomat E2E migration · in flight

Target: run a real 374-statement, 208-seed Snowflake transformation
end-to-end on DuckDB as the forcing function for parquet seeds, EXECUTE
materialization, and type inference.

- [x] `sync-pull` migrator reading `kbagent sync pull` filesystem layout.
- [x] Parquet seeds as symlinked VIEWs with quoted identifiers
      (`in.c-db.carts` stays verbatim).
- [x] Parallel seed load on 208 Slevomat parquet directories (~22 GB).
- [x] SQLGlot parse 374/374 Snowflake statements → DuckDB.
- [ ] **Successful executor run** — blocker: VARCHAR vs `INTEGER_LITERAL`
      in `CASE` because Storage stores everything as VARCHAR. Fix pending
      verification of `cfbc5ee` hybrid inference on the real dataset.
- [ ] Record a real-world benchmark number in `BENCHMARKS.md`.

Infrastructure: DO droplet 4 vCPU / 32 GB, volume at
`/mnt/volume_nyc1_juncture/juncture-data/slevomat-project/`,
read-only kbagent token for Slevomat.

Detailed status and known blockers: [`STATUS.md`](STATUS.md).

## v0.3 — "real backends"

Target: A Snowflake-based team can run the same project locally *and* in
production without code changes.

- [ ] **Snowflake adapter** (stub exists in `snowflake_adapter.py`): real
      connection, materialization, `fetch_ref` via Arrow, `MERGE INTO` for
      incrementals by `unique_key`, `CLUSTER BY`.
- [ ] **BigQuery adapter**: partitioning, clustering, external tables
      from GCS.
- [ ] **Postgres adapter**: DDL + `ON CONFLICT` for incrementals.
- [ ] **SQL dialect guard**: detect incompatible functions at compile time
      and suggest SQLGlot translations.
- [ ] **Connection-agnostic tests** — tests must pass on DuckDB locally
      and Snowflake in prod.
- [ ] **Performance budget** tests: Juncture overhead ≤ 10 % of raw query
      time (per Keboola Oldie but Goldie v2 request).

## v0.4 — "Keboola integration"

Target: Juncture ships as a Keboola component and replaces the four legacy
transformation components in a real project.

- [x] **Keboola component wrapper** scaffold (`docker/keboola/`,
      `juncture.keboola`) — reads `/data/config.json`, shells out to
      `juncture run`.
- [ ] **Real SAPI upload** of output tables (today the upload is a stub).
- [ ] **Auto-generate** `juncture.yaml` from Keboola config inside the
      wrapper (beyond what `sync-pull` does offline).
- [ ] **Input/output mapping** auto-detect via SQLGlot.
- [ ] **Dev/prod branch support**: Keboola branches → separate schemas.
- [ ] **OpenLineage events** per model; integrates with Keboola Lineage
      (emitter exists at `juncture.observability.lineage`).
- [ ] **Job artifacts**: every run uploads a `manifest.json` + logs.

## v1.0 — "production"

Target: stable API, semantic versioning, used on at least 3 customer
pipelines.

- [ ] API freeze for `juncture.core.*` public symbols.
- [ ] **Data contracts**: Pydantic models describing input/output schemas;
      CI-friendly `juncture validate-contract`.
- [ ] **Column-level lineage** exposed via the manifest and docs UI.
- [ ] **MCP server** (`juncture-mcp`) promoted from skeleton to shipping
      product — `list_models`, `compile`, `run_subgraph`, `translate_sql`,
      `explain_model` as MCP tools.
- [ ] **Official pypi release**, GitHub Actions CI/CD, docs on Read the
      Docs.
- [ ] **Python 3.13** support.

## v2.0 — "differentiating features"

Things we have no competitive pressure to ship now, but will truly matter:

- [ ] **Virtual data environments** à la SQLMesh: hashes of model
      attributes create snapshot tables; promotion is a pointer swap.
- [ ] **Semantic / metrics layer**: Cube-compatible DSL baked in.
- [ ] **AI dialect arbitrage**: run on DuckDB while data fits; spill over
      to Snowflake/BigQuery transparently.
- [ ] **Ibis materialization**: `@transform` functions using `ibis.Table`
      compiled to the target dialect.
- [ ] **Agentic authoring**: the Skill grows into a full agent loop —
      "build me a daily orders dashboard" → Juncture scaffolds, runs,
      tests, iterates, commits.

## Explicit non-goals

- **General-purpose orchestration**. Keboola flows + Dagster cover this.
- **Data ingestion**. dlt / Airbyte / Keboola extractors cover this.
- **Dashboarding**. Out of scope.
- **Fully managed cloud**. Juncture is a library + CLI; running it in
  Keboola (or in anyone's CI) is on the user.

## How to influence this roadmap

1. File an issue in the repo with the concrete use case.
2. If it's a bug blocking adoption, it jumps to the top of the next minor
   release.
3. Larger features go through a short RFC in `docs/rfcs/NNNN-title.md`.

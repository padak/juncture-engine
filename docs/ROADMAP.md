# Roadmap

*Living document. Revise as priorities shift.*

## MVP (0.1.x) — delivered

Shipped in this commit series:

- Project loader, YAML config, connection registry.
- Model discovery (SQL + Python), DAG building, cycle detection, layered
  topological sort, selector grammar (`+`, `tag:`).
- DuckDB adapter: table / view / incremental / ephemeral, per-thread cursors.
- SQLGlot parser: `ref()` extraction, dialect translation, best-effort
  validation.
- `@transform` decorator for Python models; shared `TransformContext`.
- Executor with parallelism, fail-fast, graceful skip-on-upstream-failure.
- Test runner with `not_null`, `unique`, `relationships`, `accepted_values`.
- CLI: `init`, `compile`, `run`, `test`, `docs`, `translate`.
- Examples: `simple` (4 SQL models) and `ecommerce` (5 SQL + 1 Python).
- 29 tests passing (unit + integration).
- Apache 2.0, Python 3.11+, zero required network access.

## v0.2 — "ergonomic MVP" (~2 weeks of work)

Target: Juncture is safe to use on a real laptop pipeline of 20-50 models.

- [ ] **Seeds**: CSV under `seeds/` materialize as source tables.
- [ ] **Jinja mode**: optional full Jinja rendering for dbt migrators.
- [ ] **Model selectors**: `path:marts/`, `state:modified+`, `@tag`.
- [ ] **Incremental state table** (`_juncture_state`) tracking last-run
      watermarks; `--full-refresh` flag respected.
- [ ] **Env var interpolation** in `juncture.yaml` (`${MY_VAR}`).
- [ ] **Variable layers**: `juncture.yaml` vars + `--var k=v` overrides +
      per-model `vars:` in schema.yml, merged deterministically.
- [ ] **Custom SQL tests** under `tests/` (arbitrary `.sql` returning
      failing rows).
- [ ] **Unit tests for models**: define input → expected output in YAML.
- [ ] **`juncture docs --serve`**: minimal static HTML with DAG + column
      tables (ship `docs` as a React site later).
- [ ] **Structured logging** (JSON mode for ingestion).
- [ ] **pre-commit hooks**: ruff, mypy, basic schema.yml linting.

## v0.3 — "real backends"

Target: A Snowflake-based team can run the same project locally *and* in
production without code changes.

- [ ] **Snowflake adapter**: connection, materialization, `fetch_ref` via
      Arrow, `MERGE INTO` for incrementals by `unique_key`, `CLUSTER BY`.
- [ ] **BigQuery adapter**: same plus partitioning, clustering, external
      tables from GCS.
- [ ] **Postgres adapter**: straightforward DDL + ON CONFLICT for
      incrementals.
- [ ] **SQL dialect guard**: detect incompatible functions at compile time
      and suggest translations via SQLGlot.
- [ ] **Connection-agnostic tests** — tests must pass on DuckDB locally and
      Snowflake in prod.
- [ ] **Performance budget** tests: Juncture overhead ≤10 % of raw query
      time (per Keboola Oldie but Goldie v2 request).

## v0.4 — "Keboola integration"

Target: Juncture ships as a Keboola component and replaces the four legacy
transformation components in a real project.

- [ ] **Keboola component wrapper** (`keboola-component-juncture`): Dockerfile,
      entrypoint reading `/data/config.json`, writing to Storage API via SAPI.
- [ ] **Auto-generate** `juncture.yaml` from Keboola config.
- [ ] **Input/output mapping**: optional auto-detect via SQLGlot.
- [ ] **Dev/prod branch support**: Keboola branches → separate schemas.
- [ ] **OpenLineage events** per model; integrates with Keboola Lineage.
- [ ] **Job artifacts**: every run uploads a `manifest.json` + logs.
- [ ] **Migration tool**: `juncture migrate-from-keboola --config /data/.../config.json`
      takes an existing Keboola SQL/Python transformation and converts it to
      a Juncture project.

## v1.0 — "production"

Target: stable API, semantic versioning, contributed docs, used on at
least 3 customer pipelines.

- [ ] API freeze for `juncture.core.*` public symbols.
- [ ] **Data contracts**: Pydantic models describing input/output schemas;
      CI-friendly `juncture validate-contract`.
- [ ] **Column-level lineage** exposed via the manifest and docs UI.
- [ ] **MCP server** (`juncture-mcp`): exposes `list_models`,
      `compile`, `run_subgraph`, `translate_sql`, `explain` as MCP tools.
- [ ] **Official pypi release**, GitHub Actions CI/CD, docs on Read the Docs.
- [ ] **Python 3.13** support.

## v2.0 — "differentiating features"

Things we have no competitive pressure to ship now, but will truly matter:

- [ ] **Virtual data environments** à la SQLMesh: hashes of model
      attributes create snapshot tables; promotion is a pointer swap.
- [ ] **Semantic / metrics layer**: Cube-compatible DSL baked in; metrics
      are queryable via a `/metrics` REST endpoint and accessible from BI.
- [ ] **AI dialect arbitrage**: run on DuckDB while data fits; spill over
      to Snowflake/BigQuery transparently. Cost estimator driven by
      SQLGlot query cost heuristics + adapter-reported stats.
- [ ] **Ibis materialization**: teams that prefer DataFrames over SQL can
      write `@transform` functions using `ibis.Table` and have us compile
      them to the target dialect.
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

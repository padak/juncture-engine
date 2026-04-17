# Changelog

All notable changes to Juncture are recorded here. Format: [Keep a
Changelog](https://keepachangelog.com/en/1.1.0/), versioning per
[SemVer](https://semver.org/).

## [Unreleased]

### Added

- **Incremental state store** (`juncture.core.state`): `_juncture_state`
  table with last_run_at, watermark, row_count, fingerprint. Opt-in for
  executor use in v0.3.
- **Benchmark script** (`scripts/benchmark.py`): compares Juncture vs raw
  DuckDB on a synthetic 3-model pipeline to track overhead.
- **Migration CLI** (`juncture migrate-keboola`): converts Keboola SQL
  transformation config JSON to a Juncture project.
- **OpenLineage emitter** (`juncture.observability.lineage`): START /
  COMPLETE / FAIL events per model run; SDK-optional.
- **Snowflake adapter stub** (`juncture.adapters.snowflake_adapter`):
  MERGE-based incrementals, `write_pandas` for Python models, SQLGlot
  translation at render time.
- **MCP server skeleton** (`juncture.mcp`): `list_models`, `compile_sql`,
  `run_subgraph`, `translate_sql`, `explain_model` as plain Python tools
  ready for MCP SDK wiring.
- **Keboola component wrapper** (`docker/keboola/`, `juncture.keboola`):
  Dockerfile + entrypoint reading `/data/config.json` and running the
  Juncture engine.
- **CI workflow** (`.github/workflows/ci.yml`): lint + test on 3.11 and
  3.12 across Ubuntu and macOS plus example smoke tests.
- **Makefile** with install/lint/test/examples/clean targets.
- **Custom SQL tests** under `tests/*.sql` — any query returning failing
  rows is a data test.
- **Seeds**: CSV files under `seeds/` load before the DAG runs; become
  ref-able nodes in the graph via a new `ModelKind.SEED`.
- **Jinja mode** (`jinja: true` in `juncture.yaml`): full Jinja rendering
  with `ref()` and `var()` helpers, StrictUndefined for typo catching.
- **Env var interpolation** in `juncture.yaml`: `${VAR}`,
  `${VAR:-default}`, `.env` auto-load.
- **Anthropic Skill** (`skills/juncture/SKILL.md` +
  `.claude/skills/juncture.md`): documentation + troubleshooting for AI
  agents authoring Juncture projects.

### Design documents

- `docs/RESEARCH.md` — competitive analysis of dbt, SQLMesh, Dagster,
  Coalesce, Dataform, dlt, DuckDB-native frameworks, Ibis, SQLGlot.
- `docs/DESIGN.md` — architecture, mental model, components, error
  model, Keboola integration plan.
- `docs/ROADMAP.md` — fases MVP → v0.2 → v0.3 → v0.4 → v1 → v2.

## [0.1.0a0] — 2026-04-17

### Added

- Core runtime: `Project`, `Model`, `DAG`, `Executor`, `Runner`.
- DuckDB adapter with TABLE / VIEW / INCREMENTAL / EPHEMERAL
  materialization; per-thread cursors.
- SQL parsing via SQLGlot: `ref()` extraction (two forms), dialect
  translation, validation.
- `@transform` decorator for Python models returning pandas / Polars /
  Arrow DataFrames.
- Data tests: `not_null`, `unique`, `relationships`, `accepted_values`.
- CLI: `init`, `compile`, `run`, `test`, `docs`, `translate`.
- Examples: `simple` (4 SQL models + 16 tests), `ecommerce` (5 SQL + 1
  Python, 23 tests).
- Tests: 29 passing (unit + integration).
- Apache 2.0 licence.

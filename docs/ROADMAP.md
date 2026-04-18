# Roadmap

*Living document. Phased task list with one checkbox per
deliverable. For the vision and rationale, see
[`VISION.md`](VISION.md); for the architecture, see
[`DESIGN.md`](DESIGN.md). Contributors scan this doc to pick up
work.*

## Phase 1 — DuckDB-first + web render + E2E proof

Goal: local DuckDB engine that handles a production-size Keboola
transformation end-to-end, visible through a small web UI.

### 1.1 MVP engine — delivered

- [x] Project loader, YAML config, connection registry (`04eaac5`).
- [x] Model discovery (SQL + Python), DAG building, cycle detection,
      layered topological sort.
- [x] Selector grammar (`+`, `tag:`).
- [x] DuckDB adapter: `table` / `view` / `incremental` / `ephemeral`,
      per-thread cursors.
- [x] SQLGlot parser: `ref()` extraction, dialect translation,
      best-effort validation.
- [x] `@transform` decorator for Python models; shared
      `TransformContext`.
- [x] Executor with parallelism, fail-fast, graceful
      skip-on-upstream-failure.
- [x] Test runner with `not_null`, `unique`, `relationships`,
      `accepted_values`.
- [x] CLI: `init`, `compile`, `run`, `test`, `docs`, `web` + `sql`
      / `migrate` / `debug` subcommand groups.
- [x] Examples: `simple` (4 SQL models) and `ecommerce`
      (5 SQL + 1 Python).
- [x] Apache 2.0, Python 3.11+, zero required network access.

### 1.2 Ergonomic MVP — delivered

Target: Juncture is safe to use on a real laptop pipeline of 20-50
models. (This is the scope previously tracked as v0.2; mostly folded
into Phase 1 by STRATEGY.)

- [x] Seeds (CSV): `seeds/*.csv` materialize as source tables.
- [x] Parquet seed loader + quoted identifiers (`in.c-db.carts` stays
      verbatim).
- [x] Parquet seeds as VIEW (not TABLE) + DuckDB `memory_limit` /
      `temp_directory` / `extensions` wiring.
- [x] `_discover_seeds` follows symlinked directories
      (`os.walk(followlinks=True)`) — required for kbagent-produced
      parquet pools shared across projects.
- [x] Hybrid type inference for parquet seeds (full-scan < 1 M rows,
      sampled above).
- [x] Parallel seed loading via `ThreadPoolExecutor`.
- [x] Jinja mode (`jinja: true`): `StrictUndefined`, `ref()` + `var()`
      helpers.
- [x] Env var interpolation in `juncture.yaml`: `${VAR}`,
      `${VAR:-fallback}`, `.env` auto-load.
- [x] Custom SQL tests under `tests/` (arbitrary `.sql` returning
      failing rows).
- [x] Incremental state table (`_juncture_state`).

### 1.3 Multi-statement EXECUTE + migration helpers — delivered

Target: be able to ingest a real Keboola Snowflake transformation
as-is and run it against DuckDB.

- [x] `EXECUTE` materialization for multi-statement Snowflake SQL
      migrated as-is (no `CREATE OR REPLACE` wrapping).
- [x] `migrate-keboola` migrator from raw Keboola config JSON.
- [x] `sync-pull` migrator reading the `kbagent sync pull` filesystem
      layout (symlinked parquet seeds, `EXECUTE` materialization).
- [x] `harmonize_case_types` AST pass + `juncture sanitize` CLI for
      Snowflake → DuckDB CASE type mismatches.
- [x] `StatementNode` + `build_statement_dag` — intra-script DAG API in
      the parser (`a300e37`).
- [x] Parallel EXECUTE: `config.parallelism: N` iterates intra-script
      layers through `ThreadPoolExecutor`. Default `N=1` is sequential
      (back-compat).
- [x] `juncture run --dry-run`: plan-only mode showing model layers +
      intra-EXECUTE layers without opening the adapter or loading
      seeds.
- [x] `juncture split-execute`: rewrites an EXECUTE monolith into one
      `.sql` model per CTAS target with `{{ ref(...) }}` inference;
      non-CTAS statements collected into a residual EXECUTE model with
      auto-inferred `depends_on`.
- [x] `juncture run --reuse-seeds`: skips re-inference for already
      materialized seeds.
- [x] `--parallelism` / `-P` CLI override for benchmarking EXECUTE
      runs.
- [x] `juncture compile --dot <file>` exports the DAG as Graphviz DOT.

### 1.4 Pilot migration E2E proof — delivered

Target: run a real 374-statement, 208-seed Snowflake transformation
end-to-end on DuckDB as the forcing function for parquet seeds,
EXECUTE materialization, and type inference. Done on the pilot
migration of a production-size Keboola transformation.

- [x] SQLGlot parses 374/374 Snowflake statements → DuckDB.
- [x] Parallel seed load on 208 parquet directories (~22 GB) from the
      pilot migration.
- [x] Pilot migration end-to-end success on DuckDB (374/374
      statements executed).
- [x] Cross-dialect migration field notes, failure taxonomy, and
      repair-loop blueprint captured for the next migration round.

### 1.5 Post-pilot hardening (in-flight)

The goal is to collapse the next migration from ~26 repair rounds
to 2–3 (derived from the pilot migration field notes).

- [x] **P0** — `juncture run --continue-on-error` on EXECUTE
      materialization (`duckdb_adapter._execute_raw`); emit a
      `RunReport` with per-statement errors instead of a single
      `AdapterError`.
- [x] **P0** — schema-aware `translate_sql(schema=...)` via
      `sqlglot.optimizer.annotate_types`, plus new AST passes:
      `harmonize_binary_ops`, `harmonize_function_args`,
      `fix_timestamp_arithmetic`. (`fix_date_diff_signature` already
      handled by SQLGlot's built-in DateDiff translation; re-added
      as an AST pass only if a regression fires.)
- [x] **P1** — sentinel detector in `type_inference`: per-column
      sentinel profiles on `InferenceResult.sentinels` via
      `detect_sentinels()` (downstream injection into `CAST`/`TRY_CAST`
      wrappers is a next follow-up).
- [x] **P1** — error classifier `juncture.diagnostics.classify_error`
      + `juncture diagnostics` CLI: regex → bucket lookup powering
      both AI prompts and human triage.
- [x] **P1** — `migrate-sync-pull --validate` pre-flight report.
- [ ] **P2** — statement-dependency DAG filter that separates primary
      errors from cascade errors, re-using `build_statement_dag`.
- [ ] **P2** — `juncture repair --max-iterations N --agent-model
      sonnet` (new subcommand wrapping the diagnostics → agent → patch
      loop).
- [ ] **P3** — intra-script parallel EXECUTE race fix (currently
      forces `parallelism: 1` on migrated bodies).

### 1.6 Ergonomic gaps

- [x] **Model disable toggle** — `disabled: true` in `schema.yml` +
      `juncture run --disable a,b` / `--enable-only x,y` runtime
      overrides. Disabled models report `status=disabled`; downstream
      gets `status=skipped` with `skipped_reason=upstream_disabled`.
      Does not fail the run. Exposed in `compile --json` manifest.
- [x] **Jinja macros under `macros/`** — dbt-style global macro loader.
      When `jinja: true`, every `{% macro %}` under `macros/**/*.sql`
      is auto-registered as a Jinja global so models call
      `{{ my_date(col) }}` without `{% import %}`. Powers the "define
      a rule once, use everywhere" story (VISION §Problem 2).
- [x] **Profiles (`profiles:` block)** — named overlays over
      `juncture.yaml` so one project describes several environments
      (local DuckDB, shared staging, prod Snowflake). Per-key merge on
      `vars:` and `connections.<name>:`, wholesale replace on scalars.
      `--profile` CLI flag + `JUNCTURE_PROFILE` env var + top-level
      `profile:` field, in that precedence. Unknown profile name fails
      fast. Backward-compatible: projects without a `profiles:` block
      keep the legacy `profile: <connection_name>` behaviour. Unblocks
      kbagent-branch per-schema dev/prod split (see
      [`docs/CONFIGURATION.md`](CONFIGURATION.md#profiles-profiles-block)).
- [ ] Advanced selectors: `path:marts/`, `state:modified+`.
- [ ] Model unit tests (input → expected output in YAML).
- [ ] Structured JSON logging mode for ingestion.
- [ ] Pre-commit hooks: ruff, mypy, basic `schema.yml` linting.

### 1.7 Phase 1 gate items

- [x] **Web render** — `juncture web --project <p>` starts a stdlib
      `http.server` on 127.0.0.1 that serves a single-page cytoscape.js
      DAG view plus a run-history table from
      `<project>/target/run_history.jsonl`. Zero extras dependency;
      vendored cytoscape.js. Reads the compile manifest per-request
      so schema.yml edits show up on refresh. Closes the binding gate
      that was holding Phase 2 adapter work.
- [x] Pilot-migration benchmark numbers recorded — seven scenarios
      (monolith cold/warm, parallel EXECUTE, split DAG cold +
      threads 1/4/8).

### 1.8 Web UI v2 — proposed

Four milestones (M1–M4) that extend the Phase 1 web render from
"glance only" to a tool usable by both a data engineer (source
view, diagnostics, seeds, reliability) and a CDO (ownership, SLA,
PII propagation, portfolio view). Runs in parallel with Phase 2
adapter work — neither blocks the other.

- [x] **M1 — P0 "readable"**: source viewer, kind-vs-status split,
      clickable per-model drill-down in Runs, tests panel.
      Landed with `/api/models/<name>` + `path` in manifest;
      vendored prism.js for SQL/Python syntax highlighting;
      DAG now encodes kind as shape+fill and status as border
      thickness/colour so seed / SQL / Python are distinguishable
      even on all-success runs.
- [x] **M2 — P1 project overview + search + export**:
      `juncture.yaml` + README render, fulltext DAG search,
      manifest / OpenLineage export. Adds `/api/project/config`,
      `/api/project/readme`, `/api/project/git` (best-effort with
      graceful fallback when git is absent), `/api/manifest/openlineage`
      (static RunEvent shape per model). Vendored markdown-it for
      README rendering. DAG search fades non-matches by name / tag /
      description; Esc clears.
- [x] **M3 — P1 diagnostics + seeds + reliability**: diagnostics
      bucket panel, seeds tab with sentinels, per-model
      last-20-runs sparkline. Adds `/api/seeds`, `/api/runs/<id>/diagnostics`,
      `/api/models/<name>/history`, and a bonus `/api/llm-knowledge`
      endpoint that bundles manifest + source + seeds + latest run
      into a single LLM-ingestible JSON (exposed via new "LLM kb"
      button in the DAG toolbar).
- [x] **M4 — P2 governance**: `schema.yml` gains `owner` / `team` /
      `criticality` / `sla` / `docs`; Portfolio tab, data contracts
      view, PII / retention badges, reliability dashboard.
      Landed as two PRs (schema first, UI second). UI exposes
      Portfolio + Reliability top-bar tabs, inline PII ring
      propagation in the DAG, governance block in the Metadata tab,
      contract endpoint with "would break" downstream list, and
      long-form docs rendered via the markdown-it instance vendored
      for M2.

## Phase 2 — Production backends + Keboola component

Goal: the same project runs locally on DuckDB and in production on
Snowflake / BigQuery / JDBC, and is deployable as a Keboola
component.

### 2.1 Warehouse adapters

- [ ] Snowflake adapter (stub exists in `snowflake_adapter.py`): real
      connection, `materialize_sql`, `fetch_ref` via Arrow,
      `MERGE INTO` incrementals by `unique_key`, `CLUSTER BY`.
- [ ] BigQuery adapter: partitioning, clustering, external tables from
      GCS.
- [ ] JDBC adapter: connect via a JDBC driver URL, generic DDL,
      dialect-appropriate incrementals (`MERGE` / `ON CONFLICT` /
      staged insert) depending on the underlying database.

### 2.2 Cross-dialect guarantees

- [ ] SQL dialect guard: compile-time detection of incompatible
      functions with SQLGlot translation suggestions.
- [ ] Connection-agnostic test suite: the same `schema.yml` tests pass
      on DuckDB locally and Snowflake in production.

### 2.3 Keboola component

- [x] Keboola component wrapper scaffold (`docker/keboola/`,
      `juncture.keboola`) — reads `/data/config.json`, shells out to
      `juncture run`.
- [ ] Real SAPI upload of output tables (today's upload is a stub).
- [ ] Auto-generate `juncture.yaml` from Keboola config inside the
      wrapper (beyond what `sync-pull` does offline).
- [ ] Input/output mapping auto-detect via SQLGlot.
- [ ] Dev/prod branch support — Keboola branches map to separate
      schemas.

### 2.4 Observability + artifacts

- [ ] OpenLineage START / COMPLETE / FAIL events per model wired to
      Keboola Lineage (emitter skeleton at
      `juncture.observability.lineage`).
- [ ] Job artifacts — every run uploads `manifest.json` + logs.

## Phase 3 — v1.0 production

Goal: a stable, semantically versioned API used on at least three
real customer pipelines, published on pypi with docs on Read the
Docs.

### 3.1 API + contracts

- [ ] API freeze for `juncture.core.*` public symbols — semver,
      deprecation policy, `__all__` audit.
- [ ] Data contracts: Pydantic models describing input/output
      schemas; `juncture validate-contract` CI command.
- [ ] Column-level lineage exposed via the manifest and the docs UI
      from Phase 1.

### 3.2 Agent surface

- [ ] MCP server (`juncture-mcp`) promoted from skeleton to shipping
      product — `list_models`, `compile`, `run_subgraph`,
      `translate_sql`, `explain_model` as production MCP tools.

### 3.3 Release engineering

- [ ] Python 3.13 support.
- [ ] GitHub Actions CI/CD shipping tagged pypi releases.
- [ ] Official pypi release and docs on Read the Docs.
- [ ] Performance budget tests (Juncture overhead ≤ 10 % of raw query
      time, per the "Oldie but Goldie v2" benchmark pipeline).

## Phase 4 — v2.0 differentiators

Goal: ship the features that make Juncture uniquely valuable.

### 4.1 Environment + arbitrage

- [ ] Virtual data environments à la SQLMesh: hashes of model
      attributes create snapshot tables; promotion is a pointer swap,
      so dev branches don't re-run full tables.
- [ ] AI dialect arbitrage: auto-switch DuckDB ↔ warehouse based on
      data size and cost; run on DuckDB while data fits, spill to
      Snowflake/BigQuery transparently.

### 4.2 Semantic + authoring

- [ ] Semantic / metrics layer: Cube-compatible DSL baked in so
      metrics live with the models.
- [ ] Ibis materialization: `@transform` functions using `ibis.Table`
      compiled to the target dialect.
- [ ] Agentic authoring: full agent loop where a prompt such as
      "build me a daily orders dashboard" scaffolds, runs, tests, and
      iterates the project end-to-end.

## Explicit non-goals

- General-purpose orchestration (Dagster / Airflow cover this).
- Data ingestion (dlt / Airbyte / Keboola extractors).
- Dashboarding.
- Fully managed cloud hosting.

## How to influence this roadmap

1. File an issue in the repo with the concrete use case.
2. If it blocks adoption, it jumps to the top of the next minor release.
3. Larger features go through a short RFC (internal `docs/priv/rfcs/`
   tree).

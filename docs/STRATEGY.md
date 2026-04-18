# Strategy: how we deliver Juncture, in phases

*Version 0.1 · 2026-04-18*

This document answers **"how do we deliver it, in phases?"** It is the
sequenced delivery plan for Juncture. Contributors use it to choose what
to work on next; stakeholders use it to know when major milestones ship.

For **why** Juncture exists, see [`VISION.md`](VISION.md). For the
**architecture** that each phase builds on, see [`DESIGN.md`](DESIGN.md).
For the **detailed task list** inside the current phase, see
[`ROADMAP.md`](ROADMAP.md). For the **weekly snapshot** of where we
stand today, see [`STATUS.md`](STATUS.md). For the **lessons feeding
Phase 1's finish**, see [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md).

## Phase 1 — DuckDB-first + web render + E2E proof *(in progress)*

**Goal.** A local, DuckDB-only engine that handles a real
production-size Keboola transformation end-to-end, and shows it visually
via a small web UI.

**Done-done criterion.** A new user can clone the repo, run
`juncture init`, load a Keboola transformation via
`juncture migrate-sync-pull`, run it on DuckDB, and open a browser at
`localhost:N` to see the DAG and run history for the project.

**Key deliverables.**

- [x] MVP engine — project loader, DAG, executor, adapter registry,
  `@transform`, test runner, CLI (`04eaac5`).
- [x] Seed loader (CSV + parquet) with parquet-as-VIEW materialization.
- [x] Hybrid type inference for parquet seeds (full-scan < 1 M rows,
  sampled above).
- [x] `EXECUTE` materialization for multi-statement Snowflake SQL
  migrated as-is.
- [x] Parallel EXECUTE (`config.parallelism: N`, opt-in, back-compat at
  `N=1`).
- [x] `migrate-keboola` and `migrate-sync-pull` migrators (kbagent
  filesystem layout → Juncture project with symlinked parquet seeds).
- [x] `juncture sanitize` CLI, `harmonize_case_types` AST pass,
  `split-execute`, `--reuse-seeds`, `--dry-run`.
- [x] **Pilot migration E2E success** — 374/374 statements translated
  and executed against a 22 GB parquet seed corpus from a real Keboola
  customer.
- [ ] **Continue-on-error + diagnostics** — `juncture run
  --continue-on-error` for EXECUTE, `juncture diagnostics classify`
  error classifier (Sprint A from
  [`MIGRATION_TIPS.md`](MIGRATION_TIPS.md) §8).
- [ ] **Schema-aware `translate_sql`** — feed `Project.seed_schemas()`
  into SQLGlot `annotate_types`; `harmonize_binary_ops` inserts
  `TRY_CAST` around VARCHAR operands (Sprint B).
- [ ] **Sentinel detector** in `type_inference` — emit per-column
  sentinel profiles so `CAST(col AS INT)` expands to
  `TRY_CAST(NULLIF(col, sentinel) AS BIGINT)` automatically.
- [ ] **Intra-script parallel EXECUTE race fix** — currently forces
  `parallelism: 1` on migrated bodies (Sprint A).
- [ ] **Web render** — small Python HTTP server (`juncture docs --serve`
  or equivalent) rendering the compiled DAG, per-model schema, and run
  history from the manifest. **This is the binding gate item for
  Phase 1.** Not yet in the codebase.
- [ ] Pilot-migration benchmark numbers recorded in
  [`BENCHMARKS.md`](BENCHMARKS.md).

**Out of scope for this phase.** Snowflake, BigQuery, or Postgres
adapters. Keboola component wrapper with real SAPI upload. MCP server.
Semantic layer, virtual environments, agent authoring. Anything that
requires a cloud warehouse.

## Phase 2 — Production backends + Keboola component

**Goal.** The same project runs locally on DuckDB and in production on
Snowflake / BigQuery / Postgres, and is deployable as a Keboola
component.

**Done-done criterion.** A Keboola customer can move an existing SQL
transformation from the legacy `snowflake-transformation` component to
the Juncture component with only a config change, and it runs
successfully on their production Snowflake workspace.

**Key deliverables.**

- Snowflake adapter — real connection, `materialize_sql`,
  `fetch_ref` via Arrow, `MERGE INTO` incrementals by `unique_key`,
  `CLUSTER BY`.
- BigQuery adapter — partitioning, clustering, external tables from
  GCS.
- Postgres adapter — DDL + `ON CONFLICT` incrementals.
- SQL dialect guard — compile-time detection of incompatible functions
  with SQLGlot translation suggestions.
- Connection-agnostic tests — the same `schema.yml` test suite passes
  on DuckDB locally and Snowflake in production.
- Keboola component Docker image with **real SAPI upload** of output
  tables (today's upload is a stub).
- Auto-generated `juncture.yaml` from Keboola config inside the wrapper
  (beyond what `sync-pull` does offline).
- Input/output mapping auto-detect via SQLGlot.
- Dev/prod branch support — Keboola branches map to separate schemas.
- OpenLineage START / COMPLETE / FAIL events wired to Keboola Lineage
  (emitter skeleton at `juncture.observability.lineage`).
- Job artifacts — every run uploads `manifest.json` + logs.

**Out of scope for this phase.** MCP server as a shipping product.
Virtual data environments. Productized agentic repair loop. Column-level
lineage exposed through the docs UI (manifest is enough here). Ibis
materialization.

## Phase 3 — v1.0 production

**Goal.** A stable, semantically versioned API used on at least three
real customer pipelines, published on pypi with docs on Read the Docs.

**Done-done criterion.** API freeze for `juncture.core.*` public
symbols. Official pypi release. Docs live on Read the Docs. At least
three customers have Juncture in their production pipelines.

**Key deliverables.**

- API freeze for `juncture.core.*` — semver, deprecation policy,
  `__all__` audit.
- Data contracts — Pydantic models describing input/output schemas;
  `juncture validate-contract` CI command.
- Column-level lineage exposed via the manifest and the docs UI from
  Phase 1.
- MCP server (`juncture-mcp`) promoted from skeleton to shipping —
  `list_models`, `compile`, `run_subgraph`, `translate_sql`,
  `explain_model` as production MCP tools.
- Python 3.13 support.
- GitHub Actions CI/CD shipping tagged pypi releases.
- Official pypi release and docs on Read the Docs.

**Out of scope for this phase.** Any of the v2.0 differentiator
features below. Multi-tenant orchestration. Dashboarding. General
hosting.

## Phase 4 — v2.0 differentiators

**Goal.** Ship the features that make Juncture uniquely valuable
compared to dbt and SQLMesh.

**Done-done criterion.** At least two of the four differentiator
features below are shipped and demonstrated on a real customer
pipeline.

**Key deliverables.**

- **Virtual data environments** — SQLMesh-style hash-based snapshot
  tables with pointer-swap promotion, so dev branches don't re-run full
  tables.
- **Semantic / metrics layer** — Cube-compatible DSL baked in, so
  metrics live with the models.
- **AI dialect arbitrage** — auto-switch DuckDB ↔ warehouse based on
  data size and cost; run on DuckDB while data fits, spill to
  Snowflake/BigQuery transparently.
- **Agentic authoring** — full agent loop where a prompt such as
  "build me a daily orders dashboard" scaffolds, runs, tests, and
  iterates the project end-to-end.

**Out of scope for this phase.** Anything the market forces on us
before v1.0 ships. Scope-creep beyond the four differentiators above.

## Delivery principles

- **"Done" means demonstrable, not feature-complete.** Every phase ends
  with a working demo an external person can watch — not a checklist of
  merged PRs.
- **Small conventional commits.** `feat:`, `fix:`, `docs:`, `refactor:`,
  `test:`, `chore:`. Docs update *with* the code, not after.
- **Sprint-level breakdown lives in [`ROADMAP.md`](ROADMAP.md).** This
  document does not list tasks. If it starts to, move them.
- **Phase gate is explicit.** Before Phase N+1 work begins,
  [`STATUS.md`](STATUS.md) must record the done-done criterion for
  Phase N as met.
- **Web render is the binding gate between Phase 1 and Phase 2.** The
  original project directive called out web rendering as a Phase 1
  requirement. No Phase 2 adapter work starts until a user can open
  `localhost:N` and see the DAG + run history of a production-size
  Keboola transformation running on DuckDB. This is non-negotiable; it is how we
  prove the local-first story before taking on warehouse surface area.
- **Each phase owns its out-of-scope list.** If something comes up that
  feels urgent but is marked out of scope, file it to
  [`ROADMAP.md`](ROADMAP.md); do not let it drift the phase.

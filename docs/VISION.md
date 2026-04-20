# Vision: Juncture

*What we are building, and why. One stable reference. This document
moves only when the vision itself moves — not when sprints move,
when the architecture evolves (see [`DESIGN.md`](DESIGN.md)), or
when the competitive landscape shifts.*

## Pitch

**Juncture is a multi-backend SQL + Python transformation engine that
replaces Keboola's four legacy transformation components
(`snowflake-transformation`, `python-transformation`,
`duckdb-transformation`, `dbt-transformation`) with a single engine.**
It is local-first and DuckDB-native: a developer with a laptop, a CSV,
and `uv tool install --with pandas git+https://github.com/padak/juncture-engine` can
write, run, and test a transformation with no cloud in the loop. The same project deploys unchanged to
Snowflake, BigQuery, or Postgres via SQLGlot dialect translation, and
the same engine ships as a thin Keboola component. Code lives in git
(or anywhere else), SQL and Python models share one DAG, tests are
first-class, and every workflow is callable from a stable JSON CLI
built for agents.

## The problem

Today's Keboola transformation stack is four disconnected products
that share only their unhappiness. Customers consistently hit the
same walls:

1. **Code is locked in the platform, not in git.** There is no
   cross-project reuse, no pull requests, no diffs, no branches, no
   "grep across all transformations."
2. **No macros or shared blocks.** A `WHERE` clause that defines
   "active customer" or "EU region" has to be copy-pasted into every
   script; drift is inevitable.
3. **Weak parametrization.** `run_date`, date ranges, feature flags
   and variants need ad-hoc plumbing per component. There is no
   uniform `var()` contract across SQL and Python.
4. **No DAG visibility.** Scripts chain through output tables by
   convention only. Nothing tells you which of 200 scripts produces
   `orders_enriched`, which downstream readers depend on it, or what
   would break if it changed.
5. **No parallelism.** Thirty independent scripts run sequentially,
   taking 30× longer than necessary. Customers hand-orchestrate or
   accept the wait.
6. **No conditional execution.** "Run this block only on Tuesdays,"
   "skip when upstream is empty," "re-run the last 7 days" all require
   external orchestration tricks.
7. **No side-by-side versioning.** A/B-testing two variants of a
   transformation block means duplicating everything around it; there
   is no native "keep both, route 10 % to candidate" primitive.
8. **Python and SQL cannot mix in one flow.** The components are
   siblings, not collaborators; a Python model cannot `ref()` a SQL
   model in the same DAG.
9. **No lineage.** "Which tables does this SQL actually read?" is a
   grep-and-hope exercise today. Downstream catalogs get nothing to
   work with.
10. **Single-backend vendor lock-in.** A Snowflake-transformation is a
    Snowflake artifact, full stop. Moving it to DuckDB or BigQuery
    means a rewrite.

Juncture exists because the underlying model — "a SQL file that
declares what it reads and writes, executed as part of a DAG" — is
sound. The implementations are what aged badly.

## What Juncture is

### Engine

- **Multi-backend.** DuckDB is the reference implementation and
  ships in MVP. Snowflake, BigQuery and Postgres adapters share one
  small `Adapter` interface. A single Juncture project runs
  unchanged on any of them; SQLGlot handles the dialect diffs.
- **SQL + Python in one DAG.** Python models are decorated with
  `@transform` and receive a `ctx` that exposes `ctx.ref(...)` and
  `ctx.vars()`. A Python model can depend on a SQL upstream and vice
  versa — no second component, no separate orchestration.
- **Parallelism by default.** The executor walks the DAG in layers
  through a thread pool; independent models run concurrently with a
  single CLI flag. Intra-script parallel EXECUTE is available for
  migrated multi-statement bodies.
- **Incremental materializations.** `table`, `view`, `incremental`,
  `ephemeral`, `execute`. Incremental state lives in a
  `_juncture_state` table in the target schema.
- **Data tests are first-class.** `not_null`, `unique`,
  `relationships`, `accepted_values`, custom SQL tests — all compile
  to `SELECT COUNT(*)` assertions and block the run on failure.

### Deployment

- **Local-first.** Zero required network access in standalone mode.
  A new user goes from `pip install` to a green run against DuckDB
  on their laptop in under five minutes.
- **Keboola component.** The same engine ships as a thin Docker
  wrapper that reads `/data/config.json` and shells out to
  `juncture run`. One binary, four legacy components retired.
- **Code lives anywhere.** Git, Keboola configuration storage, a
  customer's own monorepo — a Juncture project is a plain directory
  with `juncture.yaml` and `models/`. Nothing about the engine
  presumes where the files came from.

### Differentiators

- **Backend arbitrage via dialect translation.** The same project
  runs on DuckDB locally and Snowflake in production. SQLGlot
  translation means authors write one SQL and let the engine target
  the cheapest backend that fits the workload. This is unique in
  the transformation-tool space.
- **Agent-friendly by construction.** `juncture compile --json` emits
  a stable DAG manifest. Every CLI command exits non-zero on
  failure and emits structured JSON on `--json`. An MCP server
  (`list_models`, `compile_sql`, `run_subgraph`, `translate_sql`,
  `explain_model`) is on the roadmap so any LLM host can drive
  Juncture directly. A Claude Agent Skill ships in the repo.
- **Auto-documentation.** `schema.yml` column specs plus SQLGlot
  lineage extraction produce a browsable DAG + column catalog from
  code alone — no hand-maintained docs.
- **Semantic / metrics layer.** A v2 goal: express "active customer"
  or "monthly recurring revenue" once, consume from SQL, Python,
  and BI tools alike. Aligns with Cube / Malloy directionally.

### Scale

Lightweight enough for ad-hoc data cleanup on a laptop, and serious
enough for production workloads. The pilot migration of a real
Keboola customer pipeline ran **208 parquet seeds × 374 SQL
statements** end-to-end through Juncture — proving the engine
handles production-grade volume, not toy examples.

## Non-goals

Juncture is deliberately narrow. It is **not** any of:

1. **An orchestrator.** Scheduling, retries, sensors, cross-tool
   coordination are Dagster's and Airflow's job. Juncture emits the
   artifacts an orchestrator needs.
2. **A catalog.** Lineage storage, impact analysis UIs and
   cross-system metadata belong to OpenLineage and DataHub.
   Juncture emits OpenLineage events; it does not host them.
3. **An ingestion tool.** Pulling rows from SaaS APIs or OLTP
   databases is dlt's, Airbyte's, and Keboola extractors' job.
   Juncture starts at "the data is in a table or parquet file."
4. **A dashboard or a managed cloud service.** No BI, no hosted
   multi-tenant runtime. The engine is a library and a CLI; the
   Keboola wrapper provides the only managed deployment story.

We compose with these alternatives — orchestration, catalog,
ingestion, dashboarding — rather than competing.

## Success criteria

We will know Juncture has won when:

- **≥ 3 real customer pipelines** have been migrated from legacy
  Keboola transformation components and are running on Juncture in
  production.
- **Performance overhead ≤ 10 %** vs raw DuckDB on equivalent
  workloads, measured against the "Oldie but Goldie v2" benchmark
  pipeline.
- **End-to-end local demo works in under 5 minutes** for a new user:
  `uv tool install --with pandas git+https://github.com/padak/juncture-engine` →
  `juncture init` → `juncture run` → green tests, with no cloud
  credentials.
- **The Keboola component ships as the single replacement** for
  `snowflake-transformation`, `python-transformation`,
  `duckdb-transformation`, and `dbt-transformation`, and legacy
  customers have a documented migration path off each.

For the architecture that implements these choices, see
[`DESIGN.md`](DESIGN.md).

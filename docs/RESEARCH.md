# Research: competitive landscape for Juncture

*Last updated: 2026-04-17. Sources: Perplexity (Sonar Pro), with links inline.*

This document summarizes the competitive and technological landscape that
shaped the Juncture design. It is intentionally opinionated: we don't survey
every tool, just those whose ideas we either borrow or reject.

## TL;DR

| Dimension | Winner(s) | What we take |
|-----------|-----------|--------------|
| DAG + SQL rendering | dbt | `ref()` model composition |
| Parallelism, incrementals, plans | SQLMesh | Virtual environments (future), SCD2, layered executor |
| Asset-centric mental model | Dagster | `@transform` decorator mirrors `@asset` |
| Multi-dialect SQL | SQLGlot | Use directly for parsing + translation |
| Column-level lineage | Coalesce, SQLMesh | Column lineage exposed as first-class (later) |
| Python-first DataFrame | Ibis | Consider as future alternative to raw Python, not MVP |
| Schema evolution, typing | dlt | Use Pydantic models for contracts (v2) |
| DuckDB ecosystem | dbt-duckdb, yato, lea | Local-first, embedded, zero-infra |

The #1 complaint about transformation tooling in 2025 is: **"transformation is
the biggest bottleneck in the modern data stack — scattered SQL scripts, slow
deploys, inconsistent metrics, unreliable lineage."** Juncture aims squarely
at this pain.

---

## 1. dbt (data build tool)

**The good**

- Canonical project layout: `models/`, `macros/`, `seeds/`, `tests/`.
- `ref()` composition that auto-builds a DAG.
- Huge ecosystem, well-documented, familiar to every analytics engineer.

**The bad (voice of the community, 2024-2025)**

- **Jinja as both config and programming language.** Benn Stancil: "Jinja is
  not great at either" ([benn.substack.com/p/how-dbt-fails](https://benn.substack.com/p/how-dbt-fails)).
- **No type checking**, no schema validation at compile time. Type mismatches
  and renamed columns fail at runtime
  ([foundational.io/blog/dbt-limitations-avoid-errors](https://www.foundational.io/blog/dbt-limitations-avoid-errors)).
- **Monolithic scale problems**: projects with 1000+ models slow the IDE,
  manifest parse is seconds, `dbt run` against a whole project becomes risky
  ([GH dbt-core discussions #6725](https://github.com/dbt-labs/dbt-core/discussions/6725)).
- **Only-a-lineage-graph, not a relational model.** Semantic/metric layers are
  bolted on top, debugging cascades is painful.
- **Python models are second class** — dbt-python runs on warehouses, not
  locally, requires Snowpark/Dataproc, brittle.
- **Incrementals require manual `unique_key`, `is_incremental()` ceremony**;
  no native SCD2, no lookback window.
- **Dev environments are schema-based** — every change requires a full
  re-run in a dev schema. No zero-cost snapshots.
- **Orchestration is outsourced** (Airflow, Dagster, dbt Cloud). Each of
  those, separately, needs to understand the DAG.

Takeaway: dbt's mental model is right; its implementation layer is aging.

## 2. SQLMesh (Tobiko Data)

**What makes it interesting**

- **Virtual data environments.** Changes create new snapshots of tables via
  fingerprinting (attribute hash); promotion is a "virtual update" (pointer
  swap) — 134x faster and 123x cheaper than dbt's full re-runs
  ([Tobiko benchmark](https://tobikodata.com/blog/tobiko-dbt-benchmark-databricks)).
- **First-class Python models** alongside SQL with Python macros instead of
  Jinja.
- **Rich incremental strategies**: INCREMENTAL by time range / partition /
  unique key, **native SCD Type 2**, lookback windows, data diffing.
- **Built-in scheduler** with stateful plans (like Terraform `plan`/`apply`).
- **SQLGlot is a SQLMesh project** — deep semantic SQL parsing, column-level
  lineage and impact analysis come for free.

**Weaknesses**

- Smaller community and ecosystem than dbt.
- Steeper learning curve (plans, fingerprints, SCD2).
- Semantic layer is a work in progress.

Juncture borrows heavily from SQLMesh philosophy — deep SQL parsing, Python +
SQL without Jinja, plan-before-apply semantics — while staying more
approachable in the MVP. Virtual data environments are on the v2 roadmap.

## 3. Dagster (Software-defined assets)

**The mental model**

- `@asset` decorators declare *data assets* (what should exist); Dagster
  figures out how to reconcile them with the world.
- Dependencies are declared inline — the DAG emerges from code.
- IO managers cleanly separate "business logic" from "where data lives."
- Integrates with dbt via `@dbt-assets`, but can also fully replace dbt.
- Polyglot: Python + dbt + Fivetran/Airbyte in one graph.

**What we take**

- The decorator-based declaration style for Python models. `@transform`
  mirrors `@asset` but scoped to transformations, not full orchestration.
- Asset-first framing — users think in terms of "what should exist", not
  "when should a task run".

**What we don't**

- Dagster is an orchestrator-first, transformation-second. Juncture is the
  inverse. The Keboola platform already handles orchestration.

## 4. Coalesce.io

- UI-first, column-aware: each model is a node on a visual graph with
  columns as first-class citizens.
- Column-level lineage and impact analysis are baked in.
- UDPs (User-Defined Patterns) are reusable templates encoding team
  conventions.
- Designed for Snowflake, Databricks, BigQuery, Redshift, Fabric.

Juncture stays code-first but mimics the **column-aware modeling** idea: the
`schema.yml` file lists columns with descriptions and tests, and we parse
every model's SQL with SQLGlot to derive column-level lineage automatically.
The UI can be added later on top of the manifest.

## 5. Google Dataform

- BigQuery-native, `.sqlx` files with embedded JavaScript/Jinja.
- Two flavors: open-source Dataform Core (CLI) and managed service.
- Strong incremental support tailored to BigQuery partitioned tables.

Too BigQuery-specific for our needs. Useful as a reference for how to keep
incremental code readable.

## 6. dlt (Data Load Tool)

- Python-first ingestion: schemas are inferred from data and evolve
  automatically.
- Pydantic models describe contracts.
- Incremental loading is built-in.

dlt is *ingestion*; Juncture is *transformation*. They compose well:
`dlt → Keboola Storage → Juncture`. We adopt dlt's **Pydantic-first
contracts** for a future data-contracts module, not for MVP.

## 7. DuckDB-native frameworks: yato, lea, crabwalk

Public signal is thin (mid-2025), but the pattern is clear: all three offer
**"the smallest possible dbt for DuckDB"** — a project folder of `.sql` files
scanned at compile time, `ref()`-style macros, zero infrastructure, embedded
execution. None has a dominant community, each is a one-person project.

Juncture fills the same niche and adds: Python models in the same DAG,
multi-backend adapters, a real testing framework, and an AI-agent interface.

## 8. SQLGlot — the engine that makes this possible

- Parses, optimizes, transpiles across 31+ SQL dialects.
- DuckDB, Snowflake, BigQuery, Postgres, Trino are well supported.
- Translation is *best effort*: window functions, JSON, UDFs may not survive
  unchanged. Edge cases:
  - Snowflake VARIANT vs DuckDB structs: precision loss on decimals.
  - Timestamp nanoseconds lost DuckDB → Snowflake.
  - Identifier case: Postgres lowercases, Snowflake uppercases unless quoted.
  - `LEAST`/`GREATEST` NULL propagation differs (Snowflake/BigQuery ignore
    NULLs; DuckDB/Postgres return a single-element array).
- Used by SQLMesh, Ibis, SQLFrame, Daft in production.

Juncture uses SQLGlot directly:

1. For `ref()` extraction (plus our own regex for `$ref()`).
2. For validation: "does this SQL parse in the target dialect?"
3. For translation when a user points a Snowflake project at DuckDB or vice
   versa (`juncture translate` CLI).

## 9. Ibis

- Python DataFrame API compiling to 20+ backends via SQLGlot.
- Lazy evaluation, single API whether you target DuckDB, Snowflake, Spark,
  Polars, or pandas.
- 9.0+ fully replaced SQLAlchemy with SQLGlot.

Tempting as an alternative to raw SQL, but:

- Most analytics engineers want to read and write SQL, not Ibis DSL.
- Ibis still has edge cases per backend.
- A Juncture v2 may add an `ibis` materialization mode for teams that want it.

---

## What modern data engineers say they want (2025)

Synthesized from Coalesce/dbt conference talks, r/dataengineering, and
industry blogs:

1. **Fast local dev loop** — run my transformation on my laptop, in
   seconds, against a sampled slice. dbt Cloud makes this slower than local.
2. **Escape from warehouse lock-in** — Snowflake and BigQuery bills
   hurt; lakehouse formats (Iceberg, Delta, Parquet) + DuckDB for compute
   have become a credible alternative.
3. **AI integration** — Copilot-for-SQL, agent-driven pipeline edits,
   auto-generated docs. But users want the LLM's plan to be readable and
   editable, not a black box.
4. **Data quality as a first-class citizen** — assertions, great
   expectations, column tests, row tests.
5. **Observability via OpenLineage** — push-based lineage events so that
   catalog integrations work without polling.
6. **Multi-cloud, multi-backend** — portability is an explicit requirement,
   not a nice-to-have.
7. **Open standards over proprietary tools** — SQLGlot, OpenLineage,
   dbt-core. dbt Labs' commercial focus worries many teams.

## Design decisions that fall out of this

1. **DuckDB first, multi-backend from day one.** The adapter interface is
   deliberately small so Snowflake/BigQuery/Postgres adapters can follow.
2. **SQLGlot is our parser — no Jinja dependency.** Jinja can wrap our own
   `ref()` macro for dbt compatibility, but it's not required.
3. **`@transform` Python models are first-class, not second-class.** The
   same executor runs SQL and Python models; Python models can reference
   SQL upstreams and vice versa.
4. **Local first.** MVP runs entirely on a laptop with a DuckDB file;
   cloud adapters are plug-ins.
5. **Tests are just SQL.** Assertions compile to count-of-failures queries.
6. **Built-in agent support.** A Claude Agent Skill ships in the repo so
   an agent can scaffold, run, debug and iterate on Juncture projects
   without human hand-holding (subject to granted permissions).
7. **OpenLineage events on every run** (post-MVP) so the Keboola lineage
   story works out of the box.

## Things we intentionally do *not* do in MVP

- No Ibis DSL. Users write SQL or Python DataFrames.
- No column-level lineage graph in the UI (we produce the data, not the UI).
- No cloud runtime. That's the Keboola wrapper's job.
- No virtual environments (SQLMesh-style). Planned for v2.
- No semantic / metrics layer. Planned for v2 (Cube-style or Malloy-style).

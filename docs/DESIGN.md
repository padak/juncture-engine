# Design: Juncture architecture

*Version 0.1 · 2026-04-17*

This document describes the architecture of Juncture. It is the source of
truth for implementation decisions. Each section ends with "open questions"
for future review.

## 1. Goals, non-goals, constraints

### Goals

1. **Replace SQL + Python + dbt transformation components** in Keboola with
   one engine.
2. **Local-first**: a developer with Python and a CSV file can write,
   test and run a transformation on their laptop without any cloud.
3. **Multi-backend**: DuckDB locally, Snowflake/BigQuery/Postgres in prod.
4. **Mix SQL and Python** in the same DAG without ceremony.
5. **Parallelism by default** — independent models run concurrently.
6. **Testable**: column-level assertions are first-class; failures block.
7. **Agent-friendly**: every workflow is callable from a CLI with stable
   output; an Anthropic Skill ships in the repo.
8. **Keboola-deployable** as a thin wrapper around the standalone engine.

### Non-goals (for MVP)

- Being an orchestrator (Dagster's job).
- Being a catalog (OpenLineage + DataHub's job).
- Being an ingestion tool (dlt's / Airbyte's job).
- Supporting every exotic SQL dialect. DuckDB + Snowflake + BigQuery +
  Postgres cover 95 % of Keboola users.
- A UI. The CLI + JSON manifest are enough to drive any UI later.

### Hard constraints

- Python 3.11+ only (typing improvements we want to use).
- Zero required network access in standalone mode.
- Apache 2.0 licence to keep it safe for enterprises and vendors.

## 2. Mental model

A **Juncture project** is a directory of **models**. A model is either:

- A `.sql` file, or
- A Python function decorated with `@transform`.

A model declares its dependencies via `ref('other_model')` (SQL) or
`depends_on=[...]` (Python). Juncture parses this, builds a **DAG**, and the
**Executor** runs it layer-by-layer through an **Adapter** (DuckDB, Snowflake,
BigQuery, Postgres). Data tests declared in `schema.yml` compile to SQL count
queries after the run.

```
┌────────────────────┐    ┌─────────────┐
│  juncture.yaml     │    │ schema.yml  │
└─────────┬──────────┘    └──────┬──────┘
          │                      │
┌─────────▼──────────┐    ┌──────▼──────┐
│      Project       ├────►   Schemas   │
└─────────┬──────────┘    └─────────────┘
          │
          │ discover
          ▼
┌────────────────────┐    ┌─────────────┐
│       Models       │    │   DAG       │
│ (SQL + Python)     ├────► (networkx)  │
└────────────────────┘    └──────┬──────┘
                                 │
                                 │ run
                                 ▼
                        ┌─────────────────┐
                        │    Executor     │
                        │ (thread pool)   │
                        └────────┬────────┘
                                 │ materialize
                                 ▼
                        ┌─────────────────┐
                        │     Adapter     │
                        │   (DuckDB etc.) │
                        └─────────────────┘
                                 │ after success
                                 ▼
                        ┌─────────────────┐
                        │   Test Runner   │
                        └─────────────────┘
```

## 3. Components

### 3.1 Project (`juncture.core.project`)

- Reads `juncture.yaml` (config, connections, defaults).
- Walks `models/` to discover SQL files and Python modules.
- Parses `schema.yml` to attach descriptions, tests, columns.
- Returns a list of `Model` objects.

Conventions:

- Model name = file stem (`stg_orders.sql` → `stg_orders`).
- Python functions decorated with `@transform(name=...)` can override the
  name; default is the function name.
- Subdirectories under `models/` are for organization only; Juncture does
  not derive names from them. This avoids the "two models with same name
  in different dirs" footgun.

### 3.2 Model (`juncture.core.model`)

A `Model` is a dataclass with:

- `name`, `kind` (SQL / PYTHON), `materialization` (table / view /
  incremental / ephemeral)
- `sql` *or* `python_callable`
- `depends_on: set[str]`
- `columns: list[ColumnSpec]`
- `tags`, `description`, `unique_key`, `schedule_cron`, free-form `config`.

### 3.3 DAG (`juncture.core.dag`)

Wraps a `networkx.DiGraph`. Operations:

- `from_models(models)` — validate no cycles, no missing refs, no dupes.
- `topological_order()` — flat serialized order.
- `layers()` — successive sets of mutually independent nodes. This is how
  the executor parallelizes.
- `select(patterns)` — dbt-compatible selectors: `+name`, `name+`,
  `+name+`, `tag:name`.

### 3.4 Parsers (`juncture.parsers.sqlglot_parser`)

- `extract_refs(sql)` — finds `{{ ref('x') }}` and `$ref(x)` macros.
- `render_refs(sql, resolver)` — swap macros for FQ identifiers at runtime.
- `parse_sql(sql, dialect)` — validate; return deps.
- `translate_sql(sql, read, write)` — SQLGlot dialect translation.
- `extract_table_references(sql)` — best-effort table discovery for lineage
  even when ref() isn't used (e.g. raw SQL from legacy code).

Two macro forms accepted:

- `{{ ref('orders') }}` — dbt-style, familiar.
- `$ref(orders)` — brace-free, works inside shell-escaped strings.

### 3.5 Adapters (`juncture.adapters`)

Each adapter is a subclass of `Adapter` with this minimum surface:

```python
class Adapter(ABC):
    type_name: str
    dialect: str
    def connect(): ...
    def close(): ...
    def materialize_sql(model, rendered_sql, schema) -> MaterializationResult: ...
    def materialize_python(model, context, schema) -> MaterializationResult: ...
    def fetch_ref(name) -> Arrow.Table: ...
    def execute_arrow(query) -> Arrow.Table: ...
    def resolve(name, schema) -> fqn
```

**Materialization strategies**

| Kind        | DuckDB                                       | Snowflake (v1)                                 |
|-------------|----------------------------------------------|------------------------------------------------|
| table       | `CREATE OR REPLACE TABLE`                    | `CREATE OR REPLACE TABLE`                      |
| view        | `CREATE OR REPLACE VIEW`                     | `CREATE OR REPLACE VIEW`                       |
| incremental | `CREATE IF NOT EXISTS` + `INSERT OR REPLACE` | `MERGE INTO` on `unique_key`                   |
| ephemeral   | inlined upstream at render time              | inlined                                        |
| execute     | runs the script as-is (multi-statement)      | (not applicable — Snowflake runs source as-is) |

`execute` is used by the Keboola sync-pull migrator (see 3.10). It does
**not** wrap the SQL in a `CREATE OR REPLACE` shell; the whole migrated
script (hundreds of statements) is handed to the adapter, which splits
on `;` and runs statement-by-statement. This lets a 12 000-line Snowflake
transformation compile unchanged and surfaces Snowflake-only constructs
as real parse errors the author can fix iteratively.

**Parallel EXECUTE**: when the model declares
`config.parallelism: N` (N > 1), the adapter parses the body into an
intra-script DAG via `juncture.parsers.sqlglot_parser.build_statement_dag`
and walks `networkx.topological_generations` layer by layer through a
`ThreadPoolExecutor(max_workers=N)`. Each worker picks a fresh cursor
via `_thread_cursor()` and issues `USE "<schema>"` before running its
statement (DuckDB cursors don't inherit the parent connection's current
schema). Layer elapsed times are logged at INFO. Default `parallelism`
is 1 = classic sequential behaviour, fully back-compatible.

**Thread safety**: each model run — and each seed load — gets its own
`cursor()` via `DuckDBAdapter._thread_cursor`. Sharing one DuckDB
connection across threads was the first bug we hit — documented here so
no one regresses it.

### 3.6 Executor (`juncture.core.executor`)

- Given a DAG, iterates layers, runs each layer through a `ThreadPoolExecutor`.
- Per-layer parallelism is bounded by `threads` (CLI flag, default 4).
- A failed model marks its descendants as `skipped` and — if `fail_fast`
  (default true) — cancels pending futures and returns.
- All runs produce `ModelRun(status, result, error, elapsed_seconds)`.
- `run_with_refs()` builds a ref resolver (model name → FQN) before running
  so SQL sees stable identifiers.

### 3.7 Testing (`juncture.testing`)

- `TestRunner` iterates models, compiles each `schema.yml` test to SQL
  returning the count of failing rows, executes through the adapter.
- Built-in assertions: `not_null`, `unique`, `relationships`,
  `accepted_values`. All four compile to single-row `SELECT COUNT(*)` queries.
- Custom SQL tests (file-based) are planned for v1.

### 3.8 CLI (`juncture.cli`)

```
juncture init [path] [--name]
juncture compile [--project p] [--json]
juncture run [--select] [--exclude] [--threads] [--test] [--var k=v]
juncture test [--select] [--threads]
juncture docs [--output]
juncture translate <sql> --from snowflake --to duckdb
```

Output uses Rich tables for humans and JSON for agents (`--json` where
applicable). Exit code is non-zero on any model or test failure.

### 3.9 Runner (`juncture.core.runner`)

The high-level entry point used by CLI and Keboola wrapper. Takes a
`RunRequest`, returns a `RunReport`.

### 3.10 Seeds (`juncture.core.seeds`, `juncture.core.type_inference`)

Seeds sit under `seeds/` and are loaded once before the model DAG runs.
They are not part of the model DAG; a model that wants to consume a seed
references it by name via `ref('my_seed')` exactly like another model.

Two seed layouts are recognized:

- `seeds/<name>.csv` — single CSV file, loaded via DuckDB `read_csv_auto`
  and materialized as a **table**.
- `seeds/<name>/*.parquet` — directory of parquet slices, loaded via
  DuckDB `read_parquet` with a glob and materialized as a **VIEW**. This
  layout is what `kbagent storage unload-table --file-type parquet`
  produces; VIEW avoids copying multi-GB datasets into the DuckDB file.

Conventions and gotchas:

- **Seed names may contain dots** (e.g. `in.c-db.carts`) so migrated
  Snowflake-style quoted identifiers survive unchanged. Don't sanitize.
- **`_discover_seeds` follows symlinks** (`os.walk(followlinks=True)`).
  The sync-pull migrator (3.11) symlinks parquet dirs into `seeds/`.
- **Parallel load** uses `ThreadPoolExecutor`, bounded by the
  connection's `threads` setting. Every worker calls
  `DuckDBAdapter._thread_cursor()` so DuckDB sees a fresh per-thread
  cursor over the shared connection.
- **Type inference is hybrid**. For parquet seeds where DuckDB's inferred
  types are insufficient (e.g. all columns VARCHAR in Keboola Storage):
  - Under 1 M rows: **full scan**, every cell checked against
    `BOOLEAN → BIGINT → DOUBLE → DATE → TIMESTAMP → VARCHAR` precedence
    using strict regex (DuckDB's `TRY_CAST` is too lenient).
  - At or above 1 M rows: **sample** a configurable number of rows from
    each column.
  - Results are cached in `.juncture/type_cache.json` so re-runs skip the
    scan.
  - Users can override via `seeds/schema.yml` (same grammar as model
    `schema.yml`) when inference would guess wrong.

### 3.11 Migration (`juncture.migration`)

Three migration tools ship today:

1. `juncture migrate-keboola` — raw Keboola configuration-API JSON.
2. `juncture migrate-sync-pull` — kbagent sync-pull filesystem layout.
3. `juncture split-execute` — refactor an existing EXECUTE monolith
   into per-table models (see 3.11.1 below).

Two migrators share the `juncture migrate-keboola*` CLI family:

- `keboola_sql` — input is a raw Keboola configuration-API JSON payload.
  Emits a minimal Juncture project.
- `keboola_sync_pull` — input is the filesystem layout produced by
  `kbagent sync pull`:

  ```
  main/transformation/keboola.snowflake-transformation/<name>/
      _config.yml        # input/output mapping + parameters
      _description.md
      _jobs.jsonl
      transform.sql      # the single multi-statement SQL body
  ```

  Produces:

  ```
  <output_dir>/
      juncture.yaml                # DuckDB local connection
      seeds/<destination>/...      # symlinks into parquet data dir
      models/<transformation>.sql  # transform.sql verbatim, EXECUTE mat.
      MIGRATION.md                 # human-readable log of what was done
  ```

  The SQL is migrated **as-is** using the `execute` materialization
  (3.5). Snowflake constructs DuckDB rejects surface as real parse errors
  the user can fix iteratively, rather than being silently rewritten.

#### 3.11.1 split-execute (`juncture.migration.split_execute`)

Turns a single EXECUTE model into N Juncture-native `.sql` models
(+ optional residual EXECUTE block). Flow:

1. Split the script via `split_statements`.
2. Phase 1 — discover all tables produced by CTAS (`CREATE [OR REPLACE]
   TABLE|VIEW X AS SELECT ...`). These become ref() targets.
3. Phase 2 — for each statement:
   - CTAS → extract its inner `SELECT` (`_ctas_body`), rewrite every
     intra-script table reference to a sentinel identifier, render via
     SQLGlot, then string-substitute the sentinel for
     `{{ ref('name') }}`. External tables (seeds, source data) stay
     raw.
   - Anything else (INSERT / UPDATE / DELETE / DDL / SET / USE / parse
     errors) → residual, with its produced-in-script references
     accumulated as the residual's `depends_on`.
4. Unique-name guard: if the same CTAS target appears more than once,
   raise `SplitExecuteError`. Silent merging would change semantics
   when readers between the two producers exist.
5. Residual SQL is written with leading `SELECT 1 FROM {{ ref('X') }}
   LIMIT 0` hints so Juncture's regular ref() extraction picks up the
   DAG edges without a schema.yml `depends_on:` field (which today
   doesn't exist).

`juncture split-execute --dry-run` previews the plan without writing
files. Typical post-split flow is `compile --json` → inspect new DAG
→ `run`. Split projects get parallel-model execution from the standard
Juncture executor "for free" and no longer need `config.parallelism`
on the root monolith.

## 4. Configuration: `juncture.yaml`

```yaml
name: my_project
version: 0.1.0
profile: local
default_materialization: table
default_schema: main

connections:
  local:
    type: duckdb
    path: data/my_project.duckdb
    threads: 4
  production:
    type: snowflake
    account: my_account
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    database: ANALYTICS
    warehouse: COMPUTE_WH
```

Rules:

- Secrets are referenced via `${ENV_VAR}`, never hard-coded.
- `profile` selects a connection; override with `--connection`.
- Missing required vars fail fast at startup (never default silently).

## 5. Keboola integration (later)

The Keboola wrapper is a thin Docker image with this entrypoint:

```bash
juncture run \
  --project /code \
  --connection from-keboola \
  --threads $KEBOOLA_THREADS \
  --var table_date=$KEBOOLA_RUN_DATE
```

Steps the wrapper does:

1. Read Keboola component config (`/data/config.json`).
2. Generate a `juncture.yaml` with a `from-keboola` connection that points
   at the configured Snowflake/BigQuery workspace.
3. Copy or symlink input tables into the models path if needed.
4. After `juncture run`, upload tables listed in output mapping back to
   Keboola Storage via SAPI.
5. Emit OpenLineage events for each model.

## 6. AI-agent integration

Juncture is deliberately designed so an agent can:

1. Call `juncture compile --json` to see the DAG.
2. Write new `.sql` or `.py` files in `models/`.
3. Call `juncture run --select +new_model --test` to verify.
4. Parse JSON output and iterate.

The Claude Agent Skill (`skills/juncture/SKILL.md`) documents idioms,
conventions and troubleshooting for this loop.

A future MCP server (`juncture-mcp`) will expose tools like
`juncture.list_models`, `juncture.compile_sql`, `juncture.run_subgraph`,
`juncture.translate_sql` over the MCP protocol so any LLM host can drive
Juncture directly.

### 3.12 Diagnostics (`juncture.diagnostics`)

Pure-function classifier that maps a DuckDB error string to an
``ErrorClassification(bucket, subcategory, fix_hint, operands)``.
Regex-driven, no adapter/Project dependency, so it can be called from
any layer (CLI, web UI, agent prompts, MCP tools).

Buckets are coarse: ``type_mismatch`` / ``function_signature`` /
``conversion`` / ``missing_object`` / ``idempotence`` / ``parser`` /
``other``. Subcategories are fine-grained (``sentinel_string_to_typed``,
``comparison_type_mismatch``). ``fix_hint`` is a template string with
named operand substitution so a repair agent or a human can apply the
fix directly.

Consumed by ``juncture diagnostics``, which runs the project with
``--continue-on-error``, aggregates every ``StatementError`` across
models, and prints a bucketised summary plus one representative error
per subcategory. See ``docs/MIGRATION_TIPS.md`` §5.2 for the taxonomy
the regex rules encode; add new patterns to
``_RULES`` in ``classifier.py`` when a migration surfaces one.

### 3.13 Schema-aware translate (``sqlglot_parser.translate_sql``)

Takes an optional ``schema={table: {col: duckdb_type}}`` argument. When
provided, the tree is first ``qualify()``-ed, then ``annotate_types()``
populates per-node ``.type`` metadata. Three additional AST passes then
rewrite the cross-dialect gaps DuckDB's strict typing surfaces:

- ``harmonize_binary_ops`` wraps the VARCHAR side of a comparison in
  ``TRY_CAST(... AS other)`` when the other side is numeric or
  temporal (taxonomy rows #3, #4, #9, #10).
- ``harmonize_function_args`` wraps a VARCHAR argument to SUM / AVG /
  ROUND / etc. in ``TRY_CAST(... AS DOUBLE)`` (taxonomy row #6).
- ``fix_timestamp_arithmetic`` rewrites ``ts ± integer_literal`` to
  ``ts ± INTERVAL 'n' DAY`` (taxonomy row #8).

Project supplies the schema via ``Project.seed_schemas()`` which
combines ``seeds/schema.yml`` overrides with parquet DESCRIBE metadata
(via a throwaway in-memory DuckDB). Result is cached in
``.juncture/seed_schemas.json``.

When ``qualify()`` raises (ambiguous refs, unknown tables, CROSS JOIN
without USING) we fall back to un-annotated translation for that
statement — the dialect translation still happens, just without the
type-aware harmonisation. This is intentional: a migrated body is
expected to hold dialect oddities SQLGlot can't fully normalise, and
refusing the whole body over one is a worse bargain than losing one
statement's type-fixes.

### 3.14 EXECUTE continue-on-error

``duckdb_adapter._execute_raw`` honours ``model.config.continue_on_error``.
When true, per-statement failures are appended to
``MaterializationResult.statement_errors`` (a list of ``StatementError``
records with index, truncated SQL, error message, and layer for
parallel mode) instead of aborting. The executor then downgrades the
model's ``ModelRun.status`` from ``success`` to ``partial`` — distinct
from ``failed`` so cascade-skip doesn't fire and ``RunReport.ok``
stays true. Exposed via ``juncture run --continue-on-error`` and
consumed by ``juncture diagnostics``.

## 7. Cross-cutting concerns

### Error model

- `ProjectError` — bad config, missing files.
- `DAGError` — cycle, duplicate name, missing ref.
- `AdapterError` — backend-specific failures.
- `StatementError` — individual statement failure inside an EXECUTE
  body under continue-on-error (does not abort the model run;
  collected into `MaterializationResult.statement_errors`).
- `TestRunner` surfaces adapter exceptions as test failures with `error`.

### Logging

- Standard `logging` module under `juncture.*`.
- Rich CLI renders tables; JSON mode is for agents.
- Logs go to stderr; tables to stdout.

### Configuration defaults

Following the "no silent defaults for required values" rule:

- `juncture.yaml` without `connections` is an error.
- Connection without required params is an error.
- Missing env var referenced as `${X}` is an error.

## 8. Future work (briefly)

- v1: Snowflake, BigQuery, Postgres adapters; incremental backfills by
  partition; data contracts (Pydantic); OpenLineage events; seeds (CSV).
- v2: Virtual data environments (SQLMesh-style); semantic / metrics layer
  (Cube-compatible?); MCP server; Ibis materialization option.
- v3: Column-level lineage UI; AI dialect arbitrage (run on DuckDB until
  size exceeds threshold, then transparently run on warehouse).

## Open questions

1. **Jinja compatibility**. Should we fully embrace Jinja (templates) or
   keep our own mini-macro? Current answer: mini-macro plus a Jinja mode
   behind a flag in v1.
2. **Seeds**. CSV → table is trivial, but do we allow YAML descriptions
   of seeds? Current answer: v1.
3. **Unit tests for models** (input → expected output). Planned for v1.
4. **Where state lives for incremental tracking**. Current answer: a
   `_juncture_state` table in the target schema. Evaluate DuckLake for v2.
5. **CDC / realtime**. Not in scope.

---
name: juncture
description: Design, author, run and debug Juncture data transformation projects. Use when the user asks to create transformations, build DAGs, write SQL or Python models, test data pipelines, translate SQL between warehouses (DuckDB/Snowflake/BigQuery/Postgres), or migrate dbt/SQL transformations into Juncture. Triggers -- "juncture", "transformation project", "DuckDB pipeline", "dbt alternative", "model + schema.yml", "multi-backend transformation".
---

# Juncture skill

Juncture is a multi-backend SQL + Python transformation engine. Local-first,
DuckDB-native, Keboola-compatible. This skill teaches an agent to scaffold,
modify, run, debug and explain Juncture projects safely.

## When to use this skill

Use this skill when the user:

- Wants to create a new data pipeline in Juncture.
- Has an existing dbt / Keboola SQL transformation they want to migrate.
- Wants to add Python transformations alongside SQL.
- Wants to run or debug a Juncture project locally.
- Needs SQL translated between DuckDB, Snowflake, BigQuery, or Postgres.

Do **not** use this skill for: warehouse administration, data ingestion
(that's dlt / Airbyte / Keboola extractors), or BI dashboarding.

## Project shape

```
my_project/
├── juncture.yaml           # required: config + connections
├── models/                 # required: where transformations live
│   ├── staging/
│   │   ├── stg_orders.sql
│   │   └── stg_customers.sql
│   ├── marts/
│   │   ├── daily_revenue.sql
│   │   └── customer_segment.py   # Python @transform
│   └── schema.yml          # column descriptions + data tests
├── tests/                  # optional: custom SQL tests
├── macros/                 # optional: shared SQL snippets (v1)
└── seeds/                  # optional: CSV seed data (v1)
```

## Core concepts in 60 seconds

1. **Model** — a single `.sql` file *or* a Python function decorated with
   `@transform`. Each model is one node in the DAG.
2. **Dependencies** — declared via `{{ ref('other_model') }}` in SQL
   or `@transform(depends_on=['other'])` in Python. Juncture parses both
   and builds the DAG automatically.
3. **Materialization** — how the model result is persisted:
   `table` (default), `view`, `incremental`, `ephemeral`.
4. **schema.yml** — sits next to model files, declares columns,
   descriptions and data tests per model.
5. **Adapter** — the backend that runs the model (DuckDB locally,
   Snowflake / BigQuery / Postgres in prod).
6. **Executor** — runs the DAG layer by layer; each layer runs in parallel
   threads. Default threads: 4.

## The authoring loop an agent should follow

1. **Inspect first**: `juncture compile --project PATH --json`. Understand
   existing models, dependencies, and what is currently broken.
2. **Make the smallest change**: add one `.sql` file or one Python function.
3. **Run just that subtree**: `juncture run --select +your_model+ --test`.
4. **Inspect outputs**: DuckDB file is at the path from `juncture.yaml`
   (`data/my_project.duckdb` by default). Query with `duckdb` CLI:
   ```
   duckdb data/my_project.duckdb -c 'SELECT * FROM main.your_model LIMIT 5'
   ```
5. **Commit** in small, reviewable chunks.

## CLI reference (agents should prefer `--json` where available)

| Command | Purpose | Agent-friendly? |
|---------|---------|-----------------|
| `juncture init PATH [--name N]` | Scaffold a project | ✓ |
| `juncture compile [--json]` | Parse DAG; validate refs | ✓ with `--json` |
| `juncture run [--select S] [--exclude X] [--test]` | Execute | ✓ (exit code) |
| `juncture test` | Run data tests only | ✓ |
| `juncture docs [-o OUT]` | Write `manifest.json` (lineage) | ✓ |
| `juncture translate SQL --from snowflake --to duckdb` | Dialect translate | ✓ |

Exit code 0 = success, non-zero = any model or test failed.

### Selector grammar

| Selector | Meaning |
|----------|---------|
| `orders` | just this model |
| `+orders` | this + all upstream ancestors |
| `orders+` | this + all downstream descendants |
| `+orders+` | all three |
| `tag:marts` | all models tagged `marts` in schema.yml |

## Writing SQL models

Keep SQL files simple. One `SELECT` per file.

```sql
-- models/marts/daily_revenue.sql
SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(amount) AS revenue
FROM {{ ref('stg_orders') }}
GROUP BY order_date
```

Rules:

- Reference other models via `{{ ref('name') }}` **only**. Raw table names
  bypass dependency inference.
- Do not use `CREATE TABLE` or `INSERT` directly; Juncture wraps the
  `SELECT` in the right DDL for you.
- Put one model per file; the filename (minus `.sql`) is the model name.

## Writing Python models

```python
# models/marts/customer_segment.py
import pandas as pd
from juncture import transform


@transform(
    name="customer_segment",
    depends_on=["fct_completed_orders", "dim_customers"],
    description="RFM-style customer segmentation.",
)
def customer_segment(ctx):
    orders = ctx.ref("fct_completed_orders").to_pandas()
    customers = ctx.ref("dim_customers").to_pandas()
    # ... business logic ...
    return merged_df  # pandas / polars / pyarrow all accepted
```

Rules:

- The decorated function takes a single `ctx` argument
  (`juncture.core.context.TransformContext`).
- `ctx.ref(name)` returns the upstream model as a `pyarrow.Table`
  (has `to_pandas()` and `to_pylist()`).
- Return a pandas DataFrame, Polars DataFrame, or Arrow Table. Juncture
  persists it per the model's materialization.
- Do not `print()`; use `ctx.logger.info(...)`.

## Writing data tests (schema.yml)

```yaml
models:
  - name: stg_orders
    description: Raw orders staged for downstream use.
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: customer_id
        tests:
          - not_null
          - relationships: { to: main.stg_customers, field: id }
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: [completed, refunded, pending]
```

Built-in tests: `not_null`, `unique`, `relationships`, `accepted_values`.

## Dialect translation (`juncture translate`)

When migrating from Snowflake to DuckDB (local dev) or vice versa:

```bash
juncture translate 'SELECT TO_VARCHAR(42)' --from snowflake --to duckdb
```

Known edge cases (flag these when translating):

- `VARIANT` column types (Snowflake) have no direct DuckDB equivalent; use
  structs or cast to VARCHAR.
- Nanosecond timestamps in Snowflake become microseconds in DuckDB.
- Identifier case: Snowflake is uppercase by default, DuckDB lowercase.
  Quote identifiers when case matters.
- `LEAST`/`GREATEST` NULL handling differs.

Always verify translated SQL by running the transformation end-to-end.

## Connections in `juncture.yaml`

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
    type: snowflake  # v0.3+
    account: "${SNOWFLAKE_ACCOUNT}"
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    database: ANALYTICS
    warehouse: COMPUTE_WH
```

Rules:

- Never write secrets in `juncture.yaml`. Use `${ENV_VAR}` and a `.env`
  file that is gitignored.
- `--connection NAME` overrides `profile:` for a single run.

## Troubleshooting (stuff that will happen)

| Symptom | Cause | Fix |
|---------|-------|-----|
| `DAGError: depends on unknown model` | typo in `ref()` or missing file | check `models/` and filename |
| `DAGError: Cycle detected: a → b → a` | model A refs B, B refs A | break the cycle; use an ephemeral CTE if intermediate |
| `TypeError: 'NoneType' object is not subscriptable` | old bug with shared DuckDB connection | upgrade to ≥0.1.0a0; we now use per-thread cursors |
| `AdapterError: Incremental requires unique_key` | incremental model without `unique_key:` in schema.yml | declare a unique key |
| Python model fails with `KeyError: 'col'` | `ctx.ref()` returned different columns than expected | check upstream SQL; run `juncture compile --json` |
| Tests fail with `ParseError` | SQL uses syntax SQLGlot can't read | write the failing row query manually in `tests/` |

## Migration from dbt

- `models/` directory maps 1:1.
- `ref()` syntax is identical; `source()` becomes a direct table reference
  in MVP (source definitions are v0.2).
- `dbt_project.yml` → `juncture.yaml` (connections, default_materialization,
  default_schema).
- `schema.yml` mostly compatible; `tests` key is the same.
- `profiles.yml` → the `connections:` block inside `juncture.yaml`.
- Jinja macros beyond `ref()` are not in MVP; use `@transform` Python
  functions or the Jinja mode (v0.2).

## Migration from Keboola SQL / Python transformation

1. Export the transformation code into a new folder.
2. Each code block becomes a separate `.sql` file under `models/`.
3. Input mapping tables become either seeds (for constants) or the
   upstream models (for data from prior Keboola jobs).
4. Output mapping becomes `{{ ref() }}` in downstream consumers plus a
   Keboola wrapper config (v0.4) that uploads the materialized table.
5. Replace `SELECT ... FROM "in"."c-bucket"."table"` with
   `SELECT ... FROM {{ ref('table') }}`.

## What an agent should *not* do without explicit permission

- Push to a remote git repo.
- Drop tables or delete DuckDB files.
- Change the contents of `connections:` entries.
- Write secrets to any tracked file.
- Skip tests when a run fails.

## Version this skill was written against

- Juncture `0.1.0a0`.
- For newer versions, re-read `docs/DESIGN.md` and `docs/ROADMAP.md`
  before acting; APIs may have evolved.

# Configuration reference

Juncture projects have three layers of configuration:

1. **`juncture.yaml`** — project metadata, connections, defaults.
2. **Environment variables** — interpolated into `juncture.yaml` via
   `${VAR}` or `${VAR:-default}`. `.env` file in the project root is
   auto-loaded via `python-dotenv`.
3. **Per-model `schema.yml`** — descriptions, data tests, materialization
   overrides.

## `juncture.yaml`

```yaml
name: my_project
version: 0.1.0
profile: local

default_materialization: table
default_schema: main

# Optional: run SQL files through Jinja before our own ref() parser.
jinja: false

# Project-wide variables, accessible in Python via ctx.vars() and in
# Jinja mode via {{ var('key') }}.
vars:
  run_date: "2026-04-17"
  min_amount: 100

connections:
  local:
    type: duckdb
    path: data/my_project.duckdb
    threads: 4
    extensions: [httpfs, spatial]        # optional DuckDB extensions

  production:
    type: snowflake                       # requires `pip install 'juncture[snowflake]'`
    account: "${SNOWFLAKE_ACCOUNT}"
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    database: ANALYTICS
    warehouse: COMPUTE_WH
    role: ANALYST
```

## Env var interpolation

```yaml
# ${VAR}              — required; fails at startup if unset
# ${VAR:-fallback}    — optional; uses fallback if unset
connections:
  local:
    type: duckdb
    path: ${JUNCTURE_DB_PATH:-data/local.duckdb}
```

## `.env` example

Put this into a file called `.env` next to your `juncture.yaml`. Don't
commit it — add it to `.gitignore`. Templates for each backend:

```env
# Snowflake
SNOWFLAKE_ACCOUNT=my-account.region.provider
SNOWFLAKE_USER=my_user
SNOWFLAKE_PASSWORD=change-me
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ANALYST

# BigQuery (v0.3)
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
BIGQUERY_PROJECT=my-gcp-project
BIGQUERY_DATASET=analytics

# Postgres (v0.3)
POSTGRES_URL=postgresql://user:password@localhost:5432/mydb

# OpenLineage (optional)
JUNCTURE_OPENLINEAGE_URL=http://marquez.local:5000/api/v1/lineage
```

## `schema.yml`

```yaml
models:
  - name: stg_orders
    description: Raw orders staged for downstream use.
    tags: [staging]
    materialization: table          # override default
    unique_key: order_id            # used by INCREMENTAL
    columns:
      - name: order_id
        description: Primary key.
        data_type: INTEGER
        tests: [not_null, unique]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: main.dim_customers
              field: customer_id
      - name: status
        tests:
          - not_null
          - accepted_values:
              values: [completed, refunded, pending]
```

## Selectors

`juncture run --select` / `--exclude` supports dbt-compatible selectors:

| Selector       | Meaning                                                 |
|----------------|---------------------------------------------------------|
| `model_name`   | Only that model.                                        |
| `+model_name`  | Model and all upstream ancestors.                       |
| `model_name+`  | Model and all downstream descendants.                   |
| `+model_name+` | Model, ancestors, descendants.                          |
| `tag:marts`    | Every model tagged `marts` in `schema.yml`.             |

Multiple selectors are combined (union).

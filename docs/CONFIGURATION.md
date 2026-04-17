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
    memory_limit: "16GB"                 # optional, forwarded to DuckDB
    temp_directory: "/tmp/juncture"      # optional, for out-of-core ops

  production:
    type: snowflake                       # requires `pip install 'juncture[snowflake]'`
    account: "${SNOWFLAKE_ACCOUNT}"
    user: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    database: ANALYTICS
    warehouse: COMPUTE_WH
    role: ANALYST
```

### DuckDB-specific keys

- `memory_limit`: forwarded to `SET memory_limit = ...`. Recommended
  when loading many parquet seeds at once.
- `temp_directory`: forwarded to `SET temp_directory = ...`. Used by
  DuckDB for out-of-core operations and spills.
- `extensions`: list of DuckDB extensions to `INSTALL` + `LOAD` at
  connect time (e.g. `httpfs`, `spatial`, `parquet`).
- `threads`: used by both DuckDB itself and Juncture's executor + seed
  loader (both cap their `ThreadPoolExecutor` at this value).

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

## Parallel EXECUTE materialization

Models with ``materialization: execute`` run their multi-statement body
as-is. By default every statement is sent to DuckDB one at a time,
sequentially. To parallelise the body set ``config.parallelism`` on the
model:

```yaml
# models/schema.yml
models:
  - name: slevomat_main_task
    materialization: execute
    config:
      parallelism: 4
```

What happens when parallelism > 1:

1. The body is parsed into an intra-script DAG via
   ``juncture.parsers.sqlglot_parser.build_statement_dag`` — every
   statement that writes a table becomes a producer, every statement
   that reads one wires an edge.
2. Juncture walks the graph layer by layer (``networkx.topological_
   generations``). Each layer's statements are submitted to a
   ``ThreadPoolExecutor`` of the given width; the next layer starts only
   when the previous one has fully finished.
3. Per-layer elapsed time is logged at INFO so you can see where
   parallelism actually pays off vs. where DuckDB's catalog lock or
   intra-query thread scheduler serialises things back.

Guidelines:

- **Start low**, e.g. ``parallelism: 2`` or ``4``. DuckDB already
  parallelises each individual query across all threads (``SET threads =
  N``); stacking more inter-query parallelism on top can contend rather
  than help.
- **Rule of thumb**: ``parallelism × connection.threads ≈ physical cores``.
  On a 4 vCPU box, ``parallelism: 4`` with ``threads: 2`` is usually a
  better starting point than ``parallelism: 8`` with ``threads: 4``.
- **Failure semantics**: any statement throwing an exception aborts the
  run — remaining submitted futures are cancelled and the error is
  re-raised with layer + statement index context. No automatic retry.
- **Invalid values fail fast**: ``parallelism: "four"`` or ``parallelism:
  0`` raises ``AdapterError`` at startup, never silently downgrades.
- ``parallelism: 1`` (or unset) takes the classic sequential branch
  unchanged.

See also [`scripts/analyze_execute.py`](../scripts/analyze_execute.py)
— offline preview of the layer histogram, widest-layer ceiling, and
top-10 fan-out producers for a given EXECUTE script. Run it before
tuning the ``parallelism`` value.

## Seeds

Seeds live under `seeds/` and are loaded once, in parallel, before the
model DAG runs. A model references them exactly like another model:
`{{ ref('my_seed') }}` or `$ref(my_seed)`.

### Layouts

```
seeds/customers.csv                  → materialized as TABLE customers
seeds/orders/part-*.parquet          → materialized as VIEW orders
seeds/in.c-db.carts/part-*.parquet   → VIEW "in.c-db.carts"  (dots preserved)
```

- CSV → one file, loaded with `read_csv_auto`, materialized as a table.
- Parquet → a directory of slices, loaded with `read_parquet` glob,
  materialized as a **VIEW** (no copy). This is what `kbagent storage
  unload-table --file-type parquet` produces and what the sync-pull
  migrator symlinks into `seeds/`.
- **Seed names may contain dots**, so migrated Snowflake identifiers
  (e.g. `in.c-db.carts`) survive unchanged. Seed discovery also follows
  symlinks.

### `seeds/schema.yml` (type overrides)

DuckDB's inferred parquet types are often correct, but Keboola Storage
parquet exports frequently hold everything as VARCHAR. Juncture does its
own hybrid type inference (full-scan under 1 M rows, sample above) and
caches results in `.juncture/type_cache.json`.

When inference guesses wrong, override it:

```yaml
# seeds/schema.yml
seeds:
  - name: in.c-db.carts
    columns:
      - name: cart_id
        data_type: BIGINT
      - name: created_at
        data_type: TIMESTAMP
      - name: raw_payload
        data_type: VARCHAR     # force-stay-varchar (no coercion attempt)
```

The cache is invalidated when the parquet mtimes change. To rebuild
eagerly, delete `.juncture/type_cache.json`.

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

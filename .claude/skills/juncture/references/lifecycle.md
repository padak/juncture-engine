# Project lifecycle

End-to-end walkthrough from "user has nothing" to "project running in CI".
Each section answers a specific question an agent will face when shepherding
a Juncture project.

## Table of contents

1. [Bootstrap a new project](#1-bootstrap-a-new-project)
2. [First model in 5 minutes](#2-first-model-in-5-minutes)
3. [Add seeds (CSV or parquet)](#3-add-seeds-csv-or-parquet)
4. [Add tests](#4-add-tests)
5. [Iteration loop](#5-iteration-loop)
6. [Add Python model alongside SQL](#6-add-python-model-alongside-sql)
7. [Promote to dev / staging / prod with profiles](#7-promote-to-dev--staging--prod-with-profiles)
8. [Run in CI](#8-run-in-ci)

---

## 1. Bootstrap a new project

```bash
juncture init my_shop --name my_shop
cd my_shop
```

Scaffolds:

```
my_shop/
├── juncture.yaml          # DuckDB connection, profile=local
├── models/staging/stg_users.sql
├── models/marts/user_count.sql
├── models/schema.yml      # not_null + unique on stg_users.id
├── tests/                 # empty
└── data/                  # local DuckDB lives here
```

The default `juncture.yaml` defines a single DuckDB connection at
`data/<name>.duckdb`. Run it immediately to verify the install:

```bash
juncture run --project . --test
```

Expected: 2 models materialised, all tests pass, exit code 0.

---

## 2. First model in 5 minutes

Drop a `.sql` file under `models/` (any subdirectory). Filename minus `.sql`
becomes the model name.

```sql
-- models/marts/daily_revenue.sql
SELECT
    CAST(order_ts AS DATE) AS day,
    COUNT(*)               AS orders,
    SUM(amount_eur)        AS revenue_eur
FROM {{ ref('stg_orders') }}
WHERE status = 'completed'
GROUP BY 1
```

Rules:

- `{{ ref('stg_orders') }}` is the **only** way to declare a dependency.
  Raw table names bypass the DAG and silently break it later.
- One `SELECT` per file. Don't write `CREATE TABLE` / `INSERT` — Juncture
  wraps the SELECT per materialization (`table` by default).
- Filename is the model name; `daily_revenue.sql` → table `daily_revenue`
  in the default schema (`main` for DuckDB).

Run just this subtree:

```bash
juncture run --project . --select +daily_revenue
duckdb data/my_shop.duckdb -c 'SELECT * FROM main.daily_revenue LIMIT 5'
```

`+daily_revenue` = the model + every upstream ancestor; `daily_revenue+`
= model + every downstream descendant; `+x+` = both.

---

## 3. Add seeds (CSV or parquet)

Drop files into `seeds/`. Reference them via `{{ ref('seed_name') }}` —
identical to model refs. Juncture loads them before the DAG runs.

```
seeds/
├── orders.csv             # → table `orders`
└── customers/             # parquet directory
    ├── 0001.parquet
    └── 0002.parquet       # → VIEW `customers` over read_parquet('seeds/customers/*.parquet')
```

- **CSV**: plain text, header row = column names. Type inference is
  DuckDB's default (often everything VARCHAR for Keboola exports).
- **Parquet directory**: directory of sliced `.parquet` files becomes a
  VIEW (not a TABLE — saves time and memory on big seeds).
- **Symlinks are followed** — `kbagent sync pull` migrations symlink
  parquet pools shared across projects.
- **Dotted seed names survive** (`in.c-db.carts`) — don't sanitise.
- **Type inference** is hybrid: full-scan for parquet < 1M rows, sampled
  above. Cached in `.juncture/seed_schemas.json`.

Per-column type overrides via `seeds/schema.yml`:

```yaml
seeds:
  - name: orders
    columns:
      - {name: amount_eur, type: DECIMAL(18,4)}
      - {name: order_ts,   type: TIMESTAMP}
```

---

## 4. Add tests

Two flavors: **schema tests** (declarative, in `schema.yml`) and **custom
tests** (raw SQL files under `tests/`).

### Schema tests

```yaml
# models/schema.yml
version: 2
models:
  - name: stg_orders
    description: Raw orders staged for downstream use.
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: customer_id
        tests:
          - not_null
          - relationships: { to: stg_customers, field: id }
      - name: status
        tests:
          - accepted_values:
              values: [completed, refunded, pending]
```

Built-ins: `not_null`, `unique`, `relationships`, `accepted_values`. Each
compiles to a single `SELECT COUNT(*)` and passes when the count is 0.

### Custom SQL tests

Any `.sql` file under `tests/` that returns failing rows. Pass when 0 rows.

```sql
-- tests/no_negative_revenue.sql
SELECT day, revenue_eur
FROM {{ ref('daily_revenue') }}
WHERE revenue_eur < 0
```

### Running tests

```bash
juncture run  --project . --test    # models + tests in one go
juncture test --project .           # tests only (assumes models materialised)
```

Failing tests do **not** abort the run — they're reported in the run
report with `failing_rows`. The exit code reflects pass/fail for CI.

---

## 5. Iteration loop

Tight loop for a single model under heavy iteration:

```bash
# 1. Fast structural check (no DB opened)
juncture compile --project . --json | jq '.models[] | select(.name=="X")'

# 2. Plan: see layers + intra-EXECUTE without running
juncture run --project . --select +X --dry-run

# 3. Run only this subtree, with seeds reused
juncture run --project . --select +X+ --reuse-seeds --test

# 4. Inspect the materialised result
duckdb data/<project>.duckdb -c 'SELECT * FROM main.X LIMIT 20'

# 5. Open web UI for DAG + history (separate terminal)
juncture web --project .
# → http://127.0.0.1:8765
```

`--reuse-seeds` is the biggest dev-loop accelerator on projects with
parquet seeds (skips re-loading + type inference).

`--disable model_a,model_b` and `--enable-only X,Y,Z` let you toggle
subsets without editing `schema.yml`.

---

## 6. Add Python model alongside SQL

Drop a `.py` file under `models/`. Decorate one function per file with
`@transform`. The function is the model.

```python
# models/marts/cohort_retention.py
from juncture import transform


@transform(depends_on=["orders", "customers"])
def cohort_retention(ctx):
    import pandas as pd

    orders = ctx.ref("orders").to_pandas()
    orders["order_month"] = pd.to_datetime(orders["order_ts"]).dt.to_period("M").astype(str)
    cohort = orders.groupby("customer_id")["order_month"].min().rename("cohort_month")
    joined = orders.join(cohort, on="customer_id")
    return (
        joined.groupby(["cohort_month", "order_month"])["customer_id"]
        .nunique()
        .reset_index(name="active_customers")
    )
```

Notes:

- `ctx.ref("orders")` returns a `pyarrow.Table`. Convert to pandas / polars
  / pylist as needed.
- `ctx.vars("key", default)` reads the merged vars (juncture.yaml +
  `--var` CLI override).
- Returned DataFrame becomes a DuckDB table named after the function
  (overridable via `@transform(name="x")`).
- One DAG: a Python model can depend on SQL upstreams and vice versa.
- `juncture run` discovers Python models automatically — no separate
  command.

---

## 7. Promote to dev / staging / prod with profiles

Add a `profiles:` block to `juncture.yaml`. One project = many environments.

```yaml
profile: dev                          # default when --profile not given

connections:
  warehouse:
    type: duckdb
    path: data/base.duckdb

profiles:
  dev:
    default_schema: dev_petr
    connections:
      warehouse:
        path: data/dev.duckdb
    vars: { lookback_days: 7 }
  prod:
    default_schema: analytics
    connections:
      warehouse:
        type: snowflake
        account: "${SNOW_ACCOUNT}"
        database: PROD_DB
```

```bash
juncture run --project . --profile prod
JUNCTURE_PROFILE=staging juncture run --project .
```

Precedence: `--profile` > `JUNCTURE_PROFILE` > top-level `profile:`.
Full merge semantics: [`yaml-schema.md`](yaml-schema.md) §Profiles.

---

## 8. Run in CI

Minimal GitHub Actions step:

```yaml
- name: Run Juncture pipeline
  run: |
    juncture compile --project . --json > /dev/null   # fail fast on DAG errors
    juncture run     --project . --test --threads 4
  env:
    JUNCTURE_PROFILE: ci
    SNOW_ACCOUNT: ${{ secrets.SNOW_ACCOUNT }}
```

Tips:

- Compile first; it parses every `ref()` without opening the DB and exits
  fast on broken DAG.
- For CI you usually want `RunRequest.record_history=False` so
  `target/run_history.jsonl` doesn't pollute artifacts. CLI doesn't expose
  this yet — call the runner from Python if you need it.
- The `ci` profile typically points at an ephemeral schema (e.g.
  `default_schema: "ci_${GITHUB_RUN_ID}"`).
- Tests fail the build via the non-zero exit code; no extra config needed.

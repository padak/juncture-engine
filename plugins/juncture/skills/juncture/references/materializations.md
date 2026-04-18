# Materializations

Five kinds of materialization. Pick the right one and the rest of the
project gets simpler. Defined in `juncture.core.model.Materialization`:
`table`, `view`, `incremental`, `ephemeral`, `execute`.

## Table of contents

1. [Decision tree](#1-decision-tree)
2. [`table` (default)](#2-table-default)
3. [`view`](#3-view)
4. [`incremental`](#4-incremental)
5. [`ephemeral`](#5-ephemeral)
6. [`execute`](#6-execute)
7. [Setting per-model materialization](#7-setting-per-model-materialization)

---

## 1. Decision tree

```
Is this a multi-statement Snowflake script migrated as-is?
│   YES → execute
│   NO ↓
│
Does the model produce rows you'll query repeatedly?
│   NO  → view (lightweight, always-fresh; no storage)
│   YES ↓
│
Does it append/update by a key, with most rows unchanged between runs?
│   YES → incremental (with config.unique_key)
│   NO ↓
│
Is it a small, reusable shape consumed by multiple downstream marts?
│   YES → ephemeral (compiled into downstream as a CTE; no persisted table)
│   NO ↓
│
Default → table (CREATE OR REPLACE TABLE AS SELECT …)
```

---

## 2. `table` (default)

```yaml
- name: daily_revenue
  materialization: table        # implicit if omitted
```

Wraps the `SELECT` as `CREATE OR REPLACE TABLE main.daily_revenue AS
SELECT …`. Full rebuild every run. Storage cost = result size.

When to pick:

- Default for marts and aggregates.
- Anything you'll query frequently from BI / notebooks / Python models.
- Anything where a stale snapshot would mislead downstream.

When to avoid:

- Result is huge and rebuilding is expensive (use `incremental`).
- Result is small and downstream is one model (use `ephemeral`).

---

## 3. `view`

```yaml
- name: stg_active_orders
  materialization: view
```

`CREATE OR REPLACE VIEW main.stg_active_orders AS SELECT …`. No
materialised data; the SELECT runs every time downstream queries it.

When to pick:

- Trivial filtering / renaming on a single source table.
- "Logical view" that must always reflect the source (e.g. SCD2 current).
- Compute is cheap and the upstream changes often.

When to avoid:

- Multiple downstream models read it (each pays the compute).
- The SELECT joins large tables.

---

## 4. `incremental`

```yaml
- name: fct_events
  materialization: incremental
  config:
    unique_key: event_id          # required
    lookback_days: 3              # optional — how far back to re-process
```

Runs `INSERT ... ON CONFLICT(unique_key) DO UPDATE SET ...` (DuckDB) or
`MERGE INTO ... USING ... ON unique_key` (Snowflake). Only newly-arrived
rows + the lookback window get processed.

State tracking:

- `_juncture_state` table (in `juncture.core.state`) records the last
  watermark per model.
- Schema:
  ```
  _juncture_state(
      model_name VARCHAR PRIMARY KEY,
      last_run_at TIMESTAMP,
      last_max_value VARCHAR
  )
  ```
- A `--full-refresh` CLI flag rebuilds from scratch (drops the row in
  `_juncture_state` and re-runs as a `table`).

Detect "incremental mode" inside the model body via Jinja:

```sql
SELECT * FROM {{ ref('source_events') }}
{% if is_incremental() %}
WHERE event_ts >= (SELECT MAX(event_ts) FROM {{ this }})
                  - INTERVAL '{{ var("lookback_days", 3) }} days'
{% endif %}
```

`is_incremental()` is true when the table already exists AND
`--full-refresh` was not passed.

When to pick:

- Append-only or slowly-changing facts (events, orders, page views).
- Source table is large and most rows don't change between runs.
- You have a stable `unique_key` and a monotonic timestamp / sequence.

When to avoid:

- No reliable unique key — use `table` with a full refresh schedule.
- Every row changes every run — incremental is more expensive than
  rebuild.
- Source has hard deletes — incremental won't reflect them; use a
  full-refresh cron alongside.

---

## 5. `ephemeral`

```yaml
- name: customer_tier
  materialization: ephemeral
```

The model is **never persisted**. Instead, when a downstream model
references it via `{{ ref('customer_tier') }}`, Juncture inlines the
SELECT as a CTE in that downstream's compiled SQL.

```sql
-- models/marts/customer_summary.sql
SELECT c.id, t.tier
FROM {{ ref('customers') }} c
LEFT JOIN {{ ref('customer_tier') }} t USING (id)

-- compiles to:
WITH __ephemeral_customer_tier AS (
  <body of customer_tier.sql>
)
SELECT c.id, t.tier
FROM customers c
LEFT JOIN __ephemeral_customer_tier t USING (id)
```

When to pick:

- A reusable shape (with columns) consumed by multiple downstream marts.
- You want "define a derived dimension once, use everywhere" without
  paying storage cost.
- Test the result via downstream — ephemeral models can't be tested in
  isolation (no table to query).

When to avoid:

- Multiple downstream models read it AND the body is expensive — each
  inlines a copy, paying compute multiple times. Use `view` or `table`.
- You want to run schema tests directly on it.
- Body is more than a couple hundred lines — debugging the inlined CTE
  in downstream becomes painful.

---

## 6. `execute`

```yaml
- name: legacy_etl_block
  materialization: execute
  config:
    parallelism: 4              # intra-script ThreadPool size
    continue_on_error: false    # CLI --continue-on-error overrides
```

Runs the SQL body **as-is**, no `CREATE OR REPLACE` wrapping. The body
can be many statements separated by `;`. Each statement runs in
dependency order (Juncture builds an intra-script DAG via
`juncture.parsers.sqlglot_parser.build_statement_dag`).

When to pick:

- Migrated Keboola Snowflake transformations — preserves the original
  multi-statement script verbatim. `juncture migrate sync-pull` produces
  EXECUTE models exclusively.
- Stored-procedure-like bodies that mix DDL (`CREATE TABLE`), DML
  (`INSERT`, `UPDATE`), and DCL.
- Scripts where statement order matters AND can't easily be split.

When to avoid:

- New code: write proper `table` / `view` / `incremental` models with
  one SELECT each. EXECUTE is a migration affordance, not a target
  pattern.
- You want column-level lineage (the parser handles intra-script DAG
  but doesn't trace columns through DDL).

### Intra-script parallelism

Set `config.parallelism: N` to run independent statements in N threads.
Default `N=1` (sequential, back-compat). The CLI flag
`juncture run --parallelism 4` (`-P 4`) overrides per-model config —
useful for benchmarking.

### Continue-on-error

`juncture run --continue-on-error` (or `config.continue_on_error: true`)
keeps running after a failed statement and collects all errors into the
`RunReport`. Pair with `juncture debug diagnostics` for migration
triage. See [`migration.md` §4](migration.md#4-repair-loop).

### Splitting an EXECUTE monolith

When you've finished migrating and want to refactor an EXECUTE body
into proper models:

```bash
juncture sql split <script.sql> --out ./models/split --source-dialect duckdb
```

Rewrites every CTAS statement (`CREATE TABLE x AS SELECT …`) as one
`.sql` model with auto-inferred `{{ ref(...) }}`. Non-CTAS statements
collect into a residual EXECUTE model with auto-inferred `depends_on`.

---

## 7. Setting per-model materialization

Three places, in priority:

1. **`config()` macro in the SQL body** (Jinja mode):
   ```sql
   {{ config(materialized='incremental', unique_key='id') }}
   SELECT ...
   ```
2. **`schema.yml`** (recommended):
   ```yaml
   - name: fct_events
     materialization: incremental
     config: {unique_key: event_id}
   ```
3. **`juncture.yaml default_materialization`** — fallback for models
   that don't override it.

For Python models, only `@transform(materialization=Materialization.X)`
in the decorator works (no `schema.yml` config block for Python yet).

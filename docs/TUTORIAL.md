# Tutorial: from zero to a shared-code, parametrized pipeline

*A four-step onboarding narrative. Each step adds one cognitive idea
on top of the previous one. The final state lives under
[`examples/tutorial_shop/`](../examples/tutorial_shop/) — copy it
side-by-side as you read, or build your own from scratch along with
the tutorial.*

Dependencies: Python 3.11+, a checkout of Juncture, and `make install`
(or `pip install -e '.[dev,pandas]'`). No warehouse credentials, no
Docker. The whole thing runs on your laptop against DuckDB.

## The use case

You run a small online shop. You have two CSV files:

- `orders.csv` — `order_id, customer_id, order_ts, amount_eur, status`
- `customers.csv` — `customer_id, email, country, signed_up_at`

You want:

1. A daily revenue view you can slice by any time window.
2. A retention view (which cohort came back in which month).
3. A consistent "VIP / regular / new" tier applied everywhere.
4. A CLI knob to re-run any of the above for a different date window
   without editing SQL.

We will build it in four levels.

## Level 1 — from zero to a first SELECT

**Goal:** `pip install juncture`, point it at your two CSVs, compute
daily revenue.

### Scaffold

```bash
juncture init my_shop
cd my_shop
```

That creates a project skeleton:

```
my_shop/
├── juncture.yaml            # project config
├── models/                  # your SQL + Python transformations
└── seeds/                   # CSVs / parquet loaded before the DAG runs
```

### Drop your CSVs into `seeds/`

```bash
cp ~/Downloads/orders.csv    seeds/
cp ~/Downloads/customers.csv seeds/
```

Juncture will auto-load every `.csv` under `seeds/` as a source table.
The seed name is the filename without the extension: `orders.csv` →
table `orders`. Downstream models reference it via `{{ ref('orders') }}`.

### Write your first model

`models/daily_revenue.sql`:

```sql
SELECT
    CAST(order_ts AS DATE) AS day,
    COUNT(*)               AS orders,
    SUM(amount_eur)        AS revenue_eur
FROM {{ ref('orders') }}
WHERE status = 'completed'
GROUP BY 1
ORDER BY 1
```

### Run it

```bash
juncture run --project .
```

You will see a table of results: the two seeds loaded, then
`daily_revenue` computed and persisted in the local DuckDB file
(`data/my_shop.duckdb` by default).

### What you got

- A project you can `git init` and version.
- A SQL model that declares exactly what it reads (the `ref()` macro
  builds a dependency edge in the DAG).
- A local database holding the result.

This is already more than the four legacy Keboola components would
give you on their own.

---

## Level 2 — Python alongside SQL

**Goal:** add a cohort-retention matrix. Cohort pivots are painful in
SQL; pandas does them in three lines. We want Python and SQL in the
**same** DAG so one `juncture run` builds everything.

### Add a Python model

`models/cohort_retention.py`:

```python
from juncture import transform


@transform(depends_on=["orders", "customers"])
def cohort_retention(ctx):
    import pandas as pd

    orders = ctx.ref("orders").to_pandas()

    orders["order_ts"]    = pd.to_datetime(orders["order_ts"])
    orders["order_month"] = orders["order_ts"].dt.to_period("M").astype(str)
    first_order           = orders.groupby("customer_id")["order_ts"].min().reset_index()
    first_order["cohort_month"] = first_order["order_ts"].dt.to_period("M").astype(str)

    joined = orders.merge(first_order[["customer_id", "cohort_month"]], on="customer_id")
    grid = (
        joined.groupby(["cohort_month", "order_month"])["customer_id"]
        .nunique()
        .reset_index(name="active_customers")
    )
    cohort_size = (
        first_order.groupby("cohort_month")["customer_id"].nunique().reset_index(name="cohort_size")
    )
    grid = grid.merge(cohort_size, on="cohort_month")
    grid["retention_pct"] = (grid["active_customers"] / grid["cohort_size"] * 100).round(1)
    return grid
```

### What to notice

- `@transform(depends_on=[...])` registers the function as a DAG node.
- `ctx.ref("orders")` returns the **same** seed the SQL model reads.
  A Python model can depend on SQL and vice versa — they share one DAG.
- The returned DataFrame becomes a DuckDB table named `cohort_retention`
  (filename `cohort_retention.py` → model name `cohort_retention`).
- Running is still just `juncture run` — the executor discovers Python
  models automatically, no separate command.

```bash
juncture run --project .
```

Now the run output lists both `daily_revenue` and `cohort_retention`.
You can `juncture web --project .` to see them side-by-side in the DAG.

---

## Level 3 — shared code with macros and an ephemeral dimension

**Goal:** the "VIP" rule and a date format are creeping into every new
model. We want to define each **once** and change it in one place.

Juncture gives you two idioms. They solve different problems; you
will use both.

### Macros — for reusable expressions

A macro is a piece of SQL text you call by name. Think of it as a
function whose body is SQL. Good for formatters, predicates, CASE
branches.

First, flip on Jinja mode in `juncture.yaml`:

```yaml
jinja: true
```

Then drop reusable snippets under `macros/`:

`macros/dates.sql`:

```sql
{% macro my_date(col) -%}
  strftime({{ col }}, '%Y-%m-%d')
{%- endmacro %}
```

`macros/tiers.sql`:

```sql
{% macro is_vip(amount_col) -%}
  ({{ amount_col }} >= {{ var('vip_threshold_eur', 500) }})
{%- endmacro %}
```

Every `{% macro %}` under `macros/` is auto-loaded when the project
starts. No `{% import %}` in your models — just call by name:

```sql
-- models/daily_revenue.sql
SELECT
    {{ my_date('order_ts') }} AS day,
    COUNT(*)                  AS orders,
    SUM(amount_eur)           AS revenue_eur,
    SUM(CASE WHEN {{ is_vip('amount_eur') }} THEN amount_eur ELSE 0 END) AS vip_revenue_eur
FROM {{ ref('orders') }}
WHERE status = 'completed'
GROUP BY 1
```

Change `my_date` from `'%Y-%m-%d'` to `'%Y-%m-%d %H:%M:%S'` in one
file and every mart that used it follows. That is exactly the
"define once, change once" win from the [VISION](VISION.md)
§Problem 2.

### Ephemeral models — for reusable dimensions with columns

Macros are plain string substitution. If your shared concept needs
*columns* (per-customer LTV + tier, per-product margin band), make it
a proper DAG node with `materialization: ephemeral`.

`models/shared/customer_tier.sql`:

```sql
SELECT
    customer_id,
    SUM(amount_eur) AS lifetime_value_eur,
    CASE
        WHEN SUM(amount_eur) >= {{ var('vip_threshold_eur', 500) }} THEN 'vip'
        WHEN SUM(amount_eur) >= 200                                 THEN 'regular'
        ELSE 'new'
    END AS tier
FROM {{ ref('orders') }}
WHERE status = 'completed'
GROUP BY customer_id
```

`models/schema.yml`:

```yaml
version: 2
models:
  - name: customer_tier
    materialization: ephemeral
    columns:
      - name: tier
        tests:
          - accepted_values:
              values: [vip, regular, new]
```

Now any downstream mart can `ref('customer_tier')` and SELECT from
its columns:

```sql
-- models/marts/customer_summary.sql
SELECT
    c.customer_id, c.email, c.country,
    COALESCE(t.lifetime_value_eur, 0) AS lifetime_value_eur,
    COALESCE(t.tier, 'new')           AS tier
FROM {{ ref('customers') }} AS c
LEFT JOIN {{ ref('customer_tier') }} AS t USING (customer_id)
```

### When to use which

| Need | Idiom |
|---|---|
| A formatted column, a WHERE clause, a CASE expression | **Macro** (`macros/*.sql`) |
| A dimension with rows + columns consumed by multiple marts | **Ephemeral model** |
| A vectorised transformation that pandas does in three lines | **Python model** (see Level 2) |

Macros are for the small parts; ephemeral models are for the shared
shapes; Python models are for the algorithm-ish stuff. You can
combine all three in the same DAG.

---

## Level 4 — external parameters from the CLI

**Goal:** stop editing SQL when you want "last 30 days" or
"as-of 2026-02-01". Pass it from the outside.

Juncture has three parameter mechanisms, layered:

1. `juncture.yaml → vars:` block — project defaults.
2. `--var key=value` on the `juncture run` command line — per-run override.
3. `${VAR}` / `${VAR:-default}` in `juncture.yaml` — environment
   interpolation. Intended for infra (DB paths, credentials), not
   business parameters.

The precedence is `--var` > `juncture.yaml vars:` > nothing. A model
sees them **identically** whether it's SQL or Python:

```sql
-- SQL (needs jinja: true)
WHERE order_ts >= CAST('{{ var("as_of") }}' AS DATE)
                  - INTERVAL '{{ var("lookback_days") }} days'
```

```python
# Python
as_of    = pd.to_datetime(ctx.vars("as_of", "2026-03-31"))
lookback = int(ctx.vars("lookback_days", 90))
```

### Declare defaults

```yaml
# juncture.yaml
jinja: true
vars:
  as_of: "2026-03-31"
  lookback_days: 90
  vip_threshold_eur: 500
```

### Override at runtime

```bash
# default window (Jan 1 → Mar 31)
juncture run --project .

# last 30 days ending 2026-02-01
juncture run --project . \
  --var as_of=2026-02-01 \
  --var lookback_days=30

# stricter VIP bar for a one-off analysis
juncture run --project . --var vip_threshold_eur=1000
```

No SQL changes. `is_vip()` (the macro), `customer_tier` (the ephemeral
model), `daily_revenue` (the mart), and `cohort_retention` (the Python
model) all read the same override at once.

### Where the `vars:` live

- On the command line `--var key=value` wins.
- Otherwise, `juncture.yaml vars:` block wins.
- Otherwise the Jinja `var('key', default)` default is used.
- If no default and the key is missing everywhere, Jinja fails fast
  with `StrictUndefined` — no silent defaults (matches the Juncture
  contract).

---

## What you've built

By the end of Level 4 you have a pipeline that:

- Starts from two CSVs (`seeds/orders.csv`, `seeds/customers.csv`).
- Runs one staging SQL model + one ephemeral dimension + two marts +
  one Python cohort matrix in a single DAG.
- Defines date formatting once (macro) and a VIP rule once
  (macro + ephemeral) — change in one place flips everywhere.
- Takes runtime parameters from the CLI for windowing, threshold,
  and as-of dates — no SQL edits.
- Exposes all of it through `juncture web` (DAG visualiser + source
  browser + portfolio + reliability dashboard).

That is the baseline every Juncture project should reach. More
advanced ingredients — incremental materializations (`materialization:
incremental`), the `_juncture_state` checkpoint, SQL dialect
translation for a production Snowflake/BigQuery target, governance
fields (owner / SLA / PII) — sit on top and are covered in
[`CONFIGURATION.md`](CONFIGURATION.md) and
[`DESIGN.md`](DESIGN.md).

## Reference project

The complete L4 project lives at
[`examples/tutorial_shop/`](../examples/tutorial_shop/). Run it:

```bash
juncture run     --project examples/tutorial_shop --test
juncture web     --project examples/tutorial_shop

# try the override
juncture run     --project examples/tutorial_shop \
  --var as_of=2026-01-20 --var lookback_days=7
```

## Further reading

- [`VISION.md`](VISION.md) — why this engine exists.
- [`CONFIGURATION.md`](CONFIGURATION.md) — full `juncture.yaml`
  reference including seed layouts and parallel EXECUTE.
- [`DESIGN.md`](DESIGN.md) — architecture (Project, DAG, Adapter,
  Executor).

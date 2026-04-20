# Tutorial: from zero to a shared-code, parametrized pipeline

*A four-step onboarding narrative. Each step adds one cognitive idea
on top of the previous one. The final state lives under
[`examples/tutorial_shop/`](../examples/tutorial_shop/) — copy it
side-by-side as you read, or build your own from scratch along with
the tutorial.*

Dependencies: Python 3.11+ and one of the following. No warehouse
credentials, no Docker. The whole thing runs on your laptop against
DuckDB.

```bash
# Option 1 -- just want the CLI on your PATH (recommended for this tutorial)
uv tool install --with pandas --with pyarrow git+https://github.com/padak/juncture-engine

# Option 2 -- hacking on Juncture itself, from a repo checkout
git clone https://github.com/padak/juncture-engine.git && cd juncture-engine
make install     # creates .venv and installs '-e .[dev,pandas]'
```

`--with pandas --with pyarrow` is needed because Level 2 adds a Python
model that imports pandas and Juncture materializes `ctx.ref("orders")`
as an Arrow table (pyarrow); the SQL-only Levels 1, 3, 4 run without
them. If you skip them now and hit `ModuleNotFoundError: No module
named 'pandas'` or `'pyarrow'`, jump to *Managing the Juncture
environment* below.

Juncture is not on PyPI yet, so plain `pip install juncture` does not
work — use one of the two recipes above.

### Managing the Juncture environment

`uv tool install` creates an **isolated** environment under
`~/.local/share/uv/tools/juncture/`. A system `pip install pandas` or
`brew install …` will not reach it, so any extra package your Python models
import must be added to that isolated env explicitly.

```bash
# Add (or change) the list of extra packages. --reinstall re-runs install
# with the full --with set, so list every extra you want kept:
uv tool install --with pandas --with pyarrow --reinstall \
  git+https://github.com/padak/juncture-engine

# Example: also pull scikit-learn for a model that uses it
uv tool install --with pandas --with pyarrow --with scikit-learn --reinstall \
  git+https://github.com/padak/juncture-engine

# Update Juncture itself to the latest commit on main (keeps the --with list)
uv tool upgrade juncture

# Inspect / remove
uv tool list
uv tool uninstall juncture
```

If you went with *Option 2* (repo checkout, `make install`), there is no
tool env — install packages straight into the repo's `.venv`:

```bash
.venv/bin/pip install scikit-learn
```

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

**Goal:** install the CLI (see *Dependencies* above), point it at your
two CSVs, compute daily revenue.

### Scaffold

```bash
juncture init my_shop
cd my_shop
```

That creates a **minimal** project skeleton — your job is to fill it in:

```
my_shop/
├── juncture.yaml            # project config (DuckDB conn, name, profile, jinja flag)
├── README.md                # generated; short "next steps" reminder
├── macros/                  # empty: Jinja macros go here (flip jinja: true to use)
├── models/                  # empty: SQL + Python transformations go here
└── seeds/                   # empty: drop CSV / parquet inputs here
```

The generated `juncture.yaml` ships with `jinja: false` as an explicit
placeholder — you'll flip it to `true` in Level 3. Everything else (name,
DuckDB path, default materialization) is filled in based on your
directory name.

> **Want a runnable demo instead?** Pass `--with-examples` to `juncture init`
> and you'll get two ready-made models (`stg_users`, `user_count`) plus a
> `schema.yml` with data tests. The tutorial below assumes the default
> (empty) scaffold — if you used `--with-examples`, delete
> `models/staging/`, `models/marts/` and `models/schema.yml` first.

### Drop your CSVs into `seeds/`

```bash
cp ~/Downloads/orders.csv    seeds/
cp ~/Downloads/customers.csv seeds/
```

Juncture will auto-load every `.csv` under `seeds/` as a source table.
The seed name is the filename without the extension: `orders.csv` →
table `orders`. Downstream models reference it via `{{ ref('orders') }}`.

### Don't have any CSVs? Generate them.

The Juncture repo ships a zero-dependency generator tailored for this
tutorial. Clone the repo once (separately from any `uv tool install`),
then run the generator from inside **your** project directory:

```bash
# One-off clone so you have the generator on disk:
git clone https://github.com/padak/juncture-engine.git ~/src/juncture-engine

cd my_shop      # <-- stay inside your project; the default --output-dir is ./seeds
python ~/src/juncture-engine/examples/tutorial_shop/scripts/generate_data.py
#   -> writes seeds/customers.csv (50 rows) + seeds/orders.csv (200 rows)

# Larger volumes or a fully explicit path:
python ~/src/juncture-engine/examples/tutorial_shop/scripts/generate_data.py --scale medium
python ~/src/juncture-engine/examples/tutorial_shop/scripts/generate_data.py \
    --output-dir /tmp/seeds
```

Writes `customers.csv` and `orders.csv` with the same schema the tutorial
assumes. Deterministic from `--seed 42`; re-running produces the same
bytes, so commits stay stable. Scales: `tiny` (8 / 20), `small` (50 / 200,
default), `medium` (500 / 2 000).

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

### Under the hood: what DuckDB actually does

Juncture opened `data/my_shop.duckdb`, loaded each CSV seed into a
DuckDB **table** via `read_csv_auto()`, then ran your SQL model as a
`CREATE OR REPLACE TABLE daily_revenue AS …`. You can poke around with
the DuckDB CLI:

```bash
duckdb data/my_shop.duckdb -c '.tables'
# customers  daily_revenue  orders

duckdb data/my_shop.duckdb -c 'DESCRIBE orders'
# column_name   column_type
# order_id      INTEGER
# customer_id   INTEGER
# order_ts      TIMESTAMP
# amount_eur    DOUBLE
# status        VARCHAR

duckdb data/my_shop.duckdb -c 'SELECT * FROM daily_revenue LIMIT 3'
```

Two rules worth internalising now:

- **CSV seeds are `CREATE TABLE AS SELECT … read_csv_auto(…)`** — the
  data is copied into DuckDB. Fine for megabytes.
- **Parquet seeds are views** (`CREATE VIEW … read_parquet(…)`) — DuckDB
  reads from disk on demand, no copy. Matters for multi-GB datasets.

Model outputs default to `table` materialization, same `CREATE OR
REPLACE TABLE` pattern. Later levels introduce `view`, `ephemeral`, and
`incremental`.

### What you got

- A project you can `git init` and version.
- A SQL model that declares exactly what it reads (the `ref()` macro
  builds a dependency edge in the DAG).
- A local database holding the result.

This is already more than the four legacy Keboola components would
give you on their own.

### With Claude Code

If you have Claude Code with the `juncture` skill enabled (see
`skills/juncture/SKILL.md` in this repo or install it as a plugin), the
whole of Level 1 reduces to one prompt:

> *"Scaffold a Juncture project called `my_shop`, drop the two CSVs from
> `~/Downloads/` into `seeds/`, and write a `daily_revenue.sql` model
> that groups `orders` by day with order count and revenue sum."*

The skill knows the project layout, runs `juncture compile` / `run` for
you, and reports row counts and any data-test failures back. If you
don't have the CSVs, add *"generate small-scale seeds first"* and the
agent will invoke the generator from the previous section.

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

### With Claude Code

> *"Add a Python model `cohort_retention` that reads `orders` via
> `ctx.ref()` and returns a cohort-month × order-month DataFrame with
> retention percentages. Register it with `depends_on=['orders',
> 'customers']`."*

The skill writes the `@transform` function, adds the dependency
declaration, and re-runs the DAG so you see the SQL and Python models
materialize in the same execution.

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

First, flip on Jinja mode in `juncture.yaml`. Your init-generated file
already has the key sitting there as a `false` placeholder — it's a
top-level entry, same indentation level as `name` / `profile` /
`connections`. Change it to `true`:

```yaml
# juncture.yaml (relevant lines only)
name: my_shop
version: 0.1.0
profile: local

jinja: true                   # <-- was `jinja: false`; flip it

default_materialization: table
default_schema: main

connections:
  local:
    type: duckdb
    path: data/my_shop.duckdb
    threads: 4
```

Then drop reusable snippets under `macros/` (the directory already
exists — `juncture init` created it for you):

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

### With Claude Code

> *"Extract the VIP threshold into a macro `is_vip(amount_col)` under
> `macros/`, turn `customer_tier` into an ephemeral model with an
> `accepted_values` test on the `tier` column, and rewrite
> `customer_summary` to join on it."*

The skill enables `jinja: true` in `juncture.yaml`, creates the macro,
adds the `schema.yml` entry for ephemeral materialization, and verifies
the DAG still resolves by running `juncture compile --json`.

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

### With Claude Code

> *"Add `as_of` and `lookback_days` to `juncture.yaml vars:`, read them
> from `daily_revenue.sql` with `{{ var(...) }}` and from
> `cohort_retention.py` with `ctx.vars(...)`. Then run the project with
> `--var as_of=2026-01-20 --var lookback_days=7` and show me how the
> output changed."*

The skill wires the Jinja and Python sides consistently, runs both the
default build and the overridden one, and diffs the row counts so you
see the window change immediately.

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

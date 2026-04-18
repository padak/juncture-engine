# Example: `eu_ecommerce` — Fjord & Fable

A full-scope Juncture demo built around a fictional four-country EU
retailer, "Fjord & Fable". The point of the project is not to sell
hygge-themed bedding — the point is that a single Juncture project can
demonstrate every Phase-1 VISION problem in one DAG, with real volume,
seeds, SQL + Python mixing, tests, and parametrization.

## Quick start

```bash
# 1. From the repo root, generate synthetic seeds (5 000 customers / 20 000 orders)
.venv/bin/python examples/eu_ecommerce/scripts/generate_data.py --scale small

# 2. Run the whole project with data tests
.venv/bin/juncture run --project examples/eu_ecommerce --threads 4 --test
```

Default scale (`medium`) produces 50 000 customers and 200 000 orders.
`full` produces 500 000 customers and 3 000 000 orders — useful for
stress-testing the executor and measuring BENCHMARKS-style overhead.

## Mapping to VISION.md

The ten problems [`docs/VISION.md`](../../docs/VISION.md) pins down, and
how this project shows a solution:

| VISION # | Problem | Demonstrated here by |
|----------|---------|----------------------|
| 1 | Code locked in the platform, not git | The whole project is a directory of `.sql` + `.py` + `.yml` — copy it, `git diff` it, `grep` it. |
| 2 | No macros / shared blocks | `int_active_customer` is an ephemeral model. Every mart referencing "active customer" inlines the same SQL; one definition, many readers. |
| 3 | Weak parametrization | `juncture.yaml` declares `active_customer_days`, `vip_threshold_orders`, `vip_threshold_spend_eur`, `reporting_end_date`. Staging, intermediate and mart SQL all consume them through `{{ var('…') }}`. |
| 4 | No DAG visibility | `juncture compile --project examples/eu_ecommerce --json` emits a full node/edge manifest; `int_order_facts` has five explicit upstreams visible from one grep. |
| 5 | No parallelism | `juncture run --threads 4` schedules independent models per layer concurrently. The run log prints `Layer N: running K model(s) with 4 thread(s)`. |
| 6 | No conditional execution | `schema.yml` carries `schedule_cron` metadata (`daily_revenue: "0 2 * * *"`, `campaign_performance: "0 6 * * 2"`, `rfm_scores: "0 3 * * 1"`). Phase 1 exposes this to orchestrators; Juncture itself is not a scheduler. |
| 7 | No side-by-side versioning | **Deferred to Phase 4** (virtual data environments). The project doesn't try to demo A/B yet. |
| 8 | Python and SQL cannot mix in one flow | `rfm_scores` (Python) depends on `int_rfm_inputs` (SQL). `customer_ltv` (Python) depends on BOTH `int_order_facts` (SQL) and `rfm_scores` (Python) — a three-node chain crossing the SQL/Python boundary twice. |
| 9 | No lineage | `compile --json` emits `depends_on` per node. SQLGlot-powered ref extraction is the same code the engine uses for `$ref()` resolution, so what you see is what actually runs. |
| 10 | Single-backend vendor lock-in | The project runs DuckDB locally today. The same SQL body is a Snowflake-compatible `CREATE OR REPLACE TABLE AS …` via SQLGlot dialect translation — swap `juncture.yaml` `type: duckdb` for `type: snowflake`. |

## The DAG

Seven CSV seeds flow through five staging models, three intermediate
models (one of which is ephemeral), five marts and three Python
transforms:

```
                     +----------------------+
campaigns ---------> | stg_campaigns        | ----.
                     +----------------------+     |
customers ---------> | stg_customers        | --.  \
                     +----------------------+   |   \
product_categories ----.                        |    \
                       \                        |     \
products -----------> | stg_products         | --.   |      \
                      +---------------------+   |   |       \
orders  ----------->  | stg_orders          | --*---+--.     \
                      +---------------------+       |  |      \
order_items ----.                                   |  |       \
                 \                                  |  |        \
                  \--> | stg_order_items   | ---+---+--+---.    |
                       +-------------------+    |   |   |  \    |
                                                v   v   v   \   |
                           (ephemeral)        | int_order_facts  |
                          +-----------------+  +---------------+ |
                stg_orders> int_active_customer ---> customer_segments
                          +-----------------+                 |
                                                              v
                          +-----------------+    +----------------------+
                stg_orders> int_rfm_inputs  |    |  campaign_performance |
                          +-----------------+    +----------------------+
                                  |                        ^
                                  v                        |
                            +-------------+           int_order_facts
                            | rfm_scores  | (python)
                            +-------------+
                                 \   \------------+
                                  \               |
                                   v              v
                          +-------------------+  +---------------+
                          | customer_ltv      |  |  daily_revenue |
                          +-------------------+  +----------+-----+
                                                            |
                                                            v
                                                  +-----------------------+
                                                  | daily_revenue_anomalies |
                                                  +-----------------------+

                product_performance <--- stg_order_items, stg_products
                country_cohort_retention <--- stg_customers, stg_orders
```

`juncture compile --project examples/eu_ecommerce` prints the layer-by-layer
ordering the executor walks.

## Models

### Seeds (CSV, loaded once before the DAG runs)

| Seed | Rows @ small | What it represents |
|------|--------------|--------------------|
| `product_categories` | 15 | Four departments × a handful of categories |
| `products` | 100 | SKUs with cost / price / launch date |
| `customers` | 5 000 | EU customers across CZ / DE / FR / NL |
| `campaigns` | 15 | Marketing campaigns over 8 quarters |
| `orders` | 20 000 | Two years of orders with 4 statuses |
| `order_items` | ~35 000 | 1–5 line items per order |
| `web_sessions` | 2 000 | Anon + logged-in web-session stream |

### Staging (`models/staging/`, SQL)

- `stg_customers` — cast types, lowercase emails, derive `signup_year`.
- `stg_orders` — filter to closed statuses, normalise CZK → EUR.
- `stg_order_items` — join to products + orders; compute `net_amount_eur`
  and `margin_eur` line-by-line.
- `stg_products` — join to categories; precompute `margin_pct`.
- `stg_campaigns` — derive `campaign_duration_days` and `channel_group`.

### Intermediate (`models/intermediate/`)

- `int_active_customer` — **ephemeral**; the shared definition of "active
  customer" reused by marts.
- `int_order_facts` — order-grain fact with customer + campaign +
  product rollup. The join-hub for every mart.
- `int_rfm_inputs` — per-customer recency / frequency / monetary
  aggregates, fed into the Python RFM scorer.

### Marts (`models/marts/`, SQL)

- `customer_segments` — `vip` / `loyal` / `regular` / `at_risk` / `lost`;
  thresholds come from `juncture.yaml` `vars`.
- `campaign_performance` — per-campaign ROAS, return on margin.
- `daily_revenue` — revenue per `(date × country × channel_group)`;
  feeds the anomaly detector.
- `product_performance` — per-SKU sales, refund rate, realised margin.
- `country_cohort_retention` — signup-month cohort retention triangle
  per country.

### Python (`models/python/`)

- `rfm_scores` — quintile RFM scoring over `int_rfm_inputs`. Uses
  `pandas.qcut` because portable RFM in SQL is miserable.
- `daily_revenue_anomalies` — 14-day rolling-window z-score on
  `daily_revenue`; flags `|z| >= 2`.
- `customer_ltv` — closed-form CLV blending SQL (`int_order_facts`) with
  Python (`rfm_scores`); `expected_lifetime_months` is parameterised by
  `rfm_tier`.

## Running at different scales

| Scale | Customers | Products | Orders | Campaigns | Order items |
|-------|-----------|----------|--------|-----------|-------------|
| small | 5 000 | 100 | 20 000 | 15 | ~35 000 |
| medium | 50 000 | 300 | 200 000 | 80 | ~350 000 |
| full | 500 000 | 500 | 3 000 000 | 160 | ~5 000 000 |

```bash
python examples/eu_ecommerce/scripts/generate_data.py --scale small
python examples/eu_ecommerce/scripts/generate_data.py --scale medium
python examples/eu_ecommerce/scripts/generate_data.py --scale full
```

Runtime is seconds for small, tens of seconds for medium, and minutes
for full on a modern laptop — concrete numbers live in
[`docs/BENCHMARKS.md`](../../docs/BENCHMARKS.md) once measured on
reference hardware.

## Custom tests

Two SQL tests live under `tests/` and run after the model DAG:

- `assert_no_negative_revenue.sql` — no `stg_orders.total_amount_eur < 0`.
- `assert_campaign_dates_sane.sql` — `ended_at >= started_at` for every
  campaign.

Both follow the Juncture convention that a custom test returns the
failing rows; zero rows = pass.

## See also

- [`docs/VISION.md`](../../docs/VISION.md) — the ten problems this
  example demonstrates solutions for.
- [`docs/DESIGN.md`](../../docs/DESIGN.md) — Project / DAG / Adapter /
  Executor internals used here.
- [`docs/CONFIGURATION.md`](../../docs/CONFIGURATION.md) — `juncture.yaml`,
  `vars`, `schema.yml`, seeds layout reference.
- `examples/ecommerce/` — the lightweight predecessor; this project
  extends it with seeds, vars, ephemeral models, marts, and mixed
  SQL/Python DAG paths.

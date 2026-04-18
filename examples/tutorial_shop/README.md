# tutorial_shop

Companion project to [`docs/TUTORIAL.md`](../../docs/TUTORIAL.md).
Full L4 state — macros, an ephemeral dimension, a Python model, and
external `--var` overrides — so you can copy-paste-compare while
you walk the tutorial.

## Run it

```bash
# Build everything end-to-end against DuckDB + run the data tests.
juncture run --project examples/tutorial_shop --test

# Change the window from the CLI without touching SQL.
juncture run --project examples/tutorial_shop \
  --var as_of=2026-01-20 --var lookback_days=7

# Stricter VIP bar for a one-off analysis.
juncture run --project examples/tutorial_shop --var vip_threshold_eur=1000

# Browse the DAG + source + tests + portfolio.
juncture web --project examples/tutorial_shop
```

## Layout

```
examples/tutorial_shop/
├── juncture.yaml                # project config (jinja: true, vars:, DuckDB conn)
├── seeds/
│   ├── orders.csv               # 20 rows, 2 months
│   └── customers.csv            # 8 rows
├── macros/
│   ├── dates.sql                # my_date(col), my_month(col)
│   └── tiers.sql                # is_vip(amount_col) — reads var('vip_threshold_eur')
└── models/
    ├── schema.yml               # materialization + tests per model
    ├── stg_orders.sql           # type casts + filter status='completed'
    ├── shared/
    │   └── customer_tier.sql    # ephemeral: per-customer LTV + tier
    ├── marts/
    │   ├── daily_revenue.sql    # day-level revenue windowed by --var
    │   └── customer_summary.sql # customers joined with the ephemeral tier
    └── python/
        └── cohort_retention.py  # pandas cohort pivot, ctx.ref / ctx.vars
```

## What each level of the tutorial corresponds to

- **L1** — `seeds/*.csv` + a single mart that reads one via `ref()`.
- **L2** — add `models/python/cohort_retention.py`; SQL and Python
  share one DAG.
- **L3** — `macros/` loaded globally (jinja: true required),
  `materialization: ephemeral` on `customer_tier`.
- **L4** — `--var as_of=...` / `--var lookback_days=...` override the
  `juncture.yaml vars:` defaults; `ctx.vars(...)` in Python and
  `{{ var(...) }}` in SQL read from the same merged bag.

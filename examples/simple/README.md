# Example: simple

A four-model project to demonstrate Juncture's core workflow.

```
models/
├── stg_users.sql         (source)
├── stg_orders.sql        (source)
├── daily_revenue.sql     (aggregate of stg_orders)
├── customer_lifetime_value.sql (join of both)
└── schema.yml            (tests + descriptions)
```

The DAG:

```
stg_users ────┐
              ├──> customer_lifetime_value
stg_orders ───┤
              └──> daily_revenue
```

## Run

```bash
cd examples/simple
juncture compile        # show what will run
juncture run --test     # materialize + run tests
juncture docs           # export lineage to target/docs/manifest.json
```

Inspect the result:

```bash
duckdb data/simple.duckdb -c 'SELECT * FROM main.customer_lifetime_value'
```

# Example: ecommerce

A mixed SQL + Python project showing how Juncture blends the two in one DAG.

```
                    ┌─────────────────┐
raw_customers ──────┤ dim_customers   ├─┐
                    └─────────────────┘ │
                                        ├──> customer_segment (Python)
raw_orders ──> fct_completed_orders ────┘
                    │
                    └──> country_revenue (SQL)
```

- **raw_customers / raw_orders**: SQL inline seeds.
- **fct_completed_orders**: SQL filter (`status = 'completed'`).
- **dim_customers**: SQL enrichment with `days_since_signup`.
- **country_revenue**: SQL aggregate joining facts + dim.
- **customer_segment**: Python model using pandas to classify customers
  by RFM-style rules.

## Run

```bash
cd examples/ecommerce
juncture run --test
duckdb data/ecommerce.duckdb -c 'SELECT * FROM main.country_revenue'
duckdb data/ecommerce.duckdb -c 'SELECT segment, COUNT(*) FROM main.customer_segment GROUP BY 1'
```

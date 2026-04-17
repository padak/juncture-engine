# Juncture

> Multi-backend SQL + Python transformation engine. Local-first, DuckDB-native, Keboola-compatible.

**Status:** early alpha (`0.1.0a0`). See [`docs/DESIGN.md`](docs/DESIGN.md)
for architecture, [`docs/RESEARCH.md`](docs/RESEARCH.md) for competitive
analysis, [`docs/ROADMAP.md`](docs/ROADMAP.md) for what's next.

## Why?

Today, Keboola splits data transformations across four components:
`snowflake-transformation`, `python-transformation`, `duckdb-transformation`,
`dbt-transformation`. None talks to the others, code lives in Keboola (not
git), mixing SQL and Python is impossible, parallelism is manual, and
backend arbitrage is unheard of.

Juncture unifies them into one engine with these guarantees:

- **SQL and Python in the same DAG** — a Python `@transform` can consume a
  SQL model and a SQL model can consume a Python output.
- **Local-first** — runs on a laptop against DuckDB with zero network
  access. Same project runs in Keboola against Snowflake or BigQuery.
- **Multi-backend via SQLGlot** — translate SQL between DuckDB, Snowflake,
  BigQuery, Postgres.
- **Parallelism by default** — independent models run concurrently, layer
  by layer. A 30-update block can drop from 30x serial to 1x parallel.
- **Data tests are first class** — `not_null`, `unique`, `relationships`,
  `accepted_values` ship out of the box.
- **Agent-friendly** — an Anthropic Skill ships in the repo
  ([`skills/juncture/SKILL.md`](skills/juncture/SKILL.md)) so Claude or any
  agent can author, run, debug Juncture projects directly.

## Install

```bash
git clone https://github.com/keboola/new-transformations juncture
cd juncture
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,pandas]"
```

## 60-second tour

```bash
juncture init demo               # scaffold
cd demo
juncture compile                 # show the DAG
juncture run --test              # run models, then data tests
juncture docs                    # emit manifest.json (lineage)
juncture translate 'SELECT TO_VARCHAR(42)' --from snowflake --to duckdb
```

Try the examples:

```bash
cd examples/simple   && juncture run --test
cd examples/ecommerce && juncture run --test   # SQL + Python in one DAG
```

## Project layout

```
my_project/
├── juncture.yaml           # config + connections
├── models/
│   ├── stg_orders.sql
│   ├── stg_customers.sql
│   ├── customer_segment.py   # Python @transform
│   └── schema.yml            # column descriptions + data tests
```

A SQL model is a `.sql` file with one `SELECT`, referencing upstream
models via `{{ ref('stg_orders') }}`. A Python model is a function
decorated with `@transform` returning a DataFrame.

```python
from juncture import transform

@transform(depends_on=["fct_completed_orders"])
def revenue_summary(ctx):
    orders = ctx.ref("fct_completed_orders").to_pandas()
    return orders.groupby("country")["amount"].sum().reset_index()
```

## Current status

- ✅ MVP (v0.1): DuckDB adapter, SQL + Python models, DAG executor, 4
  built-in tests, CLI, 29 tests passing.
- ⏳ v0.2: seeds, Jinja mode, incremental state, env var interpolation.
- ⏳ v0.3: Snowflake, BigQuery, Postgres adapters.
- ⏳ v0.4: Keboola component wrapper, OpenLineage events.

Full roadmap in [`docs/ROADMAP.md`](docs/ROADMAP.md).

## License

Apache 2.0. See [`LICENSE`](LICENSE).

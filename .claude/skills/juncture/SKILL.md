---
name: juncture
description: Author, run, debug, and migrate Juncture data transformation projects. Juncture is a multi-backend SQL + Python engine (DuckDB-native locally, Snowflake/BigQuery/Postgres in production via SQLGlot translation) that unifies what dbt + a Python framework + a Keboola transformation component would do separately. Use when the user asks to (1) scaffold a new project, (2) write/modify SQL or Python models, (3) run / test / inspect a pipeline, (4) translate SQL between warehouses, (5) migrate a Keboola transformation (raw config JSON or `kbagent sync pull` layout), (6) classify or repair migration errors, (7) browse the DAG / run history in the web UI, or (8) configure dev/staging/prod environments via the `profiles:` block. Triggers — "juncture", "transformation project", "DuckDB pipeline", "dbt alternative", "ref() model", "migrate Keboola transformation", "EXECUTE materialization", "juncture web", "juncture profiles".
---

# Juncture skill

Juncture is a multi-backend SQL + Python transformation engine. Local-first,
DuckDB-native, Keboola-compatible. Apache 2.0, Python 3.11+. CLI entrypoint:
`juncture` (Typer).

## Project shape

```
my_project/
├── juncture.yaml           # required: connections, vars, profiles, jinja
├── models/                 # required: .sql files + .py @transform functions
│   ├── staging/stg_orders.sql
│   ├── marts/daily_revenue.sql
│   ├── marts/customer_segment.py
│   └── schema.yml          # columns, tests, materialization, governance
├── seeds/                  # optional: .csv or parquet directories
├── macros/                 # optional: shared {% macro %} (jinja: true)
├── tests/                  # optional: custom .sql tests (return failing rows)
└── target/                 # generated: run_history.jsonl, manifest.json
```

Model name = filename without extension. `models/a/x.sql` and
`models/b/x.sql` is a hard error — subdirs are organisational only.

## Core concepts in 60 seconds

1. **Model** — `.sql` file or Python function decorated with `@transform`.
2. **Dependencies** — `{{ ref('other') }}` in SQL, `depends_on=[...]` in
   Python. Both styles share one DAG; SQL can ref Python and vice versa.
3. **Materialization** — `table` (default), `view`, `incremental`,
   `ephemeral`, `execute`. Details: [`references/materializations.md`](references/materializations.md).
4. **Adapter** — DuckDB shipped; Snowflake stub. New backends implement
   `juncture.adapters.base.Adapter`.
5. **Executor** — runs DAG layer by layer; threads default to 4. Failures
   cascade: descendants get `skipped + upstream_failed`.

## CLI surface (current)

```
Core workflow:
  juncture init PATH [--name N]
  juncture compile [--json] [--dot file.dot] [--profile NAME]
  juncture run     [--select +x+] [--exclude tag:X] [--test]
                   [--var k=v] [--dry-run] [--reuse-seeds]
                   [--parallelism|-P N] [--continue-on-error]
                   [--disable a,b] [--enable-only x,y] [--profile NAME]
  juncture test    [--select s] [--profile NAME]
  juncture docs    [-o OUT] [--profile NAME]
  juncture web     [--host 127.0.0.1] [--port 8765] [--profile NAME]

Tools (sub-apps):
  juncture sql      translate | sanitize | split
  juncture migrate  keboola | sync-pull
  juncture debug    diagnostics
```

Old flat names (`juncture translate`, `juncture migrate-keboola`,
`juncture diagnostics`, …) are kept as **hidden deprecated aliases** —
existing scripts work but new code should use the sub-app form.

Exit code 0 on success, non-zero on any failed model/test. Prefer `--json`
modes for agent consumption (`compile --json`, `docs`).

### Selector grammar

| Selector | Meaning |
|----------|---------|
| `orders` | just this model |
| `+orders` | this + all upstream ancestors |
| `orders+` | this + all downstream descendants |
| `+orders+` | both directions |
| `tag:marts` | every model tagged `marts` in `schema.yml` |

## Decision tree — which reference to read

| User intent | Read first |
|---|---|
| "Start a new project / I'm new here" | [`references/lifecycle.md`](references/lifecycle.md) |
| "What goes in juncture.yaml / schema.yml?" | [`references/yaml-schema.md`](references/yaml-schema.md) |
| "Migrate from Keboola / dbt" | [`references/migration.md`](references/migration.md) |
| "Run is failing / errors I don't understand" | [`references/troubleshooting.md`](references/troubleshooting.md) |
| "Which materialization should I pick? / Incremental?" | [`references/materializations.md`](references/materializations.md) |
| "How do dev/staging/prod profiles work?" | [`references/yaml-schema.md`](references/yaml-schema.md) §Profiles |
| "What does `juncture web` show?" | This file §Web UI below |

## The authoring loop (TL;DR)

1. `juncture compile --project PATH --json` — see existing DAG, find typos.
2. Add **one** `.sql` or `.py` model.
3. `juncture run --project PATH --select +your_model+ --test` — run only
   the needed subtree.
4. Inspect: `duckdb data/<project>.duckdb -c 'SELECT * FROM main.<model> LIMIT 5'`.
5. Open `juncture web --project PATH` for DAG + run history (HTTP on 8765).

Full lifecycle (bootstrap → first model → testing → migration → CI):
[`references/lifecycle.md`](references/lifecycle.md).

## Quick examples

### SQL model

```sql
-- models/marts/daily_revenue.sql
SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
FROM {{ ref('stg_orders') }}
WHERE status = 'completed'
GROUP BY order_date
```

Use `{{ ref('name') }}` only — raw table names bypass DAG inference.
`$ref(name)` (brace-free) is also accepted. Don't write `CREATE TABLE` —
Juncture wraps your `SELECT` per materialization (exception: `execute`).

### Python model

```python
# models/marts/customer_segment.py
from juncture import transform


@transform(depends_on=["fct_orders", "dim_customers"])
def customer_segment(ctx):
    orders = ctx.ref("fct_orders").to_pandas()
    threshold = int(ctx.vars("vip_threshold_eur", 500))
    # ... business logic ...
    return result_df  # pandas / polars / pyarrow.Table all accepted
```

`ctx.ref(name)` returns a `pyarrow.Table` (`.to_pandas()`, `.to_pylist()`).
`ctx.vars(key, default)` reads merged vars (juncture.yaml + `--var`). Use
`ctx.logger.info(...)`, never `print()`.

## Web UI (`juncture web`)

```bash
juncture web --project PATH      # binds to 127.0.0.1:8765
```

stdlib `http.server`, no extras dependency. Reads project on each request
so `schema.yml` edits surface on refresh. Tabs: **DAG** (cytoscape.js with
kind shapes + status borders + PII rings + search), **Models** (source
viewer + columns + tests + history sparkline + governance), **Project**
(`juncture.yaml` + README), **Runs** (history from
`target/run_history.jsonl`, drill-down + classified diagnostics),
**Portfolio** + **Reliability** (owner/team/SLA aggregation), **LLM kb**
(single-shot JSON snapshot for ingesting into another agent).

## What an agent should NOT do without explicit permission

- Push to a remote git repo or open a PR.
- Drop tables, delete `.duckdb` files, or wipe `.juncture/` cache.
- Modify `connections:` (could break shared configs).
- Write secrets into any tracked file.
- Skip tests when a run fails — fix the root cause.
- Re-introduce `Co-Authored-By` footers in commits (project policy).

## Reference docs in the repo

- `docs/TUTORIAL.md` — four-level hands-on walkthrough (companion: `examples/tutorial_shop/`).
- `docs/CONFIGURATION.md` — full `juncture.yaml`, profiles, schema.yml.
- `docs/DESIGN.md` — architecture (Project, DAG, Adapter, Executor).
- `docs/MIGRATION_TIPS.md` — Snowflake → DuckDB cross-dialect field notes.
- `docs/ROADMAP.md` — current phase, shipped features, what's next.
- `examples/` — `simple`, `ecommerce`, `eu_ecommerce`, `tutorial_shop`.
